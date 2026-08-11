package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrInvalidInput   = errors.New("invalid input")
	ErrNotFound       = errors.New("backup not found")
	ErrLastBackup     = errors.New("the last backup in a stream cannot be deleted")
	ErrStreamConflict = errors.New("stream metadata conflicts with the existing stream")
	ErrCorrupt        = errors.New("stored backup checksum does not match metadata")
	ErrQuotaExceeded  = errors.New("backup storage quota exceeded")
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

const SchemaVersion = 3

type Options struct {
	MaxStorageBytes int64
}

type Store struct {
	db              *sql.DB
	dataDir         string
	blobDir         string
	tempDir         string
	now             func() time.Time
	maxStorageBytes int64
	mutationMu      sync.Mutex
}

type Backup struct {
	ID                    string `json:"id"`
	StreamID              string `json:"stream_id"`
	DatabaseName          string `json:"database_name"`
	SourceInstallationID  string `json:"source_installation_id"`
	Filename              string `json:"filename"`
	SizeBytes             int64  `json:"size_bytes"`
	SHA256                string `json:"sha256"`
	CreatedAt             string `json:"created_at"`
	RetentionDeletedCount int    `json:"retention_deleted_count,omitempty"`
	storagePath           string
}

type Stream struct {
	ID                  string  `json:"id"`
	DatabaseName        string  `json:"database_name"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
	BackupCount         int64   `json:"backup_count"`
	RetentionKeepLatest *int    `json:"retention_keep_latest,omitempty"`
	LatestBackup        *Backup `json:"latest_backup,omitempty"`
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type PruneResult struct {
	StreamID     string `json:"stream_id"`
	KeepLatest   int    `json:"keep_latest"`
	DeletedCount int    `json:"deleted_count"`
}

type DeleteResult struct {
	StreamID     string   `json:"stream_id"`
	DeletedIDs   []string `json:"deleted_ids"`
	DeletedCount int      `json:"deleted_count"`
}

type deletionCandidate struct {
	id   string
	path string
	size int64
}

func Open(dataDir string, options ...Options) (*Store, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("data directory is required")
	}
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	blobDir := filepath.Join(dataDir, "blobs")
	tempDir := filepath.Join(dataDir, "temporary")
	for _, dir := range []string{dataDir, blobDir, tempDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create storage directory: %w", err)
		}
	}

	metadataPath := filepath.Join(dataDir, "metadata.db")
	db, err := sql.Open("sqlite", metadataPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata database: %w", err)
	}
	db.SetMaxOpenConns(1)
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.MaxStorageBytes < 0 {
		db.Close()
		return nil, errors.New("maximum storage bytes cannot be negative")
	}
	store := &Store{
		db: db, dataDir: dataDir, blobDir: blobDir, tempDir: tempDir,
		now: time.Now, maxStorageBytes: opts.MaxStorageBytes,
	}
	if err := store.initialize(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := os.Chmod(metadataPath, 0o600); err != nil {
		db.Close()
		return nil, fmt.Errorf("protect metadata database: %w", err)
	}
	if err := store.cleanupTemporary(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) initialize(ctx context.Context) error {
	var schemaVersion int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&schemaVersion); err != nil {
		return fmt.Errorf("read metadata schema version: %w", err)
	}
	if schemaVersion > SchemaVersion {
		return fmt.Errorf("metadata schema version %d is newer than supported version %d", schemaVersion, SchemaVersion)
	}
	const schema = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
CREATE TABLE IF NOT EXISTS backup_streams (
  id TEXT PRIMARY KEY,
  database_name TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backups (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL REFERENCES backup_streams(id),
  source_installation_id TEXT NOT NULL,
  filename TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
  sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL,
  storage_path TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_backups_stream_created
  ON backups(stream_id, created_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS pending_blob_deletions (
  storage_path TEXT PRIMARY KEY,
  queued_at TEXT NOT NULL
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize metadata database: %w", err)
	}
	if schemaVersion < 3 {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE backup_streams ADD COLUMN retention_keep_latest INTEGER CHECK(retention_keep_latest BETWEEN 1 AND 1000)`); err != nil {
			return fmt.Errorf("add backup stream retention policy: %w", err)
		}
	}
	if schemaVersion < SchemaVersion {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion)); err != nil {
			return fmt.Errorf("write metadata schema version: %w", err)
		}
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) cleanupTemporary() error {
	entries, err := os.ReadDir(s.tempDir)
	if err != nil {
		return fmt.Errorf("read temporary directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(s.tempDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove abandoned temporary file: %w", err)
		}
	}
	return nil
}

func (s *Store) cleanupPendingDeletions(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT storage_path FROM pending_blob_deletions ORDER BY queued_at, storage_path`)
	if err != nil {
		return fmt.Errorf("list pending backup deletions: %w", err)
	}
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			rows.Close()
			return fmt.Errorf("scan pending backup deletion: %w", err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate pending backup deletions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close pending backup deletions: %w", err)
	}
	for _, storedPath := range paths {
		path, err := s.resolveStoragePath(storedPath)
		if err != nil {
			return err
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove pruned backup blob: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `DELETE FROM pending_blob_deletions WHERE storage_path = ?`, storedPath); err != nil {
			return fmt.Errorf("complete backup blob deletion: %w", err)
		}
	}
	return nil
}

func (s *Store) resolveStoragePath(storedPath string) (string, error) {
	path := filepath.Join(s.dataDir, filepath.Clean(storedPath))
	relative, err := filepath.Rel(s.dataDir, path)
	if err != nil || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative == ".." {
		return "", errors.New("stored backup path escapes data directory")
	}
	return path, nil
}

func ValidateIdentifier(value string) bool { return identifierPattern.MatchString(value) }

func (s *Store) CreateBackup(ctx context.Context, streamID, databaseName, sourceInstallationID string, body io.Reader) (Backup, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if !ValidateIdentifier(streamID) || !ValidateIdentifier(sourceInstallationID) {
		return Backup{}, fmt.Errorf("%w: stream or source installation identifier is invalid", ErrInvalidInput)
	}
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" || len(databaseName) > 128 {
		return Backup{}, fmt.Errorf("%w: database name must contain 1 to 128 characters", ErrInvalidInput)
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return Backup{}, err
	}
	remaining, err := s.remainingStorageBytes(ctx)
	if err != nil {
		return Backup{}, err
	}
	if s.maxStorageBytes > 0 && remaining == 0 {
		return Backup{}, ErrQuotaExceeded
	}

	id, err := randomID("bkp")
	if err != nil {
		return Backup{}, err
	}
	temporary, err := os.CreateTemp(s.tempDir, id+"-*.upload")
	if err != nil {
		return Backup{}, fmt.Errorf("create temporary upload: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()

	hash := sha256.New()
	copySource := body
	if s.maxStorageBytes > 0 {
		copySource = io.LimitReader(body, remaining+1)
	}
	size, err := io.Copy(io.MultiWriter(temporary, hash), copySource)
	if err != nil {
		return Backup{}, fmt.Errorf("store upload: %w", err)
	}
	if size == 0 {
		return Backup{}, fmt.Errorf("%w: backup body is empty", ErrInvalidInput)
	}
	if s.maxStorageBytes > 0 && size > remaining {
		return Backup{}, ErrQuotaExceeded
	}
	if err := temporary.Sync(); err != nil {
		return Backup{}, fmt.Errorf("flush temporary upload: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return Backup{}, fmt.Errorf("close temporary upload: %w", err)
	}

	createdAt := s.now().UTC()
	createdText := createdAt.Format(time.RFC3339Nano)
	filename := safeFilename(databaseName, createdAt)
	streamDir := filepath.Join(s.blobDir, streamID)
	if err := os.MkdirAll(streamDir, 0o700); err != nil {
		return Backup{}, fmt.Errorf("create stream directory: %w", err)
	}
	finalPath := filepath.Join(streamDir, id+".aipdb")
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return Backup{}, fmt.Errorf("commit uploaded backup: %w", err)
	}
	committed = true
	removeFinal := true
	defer func() {
		if removeFinal {
			_ = os.Remove(finalPath)
		}
	}()
	if err := syncDirectory(streamDir); err != nil {
		return Backup{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Backup{}, fmt.Errorf("begin metadata transaction: %w", err)
	}
	defer tx.Rollback()
	var existingName string
	err = tx.QueryRowContext(ctx, `SELECT database_name FROM backup_streams WHERE id = ?`, streamID).Scan(&existingName)
	switch {
	case err == nil && existingName != databaseName:
		return Backup{}, ErrStreamConflict
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return Backup{}, fmt.Errorf("read stream metadata: %w", err)
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, `INSERT INTO backup_streams(id, database_name, created_at, updated_at) VALUES(?, ?, ?, ?)`, streamID, databaseName, createdText, createdText); err != nil {
			return Backup{}, fmt.Errorf("create backup stream: %w", err)
		}
	default:
		if _, err := tx.ExecContext(ctx, `UPDATE backup_streams SET updated_at = ? WHERE id = ?`, createdText, streamID); err != nil {
			return Backup{}, fmt.Errorf("update backup stream: %w", err)
		}
	}

	relativePath, err := filepath.Rel(s.dataDir, finalPath)
	if err != nil {
		return Backup{}, fmt.Errorf("resolve backup path: %w", err)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if _, err := tx.ExecContext(ctx, `
INSERT INTO backups(id, stream_id, source_installation_id, filename, size_bytes, sha256, created_at, storage_path)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, id, streamID, sourceInstallationID, filename, size, digest, createdText, relativePath); err != nil {
		return Backup{}, fmt.Errorf("store backup metadata: %w", err)
	}
	retentionDeleted, err := s.applyConfiguredRetentionTx(ctx, tx, streamID)
	if err != nil {
		return Backup{}, err
	}
	if err := tx.Commit(); err != nil {
		return Backup{}, fmt.Errorf("commit backup metadata: %w", err)
	}
	removeFinal = false
	_ = s.cleanupPendingDeletions(ctx)
	return Backup{
		ID: id, StreamID: streamID, DatabaseName: databaseName,
		SourceInstallationID: sourceInstallationID, Filename: filename,
		SizeBytes: size, SHA256: digest, CreatedAt: createdText,
		RetentionDeletedCount: retentionDeleted,
	}, nil
}

func (s *Store) ListStreams(ctx context.Context, limit int, cursor string) (Page[Stream], error) {
	createdAt, id, err := decodeCursor(cursor)
	if err != nil {
		return Page[Stream]{}, err
	}
	query := `
SELECT s.id, s.database_name, s.created_at, s.updated_at, COUNT(b.id), s.retention_keep_latest
FROM backup_streams s
LEFT JOIN backups b ON b.stream_id = s.id`
	args := []any{}
	if cursor != "" {
		query += ` WHERE (s.updated_at < ? OR (s.updated_at = ? AND s.id < ?))`
		args = append(args, createdAt, createdAt, id)
	}
	query += ` GROUP BY s.id ORDER BY s.updated_at DESC, s.id DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return Page[Stream]{}, fmt.Errorf("list backup streams: %w", err)
	}
	items := make([]Stream, 0, limit+1)
	for rows.Next() {
		var item Stream
		if err := rows.Scan(&item.ID, &item.DatabaseName, &item.CreatedAt, &item.UpdatedAt, &item.BackupCount, &item.RetentionKeepLatest); err != nil {
			rows.Close()
			return Page[Stream]{}, fmt.Errorf("scan backup stream: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Page[Stream]{}, fmt.Errorf("iterate backup streams: %w", err)
	}
	if err := rows.Close(); err != nil {
		return Page[Stream]{}, fmt.Errorf("close backup stream rows: %w", err)
	}
	// Release the single SQLite connection before loading each latest backup.
	for index := range items {
		latest, err := s.latestBackup(ctx, items[index].ID, items[index].DatabaseName)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return Page[Stream]{}, err
		}
		if err == nil {
			items[index].LatestBackup = &latest
		}
	}
	page := Page[Stream]{Items: items}
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.NextCursor = encodeCursor(last.UpdatedAt, last.ID)
	}
	return page, nil
}

func (s *Store) ListBackups(ctx context.Context, streamID string, limit int, cursor string) (Page[Backup], error) {
	if !ValidateIdentifier(streamID) {
		return Page[Backup]{}, fmt.Errorf("%w: stream identifier is invalid", ErrInvalidInput)
	}
	createdAt, id, err := decodeCursor(cursor)
	if err != nil {
		return Page[Backup]{}, err
	}
	query := `
SELECT b.id, b.stream_id, s.database_name, b.source_installation_id, b.filename,
       b.size_bytes, b.sha256, b.created_at, b.storage_path
FROM backups b JOIN backup_streams s ON s.id = b.stream_id
WHERE b.stream_id = ?`
	args := []any{streamID}
	if cursor != "" {
		query += ` AND (b.created_at < ? OR (b.created_at = ? AND b.id < ?))`
		args = append(args, createdAt, createdAt, id)
	}
	query += ` ORDER BY b.created_at DESC, b.id DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return Page[Backup]{}, fmt.Errorf("list backups: %w", err)
	}
	defer rows.Close()
	items := make([]Backup, 0, limit+1)
	for rows.Next() {
		var item Backup
		if err := rows.Scan(&item.ID, &item.StreamID, &item.DatabaseName, &item.SourceInstallationID, &item.Filename, &item.SizeBytes, &item.SHA256, &item.CreatedAt, &item.storagePath); err != nil {
			return Page[Backup]{}, fmt.Errorf("scan backup: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return Page[Backup]{}, fmt.Errorf("iterate backups: %w", err)
	}
	page := Page[Backup]{Items: items}
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

func (s *Store) OpenBackup(ctx context.Context, streamID, backupID string) (Backup, *os.File, error) {
	if !ValidateIdentifier(streamID) || !ValidateIdentifier(backupID) {
		return Backup{}, nil, ErrNotFound
	}
	var item Backup
	err := s.db.QueryRowContext(ctx, `
SELECT b.id, b.stream_id, s.database_name, b.source_installation_id, b.filename,
       b.size_bytes, b.sha256, b.created_at, b.storage_path
FROM backups b JOIN backup_streams s ON s.id = b.stream_id
WHERE b.stream_id = ? AND b.id = ?`, streamID, backupID).Scan(
		&item.ID, &item.StreamID, &item.DatabaseName, &item.SourceInstallationID,
		&item.Filename, &item.SizeBytes, &item.SHA256, &item.CreatedAt, &item.storagePath,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Backup{}, nil, ErrNotFound
	}
	if err != nil {
		return Backup{}, nil, fmt.Errorf("read backup metadata: %w", err)
	}
	path, err := s.resolveStoragePath(item.storagePath)
	if err != nil {
		return Backup{}, nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return Backup{}, nil, ErrCorrupt
	}
	if err != nil {
		return Backup{}, nil, fmt.Errorf("open stored backup: %w", err)
	}
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		file.Close()
		return Backup{}, nil, fmt.Errorf("verify stored backup: %w", err)
	}
	if size != item.SizeBytes || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), item.SHA256) {
		file.Close()
		return Backup{}, nil, ErrCorrupt
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return Backup{}, nil, fmt.Errorf("rewind stored backup: %w", err)
	}
	return item, file, nil
}

func (s *Store) PruneBackups(ctx context.Context, streamID string, keepLatest int) (PruneResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if !ValidateIdentifier(streamID) || keepLatest < 1 || keepLatest > 1000 {
		return PruneResult{}, fmt.Errorf("%w: stream identifier or keep_latest is invalid", ErrInvalidInput)
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return PruneResult{}, err
	}
	var streamExists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM backup_streams WHERE id = ?`, streamID).Scan(&streamExists); errors.Is(err, sql.ErrNoRows) {
		return PruneResult{}, ErrNotFound
	} else if err != nil {
		return PruneResult{}, fmt.Errorf("read backup stream: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT id, storage_path
FROM backups
WHERE stream_id = ?
ORDER BY created_at DESC, id DESC
LIMIT -1 OFFSET ?`, streamID, keepLatest)
	if err != nil {
		return PruneResult{}, fmt.Errorf("list backups to prune: %w", err)
	}
	var candidates []deletionCandidate
	for rows.Next() {
		var item deletionCandidate
		if err := rows.Scan(&item.id, &item.path); err != nil {
			rows.Close()
			return PruneResult{}, fmt.Errorf("scan backup to prune: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PruneResult{}, fmt.Errorf("iterate backups to prune: %w", err)
	}
	if err := rows.Close(); err != nil {
		return PruneResult{}, fmt.Errorf("close backups to prune: %w", err)
	}
	if len(candidates) == 0 {
		return PruneResult{StreamID: streamID, KeepLatest: keepLatest}, nil
	}

	if err := s.deleteCandidates(ctx, streamID, candidates); err != nil {
		return PruneResult{}, err
	}
	return PruneResult{StreamID: streamID, KeepLatest: keepLatest, DeletedCount: len(candidates)}, nil
}

func (s *Store) DeleteBackups(ctx context.Context, streamID string, backupIDs []string) (DeleteResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if !ValidateIdentifier(streamID) || len(backupIDs) < 1 || len(backupIDs) > 100 {
		return DeleteResult{}, fmt.Errorf("%w: stream identifier or backup ids are invalid", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(backupIDs))
	for _, id := range backupIDs {
		if !ValidateIdentifier(id) {
			return DeleteResult{}, fmt.Errorf("%w: backup id is invalid", ErrInvalidInput)
		}
		if _, exists := seen[id]; exists {
			return DeleteResult{}, fmt.Errorf("%w: backup ids must be unique", ErrInvalidInput)
		}
		seen[id] = struct{}{}
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return DeleteResult{}, err
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE stream_id = ?`, streamID).Scan(&total); err != nil {
		return DeleteResult{}, fmt.Errorf("count stream backups: %w", err)
	}
	if total == 0 {
		return DeleteResult{}, ErrNotFound
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(backupIDs)), ",")
	args := make([]any, 0, len(backupIDs)+1)
	args = append(args, streamID)
	for _, id := range backupIDs {
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, storage_path
FROM backups
WHERE stream_id = ? AND id IN (`+placeholders+`)
ORDER BY created_at DESC, id DESC`, args...)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("list backups to delete: %w", err)
	}
	var candidates []deletionCandidate
	for rows.Next() {
		var item deletionCandidate
		if err := rows.Scan(&item.id, &item.path); err != nil {
			rows.Close()
			return DeleteResult{}, fmt.Errorf("scan backup to delete: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return DeleteResult{}, fmt.Errorf("iterate backups to delete: %w", err)
	}
	if err := rows.Close(); err != nil {
		return DeleteResult{}, fmt.Errorf("close backups to delete: %w", err)
	}
	if len(candidates) != len(backupIDs) {
		return DeleteResult{}, ErrNotFound
	}
	if total <= len(candidates) {
		return DeleteResult{}, ErrLastBackup
	}
	if err := s.deleteCandidates(ctx, streamID, candidates); err != nil {
		return DeleteResult{}, err
	}
	deletedIDs := make([]string, 0, len(candidates))
	for _, item := range candidates {
		deletedIDs = append(deletedIDs, item.id)
	}
	return DeleteResult{StreamID: streamID, DeletedIDs: deletedIDs, DeletedCount: len(deletedIDs)}, nil
}

func (s *Store) deleteCandidates(ctx context.Context, streamID string, candidates []deletionCandidate) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin backup deletion transaction: %w", err)
	}
	if err := queueDeletionCandidates(ctx, tx, streamID, candidates, s.now().UTC()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit backup deletion: %w", err)
	}
	return s.cleanupPendingDeletions(ctx)
}

func (s *Store) latestBackup(ctx context.Context, streamID, databaseName string) (Backup, error) {
	var item Backup
	err := s.db.QueryRowContext(ctx, `
SELECT id, stream_id, source_installation_id, filename, size_bytes, sha256, created_at, storage_path
FROM backups WHERE stream_id = ? ORDER BY created_at DESC, id DESC LIMIT 1`, streamID).Scan(
		&item.ID, &item.StreamID, &item.SourceInstallationID, &item.Filename,
		&item.SizeBytes, &item.SHA256, &item.CreatedAt, &item.storagePath,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Backup{}, ErrNotFound
	}
	if err != nil {
		return Backup{}, fmt.Errorf("read latest backup: %w", err)
	}
	item.DatabaseName = databaseName
	return item, nil
}

func randomID(prefix string) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate backup identifier: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(raw), nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open storage directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync storage directory: %w", err)
	}
	return nil
}

func safeFilename(databaseName string, createdAt time.Time) string {
	var builder strings.Builder
	for _, char := range strings.ToLower(databaseName) {
		switch {
		case char >= 'a' && char <= 'z', char >= '0' && char <= '9':
			builder.WriteRune(char)
		case char == '-', char == '_':
			builder.WriteRune(char)
		default:
			if builder.Len() > 0 && !strings.HasSuffix(builder.String(), "-") {
				builder.WriteByte('-')
			}
		}
	}
	name := strings.Trim(builder.String(), "-")
	if name == "" {
		name = "aipermission"
	}
	return fmt.Sprintf("%s-%s.aipdb", name, createdAt.UTC().Format("20060102T150405Z"))
}

func encodeCursor(createdAt, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(createdAt + "\n" + id))
}

func decodeCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return "", "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("%w: cursor is invalid", ErrInvalidInput)
	}
	parts := strings.Split(string(raw), "\n")
	if len(parts) != 2 || parts[0] == "" || !ValidateIdentifier(parts[1]) {
		return "", "", fmt.Errorf("%w: cursor is invalid", ErrInvalidInput)
	}
	if _, err := time.Parse(time.RFC3339Nano, parts[0]); err != nil {
		return "", "", fmt.Errorf("%w: cursor is invalid", ErrInvalidInput)
	}
	return parts[0], parts[1], nil
}
