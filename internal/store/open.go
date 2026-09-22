package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

func Open(dataDir string, options ...Options) (*Store, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("data directory is required")
	}
	if len(options) > 1 {
		return nil, errors.New("only one store options value is supported")
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
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, fmt.Errorf("protect storage directory: %w", err)
		}
	}

	metadataPath := filepath.Join(dataDir, "metadata.db")
	db, err := sql.Open("sqlite", metadataPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata database: %w", err)
	}
	db.SetMaxOpenConns(1)
	opts, err := normalizeOptions(options)
	if err != nil {
		db.Close()
		return nil, err
	}
	store := &Store{
		db: db, dataDir: dataDir, blobDir: blobDir, tempDir: tempDir,
		now: time.Now, maxStorageBytes: opts.MaxStorageBytes, maxUploadOps: opts.MaxUploadOperations,
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
	if err := store.cleanupOrphanedBlobs(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func normalizeOptions(options []Options) (Options, error) {
	var opts Options
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.MaxStorageBytes < 0 {
		return Options{}, errors.New("maximum storage bytes cannot be negative")
	}
	if opts.MaxUploadOperations < 0 {
		return Options{}, errors.New("maximum upload operations cannot be negative")
	}
	if opts.MaxUploadOperations == 0 {
		opts.MaxUploadOperations = DefaultMaxUploadOperations
	}
	return opts, nil
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
	if schemaVersion > 0 && schemaVersion < SchemaVersion {
		if err := s.createPreMigrationSnapshot(ctx, schemaVersion); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
`); err != nil {
		return fmt.Errorf("configure metadata database: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metadata schema transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, metadataSchema); err != nil {
		return fmt.Errorf("initialize metadata database: %w", err)
	}
	if schemaVersion < SchemaVersion {
		if err := migrateMetadataSchema(ctx, tx, schemaVersion); err != nil {
			return err
		}
	}
	if err := ensureMetadataIndexes(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata schema transaction: %w", err)
	}
	return s.cleanupPendingDeletions(ctx)
}

const metadataSchema = `
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
  storage_path TEXT NOT NULL UNIQUE,
  operation_key TEXT
);
CREATE INDEX IF NOT EXISTS idx_backups_stream_created
  ON backups(stream_id, created_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS pending_blob_deletions (
  storage_path TEXT PRIMARY KEY,
  queued_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backup_upload_operations (
  operation_key TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  database_name TEXT NOT NULL,
  source_installation_id TEXT NOT NULL,
  backup_id TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_backup_upload_operations_backup
  ON backup_upload_operations(backup_id);
CREATE INDEX IF NOT EXISTS idx_backup_upload_operations_created
  ON backup_upload_operations(created_at);
`

func migrateMetadataSchema(ctx context.Context, tx *sql.Tx, schemaVersion int) error {
	if err := ensureRetentionColumn(ctx, tx); err != nil {
		return err
	}
	if err := ensureOperationKeyColumn(ctx, tx); err != nil {
		return err
	}
	if schemaVersion < 5 {
		if err := normalizeMetadataTimestamps(ctx, tx); err != nil {
			return err
		}
	}
	if schemaVersion < 6 {
		if err := migrateUploadOperationLedger(ctx, tx); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion)); err != nil {
		return fmt.Errorf("write metadata schema version: %w", err)
	}
	return nil
}

func ensureRetentionColumn(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('backup_streams') WHERE name = 'retention_keep_latest'`).Scan(&count); err != nil {
		return fmt.Errorf("inspect backup stream retention column: %w", err)
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE backup_streams ADD COLUMN retention_keep_latest INTEGER CHECK(retention_keep_latest BETWEEN 1 AND 1000)`); err != nil {
			return fmt.Errorf("add backup stream retention policy: %w", err)
		}
	}
	return nil
}

func ensureOperationKeyColumn(ctx context.Context, tx *sql.Tx) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('backups') WHERE name = 'operation_key'`).Scan(&count); err != nil {
		return fmt.Errorf("inspect backup operation key column: %w", err)
	}
	if count == 0 {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE backups ADD COLUMN operation_key TEXT`); err != nil {
			return fmt.Errorf("add backup operation key: %w", err)
		}
	}
	return nil
}

func migrateUploadOperationLedger(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO backup_upload_operations (
			operation_key, stream_id, database_name, source_installation_id, backup_id, created_at
		)
		SELECT b.operation_key, b.stream_id, s.database_name, b.source_installation_id, b.id, b.created_at
		FROM backups b JOIN backup_streams s ON s.id = b.stream_id
		WHERE b.operation_key IS NOT NULL
		ON CONFLICT(operation_key) DO NOTHING`); err != nil {
		return fmt.Errorf("migrate backup upload operation ledger: %w", err)
	}
	return nil
}

func ensureMetadataIndexes(ctx context.Context, tx *sql.Tx) error {
	statements := []struct {
		query   string
		message string
	}{
		{`CREATE UNIQUE INDEX IF NOT EXISTS idx_backups_operation_key ON backups(operation_key) WHERE operation_key IS NOT NULL`, "ensure backup operation key index"},
		{`CREATE INDEX IF NOT EXISTS idx_backup_upload_operations_backup ON backup_upload_operations(backup_id)`, "ensure backup upload operation ledger index"},
		{`DROP INDEX IF EXISTS idx_backup_upload_operations_created`, "remove obsolete backup upload operation age index"},
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query); err != nil {
			return fmt.Errorf("%s: %w", statement.message, err)
		}
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
	paths, err := scanStringRows(rows, "pending backup deletion")
	if err != nil {
		return err
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

func scanStringRows(rows *sql.Rows, label string) ([]string, error) {
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan %s: %w", label, err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate %ss: %w", label, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close %ss: %w", label, err)
	}
	return values, nil
}

func (s *Store) cleanupOrphanedBlobs(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT storage_path FROM backups`)
	if err != nil {
		return fmt.Errorf("list referenced backup blobs: %w", err)
	}
	paths, err := scanStringRows(rows, "referenced backup blob")
	if err != nil {
		return err
	}
	referenced := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		referenced[filepath.Clean(path)] = struct{}{}
	}
	return filepath.WalkDir(s.blobDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk backup blob directory: %w", walkErr)
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".aipdb" {
			return nil
		}
		relative, err := filepath.Rel(s.dataDir, path)
		if err != nil {
			return fmt.Errorf("resolve backup blob path: %w", err)
		}
		if _, ok := referenced[filepath.Clean(relative)]; ok {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove orphaned backup blob: %w", err)
		}
		return nil
	})
}

func (s *Store) resolveStoragePath(storedPath string) (string, error) {
	path := filepath.Join(s.dataDir, filepath.Clean(storedPath))
	relative, err := filepath.Rel(s.dataDir, path)
	if err != nil || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative == ".." {
		return "", errors.New("stored backup path escapes data directory")
	}
	return path, nil
}
