package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

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
	items, err := scanStreams(rows, limit)
	if err != nil {
		return Page[Stream]{}, err
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

func scanStreams(rows *sql.Rows, limit int) ([]Stream, error) {
	items := make([]Stream, 0, limit+1)
	for rows.Next() {
		var item Stream
		if err := rows.Scan(&item.ID, &item.DatabaseName, &item.CreatedAt, &item.UpdatedAt, &item.BackupCount, &item.RetentionKeepLatest); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan backup stream: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate backup streams: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close backup stream rows: %w", err)
	}
	return items, nil
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
	if err := verifyBackupFile(file, item); err != nil {
		file.Close()
		return Backup{}, nil, err
	}
	return item, file, nil
}

func verifyBackupFile(file *os.File, item Backup) error {
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return fmt.Errorf("verify stored backup: %w", err)
	}
	if size != item.SizeBytes || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), item.SHA256) {
		return ErrCorrupt
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind stored backup: %w", err)
	}
	return nil
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
