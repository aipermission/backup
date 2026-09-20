package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (s *Store) createPreMigrationSnapshot(ctx context.Context, schemaVersion int) error {
	finalPath := filepath.Join(s.dataDir, fmt.Sprintf("metadata.pre-migration-v%d.db", schemaVersion))
	info, err := os.Lstat(finalPath)
	if err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("metadata migration snapshot is not a regular file: %s", finalPath)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect metadata migration snapshot: %w", err)
	}
	pendingPath := finalPath + ".pending"
	if err := os.Remove(pendingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale metadata migration snapshot: %w", err)
	}
	defer os.Remove(pendingPath)
	if _, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, pendingPath); err != nil {
		return fmt.Errorf("create metadata migration snapshot: %w", err)
	}
	if err := os.Chmod(pendingPath, 0o600); err != nil {
		return fmt.Errorf("protect metadata migration snapshot: %w", err)
	}
	if err := syncFile(pendingPath); err != nil {
		return fmt.Errorf("sync metadata migration snapshot: %w", err)
	}
	if err := validateMigrationSnapshot(ctx, pendingPath, schemaVersion); err != nil {
		return err
	}
	if err := moveFileDurably(pendingPath, finalPath, true); err != nil {
		return fmt.Errorf("publish metadata migration snapshot: %w", err)
	}
	return nil
}

func validateMigrationSnapshot(ctx context.Context, path string, expectedVersion int) error {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open metadata migration snapshot: %w", err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	var integrity string
	if err := database.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		return fmt.Errorf("check metadata migration snapshot integrity: %w", err)
	}
	if integrity != "ok" {
		return fmt.Errorf("metadata migration snapshot integrity check failed: %s", integrity)
	}
	var version int
	if err := database.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read metadata migration snapshot version: %w", err)
	}
	if version != expectedVersion {
		return fmt.Errorf("metadata migration snapshot version %d does not match source version %d", version, expectedVersion)
	}
	return nil
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
