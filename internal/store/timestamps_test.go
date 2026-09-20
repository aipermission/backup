package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "modernc.org/sqlite"
)

func TestNormalizeMetadataTimestampsProcessesBoundedBatches(t *testing.T) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE backup_streams (id TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
		CREATE TABLE backups (id TEXT PRIMARY KEY, created_at TEXT NOT NULL);
		CREATE TABLE pending_blob_deletions (storage_path TEXT PRIMARY KEY, queued_at TEXT NOT NULL);
	`); err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= metadataTimestampMigrationBatchSize; index++ {
		id := fmt.Sprintf("stream-%04d", index)
		if _, err := database.Exec(`INSERT INTO backup_streams(id, created_at, updated_at) VALUES (?, '2026-01-01T00:00:00.1Z', '2026-01-01T00:00:00.11Z')`, id); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := normalizeMetadataTimestamps(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var createdAt, updatedAt string
	if err := database.QueryRow(`SELECT created_at, updated_at FROM backup_streams WHERE id = ?`, fmt.Sprintf("stream-%04d", metadataTimestampMigrationBatchSize)).Scan(&createdAt, &updatedAt); err != nil {
		t.Fatal(err)
	}
	if createdAt != "2026-01-01T00:00:00.100000000Z" || updatedAt != "2026-01-01T00:00:00.110000000Z" {
		t.Fatalf("last batch timestamps=%q %q", createdAt, updatedAt)
	}
}
