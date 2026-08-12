package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreLifecyclePaginationAndPersistence(t *testing.T) {
	dataDir := t.TempDir()
	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 31, 10, 0, 0, 0, time.UTC)
	storage.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}

	first, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte("first")))
	if err != nil {
		t.Fatal(err)
	}
	second, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-b", bytes.NewReader([]byte("second")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CreateBackup(context.Background(), "project-b", "Project B", "install-a", bytes.NewReader([]byte("third"))); err != nil {
		t.Fatal(err)
	}

	page, err := storage.ListBackups(context.Background(), "project-a", 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != second.ID || page.NextCursor == "" {
		t.Fatalf("unexpected first page: %#v", page)
	}
	page, err = storage.ListBackups(context.Background(), "project-a", 1, page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != first.ID || page.NextCursor != "" {
		t.Fatalf("unexpected second page: %#v", page)
	}

	streams, err := storage.ListStreams(context.Background(), 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(streams.Items) != 2 || streams.Items[0].LatestBackup == nil {
		t.Fatalf("unexpected streams: %#v", streams)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	storage, err = Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	item, file, err := storage.OpenBackup(context.Background(), "project-a", first.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if item.SHA256 == "" || item.SizeBytes != 5 {
		t.Fatalf("unexpected persisted metadata: %#v", item)
	}
}

func TestStoreRejectsConflictingStreamAndRemovesBlob(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	if _, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte("first"))); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CreateBackup(context.Background(), "project-a", "Different Project", "install-a", bytes.NewReader([]byte("second"))); !errors.Is(err, ErrStreamConflict) {
		t.Fatalf("expected stream conflict, got %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(storage.blobDir, "project-a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one committed blob, got %d", len(entries))
	}
}

func TestStorePrunesOldBackupsAndPreservesLatestVersions(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	now := time.Date(2026, time.July, 31, 10, 0, 0, 0, time.UTC)
	storage.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	var created []Backup
	for _, value := range []string{"first", "second", "third", "fourth"} {
		item, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte(value)))
		if err != nil {
			t.Fatal(err)
		}
		created = append(created, item)
	}

	result, err := storage.PruneBackups(context.Background(), "project-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 2 || result.KeepLatest != 2 {
		t.Fatalf("unexpected prune result: %#v", result)
	}
	page, err := storage.ListBackups(context.Background(), "project-a", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].ID != created[3].ID || page.Items[1].ID != created[2].ID {
		t.Fatalf("unexpected retained backups: %#v", page.Items)
	}
	for _, pruned := range created[:2] {
		if _, _, err := storage.OpenBackup(context.Background(), "project-a", pruned.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected pruned backup %s to be absent, got %v", pruned.ID, err)
		}
	}
	var pending int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM pending_blob_deletions`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("pending deletion queue was not drained: count=%d err=%v", pending, err)
	}
}

func TestStorePruneValidatesStreamAndRetention(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	if _, err := storage.PruneBackups(context.Background(), "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing stream, got %v", err)
	}
	if _, err := storage.PruneBackups(context.Background(), "project-a", 0); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid retention, got %v", err)
	}
}

func TestStoreDeletesSelectedBackupsAndProtectsLastVersion(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	now := time.Date(2026, time.July, 31, 10, 0, 0, 0, time.UTC)
	storage.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	var created []Backup
	for _, value := range []string{"first", "second", "third", "fourth"} {
		item, createErr := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte(value)))
		if createErr != nil {
			t.Fatal(createErr)
		}
		created = append(created, item)
	}

	result, err := storage.DeleteBackups(context.Background(), "project-a", []string{created[3].ID, created[1].ID})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedCount != 2 || len(result.DeletedIDs) != 2 {
		t.Fatalf("unexpected delete result: %#v", result)
	}
	page, err := storage.ListBackups(context.Background(), "project-a", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].ID != created[2].ID || page.Items[1].ID != created[0].ID {
		t.Fatalf("unexpected retained backups: %#v", page.Items)
	}
	streams, err := storage.ListStreams(context.Background(), 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(streams.Items) != 1 || streams.Items[0].LatestBackup == nil || streams.Items[0].LatestBackup.ID != created[2].ID || streams.Items[0].UpdatedAt != created[2].CreatedAt {
		t.Fatalf("stream latest metadata was not updated: %#v", streams.Items)
	}
	if _, err := storage.DeleteBackups(context.Background(), "project-a", []string{created[2].ID, created[0].ID}); !errors.Is(err, ErrLastBackup) {
		t.Fatalf("expected last backup protection, got %v", err)
	}
	if _, err := storage.DeleteBackups(context.Background(), "project-a", []string{created[0].ID, created[0].ID}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected duplicate ids to be rejected, got %v", err)
	}
	if _, err := storage.DeleteBackups(context.Background(), "project-a", []string{"missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing backup, got %v", err)
	}
	var pending int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM pending_blob_deletions`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("pending deletion queue was not drained: count=%d err=%v", pending, err)
	}
}

func TestStoreDeletionFailureRollsBackAndReleasesConnection(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	first, err := storage.CreateBackup(context.Background(), "rollback-db", "Rollback DB", "install-a", bytes.NewReader([]byte("first")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CreateBackup(context.Background(), "rollback-db", "Rollback DB", "install-a", bytes.NewReader([]byte("second"))); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.db.Exec(`
		CREATE TRIGGER reject_pending_deletion BEFORE INSERT ON pending_blob_deletions
		BEGIN SELECT RAISE(ABORT, 'injected deletion queue failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.DeleteBackups(context.Background(), "rollback-db", []string{first.ID}); err == nil {
		t.Fatal("expected injected deletion queue failure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var count int
	if err := storage.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE stream_id = 'rollback-db'`).Scan(&count); err != nil {
		t.Fatalf("database connection remained locked after rollback: %v", err)
	}
	if count != 2 {
		t.Fatalf("backup count=%d, want both records after rollback", count)
	}
}

func TestStoreDetectsCorruptionAndCleansTemporaryFiles(t *testing.T) {
	dataDir := t.TempDir()
	temporaryDir := filepath.Join(dataDir, "temporary")
	if err := os.MkdirAll(temporaryDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(temporaryDir, "abandoned.upload"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	entries, err := os.ReadDir(temporaryDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary cleanup failed: entries=%d err=%v", len(entries), err)
	}
	created, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte("encrypted")))
	if err != nil {
		t.Fatal(err)
	}
	page, err := storage.ListBackups(context.Background(), "project-a", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(storage.dataDir, page.Items[0].storagePath)
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storage.OpenBackup(context.Background(), "project-a", created.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected corruption error, got %v", err)
	}
}

func TestStoreRejectsNewerMetadataSchema(t *testing.T) {
	dataDir := t.TempDir()
	metadata, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dataDir); err == nil {
		t.Fatal("expected newer metadata schema to be rejected")
	}
}

func TestStoreRejectsMultipleOptions(t *testing.T) {
	if _, err := Open(t.TempDir(), Options{}, Options{}); err == nil {
		t.Fatal("expected multiple options to be rejected")
	}
}

func TestStoreMigratesVersionOneMetadata(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	metadata, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	var schemaVersion int
	if err := storage.db.QueryRow(`PRAGMA user_version`).Scan(&schemaVersion); err != nil || schemaVersion != SchemaVersion {
		t.Fatalf("unexpected migrated schema version: version=%d err=%v", schemaVersion, err)
	}
	var tableCount int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'pending_blob_deletions'`).Scan(&tableCount); err != nil || tableCount != 1 {
		t.Fatalf("pending deletion table missing: count=%d err=%v", tableCount, err)
	}
	var retentionColumnCount int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('backup_streams') WHERE name = 'retention_keep_latest'`).Scan(&retentionColumnCount); err != nil || retentionColumnCount != 1 {
		t.Fatalf("retention column missing: count=%d err=%v", retentionColumnCount, err)
	}
}

func TestStoreRecoversInterruptedVersionThreeMigration(t *testing.T) {
	dataDir := t.TempDir()
	metadata, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.Exec(`
		CREATE TABLE backup_streams (
			id TEXT PRIMARY KEY,
			database_name TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			retention_keep_latest INTEGER CHECK(retention_keep_latest BETWEEN 1 AND 1000)
		);
		PRAGMA user_version = 2;
	`); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	var schemaVersion, retentionColumnCount int
	if err := storage.db.QueryRow(`PRAGMA user_version`).Scan(&schemaVersion); err != nil || schemaVersion != SchemaVersion {
		t.Fatalf("unexpected recovered schema version: version=%d err=%v", schemaVersion, err)
	}
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('backup_streams') WHERE name = 'retention_keep_latest'`).Scan(&retentionColumnCount); err != nil || retentionColumnCount != 1 {
		t.Fatalf("retention column duplicated or missing: count=%d err=%v", retentionColumnCount, err)
	}
}

func TestAutomaticRetentionMakesRoomWithinStorageQuota(t *testing.T) {
	storage, err := Open(t.TempDir(), Options{MaxStorageBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx := context.Background()
	first, err := storage.CreateBackup(ctx, "project-a", "Project A", "install-a", bytes.NewReader([]byte("first")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.SetRetentionPolicy(ctx, "project-a", true, 1, false); err != nil {
		t.Fatal(err)
	}
	second, err := storage.CreateBackup(ctx, "project-a", "Project A", "install-a", bytes.NewReader([]byte("later")))
	if err != nil {
		t.Fatal(err)
	}
	if second.RetentionDeletedCount != 1 {
		t.Fatalf("expected automatic retention to delete one backup, got %#v", second)
	}
	page, err := storage.ListBackups(ctx, "project-a", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != second.ID || page.Items[0].ID == first.ID {
		t.Fatalf("unexpected retained backups: %#v", page.Items)
	}
	usage, err := storage.StorageUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.UsedBytes != 5 || usage.RemainingBytes == nil || *usage.RemainingBytes != 0 {
		t.Fatalf("unexpected quota usage: %#v", usage)
	}
}

func TestUnlimitedStorageAcceptsBackup(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if _, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte("backup"))); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionPolicyBoundsAndMissingStream(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx := context.Background()
	if _, err := storage.SetRetentionPolicy(ctx, "missing", true, 1, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing stream, got %v", err)
	}
	if _, err := storage.CreateBackup(ctx, "project-a", "Project A", "install-a", bytes.NewReader([]byte("first"))); err != nil {
		t.Fatal(err)
	}
	for _, keepLatest := range []int{0, 1001} {
		if _, err := storage.SetRetentionPolicy(ctx, "project-a", true, keepLatest, false); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected keep_latest=%d to be rejected, got %v", keepLatest, err)
		}
	}
	for _, keepLatest := range []int{1, 1000} {
		if _, err := storage.SetRetentionPolicy(ctx, "project-a", true, keepLatest, false); err != nil {
			t.Fatalf("expected keep_latest=%d to be accepted: %v", keepLatest, err)
		}
	}
	if _, err := storage.SetRetentionPolicy(ctx, "project-a", false, 0, false); err != nil {
		t.Fatalf("expected disabled policy to ignore keep_latest: %v", err)
	}
}
