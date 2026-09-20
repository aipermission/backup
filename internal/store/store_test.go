package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

func TestIdempotentUploadReturnsCommittedBackupWithoutCreatingAnotherVersion(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })

	first, created, err := storage.CreateBackupIdempotent(context.Background(), "project-a", "Project A", "install-a", "operation-a", bytes.NewReader([]byte("first")))
	if err != nil || !created {
		t.Fatalf("first upload: created=%v err=%v", created, err)
	}
	replayed, created, err := storage.CreateBackupIdempotent(context.Background(), "project-a", "Project A", "install-a", "operation-a", bytes.NewReader([]byte("different-body-is-not-consumed")))
	if err != nil || created || replayed.ID != first.ID {
		t.Fatalf("replayed upload: backup=%#v created=%v err=%v", replayed, created, err)
	}
	page, err := storage.ListBackups(context.Background(), "project-a", 10, "")
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("idempotent upload created duplicate versions: items=%#v err=%v", page.Items, err)
	}
	if _, _, err := storage.CreateBackupIdempotent(context.Background(), "project-a", "Project A", "install-b", "operation-a", bytes.NewReader([]byte("second"))); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("expected operation metadata conflict, got %v", err)
	}
}

func TestBackupCreationTimestampReflectsDurableUploadCompletion(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	started := time.Date(2026, time.September, 20, 10, 0, 0, 0, time.UTC)
	finished := started.Add(5 * time.Minute)
	current := started
	storage.now = func() time.Time { return current }
	reader := &clockAdvancingReader{
		reader:  bytes.NewReader([]byte("encrypted backup")),
		Advance: func() { current = finished },
	}
	backup, err := storage.CreateBackup(t.Context(), "project-a", "Project A", "install-a", reader)
	if err != nil {
		t.Fatal(err)
	}
	if backup.CreatedAt != formatMetadataTimestamp(finished) {
		t.Fatalf("created_at=%q, want durable completion %q", backup.CreatedAt, formatMetadataTimestamp(finished))
	}
}

type clockAdvancingReader struct {
	reader  *bytes.Reader
	Advance func()
	didRun  bool
}

func (r *clockAdvancingReader) Read(destination []byte) (int, error) {
	if !r.didRun {
		r.didRun = true
		r.Advance()
	}
	return r.reader.Read(destination)
}

func TestIdempotentUploadTombstoneSurvivesRetentionAndRestart(t *testing.T) {
	dataDir := t.TempDir()
	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "operation-first", bytes.NewReader([]byte("first")))
	if err != nil || !created {
		t.Fatalf("first upload: created=%v err=%v", created, err)
	}
	if _, err := storage.SetRetentionPolicy(ctx, "project-a", true, 1, false); err != nil {
		t.Fatal(err)
	}
	if _, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "operation-second", bytes.NewReader([]byte("second"))); err != nil || !created {
		t.Fatalf("second upload: created=%v err=%v", created, err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	storage, err = Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	body := bytes.NewReader([]byte("must-not-be-consumed"))
	if _, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "operation-first", body); !errors.Is(err, ErrOperationExpired) || created {
		t.Fatalf("expired replay: created=%v err=%v", created, err)
	}
	if body.Len() != len("must-not-be-consumed") {
		t.Fatalf("expired replay consumed %d body bytes", len("must-not-be-consumed")-body.Len())
	}
	if _, _, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "different-install", "operation-first", bytes.NewReader([]byte("third"))); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("expired operation metadata drift = %v, want conflict", err)
	}
	page, err := storage.ListBackups(ctx, "project-a", 10, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].ID == first.ID {
		t.Fatalf("backups after expired replay = %#v, err=%v", page.Items, err)
	}
	var operations int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM backup_upload_operations`).Scan(&operations); err != nil || operations != 2 {
		t.Fatalf("operation tombstones = %d, err=%v", operations, err)
	}
}

func TestExpiredUploadTombstonePermanentlyPreventsOperationKeyReuse(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx := context.Background()
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	storage.now = func() time.Time { return now }
	first, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "expired-operation", bytes.NewReader([]byte("first")))
	if err != nil || !created {
		t.Fatalf("create first backup: created=%v err=%v", created, err)
	}
	now = now.Add(time.Second)
	live, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "live-operation", bytes.NewReader([]byte("second")))
	if err != nil || !created {
		t.Fatalf("create live backup: created=%v err=%v", created, err)
	}
	if _, err := storage.DeleteBackups(ctx, "project-a", []string{first.ID}); err != nil {
		t.Fatal(err)
	}
	now = now.AddDate(10, 0, 0)
	body := bytes.NewReader([]byte("replacement"))
	if _, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "expired-operation", body); !errors.Is(err, ErrOperationExpired) || created {
		t.Fatalf("reuse expired operation: created=%v err=%v", created, err)
	}
	if body.Len() != len("replacement") {
		t.Fatalf("expired operation consumed %d body bytes", len("replacement")-body.Len())
	}
	replayed, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "live-operation", bytes.NewReader([]byte("ignored")))
	if err != nil || created || replayed.ID != live.ID {
		t.Fatalf("live operation replay: backup=%#v created=%v err=%v", replayed, created, err)
	}
	var operations int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM backup_upload_operations`).Scan(&operations); err != nil || operations != 2 {
		t.Fatalf("retained operation tombstones = %d, err=%v", operations, err)
	}
}

func TestOpenRetainsOldUploadTombstones(t *testing.T) {
	dataDir := t.TempDir()
	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "old-operation", bytes.NewReader([]byte("first")))
	if err != nil || !created {
		t.Fatalf("create first backup: created=%v err=%v", created, err)
	}
	if _, err := storage.CreateBackup(ctx, "project-a", "Project A", "install-a", bytes.NewReader([]byte("second"))); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.DeleteBackups(ctx, "project-a", []string{first.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.db.Exec(`UPDATE backup_upload_operations SET created_at = '2000-01-01T00:00:00.000000000Z' WHERE operation_key = 'old-operation'`); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	storage, err = Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	var count int
	if err := storage.db.QueryRow(`SELECT COUNT(*) FROM backup_upload_operations WHERE operation_key = 'old-operation'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retained operation count=%d err=%v", count, err)
	}
	if _, created, err := storage.CreateBackupIdempotent(ctx, "project-a", "Project A", "install-a", "old-operation", bytes.NewReader([]byte("replacement"))); !errors.Is(err, ErrOperationExpired) || created {
		t.Fatalf("old operation replay: created=%v err=%v", created, err)
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

func TestStoreMigratesPopulatedHistoricalMetadata(t *testing.T) {
	for _, schemaVersion := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("version-%d", schemaVersion), func(t *testing.T) {
			dataDir := t.TempDir()
			metadata, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			retentionColumn := ""
			if schemaVersion >= 2 {
				retentionColumn = ", retention_keep_latest INTEGER CHECK(retention_keep_latest BETWEEN 1 AND 1000)"
			}
			fixture := fmt.Sprintf(`
				CREATE TABLE backup_streams (
					id TEXT PRIMARY KEY,
					database_name TEXT NOT NULL,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL%s
				);
				CREATE TABLE backups (
					id TEXT PRIMARY KEY,
					stream_id TEXT NOT NULL REFERENCES backup_streams(id),
					source_installation_id TEXT NOT NULL,
					filename TEXT NOT NULL,
					size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
					sha256 TEXT NOT NULL,
					created_at TEXT NOT NULL,
					storage_path TEXT NOT NULL UNIQUE
				);
				INSERT INTO backup_streams(id, database_name, created_at, updated_at)
				VALUES ('stream-a', 'Database A', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
				INSERT INTO backups(id, stream_id, source_installation_id, filename, size_bytes, sha256, created_at, storage_path)
				VALUES ('backup-a', 'stream-a', 'install-a', 'backup.aipdb', 7, 'sha', '2026-01-01T00:00:00Z', 'stream-a/backup.aipdb');
				PRAGMA user_version = %d;
			`, retentionColumn, schemaVersion)
			if _, err := metadata.Exec(fixture); err != nil {
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

			var migratedVersion, operationKeyColumnCount, operationKeyIndexCount, rowCount int
			if err := storage.db.QueryRow(`PRAGMA user_version`).Scan(&migratedVersion); err != nil || migratedVersion != SchemaVersion {
				t.Fatalf("unexpected schema version: version=%d err=%v", migratedVersion, err)
			}
			if err := storage.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('backups') WHERE name = 'operation_key'`).Scan(&operationKeyColumnCount); err != nil || operationKeyColumnCount != 1 {
				t.Fatalf("operation key column missing: count=%d err=%v", operationKeyColumnCount, err)
			}
			if err := storage.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_backups_operation_key'`).Scan(&operationKeyIndexCount); err != nil || operationKeyIndexCount != 1 {
				t.Fatalf("operation key index missing: count=%d err=%v", operationKeyIndexCount, err)
			}
			if err := storage.db.QueryRow(`SELECT COUNT(*) FROM backups WHERE id = 'backup-a' AND operation_key IS NULL`).Scan(&rowCount); err != nil || rowCount != 1 {
				t.Fatalf("historical backup row not preserved: count=%d err=%v", rowCount, err)
			}
		})
	}
}

func TestStoreNormalizesHistoricalTimestampsBeforeOrdering(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.Chmod(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
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
		CREATE TABLE backups (
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
		CREATE TABLE pending_blob_deletions (
			storage_path TEXT PRIMARY KEY,
			queued_at TEXT NOT NULL
		);
		INSERT INTO backup_streams(id, database_name, created_at, updated_at)
		VALUES ('stream-a', 'Database A', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00.11Z');
		INSERT INTO backups(id, stream_id, source_installation_id, filename, size_bytes, sha256, created_at, storage_path, operation_key)
		VALUES
			('older-z', 'stream-a', 'install-a', 'older.aipdb', 1, 'sha', '2026-01-01T00:00:00.1Z', 'blobs/stream-a/older-z.aipdb', 'historical-operation'),
			('newer-a', 'stream-a', 'install-a', 'newer.aipdb', 1, 'sha', '2026-01-01T00:00:00.11Z', 'blobs/stream-a/newer-a.aipdb', NULL);
		PRAGMA user_version = 4;
	`); err != nil {
		metadata.Close()
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
	if info, err := os.Stat(dataDir); err != nil || !info.IsDir() || (runtime.GOOS != "windows" && info.Mode().Perm() != 0o700) {
		t.Fatalf("storage directory permissions: info=%v err=%v", info, err)
	}
	snapshotPath := filepath.Join(dataDir, "metadata.pre-migration-v4.db")
	if info, err := os.Stat(snapshotPath); err != nil || !info.Mode().IsRegular() || (runtime.GOOS != "windows" && info.Mode().Perm() != 0o600) {
		t.Fatalf("migration snapshot: info=%v err=%v", info, err)
	}
	snapshot, err := sql.Open("sqlite", snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var snapshotVersion int
	if err := snapshot.QueryRow(`PRAGMA user_version`).Scan(&snapshotVersion); err != nil || snapshotVersion != 4 {
		t.Fatalf("migration snapshot version=%d err=%v", snapshotVersion, err)
	}
	var snapshotTimestamp string
	if err := snapshot.QueryRow(`SELECT created_at FROM backups WHERE id = 'older-z'`).Scan(&snapshotTimestamp); err != nil || snapshotTimestamp != "2026-01-01T00:00:00.1Z" {
		t.Fatalf("migration snapshot timestamp=%q err=%v", snapshotTimestamp, err)
	}

	page, err := storage.ListBackups(context.Background(), "stream-a", 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "newer-a" || page.NextCursor == "" {
		t.Fatalf("first page = %#v, want newer backup and cursor", page)
	}
	second, err := storage.ListBackups(context.Background(), "stream-a", 1, page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Items[0].ID != "older-z" {
		t.Fatalf("second page = %#v, want older backup", second)
	}
	if page.Items[0].CreatedAt != "2026-01-01T00:00:00.110000000Z" || second.Items[0].CreatedAt != "2026-01-01T00:00:00.100000000Z" {
		t.Fatalf("timestamps were not canonicalized: first=%q second=%q", page.Items[0].CreatedAt, second.Items[0].CreatedAt)
	}
	candidates, err := retentionCandidates(context.Background(), storage.db, "stream-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].id != "older-z" {
		t.Fatalf("retention candidates = %#v, want older-z", candidates)
	}
	var operationStream, operationDatabase, operationSource, operationBackup string
	if err := storage.db.QueryRow(`
		SELECT stream_id, database_name, source_installation_id, backup_id
		FROM backup_upload_operations WHERE operation_key = 'historical-operation'`,
	).Scan(&operationStream, &operationDatabase, &operationSource, &operationBackup); err != nil {
		t.Fatal(err)
	}
	if operationStream != "stream-a" || operationDatabase != "Database A" || operationSource != "install-a" || operationBackup != "older-z" {
		t.Fatalf("migrated operation = %q %q %q %q", operationStream, operationDatabase, operationSource, operationBackup)
	}
}

func TestStoreRejectsUnparseableHistoricalTimestamp(t *testing.T) {
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
			updated_at TEXT NOT NULL
		);
		INSERT INTO backup_streams(id, database_name, created_at, updated_at)
		VALUES ('stream-a', 'Database A', 'not-a-timestamp', '2026-01-01T00:00:00Z');
		PRAGMA user_version = 4;
	`); err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	if storage, err := Open(dataDir); err == nil {
		storage.Close()
		t.Fatal("expected malformed historical timestamp to reject migration")
	}
}

func TestFailedMigrationRollsBackSchemaChanges(t *testing.T) {
	dataDir := t.TempDir()
	metadataPath := filepath.Join(dataDir, "metadata.db")
	metadata, err := sql.Open("sqlite", metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.Exec(`
		CREATE TABLE backup_streams (
			id TEXT PRIMARY KEY,
			database_name TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		INSERT INTO backup_streams(id, database_name, created_at, updated_at)
		VALUES ('stream-a', 'Database A', 'not-a-timestamp', '2026-01-01T00:00:00Z');
		PRAGMA user_version = 4;
	`); err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	if storage, err := Open(dataDir); err == nil {
		storage.Close()
		t.Fatal("expected malformed migration to fail")
	}

	metadata, err = sql.Open("sqlite", metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	var schemaVersion, uploadTables, retentionColumns int
	if err := metadata.QueryRow(`PRAGMA user_version`).Scan(&schemaVersion); err != nil {
		t.Fatal(err)
	}
	if err := metadata.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'backup_upload_operations'`).Scan(&uploadTables); err != nil {
		t.Fatal(err)
	}
	if err := metadata.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('backup_streams') WHERE name = 'retention_keep_latest'`).Scan(&retentionColumns); err != nil {
		t.Fatal(err)
	}
	if schemaVersion != 4 || uploadTables != 0 || retentionColumns != 0 {
		t.Fatalf("failed migration persisted schema: version=%d upload_tables=%d retention_columns=%d", schemaVersion, uploadTables, retentionColumns)
	}
}

func TestPreMigrationSnapshotRefreshesAStaleRollbackCopy(t *testing.T) {
	dataDir := t.TempDir()
	metadata, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	if _, err := metadata.Exec(`CREATE TABLE marker (value TEXT NOT NULL); INSERT INTO marker VALUES ('first'); PRAGMA user_version = 4;`); err != nil {
		t.Fatal(err)
	}
	storage := &Store{db: metadata, dataDir: dataDir}
	if err := storage.createPreMigrationSnapshot(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.Exec(`UPDATE marker SET value = 'second'`); err != nil {
		t.Fatal(err)
	}
	if err := storage.createPreMigrationSnapshot(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	snapshot, err := sql.Open("sqlite", filepath.Join(dataDir, "metadata.pre-migration-v4.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var value string
	if err := snapshot.QueryRow(`SELECT value FROM marker`).Scan(&value); err != nil || value != "second" {
		t.Fatalf("refreshed snapshot marker=%q err=%v", value, err)
	}
}

func TestValidateMigrationSnapshotRejectsCorruptionAndWrongVersion(t *testing.T) {
	corruptPath := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(corruptPath, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateMigrationSnapshot(t.Context(), corruptPath, 4); err == nil {
		t.Fatal("corrupt migration snapshot passed validation")
	}
	wrongVersionPath := filepath.Join(t.TempDir(), "wrong-version.db")
	database, err := sql.Open("sqlite", wrongVersionPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE marker (value TEXT); PRAGMA user_version = 3;`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := validateMigrationSnapshot(t.Context(), wrongVersionPath, 4); err == nil {
		t.Fatal("wrong-version migration snapshot passed validation")
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

func TestAutomaticRetentionAlwaysProtectsTheIncomingBackup(t *testing.T) {
	storage, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx := context.Background()
	times := []time.Time{
		time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC),
		time.Date(2026, time.August, 2, 12, 0, 0, 0, time.UTC),
		time.Date(2026, time.August, 2, 12, 0, 1, 0, time.UTC),
		time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2026, time.August, 2, 12, 0, 2, 0, time.UTC),
	}
	storage.now = func() time.Time {
		value := times[0]
		times = times[1:]
		return value
	}
	first, err := storage.CreateBackup(ctx, "project-a", "Project A", "install-a", bytes.NewReader([]byte("first")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.SetRetentionPolicy(ctx, "project-a", true, 1, false); err != nil {
		t.Fatal(err)
	}
	second, err := storage.CreateBackup(ctx, "project-a", "Project A", "install-a", bytes.NewReader([]byte("second")))
	if err != nil {
		t.Fatal(err)
	}
	page, err := storage.ListBackups(ctx, "project-a", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != second.ID || page.Items[0].ID == first.ID {
		t.Fatalf("incoming backup was not protected from retention: %#v", page.Items)
	}
}

func TestStoreRemovesOrphanedBlobsOnStartup(t *testing.T) {
	dataDir := t.TempDir()
	storage, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.CreateBackup(context.Background(), "project-a", "Project A", "install-a", bytes.NewReader([]byte("valid"))); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(dataDir, "blobs", "project-a", "orphan.aipdb")
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	storage, err = Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if _, err := os.Stat(orphanPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphaned blob was not removed: %v", err)
	}
	page, err := storage.ListBackups(context.Background(), "project-a", 10, "")
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("referenced backup was not preserved: items=%#v err=%v", page.Items, err)
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
