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
