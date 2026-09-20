//go:build windows

package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncDirectoryDurablyWindows(t *testing.T) {
	if err := syncDirectoryDurably(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestSyncMigrationSnapshotWindows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.pre-migration-v4.db.pending")
	if err := os.WriteFile(path, []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := syncFile(path); err != nil {
		t.Fatal(err)
	}
}
