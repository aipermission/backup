package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPrefersTokenFile(t *testing.T) {
	t.Setenv("AIPERMISSION_BACKUP_TOKEN", "environment-token-that-must-not-be-used")
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("file-token-with-more-than-thirty-two-characters\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AIPERMISSION_BACKUP_TOKEN_FILE", path)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "file-token-with-more-than-thirty-two-characters" {
		t.Fatalf("unexpected token %q", cfg.Token)
	}
}

func TestLoadRejectsWeakToken(t *testing.T) {
	t.Setenv("AIPERMISSION_BACKUP_TOKEN_FILE", "")
	t.Setenv("AIPERMISSION_BACKUP_TOKEN", "too-short")
	if _, err := Load(); err == nil {
		t.Fatal("expected weak token to be rejected")
	}
}

func TestLoadStorageQuota(t *testing.T) {
	t.Setenv("AIPERMISSION_BACKUP_TOKEN_FILE", "")
	t.Setenv("AIPERMISSION_BACKUP_TOKEN", "test-token-with-at-least-thirty-two-characters")
	t.Setenv("AIPERMISSION_BACKUP_MAX_STORAGE_BYTES", "1048576")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxStorageBytes != 1048576 {
		t.Fatalf("max storage bytes = %d", cfg.MaxStorageBytes)
	}
}
