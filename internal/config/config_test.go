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
