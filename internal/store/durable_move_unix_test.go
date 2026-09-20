//go:build !windows

package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMoveFileDurablyWithoutReplacementPreservesExistingTarget(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(source, []byte("candidate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := moveFileDurably(source, target, false); err == nil {
		t.Fatal("no-replace move overwrote an existing target")
	}
	if content, err := os.ReadFile(target); err != nil || string(content) != "existing" {
		t.Fatalf("target = %q, err = %v", content, err)
	}
	if content, err := os.ReadFile(source); err != nil || string(content) != "candidate" {
		t.Fatalf("source = %q, err = %v", content, err)
	}
}
