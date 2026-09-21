//go:build !windows

package store

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
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

func TestMoveFileDurablyRollsBackTargetWhenPublishSyncFails(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(source, []byte("candidate"), 0o600); err != nil {
		t.Fatal(err)
	}
	publishErr := errors.New("publish sync failed")
	var syncCalls []string
	err := moveFileDurablyWith(
		source,
		target,
		false,
		os.Rename,
		os.Link,
		os.Remove,
		func(path string) error {
			syncCalls = append(syncCalls, path)
			if len(syncCalls) == 1 {
				return publishErr
			}
			return nil
		},
	)
	if !errors.Is(err, publishErr) {
		t.Fatalf("error = %v, want publish sync failure", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target remains after failed publish: %v", err)
	}
	if content, err := os.ReadFile(source); err != nil || string(content) != "candidate" {
		t.Fatalf("source = %q, err = %v", content, err)
	}
	wantCalls := []string{directory, directory}
	if !reflect.DeepEqual(syncCalls, wantCalls) {
		t.Fatalf("sync calls = %v, want %v", syncCalls, wantCalls)
	}
}

func TestMoveFileDurablyDoesNotRollBackPublishedTargetForSourceSyncFailure(t *testing.T) {
	root := t.TempDir()
	sourceDirectory := filepath.Join(root, "temporary")
	targetDirectory := filepath.Join(root, "blobs")
	if err := os.MkdirAll(sourceDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(targetDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(sourceDirectory, "source")
	target := filepath.Join(targetDirectory, "target")
	if err := os.WriteFile(source, []byte("candidate"), 0o600); err != nil {
		t.Fatal(err)
	}
	var syncCalls []string
	err := moveFileDurablyWith(
		source,
		target,
		false,
		os.Rename,
		os.Link,
		os.Remove,
		func(path string) error {
			syncCalls = append(syncCalls, path)
			if path == sourceDirectory {
				return errors.New("source cleanup sync failed")
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("published target reported failure: %v", err)
	}
	if content, err := os.ReadFile(target); err != nil || string(content) != "candidate" {
		t.Fatalf("target = %q, err = %v", content, err)
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source remains after cleanup: %v", err)
	}
	wantCalls := []string{targetDirectory, sourceDirectory}
	if !reflect.DeepEqual(syncCalls, wantCalls) {
		t.Fatalf("sync calls = %v, want %v", syncCalls, wantCalls)
	}
}
