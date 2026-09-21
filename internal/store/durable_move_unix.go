//go:build !windows

package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func moveFileDurably(source, target string, replace bool) error {
	return moveFileDurablyWith(
		source,
		target,
		replace,
		os.Rename,
		os.Link,
		os.Remove,
		syncDirectoryDurably,
	)
}

func moveFileDurablyWith(
	source, target string,
	replace bool,
	renameFile, linkFile func(string, string) error,
	removeFile func(string) error,
	syncDirectory func(string) error,
) error {
	if replace {
		if err := renameFile(source, target); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(target)); err != nil {
			return err
		}
		if sourceDirectory := filepath.Dir(source); sourceDirectory != filepath.Dir(target) {
			if err := syncDirectory(sourceDirectory); err != nil {
				return err
			}
		}
		return nil
	}

	if err := linkFile(source, target); err != nil {
		return err
	}
	targetDirectory := filepath.Dir(target)
	if err := syncDirectory(targetDirectory); err != nil {
		cleanupErr := removeFile(target)
		if cleanupErr == nil {
			cleanupErr = syncDirectory(targetDirectory)
		}
		if cleanupErr != nil {
			return errors.Join(err, fmt.Errorf("roll back unpublished target: %w", cleanupErr))
		}
		return err
	}

	// The target is committed once its directory entry is durable. Source cleanup
	// cannot safely turn that success into a failure because metadata rollback
	// would then leave the already-published target orphaned. The caller retries
	// removal, and startup cleanup handles a source entry resurrected by a crash.
	if err := removeFile(source); err == nil {
		if sourceDirectory := filepath.Dir(source); sourceDirectory != targetDirectory {
			_ = syncDirectory(sourceDirectory)
		}
	}
	return nil
}

func syncDirectoryDurably(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open storage directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync storage directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close storage directory after sync: %w", err)
	}
	return nil
}
