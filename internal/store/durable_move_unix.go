//go:build !windows

package store

import (
	"fmt"
	"os"
	"path/filepath"
)

func moveFileDurably(source, target string, replace bool) error {
	if replace {
		if err := os.Rename(source, target); err != nil {
			return err
		}
	} else {
		if err := os.Link(source, target); err != nil {
			return err
		}
		if err := syncDirectoryDurably(filepath.Dir(target)); err != nil {
			_ = os.Remove(target)
			return err
		}
		if err := os.Remove(source); err != nil {
			if os.Remove(target) == nil {
				_ = syncDirectoryDurably(filepath.Dir(target))
			}
			return err
		}
	}
	if err := syncDirectoryDurably(filepath.Dir(target)); err != nil {
		return err
	}
	if sourceDirectory := filepath.Dir(source); sourceDirectory != filepath.Dir(target) {
		if err := syncDirectoryDurably(sourceDirectory); err != nil {
			return err
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
