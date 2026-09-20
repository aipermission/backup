//go:build windows

package store

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func moveFileDurably(source, target string, replace bool) error {
	sourcePointer, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	targetPointer, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	flags := uint32(windows.MOVEFILE_WRITE_THROUGH)
	if replace {
		flags |= windows.MOVEFILE_REPLACE_EXISTING
	}
	return windows.MoveFileEx(sourcePointer, targetPointer, flags)
}

func syncDirectoryDurably(path string) error {
	encoded, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return fmt.Errorf("encode storage directory for sync: %w", err)
	}
	handle, err := windows.CreateFile(
		encoded,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open storage directory for sync: %w", err)
	}
	if err := windows.FlushFileBuffers(handle); err != nil {
		_ = windows.CloseHandle(handle)
		return fmt.Errorf("sync storage directory: %w", err)
	}
	if err := windows.CloseHandle(handle); err != nil {
		return fmt.Errorf("close storage directory after sync: %w", err)
	}
	return nil
}
