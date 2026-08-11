package store

import (
	"context"
	"fmt"
)

type StorageUsage struct {
	UsedBytes        int64  `json:"used_bytes"`
	QuotaEnabled     bool   `json:"quota_enabled"`
	QuotaBytes       int64  `json:"quota_bytes,omitempty"`
	RemainingBytes   *int64 `json:"remaining_bytes,omitempty"`
	BackupCount      int64  `json:"backup_count"`
	StreamCount      int64  `json:"stream_count"`
	PendingDeletions int64  `json:"pending_deletions"`
}

func (s *Store) StorageUsage(ctx context.Context) (StorageUsage, error) {
	var usage StorageUsage
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE((SELECT SUM(size_bytes) FROM backups), 0),
			(SELECT COUNT(*) FROM backups),
			(SELECT COUNT(*) FROM backup_streams),
			(SELECT COUNT(*) FROM pending_blob_deletions)
	`).Scan(&usage.UsedBytes, &usage.BackupCount, &usage.StreamCount, &usage.PendingDeletions)
	if err != nil {
		return StorageUsage{}, fmt.Errorf("read backup storage usage: %w", err)
	}
	if s.maxStorageBytes > 0 {
		remaining := s.maxStorageBytes - usage.UsedBytes
		if remaining < 0 {
			remaining = 0
		}
		usage.QuotaEnabled = true
		usage.QuotaBytes = s.maxStorageBytes
		usage.RemainingBytes = &remaining
	}
	return usage, nil
}

func (s *Store) remainingStorageBytes(ctx context.Context) (int64, error) {
	if s.maxStorageBytes == 0 {
		return 0, nil
	}
	usage, err := s.StorageUsage(ctx)
	if err != nil {
		return 0, err
	}
	return *usage.RemainingBytes, nil
}
