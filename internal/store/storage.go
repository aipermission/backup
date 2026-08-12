package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
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
		return math.MaxInt64, nil
	}
	usage, err := s.StorageUsage(ctx)
	if err != nil {
		return 0, err
	}
	return *usage.RemainingBytes, nil
}

func (s *Store) uploadAllowance(ctx context.Context, streamID string, remaining int64) (int64, error) {
	if remaining == math.MaxInt64 {
		return remaining, nil
	}
	var keepLatest sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT retention_keep_latest FROM backup_streams WHERE id = ?`, streamID).Scan(&keepLatest)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !keepLatest.Valid) {
		return remaining, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read upload retention policy: %w", err)
	}

	// The incoming backup becomes the newest version, so only keep_latest-1 existing
	// versions remain protected when configured retention runs after the insert.
	candidates, err := retentionCandidates(ctx, s.db, streamID, max(int(keepLatest.Int64)-1, 0))
	if err != nil {
		return 0, err
	}
	var releasable int64
	for _, candidate := range candidates {
		if candidate.size > math.MaxInt64-releasable {
			releasable = math.MaxInt64
			break
		}
		releasable += candidate.size
	}
	if releasable > math.MaxInt64-remaining {
		return math.MaxInt64, nil
	}
	return remaining + releasable, nil
}
