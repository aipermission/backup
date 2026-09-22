package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func (s *Store) PruneBackups(ctx context.Context, streamID string, keepLatest int) (PruneResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if !ValidateIdentifier(streamID) || keepLatest < 1 || keepLatest > 1000 {
		return PruneResult{}, fmt.Errorf("%w: stream identifier or keep_latest is invalid", ErrInvalidInput)
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return PruneResult{}, err
	}
	var streamExists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM backup_streams WHERE id = ?`, streamID).Scan(&streamExists); errors.Is(err, sql.ErrNoRows) {
		return PruneResult{}, ErrNotFound
	} else if err != nil {
		return PruneResult{}, fmt.Errorf("read backup stream: %w", err)
	}
	candidates, err := retentionCandidates(ctx, s.db, streamID, keepLatest)
	if err != nil {
		return PruneResult{}, err
	}
	if len(candidates) == 0 {
		return PruneResult{StreamID: streamID, KeepLatest: keepLatest}, nil
	}
	if err := s.deleteCandidates(ctx, streamID, candidates); err != nil {
		return PruneResult{}, err
	}
	return PruneResult{StreamID: streamID, KeepLatest: keepLatest, DeletedCount: len(candidates)}, nil
}

func (s *Store) DeleteBackups(ctx context.Context, streamID string, backupIDs []string) (DeleteResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := validateBackupDeletion(streamID, backupIDs); err != nil {
		return DeleteResult{}, err
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return DeleteResult{}, err
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE stream_id = ?`, streamID).Scan(&total); err != nil {
		return DeleteResult{}, fmt.Errorf("count stream backups: %w", err)
	}
	if total == 0 {
		return DeleteResult{}, ErrNotFound
	}
	candidates, err := s.loadDeletionCandidates(ctx, streamID, backupIDs)
	if err != nil {
		return DeleteResult{}, err
	}
	if len(candidates) != len(backupIDs) {
		return DeleteResult{}, ErrNotFound
	}
	if total <= len(candidates) {
		return DeleteResult{}, ErrLastBackup
	}
	if err := s.deleteCandidates(ctx, streamID, candidates); err != nil {
		return DeleteResult{}, err
	}
	deletedIDs := make([]string, 0, len(candidates))
	for _, item := range candidates {
		deletedIDs = append(deletedIDs, item.id)
	}
	return DeleteResult{StreamID: streamID, DeletedIDs: deletedIDs, DeletedCount: len(deletedIDs)}, nil
}

func validateBackupDeletion(streamID string, backupIDs []string) error {
	if !ValidateIdentifier(streamID) || len(backupIDs) < 1 || len(backupIDs) > 100 {
		return fmt.Errorf("%w: stream identifier or backup ids are invalid", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(backupIDs))
	for _, id := range backupIDs {
		if !ValidateIdentifier(id) {
			return fmt.Errorf("%w: backup id is invalid", ErrInvalidInput)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: backup ids must be unique", ErrInvalidInput)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (s *Store) loadDeletionCandidates(ctx context.Context, streamID string, backupIDs []string) ([]deletionCandidate, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(backupIDs)), ",")
	args := make([]any, 0, len(backupIDs)+1)
	args = append(args, streamID)
	for _, id := range backupIDs {
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, storage_path
FROM backups
WHERE stream_id = ? AND id IN (`+placeholders+`)
ORDER BY created_at DESC, id DESC`, args...)
	if err != nil {
		return nil, fmt.Errorf("list backups to delete: %w", err)
	}
	var candidates []deletionCandidate
	for rows.Next() {
		var item deletionCandidate
		if err := rows.Scan(&item.id, &item.path); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan backup to delete: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate backups to delete: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close backups to delete: %w", err)
	}
	return candidates, nil
}

func (s *Store) deleteCandidates(ctx context.Context, streamID string, candidates []deletionCandidate) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin backup deletion transaction: %w", err)
	}
	defer tx.Rollback()
	if err := queueDeletionCandidates(ctx, tx, streamID, candidates, s.now().UTC()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit backup deletion: %w", err)
	}
	// Metadata is authoritative after commit; a later mutation retries pending blob cleanup.
	_ = s.cleanupPendingDeletions(ctx)
	return nil
}
