package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type RetentionPolicy struct {
	StreamID   string `json:"stream_id"`
	Enabled    bool   `json:"enabled"`
	KeepLatest int    `json:"keep_latest,omitempty"`
}

type RetentionPreview struct {
	StreamID    string `json:"stream_id"`
	KeepLatest  int    `json:"keep_latest"`
	RetainCount int    `json:"retain_count"`
	RetainBytes int64  `json:"retain_bytes"`
	DeleteCount int    `json:"delete_count"`
	DeleteBytes int64  `json:"delete_bytes"`
}

type RetentionUpdateResult struct {
	Policy       RetentionPolicy  `json:"policy"`
	Preview      RetentionPreview `json:"preview"`
	DeletedCount int              `json:"deleted_count"`
}

func (s *Store) GetRetentionPolicy(ctx context.Context, streamID string) (RetentionPolicy, error) {
	if !ValidateIdentifier(streamID) {
		return RetentionPolicy{}, fmt.Errorf("%w: stream identifier is invalid", ErrInvalidInput)
	}
	var keepLatest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT retention_keep_latest FROM backup_streams WHERE id = ?`, streamID).Scan(&keepLatest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RetentionPolicy{}, ErrNotFound
		}
		return RetentionPolicy{}, fmt.Errorf("read backup retention policy: %w", err)
	}
	policy := RetentionPolicy{StreamID: streamID, Enabled: keepLatest.Valid}
	if keepLatest.Valid {
		policy.KeepLatest = int(keepLatest.Int64)
	}
	return policy, nil
}

func (s *Store) PreviewRetention(ctx context.Context, streamID string, keepLatest int) (RetentionPreview, error) {
	if !ValidateIdentifier(streamID) || keepLatest < 1 || keepLatest > 1000 {
		return RetentionPreview{}, fmt.Errorf("%w: stream identifier or keep_latest is invalid", ErrInvalidInput)
	}
	if err := requireStream(ctx, s.db, streamID); err != nil {
		return RetentionPreview{}, err
	}
	return retentionPreview(ctx, s.db, streamID, keepLatest)
}

func (s *Store) SetRetentionPolicy(ctx context.Context, streamID string, enabled bool, keepLatest int, applyNow bool) (RetentionUpdateResult, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if !ValidateIdentifier(streamID) || (enabled && (keepLatest < 1 || keepLatest > 1000)) {
		return RetentionUpdateResult{}, fmt.Errorf("%w: stream identifier or keep_latest is invalid", ErrInvalidInput)
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return RetentionUpdateResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RetentionUpdateResult{}, fmt.Errorf("begin retention policy transaction: %w", err)
	}
	defer tx.Rollback()
	if err := requireStream(ctx, tx, streamID); err != nil {
		return RetentionUpdateResult{}, err
	}
	previewKeep := keepLatest
	if !enabled {
		previewKeep = 1
	}
	preview, err := retentionPreview(ctx, tx, streamID, previewKeep)
	if err != nil {
		return RetentionUpdateResult{}, err
	}
	if !enabled {
		preview.KeepLatest = 0
		preview.RetainCount += preview.DeleteCount
		preview.RetainBytes += preview.DeleteBytes
		preview.DeleteCount = 0
		preview.DeleteBytes = 0
	}
	var retentionValue any
	if enabled {
		retentionValue = keepLatest
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backup_streams SET retention_keep_latest = ? WHERE id = ?`, retentionValue, streamID); err != nil {
		return RetentionUpdateResult{}, fmt.Errorf("update backup retention policy: %w", err)
	}
	deletedCount := 0
	if enabled && applyNow && preview.DeleteCount > 0 {
		candidates, err := retentionCandidates(ctx, tx, streamID, keepLatest)
		if err != nil {
			return RetentionUpdateResult{}, err
		}
		if err := queueDeletionCandidates(ctx, tx, streamID, candidates, s.now().UTC()); err != nil {
			return RetentionUpdateResult{}, err
		}
		deletedCount = len(candidates)
	}
	if err := tx.Commit(); err != nil {
		return RetentionUpdateResult{}, fmt.Errorf("commit backup retention policy: %w", err)
	}
	_ = s.cleanupPendingDeletions(ctx)
	policy := RetentionPolicy{StreamID: streamID, Enabled: enabled}
	if enabled {
		policy.KeepLatest = keepLatest
	}
	return RetentionUpdateResult{Policy: policy, Preview: preview, DeletedCount: deletedCount}, nil
}

func (s *Store) applyConfiguredRetentionTx(ctx context.Context, tx *sql.Tx, streamID, protectedBackupID string) (int, error) {
	var keepLatest sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT retention_keep_latest FROM backup_streams WHERE id = ?`, streamID).Scan(&keepLatest); err != nil {
		return 0, fmt.Errorf("read configured backup retention: %w", err)
	}
	if !keepLatest.Valid {
		return 0, nil
	}
	candidates, err := retentionCandidatesExcluding(ctx, tx, streamID, max(int(keepLatest.Int64)-1, 0), protectedBackupID)
	if err != nil || len(candidates) == 0 {
		return 0, err
	}
	if err := queueDeletionCandidates(ctx, tx, streamID, candidates, s.now().UTC()); err != nil {
		return 0, err
	}
	return len(candidates), nil
}

type retentionQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func requireStream(ctx context.Context, query retentionQuerier, streamID string) error {
	var exists int
	if err := query.QueryRowContext(ctx, `SELECT 1 FROM backup_streams WHERE id = ?`, streamID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read backup stream: %w", err)
	}
	return nil
}

func retentionPreview(ctx context.Context, query retentionQuerier, streamID string, keepLatest int) (RetentionPreview, error) {
	var preview RetentionPreview
	preview.StreamID = streamID
	preview.KeepLatest = keepLatest
	err := query.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN position <= ? THEN size_bytes ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN position > ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN position > ? THEN size_bytes ELSE 0 END), 0)
		FROM (
			SELECT size_bytes, ROW_NUMBER() OVER (ORDER BY created_at DESC, id DESC) AS position
			FROM backups WHERE stream_id = ?
		)
	`, keepLatest, keepLatest, keepLatest, streamID).Scan(
		&preview.RetainCount, &preview.RetainBytes, &preview.DeleteCount, &preview.DeleteBytes,
	)
	if err != nil {
		return RetentionPreview{}, fmt.Errorf("preview backup retention: %w", err)
	}
	if preview.RetainCount > keepLatest {
		preview.RetainCount = keepLatest
	}
	return preview, nil
}

func retentionCandidates(ctx context.Context, query retentionQuerier, streamID string, keepLatest int) ([]deletionCandidate, error) {
	return retentionCandidatesExcluding(ctx, query, streamID, keepLatest, "")
}

func retentionCandidatesExcluding(
	ctx context.Context,
	query retentionQuerier,
	streamID string,
	keepLatest int,
	protectedBackupID string,
) ([]deletionCandidate, error) {
	// created_at is the user-visible ordering key; random IDs provide a stable tie-break
	// when two backups share the same timestamp.
	queryText := `
		SELECT id, storage_path, size_bytes
		FROM backups
		WHERE stream_id = ?`
	args := []any{streamID}
	if protectedBackupID != "" {
		queryText += ` AND id <> ?`
		args = append(args, protectedBackupID)
	}
	queryText += ` ORDER BY created_at DESC, id DESC LIMIT -1 OFFSET ?`
	args = append(args, keepLatest)
	rows, err := query.QueryContext(ctx, queryText, args...)
	if err != nil {
		return nil, fmt.Errorf("list backups for retention: %w", err)
	}
	defer rows.Close()
	var candidates []deletionCandidate
	for rows.Next() {
		var item deletionCandidate
		if err := rows.Scan(&item.id, &item.path, &item.size); err != nil {
			return nil, fmt.Errorf("scan backup for retention: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate backups for retention: %w", err)
	}
	return candidates, nil
}

func queueDeletionCandidates(ctx context.Context, tx *sql.Tx, streamID string, candidates []deletionCandidate, queuedAt time.Time) error {
	queuedText := queuedAt.Format(time.RFC3339Nano)
	for _, item := range candidates {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO pending_blob_deletions(storage_path, queued_at)
			VALUES(?, ?)
			ON CONFLICT(storage_path) DO NOTHING`, item.path, queuedText); err != nil {
			return fmt.Errorf("queue backup blob deletion: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM backups WHERE id = ? AND stream_id = ?`, item.id, streamID); err != nil {
			return fmt.Errorf("delete backup metadata: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE backup_streams
		SET updated_at = (SELECT created_at FROM backups WHERE stream_id = ? ORDER BY created_at DESC, id DESC LIMIT 1)
		WHERE id = ?`, streamID, streamID); err != nil {
		return fmt.Errorf("update backup stream after deletion: %w", err)
	}
	return nil
}
