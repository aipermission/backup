package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const metadataTimestampLayout = "2006-01-02T15:04:05.000000000Z"
const metadataTimestampMigrationBatchSize = 256

type timestampMigrationSpec struct {
	name      string
	selectSQL string
	updateSQL string
	values    int
}

func formatMetadataTimestamp(value time.Time) string {
	return value.UTC().Format(metadataTimestampLayout)
}

func normalizeMetadataTimestamps(ctx context.Context, tx *sql.Tx) error {
	specs := []timestampMigrationSpec{
		{
			name:      "backup stream",
			selectSQL: `SELECT id, created_at, updated_at FROM backup_streams WHERE id > ? ORDER BY id LIMIT ?`,
			updateSQL: `UPDATE backup_streams SET created_at = ?, updated_at = ? WHERE id = ?`,
			values:    2,
		},
		{
			name:      "backup",
			selectSQL: `SELECT id, created_at FROM backups WHERE id > ? ORDER BY id LIMIT ?`,
			updateSQL: `UPDATE backups SET created_at = ? WHERE id = ?`,
			values:    1,
		},
		{
			name:      "pending blob deletion",
			selectSQL: `SELECT storage_path, queued_at FROM pending_blob_deletions WHERE storage_path > ? ORDER BY storage_path LIMIT ?`,
			updateSQL: `UPDATE pending_blob_deletions SET queued_at = ? WHERE storage_path = ?`,
			values:    1,
		},
	}
	for _, spec := range specs {
		if err := normalizeTimestampRows(ctx, tx, spec); err != nil {
			return err
		}
	}
	return nil
}

func normalizeTimestampRows(ctx context.Context, tx *sql.Tx, spec timestampMigrationSpec) error {
	type update struct {
		key    string
		values []string
	}
	lastKey := ""
	for {
		rows, err := tx.QueryContext(ctx, spec.selectSQL, lastKey, metadataTimestampMigrationBatchSize)
		if err != nil {
			return fmt.Errorf("read historical %s timestamps: %w", spec.name, err)
		}
		updates := make([]update, 0, metadataTimestampMigrationBatchSize)
		for rows.Next() {
			var key string
			values := make([]string, spec.values)
			destinations := make([]any, 0, spec.values+1)
			destinations = append(destinations, &key)
			for index := range values {
				destinations = append(destinations, &values[index])
			}
			if err := rows.Scan(destinations...); err != nil {
				rows.Close()
				return fmt.Errorf("scan historical %s timestamps: %w", spec.name, err)
			}
			for index, raw := range values {
				parsed, err := time.Parse(time.RFC3339Nano, raw)
				if err != nil {
					rows.Close()
					return fmt.Errorf("parse historical %s timestamp for %q: %w", spec.name, key, err)
				}
				values[index] = formatMetadataTimestamp(parsed)
			}
			updates = append(updates, update{key: key, values: values})
			lastKey = key
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate historical %s timestamps: %w", spec.name, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close historical %s timestamps: %w", spec.name, err)
		}
		for _, item := range updates {
			arguments := make([]any, 0, len(item.values)+1)
			for _, value := range item.values {
				arguments = append(arguments, value)
			}
			arguments = append(arguments, item.key)
			if _, err := tx.ExecContext(ctx, spec.updateSQL, arguments...); err != nil {
				return fmt.Errorf("normalize historical %s timestamps: %w", spec.name, err)
			}
		}
		if len(updates) < metadataTimestampMigrationBatchSize {
			return nil
		}
	}
}
