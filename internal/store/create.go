package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type backupUploadOperation struct {
	streamID             string
	databaseName         string
	sourceInstallationID string
}

type backupInput struct {
	streamID           string
	databaseName       string
	sourceInstallation string
	operationKey       string
	id                 string
	createdAt          time.Time
	createdText        string
	filename           string
	temporaryPath      string
	finalPath          string
	relativePath       string
	size               int64
	digest             string
}

func (s *Store) CreateBackup(ctx context.Context, streamID, databaseName, sourceInstallationID string, body io.Reader) (Backup, error) {
	backup, _, err := s.createBackup(ctx, streamID, databaseName, sourceInstallationID, "", body)
	return backup, err
}

func (s *Store) CreateBackupIdempotent(ctx context.Context, streamID, databaseName, sourceInstallationID, operationKey string, body io.Reader) (Backup, bool, error) {
	return s.createBackup(ctx, streamID, databaseName, sourceInstallationID, operationKey, body)
}

func (s *Store) createBackup(ctx context.Context, streamID, databaseName, sourceInstallationID, operationKey string, body io.Reader) (Backup, bool, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	databaseName, err := validateBackupInput(streamID, databaseName, sourceInstallationID, operationKey)
	if err != nil {
		return Backup{}, false, err
	}
	if operationKey != "" {
		existing, replayed, err := s.resolveUploadReplay(ctx, streamID, databaseName, sourceInstallationID, operationKey)
		if err != nil || replayed {
			return existing, false, err
		}
	}
	if err := s.cleanupPendingDeletions(ctx); err != nil {
		return Backup{}, false, err
	}
	uploadAllowance, err := s.backupUploadAllowance(ctx, streamID)
	if err != nil {
		return Backup{}, false, err
	}
	input, err := s.stageBackup(body, uploadAllowance)
	if err != nil {
		return Backup{}, false, err
	}
	defer os.Remove(input.temporaryPath)
	input.streamID = streamID
	input.databaseName = databaseName
	input.sourceInstallation = sourceInstallationID
	input.operationKey = operationKey
	if err := s.publishBackupBlob(&input); err != nil {
		return Backup{}, false, err
	}
	removeFinal := true
	defer func() {
		if removeFinal {
			_ = os.Remove(input.finalPath)
		}
	}()
	backup, err := s.commitBackupMetadata(ctx, input)
	if err != nil {
		return Backup{}, false, err
	}
	removeFinal = false
	_ = s.cleanupPendingDeletions(ctx)
	return backup, true, nil
}

func validateBackupInput(streamID, databaseName, sourceInstallationID, operationKey string) (string, error) {
	if !ValidateIdentifier(streamID) || !ValidateIdentifier(sourceInstallationID) {
		return "", fmt.Errorf("%w: stream or source installation identifier is invalid", ErrInvalidInput)
	}
	if operationKey != "" && !ValidateIdentifier(operationKey) {
		return "", fmt.Errorf("%w: operation key is invalid", ErrInvalidInput)
	}
	databaseName = strings.TrimSpace(databaseName)
	if databaseName == "" || len(databaseName) > 128 {
		return "", fmt.Errorf("%w: database name must contain 1 to 128 characters", ErrInvalidInput)
	}
	return databaseName, nil
}

func (s *Store) resolveUploadReplay(ctx context.Context, streamID, databaseName, sourceInstallationID, operationKey string) (Backup, bool, error) {
	existing, operation, err := s.backupByOperationKey(ctx, operationKey)
	switch {
	case err == nil:
		if operation.streamID != streamID || operation.databaseName != databaseName || operation.sourceInstallationID != sourceInstallationID {
			return Backup{}, false, ErrOperationConflict
		}
		if existing.ID == "" {
			return Backup{}, false, ErrOperationExpired
		}
		return existing, true, nil
	case !errors.Is(err, ErrNotFound):
		return Backup{}, false, err
	}
	if err := s.ensureUploadOperationCapacity(ctx); err != nil {
		return Backup{}, false, err
	}
	return Backup{}, false, nil
}

func (s *Store) backupUploadAllowance(ctx context.Context, streamID string) (int64, error) {
	remaining, err := s.remainingStorageBytes(ctx)
	if err != nil {
		return 0, err
	}
	allowance, err := s.uploadAllowance(ctx, streamID, remaining)
	if err != nil {
		return 0, err
	}
	if allowance == 0 {
		return 0, ErrQuotaExceeded
	}
	return allowance, nil
}

func (s *Store) stageBackup(body io.Reader, uploadAllowance int64) (result backupInput, returnErr error) {
	id, err := randomID("bkp")
	if err != nil {
		return backupInput{}, err
	}
	temporary, err := os.CreateTemp(s.tempDir, id+"-*.upload")
	if err != nil {
		return backupInput{}, fmt.Errorf("create temporary upload: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if returnErr != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	hash := sha256.New()
	copySource := body
	if uploadAllowance < math.MaxInt64 {
		copySource = io.LimitReader(body, uploadAllowance+1)
	}
	size, err := io.Copy(io.MultiWriter(temporary, hash), copySource)
	if err != nil {
		return backupInput{}, fmt.Errorf("store upload: %w", err)
	}
	if size == 0 {
		return backupInput{}, fmt.Errorf("%w: backup body is empty", ErrInvalidInput)
	}
	if size > uploadAllowance {
		return backupInput{}, ErrQuotaExceeded
	}
	if err := temporary.Sync(); err != nil {
		return backupInput{}, fmt.Errorf("flush temporary upload: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return backupInput{}, fmt.Errorf("close temporary upload: %w", err)
	}
	return backupInput{id: id, temporaryPath: temporaryPath, size: size, digest: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (s *Store) publishBackupBlob(input *backupInput) error {
	input.createdAt = s.now().UTC()
	input.createdText = formatMetadataTimestamp(input.createdAt)
	input.filename = safeFilename(input.databaseName, input.createdAt)
	streamDir := filepath.Join(s.blobDir, input.streamID)
	if err := os.MkdirAll(streamDir, 0o700); err != nil {
		return fmt.Errorf("create stream directory: %w", err)
	}
	if err := syncDirectoryDurably(s.blobDir); err != nil {
		return fmt.Errorf("sync stream directory creation: %w", err)
	}
	input.finalPath = filepath.Join(streamDir, input.id+".aipdb")
	relativePath, err := filepath.Rel(s.dataDir, input.finalPath)
	if err != nil {
		return fmt.Errorf("resolve backup path: %w", err)
	}
	input.relativePath = relativePath
	if err := moveFileDurably(input.temporaryPath, input.finalPath, false); err != nil {
		return fmt.Errorf("commit uploaded backup: %w", err)
	}
	return nil
}

func (s *Store) commitBackupMetadata(ctx context.Context, input backupInput) (Backup, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Backup{}, fmt.Errorf("begin metadata transaction: %w", err)
	}
	defer tx.Rollback()
	if err := upsertBackupStream(ctx, tx, input); err != nil {
		return Backup{}, err
	}
	if err := insertBackupMetadata(ctx, tx, input); err != nil {
		return Backup{}, err
	}
	retentionDeleted, err := s.applyConfiguredRetentionTx(ctx, tx, input.streamID, input.id)
	if err != nil {
		return Backup{}, err
	}
	if err := s.ensureStorageQuotaTx(ctx, tx); err != nil {
		return Backup{}, err
	}
	if err := tx.Commit(); err != nil {
		return Backup{}, fmt.Errorf("commit backup metadata: %w", err)
	}
	return Backup{
		ID: input.id, StreamID: input.streamID, DatabaseName: input.databaseName,
		SourceInstallationID: input.sourceInstallation, Filename: input.filename,
		SizeBytes: input.size, SHA256: input.digest, CreatedAt: input.createdText,
		RetentionDeletedCount: retentionDeleted,
	}, nil
}

func upsertBackupStream(ctx context.Context, tx *sql.Tx, input backupInput) error {
	var existingName string
	err := tx.QueryRowContext(ctx, `SELECT database_name FROM backup_streams WHERE id = ?`, input.streamID).Scan(&existingName)
	switch {
	case err == nil && existingName != input.databaseName:
		return ErrStreamConflict
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("read stream metadata: %w", err)
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, `INSERT INTO backup_streams(id, database_name, created_at, updated_at) VALUES(?, ?, ?, ?)`, input.streamID, input.databaseName, input.createdText, input.createdText); err != nil {
			return fmt.Errorf("create backup stream: %w", err)
		}
	default:
		if _, err := tx.ExecContext(ctx, `UPDATE backup_streams SET updated_at = ? WHERE id = ?`, input.createdText, input.streamID); err != nil {
			return fmt.Errorf("update backup stream: %w", err)
		}
	}
	return nil
}

func insertBackupMetadata(ctx context.Context, tx *sql.Tx, input backupInput) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO backups(id, stream_id, source_installation_id, filename, size_bytes, sha256, created_at, storage_path, operation_key)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''))`, input.id, input.streamID, input.sourceInstallation, input.filename, input.size, input.digest, input.createdText, input.relativePath, input.operationKey); err != nil {
		return fmt.Errorf("store backup metadata: %w", err)
	}
	if input.operationKey == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO backup_upload_operations (
			operation_key, stream_id, database_name, source_installation_id, backup_id, created_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		input.operationKey, input.streamID, input.databaseName, input.sourceInstallation, input.id, input.createdText,
	); err != nil {
		return fmt.Errorf("store backup upload operation: %w", err)
	}
	return nil
}

func (s *Store) ensureUploadOperationCapacity(ctx context.Context) error {
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backup_upload_operations`).Scan(&count); err != nil {
		return fmt.Errorf("read backup upload operation capacity: %w", err)
	}
	if count >= s.maxUploadOps {
		return ErrOperationCapacity
	}
	return nil
}

func (s *Store) backupByOperationKey(ctx context.Context, operationKey string) (Backup, backupUploadOperation, error) {
	var backup Backup
	var operation backupUploadOperation
	var backupID, backupStreamID, backupDatabaseName, backupSourceID, filename, sha256Value, createdAt, storagePath sql.NullString
	var sizeBytes sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
SELECT o.stream_id, o.database_name, o.source_installation_id,
       b.id, b.stream_id, s.database_name, b.source_installation_id, b.filename,
       b.size_bytes, b.sha256, b.created_at, b.storage_path
FROM backup_upload_operations o
LEFT JOIN backups b ON b.id = o.backup_id
LEFT JOIN backup_streams s ON s.id = b.stream_id
WHERE o.operation_key = ?`, operationKey).Scan(
		&operation.streamID, &operation.databaseName, &operation.sourceInstallationID,
		&backupID, &backupStreamID, &backupDatabaseName, &backupSourceID, &filename,
		&sizeBytes, &sha256Value, &createdAt, &storagePath,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Backup{}, backupUploadOperation{}, ErrNotFound
	}
	if err != nil {
		return Backup{}, backupUploadOperation{}, fmt.Errorf("read backup operation: %w", err)
	}
	if backupID.Valid {
		backup = Backup{
			ID: backupID.String, StreamID: backupStreamID.String, DatabaseName: backupDatabaseName.String,
			SourceInstallationID: backupSourceID.String, Filename: filename.String, SizeBytes: sizeBytes.Int64,
			SHA256: sha256Value.String, CreatedAt: createdAt.String, storagePath: storagePath.String,
		}
	}
	return backup, operation, nil
}

func randomID(prefix string) (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate backup identifier: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(raw), nil
}

func safeFilename(databaseName string, createdAt time.Time) string {
	var builder strings.Builder
	for _, char := range strings.ToLower(databaseName) {
		switch {
		case char >= 'a' && char <= 'z', char >= '0' && char <= '9':
			builder.WriteRune(char)
		case char == '-', char == '_':
			builder.WriteRune(char)
		default:
			if builder.Len() > 0 && !strings.HasSuffix(builder.String(), "-") {
				builder.WriteByte('-')
			}
		}
	}
	name := strings.Trim(builder.String(), "-")
	if name == "" {
		name = "aipermission"
	}
	return fmt.Sprintf("%s-%s.aipdb", name, createdAt.UTC().Format("20060102T150405Z"))
}
