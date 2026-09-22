package store

import (
	"database/sql"
	"errors"
	"regexp"
	"sync"
	"time"
)

var (
	ErrInvalidInput      = errors.New("invalid input")
	ErrNotFound          = errors.New("backup not found")
	ErrLastBackup        = errors.New("the last backup in a stream cannot be deleted")
	ErrStreamConflict    = errors.New("stream metadata conflicts with the existing stream")
	ErrCorrupt           = errors.New("stored backup checksum does not match metadata")
	ErrQuotaExceeded     = errors.New("backup storage quota exceeded")
	ErrOperationConflict = errors.New("backup upload operation conflicts with existing metadata")
	ErrOperationExpired  = errors.New("backup upload operation is no longer replayable")
	ErrOperationCapacity = errors.New("backup upload operation ledger is full")
	identifierPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

const (
	SchemaVersion              = 6
	DefaultMaxUploadOperations = int64(1_000_000)
)

type Options struct {
	MaxStorageBytes     int64
	MaxUploadOperations int64
}

type Store struct {
	db              *sql.DB
	dataDir         string
	blobDir         string
	tempDir         string
	now             func() time.Time
	maxStorageBytes int64
	maxUploadOps    int64
	mutationMu      sync.Mutex
}

type Backup struct {
	ID                    string `json:"id"`
	StreamID              string `json:"stream_id"`
	DatabaseName          string `json:"database_name"`
	SourceInstallationID  string `json:"source_installation_id"`
	Filename              string `json:"filename"`
	SizeBytes             int64  `json:"size_bytes"`
	SHA256                string `json:"sha256"`
	CreatedAt             string `json:"created_at"`
	RetentionDeletedCount int    `json:"retention_deleted_count,omitempty"`
	storagePath           string
}

type Stream struct {
	ID                  string  `json:"id"`
	DatabaseName        string  `json:"database_name"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
	BackupCount         int64   `json:"backup_count"`
	RetentionKeepLatest *int    `json:"retention_keep_latest,omitempty"`
	LatestBackup        *Backup `json:"latest_backup,omitempty"`
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type PruneResult struct {
	StreamID     string `json:"stream_id"`
	KeepLatest   int    `json:"keep_latest"`
	DeletedCount int    `json:"deleted_count"`
}

type DeleteResult struct {
	StreamID     string   `json:"stream_id"`
	DeletedIDs   []string `json:"deleted_ids"`
	DeletedCount int      `json:"deleted_count"`
}

type deletionCandidate struct {
	id   string
	path string
	size int64
}

func ValidateIdentifier(value string) bool { return identifierPattern.MatchString(value) }
