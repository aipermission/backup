package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/aipermission/backup/internal/store"
)

const protocolVersion = "2"

type Config struct {
	Token          string
	MaxUploadBytes int64
	Version        string
}

type Server struct {
	config    Config
	store     *store.Store
	logger    *slog.Logger
	tokenHash [sha256.Size]byte
	api       http.Handler
}

func New(config Config, storage *store.Store, logger *slog.Logger) http.Handler {
	tokenHash := sha256.Sum256([]byte(config.Token))
	config.Token = ""
	server := &Server{config: config, store: storage, logger: logger, tokenHash: tokenHash}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.Handle("GET /v1/info", server.auth(http.HandlerFunc(server.info)))
	mux.Handle("GET /v1/storage", server.auth(server.protocol(http.HandlerFunc(server.storageInfo))))
	mux.Handle("GET /v1/streams", server.auth(server.protocol(http.HandlerFunc(server.listStreams))))
	mux.Handle("POST /v1/streams/{stream_id}/backups", server.auth(server.protocol(http.HandlerFunc(server.upload))))
	mux.Handle("GET /v1/streams/{stream_id}/backups", server.auth(server.protocol(http.HandlerFunc(server.listBackups))))
	mux.Handle("POST /v1/streams/{stream_id}/prune", server.auth(server.protocol(http.HandlerFunc(server.pruneBackups))))
	mux.Handle("GET /v1/streams/{stream_id}/retention", server.auth(server.protocol(http.HandlerFunc(server.getRetention))))
	mux.Handle("POST /v1/streams/{stream_id}/retention/preview", server.auth(server.protocol(http.HandlerFunc(server.previewRetention))))
	mux.Handle("PUT /v1/streams/{stream_id}/retention", server.auth(server.protocol(http.HandlerFunc(server.updateRetention))))
	mux.Handle("POST /v1/streams/{stream_id}/backups/delete", server.auth(server.protocol(http.HandlerFunc(server.deleteBackups))))
	mux.Handle("DELETE /v1/streams/{stream_id}/backups/{backup_id}", server.auth(server.protocol(http.HandlerFunc(server.deleteBackup))))
	mux.Handle("GET /v1/streams/{stream_id}/backups/{backup_id}", server.auth(server.protocol(http.HandlerFunc(server.download))))
	server.api = mux
	return server.recover(server.securityHeaders(server.requestLog(mux)))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) info(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":          "aipermission-backup",
		"version":          s.config.Version,
		"protocol_version": protocolVersion,
		"capabilities":     []string{"immutable_upload", "list_streams", "list_versions", "download", "prune_versions", "delete_versions", "storage_usage", "automatic_retention"},
		"max_upload_bytes": s.config.MaxUploadBytes,
		"storage_schema":   store.SchemaVersion,
	})
}

func (s *Server) deleteBackup(w http.ResponseWriter, r *http.Request) {
	s.deleteBackupIDs(w, r, []string{r.PathValue("backup_id")})
}

func (s *Server) deleteBackups(w http.ResponseWriter, r *http.Request) {
	var request struct {
		BackupIDs []string `json:"backup_ids"`
	}
	if !decodeJSONRequest(w, r, 8192, &request, "invalid_delete_request", "request must contain 1 to 100 backup_ids") {
		return
	}
	s.deleteBackupIDs(w, r, request.BackupIDs)
}

func (s *Server) deleteBackupIDs(w http.ResponseWriter, r *http.Request, backupIDs []string) {
	result, err := s.store.DeleteBackups(r.Context(), r.PathValue("stream_id"), backupIDs)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, "invalid_delete_request", "backup_ids must contain 1 to 100 unique backup identifiers")
		case errors.Is(err, store.ErrLastBackup):
			writeError(w, http.StatusConflict, "last_backup_protected", "at least one backup version must remain in the stream")
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, "backup_not_found", "one or more selected backups were not found")
		default:
			s.logger.Error("delete backups", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "backup versions could not be deleted")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) pruneBackups(w http.ResponseWriter, r *http.Request) {
	var request struct {
		KeepLatest int `json:"keep_latest"`
	}
	if !decodeJSONRequest(w, r, 4096, &request, "invalid_prune_request", "request must contain keep_latest between 1 and 1000") {
		return
	}
	result, err := s.store.PruneBackups(r.Context(), r.PathValue("stream_id"), request.KeepLatest)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, "invalid_prune_request", "keep_latest must be between 1 and 1000")
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, "stream_not_found", "backup stream was not found")
		default:
			s.logger.Error("prune backups", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "backup versions could not be pruned")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeJSONRequest(w http.ResponseWriter, r *http.Request, maxBytes int64, target any, errorCode, errorMessage string) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, errorCode, errorMessage)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, errorCode, "request must contain one JSON object")
		return false
	}
	return true
}

func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/octet-stream" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/octet-stream")
		return
	}
	databaseName := strings.TrimSpace(r.Header.Get("X-AIPermission-Database-Name"))
	sourceID := strings.TrimSpace(r.Header.Get("X-AIPermission-Source-Installation-ID"))
	if databaseName == "" || sourceID == "" {
		writeError(w, http.StatusBadRequest, "metadata_required", "database name and source installation id headers are required")
		return
	}
	if r.ContentLength > s.config.MaxUploadBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "upload_too_large", "backup exceeds the configured upload limit")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxUploadBytes)
	backup, err := s.store.CreateBackup(r.Context(), r.PathValue("stream_id"), databaseName, sourceID, r.Body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		switch {
		case errors.Is(err, store.ErrStreamConflict):
			writeError(w, http.StatusConflict, "stream_conflict", err.Error())
		case errors.As(err, &maxBytesError):
			writeError(w, http.StatusRequestEntityTooLarge, "upload_too_large", "backup exceeds the configured upload limit")
		case errors.Is(err, store.ErrInvalidInput):
			writeError(w, http.StatusBadRequest, "invalid_backup", strings.TrimPrefix(err.Error(), store.ErrInvalidInput.Error()+": "))
		case errors.Is(err, store.ErrQuotaExceeded):
			writeError(w, http.StatusInsufficientStorage, "storage_quota_exceeded", "backup storage quota does not have enough remaining capacity")
		default:
			s.logger.Error("upload backup", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "backup could not be stored")
		}
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/v1/streams/%s/backups/%s", backup.StreamID, backup.ID))
	writeJSON(w, http.StatusCreated, backup)
}

func (s *Server) listStreams(w http.ResponseWriter, r *http.Request) {
	limit, ok := pageLimit(w, r)
	if !ok {
		return
	}
	page, err := s.store.ListStreams(r.Context(), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid_list_request", "cursor is invalid")
		} else {
			s.logger.Error("list backup streams", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "backup streams could not be listed")
		}
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) listBackups(w http.ResponseWriter, r *http.Request) {
	limit, ok := pageLimit(w, r)
	if !ok {
		return
	}
	page, err := s.store.ListBackups(r.Context(), r.PathValue("stream_id"), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid_list_request", "stream identifier or cursor is invalid")
		} else {
			s.logger.Error("list backups", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "backups could not be listed")
		}
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	backup, file, err := s.store.OpenBackup(r.Context(), r.PathValue("stream_id"), r.PathValue("backup_id"))
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, "backup_not_found", "backup was not found")
		case errors.Is(err, store.ErrCorrupt):
			writeError(w, http.StatusConflict, "backup_corrupt", "stored backup failed integrity verification")
		default:
			s.logger.Error("download backup", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "backup could not be downloaded")
		}
		return
	}
	defer file.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": backup.Filename}))
	w.Header().Set("Content-Length", strconv.FormatInt(backup.SizeBytes, 10))
	w.Header().Set("X-AIPermission-SHA256", backup.SHA256)
	w.Header().Set("X-AIPermission-Backup-ID", backup.ID)
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, file); err != nil {
		s.logger.Warn("stream backup download", "backup_id", backup.ID, "error", err)
	}
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, prefix) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer authentication is required")
			return
		}
		provided := sha256.Sum256([]byte(strings.TrimSpace(strings.TrimPrefix(header, prefix))))
		if subtle.ConstantTimeCompare(provided[:], s.tokenHash[:]) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer authentication is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) protocol(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-AIPermission-Protocol-Version") != protocolVersion {
			writeError(w, http.StatusUpgradeRequired, "protocol_mismatch", "X-AIPermission-Protocol-Version must be "+protocolVersion)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("panic recovered", "type", fmt.Sprintf("%T", recovered))
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-AIPermission-Protocol-Version", protocolVersion)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

func pageLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 100")
			return 0, false
		}
		limit = parsed
	}
	return limit, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
