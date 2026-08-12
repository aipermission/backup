package api

import (
	"errors"
	"net/http"

	"github.com/aipermission/backup/internal/store"
)

func (s *Server) storageInfo(w http.ResponseWriter, r *http.Request) {
	usage, err := s.store.StorageUsage(r.Context())
	if err != nil {
		s.logger.Error("read backup storage usage", "error", err)
		writeError(w, http.StatusInternalServerError, "storage_usage_failed", "backup storage usage could not be read")
		return
	}
	writeJSON(w, http.StatusOK, usage)
}

func (s *Server) getRetention(w http.ResponseWriter, r *http.Request) {
	policy, err := s.store.GetRetentionPolicy(r.Context(), r.PathValue("stream_id"))
	if err != nil {
		s.writeRetentionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) previewRetention(w http.ResponseWriter, r *http.Request) {
	var request struct {
		KeepLatest int `json:"keep_latest"`
	}
	if !decodeJSONRequest(w, r, 4096, &request, "invalid_retention_request", "retention request is invalid") {
		return
	}
	preview, err := s.store.PreviewRetention(r.Context(), r.PathValue("stream_id"), request.KeepLatest)
	if err != nil {
		s.writeRetentionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, preview)
}

func (s *Server) updateRetention(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Enabled    bool `json:"enabled"`
		KeepLatest int  `json:"keep_latest"`
		ApplyNow   bool `json:"apply_now"`
	}
	if !decodeJSONRequest(w, r, 4096, &request, "invalid_retention_request", "retention request is invalid") {
		return
	}
	result, err := s.store.SetRetentionPolicy(r.Context(), r.PathValue("stream_id"), request.Enabled, request.KeepLatest, request.ApplyNow)
	if err != nil {
		s.writeRetentionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) writeRetentionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid_retention_request", "stream id or keep_latest is invalid")
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "stream_not_found", "backup stream was not found")
	default:
		s.logger.Error("backup retention", "error", err)
		writeError(w, http.StatusInternalServerError, "retention_failed", "backup retention operation failed")
	}
}
