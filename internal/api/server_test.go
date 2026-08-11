package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aipermission/backup/internal/store"
)

const testToken = "test-token-with-at-least-thirty-two-characters"

func TestBackupLifecycleAndAuthentication(t *testing.T) {
	server := newTestServer(t, 1024)

	request := httptest.NewRequest(http.MethodGet, "/v1/streams", nil)
	request.Header.Set("X-AIPermission-Protocol-Version", protocolVersion)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", response.Code)
	}

	payload := []byte("encrypted-aipdb-fixture")
	request = authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-AIPermission-Database-Name", "Project A")
	request.Header.Set("X-AIPermission-Source-Installation-ID", "install-a")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", response.Code, response.Body.String())
	}
	var created store.Backup
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}

	request = authorizedRequest(http.MethodGet, "/v1/streams/project-a/backups", nil)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(created.ID)) {
		t.Fatalf("backup missing from list: %d %s", response.Code, response.Body.String())
	}

	request = authorizedRequest(http.MethodGet, "/v1/streams/project-a/backups/"+created.ID, nil)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), payload) {
		t.Fatalf("unexpected download: %d %q", response.Code, response.Body.Bytes())
	}

	request = authorizedRequest(http.MethodPost, "/v1/streams/project-a/prune", bytes.NewBufferString(`{"keep_latest":1}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"keep_latest":1`)) {
		t.Fatalf("unexpected prune response: %d %s", response.Code, response.Body.String())
	}
}

func TestProtocolAndUploadLimits(t *testing.T) {
	server := newTestServer(t, 4)
	request := httptest.NewRequest(http.MethodGet, "/v1/streams", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("expected 426, got %d", response.Code)
	}

	request = authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups", bytes.NewReader([]byte("12345")))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-AIPermission-Database-Name", "Project A")
	request.Header.Set("X-AIPermission-Source-Installation-ID", "install-a")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", response.Code, response.Body.String())
	}
}

func TestPruneRejectsUnsafeRetention(t *testing.T) {
	server := newTestServer(t, 1024)
	request := authorizedRequest(http.MethodPost, "/v1/streams/project-a/prune", bytes.NewBufferString(`{"keep_latest":0}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", response.Code, response.Body.String())
	}
}

func TestDeleteSelectedBackupsAndProtectLastVersion(t *testing.T) {
	server := newTestServer(t, 1024)
	created := make([]store.Backup, 0, 3)
	for _, payload := range []string{"first", "second", "third"} {
		request := authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups", bytes.NewBufferString(payload))
		request.Header.Set("Content-Type", "application/octet-stream")
		request.Header.Set("X-AIPermission-Database-Name", "Project A")
		request.Header.Set("X-AIPermission-Source-Installation-ID", "install-a")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("create backup: %d %s", response.Code, response.Body.String())
		}
		var item store.Backup
		if err := json.NewDecoder(response.Body).Decode(&item); err != nil {
			t.Fatal(err)
		}
		created = append(created, item)
	}

	request := authorizedRequest(http.MethodDelete, "/v1/streams/project-a/backups/"+created[0].ID, nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(created[0].ID)) {
		t.Fatalf("delete one backup: %d %s", response.Code, response.Body.String())
	}

	request = authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups/delete", bytes.NewBufferString(`{"backup_ids":["`+created[1].ID+`"]}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(created[1].ID)) {
		t.Fatalf("delete selected backups: %d %s", response.Code, response.Body.String())
	}

	request = authorizedRequest(http.MethodDelete, "/v1/streams/project-a/backups/"+created[2].ID, nil)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !bytes.Contains(response.Body.Bytes(), []byte("last_backup_protected")) {
		t.Fatalf("last backup was not protected: %d %s", response.Code, response.Body.String())
	}
}

func TestAutomaticRetentionPreviewAndStorageUsage(t *testing.T) {
	server := newTestServerWithStorageQuota(t, 1024, 32)
	for _, payload := range []string{"first", "second", "third"} {
		response := uploadTestBackup(t, server, payload)
		if response.Code != http.StatusCreated {
			t.Fatalf("create backup: %d %s", response.Code, response.Body.String())
		}
	}

	request := authorizedRequest(http.MethodPost, "/v1/streams/project-a/retention/preview", bytes.NewBufferString(`{"keep_latest":2}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"delete_count":1`)) {
		t.Fatalf("preview retention: %d %s", response.Code, response.Body.String())
	}

	request = authorizedRequest(http.MethodPut, "/v1/streams/project-a/retention", bytes.NewBufferString(`{"enabled":true,"keep_latest":2,"apply_now":true}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"deleted_count":1`)) {
		t.Fatalf("apply retention: %d %s", response.Code, response.Body.String())
	}

	response = uploadTestBackup(t, server, "fourth")
	if response.Code != http.StatusCreated || !bytes.Contains(response.Body.Bytes(), []byte(`"retention_deleted_count":1`)) {
		t.Fatalf("automatic retention: %d %s", response.Code, response.Body.String())
	}

	request = authorizedRequest(http.MethodGet, "/v1/streams/project-a/backups", nil)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || bytes.Count(response.Body.Bytes(), []byte(`"stream_id":"project-a"`)) != 2 {
		t.Fatalf("retained backup list: %d %s", response.Code, response.Body.String())
	}

	request = authorizedRequest(http.MethodGet, "/v1/storage", nil)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"quota_enabled":true`)) || !bytes.Contains(response.Body.Bytes(), []byte(`"backup_count":2`)) {
		t.Fatalf("storage usage: %d %s", response.Code, response.Body.String())
	}
}

func TestStorageQuotaRejectsUploadWithoutCreatingVersion(t *testing.T) {
	server := newTestServerWithStorageQuota(t, 1024, 5)
	response := uploadTestBackup(t, server, "123456")
	if response.Code != http.StatusInsufficientStorage || !bytes.Contains(response.Body.Bytes(), []byte("storage_quota_exceeded")) {
		t.Fatalf("quota rejection: %d %s", response.Code, response.Body.String())
	}

	request := authorizedRequest(http.MethodGet, "/v1/streams", nil)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte("project-a")) {
		t.Fatalf("quota rejection created stream metadata: %d %s", response.Code, response.Body.String())
	}
}

func newTestServer(t *testing.T, maxBytes int64) http.Handler {
	return newTestServerWithStorageQuota(t, maxBytes, 0)
}

func newTestServerWithStorageQuota(t *testing.T, maxBytes, maxStorageBytes int64) http.Handler {
	t.Helper()
	storage, err := store.Open(t.TempDir(), store.Options{MaxStorageBytes: maxStorageBytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	return New(Config{Token: testToken, MaxUploadBytes: maxBytes, Version: "test"}, storage, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func uploadTestBackup(t *testing.T, server http.Handler, payload string) *httptest.ResponseRecorder {
	t.Helper()
	request := authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups", bytes.NewBufferString(payload))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-AIPermission-Database-Name", "Project A")
	request.Header.Set("X-AIPermission-Source-Installation-ID", "install-a")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func authorizedRequest(method, target string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, target, body)
	request.Header.Set("Authorization", "Bearer "+testToken)
	request.Header.Set("X-AIPermission-Protocol-Version", protocolVersion)
	return request
}
