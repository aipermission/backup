package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aipermission/backup/internal/store"
)

const testToken = "test-token-with-at-least-thirty-two-characters"

var testOperationSequence atomic.Uint64

func TestInfoRequiresAuthenticationAndPublishesExactProtocolContract(t *testing.T) {
	server := newTestServer(t, 1024)
	request := httptest.NewRequest(http.MethodGet, "/v1/info", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", response.Code, http.StatusUnauthorized)
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/info", nil)
	request.Header.Set("Authorization", "Bearer "+testToken)
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type = %q, want application/json", response.Header().Get("Content-Type"))
	}
	payload := response.Body.Bytes()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 {
		t.Fatalf("info fields = %v, want exact six-field contract", fields)
	}
	var info struct {
		Service         string   `json:"service"`
		Version         string   `json:"version"`
		ProtocolVersion string   `json:"protocol_version"`
		Capabilities    []string `json:"capabilities"`
		MaxUploadBytes  int64    `json:"max_upload_bytes"`
		StorageSchema   int      `json:"storage_schema"`
	}
	if err := json.Unmarshal(payload, &info); err != nil {
		t.Fatal(err)
	}
	wantCapabilities := []string{
		"immutable_upload", "idempotent_upload", "upload_operation_tombstones", "list_streams", "list_versions",
		"download", "prune_versions", "delete_versions", "storage_usage", "automatic_retention",
	}
	if info.Service != "aipermission-backup" || info.Version != "test" || info.ProtocolVersion != protocolVersion ||
		info.MaxUploadBytes != 1024 || info.StorageSchema != store.SchemaVersion || !reflect.DeepEqual(info.Capabilities, wantCapabilities) {
		t.Fatalf("unexpected info contract: %#v", info)
	}
}

func TestReleaseComposePinMatchesProtocolVersion(t *testing.T) {
	compose, err := os.ReadFile("../../docker-compose.release.yml")
	if err != nil {
		t.Fatal(err)
	}
	want := "AIPERMISSION_BACKUP_VERSION:-0." + protocolVersion + ".0"
	if !strings.Contains(string(compose), want) {
		t.Fatalf("release Compose does not pin the protocol %s service image; want %q", protocolVersion, want)
	}
}

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

func TestUploadOperationIDDeduplicatesLostResponses(t *testing.T) {
	server := newTestServer(t, 1024)
	upload := func(payload string) *httptest.ResponseRecorder {
		request := authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups", bytes.NewBufferString(payload))
		request.Header.Set("Content-Type", "application/octet-stream")
		request.Header.Set("X-AIPermission-Database-Name", "Project A")
		request.Header.Set("X-AIPermission-Source-Installation-ID", "install-a")
		request.Header.Set("X-AIPermission-Operation-ID", "stable-operation")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}
	first := upload("encrypted-backup")
	second := upload("body-is-ignored-for-a-replay")
	if first.Code != http.StatusCreated || second.Code != http.StatusOK {
		t.Fatalf("unexpected statuses: first=%d second=%d", first.Code, second.Code)
	}
	var firstBackup, secondBackup store.Backup
	if err := json.NewDecoder(first.Body).Decode(&firstBackup); err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(second.Body).Decode(&secondBackup); err != nil {
		t.Fatal(err)
	}
	if firstBackup.ID != secondBackup.ID {
		t.Fatalf("idempotent replay returned different backups: %q != %q", firstBackup.ID, secondBackup.ID)
	}
}

func TestUploadOperationReturnsGoneAfterBackupDeletion(t *testing.T) {
	server := newTestServer(t, 1024)
	upload := func(operationID, payload string) *httptest.ResponseRecorder {
		request := authorizedRequest(http.MethodPost, "/v1/streams/project-a/backups", bytes.NewBufferString(payload))
		request.Header.Set("Content-Type", "application/octet-stream")
		request.Header.Set("X-AIPermission-Database-Name", "Project A")
		request.Header.Set("X-AIPermission-Source-Installation-ID", "install-a")
		request.Header.Set("X-AIPermission-Operation-ID", operationID)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}
	first := upload("deleted-operation", "first")
	second := upload("retained-operation", "second")
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("upload statuses: first=%d second=%d", first.Code, second.Code)
	}
	var firstBackup store.Backup
	if err := json.NewDecoder(first.Body).Decode(&firstBackup); err != nil {
		t.Fatal(err)
	}
	request := authorizedRequest(http.MethodDelete, "/v1/streams/project-a/backups/"+firstBackup.ID, nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("delete first backup: %d %s", response.Code, response.Body.String())
	}

	replay := upload("deleted-operation", "must-not-create-third")
	if replay.Code != http.StatusGone || !bytes.Contains(replay.Body.Bytes(), []byte(`"code":"operation_expired"`)) {
		t.Fatalf("expired replay: %d %s", replay.Code, replay.Body.String())
	}
}

func TestUploadOperationCapacityReturnsStableFailure(t *testing.T) {
	storage, err := store.Open(t.TempDir(), store.Options{MaxUploadOperations: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	server := New(Config{Token: testToken, MaxUploadBytes: 1024, Version: "test"}, storage, slog.New(slog.NewTextHandler(io.Discard, nil)))

	first := uploadTestBackup(t, server, "first")
	if first.Code != http.StatusCreated {
		t.Fatalf("first upload: %d %s", first.Code, first.Body.String())
	}
	second := uploadTestBackup(t, server, "second")
	if second.Code != http.StatusInsufficientStorage || !bytes.Contains(second.Body.Bytes(), []byte(`"code":"operation_ledger_full"`)) {
		t.Fatalf("capacity rejection: %d %s", second.Code, second.Body.String())
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
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"quota_enabled":true`)) ||
		!bytes.Contains(response.Body.Bytes(), []byte(`"backup_count":2`)) ||
		!bytes.Contains(response.Body.Bytes(), []byte(`"upload_operations":4`)) ||
		!bytes.Contains(response.Body.Bytes(), []byte(`"upload_operation_limit":1000000`)) {
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
	request.Header.Set("X-AIPermission-Operation-ID", "test-operation-"+strconv.FormatUint(testOperationSequence.Add(1), 10))
	return request
}
