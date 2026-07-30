package controllers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/managedcontrol"
	executionapi "github.com/aoagents/agent-orchestrator/backend/internal/service/executionapi"
	executionjournal "github.com/aoagents/agent-orchestrator/backend/internal/service/executionjournal"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

const (
	executionTestSecret     = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	executionTestGeneration = "execution-http-test-1"
)

var validLaunchRequest = `{"version":1,"externalRunId":"external-http-1","operation":"launch","idempotencyKey":"launch-http-1","request":{"prompt":"hello"}}`

type fakeExecutionBackend struct {
	executeCalls int
	lastRaw      []byte
	result       executionjournal.Result
	executeErr   error
	journal      domain.ExecutionOperationJournal
	journalFound bool
	journalErr   error
	binding      domain.ExecutionRunBinding
	bindingFound bool
	bindingErr   error
}

func (f *fakeExecutionBackend) Execute(_ context.Context, raw []byte) (executionjournal.Result, error) {
	f.executeCalls++
	f.lastRaw = append([]byte(nil), raw...)
	return f.result, f.executeErr
}

func (f *fakeExecutionBackend) GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error) {
	return f.journal, f.journalFound, f.journalErr
}

func (f *fakeExecutionBackend) GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error) {
	return f.binding, f.bindingFound, f.bindingErr
}

type deterministicExecutionDispatcher struct {
	calls int
}

func (d *deterministicExecutionDispatcher) Dispatch(_ context.Context, command executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error) {
	d.calls++
	if command.Operation != domain.ExecutionLaunch {
		return executionjournal.DispatchReceipt{}, errors.New("unexpected non-launch operation")
	}
	return executionjournal.DispatchReceipt{
		RunID:      "run-http-1",
		ResultJSON: []byte(`{"accepted":true,"backend":"deterministic-test"}`),
	}, nil
}

func executionServer(t *testing.T, backend interface {
	Execute(context.Context, []byte) (executionjournal.Result, error)
	GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error)
	GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error)
}) *httptest.Server {
	t.Helper()
	managed, err := managedcontrol.Load(strings.NewReader(`{"version":1,"rootSecretHex":"` + executionTestSecret + `","generation":"` + executionTestGeneration + `"}`))
	if err != nil {
		t.Fatalf("load managed runtime: %v", err)
	}
	t.Cleanup(managed.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := httpd.NewRouterWithControl(config.Config{}, log, nil, managed, httpd.APIDeps{Execution: backend}, httpd.ControlDeps{})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

func executionRequest(t *testing.T, server *httptest.Server, method, path, body string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+executionTestSecret)
	request.Header.Set("X-AO-Daemon-Generation", executionTestGeneration)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "launch-http-1")
	return request
}

func performExecutionRequest(t *testing.T, request *http.Request) (int, []byte) {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return response.StatusCode, body
}

func executionErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var decoded struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, body)
	}
	return decoded.Code
}

func TestExecutionControllerPassesValidatedRequestBytesUnchanged(t *testing.T) {
	backend := &fakeExecutionBackend{result: executionjournal.Result{
		OperationID: strings.Repeat("a", 64), ExternalRunID: "external-http-1",
		RunID: "run-http-1", Operation: domain.ExecutionLaunch,
		ProcessGeneration: 1, State: domain.ExecutionResult, ResultJSON: []byte(`{}`),
	}}
	server := executionServer(t, backend)
	raw := `{ "request": { "prompt": "hello" }, "idempotencyKey": "launch-http-1", "operation": "launch", "externalRunId": "external-http-1", "version": 1 }`
	request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", raw)
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	status, body := performExecutionRequest(t, request)
	if status != http.StatusOK {
		t.Fatalf("response = %d %s", status, body)
	}
	if !bytes.Equal(backend.lastRaw, []byte(raw)) {
		t.Fatalf("backend bytes changed\n got: %q\nwant: %q", backend.lastRaw, raw)
	}
}

func TestExecutionControllerRejectsNonCanonicalTransportBeforeBackend(t *testing.T) {
	backend := &fakeExecutionBackend{}
	server := executionServer(t, backend)

	tests := []struct {
		name       string
		body       string
		mutate     func(*http.Request)
		wantStatus int
		wantCode   string
	}{
		{name: "missing content type", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header.Del("Content-Type") }, wantStatus: http.StatusUnsupportedMediaType, wantCode: "EXECUTION_CONTENT_TYPE_REQUIRED"},
		{name: "wrong content type", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, wantStatus: http.StatusUnsupportedMediaType, wantCode: "EXECUTION_CONTENT_TYPE_REQUIRED"},
		{name: "extra media parameter", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header.Set("Content-Type", "application/json; profile=test") }, wantStatus: http.StatusUnsupportedMediaType, wantCode: "EXECUTION_CONTENT_TYPE_REQUIRED"},
		{name: "duplicate content type", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header["Content-Type"] = []string{"application/json", "application/json"} }, wantStatus: http.StatusUnsupportedMediaType, wantCode: "EXECUTION_CONTENT_TYPE_REQUIRED"},
		{name: "missing idempotency header", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header.Del("Idempotency-Key") }, wantStatus: http.StatusBadRequest, wantCode: "IDEMPOTENCY_KEY_REQUIRED"},
		{name: "duplicate idempotency header", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header["Idempotency-Key"] = []string{"launch-http-1", "launch-http-1"} }, wantStatus: http.StatusBadRequest, wantCode: "IDEMPOTENCY_KEY_REQUIRED"},
		{name: "idempotency mismatch", body: validLaunchRequest, mutate: func(r *http.Request) { r.Header.Set("Idempotency-Key", "launch-http-2") }, wantStatus: http.StatusBadRequest, wantCode: "IDEMPOTENCY_KEY_MISMATCH"},
		{name: "unknown request field", body: `{"version":1,"externalRunId":"external-http-1","operation":"launch","idempotencyKey":"launch-http-1","request":{},"extra":true}`, wantStatus: http.StatusBadRequest, wantCode: "INVALID_EXECUTION_REQUEST"},
		{name: "duplicate request field", body: `{"version":1,"externalRunId":"external-http-1","operation":"launch","idempotencyKey":"launch-http-1","idempotencyKey":"launch-http-1","request":{}}`, wantStatus: http.StatusBadRequest, wantCode: "INVALID_EXECUTION_REQUEST"},
		{name: "non-object payload", body: `{"version":1,"externalRunId":"external-http-1","operation":"launch","idempotencyKey":"launch-http-1","request":[]}`, wantStatus: http.StatusBadRequest, wantCode: "INVALID_EXECUTION_REQUEST"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := backend.executeCalls
			request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", tc.body)
			if tc.mutate != nil {
				tc.mutate(request)
			}
			status, body := performExecutionRequest(t, request)
			if status != tc.wantStatus || executionErrorCode(t, body) != tc.wantCode {
				t.Fatalf("response = %d %s, want %d %s", status, body, tc.wantStatus, tc.wantCode)
			}
			if backend.executeCalls != before {
				t.Fatalf("backend execute calls = %d, want %d", backend.executeCalls, before)
			}
		})
	}
}

func TestExecutionControllerBoundsFixedAndChunkedBodies(t *testing.T) {
	backend := &fakeExecutionBackend{}
	server := executionServer(t, backend)
	oversized := strings.Repeat("x", (1<<20)+1)
	for _, chunked := range []bool{false, true} {
		request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", oversized)
		if chunked {
			request.ContentLength = -1
		}
		status, body := performExecutionRequest(t, request)
		if status != http.StatusRequestEntityTooLarge || executionErrorCode(t, body) != "EXECUTION_REQUEST_TOO_LARGE" {
			t.Fatalf("chunked=%v response=%d %s", chunked, status, body)
		}
	}
	if backend.executeCalls != 0 {
		t.Fatalf("backend execute calls = %d, want zero", backend.executeCalls)
	}
}

func TestExecutionHTTPExactReplayConflictAndSanitizedReads(t *testing.T) {
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	dispatcher := &deterministicExecutionDispatcher{}
	service, err := executionjournal.New(executionjournal.Deps{
		Store: store, Dispatcher: dispatcher, DispatchOwner: "http-test-owner-1",
		Now:    func() time.Time { return time.Date(2026, time.July, 30, 18, 0, 0, 0, time.UTC) },
		Random: bytes.NewReader(bytes.Repeat([]byte{7}, 64)),
	})
	if err != nil {
		t.Fatalf("new execution service: %v", err)
	}
	server := executionServer(t, executionapi.New(store, service))
	requestBody := `{"version":1,"externalRunId":"external-http-1","operation":"launch","idempotencyKey":"launch-http-1","request":{"privateCredential":"must-not-leak","prompt":"hello"}}`

	request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", requestBody)
	status, firstBody := performExecutionRequest(t, request)
	if status != http.StatusOK || !strings.Contains(string(firstBody), `"replayed":false`) {
		t.Fatalf("first response = %d %s", status, firstBody)
	}
	var first struct {
		OperationID string `json:"operationId"`
	}
	if err := json.Unmarshal(firstBody, &first); err != nil || first.OperationID == "" {
		t.Fatalf("decode first response: %v body=%s", err, firstBody)
	}

	canonicalReplayBody := `{ "request": { "prompt": "hello", "privateCredential": "must-not-leak" }, "idempotencyKey": "launch-http-1", "operation": "launch", "externalRunId": "external-http-1", "version": 1 }`
	request = executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", canonicalReplayBody)
	status, replayBody := performExecutionRequest(t, request)
	if status != http.StatusOK || !strings.Contains(string(replayBody), `"replayed":true`) {
		t.Fatalf("replay response = %d %s", status, replayBody)
	}
	conflictBody := strings.Replace(requestBody, `"prompt":"hello"`, `"prompt":"different"`, 1)
	request = executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", conflictBody)
	status, body := performExecutionRequest(t, request)
	if status != http.StatusConflict || executionErrorCode(t, body) != "EXECUTION_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict response = %d %s", status, body)
	}
	if dispatcher.calls != 1 {
		t.Fatalf("dispatcher calls = %d, want 1", dispatcher.calls)
	}

	for _, path := range []string{
		"/api/v1/execution/operations/" + first.OperationID,
		"/api/v1/execution/bindings/external-http-1",
	} {
		request = executionRequest(t, server, http.MethodGet, path, "")
		status, body = performExecutionRequest(t, request)
		if status != http.StatusOK {
			t.Fatalf("GET %s = %d %s", path, status, body)
		}
		lower := strings.ToLower(string(body))
		for _, forbidden := range []string{"must-not-leak", "idempotencykey", "requesthash", "requestjson", "dispatchowner", "resulthash", "launchoperationid", "launchrequesthash"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("GET %s leaked %q: %s", path, forbidden, body)
			}
		}
	}
}

func TestExecutionUnavailableMutationDoesNotCreateJournalState(t *testing.T) {
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := executionServer(t, executionapi.New(store, nil))
	request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", validLaunchRequest)
	status, body := performExecutionRequest(t, request)
	if status != http.StatusServiceUnavailable || executionErrorCode(t, body) != "EXECUTION_UNAVAILABLE" {
		t.Fatalf("response = %d %s", status, body)
	}
	parsed, err := executionjournal.ParseRequest([]byte(validLaunchRequest))
	if err != nil {
		t.Fatalf("parse request: %v", err)
	}
	if _, found, err := store.GetExecutionOperation(context.Background(), parsed.OperationID); err != nil || found {
		t.Fatalf("operation after unavailable execute: found=%v err=%v", found, err)
	}
	if _, found, err := store.GetExecutionRunBinding(context.Background(), parsed.ExternalRunID); err != nil || found {
		t.Fatalf("binding after unavailable execute: found=%v err=%v", found, err)
	}
}

func TestExecutionResponseOmitsUnsetLifecycleAndPreservesEmptyResult(t *testing.T) {
	backend := &fakeExecutionBackend{
		journalFound: true,
		journal: domain.ExecutionOperationJournal{
			OperationID:             strings.Repeat("0", 64),
			ExternalRunID:           "external-http-1",
			Operation:               domain.ExecutionLaunch,
			TargetProcessGeneration: 1,
			State:                   domain.ExecutionAccepted,
			AcceptedAt:              time.Date(2026, time.July, 30, 19, 0, 0, 0, time.UTC),
			ResultJSON:              []byte(`{}`),
		},
	}
	server := executionServer(t, backend)
	request := executionRequest(t, server, http.MethodGet, "/api/v1/execution/operations/"+strings.Repeat("0", 64), "")
	status, body := performExecutionRequest(t, request)
	if status != http.StatusOK {
		t.Fatalf("response = %d %s", status, body)
	}
	response := string(body)
	if !strings.Contains(response, `"result":{}`) {
		t.Fatalf("empty durable result was omitted: %s", body)
	}
	for _, forbidden := range []string{"dispatchedAt", "completedAt", "0001-01-01"} {
		if strings.Contains(response, forbidden) {
			t.Fatalf("unset lifecycle field %q leaked: %s", forbidden, body)
		}
	}
}

func TestExecutionControllerRejectsCorruptOrOversizedDurableResult(t *testing.T) {
	results := map[string][]byte{
		"malformed": []byte(`{"secret":"must-not-leak"`),
		"scalar":    []byte(`true`),
		"oversized": []byte(`{"value":"` + strings.Repeat("x", 1<<20) + `"}`),
	}
	for name, resultJSON := range results {
		t.Run(name, func(t *testing.T) {
			backend := &fakeExecutionBackend{result: executionjournal.Result{
				OperationID: strings.Repeat("a", 64), ExternalRunID: "external-http-1",
				RunID: "run-http-1", Operation: domain.ExecutionLaunch,
				ProcessGeneration: 1, State: domain.ExecutionResult, ResultJSON: resultJSON,
			}}
			server := executionServer(t, backend)
			request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", validLaunchRequest)
			status, body := performExecutionRequest(t, request)
			if status != http.StatusServiceUnavailable || executionErrorCode(t, body) != "EXECUTION_STORAGE_FAILURE" {
				t.Fatalf("response = %d %s", status, body)
			}
			if strings.Contains(string(body), "must-not-leak") {
				t.Fatalf("response leaked corrupt durable result: %s", body)
			}
		})
	}
}

func TestExecutionErrorMappingDoesNotExposeBackendCauses(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "run not found", err: &executionjournal.OperationError{Code: executionjournal.CodeRunNotFound, Reason: "private-cause"}, wantStatus: http.StatusNotFound, wantCode: "EXECUTION_RUN_NOT_FOUND"},
		{name: "busy", err: &executionjournal.OperationError{Code: executionjournal.CodeRunBusy, Reason: "private-cause"}, wantStatus: http.StatusConflict, wantCode: "EXECUTION_RUN_BUSY"},
		{name: "generation", err: &executionjournal.OperationError{Code: executionjournal.CodeProcessGenerationMismatch, Reason: "private-cause"}, wantStatus: http.StatusConflict, wantCode: "EXECUTION_PROCESS_GENERATION_MISMATCH"},
		{name: "not configured", err: &executionjournal.OperationError{Code: executionjournal.CodeNotConfigured, Reason: "private-cause"}, wantStatus: http.StatusServiceUnavailable, wantCode: "EXECUTION_UNAVAILABLE"},
		{name: "storage", err: &executionjournal.OperationError{Code: executionjournal.CodeStorageFailure, Reason: "private-cause"}, wantStatus: http.StatusServiceUnavailable, wantCode: "EXECUTION_STORAGE_FAILURE"},
		{name: "unknown", err: errors.New("secret backend cause"), wantStatus: http.StatusServiceUnavailable, wantCode: "EXECUTION_STORAGE_FAILURE"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeExecutionBackend{executeErr: tc.err}
			server := executionServer(t, backend)
			request := executionRequest(t, server, http.MethodPost, "/api/v1/execution/operations", validLaunchRequest)
			status, body := performExecutionRequest(t, request)
			if status != tc.wantStatus || executionErrorCode(t, body) != tc.wantCode {
				t.Fatalf("response = %d %s, want %d %s", status, body, tc.wantStatus, tc.wantCode)
			}
			if strings.Contains(string(body), "private-cause") || strings.Contains(string(body), "secret backend cause") {
				t.Fatalf("response leaked backend cause: %s", body)
			}
		})
	}
}
