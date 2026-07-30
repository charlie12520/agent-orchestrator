package httpd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	executionjournal "github.com/aoagents/agent-orchestrator/backend/internal/service/executionjournal"
)

type executionHTTPBackend struct{}

func (*executionHTTPBackend) Execute(context.Context, []byte) (executionjournal.Result, error) {
	return executionjournal.Result{}, nil
}

func (*executionHTTPBackend) GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error) {
	return domain.ExecutionOperationJournal{}, false, nil
}

func (*executionHTTPBackend) GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error) {
	return domain.ExecutionRunBinding{}, false, nil
}

func TestExecutionRoutesRequireManagedRootAuthBeforeAvailability(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})

	cases := []struct {
		name   string
		req    *http.Request
		status int
		code   string
	}{
		{name: "missing bearer", req: httptest.NewRequest(http.MethodGet, "/api/v1/execution/operations/"+strings.Repeat("0", 64), nil), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "wrong bearer", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1")
			r.Header.Set("Authorization", "Bearer "+strings.Repeat("f", 64))
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "duplicate bearer", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1")
			r.Header.Add("Authorization", "Bearer "+managedSecretHex)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "missing generation", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1")
			r.Header.Del(daemonGenerationHeader)
			return r
		}(), status: http.StatusConflict, code: "STALE_DAEMON_CONTROL_GENERATION"},
		{name: "duplicate generation", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1")
			r.Header.Add(daemonGenerationHeader, managedGeneration)
			return r
		}(), status: http.StatusConflict, code: "STALE_DAEMON_CONTROL_GENERATION"},
		{name: "stale generation", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1")
			r.Header.Set(daemonGenerationHeader, "stale-1")
			return r
		}(), status: http.StatusConflict, code: "STALE_DAEMON_CONTROL_GENERATION"},
		{name: "authenticated unavailable", req: managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1"), status: http.StatusServiceUnavailable, code: "EXECUTION_UNAVAILABLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, tc.req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d body=%s, want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if got := responseErrorCode(t, rec); got != tc.code {
				t.Fatalf("code = %q body=%s, want %q", got, rec.Body.String(), tc.code)
			}
		})
	}
}

func responseErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode API error: %v body=%s", err, rec.Body.String())
	}
	return body.Code
}

func TestExecutionRoutesAreAbsentOutsideManagedPrimaryTransport(t *testing.T) {
	backend := &executionHTTPBackend{}
	unmanaged := NewRouterWithControl(config.Config{}, discardLogger(), nil, APIDeps{Execution: backend}, ControlDeps{})
	rec := httptest.NewRecorder()
	unmanaged.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/execution/bindings/external-1", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unmanaged status = %d body=%s, want 404", rec.Code, rec.Body.String())
	}

	managed := managedRuntimeUnderTest(t)
	primary := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{Execution: backend}, ControlDeps{})
	lanScoped := markTransportScope(transportScopeLAN, primary)
	lanRequest := managedRequest(http.MethodGet, "/api/v1/execution/bindings/external-1")
	lanRequest.Host = "127.0.0.1:3001"
	lanRequest.Header.Set("X-Forwarded-For", "127.0.0.1")
	lanRequest.Header.Set("X-Real-IP", "127.0.0.1")
	rec = httptest.NewRecorder()
	lanScoped.ServeHTTP(rec, lanRequest)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("LAN-scoped status = %d body=%s, want 404", rec.Code, rec.Body.String())
	}
}

func TestExecutionRouteAliasesDoNotRedirectOrReachHandler(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{Execution: &executionHTTPBackend{}}, ControlDeps{})
	paths := []string{
		"/api/v1/execution/operations/",
		"/api/v1//execution/operations",
		"/api/v1/%65xecution/operations",
		"/api/v1/execution/%6fperations",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, managedRequest(http.MethodPost, path))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d body=%s, want 404", rec.Code, rec.Body.String())
			}
			if location := rec.Header().Get("Location"); location != "" {
				t.Fatalf("Location = %q, want no redirect", location)
			}
		})
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, managedRequest(http.MethodPut, "/api/v1/execution/operations"))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method status = %d body=%s, want 405", rec.Code, rec.Body.String())
	}
}
