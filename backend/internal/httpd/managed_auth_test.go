package httpd

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/managedcontrol"
)

const (
	managedSecretHex  = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	managedGeneration = "gen-managed-1"
)

func managedRuntimeUnderTest(t *testing.T) *managedcontrol.Runtime {
	t.Helper()
	runtime, err := managedcontrol.Load(strings.NewReader(`{"version":1,"rootSecretHex":"` + managedSecretHex + `","generation":"` + managedGeneration + `"}`))
	if err != nil {
		t.Fatalf("Load runtime: %v", err)
	}
	t.Cleanup(runtime.Close)
	return runtime
}

func managedRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.Host = "127.0.0.1:3001"
	req.Header.Set("Authorization", "Bearer "+managedSecretHex)
	req.Header.Set(daemonGenerationHeader, managedGeneration)
	return req
}

func rawHTTP(t *testing.T, addr string, raw string) (int, http.Header, string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial raw http: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set raw http deadline: %v", err)
	}
	if _, err := io.WriteString(conn, raw); err != nil {
		t.Fatalf("write raw http: %v", err)
	}
	rawResponse, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read raw http bytes: %v", err)
	}
	parts := strings.SplitN(string(rawResponse), "\r\n\r\n", 2)
	if len(parts) != 2 {
		t.Fatalf("split raw http response: %q", string(rawResponse))
	}
	lines := strings.Split(parts[0], "\r\n")
	if len(lines) == 0 {
		t.Fatalf("missing raw status line: %q", string(rawResponse))
	}
	statusFields := strings.Fields(lines[0])
	if len(statusFields) < 2 {
		t.Fatalf("bad raw status line: %q", lines[0])
	}
	statusCode, err := strconv.Atoi(statusFields[1])
	if err != nil {
		t.Fatalf("parse raw status code %q: %v", statusFields[1], err)
	}
	headers := make(http.Header)
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("bad raw header line: %q", line)
		}
		headers.Add(strings.TrimSpace(name), strings.TrimSpace(value))
	}
	return statusCode, headers, parts[1]
}

func TestManagedHealthAttestationIsTruthful(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{StartupWorkingDirectory: "/startup"}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d", rec.Code)
	}
	var body struct {
		Service     string `json:"service"`
		Generation  string `json:"generation"`
		Attestation struct {
			Protocols struct {
				AuthenticatedIPC        int `json:"authenticatedIpc"`
				DaemonControlGeneration int `json:"daemonControlGeneration"`
				GenerationFencing       int `json:"generationFencing"`
			} `json:"protocols"`
			Capabilities map[string]bool `json:"capabilities"`
		} `json:"attestation"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if body.Service != "agent-orchestrator-daemon" {
		t.Fatalf("service = %q", body.Service)
	}
	if body.Generation != managedGeneration {
		t.Fatalf("generation = %q", body.Generation)
	}
	if body.Attestation.Protocols.AuthenticatedIPC != 1 || body.Attestation.Protocols.DaemonControlGeneration != 1 {
		t.Fatalf("protocols = %+v", body.Attestation.Protocols)
	}
	if body.Attestation.Protocols.GenerationFencing != 0 {
		t.Fatalf("generationFencing = %d, want 0", body.Attestation.Protocols.GenerationFencing)
	}
	if !body.Attestation.Capabilities["authenticatedIpc"] || !body.Attestation.Capabilities["daemonControlGeneration"] || body.Attestation.Capabilities["generationFencing"] {
		t.Fatalf("capabilities = %+v", body.Attestation.Capabilities)
	}
	if strings.Contains(rec.Body.String(), managedSecretHex) {
		t.Fatal("health leaked managed secret")
	}
}

func TestUnmanagedHealthDoesNotClaimManagedAuth(t *testing.T) {
	router := newTestRouter(config.Config{}, discardLogger(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var body struct {
		Generation  string `json:"generation"`
		Attestation struct {
			Protocols struct {
				AuthenticatedIPC        int `json:"authenticatedIpc"`
				DaemonControlGeneration int `json:"daemonControlGeneration"`
			} `json:"protocols"`
			Capabilities map[string]bool `json:"capabilities"`
		} `json:"attestation"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if body.Generation != "" || body.Attestation.Protocols.AuthenticatedIPC != 0 || body.Attestation.Protocols.DaemonControlGeneration != 0 {
		t.Fatalf("unmanaged health = %+v", body)
	}
	if body.Attestation.Capabilities["authenticatedIpc"] || body.Attestation.Capabilities["daemonControlGeneration"] {
		t.Fatalf("unmanaged capabilities = %+v", body.Attestation.Capabilities)
	}
}

func TestManagedRoutesRequireStrictBearerAndGeneration(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{
		RequestShutdown: func() {},
	})
	cases := []struct {
		name   string
		req    *http.Request
		status int
		code   string
	}{
		{name: "missing auth", req: httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "wrong auth", req: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer deadbeef")
			r.Header.Set(daemonGenerationHeader, managedGeneration)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "uppercase auth rejected", req: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer "+strings.ToUpper(managedSecretHex))
			r.Header.Set(daemonGenerationHeader, managedGeneration)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "auth double space rejected", req: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer  "+managedSecretHex)
			r.Header.Set(daemonGenerationHeader, managedGeneration)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "auth lowercase scheme rejected", req: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
			r.Header.Set("Authorization", "bearer "+managedSecretHex)
			r.Header.Set(daemonGenerationHeader, managedGeneration)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "duplicate auth", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/sessions")
			r.Header.Add("Authorization", "Bearer "+managedSecretHex)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "alternate auth carrier header", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/sessions")
			r.Header.Set("X-AO-Authorization", "Bearer "+managedSecretHex)
			return r
		}(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "query credential", req: func() *http.Request { r := managedRequest(http.MethodGet, "/api/v1/sessions?token=abc"); return r }(), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "missing generation", req: func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
			r.Header.Set("Authorization", "Bearer "+managedSecretHex)
			return r
		}(), status: http.StatusConflict, code: "STALE_DAEMON_CONTROL_GENERATION"},
		{name: "duplicate generation rejected", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/sessions")
			r.Header.Add(daemonGenerationHeader, managedGeneration)
			return r
		}(), status: http.StatusConflict, code: "STALE_DAEMON_CONTROL_GENERATION"},
		{name: "wrong generation", req: func() *http.Request {
			r := managedRequest(http.MethodGet, "/api/v1/sessions")
			r.Header.Set(daemonGenerationHeader, "gen-stale")
			return r
		}(), status: http.StatusConflict, code: "STALE_DAEMON_CONTROL_GENERATION"},
		{name: "head non-probe still auths", req: httptest.NewRequest(http.MethodHead, "/api/v1/sessions", nil), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "unknown route still auths first", req: httptest.NewRequest(http.MethodGet, "/not-a-route", nil), status: http.StatusUnauthorized, code: "BAD_BEARER"},
		{name: "authenticated unknown route hits 404 after auth", req: managedRequest(http.MethodGet, "/not-a-route"), status: http.StatusNotFound, code: ""},
		{name: "authenticated shutdown works", req: managedRequest(http.MethodPost, "/shutdown"), status: http.StatusAccepted, code: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, tc.req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.code != "" && !strings.Contains(rec.Body.String(), tc.code) {
				t.Fatalf("body = %s, want %s", rec.Body.String(), tc.code)
			}
		})
	}
}

func TestManagedPublicHeadProbesAreUnauthenticatedAndBodyless(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{StartupWorkingDirectory: "/startup"}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			getRec := httptest.NewRecorder()
			router.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, path, nil))
			if getRec.Code != http.StatusOK {
				t.Fatalf("GET %s = %d", path, getRec.Code)
			}

			headRec := httptest.NewRecorder()
			router.ServeHTTP(headRec, httptest.NewRequest(http.MethodHead, path, nil))
			if headRec.Code != http.StatusOK {
				t.Fatalf("HEAD %s = %d", path, headRec.Code)
			}
			if headRec.Body.Len() != 0 {
				t.Fatalf("HEAD %s body length = %d, want 0", path, headRec.Body.Len())
			}
			if got := headRec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
				t.Fatalf("HEAD %s content-type = %q", path, got)
			}
			if got := headRec.Header().Get("Content-Length"); got != getRec.Header().Get("Content-Length") {
				t.Fatalf("HEAD %s content-length = %q, GET content-length = %q", path, got, getRec.Header().Get("Content-Length"))
			}
		})
	}
}

func TestManagedPublicProbeExemptionRequiresLiteralCanonicalRequestTarget(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{StartupWorkingDirectory: "/startup"}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")

	for _, tc := range []struct {
		name    string
		raw     string
		status  int
		bodyHas string
	}{
		{name: "encoded alias not public", raw: "GET /%68ealthz HTTP/1.1\r\nHost: " + addr + "\r\nConnection: close\r\n\r\n", status: http.StatusUnauthorized, bodyHas: "BAD_BEARER"},
		{name: "query not public", raw: "GET /healthz?foo=bar HTTP/1.1\r\nHost: " + addr + "\r\nConnection: close\r\n\r\n", status: http.StatusUnauthorized, bodyHas: "BAD_BEARER"},
		{name: "token query not public", raw: "GET /healthz?token=x HTTP/1.1\r\nHost: " + addr + "\r\nConnection: close\r\n\r\n", status: http.StatusUnauthorized, bodyHas: "BAD_BEARER"},
		{name: "force query not public", raw: "GET /healthz? HTTP/1.1\r\nHost: " + addr + "\r\nConnection: close\r\n\r\n", status: http.StatusUnauthorized, bodyHas: "BAD_BEARER"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, body := rawHTTP(t, addr, tc.raw)
			if status != tc.status {
				t.Fatalf("status = %d, want %d body=%s", status, tc.status, body)
			}
			if !strings.Contains(body, tc.bodyHas) {
				t.Fatalf("body = %s, want %s", body, tc.bodyHas)
			}
		})
	}
}

func TestManagedPublicProbeRawHTTPBehaviorIsExplicit(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{StartupWorkingDirectory: "/startup"}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")

	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run("public-"+path, func(t *testing.T) {
			getStatus, getHeaders, getBody := rawHTTP(t, addr, "GET "+path+" HTTP/1.1\r\nHost: "+addr+"\r\nConnection: close\r\n\r\n")
			if getStatus != http.StatusOK {
				t.Fatalf("GET %s = %d body=%s", path, getStatus, getBody)
			}
			headStatus, headHeaders, headBody := rawHTTP(t, addr, "HEAD "+path+" HTTP/1.1\r\nHost: "+addr+"\r\nConnection: close\r\n\r\n")
			if headStatus != http.StatusOK {
				t.Fatalf("HEAD %s = %d body=%s", path, headStatus, headBody)
			}
			if headBody != "" {
				t.Fatalf("HEAD %s body = %q, want empty", path, headBody)
			}
			if headHeaders.Get("Content-Length") != getHeaders.Get("Content-Length") {
				t.Fatalf("HEAD %s content-length = %q, GET content-length = %q", path, headHeaders.Get("Content-Length"), getHeaders.Get("Content-Length"))
			}
		})
		t.Run("authenticated-query-"+path, func(t *testing.T) {
			status, _, body := rawHTTP(t, addr,
				"GET "+path+"?foo=bar HTTP/1.1\r\nHost: "+addr+"\r\nAuthorization: Bearer "+managedSecretHex+"\r\n"+daemonGenerationHeader+": "+managedGeneration+"\r\nConnection: close\r\n\r\n")
			if status != http.StatusOK {
				t.Fatalf("authenticated GET %s?foo=bar = %d body=%s", path, status, body)
			}
		})
	}

	status, _, body := rawHTTP(t, addr,
		"GET /%68ealthz HTTP/1.1\r\nHost: "+addr+"\r\nAuthorization: Bearer "+managedSecretHex+"\r\n"+daemonGenerationHeader+": "+managedGeneration+"\r\nConnection: close\r\n\r\n")
	if status != http.StatusNotFound {
		t.Fatalf("authenticated encoded alias = %d, want 404 body=%s", status, body)
	}
}

func TestManagedHTTPParsesOuterOWSBeforeSemanticValidation(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")

	status, _, body := rawHTTP(t, addr,
		"GET /api/v1/sessions HTTP/1.1\r\nHost: "+addr+"\r\nAuthorization:\t Bearer "+managedSecretHex+" \t\r\n"+daemonGenerationHeader+":\t"+managedGeneration+" \t\r\nConnection: close\r\n\r\n")
	if status != http.StatusNotImplemented {
		t.Fatalf("outer OWS normalized request = %d, want 501 body=%s", status, body)
	}
}

func TestManagedEventsAndMuxAreProtected(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	ts := httptest.NewServer(router)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/events")
	if err != nil {
		t.Fatalf("GET events: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/v1/events = %d, want 401", resp.StatusCode)
	}

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/mux"
	_, resp, err = websocket.Dial(context.Background(), wsURL, nil)
	if err == nil {
		t.Fatal("websocket dial without auth succeeded")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("mux status = %v, want 401", resp)
	}
}

func TestManagedPreflightRequiresAuth(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/sessions", nil)
	req.Header.Set("Origin", "app://renderer")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("preflight status = %d, want 401", rec.Code)
	}
}

func TestManagedConcurrentAuthRequests(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, managedRequest(http.MethodGet, "/api/v1/sessions"))
			if rec.Code != http.StatusNotImplemented {
				errs <- &statusError{got: rec.Code, want: http.StatusNotImplemented}
				return
			}
			errs <- nil
		}()
	}
	timeout := time.After(3 * time.Second)
	for i := 0; i < 8; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatal("concurrent auth requests timed out")
		}
	}
}

type statusError struct {
	got  int
	want int
}

func (e *statusError) Error() string {
	return "unexpected status"
}
