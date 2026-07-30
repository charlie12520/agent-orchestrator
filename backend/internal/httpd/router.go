// Package httpd builds and runs the daemon's HTTP surface: middleware, health
// probes, daemon control, REST APIs, and terminal WebSocket routing.
package httpd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/managedcontrol"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/telemetrymeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/terminal"
)

// ControlDeps carries the daemon-control hooks the router exposes, such as the
// callback that requests a graceful shutdown.
type ControlDeps struct {
	RequestShutdown func()
}

// NewRouterWithControl builds the root router with the standard middleware
// stack, the API surface, and the daemon-control hooks wired from ControlDeps.
// Missing Managers in deps keep routes registered but return OpenAPI-backed 501
// responses.
//
// Middleware order (outermost first):
//
//	RequestID      → attach a request id for correlation
//	RealIP         → normalise client IP (loopback proxy from the dev server)
//	requestLogger  → slog-backed access log + 5xx telemetry, carries the request id
//	recoverer      → turn a handler panic into 500 instead of crashing the daemon
//	cors           → CORS allowlist for the Electron renderer / dev origins
//
// The per-request timeout is deliberately not global: it wraps only bounded
// REST routes, never long-lived terminal streams or health probes.
func NewRouterWithControl(cfg config.Config, log *slog.Logger, termMgr *terminal.Manager, args ...any) chi.Router {
	log = loggerOrDefault(log)
	r := chi.NewRouter()
	managed, deps, control := parseRouterArgs(args)
	api := NewAPI(cfg, deps)

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(primaryLoopbackAuthMiddleware(managed))
	r.Use(requestLogger(log, deps.Telemetry))
	r.Use(recoverTelemetry(log, deps.Telemetry))
	r.Use(corsMiddleware(cfg.AllowedOrigins))
	r.Use(previewOriginMiddleware(api.sessions))

	// JSON envelopes for unmatched routes / methods — chi's defaults are
	// text/plain, which would break consumers that parse every response as
	// the locked APIError shape.
	r.NotFound(notFoundJSON)
	r.MethodNotAllowed(methodNotAllowedJSON)

	mountHealth(r, cfg, managed)
	mountTerminalMux(r, termMgr, log)
	mountControl(r, control)
	mountTelemetry(r, cfg, deps.Telemetry)
	mountMobile(r, deps.Mobile)
	api.Register(r)

	return r
}

func parseRouterArgs(args []any) (*managedcontrol.Runtime, APIDeps, ControlDeps) {
	switch len(args) {
	case 2:
		deps, _ := args[0].(APIDeps)
		control, _ := args[1].(ControlDeps)
		return nil, deps, control
	case 3:
		managed, _ := args[0].(*managedcontrol.Runtime)
		deps, _ := args[1].(APIDeps)
		control, _ := args[2].(ControlDeps)
		return managed, deps, control
	default:
		panic("NewRouterWithControl expects (deps, control) or (managed, deps, control)")
	}
}

func previewOriginMiddleware(sessions *controllers.SessionsController) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if sessions != nil && sessions.PreviewOrigin(w, r) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// mountHealth registers the liveness and readiness probes the Electron
// supervisor polls before letting the renderer connect.
func mountHealth(r chi.Router, cfg config.Config, managed *managedcontrol.Runtime) {
	healthz := daemonProbeHandler("ok", cfg, managed)
	readyz := daemonProbeHandler("ready", cfg, managed)
	r.Get("/healthz", healthz)
	r.Head("/healthz", healthz)
	r.Get("/readyz", readyz)
	r.Head("/readyz", readyz)
}

// mountControl registers the loopback daemon-control endpoints. /shutdown is
// unauthenticated and state-changing, so it is gated by localControlRequest to
// keep a browser the user happens to have open (CSRF / DNS-rebinding) or a
// remote client from being able to kill the daemon.
func mountControl(r chi.Router, deps ControlDeps) {
	if deps.RequestShutdown == nil {
		return
	}
	r.Post("/shutdown", func(w http.ResponseWriter, req *http.Request) {
		if !localControlRequest(req) {
			envelope.WriteJSON(w, http.StatusForbidden, map[string]any{
				"status":  "forbidden",
				"service": daemonmeta.ServiceName,
			})
			return
		}
		envelope.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":  "shutting_down",
			"service": daemonmeta.ServiceName,
			"pid":     os.Getpid(),
		})
		deps.RequestShutdown()
	})
}

// mountMobile registers the Connect Mobile control routes: status, enable,
// disable, and regenerate. These toggle the LAN bridge that lets a phone reach
// the daemon. They must be reachable from the desktop renderer — a browser
// context that always sends an Origin header — so they are NOT gated by
// localControlRequest (which rejects any Origin-bearing request and is meant for
// the CLI). The "phone must never toggle its own access" invariant is enforced
// on the LAN listener instead, by lanControlBlock, which 404s /api/v1/mobile on
// the 0.0.0.0 socket the phone reaches — a transport-based check that cannot be
// spoofed with a forged Host header. On the loopback listener these routes are
// protected by the same CORS allowlist as every other app route.
func mountMobile(r chi.Router, c *controllers.MobileController) {
	if c == nil {
		return
	}
	r.Get("/api/v1/mobile/status", c.Status)
	r.Post("/api/v1/mobile/enable", c.Enable)
	r.Post("/api/v1/mobile/disable", c.Disable)
	r.Post("/api/v1/mobile/regenerate", c.Regenerate)
}

type cliInvokedRequest struct {
	Command     string `json:"command"`
	CommandPath string `json:"commandPath"`
	ActorType   string `json:"actorType"`
}

type cliUsageErrorRequest struct {
	Command     string `json:"command"`
	CommandPath string `json:"commandPath"`
	Error       string `json:"error"`
}

func mountTelemetry(r chi.Router, cfg config.Config, sink ports.EventSink) {
	if sink == nil {
		return
	}
	// CLI telemetry is capped to bounded uniques: ao.app.active once per UTC
	// six-hour slot for user-context CLI activity (matching the renderer
	// heartbeat) and ao.cli.invoked once per actor type + command path per UTC
	// day. Scripts and agent sessions invoke read-only commands (status, ls,
	// get) in polling loops, so raw invocation counts measure automation, not
	// usage; bounded uniques keep the "which commands, how many users" signal
	// without the firehose. The reservation state is persisted under DataDir so
	// daemon restarts cannot turn polling loops back into raw event volume.
	cliTelemetry := newCLITelemetryReservoir(cfg.DataDir)
	r.Post("/internal/telemetry/cli-invoked", func(w http.ResponseWriter, req *http.Request) {
		if !localControlRequest(req) {
			envelope.WriteJSON(w, http.StatusForbidden, map[string]any{
				"status":  "forbidden",
				"service": daemonmeta.ServiceName,
			})
			return
		}

		var body cliInvokedRequest
		dec := json.NewDecoder(req.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			envelope.WriteAPIError(w, req, http.StatusBadRequest, "bad_request", "INVALID_JSON", "request body must be valid JSON", nil)
			return
		}
		if body.CommandPath == "" {
			envelope.WriteAPIError(w, req, http.StatusBadRequest, "bad_request", "COMMAND_PATH_REQUIRED", "commandPath is required", nil)
			return
		}
		actorType := cliActorType(body.ActorType, body.CommandPath)
		if actorType == "system" {
			w.WriteHeader(http.StatusAccepted)
			return
		}

		if now := time.Now(); cliTelemetry.reserveInvoked(now, actorType, body.CommandPath) {
			sink.Emit(req.Context(), ports.TelemetryEvent{
				Name:       "ao.cli.invoked",
				Source:     "cli",
				OccurredAt: now.UTC(),
				Level:      ports.TelemetryLevelInfo,
				RequestID:  middleware.GetReqID(req.Context()),
				Payload: map[string]any{
					"command":      body.Command,
					"command_path": body.CommandPath,
					"actor_type":   actorType,
				},
			})
		}
		if actorType == "user" {
			if now := time.Now(); cliTelemetry.reserveActive(now) {
				sink.Emit(req.Context(), ports.TelemetryEvent{
					Name:       "ao.app.active",
					Source:     "cli",
					OccurredAt: now.UTC(),
					Level:      ports.TelemetryLevelInfo,
					RequestID:  middleware.GetReqID(req.Context()),
					Payload: map[string]any{
						"channel":      "cli",
						"command":      body.Command,
						"command_path": body.CommandPath,
						"actor_type":   actorType,
					},
				})
			}
		}
		w.WriteHeader(http.StatusAccepted)
	})
	r.Post("/internal/telemetry/cli-usage-error", func(w http.ResponseWriter, req *http.Request) {
		if !localControlRequest(req) {
			envelope.WriteJSON(w, http.StatusForbidden, map[string]any{
				"status":  "forbidden",
				"service": daemonmeta.ServiceName,
			})
			return
		}

		var body cliUsageErrorRequest
		dec := json.NewDecoder(req.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			envelope.WriteAPIError(w, req, http.StatusBadRequest, "bad_request", "INVALID_JSON", "request body must be valid JSON", nil)
			return
		}
		if body.CommandPath == "" {
			envelope.WriteAPIError(w, req, http.StatusBadRequest, "bad_request", "COMMAND_PATH_REQUIRED", "commandPath is required", nil)
			return
		}

		sink.Emit(req.Context(), ports.TelemetryEvent{
			Name:       "ao.cli.usage_errors",
			Source:     "cli",
			OccurredAt: time.Now().UTC(),
			Level:      ports.TelemetryLevelWarn,
			RequestID:  middleware.GetReqID(req.Context()),
			Payload: map[string]any{
				"component":    "cli",
				"operation":    "command_parse",
				"command":      body.Command,
				"command_path": body.CommandPath,
				"error_kind":   "usage",
				"fingerprint":  telemetrymeta.Fingerprint("cli", "command_parse", body.CommandPath, "usage"),
			},
		})
		w.WriteHeader(http.StatusAccepted)
	})
}

func cliActorType(actorType, commandPath string) string {
	switch actorType {
	case "agent", "user":
		return actorType
	case "system":
		return "system"
	}
	switch commandPath {
	case "ao hooks":
		return "agent"
	case "ao daemon", "ao start", "ao completion", "ao help", "ao pty-host":
		return "system"
	default:
		return "user"
	}
}

// localControlRequest reports whether a control request is a trusted local
// caller. The Go CLI client addresses the daemon by its loopback host and
// never sets an Origin header; a cross-site browser fetch always carries an
// Origin, and a DNS-rebinding attempt resolves a non-loopback Host. Rejecting
// either closes the CSRF/rebinding vector while leaving the CLI unaffected.
func localControlRequest(r *http.Request) bool {
	if r.Header.Get("Origin") != "" {
		return false
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	switch host {
	case "127.0.0.1", "::1", "localhost":
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// daemonProbePayload is shared by /healthz and /readyz. Dependency
// initialization happens before the server is constructed, so a listening
// daemon is ready to answer requests.
func daemonProbePayload(status string, cfg config.Config, managed *managedcontrol.Runtime) map[string]any {
	attestation := daemonmeta.Current()
	generation := ""
	if managed != nil {
		attestation = managed.Attestation()
		generation = managed.Generation()
	}
	payload := map[string]any{
		"status":      status,
		"service":     daemonmeta.ServiceName,
		"pid":         os.Getpid(),
		"attestation": attestation,
	}
	if generation != "" {
		payload["generation"] = generation
	}
	if exe, err := os.Executable(); err == nil && exe != "" {
		payload["executablePath"] = exe
	}
	if cwd, err := os.Getwd(); err == nil && cwd != "" {
		payload["workingDirectory"] = cwd
	}
	if cfg.StartupWorkingDirectory != "" {
		payload["startupWorkingDirectory"] = cfg.StartupWorkingDirectory
	}
	return payload
}

func daemonProbeHandler(status string, cfg config.Config, managed *managedcontrol.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeProbeJSON(w, r, http.StatusOK, daemonProbePayload(status, cfg, managed))
	}
}

func writeProbeJSON(w http.ResponseWriter, r *http.Request, status int, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		envelope.WriteJSON(w, status, payload)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(body)
}

type transportScopeKey struct{}

const (
	daemonGenerationHeader = "X-AO-Daemon-Generation"
	transportScopeLAN      = "lan"
)

var (
	strictBearerHeaderPattern = regexp.MustCompile(`^Bearer ([0-9a-f]{64})$`)
	strictGenerationPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

func primaryLoopbackAuthMiddleware(managed *managedcontrol.Runtime) func(http.Handler) http.Handler {
	if managed == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if transportScope(r) == transportScopeLAN || isPublicManagedProbe(r) {
				next.ServeHTTP(w, r)
				return
			}
			token, ok := strictBearerToken(r)
			if !ok || hasAlternateCredentialCarrier(r) || !managed.MatchesBearerHex(token) {
				envelope.WriteAPIError(w, r, http.StatusUnauthorized, "unauthorized", "BAD_BEARER",
					"missing or invalid daemon bearer credential", nil)
				return
			}
			generation, ok := strictSingleHeader(r, daemonGenerationHeader, validCanonicalGeneration)
			if !ok || !managed.MatchesGeneration(generation) {
				envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "STALE_DAEMON_CONTROL_GENERATION",
					"daemon generation mismatch; refresh daemon identity", nil)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func isPublicManagedProbe(r *http.Request) bool {
	if r == nil {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	if r.URL == nil || r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery {
		return false
	}
	switch r.URL.Path {
	case "/healthz", "/readyz":
		return true
	default:
		return false
	}
}

func strictBearerToken(r *http.Request) (string, bool) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", false
	}
	match := strictBearerHeaderPattern.FindStringSubmatch(strings.TrimSpace(values[0]))
	if len(match) != 2 {
		return "", false
	}
	return match[1], true
}

func strictSingleHeader(r *http.Request, key string, validator func(string) bool) (string, bool) {
	values := r.Header.Values(key)
	if len(values) != 1 {
		return "", false
	}
	value := strings.TrimSpace(values[0])
	if validator != nil && !validator(value) {
		return "", false
	}
	return value, true
}

func validCanonicalGeneration(value string) bool {
	return value != "" && strictGenerationPattern.MatchString(value)
}

func hasAlternateCredentialCarrier(r *http.Request) bool {
	for key := range r.URL.Query() {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "access_token", "token", "authorization", "bearer":
			return true
		}
	}
	for _, key := range []string{"Proxy-Authorization", "X-Authorization", "X-AO-Authorization"} {
		if len(r.Header.Values(key)) > 0 {
			return true
		}
	}
	for _, cookie := range r.Cookies() {
		name := strings.ToLower(strings.TrimSpace(cookie.Name))
		if name == "authorization" || name == "access_token" || name == "token" || name == "bearer" || name == strings.ToLower(authCookieName) {
			return true
		}
	}
	return false
}

func transportScope(r *http.Request) string {
	if r == nil {
		return ""
	}
	scope, _ := r.Context().Value(transportScopeKey{}).(string)
	return scope
}

func markTransportScope(scope string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if transportScope(r) == scope {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, transportScopeKey{}, scope)))
	})
}
