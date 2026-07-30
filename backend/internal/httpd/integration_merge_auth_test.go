package httpd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
)

func TestManagedIntegrationMergeRoutesAuthenticateBeforeUnwiredResponse(t *testing.T) {
	managed := managedRuntimeUnderTest(t)
	if managed.Attestation().Capabilities["prMerge"] {
		t.Fatal("managed control must not advertise prMerge before broker wiring")
	}
	router := newManagedTestRouter(config.Config{}, discardLogger(), nil, managed, APIDeps{}, ControlDeps{})

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "issue", path: "/api/v1/integration/merge-leases"},
		{name: "revoke", path: "/api/v1/integration/merge-leases/iml_abcdefghijklmnop/revoke"},
		{name: "merge", path: "/api/v1/integration/merges"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unauthorized := httptest.NewRecorder()
			router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if unauthorized.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated %s = %d, want 401", tc.path, unauthorized.Code)
			}

			authorized := httptest.NewRecorder()
			router.ServeHTTP(authorized, managedRequest(http.MethodPost, tc.path))
			if authorized.Code != http.StatusNotImplemented {
				t.Fatalf("authenticated unwired %s = %d, want 501", tc.path, authorized.Code)
			}
		})
	}
}
