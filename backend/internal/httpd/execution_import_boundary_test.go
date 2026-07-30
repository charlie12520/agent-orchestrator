package httpd

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestExecutionFoundationImportBoundary keeps A2 honest: the HTTP seam may
// depend on A1, but it must not quietly become the real runtime dispatcher.
func TestExecutionFoundationImportBoundary(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file")
	}
	httpdDir := filepath.Dir(currentFile)
	internalDir := filepath.Dir(httpdDir)
	targets := []string{
		filepath.Join(httpdDir, "controllers", "execution.go"),
		filepath.Join(internalDir, "service", "executionapi", "service.go"),
	}
	for _, target := range targets {
		body, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read %s: %v", target, err)
		}
		normalized := strings.ToLower(strings.ReplaceAll(string(body), `\`, "/"))
		for _, forbidden := range []string{"/adapters/", "session_manager", "sessionmanager", "internal/harness", "adapters/runtime"} {
			if strings.Contains(normalized, forbidden) {
				t.Fatalf("%s crosses forbidden execution boundary %q", target, forbidden)
			}
		}
	}

	daemonPath := filepath.Join(internalDir, "daemon", "daemon.go")
	daemonBody, err := os.ReadFile(daemonPath)
	if err != nil {
		t.Fatalf("read daemon wiring: %v", err)
	}
	wiring := string(daemonBody)
	if !strings.Contains(wiring, "Execution: executionapisvc.New(store, nil)") {
		t.Fatal("production execution API must remain wired with a nil mutation executor")
	}
	if strings.Contains(wiring, "executionjournal.New(") {
		t.Fatal("production daemon must not construct the A1 mutation service in A2")
	}
}
