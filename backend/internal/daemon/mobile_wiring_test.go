package daemon

import (
	"bytes"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/mobilebridge"
)

func TestManagedMobileWiringSkipsConstructorAndPersistedRestore(t *testing.T) {
	dataDir := t.TempDir()
	configPath := mobilebridge.Path(dataDir)
	if err := mobilebridge.Save(configPath, mobilebridge.State{
		Enabled:  true,
		Password: "persisted-password",
		LastPort: 45123,
	}); err != nil {
		t.Fatalf("save enabled mobile state: %v", err)
	}
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read enabled mobile state: %v", err)
	}

	wiring := newMobileWiring(dataDir, true)
	if wiring.bridge != nil || wiring.configPath != "" {
		t.Fatalf("managed wiring retained persistence/listener bridge: %+v", wiring)
	}
	constructorCalls := 0
	lan, err := wiring.attach(http.NotFoundHandler(), slog.Default(), func(http.Handler, int, *slog.Logger) *httpd.LANManager {
		constructorCalls++
		return nil
	})
	if err != nil {
		t.Fatalf("attach managed mobile: %v", err)
	}
	if lan != nil {
		t.Fatal("managed wiring returned a LAN manager")
	}
	if constructorCalls != 0 {
		t.Fatalf("LAN constructor calls = %d, want 0", constructorCalls)
	}
	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read mobile state after managed attach: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("managed attach mutated persisted mobile state\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestStandaloneMobileWiringKeepsOrdinaryBridge(t *testing.T) {
	wiring := newMobileWiring(t.TempDir(), false)
	if wiring.bridge == nil || wiring.configPath == "" {
		t.Fatalf("standalone wiring omitted ordinary bridge: %+v", wiring)
	}
	constructorCalls := 0
	lan, err := wiring.attach(http.NotFoundHandler(), slog.Default(), func(handler http.Handler, port int, log *slog.Logger) *httpd.LANManager {
		constructorCalls++
		return httpd.NewMobileLAN(handler, port, log)
	})
	if err != nil {
		t.Fatalf("attach standalone mobile: %v", err)
	}
	if lan == nil || constructorCalls != 1 {
		t.Fatalf("standalone attach = lan %v constructor calls %d", lan, constructorCalls)
	}
}
