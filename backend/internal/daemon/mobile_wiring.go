package daemon

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/aoagents/agent-orchestrator/backend/internal/mobilebridge"
)

type mobileWiring struct {
	controller *controllers.MobileController
	bridge     *controllers.BridgeService
	configPath string
}

type mobileLANFactory func(http.Handler, int, *slog.Logger) *httpd.LANManager

// newMobileWiring makes managed mode structurally incapable of reaching the
// Connect Mobile persistence and listener implementation. Standalone mode keeps
// the existing BridgeService and late-binding cycle.
func newMobileWiring(dataDir string, managed bool) mobileWiring {
	if managed {
		return mobileWiring{
			controller: &controllers.MobileController{Bridge: &controllers.ManagedDisabledMobileBridge{}},
		}
	}

	configPath := mobilebridge.Path(dataDir)
	bridge := &controllers.BridgeService{
		ConfigPath:  configPath,
		DefaultPort: mobilebridge.DefaultPort,
	}
	return mobileWiring{
		controller: &controllers.MobileController{Bridge: bridge},
		bridge:     bridge,
		configPath: configPath,
	}
}

// attach constructs and restores the ordinary Connect Mobile LAN listener.
// The bridge nil-check is the managed-mode security boundary: it occurs before
// invoking the constructor or reading persisted mobile state.
func (w mobileWiring) attach(handler http.Handler, log *slog.Logger, newLAN mobileLANFactory) (*httpd.LANManager, error) {
	if w.bridge == nil {
		return nil, nil
	}
	if newLAN == nil {
		return nil, fmt.Errorf("construct mobile LAN listener: factory is required")
	}

	lan := newLAN(handler, mobilebridge.DefaultPort, log)
	if lan == nil {
		return nil, fmt.Errorf("construct mobile LAN listener: factory returned nil")
	}
	w.bridge.LAN = lan
	if err := restoreMobileOnBoot(w.configPath, lan); err != nil {
		return lan, err
	}
	return lan, nil
}
