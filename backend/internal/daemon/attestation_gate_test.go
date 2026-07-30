package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
)

func TestRunRejectsAmbiguousReleaseBeforeDurableMutation(t *testing.T) {
	originalVersion, originalCommit, originalMode := daemonmeta.BuildVersion, daemonmeta.ForkCommit, daemonmeta.BuildMode
	t.Cleanup(func() {
		daemonmeta.BuildVersion, daemonmeta.ForkCommit, daemonmeta.BuildMode = originalVersion, originalCommit, originalMode
	})
	daemonmeta.BuildVersion = "dev"
	daemonmeta.ForkCommit = strings.Repeat("a", 40)
	daemonmeta.BuildMode = "release"

	dataDir := filepath.Join(t.TempDir(), "must-not-exist")
	t.Setenv("AO_DATA_DIR", dataDir)
	if err := Run(); err == nil || !strings.Contains(err.Error(), "validate build identity") {
		t.Fatalf("Run() error = %v, want build identity rejection", err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("data directory was touched before build validation: %v", err)
	}
}
