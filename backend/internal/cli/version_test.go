package cli

import (
	"encoding/json"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
)

func TestVersionJSONEmitsMachineReadableAttestation(t *testing.T) {
	out, _, err := executeCLI(t, Deps{}, "version", "--json")
	if err != nil {
		t.Fatalf("version --json: %v", err)
	}
	var got daemonmeta.Attestation
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode version JSON: %v\n%s", err, out)
	}
	if got.Distribution != daemonmeta.Distribution || got.Upstream.Commit != daemonmeta.UpstreamCommit ||
		got.Protocols.TerminalMux != daemonmeta.TerminalMuxProtocolVersion || !got.Capabilities["buildAttestation"] {
		t.Fatalf("attestation = %+v", got)
	}
}
