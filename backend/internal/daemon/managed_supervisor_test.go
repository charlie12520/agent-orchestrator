package daemon

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
)

func TestManagedModeDoesNotStartFrontendDeathSupervisor(t *testing.T) {
	listenCalled := false
	startFrontendDeathSupervisor(
		context.Background(),
		"managed/run/running.json",
		true,
		func() { t.Fatal("managed frontend supervisor requested daemon shutdown") },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(string) (net.Listener, string, error) {
			listenCalled = true
			return nil, "", nil
		},
	)
	if listenCalled {
		t.Fatal("managed daemon opened the standalone frontend-death supervisor")
	}
}
