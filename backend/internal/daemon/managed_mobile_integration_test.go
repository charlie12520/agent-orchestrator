package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/mobilebridge"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

const (
	managedMobileHelperEnv = "AO_TEST_MANAGED_MOBILE_HELPER"
	managedTestSecretHex   = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	managedTestGeneration  = "managed-mobile-test"
)

// TestManagedDaemonMobileHelper is entered only by
// TestManagedDaemonDoesNotRestoreOrStartMobileLAN's child process. Stdin is an
// inherited anonymous pipe, matching the real supervisor bootstrap boundary.
func TestManagedDaemonMobileHelper(t *testing.T) {
	if os.Getenv(managedMobileHelperEnv) != "1" {
		return
	}
	if err := RunWithOptions(RunOptions{ManagedBootstrap: os.Stdin}); err != nil {
		t.Fatalf("managed daemon: %v", err)
	}
}

func TestManagedDaemonDoesNotRestoreOrStartMobileLAN(t *testing.T) {
	dataDir := t.TempDir()
	runFilePath := filepath.Join(dataDir, "running.json")
	primaryPort := unusedTCPPort(t)
	mobilePort := unusedTCPPort(t)
	for mobilePort == primaryPort {
		mobilePort = unusedTCPPort(t)
	}

	mobileConfigPath := mobilebridge.Path(dataDir)
	if err := mobilebridge.Save(mobileConfigPath, mobilebridge.State{
		Enabled:  true,
		Password: "persisted-password",
		LastPort: mobilePort,
	}); err != nil {
		t.Fatalf("save enabled mobile state: %v", err)
	}
	before, err := os.ReadFile(mobileConfigPath)
	if err != nil {
		t.Fatalf("read enabled mobile state: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestManagedDaemonMobileHelper$")
	cmd.Env = managedDaemonTestEnv(os.Environ(), dataDir, runFilePath, primaryPort)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("create bootstrap pipe: %v", err)
	}
	var output lockedBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start managed daemon helper: %v", err)
	}
	processDone := make(chan error, 1)
	waitCompleted := make(chan struct{})
	go func() {
		processDone <- cmd.Wait()
		close(waitCompleted)
	}()
	stopped := false
	t.Cleanup(func() {
		_ = stdin.Close()
		if stopped {
			return
		}
		select {
		case <-waitCompleted:
			return
		default:
		}
		_ = cmd.Process.Kill()
		select {
		case <-processDone:
		case <-time.After(5 * time.Second):
		}
	})

	bootstrap := fmt.Sprintf(`{"version":1,"rootSecretHex":"%s","generation":"%s"}`, managedTestSecretHex, managedTestGeneration)
	if _, err := io.WriteString(stdin, bootstrap); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatalf("close bootstrap pipe: %v", err)
	}

	info := waitForManagedDaemon(t, runFilePath, processDone, &output)
	if info.Attestation == nil || !info.Attestation.Capabilities["managedMobileLANDisabled"] {
		t.Fatalf("runfile attestation did not prove managed mobile LAN disablement: %+v", info.Attestation)
	}
	baseURL := "http://127.0.0.1:" + strconv.Itoa(info.Port)
	client := &http.Client{Timeout: 2 * time.Second}

	statusResp := doManagedRequest(t, client, http.MethodGet, baseURL+"/api/v1/mobile/status")
	defer statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(statusResp.Body)
		t.Fatalf("mobile status = %d: %s", statusResp.StatusCode, body)
	}
	statusBody, err := io.ReadAll(statusResp.Body)
	if err != nil {
		t.Fatalf("read mobile status: %v", err)
	}
	if !bytes.Contains(statusBody, []byte(`"enabled":false`)) || bytes.Contains(statusBody, []byte("persisted-password")) {
		t.Fatalf("managed mobile status exposed persisted bridge state: %s", statusBody)
	}

	for _, path := range []string{"/api/v1/mobile/enable", "/api/v1/mobile/regenerate", "/api/v1/mobile/disable"} {
		resp := doManagedRequest(t, client, http.MethodPost, baseURL+path)
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read %s response: %v", path, readErr)
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s status = %d, want 403: %s", path, resp.StatusCode, body)
		}
		var apiErr envelope.APIError
		if err := json.Unmarshal(body, &apiErr); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
		if apiErr.Code != "MOBILE_DISABLED_MANAGED" {
			t.Fatalf("%s code = %q, want MOBILE_DISABLED_MANAGED", path, apiErr.Code)
		}
	}

	conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(mobilePort)), 300*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		t.Fatalf("persisted Connect Mobile port %d accepted a connection in managed mode", mobilePort)
	}
	after, err := os.ReadFile(mobileConfigPath)
	if err != nil {
		t.Fatalf("read mobile state after managed controls: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("managed daemon mutated persisted mobile state\nbefore: %s\nafter:  %s", before, after)
	}

	shutdownResp := doManagedRequest(t, client, http.MethodPost, baseURL+"/shutdown")
	shutdownBody, _ := io.ReadAll(shutdownResp.Body)
	_ = shutdownResp.Body.Close()
	if shutdownResp.StatusCode != http.StatusAccepted {
		t.Fatalf("shutdown status = %d: %s", shutdownResp.StatusCode, shutdownBody)
	}
	select {
	case err := <-processDone:
		stopped = true
		if err != nil {
			t.Fatalf("managed daemon helper exit: %v\n%s", err, output.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("managed daemon did not stop\n%s", output.String())
	}
}

func unusedTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("release TCP port: %v", err)
	}
	return port
}

func managedDaemonTestEnv(base []string, dataDir, runFilePath string, port int) []string {
	env := make([]string, 0, len(base)+5)
	for _, item := range base {
		name, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(strings.ToUpper(name), "AO_") {
			continue
		}
		env = append(env, item)
	}
	return append(env,
		managedMobileHelperEnv+"=1",
		"AO_DATA_DIR="+dataDir,
		"AO_RUN_FILE="+runFilePath,
		"AO_PORT="+strconv.Itoa(port),
		"AO_AGENT=claude-code",
	)
}

func waitForManagedDaemon(t *testing.T, runFilePath string, processDone <-chan error, output *lockedBuffer) *runfile.Info {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-processDone:
			t.Fatalf("managed daemon exited before ready: %v\n%s", err, output.String())
		default:
		}
		info, err := runfile.Read(runFilePath)
		if err == nil && info != nil && info.Port > 0 {
			client := &http.Client{Timeout: 500 * time.Millisecond}
			resp, probeErr := client.Get("http://127.0.0.1:" + strconv.Itoa(info.Port) + "/readyz")
			if probeErr == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return info
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("managed daemon did not become ready\n%s", output.String())
	return nil
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func doManagedRequest(t *testing.T, client *http.Client, method, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("new managed request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+managedTestSecretHex)
	req.Header.Set("X-AO-Daemon-Generation", managedTestGeneration)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("managed request %s %s: %v", method, url, err)
	}
	return resp
}
