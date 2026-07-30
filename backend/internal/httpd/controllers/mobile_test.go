package controllers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type fakeBridge struct{ enabled bool }

func (f *fakeBridge) Status() MobileStatusResponse {
	return MobileStatusResponse{Enabled: f.enabled, Host: "192.168.1.42", Port: 3011}
}
func (f *fakeBridge) Enable() (MobileStatusResponse, error) {
	f.enabled = true
	r := f.Status()
	r.Password = "abcd1234"
	return r, nil
}
func (f *fakeBridge) Disable() error { f.enabled = false; return nil }
func (f *fakeBridge) Regenerate() (MobileStatusResponse, error) {
	r := f.Status()
	r.Password = "wxyz5678"
	return r, nil
}

// fakeLAN is a minimal LANController for exercising BridgeService directly.
type fakeLAN struct {
	running   bool
	hash      string
	stopCalls int
}

func (f *fakeLAN) Start(port int) (int, error) { f.running = true; return port, nil }
func (f *fakeLAN) Stop(ctx context.Context) error {
	f.stopCalls++
	f.running = false
	return nil
}
func (f *fakeLAN) Running() bool            { return f.running }
func (f *fakeLAN) BoundPort() int           { return 3011 }
func (f *fakeLAN) SetPasswordHash(h string) { f.hash = h }
func (f *fakeLAN) PasswordHash() string     { return f.hash }

// When Save fails during a fresh enable, the listener that Start already opened
// must be torn back down and the armed hash rolled back — otherwise a LAN
// listener stays live on 0.0.0.0 while persisted state/UI say enable failed.
func TestMobileEnableRollsBackListenerWhenSaveFails(t *testing.T) {
	// A ConfigPath whose parent is a regular file makes mobilebridge.Save's
	// MkdirAll (and thus Save) fail deterministically.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	lan := &fakeLAN{}
	b := &BridgeService{LAN: lan, ConfigPath: filepath.Join(blocker, "mobile", "config.json"), DefaultPort: 3011}

	if _, err := b.Enable(); err == nil {
		t.Fatal("expected enable to fail on Save error")
	}
	if lan.Running() {
		t.Fatal("listener still running after failed enable; must be stopped")
	}
	if lan.stopCalls == 0 {
		t.Fatal("expected Stop to be called on rollback")
	}
	if lan.hash != "" {
		t.Fatalf("expected hash rolled back to empty, got %q", lan.hash)
	}
}

func TestMobileEnableReturnsPassword(t *testing.T) {
	c := &MobileController{Bridge: &fakeBridge{}}
	w := httptest.NewRecorder()
	c.Enable(w, httptest.NewRequest(http.MethodPost, "/api/v1/mobile/enable", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("got %d", w.Code)
	}
	var got MobileStatusResponse
	json.NewDecoder(w.Body).Decode(&got)
	if !got.Enabled || got.Password != "abcd1234" || got.Warning == "" {
		t.Fatalf("bad response: %+v", got)
	}
}

func TestManagedDisabledMobileBridgeFailsClosed(t *testing.T) {
	c := &MobileController{Bridge: &ManagedDisabledMobileBridge{}}

	status := httptest.NewRecorder()
	c.Status(status, httptest.NewRequest(http.MethodGet, "/api/v1/mobile/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", status.Code)
	}
	var gotStatus MobileStatusResponse
	if err := json.NewDecoder(status.Body).Decode(&gotStatus); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if gotStatus.Enabled || gotStatus.Host != "" || gotStatus.Port != 0 || gotStatus.Password != "" {
		t.Fatalf("managed mobile status exposed active connection state: %+v", gotStatus)
	}

	tests := []struct {
		name   string
		path   string
		invoke func(http.ResponseWriter, *http.Request)
	}{
		{name: "enable", path: "/api/v1/mobile/enable", invoke: c.Enable},
		{name: "disable", path: "/api/v1/mobile/disable", invoke: c.Disable},
		{name: "regenerate", path: "/api/v1/mobile/regenerate", invoke: c.Regenerate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.invoke(w, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if w.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", w.Code)
			}
			var got struct {
				Error   string `json:"error"`
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if got.Error != "forbidden" || got.Code != "MOBILE_DISABLED_MANAGED" || got.Message != managedMobileDisabledMessage {
				t.Fatalf("error = %+v", got)
			}
		})
	}
}
