package managedcontrol

import (
	"os"
	"strings"
	"testing"
)

const testSecretHex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

func TestLoadAcceptsManagedBootstrap(t *testing.T) {
	runtime, err := Load(strings.NewReader(`{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":"gen-123"}`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer runtime.Close()
	if runtime.Generation() != "gen-123" {
		t.Fatalf("generation = %q", runtime.Generation())
	}
	if !runtime.MatchesBearerHex(testSecretHex) {
		t.Fatal("expected bearer match")
	}
	if !runtime.MatchesGeneration("gen-123") {
		t.Fatal("expected generation match")
	}
	if runtime.MatchesBearerHex(strings.ToUpper(testSecretHex)) {
		t.Fatal("uppercase bearer matched")
	}
	if runtime.MatchesBearerHex(testSecretHex + " ") {
		t.Fatal("whitespace-normalized bearer matched")
	}
	if !runtime.Attestation().Capabilities["authenticatedIpc"] || !runtime.Attestation().Capabilities["daemonControlGeneration"] {
		t.Fatalf("managed attestation = %+v", runtime.Attestation())
	}
}

func TestLoadRejectsBootstrapFailures(t *testing.T) {
	oversized := `{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":"` + strings.Repeat("a", MaxBootstrapBytes) + `"}`
	cases := []struct {
		name string
		body string
	}{
		{name: "missing", body: ``},
		{name: "partial", body: `{"version":1`},
		{name: "trailing", body: `{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}x`},
		{name: "unknown field", body: `{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1","extra":true}`},
		{name: "duplicate version", body: `{"version":1,"version":2,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "string version", body: `{"version":"1","rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "float version", body: `{"version":1.0,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "exponent version", body: `{"version":1e0,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "negative version", body: `{"version":-1,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "wrong version number", body: `{"version":2,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "duplicate secret", body: `{"version":1,"rootSecretHex":"` + testSecretHex + `","rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "duplicate generation", body: `{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":"gen-1","generation":"gen-2"}`},
		{name: "missing version", body: `{"rootSecretHex":"` + testSecretHex + `","generation":"gen-1"}`},
		{name: "short secret", body: `{"version":1,"rootSecretHex":"00","generation":"gen-1"}`},
		{name: "uppercase secret", body: `{"version":1,"rootSecretHex":"` + strings.ToUpper(testSecretHex) + `","generation":"gen-1"}`},
		{name: "secret whitespace", body: `{"version":1,"rootSecretHex":"` + testSecretHex + ` ","generation":"gen-1"}`},
		{name: "bad generation", body: `{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":" bad "}`},
		{name: "oversized", body: oversized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime, err := Load(strings.NewReader(tc.body))
			if err == nil {
				runtime.Close()
				t.Fatal("Load succeeded, want error")
			}
		})
	}
}

func TestLoadProcessBootstrapRequiresPipe(t *testing.T) {
	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer pipeReader.Close()
	go func() {
		_, _ = pipeWriter.WriteString(`{"version":1,"rootSecretHex":"` + testSecretHex + `","generation":"gen-123"}`)
		_ = pipeWriter.Close()
	}()
	runtime, err := LoadProcessBootstrap(pipeReader)
	if err != nil {
		t.Fatalf("LoadProcessBootstrap(pipe): %v", err)
	}
	runtime.Close()

	filePath := t.TempDir() + `\bootstrap.json`
	if err := os.WriteFile(filePath, []byte(`{"version":1,"rootSecretHex":"`+testSecretHex+`","generation":"gen-123"}`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	if _, err := LoadProcessBootstrap(file); err == nil {
		t.Fatal("LoadProcessBootstrap(file) succeeded, want error")
	}
}
