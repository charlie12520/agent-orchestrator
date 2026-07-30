package managedcontrol

import (
	"bytes"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
)

const (
	BootstrapEnvelopeVersion = 1
	MaxBootstrapBytes        = 4096
)

var generationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var lowerHex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type bootstrapEnvelope struct {
	Version       int    `json:"version"`
	RootSecretHex string `json:"rootSecretHex"`
	Generation    string `json:"generation"`
}

// Runtime is the managed SuperOrch control boundary for one daemon process.
// The secret remains memory-only; the generation is non-secret and may be
// surfaced to callers so they can detect daemon restarts.
type Runtime struct {
	secret      [32]byte
	generation  string
	attestation daemonmeta.Attestation
}

func Load(reader io.Reader) (*Runtime, error) {
	if reader == nil {
		return nil, fmt.Errorf("bootstrap missing")
	}

	data, err := io.ReadAll(io.LimitReader(reader, MaxBootstrapBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read bootstrap: %w", err)
	}
	defer zeroBytes(data)
	if len(data) == 0 {
		return nil, fmt.Errorf("bootstrap missing")
	}
	if len(data) > MaxBootstrapBytes {
		return nil, fmt.Errorf("bootstrap exceeds %d bytes", MaxBootstrapBytes)
	}

	envelope, err := parseBootstrapEnvelope(data)
	if err != nil {
		return nil, fmt.Errorf("parse bootstrap: %w", err)
	}
	if envelope.Version != BootstrapEnvelopeVersion {
		return nil, fmt.Errorf("unsupported bootstrap version %d", envelope.Version)
	}
	generation := envelope.Generation
	if !generationPattern.MatchString(generation) {
		return nil, fmt.Errorf("invalid daemon generation")
	}
	if !lowerHex64Pattern.MatchString(envelope.RootSecretHex) {
		return nil, fmt.Errorf("root secret must be exactly 64 lowercase hex characters")
	}
	rawSecret, err := hex.DecodeString(envelope.RootSecretHex)
	if err != nil {
		return nil, fmt.Errorf("invalid root secret encoding")
	}
	defer zeroBytes(rawSecret)
	if len(rawSecret) != 32 {
		return nil, fmt.Errorf("root secret must be 32 bytes")
	}

	runtime := &Runtime{
		generation:  generation,
		attestation: daemonmeta.WithManagedControl(daemonmeta.Current()),
	}
	copy(runtime.secret[:], rawSecret)
	return runtime, nil
}

func LoadProcessBootstrap(reader io.Reader) (*Runtime, error) {
	file, ok := reader.(*os.File)
	if !ok {
		return nil, fmt.Errorf("bootstrap must come from an inherited pipe")
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat bootstrap pipe: %w", err)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		if info.Mode()&os.ModeCharDevice != 0 {
			return nil, fmt.Errorf("bootstrap must come from a pipe, not an interactive terminal")
		}
		return nil, fmt.Errorf("bootstrap must come from an inherited pipe")
	}
	return Load(reader)
}

func parseBootstrapEnvelope(data []byte) (bootstrapEnvelope, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return bootstrapEnvelope{}, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return bootstrapEnvelope{}, fmt.Errorf("bootstrap must be a JSON object")
	}

	var envelope bootstrapEnvelope
	seen := make(map[string]struct{}, 3)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return bootstrapEnvelope{}, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return bootstrapEnvelope{}, fmt.Errorf("bootstrap object key must be a string")
		}
		if _, exists := seen[key]; exists {
			return bootstrapEnvelope{}, fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = struct{}{}

		switch key {
		case "version":
			valueTok, err := dec.Token()
			if err != nil {
				return bootstrapEnvelope{}, err
			}
			value, ok := valueTok.(json.Number)
			if !ok {
				return bootstrapEnvelope{}, fmt.Errorf("version must be the canonical JSON number 1")
			}
			if value.String() != "1" {
				return bootstrapEnvelope{}, fmt.Errorf("version must be the canonical JSON number 1")
			}
			envelope.Version = BootstrapEnvelopeVersion
		case "rootSecretHex":
			if err := dec.Decode(&envelope.RootSecretHex); err != nil {
				return bootstrapEnvelope{}, err
			}
		case "generation":
			if err := dec.Decode(&envelope.Generation); err != nil {
				return bootstrapEnvelope{}, err
			}
		default:
			return bootstrapEnvelope{}, fmt.Errorf("unknown field %q", key)
		}
	}

	tok, err = dec.Token()
	if err != nil {
		return bootstrapEnvelope{}, err
	}
	delim, ok = tok.(json.Delim)
	if !ok || delim != '}' {
		return bootstrapEnvelope{}, fmt.Errorf("bootstrap must end with an object")
	}
	if err := consumeEOF(dec); err != nil {
		return bootstrapEnvelope{}, err
	}

	for _, field := range []string{"version", "rootSecretHex", "generation"} {
		if _, ok := seen[field]; !ok {
			return bootstrapEnvelope{}, fmt.Errorf("missing field %q", field)
		}
	}
	return envelope, nil
}

func consumeEOF(dec *json.Decoder) error {
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing data")
		}
		return err
	}
	return nil
}

func zeroBytes(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	for i := range r.secret {
		r.secret[i] = 0
	}
}

func (r *Runtime) Generation() string {
	if r == nil {
		return ""
	}
	return r.generation
}

func (r *Runtime) Attestation() daemonmeta.Attestation {
	if r == nil {
		return daemonmeta.Current()
	}
	return r.attestation
}

func (r *Runtime) MatchesBearerHex(token string) bool {
	if r == nil {
		return false
	}
	if !lowerHex64Pattern.MatchString(token) {
		return false
	}
	candidate, err := hex.DecodeString(token)
	if err != nil {
		return false
	}
	defer zeroBytes(candidate)
	if len(candidate) != len(r.secret) {
		return false
	}
	return subtle.ConstantTimeCompare(candidate, r.secret[:]) == 1
}

func (r *Runtime) MatchesGeneration(generation string) bool {
	if r == nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(generation), []byte(r.generation)) == 1
}
