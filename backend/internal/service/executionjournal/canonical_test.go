package executionjournal

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestParseRequestCanonicalIdentityAndGenerationRules(t *testing.T) {
	first := []byte(`{
		"version":1,
		"externalRunId":"external-1",
		"operation":"launch",
		"idempotencyKey":"launch-1",
		"request":{"message":"caf\u00e9","count":1}
	}`)
	alias := []byte(`{"request":{"count":1.0,"message":"café"},"idempotencyKey":"launch-1","operation":"launch","externalRunId":"external-1","version":1e0}`)
	a, err := ParseRequest(first)
	if err != nil {
		t.Fatalf("parse first: %v", err)
	}
	b, err := ParseRequest(alias)
	if err != nil {
		t.Fatalf("parse alias: %v", err)
	}
	if !bytes.Equal(a.CanonicalJSON, b.CanonicalJSON) || a.RequestHash != b.RequestHash || a.OperationID != b.OperationID {
		t.Fatalf("canonical aliases drifted:\n%s\n%s", a.CanonicalJSON, b.CanonicalJSON)
	}
	if a.TargetProcessGeneration != 1 || a.ExpectedProcessGeneration != 0 || a.RunID != "" {
		t.Fatalf("launch generation = %+v", a)
	}

	resume, err := ParseRequest([]byte(`{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"resume","idempotencyKey":"resume-1","expectedProcessGeneration":7,"request":{"message":"continue"}}`))
	if err != nil {
		t.Fatalf("parse resume: %v", err)
	}
	if resume.Operation != domain.ExecutionResume || resume.ExpectedProcessGeneration != 7 || resume.TargetProcessGeneration != 8 {
		t.Fatalf("resume generation = %+v", resume)
	}
	send, err := ParseRequest([]byte(`{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"send","idempotencyKey":"send-1","expectedProcessGeneration":7,"request":{"message":"hello"}}`))
	if err != nil || send.TargetProcessGeneration != 7 {
		t.Fatalf("send generation = %+v err=%v", send, err)
	}
}

func TestParseRequestRejectsAliasesUnknownEnvelopeAndLossyJSON(t *testing.T) {
	basePrefix := `{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":`
	tests := []struct {
		name string
		raw  string
	}{
		{"duplicate envelope key", `{"version":1,"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{}}`},
		{"escaped duplicate payload key", basePrefix + `{"name":1,"n\u0061me":2}}`},
		{"unknown envelope field", `{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{},"ignored":true}`},
		{"non-finite overflow", basePrefix + `{"count":1e400}}`},
		{"unpaired high surrogate", basePrefix + `{"value":"\ud800"}}`},
		{"unpaired low surrogate", basePrefix + `{"value":"\udc00"}}`},
		{"trailing token", basePrefix + `{}} true`},
		{"launch carries run id", `{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"launch","idempotencyKey":"launch-1","request":{}}`},
		{"launch carries generation", `{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","expectedProcessGeneration":1,"request":{}}`},
		{"mutation omits run id", `{"version":1,"externalRunId":"external-1","operation":"send","idempotencyKey":"send-1","expectedProcessGeneration":1,"request":{}}`},
		{"mutation omits generation", `{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"send","idempotencyKey":"send-1","request":{}}`},
		{"fractional generation", `{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"send","idempotencyKey":"send-1","expectedProcessGeneration":1.5,"request":{}}`},
		{"zero generation", `{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"send","idempotencyKey":"send-1","expectedProcessGeneration":0,"request":{}}`},
		{"unsafe generation", `{"version":1,"externalRunId":"external-1","runId":"ao-run-1","operation":"send","idempotencyKey":"send-1","expectedProcessGeneration":9007199254740992,"request":{}}`},
		{"payload not object", `{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseRequest([]byte(test.raw)); operationErrorCode(err) != CodeInvalidRequest {
				t.Fatalf("ParseRequest error = %v, want invalid_request", err)
			}
		})
	}
}

func TestParseRequestPreservesOpaqueFieldsAndUnicodeValuesInIdentity(t *testing.T) {
	composed, err := ParseRequest([]byte(`{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{"prompt":"é","naïve":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	decomposed, err := ParseRequest([]byte(`{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{"prompt":"e\u0301","naïve":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	withoutUnknown, err := ParseRequest([]byte(`{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{"prompt":"é"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if composed.RequestHash == decomposed.RequestHash {
		t.Fatal("distinct Unicode scalar sequences aliased to one request")
	}
	if composed.RequestHash == withoutUnknown.RequestHash {
		t.Fatal("opaque payload field was ignored instead of entering request identity")
	}
}

func TestParseRequestMatchesECMAScriptNumberAndPropertyCanonicalization(t *testing.T) {
	first, err := ParseRequest([]byte(`{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{"10":10,"2":2,"small":0.000001,"tiny":0.0000001,"huge":1e21,"zero":-0,"rounded":9007199254740993}}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"externalRunId":"external-1","idempotencyKey":"launch-1","operation":"launch","request":{"2":2,"10":10,"huge":1e+21,"rounded":9007199254740992,"small":0.000001,"tiny":1e-7,"zero":0},"version":1}`
	if string(first.CanonicalJSON) != want {
		t.Fatalf("canonical JSON = %s\nwant = %s", first.CanonicalJSON, want)
	}

	alias, err := ParseRequest([]byte(`{"idempotencyKey":"launch-1","externalRunId":"external-1","version":1.0,"operation":"launch","request":{"rounded":9007199254740992,"zero":0,"huge":1000000000000000000000,"tiny":1e-7,"small":1e-6,"2":2.0,"10":1e1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.RequestHash != alias.RequestHash {
		t.Fatalf("ECMAScript numeric aliases drifted:\n%s\n%s", first.CanonicalJSON, alias.CanonicalJSON)
	}
}

func TestParseRequestRejectsLoneSurrogatesWithoutAliasingReplacementRune(t *testing.T) {
	prefix := `{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{"value":`
	for _, raw := range []string{
		prefix + `"\ud800"}}`,
		prefix + `"\udc00"}}`,
		prefix + `"\udc00\ud800"}}`,
		prefix + `"\ud800\u0041"}}`,
		prefix + `"\ud800\ud800"}}`,
	} {
		if _, err := ParseRequest([]byte(raw)); operationErrorCode(err) != CodeInvalidRequest {
			t.Fatalf("surrogate input %q error = %v, want invalid_request", raw, err)
		}
	}

	pair, err := ParseRequest([]byte(prefix + `"\ud83d\ude80"}}`))
	if err != nil {
		t.Fatalf("valid surrogate pair: %v", err)
	}
	literal, err := ParseRequest([]byte(prefix + `"🚀"}}`))
	if err != nil {
		t.Fatalf("literal astral rune: %v", err)
	}
	if pair.RequestHash != literal.RequestHash {
		t.Fatalf("valid astral aliases drifted:\n%s\n%s", pair.CanonicalJSON, literal.CanonicalJSON)
	}
	replacement, err := ParseRequest([]byte(prefix + `"\ufffd"}}`))
	if err != nil {
		t.Fatalf("replacement rune: %v", err)
	}
	if pair.RequestHash == replacement.RequestHash {
		t.Fatal("valid astral pair aliased the replacement rune")
	}
}

func TestParseRequestBoundsDepthNodesAndBytes(t *testing.T) {
	prefix := `{"version":1,"externalRunId":"external-1","operation":"launch","idempotencyKey":"launch-1","request":{"value":`
	deep := prefix + strings.Repeat("[", maxCanonicalDepth+2) + "0" + strings.Repeat("]", maxCanonicalDepth+2) + `}}`
	if _, err := ParseRequest([]byte(deep)); operationErrorCode(err) != CodeInvalidRequest {
		t.Fatalf("deep request error = %v", err)
	}

	large := append([]byte(prefix+`"`), bytes.Repeat([]byte{'a'}, maxCanonicalBytes)...)
	large = append(large, []byte(`"}}`)...)
	if _, err := ParseRequest(large); operationErrorCode(err) != CodeInvalidRequest {
		t.Fatalf("large request error = %v", err)
	}

	var fields strings.Builder
	fields.WriteString(prefix[:len(prefix)-len(`"value":`)])
	for i := 0; i <= maxObjectFields; i++ {
		if i != 0 {
			fields.WriteByte(',')
		}
		fmt.Fprintf(&fields, `"f%d":%d`, i, i)
	}
	fields.WriteString(`}}`)
	if _, err := ParseRequest([]byte(fields.String())); operationErrorCode(err) != CodeInvalidRequest {
		t.Fatalf("wide request error = %v", err)
	}

	nodes := prefix + `[` + strings.Repeat("0,", maxCanonicalNodes) + `0]}}`
	if _, err := ParseRequest([]byte(nodes)); operationErrorCode(err) != CodeInvalidRequest {
		t.Fatalf("node-heavy request error = %v", err)
	}
}

func TestCanonicalResultIsStableAndStrict(t *testing.T) {
	a, hashA, err := canonicalizeResult([]byte(`{"z":2,"ok":true,"nested":{"value":"x"}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, hashB, err := canonicalizeResult([]byte(`{"nested":{"value":"x"},"ok":true,"z":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) || hashA != hashB {
		t.Fatalf("result canonicalization drifted: %s vs %s", a, b)
	}
	if _, _, err := canonicalizeResult([]byte(`{"ok":true,"ok":false}`)); operationErrorCode(err) != CodeInvalidRequest {
		t.Fatalf("duplicate result error = %v", err)
	}
}

func operationErrorCode(err error) ErrorCode {
	code, _, _ := ErrorInfo(err)
	return code
}
