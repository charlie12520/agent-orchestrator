// Package executionjournal implements AO's durable, backend-neutral execution
// mutation journal. It has no HTTP, daemon, harness, or process-runtime wiring.
package executionjournal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

const (
	ContractVersion       = 1
	maxCanonicalBytes     = 1 << 20
	maxCanonicalDepth     = 32
	maxCanonicalNodes     = 50_000
	maxObjectFields       = 4_096
	maxStringBytes        = 1 << 20
	maxSafeProcessInteger = int64(9_007_199_254_740_991)
)

type canonicalKind uint8

const (
	canonicalNull canonicalKind = iota
	canonicalBool
	canonicalNumber
	canonicalString
	canonicalArray
	canonicalObject
)

type canonicalNode struct {
	kind    canonicalKind
	boolean bool
	number  float64
	text    string
	array   []canonicalNode
	object  map[string]canonicalNode
}

type decodeState struct {
	nodes int
}

// Request is the validated immutable identity passed to the journal service.
// Payload fields are opaque to this foundation but are never ignored: every
// key/value is preserved in CanonicalJSON and therefore in RequestHash.
type Request struct {
	ExternalRunID             string
	RunID                     string
	Operation                 domain.ExecutionOperation
	IdempotencyKey            string
	ExpectedProcessGeneration int64
	TargetProcessGeneration   int64
	OperationID               string
	CanonicalJSON             []byte
	RequestHash               [sha256.Size]byte
}

// ParseRequest rejects lossy or alias-prone JSON and returns the one canonical
// request identity used by both initial dispatch and every retry/reconciliation.
// Strings are Unicode-scalar strings: adapters must reject lone UTF-16
// surrogates before transport instead of allowing them to alias U+FFFD.
func ParseRequest(raw []byte) (Request, error) {
	root, err := decodeCanonicalJSON(raw)
	if err != nil {
		return Request{}, invalid("invalid_json", err)
	}
	if root.kind != canonicalObject {
		return Request{}, invalid("request_must_be_object", nil)
	}
	allowed := map[string]bool{
		"version": true, "externalRunId": true, "runId": true,
		"operation": true, "idempotencyKey": true,
		"expectedProcessGeneration": true, "request": true,
	}
	for key := range root.object {
		if !allowed[key] {
			return Request{}, invalid("unknown_request_field", nil)
		}
	}
	version, ok := integerField(root, "version")
	if !ok || version != ContractVersion {
		return Request{}, invalid("unsupported_contract_version", nil)
	}
	externalRunID, ok := stringField(root, "externalRunId")
	if !ok || !validIdentifier(externalRunID) {
		return Request{}, invalid("invalid_external_run_id", nil)
	}
	operationText, ok := stringField(root, "operation")
	operation := domain.ExecutionOperation(operationText)
	if !ok || !validOperation(operation) {
		return Request{}, invalid("invalid_operation", nil)
	}
	idempotencyKey, ok := stringField(root, "idempotencyKey")
	if !ok || !validIdentifier(idempotencyKey) {
		return Request{}, invalid("invalid_idempotency_key", nil)
	}
	payload, ok := root.object["request"]
	if !ok || payload.kind != canonicalObject {
		return Request{}, invalid("operation_request_must_be_object", nil)
	}

	request := Request{
		ExternalRunID:  externalRunID,
		Operation:      operation,
		IdempotencyKey: idempotencyKey,
	}
	if operation == domain.ExecutionLaunch {
		if _, exists := root.object["runId"]; exists {
			return Request{}, invalid("launch_run_id_is_backend_assigned", nil)
		}
		if _, exists := root.object["expectedProcessGeneration"]; exists {
			return Request{}, invalid("launch_generation_is_backend_assigned", nil)
		}
		request.TargetProcessGeneration = 1
	} else {
		runID, present := stringField(root, "runId")
		if !present || !validIdentifier(runID) {
			return Request{}, invalid("invalid_run_id", nil)
		}
		expected, present := integerField(root, "expectedProcessGeneration")
		if !present || expected < 1 || expected > maxSafeProcessInteger {
			return Request{}, invalid("invalid_expected_process_generation", nil)
		}
		request.RunID = runID
		request.ExpectedProcessGeneration = expected
		request.TargetProcessGeneration = expected
		if advancesProcessGeneration(operation) {
			if expected == maxSafeProcessInteger {
				return Request{}, invalid("process_generation_exhausted", nil)
			}
			request.TargetProcessGeneration++
		}
	}

	request.CanonicalJSON = appendCanonical(nil, root)
	request.RequestHash = sha256.Sum256(request.CanonicalJSON)
	operationHash := sha256.Sum256([]byte(externalRunID + "\x00" + operationText + "\x00" + idempotencyKey))
	request.OperationID = hex.EncodeToString(operationHash[:])
	return request, nil
}

func canonicalizeResult(raw []byte) ([]byte, [sha256.Size]byte, error) {
	root, err := decodeCanonicalJSON(raw)
	if err != nil {
		return nil, [sha256.Size]byte{}, invalid("invalid_result_json", err)
	}
	if root.kind != canonicalObject {
		return nil, [sha256.Size]byte{}, invalid("result_must_be_object", nil)
	}
	canonical := appendCanonical(nil, root)
	return canonical, sha256.Sum256(canonical), nil
}

func decodeCanonicalJSON(raw []byte) (canonicalNode, error) {
	if len(raw) == 0 || len(raw) > maxCanonicalBytes {
		return canonicalNode{}, fmt.Errorf("JSON must contain 1-%d bytes", maxCanonicalBytes)
	}
	if !utf8.Valid(raw) {
		return canonicalNode{}, fmt.Errorf("JSON must be valid UTF-8")
	}
	if err := validateJSONStringEscapes(raw); err != nil {
		return canonicalNode{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	state := &decodeState{}
	root, err := decodeNode(decoder, state, 0)
	if err != nil {
		return canonicalNode{}, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return canonicalNode{}, err
		}
		return canonicalNode{}, fmt.Errorf("trailing JSON token %v", token)
	}
	return root, nil
}

func decodeNode(decoder *json.Decoder, state *decodeState, depth int) (canonicalNode, error) {
	state.nodes++
	if state.nodes > maxCanonicalNodes {
		return canonicalNode{}, fmt.Errorf("JSON exceeds %d values", maxCanonicalNodes)
	}
	token, err := decoder.Token()
	if err != nil {
		return canonicalNode{}, err
	}
	switch value := token.(type) {
	case nil:
		return canonicalNode{kind: canonicalNull}, nil
	case bool:
		return canonicalNode{kind: canonicalBool, boolean: value}, nil
	case string:
		if len(value) > maxStringBytes {
			return canonicalNode{}, fmt.Errorf("string exceeds %d bytes", maxStringBytes)
		}
		return canonicalNode{kind: canonicalString, text: value}, nil
	case json.Number:
		number, err := parseCanonicalNumber(string(value))
		if err != nil {
			return canonicalNode{}, err
		}
		return canonicalNode{kind: canonicalNumber, number: number}, nil
	case json.Delim:
		if depth >= maxCanonicalDepth {
			return canonicalNode{}, fmt.Errorf("JSON exceeds depth %d", maxCanonicalDepth)
		}
		switch value {
		case '{':
			object := make(map[string]canonicalNode)
			for decoder.More() {
				if len(object) >= maxObjectFields {
					return canonicalNode{}, fmt.Errorf("object exceeds %d fields", maxObjectFields)
				}
				keyToken, err := decoder.Token()
				if err != nil {
					return canonicalNode{}, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return canonicalNode{}, fmt.Errorf("object key must be a string")
				}
				if _, duplicate := object[key]; duplicate {
					return canonicalNode{}, fmt.Errorf("duplicate object key %q", key)
				}
				child, err := decodeNode(decoder, state, depth+1)
				if err != nil {
					return canonicalNode{}, err
				}
				object[key] = child
			}
			if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
				return canonicalNode{}, fmt.Errorf("unterminated object")
			}
			return canonicalNode{kind: canonicalObject, object: object}, nil
		case '[':
			array := make([]canonicalNode, 0)
			for decoder.More() {
				child, err := decodeNode(decoder, state, depth+1)
				if err != nil {
					return canonicalNode{}, err
				}
				array = append(array, child)
			}
			if closing, err := decoder.Token(); err != nil || closing != json.Delim(']') {
				return canonicalNode{}, fmt.Errorf("unterminated array")
			}
			return canonicalNode{kind: canonicalArray, array: array}, nil
		default:
			return canonicalNode{}, fmt.Errorf("unexpected delimiter %q", value)
		}
	default:
		return canonicalNode{}, fmt.Errorf("unsupported JSON token")
	}
}

func parseCanonicalNumber(raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, fmt.Errorf("number must be finite")
	}
	// Match ECMAScript JSON.stringify: negative zero is serialized as 0.
	if value == 0 {
		return 0, nil
	}
	return value, nil
}

func appendCanonical(dst []byte, node canonicalNode) []byte {
	switch node.kind {
	case canonicalNull:
		return append(dst, "null"...)
	case canonicalBool:
		return strconv.AppendBool(dst, node.boolean)
	case canonicalNumber:
		// encoding/json uses the same ES6-compatible shortest binary64 form as
		// JSON.stringify, including its fixed/exponent thresholds.
		encoded, err := json.Marshal(node.number)
		if err != nil {
			panic("validated canonical number became invalid")
		}
		return append(dst, encoded...)
	case canonicalString:
		return appendJSONString(dst, node.text)
	case canonicalArray:
		dst = append(dst, '[')
		for i, child := range node.array {
			if i != 0 {
				dst = append(dst, ',')
			}
			dst = appendCanonical(dst, child)
		}
		return append(dst, ']')
	case canonicalObject:
		keys := make([]string, 0, len(node.object))
		for key := range node.object {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return javascriptPropertyLess(keys[i], keys[j]) })
		dst = append(dst, '{')
		for i, key := range keys {
			if i != 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, key)
			dst = append(dst, ':')
			dst = appendCanonical(dst, node.object[key])
		}
		return append(dst, '}')
	default:
		panic("unknown canonical JSON node")
	}
}

func stringField(node canonicalNode, key string) (string, bool) {
	value, ok := node.object[key]
	return value.text, ok && value.kind == canonicalString
}

func integerField(node canonicalNode, key string) (int64, bool) {
	value, ok := node.object[key]
	if !ok || value.kind != canonicalNumber || math.Trunc(value.number) != value.number || value.number < -float64(maxSafeProcessInteger) || value.number > float64(maxSafeProcessInteger) {
		return 0, false
	}
	return int64(value.number), true
}

// appendJSONString matches JSON.stringify for valid Unicode scalar strings.
// decodeCanonicalJSON rejects malformed UTF-8 and unpaired surrogate escapes,
// so no lossy replacement-character alias can reach this encoder.
func appendJSONString(dst []byte, value string) []byte {
	dst = append(dst, '"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, `\b`...)
		case '\t':
			dst = append(dst, `\t`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\f':
			dst = append(dst, `\f`...)
		case '\r':
			dst = append(dst, `\r`...)
		default:
			if r < 0x20 {
				dst = append(dst, `\u00`...)
				dst = append(dst, "0123456789abcdef"[byte(r)>>4], "0123456789abcdef"[byte(r)&0x0f])
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	return append(dst, '"')
}

// javascriptPropertyLess reproduces the key order JSON.stringify observes
// after SuperOrch inserts Object.keys(value).sort() into a fresh object:
// array-index properties first in numeric order, then UTF-16 lexical order.
func javascriptPropertyLess(left, right string) bool {
	leftIndex, leftIsIndex := javascriptArrayIndex(left)
	rightIndex, rightIsIndex := javascriptArrayIndex(right)
	if leftIsIndex != rightIsIndex {
		return leftIsIndex
	}
	if leftIsIndex {
		return leftIndex < rightIndex
	}
	leftUnits := utf16.Encode([]rune(left))
	rightUnits := utf16.Encode([]rune(right))
	for i := 0; i < len(leftUnits) && i < len(rightUnits); i++ {
		if leftUnits[i] != rightUnits[i] {
			return leftUnits[i] < rightUnits[i]
		}
	}
	return len(leftUnits) < len(rightUnits)
}

func javascriptArrayIndex(value string) (uint64, bool) {
	if value == "" || value == "-0" || len(value) > 10 || len(value) > 1 && value[0] == '0' {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	return parsed, err == nil && parsed < 1<<32-1
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 || !isASCIIAlphaNumeric(value[0]) {
		return false
	}
	for _, b := range []byte(value[1:]) {
		if !isASCIIAlphaNumeric(b) && b != '.' && b != '_' && b != ':' && b != '-' {
			return false
		}
	}
	return true
}

func validateJSONStringEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || i+1 >= len(raw) {
				continue
			}
			i++
			if raw[i] != 'u' {
				continue
			}
			unit, ok := jsonHexCodeUnit(raw, i+1)
			if !ok {
				continue // encoding/json reports the malformed escape precisely.
			}
			i += 4
			switch {
			case unit >= 0xd800 && unit <= 0xdbff:
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return fmt.Errorf("JSON string contains an unpaired surrogate escape")
				}
				low, valid := jsonHexCodeUnit(raw, i+3)
				if !valid || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("JSON string contains an unpaired surrogate escape")
				}
				i += 6
			case unit >= 0xdc00 && unit <= 0xdfff:
				return fmt.Errorf("JSON string contains an unpaired surrogate escape")
			}
		}
	}
	return nil
}

func jsonHexCodeUnit(raw []byte, start int) (uint16, bool) {
	if start+4 > len(raw) {
		return 0, false
	}
	var value uint16
	for _, b := range raw[start : start+4] {
		value <<= 4
		switch {
		case b >= '0' && b <= '9':
			value += uint16(b - '0')
		case b >= 'a' && b <= 'f':
			value += uint16(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value += uint16(b-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func isASCIIAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func validOperation(operation domain.ExecutionOperation) bool {
	switch operation {
	case domain.ExecutionLaunch, domain.ExecutionSend, domain.ExecutionInterrupt,
		domain.ExecutionResume, domain.ExecutionRestore, domain.ExecutionStop,
		domain.ExecutionCleanup:
		return true
	default:
		return false
	}
}

func advancesProcessGeneration(operation domain.ExecutionOperation) bool {
	return operation == domain.ExecutionResume || operation == domain.ExecutionRestore
}
