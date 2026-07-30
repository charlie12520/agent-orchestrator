package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	executionapi "github.com/aoagents/agent-orchestrator/backend/internal/service/executionapi"
	executionjournal "github.com/aoagents/agent-orchestrator/backend/internal/service/executionjournal"
)

const maxExecutionRequestBytes = 1 << 20

var (
	executionOperationIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	executionIdentifierPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// ExecutionBackend is the only HTTP-to-execution boundary. Production wires
// read-only durable state and leaves mutation unavailable until a real,
// reviewed dispatcher exists.
type ExecutionBackend interface {
	Execute(context.Context, []byte) (executionjournal.Result, error)
	GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error)
	GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error)
}

type ExecutionController struct {
	Backend ExecutionBackend
}

func (c *ExecutionController) Register(r chi.Router) {
	r.Post("/execution/operations", c.execute)
	r.Get("/execution/operations/{operationId}", c.getOperation)
	r.Get("/execution/bindings/{externalRunId}", c.getBinding)
}

// ExecuteOperationRequest documents the existing A1 canonical request. The
// handler passes the original bytes to A1 after strict validation so no decode
// and re-encode step can change request identity.
type ExecuteOperationRequest struct {
	_                         struct{}                  `additionalProperties:"false"`
	Version                   int                       `json:"version" enum:"1"`
	ExternalRunID             string                    `json:"externalRunId" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	RunID                     string                    `json:"runId,omitempty" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	Operation                 domain.ExecutionOperation `json:"operation" enum:"launch,send,interrupt,resume,restore,stop,cleanup"`
	IdempotencyKey            string                    `json:"idempotencyKey" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	ExpectedProcessGeneration int64                     `json:"expectedProcessGeneration,omitempty" minimum:"1" maximum:"9007199254740991"`
	Request                   map[string]any            `json:"request" nullable:"false"`
}

type ExecutionIdempotencyHeader struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"1" maxLength:"128" description:"Must exactly equal the canonical request body's idempotencyKey."`
}

type ExecutionManagedHeaders struct {
	Authorization    string `header:"Authorization" required:"true" pattern:"^Bearer [0-9a-f]{64}$" description:"Exact managed-daemon bearer credential."`
	DaemonGeneration string `header:"X-AO-Daemon-Generation" required:"true" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$" description:"Exact current managed-daemon generation."`
}

type ExecutionOperationIDParam struct {
	OperationID string `path:"operationId" minLength:"64" maxLength:"64" pattern:"^[0-9a-f]{64}$"`
}

type ExecutionExternalRunIDParam struct {
	ExternalRunID string `path:"externalRunId" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
}

type ExecuteOperationResponse struct {
	Version           int                          `json:"version" enum:"1"`
	OperationID       string                       `json:"operationId" minLength:"64" maxLength:"64" pattern:"^[0-9a-f]{64}$"`
	ExternalRunID     string                       `json:"externalRunId" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	RunID             string                       `json:"runId,omitempty" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	Operation         domain.ExecutionOperation    `json:"operation" enum:"launch,send,interrupt,resume,restore,stop,cleanup"`
	ProcessGeneration int64                        `json:"processGeneration" minimum:"1" maximum:"9007199254740991"`
	State             domain.ExecutionJournalState `json:"state" enum:"accepted,dispatched,result,ambiguous"`
	Result            *map[string]any              `json:"result,omitempty" nullable:"false"`
	Replayed          bool                         `json:"replayed"`
}

// ExecutionOperationResponse is intentionally sanitized: request bytes,
// request/result hashes, idempotency keys, and dispatch ownership never cross
// the HTTP boundary.
type ExecutionOperationResponse struct {
	Version                   int                          `json:"version" enum:"1"`
	OperationID               string                       `json:"operationId" minLength:"64" maxLength:"64" pattern:"^[0-9a-f]{64}$"`
	ExternalRunID             string                       `json:"externalRunId" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	RunID                     string                       `json:"runId,omitempty" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	Operation                 domain.ExecutionOperation    `json:"operation" enum:"launch,send,interrupt,resume,restore,stop,cleanup"`
	ExpectedProcessGeneration int64                        `json:"expectedProcessGeneration,omitempty" minimum:"1" maximum:"9007199254740991"`
	TargetProcessGeneration   int64                        `json:"targetProcessGeneration" minimum:"1" maximum:"9007199254740991"`
	State                     domain.ExecutionJournalState `json:"state" enum:"accepted,dispatched,result,ambiguous"`
	AcceptedAt                time.Time                    `json:"acceptedAt"`
	DispatchedAt              *time.Time                   `json:"dispatchedAt,omitempty" nullable:"false"`
	CompletedAt               *time.Time                   `json:"completedAt,omitempty" nullable:"false"`
	ResultRunID               string                       `json:"resultRunId,omitempty"`
	ResultProcessGeneration   int64                        `json:"resultProcessGeneration,omitempty"`
	Result                    *map[string]any              `json:"result,omitempty" nullable:"false"`
}

// ExecutionBindingResponse omits launch request identity and exposes only the
// durable public ownership state needed by a managed supervisor.
type ExecutionBindingResponse struct {
	Version            int                             `json:"version" enum:"1"`
	ExternalRunID      string                          `json:"externalRunId" minLength:"1" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	RunID              string                          `json:"runId,omitempty" maxLength:"128" pattern:"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"`
	State              domain.ExecutionRunBindingState `json:"state" enum:"launching,active,stopped,cleaned,busy,ambiguous"`
	ProcessGeneration  int64                           `json:"processGeneration" minimum:"0" maximum:"9007199254740991"`
	PendingOperationID string                          `json:"pendingOperationId,omitempty" minLength:"64" maxLength:"64" pattern:"^[0-9a-f]{64}$"`
	CreatedAt          time.Time                       `json:"createdAt"`
	UpdatedAt          time.Time                       `json:"updatedAt"`
}

func (c *ExecutionController) execute(w http.ResponseWriter, r *http.Request) {
	if c == nil || c.Backend == nil {
		writeExecutionUnavailable(w, r)
		return
	}
	if !validExecutionContentType(r) {
		envelope.WriteAPIError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type", "EXECUTION_CONTENT_TYPE_REQUIRED", "Content-Type must be application/json", nil)
		return
	}
	if r.ContentLength > maxExecutionRequestBytes {
		writeExecutionTooLarge(w, r)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxExecutionRequestBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeExecutionTooLarge(w, r)
			return
		}
		writeInvalidExecutionRequest(w, r)
		return
	}
	request, err := executionjournal.ParseRequest(raw)
	if err != nil {
		writeInvalidExecutionRequest(w, r)
		return
	}
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 || values[0] == "" {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "IDEMPOTENCY_KEY_REQUIRED", "Exactly one Idempotency-Key header is required", nil)
		return
	}
	if values[0] != request.IdempotencyKey {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "IDEMPOTENCY_KEY_MISMATCH", "Idempotency-Key must exactly match the request body", nil)
		return
	}

	result, err := c.Backend.Execute(r.Context(), raw)
	if err != nil {
		writeExecutionError(w, r, err)
		return
	}
	response, err := executeOperationResponse(result)
	if err != nil {
		writeExecutionStorageFailure(w, r)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, response)
}

func (c *ExecutionController) getOperation(w http.ResponseWriter, r *http.Request) {
	if c == nil || c.Backend == nil {
		writeExecutionUnavailable(w, r)
		return
	}
	operationID := chi.URLParam(r, "operationId")
	if !executionOperationIDPattern.MatchString(operationID) {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_EXECUTION_OPERATION_ID", "Invalid execution operation id", nil)
		return
	}
	journal, found, err := c.Backend.GetExecutionOperation(r.Context(), operationID)
	if err != nil {
		writeExecutionReadError(w, r, err)
		return
	}
	if !found {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_OPERATION_NOT_FOUND", "Execution operation not found", nil)
		return
	}
	response, err := executionOperationResponse(journal)
	if err != nil {
		writeExecutionStorageFailure(w, r)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, response)
}

func (c *ExecutionController) getBinding(w http.ResponseWriter, r *http.Request) {
	if c == nil || c.Backend == nil {
		writeExecutionUnavailable(w, r)
		return
	}
	externalRunID := chi.URLParam(r, "externalRunId")
	if !executionIdentifierPattern.MatchString(externalRunID) {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_EXTERNAL_RUN_ID", "Invalid external run id", nil)
		return
	}
	binding, found, err := c.Backend.GetExecutionRunBinding(r.Context(), externalRunID)
	if err != nil {
		writeExecutionReadError(w, r, err)
		return
	}
	if !found {
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_BINDING_NOT_FOUND", "Execution binding not found", nil)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, ExecutionBindingResponse{
		Version: executionjournal.ContractVersion, ExternalRunID: binding.ExternalRunID,
		RunID: binding.RunID, State: binding.State, ProcessGeneration: binding.ProcessGeneration,
		PendingOperationID: binding.PendingOperationID, CreatedAt: binding.CreatedAt, UpdatedAt: binding.UpdatedAt,
	})
}

func validExecutionContentType(r *http.Request) bool {
	values := r.Header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, params, err := mime.ParseMediaType(values[0])
	if err != nil || !strings.EqualFold(mediaType, "application/json") || len(params) > 1 {
		return false
	}
	for key, value := range params {
		if !strings.EqualFold(key, "charset") || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}

func executeOperationResponse(result executionjournal.Result) (ExecuteOperationResponse, error) {
	decoded, err := decodeExecutionResult(result.ResultJSON)
	if err != nil {
		return ExecuteOperationResponse{}, err
	}
	return ExecuteOperationResponse{
		Version: executionjournal.ContractVersion, OperationID: result.OperationID,
		ExternalRunID: result.ExternalRunID, RunID: result.RunID, Operation: result.Operation,
		ProcessGeneration: result.ProcessGeneration, State: result.State,
		Result: decoded, Replayed: result.Replayed,
	}, nil
}

func executionOperationResponse(journal domain.ExecutionOperationJournal) (ExecutionOperationResponse, error) {
	decoded, err := decodeExecutionResult(journal.ResultJSON)
	if err != nil {
		return ExecutionOperationResponse{}, err
	}
	return ExecutionOperationResponse{
		Version: executionjournal.ContractVersion, OperationID: journal.OperationID,
		ExternalRunID: journal.ExternalRunID, RunID: journal.RunID, Operation: journal.Operation,
		ExpectedProcessGeneration: journal.ExpectedProcessGeneration,
		TargetProcessGeneration:   journal.TargetProcessGeneration, State: journal.State,
		AcceptedAt: journal.AcceptedAt, DispatchedAt: executionTimeIfSet(journal.DispatchedAt),
		CompletedAt: executionTimeIfSet(journal.CompletedAt), ResultRunID: journal.ResultRunID,
		ResultProcessGeneration: journal.ResultProcessGeneration, Result: decoded,
	}, nil
}

func decodeExecutionResult(raw []byte) (*map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > maxExecutionRequestBytes || !json.Valid(raw) {
		return nil, errors.New("invalid durable execution result")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("execution result must be an object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("execution result has trailing data")
	}
	return &result, nil
}

func executionTimeIfSet(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}

func writeInvalidExecutionRequest(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_EXECUTION_REQUEST", "Invalid execution request", nil)
}

func writeExecutionTooLarge(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusRequestEntityTooLarge, "too_large", "EXECUTION_REQUEST_TOO_LARGE", "Execution request exceeds 1048576 bytes", nil)
}

func writeExecutionUnavailable(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "EXECUTION_UNAVAILABLE", "Execution service is not available", nil)
}

func writeExecutionStorageFailure(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "EXECUTION_STORAGE_FAILURE", "Execution state is unavailable", nil)
}

func writeExecutionReadError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, executionapi.ErrUnavailable) {
		writeExecutionUnavailable(w, r)
		return
	}
	writeExecutionStorageFailure(w, r)
}

func writeExecutionError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, executionapi.ErrUnavailable) {
		writeExecutionUnavailable(w, r)
		return
	}
	code, _, ok := executionjournal.ErrorInfo(err)
	if !ok {
		writeExecutionStorageFailure(w, r)
		return
	}
	switch code {
	case executionjournal.CodeInvalidRequest:
		writeInvalidExecutionRequest(w, r)
	case executionjournal.CodeRunNotFound:
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "EXECUTION_RUN_NOT_FOUND", "Execution run not found", nil)
	case executionjournal.CodeIdempotencyConflict:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "EXECUTION_IDEMPOTENCY_CONFLICT", "Execution idempotency conflict", nil)
	case executionjournal.CodeRunConflict:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "EXECUTION_RUN_CONFLICT", "Execution run identity conflict", nil)
	case executionjournal.CodeRunBusy:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "EXECUTION_RUN_BUSY", "Execution run has an unresolved operation", nil)
	case executionjournal.CodeProcessGenerationMismatch:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "EXECUTION_PROCESS_GENERATION_MISMATCH", "Execution process generation is stale", nil)
	case executionjournal.CodeReconciliationRequired:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "EXECUTION_RECONCILIATION_REQUIRED", "Execution result requires trusted reconciliation", nil)
	case executionjournal.CodeNotConfigured:
		writeExecutionUnavailable(w, r)
	case executionjournal.CodeInvalidResult, executionjournal.CodeStorageFailure:
		writeExecutionStorageFailure(w, r)
	default:
		writeExecutionStorageFailure(w, r)
	}
}
