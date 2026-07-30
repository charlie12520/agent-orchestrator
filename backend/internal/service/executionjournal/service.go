package executionjournal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ErrorCode is a stable, bounded execution-foundation failure class.
type ErrorCode string

const (
	CodeInvalidRequest            ErrorCode = "invalid_request"
	CodeIdempotencyConflict       ErrorCode = "idempotency_conflict"
	CodeRunNotFound               ErrorCode = "run_not_found"
	CodeRunConflict               ErrorCode = "run_conflict"
	CodeRunBusy                   ErrorCode = "run_busy"
	CodeProcessGenerationMismatch ErrorCode = "process_generation_mismatch"
	CodeReconciliationRequired    ErrorCode = "reconciliation_required"
	CodeInvalidResult             ErrorCode = "invalid_result"
	CodeNotConfigured             ErrorCode = "not_configured"
	CodeStorageFailure            ErrorCode = "storage_failure"
)

// OperationError never exposes dispatcher or storage details through Error().
// Trusted callers may inspect the wrapped cause with errors.Unwrap.
type OperationError struct {
	Code                      ErrorCode
	Reason                    string
	ExpectedProcessGeneration int64
	ActualProcessGeneration   int64
	cause                     error
}

func (e *OperationError) Error() string {
	if e == nil {
		return "execution journal error"
	}
	if e.Reason == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Reason
}

func (e *OperationError) Unwrap() error { return e.cause }

// ErrorInfo extracts the stable public fields from an operation error.
func ErrorInfo(err error) (ErrorCode, string, bool) {
	var operationError *OperationError
	if !errors.As(err, &operationError) {
		return "", "", false
	}
	return operationError.Code, operationError.Reason, true
}

func invalid(reason string, cause error) error {
	return operationError(CodeInvalidRequest, reason, cause)
}

func operationError(code ErrorCode, reason string, cause error) error {
	if len(reason) > 128 {
		reason = reason[:128]
	}
	return &OperationError{Code: code, Reason: reason, cause: cause}
}

func generationError(expected, actual int64) error {
	return &OperationError{
		Code:                      CodeProcessGenerationMismatch,
		Reason:                    "stale_process_generation",
		ExpectedProcessGeneration: expected,
		ActualProcessGeneration:   actual,
	}
}

// AcceptStatus is the atomic durable acceptance decision made by the store.
type AcceptStatus string

const (
	AcceptCreated            AcceptStatus = "created"
	AcceptExisting           AcceptStatus = "existing"
	AcceptRunNotFound        AcceptStatus = "run_not_found"
	AcceptRunConflict        AcceptStatus = "run_conflict"
	AcceptRunBusy            AcceptStatus = "run_busy"
	AcceptGenerationMismatch AcceptStatus = "generation_mismatch"
)

// Acceptance contains the journal/binding snapshot returned from one atomic
// accept transaction.
type Acceptance struct {
	Status                  AcceptStatus
	Journal                 domain.ExecutionOperationJournal
	Binding                 domain.ExecutionRunBinding
	ActualProcessGeneration int64
}

// Transition is the current durable journal/binding plus whether this caller's
// exact compare-and-swap changed it.
type Transition struct {
	Journal domain.ExecutionOperationJournal
	Binding domain.ExecutionRunBinding
	Changed bool
}

// Completion is the exact canonical result committed around a dispatch fence.
type Completion struct {
	OperationID       string
	ExternalRunID     string
	RunID             string
	ProcessGeneration int64
	ResultJSON        []byte
	ResultHash        []byte
	CompletedAt       time.Time
}

// Store is the durable operation-journal and run-binding boundary.
type Store interface {
	AcceptExecutionOperation(context.Context, domain.ExecutionOperationJournal) (Acceptance, error)
	GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error)
	GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error)
	ClaimExecutionDispatch(context.Context, string, string, string, time.Time) (Transition, error)
	ConfirmExecutionDispatch(context.Context, string, string, string) (Transition, error)
	// Owner terminalization requires the caller-held fence returned by neither
	// reads nor transitions. Reconciled refinement is a separate ambiguous-only
	// path and cannot accept an owner fence.
	RecordExecutionResult(context.Context, Completion, string, string) (Transition, error)
	RecordExecutionAmbiguous(context.Context, Completion, string, string) (Transition, error)
	RecordReconciledExecutionResult(context.Context, Completion) (Transition, error)
}

// DispatchCommand is the immutable command handed across the only side-effect
// boundary. RequestJSON is a private copy of the canonical accepted request.
type DispatchCommand struct {
	OperationID       string
	ExternalRunID     string
	RunID             string
	Operation         domain.ExecutionOperation
	ProcessGeneration int64
	RequestJSON       []byte
}

// DispatchReceipt is the backend receipt. Launch must assign RunID; mutations
// may omit it or repeat the already-bound RunID.
type DispatchReceipt struct {
	RunID      string
	ResultJSON []byte
}

// Dispatcher is intentionally unwired in AO-A1. Tests use a deterministic fake;
// a later integration layer will adapt the real harness/session manager.
type Dispatcher interface {
	Dispatch(context.Context, DispatchCommand) (DispatchReceipt, error)
}

// FaultPoint identifies a crash seam. A fault returns immediately without
// compensating durable state, matching a process loss at that boundary.
type FaultPoint string

const (
	FaultAfterAccept     FaultPoint = "after_accept"
	FaultAfterDispatch   FaultPoint = "after_dispatch"
	FaultAfterSideEffect FaultPoint = "after_side_effect"
	FaultAfterResult     FaultPoint = "after_result"
)

// FaultInjector is test-only; production construction should leave it nil.
type FaultInjector interface {
	Fail(context.Context, FaultPoint) error
}

// Deps are the complete foundation dependencies. DispatchOwner must identify
// one service/process generation and is durably paired with a random fence.
type Deps struct {
	Store         Store
	Dispatcher    Dispatcher
	DispatchOwner string
	Now           func() time.Time
	Random        io.Reader
	Faults        FaultInjector
}

// Service serializes mutations per external run in-process; SQLite binding CAS
// provides the cross-process fence.
type Service struct {
	store         Store
	dispatcher    Dispatcher
	dispatchOwner string
	now           func() time.Time
	random        io.Reader
	faults        FaultInjector
	stripes       [64]sync.Mutex
}

// New constructs an unwired execution-journal service.
func New(deps Deps) (*Service, error) {
	if deps.Store == nil || deps.Dispatcher == nil || !validDispatchOwner(deps.DispatchOwner) {
		return nil, operationError(CodeNotConfigured, "store_dispatcher_and_unique_owner_required", nil)
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Random == nil {
		deps.Random = rand.Reader
	}
	return &Service{
		store:         deps.Store,
		dispatcher:    deps.Dispatcher,
		dispatchOwner: deps.DispatchOwner,
		now:           deps.Now,
		random:        deps.Random,
		faults:        deps.Faults,
	}, nil
}

// Result is the exact durable outcome returned to a caller. Replayed is an
// ephemeral delivery fact and is never included in the durable JSON result.
type Result struct {
	OperationID       string
	ExternalRunID     string
	RunID             string
	Operation         domain.ExecutionOperation
	ProcessGeneration int64
	State             domain.ExecutionJournalState
	ResultJSON        []byte
	Replayed          bool
}

// Execute validates, accepts, claims, and at most once crosses the dispatcher
// boundary. Dispatched/ambiguous retries never invoke Dispatcher again.
func (s *Service) Execute(ctx context.Context, rawRequest []byte) (Result, error) {
	request, err := ParseRequest(rawRequest)
	if err != nil {
		return Result{}, err
	}
	unlock := s.lockRun(request.ExternalRunID)
	defer unlock()

	now := s.now().UTC()
	journal := domain.ExecutionOperationJournal{
		OperationID:               request.OperationID,
		ExternalRunID:             request.ExternalRunID,
		RunID:                     request.RunID,
		Operation:                 request.Operation,
		IdempotencyKey:            request.IdempotencyKey,
		RequestHash:               append([]byte(nil), request.RequestHash[:]...),
		RequestJSON:               append([]byte(nil), request.CanonicalJSON...),
		ExpectedProcessGeneration: request.ExpectedProcessGeneration,
		TargetProcessGeneration:   request.TargetProcessGeneration,
		State:                     domain.ExecutionAccepted,
		AcceptedAt:                now,
	}
	accepted, err := s.store.AcceptExecutionOperation(ctx, journal)
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "accept_failed", err)
	}
	switch accepted.Status {
	case AcceptRunNotFound:
		return Result{}, operationError(CodeRunNotFound, "run_not_found", nil)
	case AcceptRunConflict:
		return Result{}, operationError(CodeRunConflict, "run_identity_conflict", nil)
	case AcceptRunBusy:
		return Result{}, operationError(CodeRunBusy, "run_has_unresolved_operation", nil)
	case AcceptGenerationMismatch:
		return Result{}, generationError(request.ExpectedProcessGeneration, accepted.ActualProcessGeneration)
	case AcceptExisting:
		if !sameRequest(accepted.Journal, request) {
			return Result{}, operationError(CodeIdempotencyConflict, "idempotency_key_reused_for_different_request", nil)
		}
		return s.resumeJournal(ctx, accepted.Journal, true)
	case AcceptCreated:
		if !sameRequest(accepted.Journal, request) {
			return Result{}, operationError(CodeStorageFailure, "accepted_request_identity_mismatch", nil)
		}
	default:
		return Result{}, operationError(CodeStorageFailure, "unknown_accept_status", nil)
	}
	if err := s.inject(ctx, FaultAfterAccept); err != nil {
		return Result{}, err
	}
	return s.dispatchAccepted(ctx, accepted.Journal)
}

// RefineExactResult records authoritative proof of exact success for a
// durably ambiguous operation. A merely dispatched operation remains fenced
// until a future integration supplies trusted owner-death proof; foreign code
// can never clear a live dispatch. This method never calls Dispatcher.
func (s *Service) RefineExactResult(ctx context.Context, rawRequest []byte, receipt DispatchReceipt) (Result, error) {
	request, err := ParseRequest(rawRequest)
	if err != nil {
		return Result{}, err
	}
	unlock := s.lockRun(request.ExternalRunID)
	defer unlock()
	journal, found, err := s.store.GetExecutionOperation(ctx, request.OperationID)
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "journal_lookup_failed", err)
	}
	if !found {
		return Result{}, operationError(CodeRunNotFound, "operation_not_found", nil)
	}
	if !sameRequest(journal, request) {
		return Result{}, operationError(CodeIdempotencyConflict, "refinement_request_mismatch", nil)
	}
	if journal.State == domain.ExecutionResult {
		return resultFromJournal(journal, true), nil
	}
	if journal.State == domain.ExecutionAccepted || journal.State == domain.ExecutionDispatched {
		return Result{}, operationError(CodeReconciliationRequired, "operation_is_not_durably_ambiguous", nil)
	}
	if journal.State != domain.ExecutionAmbiguous {
		return Result{}, operationError(CodeStorageFailure, "invalid_journal_state", nil)
	}
	completion, err := completionFromReceipt(request, journal, receipt, s.now().UTC())
	if err != nil {
		return Result{}, err
	}
	transition, err := s.store.RecordReconciledExecutionResult(ctx, completion)
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "record_refined_result_failed", err)
	}
	return resultFromJournal(transition.Journal, !transition.Changed), nil
}

func (s *Service) dispatchAccepted(ctx context.Context, journal domain.ExecutionOperationJournal) (Result, error) {
	fenceBytes := make([]byte, 16)
	if _, err := io.ReadFull(s.random, fenceBytes); err != nil {
		return Result{}, operationError(CodeNotConfigured, "dispatch_fence_unavailable", err)
	}
	fence := hex.EncodeToString(fenceBytes)
	dispatched, err := s.store.ClaimExecutionDispatch(ctx, journal.OperationID, s.dispatchOwner, fence, s.now().UTC())
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "dispatch_claim_failed", err)
	}
	if !dispatched.Changed {
		return s.resumeJournal(ctx, dispatched.Journal, true)
	}
	journal = dispatched.Journal
	if err := s.inject(ctx, FaultAfterDispatch); err != nil {
		return Result{}, err
	}
	confirmed, err := s.store.ConfirmExecutionDispatch(ctx, journal.OperationID, s.dispatchOwner, fence)
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "dispatch_confirmation_failed", err)
	}
	if !confirmed.Changed {
		return s.resumeJournal(ctx, confirmed.Journal, true)
	}

	receipt, dispatchErr := s.dispatcher.Dispatch(ctx, DispatchCommand{
		OperationID:       journal.OperationID,
		ExternalRunID:     journal.ExternalRunID,
		RunID:             journal.RunID,
		Operation:         journal.Operation,
		ProcessGeneration: journal.TargetProcessGeneration,
		RequestJSON:       append([]byte(nil), journal.RequestJSON...),
	})
	if err := s.inject(ctx, FaultAfterSideEffect); err != nil {
		return Result{}, err
	}
	if dispatchErr != nil {
		return s.recordOwnerAmbiguity(ctx, journal, fence)
	}
	request := requestFromJournal(journal)
	completion, err := completionFromReceipt(request, journal, receipt, s.now().UTC())
	if err != nil {
		return s.recordOwnerAmbiguity(ctx, journal, fence)
	}
	completed, err := s.store.RecordExecutionResult(ctx, completion, s.dispatchOwner, fence)
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "record_result_failed", err)
	}
	if !completed.Changed {
		return s.resumeJournal(ctx, completed.Journal, true)
	}
	if err := s.inject(ctx, FaultAfterResult); err != nil {
		return Result{}, err
	}
	return resultFromJournal(completed.Journal, false), nil
}

func (s *Service) recordOwnerAmbiguity(ctx context.Context, journal domain.ExecutionOperationJournal, fence string) (Result, error) {
	canonical, hash, err := canonicalizeResult([]byte(`{"status":"ambiguous"}`))
	if err != nil {
		panic(err)
	}
	transition, err := s.store.RecordExecutionAmbiguous(ctx, Completion{
		OperationID: journal.OperationID, ExternalRunID: journal.ExternalRunID,
		ResultJSON: canonical, ResultHash: hash[:], CompletedAt: s.now().UTC(),
	}, s.dispatchOwner, fence)
	if err != nil {
		return Result{}, operationError(CodeStorageFailure, "record_ambiguity_failed", err)
	}
	return resultFromJournal(transition.Journal, !transition.Changed), nil
}

func (s *Service) resumeJournal(ctx context.Context, journal domain.ExecutionOperationJournal, replayed bool) (Result, error) {
	switch journal.State {
	case domain.ExecutionResult, domain.ExecutionAmbiguous:
		return resultFromJournal(journal, replayed), nil
	case domain.ExecutionDispatched:
		return Result{}, operationError(CodeReconciliationRequired, "dispatched_operation_will_not_be_redispatched", nil)
	case domain.ExecutionAccepted:
		return s.dispatchAccepted(ctx, journal)
	default:
		return Result{}, operationError(CodeStorageFailure, "invalid_journal_state", nil)
	}
}

func completionFromReceipt(request Request, journal domain.ExecutionOperationJournal, receipt DispatchReceipt, completedAt time.Time) (Completion, error) {
	runID := receipt.RunID
	if request.Operation == domain.ExecutionLaunch {
		if !validIdentifier(runID) {
			return Completion{}, operationError(CodeInvalidResult, "launch_result_missing_valid_run_id", nil)
		}
	} else {
		if runID == "" {
			runID = request.RunID
		}
		if runID != request.RunID {
			return Completion{}, operationError(CodeInvalidResult, "result_run_id_mismatch", nil)
		}
	}
	canonical, hash, err := canonicalizeResult(receipt.ResultJSON)
	if err != nil {
		return Completion{}, operationError(CodeInvalidResult, "result_json_not_canonicalizable", err)
	}
	return Completion{
		OperationID: journal.OperationID, ExternalRunID: journal.ExternalRunID,
		RunID: runID, ProcessGeneration: journal.TargetProcessGeneration,
		ResultJSON: canonical, ResultHash: hash[:], CompletedAt: completedAt,
	}, nil
}

func resultFromJournal(journal domain.ExecutionOperationJournal, replayed bool) Result {
	return Result{
		OperationID: journal.OperationID, ExternalRunID: journal.ExternalRunID,
		RunID: journal.ResultRunID, Operation: journal.Operation,
		ProcessGeneration: journal.ResultProcessGeneration, State: journal.State,
		ResultJSON: append([]byte(nil), journal.ResultJSON...), Replayed: replayed,
	}
}

func sameRequest(journal domain.ExecutionOperationJournal, request Request) bool {
	return journal.OperationID == request.OperationID &&
		journal.ExternalRunID == request.ExternalRunID &&
		journal.Operation == request.Operation &&
		journal.IdempotencyKey == request.IdempotencyKey &&
		journal.RunID == request.RunID &&
		journal.ExpectedProcessGeneration == request.ExpectedProcessGeneration &&
		journal.TargetProcessGeneration == request.TargetProcessGeneration &&
		len(journal.RequestHash) == sha256.Size &&
		subtle.ConstantTimeCompare(journal.RequestHash, request.RequestHash[:]) == 1 &&
		subtle.ConstantTimeCompare(journal.RequestJSON, request.CanonicalJSON) == 1
}

func requestFromJournal(journal domain.ExecutionOperationJournal) Request {
	var hash [sha256.Size]byte
	copy(hash[:], journal.RequestHash)
	return Request{
		ExternalRunID: journal.ExternalRunID, RunID: journal.RunID,
		Operation: journal.Operation, IdempotencyKey: journal.IdempotencyKey,
		ExpectedProcessGeneration: journal.ExpectedProcessGeneration,
		TargetProcessGeneration:   journal.TargetProcessGeneration,
		OperationID:               journal.OperationID, CanonicalJSON: append([]byte(nil), journal.RequestJSON...), RequestHash: hash,
	}
}

func (s *Service) inject(ctx context.Context, point FaultPoint) error {
	if s.faults == nil {
		return nil
	}
	return s.faults.Fail(ctx, point)
}

func (s *Service) lockRun(externalRunID string) func() {
	hash := sha256.Sum256([]byte(externalRunID))
	lock := &s.stripes[int(hash[0])%len(s.stripes)]
	lock.Lock()
	return lock.Unlock
}

func validDispatchOwner(value string) bool {
	return len(value) >= 8 && validIdentifier(value)
}
