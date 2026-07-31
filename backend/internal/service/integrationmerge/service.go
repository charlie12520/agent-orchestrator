// Package integrationmerge implements the deterministic, durable merge actor
// used by SuperOrch. It deliberately has no shell or GitHub CLI dependency:
// all authoritative reads and the single external merge side effect go through
// the least-privilege Broker interface.
package integrationmerge

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

const (
	// ContractVersion independently versions the hardened merge contract.
	ContractVersion            = 1
	defaultLeaseTTL            = 5 * time.Minute
	maxLeaseTTL                = 15 * time.Minute
	maxEvidence                = 200
	maxPolicyItems             = 100
	maxBodyString              = 512
	reconciliationPollInterval = 100 * time.Millisecond
)

// ErrorCode is a stable, non-secret service failure classification.
type ErrorCode string

const (
	CodeInvalidInput             ErrorCode = "invalid_input"
	CodeUnauthorized             ErrorCode = "unauthorized"
	CodeNotConfigured            ErrorCode = "not_configured"
	CodeLeaseNotFound            ErrorCode = "lease_not_found"
	CodeActiveLeaseExists        ErrorCode = "active_lease_exists"
	CodeLeaseConsumed            ErrorCode = "lease_consumed"
	CodeLeaseRevoked             ErrorCode = "lease_revoked"
	CodeLeaseExpired             ErrorCode = "lease_expired"
	CodeCapabilityRejected       ErrorCode = "capability_rejected"
	CodeIdempotencyConflict      ErrorCode = "idempotency_conflict"
	CodeManualApprovalRequired   ErrorCode = "manual_approval_required"
	CodeRevalidationRequired     ErrorCode = "revalidation_required"
	CodeReconciliationInProgress ErrorCode = "reconciliation_in_progress"
	CodeBrokerUnavailable        ErrorCode = "broker_unavailable"
	CodeInternal                 ErrorCode = "internal"
)

// OperationError intentionally exposes only bounded code/reason fields. The
// wrapped cause is available to trusted callers via errors.Unwrap, but is never
// appropriate for an HTTP response because broker/storage errors may contain
// operational detail.
type OperationError struct {
	Code   ErrorCode
	Reason string
	cause  error
}

func (e *OperationError) Error() string {
	if e == nil {
		return "integration merge error"
	}
	if e.Reason == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Reason
}

func (e *OperationError) Unwrap() error { return e.cause }

func opError(code ErrorCode, reason string, cause error) error {
	if len(reason) > 128 {
		reason = reason[:128]
	}
	return &OperationError{Code: code, Reason: reason, cause: cause}
}

// ErrorInfo extracts a stable operation code and reason.
func ErrorInfo(err error) (ErrorCode, string, bool) {
	var op *OperationError
	if !errors.As(err, &op) {
		return "", "", false
	}
	return op.Code, op.Reason, true
}

// RootAuthorizer is supplied by the separately managed control plane. It must
// authenticate a root-control request from context; merge capabilities are an
// additional bound authorization, not a substitute for root authentication.
type RootAuthorizer interface {
	AuthorizeRoot(context.Context) error
}

// DispatchOwnerLiveness is a trustworthy, non-timeout proof about the
// process/generation that owns one durable dispatch fence.
type DispatchOwnerLiveness string

const (
	DispatchOwnerAlive   DispatchOwnerLiveness = "alive"
	DispatchOwnerDead    DispatchOwnerLiveness = "dead"
	DispatchOwnerUnknown DispatchOwnerLiveness = "unknown"
)

// DispatchOwnerVerifier is supplied by the managed supervisor boundary. A
// timeout alone must never be interpreted as owner death: only Dead is proof
// that no paused invocation can resume.
type DispatchOwnerVerifier interface {
	VerifyDispatchOwner(context.Context, string) (DispatchOwnerLiveness, error)
}

// Broker is the only outbound merge authority. A production implementation
// must be a separate least-privilege MergeBroker/GitHub App client.
type Broker interface {
	Candidate(context.Context, string, int) (domain.IntegrationMergeCandidate, error)
	Merge(context.Context, BrokerMergeCommand) (domain.IntegrationBrokerMergeResult, error)
}

// BrokerMergeCommand binds the external call to the immutable target SHA and
// strategy. Implementations must use an API precondition equivalent to these
// fields; branch-only merge calls do not satisfy this contract.
type BrokerMergeCommand struct {
	Repository      string
	PRNumber        int
	ExpectedHeadSHA string
	Strategy        domain.MergeStrategy
}

// Store is the durable lease and idempotency journal boundary.
type Store interface {
	CreateIntegrationMergeLease(context.Context, domain.IntegrationMergeLease, time.Time) (bool, error)
	GetIntegrationMergeLease(context.Context, string) (domain.IntegrationMergeLease, bool, error)
	RevokeIntegrationMergeLease(context.Context, string, time.Time) (domain.IntegrationMergeLease, bool, error)
	GetIntegrationMergeJournal(context.Context, string) (domain.IntegrationMergeJournal, bool, error)
	AcceptIntegrationMerge(context.Context, domain.IntegrationMergeJournal, time.Time) (bool, domain.IntegrationMergeLeaseStatus, error)
	MarkIntegrationMergeDispatched(context.Context, string, string, string, time.Time) (domain.IntegrationMergeJournal, bool, error)
	ConfirmIntegrationMergeDispatch(context.Context, string, string, string) (domain.IntegrationMergeJournal, bool, error)
	RecordIntegrationMergePreDispatchResult(context.Context, string, domain.IntegrationMergeOutcome, time.Time) (domain.IntegrationMergeJournal, bool, error)
	RecordIntegrationMergeFencedResult(context.Context, string, string, string, domain.IntegrationMergeOutcome, time.Time) (domain.IntegrationMergeJournal, bool, error)
	RecordIntegrationMergeReconciledResult(context.Context, string, domain.IntegrationMergeOutcome, time.Time) (domain.IntegrationMergeJournal, bool, error)
	RecordIntegrationMergeRefinedResult(context.Context, string, domain.IntegrationMergeOutcome, time.Time) (domain.IntegrationMergeJournal, bool, error)
	RecordIntegrationMergeFencedAmbiguous(context.Context, string, string, string, domain.IntegrationMergeOutcome, time.Time) (domain.IntegrationMergeJournal, bool, error)
}

// FaultInjector exists only to test crash boundaries. Production wiring should
// leave it nil.
type FaultInjector interface {
	BeforeBrokerMerge(context.Context) error
	AfterBrokerMerge(context.Context, domain.IntegrationBrokerMergeResult) error
}

// Deps are mandatory construction dependencies. DispatchOwner must uniquely
// identify this Service instance/process generation; OwnerVerifier is the
// managed supervisor's non-timeout liveness authority for other owners.
type Deps struct {
	Store            Store
	Broker           Broker
	RootAuthorizer   RootAuthorizer
	DispatchOwner    string
	OwnerVerifier    DispatchOwnerVerifier
	Now              func() time.Time
	Random           io.Reader
	ReconcileTimeout time.Duration
	Faults           FaultInjector
}

// Service serializes the single-candidate lane in-process. SQLite transactions
// enforce the durable lease/journal transitions across restarts.
type Service struct {
	store          Store
	broker         Broker
	authorizer     RootAuthorizer
	dispatchOwner  string
	ownerVerifier  DispatchOwnerVerifier
	now            func() time.Time
	random         io.Reader
	reconcileLimit time.Duration
	faults         FaultInjector
	gate           sync.Mutex
	ownedFences    map[string]string
}

// New constructs the merge actor. Production must not call New until a managed
// root authorizer, separate least-privilege broker, unique dispatch owner, and
// trustworthy owner verifier are all available.
func New(d Deps) (*Service, error) {
	if d.Store == nil || d.Broker == nil || d.RootAuthorizer == nil || d.OwnerVerifier == nil || !validDispatchOwner(d.DispatchOwner) {
		return nil, opError(CodeNotConfigured, "managed_control_merge_broker_and_dispatch_fence_required", nil)
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Random == nil {
		d.Random = rand.Reader
	}
	if d.ReconcileTimeout <= 0 {
		d.ReconcileTimeout = 10 * time.Second
	}
	return &Service{
		store:          d.Store,
		broker:         d.Broker,
		authorizer:     d.RootAuthorizer,
		dispatchOwner:  d.DispatchOwner,
		ownerVerifier:  d.OwnerVerifier,
		now:            d.Now,
		random:         d.Random,
		reconcileLimit: d.ReconcileTimeout,
		faults:         d.Faults,
		ownedFences:    make(map[string]string),
	}, nil
}

// Manager is the controller-facing versioned contract.
type Manager interface {
	IssueLease(context.Context, IssueLeaseInput) (IssuedLease, error)
	RevokeLease(context.Context, string) (domain.IntegrationMergeLease, error)
	Merge(context.Context, MergeInput) (domain.IntegrationMergeOutcome, error)
}

// IssueLeaseInput is the full immutable authorization snapshot supplied by
// SuperOrch after it has selected exactly one candidate.
type IssueLeaseInput struct {
	Repository             string
	SourceRepository       string
	PRNumber               int
	SourceBranch           string
	ExpectedHeadSHA        string
	BaseRepository         string
	BaseBranch             string
	Strategy               domain.MergeStrategy
	CheckPolicy            domain.IntegrationCheckPolicy
	ReviewPolicy           domain.IntegrationReviewPolicy
	ManualApprovalRequired bool
	ExpiresAt              time.Time
}

// IssuedLease returns the capability plaintext exactly once.
type IssuedLease struct {
	Lease          domain.IntegrationMergeLease
	GateCapability string
}

// MergeInput names the durable operation. It deliberately carries no check,
// review, branch, or SHA claims; those come from the lease and fresh broker
// facts. ManualApproval is the explicit root-authenticated human release when
// the lease requested a pause.
type MergeInput struct {
	LeaseID        string
	GateCapability string
	IdempotencyKey string
	ManualApproval bool
}

// IssueLease creates a single active, bounded lease. The capability digest is
// persisted; plaintext exists only in the returned value.
func (s *Service) IssueLease(ctx context.Context, in IssueLeaseInput) (IssuedLease, error) {
	if err := s.authorize(ctx); err != nil {
		return IssuedLease{}, err
	}
	now := s.now().UTC()
	lease, err := normalizeLeaseInput(in, now)
	if err != nil {
		return IssuedLease{}, err
	}
	leaseID, err := randomToken(s.random, "iml_", 18)
	if err != nil {
		return IssuedLease{}, opError(CodeInternal, "lease_randomness_failed", err)
	}
	capability, err := randomToken(s.random, "imc_", 32)
	if err != nil {
		return IssuedLease{}, opError(CodeInternal, "capability_randomness_failed", err)
	}
	digest := sha256.Sum256([]byte(capability))
	lease.ID = leaseID
	lease.CapabilityDigest = append([]byte(nil), digest[:]...)
	lease.Status = domain.IntegrationMergeLeaseActive
	lease.CreatedAt = now

	s.gate.Lock()
	defer s.gate.Unlock()
	created, err := s.store.CreateIntegrationMergeLease(ctx, lease, now)
	if err != nil {
		return IssuedLease{}, opError(CodeInternal, "lease_persistence_failed", err)
	}
	if !created {
		return IssuedLease{}, opError(CodeActiveLeaseExists, "single_candidate_lease_already_active", nil)
	}
	return IssuedLease{Lease: lease, GateCapability: capability}, nil
}

// RevokeLease atomically revokes an active lease exactly once.
func (s *Service) RevokeLease(ctx context.Context, leaseID string) (domain.IntegrationMergeLease, error) {
	if err := s.authorize(ctx); err != nil {
		return domain.IntegrationMergeLease{}, err
	}
	if err := validateLeaseID(leaseID); err != nil {
		return domain.IntegrationMergeLease{}, err
	}
	s.gate.Lock()
	defer s.gate.Unlock()
	lease, revoked, err := s.store.RevokeIntegrationMergeLease(ctx, leaseID, s.now().UTC())
	if err != nil {
		return domain.IntegrationMergeLease{}, opError(CodeInternal, "lease_revocation_failed", err)
	}
	if lease.ID == "" {
		return domain.IntegrationMergeLease{}, opError(CodeLeaseNotFound, "unknown_lease", nil)
	}
	if revoked {
		return lease, nil
	}
	return domain.IntegrationMergeLease{}, leaseStateError(lease.Status)
}

// Merge consumes a lease and executes or reconciles one idempotent operation.
func (s *Service) Merge(ctx context.Context, in MergeInput) (domain.IntegrationMergeOutcome, error) {
	if err := s.authorize(ctx); err != nil {
		return domain.IntegrationMergeOutcome{}, err
	}
	if err := validateMergeInput(in); err != nil {
		return domain.IntegrationMergeOutcome{}, err
	}

	s.gate.Lock()
	defer s.gate.Unlock()

	lease, found, err := s.store.GetIntegrationMergeLease(ctx, in.LeaseID)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "lease_lookup_failed", err)
	}
	if !found {
		// Keep the capability comparison path present even for an unknown id so
		// callers cannot turn it into a useful capability oracle.
		dummy := sha256.Sum256([]byte("unknown-integration-merge-lease"))
		provided := sha256.Sum256([]byte(in.GateCapability))
		_ = subtle.ConstantTimeCompare(dummy[:], provided[:])
		return domain.IntegrationMergeOutcome{}, opError(CodeLeaseNotFound, "unknown_lease", nil)
	}
	if !capabilityMatches(lease.CapabilityDigest, in.GateCapability) {
		return domain.IntegrationMergeOutcome{}, opError(CodeCapabilityRejected, "capability_rejected", nil)
	}
	hash, err := canonicalRequestHash(lease, in.ManualApproval)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "request_hash_failed", err)
	}

	journal, journalFound, err := s.store.GetIntegrationMergeJournal(ctx, in.IdempotencyKey)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "journal_lookup_failed", err)
	}
	if journalFound {
		if subtle.ConstantTimeCompare(journal.RequestHash, hash) != 1 {
			return domain.IntegrationMergeOutcome{}, opError(CodeIdempotencyConflict, "key_bound_to_different_request", nil)
		}
		return s.resumeJournal(ctx, lease, journal)
	}

	if lease.Status != domain.IntegrationMergeLeaseActive {
		return domain.IntegrationMergeOutcome{}, leaseStateError(lease.Status)
	}
	if !lease.ExpiresAt.After(s.now().UTC()) {
		return domain.IntegrationMergeOutcome{}, opError(CodeLeaseExpired, "lease_expired", nil)
	}

	candidate, err := s.broker.Candidate(ctx, lease.Repository, lease.PRNumber)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeBrokerUnavailable, "candidate_read_failed", err)
	}
	if reason := candidateRevalidationReason(lease, candidate); reason != "" {
		return domain.IntegrationMergeOutcome{}, opError(CodeRevalidationRequired, reason, nil)
	}
	if lease.ManualApprovalRequired && !in.ManualApproval {
		return domain.IntegrationMergeOutcome{}, opError(CodeManualApprovalRequired, "manual_approval_required", nil)
	}

	now := s.now().UTC()
	journal = domain.IntegrationMergeJournal{
		IdempotencyKey: in.IdempotencyKey,
		RequestHash:    append([]byte(nil), hash...),
		LeaseID:        lease.ID,
		State:          domain.IntegrationMergeAccepted,
		AcceptedAt:     now,
	}
	accepted, state, err := s.store.AcceptIntegrationMerge(ctx, journal, now)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "accept_persistence_failed", err)
	}
	if !accepted {
		return domain.IntegrationMergeOutcome{}, leaseStateError(state)
	}
	return s.dispatchAccepted(ctx, lease, journal)
}

func (s *Service) resumeJournal(ctx context.Context, lease domain.IntegrationMergeLease, journal domain.IntegrationMergeJournal) (domain.IntegrationMergeOutcome, error) {
	switch journal.State {
	case domain.IntegrationMergeResult:
		if journal.Outcome == nil {
			return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "terminal_journal_missing_outcome", nil)
		}
		delete(s.ownedFences, journal.IdempotencyKey)
		return cloneOutcome(*journal.Outcome), nil
	case domain.IntegrationMergeAmbiguous:
		if journal.Outcome == nil {
			return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "terminal_journal_missing_outcome", nil)
		}
		// Ambiguity is terminal for dispatch: it can never cause another
		// Broker.Merge. A later authoritative exact-success observation may,
		// however, refine the durable record to the operation's true result.
		refineCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
		candidate, err := s.broker.Candidate(refineCtx, lease.Repository, lease.PRNumber)
		cancel()
		if err == nil && mergedOperationProven(lease, candidate) {
			outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeMerged, "", candidate.MergeCommitSHA, candidate.MergedAt)
			return s.recordRefinedResult(ctx, journal.IdempotencyKey, outcome)
		}
		delete(s.ownedFences, journal.IdempotencyKey)
		return cloneOutcome(*journal.Outcome), nil
	case domain.IntegrationMergeDispatched:
		return s.reconcileDispatched(ctx, lease, journal)
	case domain.IntegrationMergeAccepted:
		candidate, err := s.broker.Candidate(ctx, lease.Repository, lease.PRNumber)
		if err != nil {
			return domain.IntegrationMergeOutcome{}, opError(CodeBrokerUnavailable, "candidate_read_failed", err)
		}
		if reason := candidateRevalidationReason(lease, candidate); reason != "" {
			outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeRevalidationRequired, reason, "", s.now().UTC())
			return s.recordPreDispatchResult(ctx, journal.IdempotencyKey, outcome)
		}
		return s.dispatchAccepted(ctx, lease, journal)
	default:
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "invalid_journal_state", nil)
	}
}

func (s *Service) dispatchAccepted(ctx context.Context, lease domain.IntegrationMergeLease, journal domain.IntegrationMergeJournal) (domain.IntegrationMergeOutcome, error) {
	fence, err := randomToken(s.random, "imf_", 18)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "dispatch_fence_randomness_failed", err)
	}
	dispatchedAt := s.now().UTC()
	updated, changed, err := s.store.MarkIntegrationMergeDispatched(ctx, journal.IdempotencyKey, s.dispatchOwner, fence, dispatchedAt)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "dispatch_persistence_failed", err)
	}
	if !changed {
		return s.resumeJournal(ctx, lease, updated)
	}
	journal = updated
	s.ownedFences[journal.IdempotencyKey] = journal.DispatchFence

	if s.faults != nil {
		if err := s.faults.BeforeBrokerMerge(ctx); err != nil {
			// Deliberately leave the durable state at dispatched. A restarted
			// actor cannot infer whether the call crossed the process boundary and
			// therefore reconciles instead of dispatching again.
			return domain.IntegrationMergeOutcome{}, err
		}
	}

	// Re-read authoritative facts after the durable dispatch and any pause. A
	// candidate change fences the journal before the external call.
	candidate, err := s.broker.Candidate(ctx, lease.Repository, lease.PRNumber)
	if err != nil {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeAmbiguous, "authoritative_pre_dispatch_revalidation_unavailable", "", s.now().UTC())
		return s.recordFencedAmbiguous(ctx, journal, outcome)
	}
	if mergedOperationProven(lease, candidate) {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeMerged, "", candidate.MergeCommitSHA, candidate.MergedAt)
		return s.recordReconciledResult(ctx, journal.IdempotencyKey, outcome)
	}
	if candidate.Merged || candidate.Ambiguous {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeAmbiguous, "authorized_merge_not_proven", "", s.now().UTC())
		return s.recordFencedAmbiguous(ctx, journal, outcome)
	}
	if reason := candidateRevalidationReason(lease, candidate); reason != "" {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeRevalidationRequired, reason, "", s.now().UTC())
		return s.recordFencedResult(ctx, journal, outcome)
	}

	// This owner/fence check is the final durable operation before Broker.Merge.
	// Foreign services may reconcile success but cannot invalidate a live owner,
	// so a paused owner cannot resume after its fence was terminalized.
	confirmed, ownsFence, err := s.store.ConfirmIntegrationMergeDispatch(ctx, journal.IdempotencyKey, journal.DispatchOwner, journal.DispatchFence)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "dispatch_fence_confirmation_failed", err)
	}
	if !ownsFence {
		outcome, transitionErr := terminalOrInProgress(confirmed, "dispatch_fence_lost")
		if transitionErr == nil {
			delete(s.ownedFences, journal.IdempotencyKey)
		}
		return outcome, transitionErr
	}
	journal = confirmed

	receipt, mergeErr := s.broker.Merge(ctx, BrokerMergeCommand{
		Repository:      lease.Repository,
		PRNumber:        lease.PRNumber,
		ExpectedHeadSHA: lease.ExpectedHeadSHA,
		Strategy:        lease.Strategy,
	})
	if mergeErr == nil && s.faults != nil {
		if err := s.faults.AfterBrokerMerge(ctx, receipt); err != nil {
			mergeErr = err
		}
	}
	if mergeErr != nil || !validReceipt(lease, receipt) {
		return s.reconcileDispatched(ctx, lease, journal)
	}
	outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeMerged, "", receipt.MergeCommitSHA, receipt.MergedAt)
	return s.recordFencedResult(ctx, journal, outcome)
}

// reconcileDispatched never calls Broker.Merge. Exact authoritative success is
// the only terminal result a foreign owner may write while the recorded owner
// could still be alive. An eligible, unmerged candidate remains dispatched
// unless trusted managed control proves that owner dead.
func (s *Service) reconcileDispatched(ctx context.Context, lease domain.IntegrationMergeLease, journal domain.IntegrationMergeJournal) (domain.IntegrationMergeOutcome, error) {
	if !validDispatchFence(journal.DispatchOwner, journal.DispatchFence) {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "invalid_dispatch_fence", nil)
	}
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
	defer cancel()
	candidate, err := s.broker.Candidate(reconcileCtx, lease.Repository, lease.PRNumber)
	if err == nil && mergedOperationProven(lease, candidate) {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeMerged, "", candidate.MergeCommitSHA, candidate.MergedAt)
		return s.recordReconciledResult(reconcileCtx, journal.IdempotencyKey, outcome)
	}

	// Re-entering this same Service is safe because its process-local gate is
	// held: any prior Broker.Merge invocation by this owner has returned. A
	// foreign owner requires an affirmative non-timeout death proof before any
	// non-success terminal write, even when its candidate read looks invalid;
	// otherwise it could race the live owner's already-confirmed broker call.
	ownerMayTerminalize := s.locallyOwnsDispatchFence(journal)
	if !ownerMayTerminalize {
		liveness, verifyErr := s.ownerVerifier.VerifyDispatchOwner(reconcileCtx, journal.DispatchOwner)
		ownerMayTerminalize = verifyErr == nil && liveness == DispatchOwnerDead
	}
	if !ownerMayTerminalize {
		return domain.IntegrationMergeOutcome{}, opError(CodeReconciliationInProgress, "dispatch_reconciliation_in_progress", nil)
	}

	// Once the merge call has returned uncertain, the authoritative read model
	// can lag the accepted side effect. Poll only after local ownership or
	// trusted owner-death proof; this never dispatches and keeps a live foreign
	// owner's retry responsive. Exact success wins as soon as it is visible.
	candidate, err = s.settleDispatchedCandidate(reconcileCtx, lease, candidate, err)
	if err == nil && mergedOperationProven(lease, candidate) {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeMerged, "", candidate.MergeCommitSHA, candidate.MergedAt)
		return s.recordReconciledResult(reconcileCtx, journal.IdempotencyKey, outcome)
	}
	if err == nil && (candidate.Merged || candidate.Ambiguous) {
		outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeAmbiguous, "authorized_merge_not_proven", "", s.now().UTC())
		return s.recordFencedAmbiguous(reconcileCtx, journal, outcome)
	}
	if err == nil {
		if candidateReason := candidateRevalidationReason(lease, candidate); candidateReason != "" {
			outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeRevalidationRequired, candidateReason, "", s.now().UTC())
			return s.recordFencedResult(reconcileCtx, journal, outcome)
		}
	}
	reason := "authorized_merge_not_proven"
	if err != nil {
		reason = "authoritative_reconciliation_unavailable"
	}
	outcome := outcomeFromCandidate(lease, journal, candidate, domain.IntegrationMergeOutcomeAmbiguous, reason, "", s.now().UTC())
	return s.recordFencedAmbiguous(reconcileCtx, journal, outcome)
}

func (s *Service) settleDispatchedCandidate(ctx context.Context, lease domain.IntegrationMergeLease, candidate domain.IntegrationMergeCandidate, candidateErr error) (domain.IntegrationMergeCandidate, error) {
	if candidateErr == nil && mergedOperationProven(lease, candidate) {
		return candidate, nil
	}
	timer := time.NewTimer(reconciliationPollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return candidate, candidateErr
		case <-timer.C:
			candidate, candidateErr = s.broker.Candidate(ctx, lease.Repository, lease.PRNumber)
			if candidateErr == nil && mergedOperationProven(lease, candidate) {
				return candidate, nil
			}
			timer.Reset(reconciliationPollInterval)
		}
	}
}

func (s *Service) recordPreDispatchResult(ctx context.Context, key string, outcome domain.IntegrationMergeOutcome) (domain.IntegrationMergeOutcome, error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
	defer cancel()
	journal, ok, err := s.store.RecordIntegrationMergePreDispatchResult(persistCtx, key, outcome, outcome.CompletedAt)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "result_persistence_failed", err)
	}
	return persistedOutcome(journal, ok, "result_transition_rejected")
}

func (s *Service) recordFencedResult(ctx context.Context, journal domain.IntegrationMergeJournal, outcome domain.IntegrationMergeOutcome) (domain.IntegrationMergeOutcome, error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
	defer cancel()
	stored, ok, err := s.store.RecordIntegrationMergeFencedResult(persistCtx, journal.IdempotencyKey, journal.DispatchOwner, journal.DispatchFence, outcome, outcome.CompletedAt)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "result_persistence_failed", err)
	}
	result, resultErr := persistedOutcome(stored, ok, "result_transition_rejected")
	if resultErr == nil {
		delete(s.ownedFences, journal.IdempotencyKey)
	}
	return result, resultErr
}

func (s *Service) recordReconciledResult(ctx context.Context, key string, outcome domain.IntegrationMergeOutcome) (domain.IntegrationMergeOutcome, error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
	defer cancel()
	journal, ok, err := s.store.RecordIntegrationMergeReconciledResult(persistCtx, key, outcome, outcome.CompletedAt)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "result_persistence_failed", err)
	}
	result, resultErr := persistedOutcome(journal, ok, "result_transition_rejected")
	if resultErr == nil {
		delete(s.ownedFences, key)
	}
	return result, resultErr
}

func (s *Service) recordRefinedResult(ctx context.Context, key string, outcome domain.IntegrationMergeOutcome) (domain.IntegrationMergeOutcome, error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
	defer cancel()
	journal, ok, err := s.store.RecordIntegrationMergeRefinedResult(persistCtx, key, outcome, outcome.CompletedAt)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "result_persistence_failed", err)
	}
	result, resultErr := persistedOutcome(journal, ok, "result_refinement_rejected")
	if resultErr == nil {
		delete(s.ownedFences, key)
	}
	return result, resultErr
}

func (s *Service) recordFencedAmbiguous(ctx context.Context, journal domain.IntegrationMergeJournal, outcome domain.IntegrationMergeOutcome) (domain.IntegrationMergeOutcome, error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.reconcileLimit)
	defer cancel()
	stored, ok, err := s.store.RecordIntegrationMergeFencedAmbiguous(persistCtx, journal.IdempotencyKey, journal.DispatchOwner, journal.DispatchFence, outcome, outcome.CompletedAt)
	if err != nil {
		return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "ambiguity_persistence_failed", err)
	}
	result, resultErr := persistedOutcome(stored, ok, "ambiguity_transition_rejected")
	if resultErr == nil {
		delete(s.ownedFences, journal.IdempotencyKey)
	}
	return result, resultErr
}

func (s *Service) locallyOwnsDispatchFence(journal domain.IntegrationMergeJournal) bool {
	if journal.DispatchOwner != s.dispatchOwner {
		return false
	}
	fence, ok := s.ownedFences[journal.IdempotencyKey]
	return ok && subtle.ConstantTimeCompare([]byte(fence), []byte(journal.DispatchFence)) == 1
}

func persistedOutcome(journal domain.IntegrationMergeJournal, changed bool, rejectedReason string) (domain.IntegrationMergeOutcome, error) {
	if journal.Outcome != nil && (journal.State == domain.IntegrationMergeResult || journal.State == domain.IntegrationMergeAmbiguous) {
		return cloneOutcome(*journal.Outcome), nil
	}
	if !changed && journal.State == domain.IntegrationMergeDispatched {
		return domain.IntegrationMergeOutcome{}, opError(CodeReconciliationInProgress, "dispatch_reconciliation_in_progress", nil)
	}
	return domain.IntegrationMergeOutcome{}, opError(CodeInternal, rejectedReason, nil)
}

func terminalOrInProgress(journal domain.IntegrationMergeJournal, reason string) (domain.IntegrationMergeOutcome, error) {
	if journal.Outcome != nil && (journal.State == domain.IntegrationMergeResult || journal.State == domain.IntegrationMergeAmbiguous) {
		return cloneOutcome(*journal.Outcome), nil
	}
	if journal.State == domain.IntegrationMergeDispatched {
		return domain.IntegrationMergeOutcome{}, opError(CodeReconciliationInProgress, reason, nil)
	}
	return domain.IntegrationMergeOutcome{}, opError(CodeInternal, "dispatch_fence_transition_rejected", nil)
}

func (s *Service) authorize(ctx context.Context) error {
	if s == nil || s.authorizer == nil || s.broker == nil || s.store == nil || s.ownerVerifier == nil || !validDispatchOwner(s.dispatchOwner) {
		return opError(CodeNotConfigured, "managed_control_merge_broker_and_dispatch_fence_required", nil)
	}
	if err := s.authorizer.AuthorizeRoot(ctx); err != nil {
		return opError(CodeUnauthorized, "root_authentication_required", err)
	}
	return nil
}

func normalizeLeaseInput(in IssueLeaseInput, now time.Time) (domain.IntegrationMergeLease, error) {
	repository, err := canonicalGitHubRepository(in.Repository)
	if err != nil {
		return domain.IntegrationMergeLease{}, err
	}
	sourceRepository, err := canonicalGitHubRepository(in.SourceRepository)
	if err != nil {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "invalid_source_repository", err)
	}
	baseRepository, err := canonicalGitHubRepository(in.BaseRepository)
	if err != nil {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "invalid_base_repository", err)
	}
	if repository != baseRepository {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "repository_must_equal_base_repository", nil)
	}
	if in.PRNumber <= 0 || in.PRNumber > 1_000_000_000 {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "invalid_pr_number", nil)
	}
	if !validBranch(in.SourceBranch) {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "invalid_source_branch", nil)
	}
	if !validBranch(in.BaseBranch) {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "invalid_base_branch", nil)
	}
	head, ok := normalizeSHA(in.ExpectedHeadSHA)
	if !ok {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "expected_head_sha_must_be_full", nil)
	}
	strategy, ok := normalizeStrategy(in.Strategy)
	if !ok {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "unsupported_merge_strategy", nil)
	}
	checkPolicy, err := normalizeCheckPolicy(in.CheckPolicy)
	if err != nil {
		return domain.IntegrationMergeLease{}, err
	}
	reviewPolicy, err := normalizeReviewPolicy(in.ReviewPolicy)
	if err != nil {
		return domain.IntegrationMergeLease{}, err
	}
	expiresAt := in.ExpiresAt.UTC()
	if expiresAt.IsZero() {
		expiresAt = now.Add(defaultLeaseTTL)
	}
	if !expiresAt.After(now) || expiresAt.After(now.Add(maxLeaseTTL)) {
		return domain.IntegrationMergeLease{}, opError(CodeInvalidInput, "lease_expiry_out_of_bounds", nil)
	}
	return domain.IntegrationMergeLease{
		Repository:             repository,
		SourceRepository:       sourceRepository,
		PRNumber:               in.PRNumber,
		SourceBranch:           in.SourceBranch,
		ExpectedHeadSHA:        head,
		BaseRepository:         baseRepository,
		BaseBranch:             in.BaseBranch,
		Strategy:               strategy,
		CheckPolicy:            checkPolicy,
		ReviewPolicy:           reviewPolicy,
		ManualApprovalRequired: in.ManualApprovalRequired,
		ExpiresAt:              expiresAt,
	}, nil
}

func validateMergeInput(in MergeInput) error {
	if err := validateLeaseID(in.LeaseID); err != nil {
		return err
	}
	if len(in.GateCapability) < 32 || len(in.GateCapability) > 128 || strings.ContainsAny(in.GateCapability, "\r\n\t ") {
		return opError(CodeInvalidInput, "invalid_capability_shape", nil)
	}
	if len(in.IdempotencyKey) < 8 || len(in.IdempotencyKey) > 128 || !idempotencyPattern.MatchString(in.IdempotencyKey) {
		return opError(CodeInvalidInput, "invalid_idempotency_key", nil)
	}
	return nil
}

func validateLeaseID(id string) error {
	if len(id) < 16 || len(id) > 64 || !leaseIDPattern.MatchString(id) {
		return opError(CodeInvalidInput, "invalid_lease_id", nil)
	}
	return nil
}

func canonicalRequestHash(lease domain.IntegrationMergeLease, manualApproval bool) ([]byte, error) {
	payload := struct {
		Version                int                            `json:"version"`
		LeaseID                string                         `json:"leaseId"`
		Repository             string                         `json:"repository"`
		SourceRepository       string                         `json:"sourceRepository"`
		PRNumber               int                            `json:"prNumber"`
		SourceBranch           string                         `json:"sourceBranch"`
		ExpectedHeadSHA        string                         `json:"expectedHeadSha"`
		BaseRepository         string                         `json:"baseRepository"`
		BaseBranch             string                         `json:"baseBranch"`
		Strategy               domain.MergeStrategy           `json:"mergeStrategy"`
		CheckPolicy            domain.IntegrationCheckPolicy  `json:"checkPolicy"`
		ReviewPolicy           domain.IntegrationReviewPolicy `json:"reviewPolicy"`
		ManualApprovalRequired bool                           `json:"manualApprovalRequired"`
		ManualApproval         bool                           `json:"manualApproval"`
	}{
		Version: ContractVersion, LeaseID: lease.ID, Repository: lease.Repository,
		SourceRepository: lease.SourceRepository, PRNumber: lease.PRNumber,
		SourceBranch: lease.SourceBranch, ExpectedHeadSHA: lease.ExpectedHeadSHA,
		BaseRepository: lease.BaseRepository, BaseBranch: lease.BaseBranch,
		Strategy: lease.Strategy, CheckPolicy: lease.CheckPolicy,
		ReviewPolicy: lease.ReviewPolicy, ManualApprovalRequired: lease.ManualApprovalRequired,
		ManualApproval: manualApproval,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(encoded)
	return append([]byte(nil), hash[:]...), nil
}

func candidateRevalidationReason(lease domain.IntegrationMergeLease, candidate domain.IntegrationMergeCandidate) string {
	if candidate.Ambiguous {
		return "candidate_ambiguous"
	}
	if candidate.Merged {
		return "candidate_already_merged"
	}
	if !candidateIdentityMatches(lease, candidate) {
		return "candidate_identity_changed"
	}
	switch candidate.Mergeability {
	case "clean":
	case "conflicting":
		return "merge_conflict"
	case "behind":
		return "base_update_required"
	case "blocked":
		return "candidate_blocked"
	default:
		return "mergeability_unknown"
	}
	strategy, ok := normalizeStrategy(candidate.ConfiguredStrategy)
	if !ok || strategy != lease.Strategy {
		return "merge_strategy_drift"
	}
	checkPolicy, err := normalizeCheckPolicy(candidate.CheckPolicy)
	if err != nil || !equalCanonical(lease.CheckPolicy, checkPolicy) {
		return "check_policy_drift"
	}
	reviewPolicy, err := normalizeReviewPolicy(candidate.ReviewPolicy)
	if err != nil || !equalCanonical(lease.ReviewPolicy, reviewPolicy) {
		return "review_policy_drift"
	}
	if len(candidate.Checks) > maxEvidence {
		return "check_evidence_unbounded"
	}
	checks := make(map[string]domain.IntegrationCheckEvidence, len(candidate.Checks))
	observedPending := false
	observedFailing := false
	for _, check := range candidate.Checks {
		checkHead, validHead := normalizeSHA(check.HeadSHA)
		if check.Name == "" || len(check.Name) > 128 || !validHead || checkHead != lease.ExpectedHeadSHA {
			return "check_evidence_not_exact_head"
		}
		if check.CompletedAt.IsZero() {
			return "invalid_check_evidence"
		}
		if _, duplicate := checks[check.Name]; duplicate {
			return "duplicate_check_evidence"
		}
		switch check.Status {
		case "passing":
		case "pending":
			observedPending = true
		case "failing":
			observedFailing = true
		default:
			return "invalid_check_evidence"
		}
		checks[check.Name] = check
	}
	for _, required := range lease.CheckPolicy.RequiredChecks {
		check, ok := checks[required]
		if !ok {
			return "required_check_missing"
		}
		if check.Status == "pending" {
			return "checks_pending"
		}
		if check.Status != "passing" {
			return "checks_failing"
		}
	}
	if lease.CheckPolicy.RequireAllObservedPassing {
		if observedPending {
			return "checks_pending"
		}
		if observedFailing {
			return "checks_failing"
		}
	}
	if len(candidate.Reviews) > maxEvidence {
		return "review_evidence_unbounded"
	}
	approvals := make(map[string]struct{})
	exactHeadReviewers := make(map[string]struct{})
	for _, review := range candidate.Reviews {
		name := strings.ToLower(strings.TrimSpace(review.Reviewer))
		if name == "" || len(name) > 128 {
			return "invalid_review_evidence"
		}
		reviewHead, validHead := normalizeSHA(review.HeadSHA)
		if !validHead {
			return "invalid_review_evidence"
		}
		if review.SubmittedAt.IsZero() {
			return "invalid_review_evidence"
		}
		switch review.Decision {
		case "approved", "changes_requested", "commented", "dismissed":
		default:
			return "invalid_review_evidence"
		}
		if reviewHead != lease.ExpectedHeadSHA {
			continue
		}
		if _, duplicate := exactHeadReviewers[name]; duplicate {
			return "duplicate_review_evidence"
		}
		exactHeadReviewers[name] = struct{}{}
		if review.Decision == "changes_requested" {
			return "changes_requested"
		}
		if review.Decision == "approved" {
			approvals[name] = struct{}{}
		}
	}
	if len(approvals) < lease.ReviewPolicy.RequiredApprovals {
		return "exact_head_review_missing"
	}
	for _, reviewer := range lease.ReviewPolicy.RequiredReviewers {
		if _, ok := approvals[reviewer]; !ok {
			return "required_reviewer_missing"
		}
	}
	if candidate.UnresolvedReviewThreads < 0 || candidate.UnresolvedReviewThreads > 100_000 {
		return "invalid_review_thread_count"
	}
	if lease.ReviewPolicy.RequireResolvedThreads && candidate.UnresolvedReviewThreads != 0 {
		return "unresolved_review_threads"
	}
	return ""
}

func candidateIdentityMatches(lease domain.IntegrationMergeLease, candidate domain.IntegrationMergeCandidate) bool {
	repository, err := canonicalGitHubRepository(candidate.Repository)
	if err != nil || repository != lease.Repository {
		return false
	}
	sourceRepository, err := canonicalGitHubRepository(candidate.SourceRepository)
	if err != nil || sourceRepository != lease.SourceRepository {
		return false
	}
	baseRepository, err := canonicalGitHubRepository(candidate.BaseRepository)
	if err != nil || baseRepository != lease.BaseRepository {
		return false
	}
	head, ok := normalizeSHA(candidate.HeadSHA)
	return ok && candidate.PRNumber == lease.PRNumber && candidate.SourceBranch == lease.SourceBranch &&
		head == lease.ExpectedHeadSHA && candidate.BaseBranch == lease.BaseBranch
}

func mergedOperationProven(lease domain.IntegrationMergeLease, candidate domain.IntegrationMergeCandidate) bool {
	if candidate.Ambiguous || !candidate.Merged || !candidateIdentityMatches(lease, candidate) {
		return false
	}
	mergedHead, ok := normalizeSHA(candidate.MergedExpectedHeadSHA)
	if !ok || mergedHead != lease.ExpectedHeadSHA {
		return false
	}
	commit, ok := normalizeSHA(candidate.MergeCommitSHA)
	if !ok || commit == "" {
		return false
	}
	strategy, ok := normalizeStrategy(candidate.MergeStrategyEvidence)
	if !ok || strategy != lease.Strategy {
		return false
	}
	// Reconciliation also needs the original exact-head gate evidence. Evaluate
	// it against an otherwise unmerged/clean view so merged state itself is not
	// treated as a pre-dispatch rejection.
	evidence := candidate
	evidence.Merged = false
	evidence.Mergeability = "clean"
	return candidateRevalidationReason(lease, evidence) == ""
}

func validReceipt(lease domain.IntegrationMergeLease, receipt domain.IntegrationBrokerMergeResult) bool {
	repository, err := canonicalGitHubRepository(receipt.Repository)
	if err != nil || repository != lease.Repository || receipt.PRNumber != lease.PRNumber {
		return false
	}
	head, ok := normalizeSHA(receipt.ExpectedHeadSHA)
	if !ok || head != lease.ExpectedHeadSHA || receipt.Strategy != lease.Strategy {
		return false
	}
	_, ok = normalizeSHA(receipt.MergeCommitSHA)
	return ok && !receipt.MergedAt.IsZero()
}

func outcomeFromCandidate(lease domain.IntegrationMergeLease, journal domain.IntegrationMergeJournal, candidate domain.IntegrationMergeCandidate, status domain.IntegrationMergeOutcomeStatus, reason, mergeCommit string, completedAt time.Time) domain.IntegrationMergeOutcome {
	checks := append([]domain.IntegrationCheckEvidence(nil), candidate.Checks...)
	reviews := append([]domain.IntegrationReviewEvidence(nil), candidate.Reviews...)
	if checks == nil {
		checks = []domain.IntegrationCheckEvidence{}
	}
	if reviews == nil {
		reviews = []domain.IntegrationReviewEvidence{}
	}
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	return domain.IntegrationMergeOutcome{
		Version: ContractVersion, Status: status, Reason: reason,
		IdempotencyKey: journal.IdempotencyKey, Repository: lease.Repository,
		SourceRepository: lease.SourceRepository, PRNumber: lease.PRNumber,
		SourceBranch: lease.SourceBranch, ExpectedHeadSHA: lease.ExpectedHeadSHA,
		BaseRepository: lease.BaseRepository, BaseBranch: lease.BaseBranch,
		Strategy: lease.Strategy, CheckPolicy: lease.CheckPolicy, Checks: checks,
		ReviewPolicy: lease.ReviewPolicy, Reviews: reviews,
		UnresolvedReviewThreads: candidate.UnresolvedReviewThreads,
		MergeCommitSHA:          mergeCommit, AcceptedAt: journal.AcceptedAt,
		DispatchedAt: journal.DispatchedAt, CompletedAt: completedAt.UTC(),
	}
}

func cloneOutcome(in domain.IntegrationMergeOutcome) domain.IntegrationMergeOutcome {
	out := in
	out.Checks = append([]domain.IntegrationCheckEvidence(nil), in.Checks...)
	out.Reviews = append([]domain.IntegrationReviewEvidence(nil), in.Reviews...)
	out.CheckPolicy.RequiredChecks = append([]string(nil), in.CheckPolicy.RequiredChecks...)
	out.ReviewPolicy.RequiredReviewers = append([]string(nil), in.ReviewPolicy.RequiredReviewers...)
	return out
}

func capabilityMatches(stored []byte, capability string) bool {
	provided := sha256.Sum256([]byte(capability))
	return len(stored) == sha256.Size && subtle.ConstantTimeCompare(stored, provided[:]) == 1
}

func randomToken(r io.Reader, prefix string, bytes int) (string, error) {
	buf := make([]byte, bytes)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(buf), nil
}

func canonicalGitHubRepository(raw string) (string, error) {
	if len(raw) == 0 || len(raw) > 256 || !utf8.ValidString(raw) {
		return "", opError(CodeInvalidInput, "invalid_repository", nil)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Hostname(), "github.com") || u.Port() != "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return "", opError(CodeInvalidInput, "repository_must_be_canonical_github_https", err)
	}
	path := strings.TrimSuffix(strings.Trim(u.EscapedPath(), "/"), ".git")
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) != 2 || !repoPartPattern.MatchString(parts[0]) || !repoPartPattern.MatchString(parts[1]) {
		return "", opError(CodeInvalidInput, "repository_must_name_owner_and_repo", nil)
	}
	return "https://github.com/" + strings.ToLower(parts[0]) + "/" + strings.ToLower(parts[1]), nil
}

func normalizeSHA(raw string) (string, bool) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if len(value) != 40 {
		return "", false
	}
	decoded, err := hex.DecodeString(value)
	return value, err == nil && len(decoded) == 20
}

func normalizeStrategy(strategy domain.MergeStrategy) (domain.MergeStrategy, bool) {
	if strategy == "" {
		return domain.MergeStrategySquash, true
	}
	switch strategy {
	case domain.MergeStrategySquash, domain.MergeStrategyMerge, domain.MergeStrategyRebase:
		return strategy, true
	default:
		return "", false
	}
}

func normalizeCheckPolicy(policy domain.IntegrationCheckPolicy) (domain.IntegrationCheckPolicy, error) {
	policy.Revision = strings.TrimSpace(policy.Revision)
	if policy.Revision == "" || len(policy.Revision) > 128 || len(policy.RequiredChecks) > maxPolicyItems {
		return domain.IntegrationCheckPolicy{}, opError(CodeInvalidInput, "invalid_check_policy", nil)
	}
	seen := make(map[string]struct{}, len(policy.RequiredChecks))
	out := make([]string, 0, len(policy.RequiredChecks))
	for _, item := range policy.RequiredChecks {
		item = strings.TrimSpace(item)
		if item == "" || len(item) > 128 || strings.ContainsAny(item, "\r\n") {
			return domain.IntegrationCheckPolicy{}, opError(CodeInvalidInput, "invalid_required_check", nil)
		}
		if _, exists := seen[item]; exists {
			return domain.IntegrationCheckPolicy{}, opError(CodeInvalidInput, "duplicate_required_check", nil)
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	policy.RequiredChecks = out
	return policy, nil
}

func normalizeReviewPolicy(policy domain.IntegrationReviewPolicy) (domain.IntegrationReviewPolicy, error) {
	policy.Revision = strings.TrimSpace(policy.Revision)
	if policy.Revision == "" || len(policy.Revision) > 128 || policy.RequiredApprovals < 0 ||
		policy.RequiredApprovals > 100 || len(policy.RequiredReviewers) > maxPolicyItems {
		return domain.IntegrationReviewPolicy{}, opError(CodeInvalidInput, "invalid_review_policy", nil)
	}
	seen := make(map[string]struct{}, len(policy.RequiredReviewers))
	out := make([]string, 0, len(policy.RequiredReviewers))
	for _, reviewer := range policy.RequiredReviewers {
		reviewer = strings.ToLower(strings.TrimSpace(reviewer))
		if reviewer == "" || len(reviewer) > 128 || strings.ContainsAny(reviewer, "\r\n") {
			return domain.IntegrationReviewPolicy{}, opError(CodeInvalidInput, "invalid_required_reviewer", nil)
		}
		if _, exists := seen[reviewer]; exists {
			return domain.IntegrationReviewPolicy{}, opError(CodeInvalidInput, "duplicate_required_reviewer", nil)
		}
		seen[reviewer] = struct{}{}
		out = append(out, reviewer)
	}
	sort.Strings(out)
	policy.RequiredReviewers = out
	return policy, nil
}

func equalCanonical(a, b any) bool {
	aJSON, aErr := json.Marshal(a)
	bJSON, bErr := json.Marshal(b)
	return aErr == nil && bErr == nil && subtle.ConstantTimeCompare(aJSON, bJSON) == 1
}

func validBranch(branch string) bool {
	if branch == "" || len(branch) > 255 || !utf8.ValidString(branch) || strings.HasPrefix(branch, "/") ||
		strings.HasSuffix(branch, "/") || strings.HasSuffix(branch, ".") || strings.Contains(branch, "..") ||
		strings.Contains(branch, "@{") || strings.ContainsAny(branch, " ~^:?*[\\\r\n\t") {
		return false
	}
	return true
}

func validDispatchOwner(owner string) bool {
	return len(owner) >= 8 && len(owner) <= 128 && dispatchOwnerPattern.MatchString(owner)
}

func validDispatchFence(owner, fence string) bool {
	return validDispatchOwner(owner) && len(fence) >= 16 && len(fence) <= 64 && dispatchFencePattern.MatchString(fence)
}

func leaseStateError(status domain.IntegrationMergeLeaseStatus) error {
	switch status {
	case domain.IntegrationMergeLeaseConsumed:
		return opError(CodeLeaseConsumed, "lease_already_consumed", nil)
	case domain.IntegrationMergeLeaseRevoked:
		return opError(CodeLeaseRevoked, "lease_revoked", nil)
	case domain.IntegrationMergeLeaseExpired:
		return opError(CodeLeaseExpired, "lease_expired", nil)
	default:
		return opError(CodeInternal, "lease_transition_rejected", nil)
	}
}

var (
	repoPartPattern      = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)
	leaseIDPattern       = regexp.MustCompile(`^iml_[A-Za-z0-9_-]+$`)
	idempotencyPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	dispatchOwnerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	dispatchFencePattern = regexp.MustCompile(`^imf_[A-Za-z0-9_-]+$`)
)

// Keep fmt referenced in builds where error wrapping gets optimized away by
// test-only substitutions; it also gives broker implementations a canonical
// command formatter without ever including capability material.
func (c BrokerMergeCommand) String() string {
	return fmt.Sprintf("%s#%d@%s:%s", c.Repository, c.PRNumber, c.ExpectedHeadSHA, c.Strategy)
}
