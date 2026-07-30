package integrationmerge

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

var testNow = time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)

type allowRoot struct{ err error }

func (a allowRoot) AuthorizeRoot(context.Context) error { return a.err }

type fixedOwnerVerifier struct {
	liveness DispatchOwnerLiveness
	err      error
}

func (v fixedOwnerVerifier) VerifyDispatchOwner(context.Context, string) (DispatchOwnerLiveness, error) {
	return v.liveness, v.err
}

type fakeStore struct {
	lease          domain.IntegrationMergeLease
	journal        domain.IntegrationMergeJournal
	failResultOnce bool
}

func (f *fakeStore) CreateIntegrationMergeLease(_ context.Context, lease domain.IntegrationMergeLease, now time.Time) (bool, error) {
	if f.lease.Status == domain.IntegrationMergeLeaseActive && !f.lease.ExpiresAt.After(now) {
		f.lease.Status = domain.IntegrationMergeLeaseExpired
	}
	if f.lease.Status == domain.IntegrationMergeLeaseActive {
		return false, nil
	}
	f.lease = cloneLease(lease)
	return true, nil
}

func (f *fakeStore) GetIntegrationMergeLease(_ context.Context, id string) (domain.IntegrationMergeLease, bool, error) {
	if f.lease.ID != id {
		return domain.IntegrationMergeLease{}, false, nil
	}
	return cloneLease(f.lease), true, nil
}

func (f *fakeStore) RevokeIntegrationMergeLease(_ context.Context, id string, now time.Time) (domain.IntegrationMergeLease, bool, error) {
	if f.lease.ID != id {
		return domain.IntegrationMergeLease{}, false, nil
	}
	if f.lease.Status == domain.IntegrationMergeLeaseActive && !f.lease.ExpiresAt.After(now) {
		f.lease.Status = domain.IntegrationMergeLeaseExpired
	}
	if f.lease.Status != domain.IntegrationMergeLeaseActive {
		return cloneLease(f.lease), false, nil
	}
	f.lease.Status = domain.IntegrationMergeLeaseRevoked
	f.lease.RevokedAt = now
	return cloneLease(f.lease), true, nil
}

func (f *fakeStore) GetIntegrationMergeJournal(_ context.Context, key string) (domain.IntegrationMergeJournal, bool, error) {
	if f.journal.IdempotencyKey != key {
		return domain.IntegrationMergeJournal{}, false, nil
	}
	return cloneJournal(f.journal), true, nil
}

func (f *fakeStore) AcceptIntegrationMerge(_ context.Context, journal domain.IntegrationMergeJournal, now time.Time) (bool, domain.IntegrationMergeLeaseStatus, error) {
	if f.lease.Status == domain.IntegrationMergeLeaseActive && !f.lease.ExpiresAt.After(now) {
		f.lease.Status = domain.IntegrationMergeLeaseExpired
	}
	if f.lease.Status != domain.IntegrationMergeLeaseActive {
		return false, f.lease.Status, nil
	}
	f.lease.Status = domain.IntegrationMergeLeaseConsumed
	f.lease.ConsumedAt = now
	f.journal = cloneJournal(journal)
	return true, f.lease.Status, nil
}

func (f *fakeStore) MarkIntegrationMergeDispatched(_ context.Context, key, owner, fence string, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	if f.journal.IdempotencyKey != key {
		return domain.IntegrationMergeJournal{}, false, errors.New("missing journal")
	}
	if f.journal.State != domain.IntegrationMergeAccepted {
		return cloneJournal(f.journal), false, nil
	}
	f.journal.State = domain.IntegrationMergeDispatched
	f.journal.DispatchedAt = at
	f.journal.DispatchOwner = owner
	f.journal.DispatchFence = fence
	return cloneJournal(f.journal), true, nil
}

func (f *fakeStore) ConfirmIntegrationMergeDispatch(_ context.Context, key, owner, fence string) (domain.IntegrationMergeJournal, bool, error) {
	if f.journal.IdempotencyKey != key {
		return domain.IntegrationMergeJournal{}, false, errors.New("missing journal")
	}
	ok := f.journal.State == domain.IntegrationMergeDispatched && f.journal.DispatchOwner == owner && f.journal.DispatchFence == fence
	return cloneJournal(f.journal), ok, nil
}

func (f *fakeStore) RecordIntegrationMergePreDispatchResult(_ context.Context, key string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	return f.recordResult(key, "", "", outcome, at, false)
}

func (f *fakeStore) RecordIntegrationMergeFencedResult(_ context.Context, key, owner, fence string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	return f.recordResult(key, owner, fence, outcome, at, true)
}

func (f *fakeStore) RecordIntegrationMergeReconciledResult(_ context.Context, key string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	return f.recordResult(key, "", "", outcome, at, true)
}

func (f *fakeStore) RecordIntegrationMergeRefinedResult(_ context.Context, key string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	if f.journal.IdempotencyKey != key || f.journal.State != domain.IntegrationMergeAmbiguous {
		return cloneJournal(f.journal), false, nil
	}
	f.journal.State = domain.IntegrationMergeResult
	f.journal.CompletedAt = at
	copy := cloneOutcome(outcome)
	f.journal.Outcome = &copy
	return cloneJournal(f.journal), true, nil
}

func (f *fakeStore) recordResult(key, owner, fence string, outcome domain.IntegrationMergeOutcome, at time.Time, dispatched bool) (domain.IntegrationMergeJournal, bool, error) {
	if f.failResultOnce {
		f.failResultOnce = false
		return domain.IntegrationMergeJournal{}, false, errors.New("injected database failure")
	}
	if f.journal.IdempotencyKey != key || (dispatched && f.journal.State != domain.IntegrationMergeDispatched) || (!dispatched && f.journal.State != domain.IntegrationMergeAccepted) {
		return cloneJournal(f.journal), false, nil
	}
	if owner != "" && (f.journal.DispatchOwner != owner || f.journal.DispatchFence != fence) {
		return cloneJournal(f.journal), false, nil
	}
	f.journal.State = domain.IntegrationMergeResult
	f.journal.CompletedAt = at
	copy := cloneOutcome(outcome)
	f.journal.Outcome = &copy
	return cloneJournal(f.journal), true, nil
}

func (f *fakeStore) RecordIntegrationMergeFencedAmbiguous(_ context.Context, key, owner, fence string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	if f.journal.IdempotencyKey != key || f.journal.State != domain.IntegrationMergeDispatched || f.journal.DispatchOwner != owner || f.journal.DispatchFence != fence {
		return cloneJournal(f.journal), false, nil
	}
	f.journal.State = domain.IntegrationMergeAmbiguous
	f.journal.CompletedAt = at
	copy := cloneOutcome(outcome)
	f.journal.Outcome = &copy
	return cloneJournal(f.journal), true, nil
}

type fakeBroker struct {
	candidate    domain.IntegrationMergeCandidate
	candidateErr error
	mergeErr     error
	mergeCalls   int
	commands     []BrokerMergeCommand
	noSideEffect bool
}

func (f *fakeBroker) Candidate(context.Context, string, int) (domain.IntegrationMergeCandidate, error) {
	return cloneCandidate(f.candidate), f.candidateErr
}

func (f *fakeBroker) Merge(_ context.Context, command BrokerMergeCommand) (domain.IntegrationBrokerMergeResult, error) {
	f.mergeCalls++
	f.commands = append(f.commands, command)
	result := domain.IntegrationBrokerMergeResult{
		Repository: command.Repository, PRNumber: command.PRNumber,
		ExpectedHeadSHA: command.ExpectedHeadSHA, Strategy: command.Strategy,
		MergeCommitSHA: strings.Repeat("b", 40), MergedAt: testNow.Add(time.Minute),
	}
	if !f.noSideEffect {
		f.candidate.Merged = true
		f.candidate.MergedExpectedHeadSHA = command.ExpectedHeadSHA
		f.candidate.MergeCommitSHA = result.MergeCommitSHA
		f.candidate.MergeStrategyEvidence = command.Strategy
		f.candidate.MergedAt = result.MergedAt
	}
	return result, f.mergeErr
}

type fault struct {
	before error
	after  error
}

func (f fault) BeforeBrokerMerge(context.Context) error { return f.before }
func (f fault) AfterBrokerMerge(context.Context, domain.IntegrationBrokerMergeResult) error {
	return f.after
}

func TestCandidateRevalidationMatrix(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.IntegrationMergeCandidate)
		reason string
	}{
		{"wrong repository", func(c *domain.IntegrationMergeCandidate) { c.Repository = "https://github.com/acme/other" }, "candidate_identity_changed"},
		{"wrong source repository", func(c *domain.IntegrationMergeCandidate) { c.SourceRepository = "https://github.com/acme/fork" }, "candidate_identity_changed"},
		{"wrong PR", func(c *domain.IntegrationMergeCandidate) { c.PRNumber++ }, "candidate_identity_changed"},
		{"stale head", func(c *domain.IntegrationMergeCandidate) { c.HeadSHA = strings.Repeat("c", 40) }, "candidate_identity_changed"},
		{"wrong branch", func(c *domain.IntegrationMergeCandidate) { c.SourceBranch = "feature/other" }, "candidate_identity_changed"},
		{"wrong base", func(c *domain.IntegrationMergeCandidate) { c.BaseBranch = "release" }, "candidate_identity_changed"},
		{"wrong base repository", func(c *domain.IntegrationMergeCandidate) { c.BaseRepository = "https://github.com/acme/other" }, "candidate_identity_changed"},
		{"conflict", func(c *domain.IntegrationMergeCandidate) { c.Mergeability = "conflicting" }, "merge_conflict"},
		{"update needed", func(c *domain.IntegrationMergeCandidate) { c.Mergeability = "behind" }, "base_update_required"},
		{"blocked", func(c *domain.IntegrationMergeCandidate) { c.Mergeability = "blocked" }, "candidate_blocked"},
		{"ambiguous", func(c *domain.IntegrationMergeCandidate) { c.Ambiguous = true }, "candidate_ambiguous"},
		{"pending check", func(c *domain.IntegrationMergeCandidate) { c.Checks[0].Status = "pending" }, "checks_pending"},
		{"failing check", func(c *domain.IntegrationMergeCandidate) { c.Checks[0].Status = "failing" }, "checks_failing"},
		{"missing check", func(c *domain.IntegrationMergeCandidate) { c.Checks = c.Checks[1:] }, "required_check_missing"},
		{"stale check", func(c *domain.IntegrationMergeCandidate) { c.Checks[0].HeadSHA = strings.Repeat("c", 40) }, "check_evidence_not_exact_head"},
		{"undated check", func(c *domain.IntegrationMergeCandidate) { c.Checks[0].CompletedAt = time.Time{} }, "invalid_check_evidence"},
		{"missing exact-head review", func(c *domain.IntegrationMergeCandidate) { c.Reviews[0].HeadSHA = strings.Repeat("c", 40) }, "exact_head_review_missing"},
		{"undated review", func(c *domain.IntegrationMergeCandidate) { c.Reviews[0].SubmittedAt = time.Time{} }, "invalid_review_evidence"},
		{"duplicate exact-head review", func(c *domain.IntegrationMergeCandidate) { c.Reviews = append(c.Reviews, c.Reviews[0]) }, "duplicate_review_evidence"},
		{"changes requested", func(c *domain.IntegrationMergeCandidate) { c.Reviews[0].Decision = "changes_requested" }, "changes_requested"},
		{"unresolved threads", func(c *domain.IntegrationMergeCandidate) { c.UnresolvedReviewThreads = 1 }, "unresolved_review_threads"},
		{"strategy drift", func(c *domain.IntegrationMergeCandidate) { c.ConfiguredStrategy = domain.MergeStrategyMerge }, "merge_strategy_drift"},
		{"check policy drift", func(c *domain.IntegrationMergeCandidate) { c.CheckPolicy.Revision = "checks-v2" }, "check_policy_drift"},
		{"review policy drift", func(c *domain.IntegrationMergeCandidate) { c.ReviewPolicy.RequiredApprovals = 2 }, "review_policy_drift"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, store, broker, issued := newTestService(t, nil)
			tc.mutate(&broker.candidate)
			_, err := svc.Merge(context.Background(), mergeInput(issued, "operation-01"))
			assertCodeReason(t, err, CodeRevalidationRequired, tc.reason)
			if broker.mergeCalls != 0 || store.lease.Status != domain.IntegrationMergeLeaseActive || store.journal.IdempotencyKey != "" {
				t.Fatalf("rejected candidate changed state: calls=%d lease=%s journal=%q", broker.mergeCalls, store.lease.Status, store.journal.IdempotencyKey)
			}
		})
	}
}

func TestDefaultSquashSuccessAndIdempotency(t *testing.T) {
	svc, store, broker, issued := newTestService(t, nil)
	in := mergeInput(issued, "operation-01")
	out, err := svc.Merge(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != domain.IntegrationMergeOutcomeMerged || out.Strategy != domain.MergeStrategySquash || len(out.Checks) != 2 || len(out.Reviews) != 1 {
		t.Fatalf("outcome = %#v", out)
	}
	if broker.mergeCalls != 1 || len(broker.commands) != 1 || broker.commands[0].ExpectedHeadSHA != strings.Repeat("a", 40) || broker.commands[0].Strategy != domain.MergeStrategySquash {
		t.Fatalf("broker calls=%d commands=%#v", broker.mergeCalls, broker.commands)
	}
	if store.lease.Status != domain.IntegrationMergeLeaseConsumed || store.journal.State != domain.IntegrationMergeResult {
		t.Fatalf("lease=%s journal=%s", store.lease.Status, store.journal.State)
	}
	retry, err := svc.Merge(context.Background(), in)
	if err != nil || retry.MergeCommitSHA != out.MergeCommitSHA || broker.mergeCalls != 1 {
		t.Fatalf("retry=%#v err=%v calls=%d", retry, err, broker.mergeCalls)
	}
	conflict := in
	conflict.ManualApproval = true
	_, err = svc.Merge(context.Background(), conflict)
	assertCodeReason(t, err, CodeIdempotencyConflict, "key_bound_to_different_request")
	newOperation := in
	newOperation.IdempotencyKey = "operation-02"
	_, err = svc.Merge(context.Background(), newOperation)
	assertCodeReason(t, err, CodeLeaseConsumed, "lease_already_consumed")
}

func TestManualApprovalPause(t *testing.T) {
	svc, store, broker, issued := newTestService(t, func(in *IssueLeaseInput) { in.ManualApprovalRequired = true })
	in := mergeInput(issued, "manual-op-01")
	_, err := svc.Merge(context.Background(), in)
	assertCodeReason(t, err, CodeManualApprovalRequired, "manual_approval_required")
	if broker.mergeCalls != 0 || store.lease.Status != domain.IntegrationMergeLeaseActive || store.journal.IdempotencyKey != "" {
		t.Fatalf("manual pause mutated state")
	}
	in.ManualApproval = true
	if _, err := svc.Merge(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if broker.mergeCalls != 1 {
		t.Fatalf("merge calls=%d", broker.mergeCalls)
	}
}

func TestExpiredRevokedAndCapabilityRejected(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		now := testNow
		svc, _, _, issued := newTestServiceAt(t, &now, nil, nil)
		now = now.Add(6 * time.Minute)
		_, err := svc.Merge(context.Background(), mergeInput(issued, "expired-op1"))
		assertCodeReason(t, err, CodeLeaseExpired, "lease_expired")
	})
	t.Run("revoked", func(t *testing.T) {
		svc, _, broker, issued := newTestService(t, nil)
		if _, err := svc.RevokeLease(context.Background(), issued.Lease.ID); err != nil {
			t.Fatal(err)
		}
		_, err := svc.Merge(context.Background(), mergeInput(issued, "revoked-op1"))
		assertCodeReason(t, err, CodeLeaseRevoked, "lease_revoked")
		if broker.mergeCalls != 0 {
			t.Fatal("revoked lease called broker")
		}
	})
	t.Run("wrong capability", func(t *testing.T) {
		svc, _, broker, issued := newTestService(t, nil)
		in := mergeInput(issued, "wrong-cap1")
		in.GateCapability = "imc_" + strings.Repeat("x", 50)
		_, err := svc.Merge(context.Background(), in)
		assertCodeReason(t, err, CodeCapabilityRejected, "capability_rejected")
		if broker.mergeCalls != 0 {
			t.Fatal("wrong capability called broker")
		}
	})
}

func TestCrashAndReconciliationNeverRedispatch(t *testing.T) {
	t.Run("restart from accepted dispatches once", func(t *testing.T) {
		now := testNow
		_, store, broker, issued := newTestServiceAt(t, &now, nil, nil)
		in := mergeInput(issued, "accepted-001")
		hash, err := canonicalRequestHash(issued.Lease, in.ManualApproval)
		if err != nil {
			t.Fatal(err)
		}
		journal := domain.IntegrationMergeJournal{
			IdempotencyKey: in.IdempotencyKey, RequestHash: hash, LeaseID: issued.Lease.ID,
			State: domain.IntegrationMergeAccepted, AcceptedAt: now,
		}
		accepted, _, err := store.AcceptIntegrationMerge(context.Background(), journal, now)
		if err != nil || !accepted {
			t.Fatalf("accept=%v err=%v", accepted, err)
		}
		restarted := mustServiceWithOwner(t, store, broker, &now, nil, "dispatch-owner-restarted", fixedOwnerVerifier{liveness: DispatchOwnerDead})
		out, err := restarted.Merge(context.Background(), in)
		if err != nil || out.Status != domain.IntegrationMergeOutcomeMerged || broker.mergeCalls != 1 {
			t.Fatalf("out=%#v err=%v calls=%d", out, err, broker.mergeCalls)
		}
	})

	t.Run("crash immediately before call", func(t *testing.T) {
		crash := errors.New("injected crash before broker")
		now := testNow
		svc, store, broker, issued := newTestServiceAt(t, &now, fault{before: crash}, nil)
		in := mergeInput(issued, "pre-call-01")
		if _, err := svc.Merge(context.Background(), in); !errors.Is(err, crash) {
			t.Fatalf("err=%v", err)
		}
		if store.journal.State != domain.IntegrationMergeDispatched || broker.mergeCalls != 0 {
			t.Fatalf("journal=%s calls=%d", store.journal.State, broker.mergeCalls)
		}
		restarted := mustServiceWithOwner(t, store, broker, &now, nil, "dispatch-owner-crash-recovery", fixedOwnerVerifier{liveness: DispatchOwnerDead})
		out, err := restarted.Merge(context.Background(), in)
		if err != nil || out.Status != domain.IntegrationMergeOutcomeAmbiguous || broker.mergeCalls != 0 {
			t.Fatalf("out=%#v err=%v calls=%d", out, err, broker.mergeCalls)
		}
		if _, err := restarted.Merge(context.Background(), in); err != nil || broker.mergeCalls != 0 {
			t.Fatalf("terminal retry err=%v calls=%d", err, broker.mergeCalls)
		}
	})

	t.Run("response loss after external side effect", func(t *testing.T) {
		now := testNow
		svc, _, broker, issued := newTestServiceAt(t, &now, nil, nil)
		broker.mergeErr = errors.New("response lost")
		in := mergeInput(issued, "response-01")
		out, err := svc.Merge(context.Background(), in)
		if err != nil || out.Status != domain.IntegrationMergeOutcomeMerged || broker.mergeCalls != 1 {
			t.Fatalf("out=%#v err=%v calls=%d", out, err, broker.mergeCalls)
		}
		if _, err := svc.Merge(context.Background(), in); err != nil || broker.mergeCalls != 1 {
			t.Fatalf("retry err=%v calls=%d", err, broker.mergeCalls)
		}
	})

	t.Run("database failure after side effect then restart", func(t *testing.T) {
		now := testNow
		svc, store, broker, issued := newTestServiceAt(t, &now, nil, nil)
		store.failResultOnce = true
		in := mergeInput(issued, "db-fail-001")
		_, err := svc.Merge(context.Background(), in)
		assertCodeReason(t, err, CodeInternal, "result_persistence_failed")
		if broker.mergeCalls != 1 || store.journal.State != domain.IntegrationMergeDispatched {
			t.Fatalf("calls=%d journal=%s", broker.mergeCalls, store.journal.State)
		}
		restarted := mustService(t, store, broker, &now, nil)
		out, err := restarted.Merge(context.Background(), in)
		if err != nil || out.Status != domain.IntegrationMergeOutcomeMerged || broker.mergeCalls != 1 {
			t.Fatalf("out=%#v err=%v calls=%d", out, err, broker.mergeCalls)
		}
	})
}

func TestReconciliationRequiresExactAuthorizedOperation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.IntegrationMergeCandidate)
	}{
		{"not merged", func(c *domain.IntegrationMergeCandidate) { c.Merged = false }},
		{"wrong repository", func(c *domain.IntegrationMergeCandidate) { c.Repository = "https://github.com/acme/other" }},
		{"wrong PR", func(c *domain.IntegrationMergeCandidate) { c.PRNumber++ }},
		{"wrong original head", func(c *domain.IntegrationMergeCandidate) { c.MergedExpectedHeadSHA = strings.Repeat("c", 40) }},
		{"missing merge commit", func(c *domain.IntegrationMergeCandidate) { c.MergeCommitSHA = "" }},
		{"wrong strategy evidence", func(c *domain.IntegrationMergeCandidate) { c.MergeStrategyEvidence = domain.MergeStrategyMerge }},
		{"ambiguous authoritative state", func(c *domain.IntegrationMergeCandidate) { c.Ambiguous = true }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := testNow
			_, store, broker, issued := newTestServiceAt(t, &now, nil, nil)
			in := mergeInput(issued, "reconcile-01")
			hash, err := canonicalRequestHash(issued.Lease, in.ManualApproval)
			if err != nil {
				t.Fatal(err)
			}
			journal := domain.IntegrationMergeJournal{
				IdempotencyKey: in.IdempotencyKey, RequestHash: hash, LeaseID: issued.Lease.ID,
				State: domain.IntegrationMergeAccepted, AcceptedAt: now,
			}
			if accepted, _, err := store.AcceptIntegrationMerge(context.Background(), journal, now); err != nil || !accepted {
				t.Fatalf("accept=%v err=%v", accepted, err)
			}
			if _, changed, err := store.MarkIntegrationMergeDispatched(context.Background(), in.IdempotencyKey, "dispatch-owner-primary", "imf_abcdefghijklmnop", now.Add(time.Second)); err != nil || !changed {
				t.Fatalf("dispatch=%v err=%v", changed, err)
			}
			broker.candidate.Merged = true
			broker.candidate.MergedExpectedHeadSHA = issued.Lease.ExpectedHeadSHA
			broker.candidate.MergeCommitSHA = strings.Repeat("b", 40)
			broker.candidate.MergeStrategyEvidence = issued.Lease.Strategy
			broker.candidate.MergedAt = now.Add(2 * time.Second)
			tc.mutate(&broker.candidate)

			restarted := mustServiceWithOwner(t, store, broker, &now, nil, "dispatch-owner-reconcile", fixedOwnerVerifier{liveness: DispatchOwnerDead})
			out, err := restarted.Merge(context.Background(), in)
			if err != nil || out.Status != domain.IntegrationMergeOutcomeAmbiguous || broker.mergeCalls != 0 {
				t.Fatalf("out=%#v err=%v calls=%d", out, err, broker.mergeCalls)
			}
		})
	}
}

func TestRootAuthorizationAndConstructionAreFailClosed(t *testing.T) {
	if _, err := New(Deps{}); err == nil {
		t.Fatal("New without managed control and broker succeeded")
	} else {
		assertCodeReason(t, err, CodeNotConfigured, "managed_control_merge_broker_and_dispatch_fence_required")
	}
	store := &fakeStore{}
	broker := &fakeBroker{candidate: validCandidate()}
	now := testNow
	if _, err := New(Deps{Store: store, Broker: broker, RootAuthorizer: allowRoot{}, DispatchOwner: "dispatch-owner-without-verifier"}); err == nil {
		t.Fatal("New without trusted dispatch owner verifier succeeded")
	} else {
		assertCodeReason(t, err, CodeNotConfigured, "managed_control_merge_broker_and_dispatch_fence_required")
	}
	if _, err := New(Deps{Store: store, Broker: broker, RootAuthorizer: allowRoot{}, DispatchOwner: "short", OwnerVerifier: fixedOwnerVerifier{}}); err == nil {
		t.Fatal("New with an invalid dispatch owner succeeded")
	} else {
		assertCodeReason(t, err, CodeNotConfigured, "managed_control_merge_broker_and_dispatch_fence_required")
	}
	svc, err := New(Deps{
		Store: store, Broker: broker, RootAuthorizer: allowRoot{err: errors.New("denied")},
		DispatchOwner: "dispatch-owner-denied", OwnerVerifier: fixedOwnerVerifier{liveness: DispatchOwnerAlive},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.IssueLease(context.Background(), validIssueInput(now))
	assertCodeReason(t, err, CodeUnauthorized, "root_authentication_required")
}

func newTestService(t *testing.T, mutate func(*IssueLeaseInput)) (*Service, *fakeStore, *fakeBroker, IssuedLease) {
	t.Helper()
	now := testNow
	return newTestServiceAt(t, &now, nil, mutate)
}

func newTestServiceAt(t *testing.T, now *time.Time, faults FaultInjector, mutate func(*IssueLeaseInput)) (*Service, *fakeStore, *fakeBroker, IssuedLease) {
	t.Helper()
	store := &fakeStore{}
	broker := &fakeBroker{candidate: validCandidate()}
	svc := mustService(t, store, broker, now, faults)
	in := validIssueInput(*now)
	if mutate != nil {
		mutate(&in)
	}
	issued, err := svc.IssueLease(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return svc, store, broker, issued
}

func mustService(t *testing.T, store Store, broker Broker, now *time.Time, faults FaultInjector) *Service {
	return mustServiceWithOwner(t, store, broker, now, faults, "dispatch-owner-primary", fixedOwnerVerifier{liveness: DispatchOwnerAlive})
}

func mustServiceWithOwner(t *testing.T, store Store, broker Broker, now *time.Time, faults FaultInjector, owner string, verifier DispatchOwnerVerifier) *Service {
	t.Helper()
	svc, err := New(Deps{
		Store: store, Broker: broker, RootAuthorizer: allowRoot{},
		DispatchOwner: owner, OwnerVerifier: verifier,
		Now:    func() time.Time { return now.UTC() },
		Random: bytes.NewReader(bytes.Repeat([]byte{7}, 256)), Faults: faults,
		ReconcileTimeout: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func validIssueInput(now time.Time) IssueLeaseInput {
	return IssueLeaseInput{
		Repository: "https://github.com/Acme/Repo.git", SourceRepository: "https://github.com/Acme/Repo",
		PRNumber: 42, SourceBranch: "feature/hardened-merge", ExpectedHeadSHA: strings.Repeat("a", 40),
		BaseRepository: "https://github.com/acme/repo", BaseBranch: "main",
		CheckPolicy:  domain.IntegrationCheckPolicy{Revision: "checks-v1", RequiredChecks: []string{"test", "build"}, RequireAllObservedPassing: true},
		ReviewPolicy: domain.IntegrationReviewPolicy{Revision: "reviews-v1", RequiredApprovals: 1, RequiredReviewers: []string{"Alice"}, RequireResolvedThreads: true},
		ExpiresAt:    now.Add(5 * time.Minute),
	}
}

func validCandidate() domain.IntegrationMergeCandidate {
	head := strings.Repeat("a", 40)
	return domain.IntegrationMergeCandidate{
		Repository: "https://github.com/acme/repo", SourceRepository: "https://github.com/acme/repo",
		PRNumber: 42, SourceBranch: "feature/hardened-merge", HeadSHA: head,
		BaseRepository: "https://github.com/acme/repo", BaseBranch: "main", Mergeability: "clean",
		ConfiguredStrategy: domain.MergeStrategySquash,
		CheckPolicy:        domain.IntegrationCheckPolicy{Revision: "checks-v1", RequiredChecks: []string{"build", "test"}, RequireAllObservedPassing: true},
		Checks: []domain.IntegrationCheckEvidence{
			{Name: "build", HeadSHA: head, Status: "passing", CompletedAt: testNow.Add(-2 * time.Minute)},
			{Name: "test", HeadSHA: head, Status: "passing", CompletedAt: testNow.Add(-time.Minute)},
		},
		ReviewPolicy: domain.IntegrationReviewPolicy{Revision: "reviews-v1", RequiredApprovals: 1, RequiredReviewers: []string{"alice"}, RequireResolvedThreads: true},
		Reviews:      []domain.IntegrationReviewEvidence{{Reviewer: "alice", HeadSHA: head, Decision: "approved", SubmittedAt: testNow.Add(-time.Minute)}},
	}
}

func mergeInput(issued IssuedLease, key string) MergeInput {
	return MergeInput{LeaseID: issued.Lease.ID, GateCapability: issued.GateCapability, IdempotencyKey: key}
}

func assertCodeReason(t *testing.T, err error, wantCode ErrorCode, wantReason string) {
	t.Helper()
	code, reason, ok := ErrorInfo(err)
	if !ok || code != wantCode || reason != wantReason {
		t.Fatalf("error = %v code=%q reason=%q, want %q/%q", err, code, reason, wantCode, wantReason)
	}
}

func cloneLease(in domain.IntegrationMergeLease) domain.IntegrationMergeLease {
	out := in
	out.CapabilityDigest = append([]byte(nil), in.CapabilityDigest...)
	out.CheckPolicy.RequiredChecks = append([]string(nil), in.CheckPolicy.RequiredChecks...)
	out.ReviewPolicy.RequiredReviewers = append([]string(nil), in.ReviewPolicy.RequiredReviewers...)
	return out
}

func cloneJournal(in domain.IntegrationMergeJournal) domain.IntegrationMergeJournal {
	out := in
	out.RequestHash = append([]byte(nil), in.RequestHash...)
	if in.Outcome != nil {
		copy := cloneOutcome(*in.Outcome)
		out.Outcome = &copy
	}
	return out
}

func cloneCandidate(in domain.IntegrationMergeCandidate) domain.IntegrationMergeCandidate {
	out := in
	out.Checks = append([]domain.IntegrationCheckEvidence(nil), in.Checks...)
	out.Reviews = append([]domain.IntegrationReviewEvidence(nil), in.Reviews...)
	out.CheckPolicy.RequiredChecks = append([]string(nil), in.CheckPolicy.RequiredChecks...)
	out.ReviewPolicy.RequiredReviewers = append([]string(nil), in.ReviewPolicy.RequiredReviewers...)
	return out
}
