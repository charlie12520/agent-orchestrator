package store_test

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestIntegrationMergeLeaseSingleCandidateExpiryAndRevocation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, time.July, 30, 13, 0, 0, 0, time.UTC)
	first := integrationLease("iml_first-candidate-0001", now)
	created, err := store.CreateIntegrationMergeLease(ctx, first, now)
	if err != nil || !created {
		t.Fatalf("create first = %v, %v", created, err)
	}
	second := integrationLease("iml_second-candidate-002", now)
	created, err = store.CreateIntegrationMergeLease(ctx, second, now)
	if err != nil || created {
		t.Fatalf("create concurrent = %v, %v", created, err)
	}

	afterExpiry := now.Add(6 * time.Minute)
	second.CreatedAt = afterExpiry
	second.ExpiresAt = afterExpiry.Add(5 * time.Minute)
	created, err = store.CreateIntegrationMergeLease(ctx, second, afterExpiry)
	if err != nil || !created {
		t.Fatalf("create after expiry = %v, %v", created, err)
	}
	loadedFirst, found, err := store.GetIntegrationMergeLease(ctx, first.ID)
	if err != nil || !found || loadedFirst.Status != domain.IntegrationMergeLeaseExpired {
		t.Fatalf("expired first = %#v, %v, %v", loadedFirst, found, err)
	}
	revoked, changed, err := store.RevokeIntegrationMergeLease(ctx, second.ID, afterExpiry.Add(time.Minute))
	if err != nil || !changed || revoked.Status != domain.IntegrationMergeLeaseRevoked || revoked.RevokedAt.IsZero() {
		t.Fatalf("revoke = %#v, %v, %v", revoked, changed, err)
	}
	again, changed, err := store.RevokeIntegrationMergeLease(ctx, second.ID, afterExpiry.Add(2*time.Minute))
	if err != nil || changed || again.Status != domain.IntegrationMergeLeaseRevoked {
		t.Fatalf("second revoke = %#v, %v, %v", again, changed, err)
	}
}

func TestIntegrationMergeAcceptanceIsTransactionalAndExactOnce(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, time.July, 30, 14, 0, 0, 0, time.UTC)
	lease := integrationLease("iml_transactional-lease-01", now)
	if created, err := store.CreateIntegrationMergeLease(ctx, lease, now); err != nil || !created {
		t.Fatalf("create = %v, %v", created, err)
	}

	// The invalid key violates the journal constraint after the consume UPDATE.
	// The transaction must roll both operations back.
	bad := integrationJournal("bad", lease.ID, now)
	if accepted, _, err := store.AcceptIntegrationMerge(ctx, bad, now); err == nil || accepted {
		t.Fatalf("invalid journal accepted=%v err=%v", accepted, err)
	}
	loaded, found, err := store.GetIntegrationMergeLease(ctx, lease.ID)
	if err != nil || !found || loaded.Status != domain.IntegrationMergeLeaseActive || !loaded.ConsumedAt.IsZero() {
		t.Fatalf("lease after rollback = %#v found=%v err=%v", loaded, found, err)
	}

	journal := integrationJournal("operation-store-01", lease.ID, now)
	accepted, status, err := store.AcceptIntegrationMerge(ctx, journal, now)
	if err != nil || !accepted || status != domain.IntegrationMergeLeaseConsumed {
		t.Fatalf("accept = %v, %s, %v", accepted, status, err)
	}
	loaded, _, _ = store.GetIntegrationMergeLease(ctx, lease.ID)
	if loaded.Status != domain.IntegrationMergeLeaseConsumed || loaded.ConsumedAt.IsZero() {
		t.Fatalf("consumed lease = %#v", loaded)
	}
	storedJournal, found, err := store.GetIntegrationMergeJournal(ctx, journal.IdempotencyKey)
	if err != nil || !found || storedJournal.State != domain.IntegrationMergeAccepted {
		t.Fatalf("journal = %#v found=%v err=%v", storedJournal, found, err)
	}

	other := integrationJournal("operation-store-02", lease.ID, now.Add(time.Second))
	accepted, status, err = store.AcceptIntegrationMerge(ctx, other, now.Add(time.Second))
	if err != nil || accepted || status != domain.IntegrationMergeLeaseConsumed {
		t.Fatalf("second accept = %v, %s, %v", accepted, status, err)
	}
	if _, found, err := store.GetIntegrationMergeJournal(ctx, other.IdempotencyKey); err != nil || found {
		t.Fatalf("new journal for consumed lease found=%v err=%v", found, err)
	}
}

func TestIntegrationMergeJournalTransitionsAndEvidence(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, time.July, 30, 15, 0, 0, 0, time.UTC)
	lease := integrationLease("iml_journal-transition-01", now)
	created, err := store.CreateIntegrationMergeLease(ctx, lease, now)
	if err != nil || !created {
		t.Fatal(err)
	}
	journal := integrationJournal("operation-result-01", lease.ID, now)
	accepted, _, err := store.AcceptIntegrationMerge(ctx, journal, now)
	if err != nil || !accepted {
		t.Fatalf("accept = %v, %v", accepted, err)
	}
	dispatched, changed, err := store.MarkIntegrationMergeDispatched(ctx, journal.IdempotencyKey, now.Add(time.Second))
	if err != nil || !changed || dispatched.State != domain.IntegrationMergeDispatched || dispatched.DispatchedAt.IsZero() {
		t.Fatalf("dispatch = %#v, %v, %v", dispatched, changed, err)
	}
	outcome := integrationOutcome(journal.IdempotencyKey, lease, dispatched, domain.IntegrationMergeOutcomeMerged)
	result, changed, err := store.RecordIntegrationMergeResult(ctx, journal.IdempotencyKey, outcome, outcome.CompletedAt)
	if err != nil || !changed || result.State != domain.IntegrationMergeResult || result.Outcome == nil {
		t.Fatalf("result = %#v, %v, %v", result, changed, err)
	}
	if result.Outcome.MergeCommitSHA != strings.Repeat("b", 40) || len(result.Outcome.Checks) != 1 || len(result.Outcome.Reviews) != 1 {
		t.Fatalf("durable evidence = %#v", result.Outcome)
	}
	if _, changed, err := store.MarkIntegrationMergeDispatched(ctx, journal.IdempotencyKey, now.Add(3*time.Second)); err != nil || changed {
		t.Fatalf("terminal redispatch changed=%v err=%v", changed, err)
	}
}

func TestIntegrationMergeAcceptedMayEndInPreDispatchRevalidationResult(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := time.Date(2026, time.July, 30, 16, 0, 0, 0, time.UTC)
	lease := integrationLease("iml_predispatch-result-01", now)
	created, err := store.CreateIntegrationMergeLease(ctx, lease, now)
	if err != nil || !created {
		t.Fatal(err)
	}
	journal := integrationJournal("operation-reval-01", lease.ID, now)
	accepted, _, err := store.AcceptIntegrationMerge(ctx, journal, now)
	if err != nil || !accepted {
		t.Fatal(err)
	}
	outcome := integrationOutcome(journal.IdempotencyKey, lease, journal, domain.IntegrationMergeOutcomeRevalidationRequired)
	outcome.Reason = "candidate_identity_changed"
	outcome.MergeCommitSHA = ""
	result, changed, err := store.RecordIntegrationMergeResult(ctx, journal.IdempotencyKey, outcome, outcome.CompletedAt)
	if err != nil || !changed || result.State != domain.IntegrationMergeResult || !result.DispatchedAt.IsZero() {
		t.Fatalf("predispatch result = %#v changed=%v err=%v", result, changed, err)
	}
}

func integrationLease(id string, now time.Time) domain.IntegrationMergeLease {
	digest := sha256.Sum256([]byte("one-time-random-capability"))
	return domain.IntegrationMergeLease{
		ID: id, Repository: "https://github.com/acme/repo", SourceRepository: "https://github.com/acme/repo",
		PRNumber: 42, SourceBranch: "feature/hardened", ExpectedHeadSHA: strings.Repeat("a", 40),
		BaseRepository: "https://github.com/acme/repo", BaseBranch: "main", Strategy: domain.MergeStrategySquash,
		CheckPolicy:      domain.IntegrationCheckPolicy{Revision: "checks-v1", RequiredChecks: []string{"build"}, RequireAllObservedPassing: true},
		ReviewPolicy:     domain.IntegrationReviewPolicy{Revision: "reviews-v1", RequiredApprovals: 1, RequiredReviewers: []string{"alice"}, RequireResolvedThreads: true},
		CapabilityDigest: digest[:], Status: domain.IntegrationMergeLeaseActive,
		CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	}
}

func integrationJournal(key, leaseID string, now time.Time) domain.IntegrationMergeJournal {
	hash := sha256.Sum256([]byte("canonical-request"))
	return domain.IntegrationMergeJournal{
		IdempotencyKey: key, RequestHash: hash[:], LeaseID: leaseID,
		State: domain.IntegrationMergeAccepted, AcceptedAt: now,
	}
}

func integrationOutcome(key string, lease domain.IntegrationMergeLease, journal domain.IntegrationMergeJournal, status domain.IntegrationMergeOutcomeStatus) domain.IntegrationMergeOutcome {
	return domain.IntegrationMergeOutcome{
		Version: 1, Status: status, IdempotencyKey: key, Repository: lease.Repository,
		SourceRepository: lease.SourceRepository, PRNumber: lease.PRNumber,
		SourceBranch: lease.SourceBranch, ExpectedHeadSHA: lease.ExpectedHeadSHA,
		BaseRepository: lease.BaseRepository, BaseBranch: lease.BaseBranch, Strategy: lease.Strategy,
		CheckPolicy:    lease.CheckPolicy,
		Checks:         []domain.IntegrationCheckEvidence{{Name: "build", HeadSHA: lease.ExpectedHeadSHA, Status: "passing", CompletedAt: journal.AcceptedAt}},
		ReviewPolicy:   lease.ReviewPolicy,
		Reviews:        []domain.IntegrationReviewEvidence{{Reviewer: "alice", HeadSHA: lease.ExpectedHeadSHA, Decision: "approved", SubmittedAt: journal.AcceptedAt}},
		MergeCommitSHA: strings.Repeat("b", 40), AcceptedAt: journal.AcceptedAt,
		DispatchedAt: journal.DispatchedAt, CompletedAt: journal.AcceptedAt.Add(2 * time.Second),
	}
}
