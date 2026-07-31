package store_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	integrationmerge "github.com/aoagents/agent-orchestrator/backend/internal/service/integrationmerge"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type raceRootAuthorizer struct{}

func (raceRootAuthorizer) AuthorizeRoot(context.Context) error { return nil }

type raceOwnerVerifier struct {
	mu     sync.Mutex
	states map[string]integrationmerge.DispatchOwnerLiveness
	err    error
}

func newRaceOwnerVerifier() *raceOwnerVerifier {
	return &raceOwnerVerifier{states: make(map[string]integrationmerge.DispatchOwnerLiveness)}
}

func (v *raceOwnerVerifier) VerifyDispatchOwner(_ context.Context, owner string) (integrationmerge.DispatchOwnerLiveness, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.err != nil {
		return integrationmerge.DispatchOwnerUnknown, v.err
	}
	state, ok := v.states[owner]
	if !ok {
		return integrationmerge.DispatchOwnerUnknown, nil
	}
	return state, nil
}

func (v *raceOwnerVerifier) set(owner string, state integrationmerge.DispatchOwnerLiveness) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.states[owner] = state
}

func (v *raceOwnerVerifier) setError(err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.err = err
}

type raceMergeBroker struct {
	mu                 sync.Mutex
	candidate          domain.IntegrationMergeCandidate
	mergeCalls         int
	now                time.Time
	pendingMerge       *domain.IntegrationBrokerMergeResult
	visibilityLagReads int
}

func newRaceMergeBroker(now time.Time) *raceMergeBroker {
	head := strings.Repeat("a", 40)
	return &raceMergeBroker{now: now, candidate: domain.IntegrationMergeCandidate{
		Repository: "https://github.com/acme/repo", SourceRepository: "https://github.com/acme/repo",
		PRNumber: 42, SourceBranch: "feature/fenced", HeadSHA: head,
		BaseRepository: "https://github.com/acme/repo", BaseBranch: "main", Mergeability: "clean",
		ConfiguredStrategy: domain.MergeStrategySquash,
		CheckPolicy:        domain.IntegrationCheckPolicy{Revision: "checks-v1", RequiredChecks: []string{"build"}, RequireAllObservedPassing: true},
		Checks:             []domain.IntegrationCheckEvidence{{Name: "build", HeadSHA: head, Status: "passing", CompletedAt: now.Add(-time.Minute)}},
		ReviewPolicy:       domain.IntegrationReviewPolicy{Revision: "reviews-v1", RequiredApprovals: 1, RequiredReviewers: []string{"alice"}, RequireResolvedThreads: true},
		Reviews:            []domain.IntegrationReviewEvidence{{Reviewer: "alice", HeadSHA: head, Decision: "approved", SubmittedAt: now.Add(-time.Minute)}},
	}}
}

func (b *raceMergeBroker) Candidate(context.Context, string, int) (domain.IntegrationMergeCandidate, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pendingMerge != nil {
		if b.visibilityLagReads > 0 {
			b.visibilityLagReads--
			return cloneRaceCandidate(b.candidate), nil
		}
		b.applyMerge(*b.pendingMerge)
		b.pendingMerge = nil
	}
	return cloneRaceCandidate(b.candidate), nil
}

func (b *raceMergeBroker) Merge(_ context.Context, command integrationmerge.BrokerMergeCommand) (domain.IntegrationBrokerMergeResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mergeCalls++
	mergedAt := b.now.Add(time.Minute)
	commit := strings.Repeat("b", 40)
	result := domain.IntegrationBrokerMergeResult{
		Repository: command.Repository, PRNumber: command.PRNumber,
		ExpectedHeadSHA: command.ExpectedHeadSHA, Strategy: command.Strategy,
		MergeCommitSHA: commit, MergedAt: mergedAt,
	}
	if b.visibilityLagReads > 0 {
		copy := result
		b.pendingMerge = &copy
	} else {
		b.applyMerge(result)
	}
	return result, nil
}

func (b *raceMergeBroker) applyMerge(result domain.IntegrationBrokerMergeResult) {
	b.candidate.Merged = true
	b.candidate.MergedExpectedHeadSHA = result.ExpectedHeadSHA
	b.candidate.MergeCommitSHA = result.MergeCommitSHA
	b.candidate.MergeStrategyEvidence = result.Strategy
	b.candidate.MergedAt = result.MergedAt
}

func (b *raceMergeBroker) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mergeCalls
}

func (b *raceMergeBroker) setCheckStatus(status string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.candidate.Checks[0].Status = status
}

func (b *raceMergeBroker) setVisibilityLag(reads int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.visibilityLagReads = reads
}

func (b *raceMergeBroker) markExternallyMerged() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.candidate.Merged = true
	b.candidate.MergedExpectedHeadSHA = b.candidate.HeadSHA
	b.candidate.MergeCommitSHA = strings.Repeat("b", 40)
	b.candidate.MergeStrategyEvidence = b.candidate.ConfiguredStrategy
	b.candidate.MergedAt = b.now.Add(time.Minute)
}

type raceFault struct {
	before func() error
	after  error
}

func (f raceFault) BeforeBrokerMerge(context.Context) error {
	if f.before == nil {
		return nil
	}
	return f.before()
}

func (f raceFault) AfterBrokerMerge(context.Context, domain.IntegrationBrokerMergeResult) error {
	return f.after
}

func TestIntegrationMergeTwoServicesCannotTerminalizeLivePausedOwner(t *testing.T) {
	for _, tc := range []struct {
		name               string
		sameOwner          bool
		temporarilyInvalid bool
	}{
		{name: "different-owner"},
		{name: "same-owner-string", sameOwner: true},
		{name: "invalid-snapshot-cannot-fence-live-owner", temporarilyInvalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeA, storeB := newRaceStorePair(t)
			now := time.Date(2026, time.July, 30, 18, 0, 0, 0, time.UTC)
			broker := newRaceMergeBroker(now)
			verifier := newRaceOwnerVerifier()
			ownerA := "dispatch-owner-service-a"
			ownerB := "dispatch-owner-service-b"
			if tc.sameOwner {
				ownerB = ownerA
			}
			verifier.set(ownerA, integrationmerge.DispatchOwnerAlive)

			entered := make(chan struct{})
			release := make(chan struct{})
			var enterOnce sync.Once
			serviceA := newRaceMergeService(t, storeA, broker, verifier, ownerA, now, 1, raceFault{before: func() error {
				enterOnce.Do(func() { close(entered) })
				<-release
				return nil
			}})
			serviceB := newRaceMergeService(t, storeB, broker, verifier, ownerB, now, 2, nil)
			issued := issueRaceLease(t, serviceA, now)
			input := raceMergeInput(issued, "race-live-owner-01")

			type mergeResult struct {
				out domain.IntegrationMergeOutcome
				err error
			}
			resultA := make(chan mergeResult, 1)
			go func() {
				out, err := serviceA.Merge(context.Background(), input)
				resultA <- mergeResult{out: out, err: err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("service A did not pause after durable dispatch")
			}
			if tc.temporarilyInvalid {
				broker.setCheckStatus("pending")
			}

			if _, err := serviceB.Merge(context.Background(), input); integrationMergeErrorCode(err) != integrationmerge.CodeReconciliationInProgress {
				t.Fatalf("service B retry error = %v", err)
			}
			journal, found, err := storeB.GetIntegrationMergeJournal(context.Background(), input.IdempotencyKey)
			if err != nil || !found || journal.State != domain.IntegrationMergeDispatched || broker.calls() != 0 {
				t.Fatalf("paused journal=%#v found=%v err=%v calls=%d", journal, found, err, broker.calls())
			}
			if tc.temporarilyInvalid {
				broker.setCheckStatus("passing")
			}

			close(release)
			select {
			case result := <-resultA:
				if result.err != nil || result.out.Status != domain.IntegrationMergeOutcomeMerged {
					t.Fatalf("service A result=%#v err=%v", result.out, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("service A did not resume")
			}
			if broker.calls() != 1 {
				t.Fatalf("merge calls=%d, want exactly one", broker.calls())
			}
			retry, err := serviceB.Merge(context.Background(), input)
			if err != nil || retry.Status != domain.IntegrationMergeOutcomeMerged || retry.MergeCommitSHA != strings.Repeat("b", 40) || broker.calls() != 1 {
				t.Fatalf("service B terminal retry=%#v err=%v calls=%d", retry, err, broker.calls())
			}
		})
	}
}

func TestIntegrationMergeReusedOwnerRequiresProofAndDeadOwnerFailsClosed(t *testing.T) {
	storeA, storeB := newRaceStorePair(t)
	now := time.Date(2026, time.July, 30, 19, 0, 0, 0, time.UTC)
	broker := newRaceMergeBroker(now)
	verifier := newRaceOwnerVerifier()
	owner := "dispatch-owner-reused"
	crash := errors.New("crash before merge")
	serviceA := newRaceMergeService(t, storeA, broker, verifier, owner, now, 3, raceFault{before: func() error { return crash }})
	issued := issueRaceLease(t, serviceA, now)
	input := raceMergeInput(issued, "race-reused-owner-01")
	if _, err := serviceA.Merge(context.Background(), input); !errors.Is(err, crash) {
		t.Fatalf("service A crash error=%v", err)
	}

	foreign := newRaceMergeService(t, storeB, broker, verifier, "dispatch-owner-foreign", now, 4, nil)
	if _, err := foreign.Merge(context.Background(), input); integrationMergeErrorCode(err) != integrationmerge.CodeReconciliationInProgress {
		t.Fatalf("unproven foreign owner error=%v", err)
	}
	reused := newRaceMergeService(t, storeB, broker, verifier, owner, now, 5, nil)
	if _, err := reused.Merge(context.Background(), input); integrationMergeErrorCode(err) != integrationmerge.CodeReconciliationInProgress {
		t.Fatalf("reused owner bypassed local fence proof: %v", err)
	}
	verifier.setError(context.DeadlineExceeded)
	if _, err := reused.Merge(context.Background(), input); integrationMergeErrorCode(err) != integrationmerge.CodeReconciliationInProgress {
		t.Fatalf("liveness timeout was treated as death proof: %v", err)
	}
	verifier.setError(nil)
	journal, _, err := storeB.GetIntegrationMergeJournal(context.Background(), input.IdempotencyKey)
	if err != nil || journal.State != domain.IntegrationMergeDispatched || broker.calls() != 0 {
		t.Fatalf("unproven state=%#v err=%v calls=%d", journal, err, broker.calls())
	}

	verifier.set(owner, integrationmerge.DispatchOwnerDead)
	out, err := reused.Merge(context.Background(), input)
	if err != nil || out.Status != domain.IntegrationMergeOutcomeAmbiguous || broker.calls() != 0 {
		t.Fatalf("dead owner result=%#v err=%v calls=%d", out, err, broker.calls())
	}
	journal, _, err = storeB.GetIntegrationMergeJournal(context.Background(), input.IdempotencyKey)
	if err != nil || journal.State != domain.IntegrationMergeAmbiguous {
		t.Fatalf("dead owner journal=%#v err=%v", journal, err)
	}
}

func TestIntegrationMergeExactForeignReconciliationFencesPausedOwnerWithoutSecondMerge(t *testing.T) {
	storeA, storeB := newRaceStorePair(t)
	now := time.Date(2026, time.July, 30, 19, 30, 0, 0, time.UTC)
	broker := newRaceMergeBroker(now)
	verifier := newRaceOwnerVerifier()
	ownerA := "dispatch-owner-exact-a"
	verifier.set(ownerA, integrationmerge.DispatchOwnerAlive)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	serviceA := newRaceMergeService(t, storeA, broker, verifier, ownerA, now, 7, raceFault{before: func() error {
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil
	}})
	serviceB := newRaceMergeService(t, storeB, broker, verifier, "dispatch-owner-exact-b", now, 8, nil)
	issued := issueRaceLease(t, serviceA, now)
	input := raceMergeInput(issued, "race-exact-reconcile-01")
	type mergeResult struct {
		out domain.IntegrationMergeOutcome
		err error
	}
	resultA := make(chan mergeResult, 1)
	go func() {
		out, err := serviceA.Merge(context.Background(), input)
		resultA <- mergeResult{out: out, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("service A did not pause after durable dispatch")
	}

	broker.markExternallyMerged()
	reconciled, err := serviceB.Merge(context.Background(), input)
	if err != nil || reconciled.Status != domain.IntegrationMergeOutcomeMerged || broker.calls() != 0 {
		t.Fatalf("foreign reconciliation=%#v err=%v calls=%d", reconciled, err, broker.calls())
	}
	close(release)
	select {
	case result := <-resultA:
		if result.err != nil || result.out.Status != domain.IntegrationMergeOutcomeMerged {
			t.Fatalf("paused owner result=%#v err=%v", result.out, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("paused owner did not return reconciled result")
	}
	if broker.calls() != 0 {
		t.Fatalf("paused owner issued a second merge call: %d", broker.calls())
	}
}

func TestIntegrationMergeSQLiteResponseLossReconcilesExactSuccess(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, time.July, 30, 20, 0, 0, 0, time.UTC)
	broker := newRaceMergeBroker(now)
	verifier := newRaceOwnerVerifier()
	service := newRaceMergeService(t, store, broker, verifier, "dispatch-owner-response-loss", now, 6, raceFault{after: errors.New("response lost")})
	issued := issueRaceLease(t, service, now)
	input := raceMergeInput(issued, "race-response-loss-01")
	out, err := service.Merge(context.Background(), input)
	if err != nil || out.Status != domain.IntegrationMergeOutcomeMerged || out.MergeCommitSHA != strings.Repeat("b", 40) || broker.calls() != 1 {
		t.Fatalf("response-loss result=%#v err=%v calls=%d", out, err, broker.calls())
	}
	retry, err := service.Merge(context.Background(), input)
	if err != nil || retry.MergeCommitSHA != out.MergeCommitSHA || broker.calls() != 1 {
		t.Fatalf("response-loss retry=%#v err=%v calls=%d", retry, err, broker.calls())
	}
}

func TestIntegrationMergeSQLiteResponseLossWaitsForDelayedAuthoritativeVisibility(t *testing.T) {
	store := newTestStore(t)
	now := time.Date(2026, time.July, 30, 20, 30, 0, 0, time.UTC)
	broker := newRaceMergeBroker(now)
	broker.setVisibilityLag(2)
	verifier := newRaceOwnerVerifier()
	service := newRaceMergeServiceWithTimeout(t, store, broker, verifier, "dispatch-owner-delayed-visibility", now, 9, raceFault{after: errors.New("response lost")}, time.Second)
	issued := issueRaceLease(t, service, now)
	input := raceMergeInput(issued, "race-delayed-visibility-01")
	out, err := service.Merge(context.Background(), input)
	if err != nil || out.Status != domain.IntegrationMergeOutcomeMerged || out.MergeCommitSHA != strings.Repeat("b", 40) || broker.calls() != 1 {
		t.Fatalf("delayed response-loss result=%#v err=%v calls=%d", out, err, broker.calls())
	}
	journal, found, err := store.GetIntegrationMergeJournal(context.Background(), input.IdempotencyKey)
	if err != nil || !found || journal.State != domain.IntegrationMergeResult {
		t.Fatalf("delayed response-loss journal=%#v found=%v err=%v", journal, found, err)
	}
}

func TestIntegrationMergeLateExactProofRefinesAmbiguityWithoutRedispatch(t *testing.T) {
	storeA, storeB := newRaceStorePair(t)
	now := time.Date(2026, time.July, 30, 21, 0, 0, 0, time.UTC)
	broker := newRaceMergeBroker(now)
	broker.setVisibilityLag(10)
	verifier := newRaceOwnerVerifier()
	serviceA := newRaceMergeServiceWithTimeout(t, storeA, broker, verifier, "dispatch-owner-late-proof-a", now, 10, raceFault{after: errors.New("response lost")}, 150*time.Millisecond)
	issued := issueRaceLease(t, serviceA, now)
	input := raceMergeInput(issued, "race-late-proof-01")
	out, err := serviceA.Merge(context.Background(), input)
	if err != nil || out.Status != domain.IntegrationMergeOutcomeAmbiguous || broker.calls() != 1 {
		t.Fatalf("initial ambiguity=%#v err=%v calls=%d", out, err, broker.calls())
	}

	broker.setVisibilityLag(0)
	serviceB := newRaceMergeService(t, storeB, broker, verifier, "dispatch-owner-late-proof-b", now, 11, nil)
	refined, err := serviceB.Merge(context.Background(), input)
	if err != nil || refined.Status != domain.IntegrationMergeOutcomeMerged || refined.MergeCommitSHA != strings.Repeat("b", 40) || broker.calls() != 1 {
		t.Fatalf("refined result=%#v err=%v calls=%d", refined, err, broker.calls())
	}
	journal, found, err := storeB.GetIntegrationMergeJournal(context.Background(), input.IdempotencyKey)
	if err != nil || !found || journal.State != domain.IntegrationMergeResult {
		t.Fatalf("refined journal=%#v found=%v err=%v", journal, found, err)
	}
}

func newRaceStorePair(t *testing.T) (*sqlite.Store, *sqlite.Store) {
	t.Helper()
	dataDir := t.TempDir()
	first, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatalf("open first race store: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatalf("open second race store: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	return first, second
}

func newRaceMergeService(t *testing.T, store integrationmerge.Store, broker integrationmerge.Broker, verifier integrationmerge.DispatchOwnerVerifier, owner string, now time.Time, seed byte, faults integrationmerge.FaultInjector) *integrationmerge.Service {
	t.Helper()
	return newRaceMergeServiceWithTimeout(t, store, broker, verifier, owner, now, seed, faults, 250*time.Millisecond)
}

func newRaceMergeServiceWithTimeout(t *testing.T, store integrationmerge.Store, broker integrationmerge.Broker, verifier integrationmerge.DispatchOwnerVerifier, owner string, now time.Time, seed byte, faults integrationmerge.FaultInjector, reconcileTimeout time.Duration) *integrationmerge.Service {
	t.Helper()
	service, err := integrationmerge.New(integrationmerge.Deps{
		Store: store, Broker: broker, RootAuthorizer: raceRootAuthorizer{},
		DispatchOwner: owner, OwnerVerifier: verifier, Faults: faults,
		Now: func() time.Time { return now }, Random: bytes.NewReader(bytes.Repeat([]byte{seed}, 1024)),
		ReconcileTimeout: reconcileTimeout,
	})
	if err != nil {
		t.Fatalf("new merge service: %v", err)
	}
	return service
}

func issueRaceLease(t *testing.T, service *integrationmerge.Service, now time.Time) integrationmerge.IssuedLease {
	t.Helper()
	issued, err := service.IssueLease(context.Background(), integrationmerge.IssueLeaseInput{
		Repository: "https://github.com/acme/repo", SourceRepository: "https://github.com/acme/repo",
		PRNumber: 42, SourceBranch: "feature/fenced", ExpectedHeadSHA: strings.Repeat("a", 40),
		BaseRepository: "https://github.com/acme/repo", BaseBranch: "main",
		CheckPolicy:  domain.IntegrationCheckPolicy{Revision: "checks-v1", RequiredChecks: []string{"build"}, RequireAllObservedPassing: true},
		ReviewPolicy: domain.IntegrationReviewPolicy{Revision: "reviews-v1", RequiredApprovals: 1, RequiredReviewers: []string{"alice"}, RequireResolvedThreads: true},
		ExpiresAt:    now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("issue merge lease: %v", err)
	}
	return issued
}

func raceMergeInput(issued integrationmerge.IssuedLease, key string) integrationmerge.MergeInput {
	return integrationmerge.MergeInput{LeaseID: issued.Lease.ID, GateCapability: issued.GateCapability, IdempotencyKey: key}
}

func integrationMergeErrorCode(err error) integrationmerge.ErrorCode {
	code, _, _ := integrationmerge.ErrorInfo(err)
	return code
}

func cloneRaceCandidate(in domain.IntegrationMergeCandidate) domain.IntegrationMergeCandidate {
	out := in
	out.Checks = append([]domain.IntegrationCheckEvidence(nil), in.Checks...)
	out.Reviews = append([]domain.IntegrationReviewEvidence(nil), in.Reviews...)
	out.CheckPolicy.RequiredChecks = append([]string(nil), in.CheckPolicy.RequiredChecks...)
	out.ReviewPolicy.RequiredReviewers = append([]string(nil), in.ReviewPolicy.RequiredReviewers...)
	return out
}
