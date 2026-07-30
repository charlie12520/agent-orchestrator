package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	executionjournal "github.com/aoagents/agent-orchestrator/backend/internal/service/executionjournal"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type executionDispatchFake struct {
	mu      sync.Mutex
	calls   []executionjournal.DispatchCommand
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	handler func(executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error)
}

func (f *executionDispatchFake) Dispatch(ctx context.Context, command executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error) {
	command.RequestJSON = append([]byte(nil), command.RequestJSON...)
	f.mu.Lock()
	f.calls = append(f.calls, command)
	f.mu.Unlock()
	if f.entered != nil {
		f.once.Do(func() { close(f.entered) })
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return executionjournal.DispatchReceipt{}, ctx.Err()
		}
	}
	if f.handler != nil {
		return f.handler(command)
	}
	return executionReceipt(command), nil
}

func (f *executionDispatchFake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *executionDispatchFake) commands() []executionjournal.DispatchCommand {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]executionjournal.DispatchCommand(nil), f.calls...)
}

type executionFault struct {
	point executionjournal.FaultPoint
	err   error
	mu    sync.Mutex
	fired bool
}

func (f *executionFault) Fail(_ context.Context, point executionjournal.FaultPoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if point != f.point || f.fired {
		return nil
	}
	f.fired = true
	return f.err
}

type executionTerminalWriteStore struct {
	executionjournal.Store
	claimDispatch   func(context.Context, string, string, string, time.Time) (executionjournal.Transition, error)
	confirmDispatch func(context.Context, string, string, string) (executionjournal.Transition, error)
	recordResult    func(context.Context, executionjournal.Completion, string, string) (executionjournal.Transition, error)
	recordAmbiguous func(context.Context, executionjournal.Completion, string, string) (executionjournal.Transition, error)
}

type executionFinalizationContextKey struct{}

type executionControlledDeadlineContext struct {
	context.Context
	mu       sync.Mutex
	deadline time.Time
	done     chan struct{}
	err      error
	once     sync.Once
}

func newExecutionControlledDeadlineContext(parent context.Context) *executionControlledDeadlineContext {
	return &executionControlledDeadlineContext{
		Context:  parent,
		deadline: time.Now().Add(time.Hour),
		done:     make(chan struct{}),
	}
}

func (c *executionControlledDeadlineContext) Deadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline, true
}

func (c *executionControlledDeadlineContext) Done() <-chan struct{} { return c.done }

func (c *executionControlledDeadlineContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *executionControlledDeadlineContext) expire() {
	c.once.Do(func() {
		c.mu.Lock()
		c.deadline = time.Now()
		c.err = context.DeadlineExceeded
		c.mu.Unlock()
		close(c.done)
	})
}

func detachedContextError(ctx context.Context, wantValue string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("service-owned context began canceled: %w", err)
	}
	if wantValue != "" {
		if value := ctx.Value(executionFinalizationContextKey{}); value != wantValue {
			return fmt.Errorf("service-owned context lost caller value: %v", value)
		}
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("service-owned context has no deadline")
	}
	if !deadline.After(time.Now()) {
		return fmt.Errorf("service-owned deadline is not fresh: %s", deadline)
	}
	return nil
}

func (s *executionTerminalWriteStore) ClaimExecutionDispatch(ctx context.Context, operationID, owner, fence string, at time.Time) (executionjournal.Transition, error) {
	if s.claimDispatch != nil {
		return s.claimDispatch(ctx, operationID, owner, fence, at)
	}
	return s.Store.ClaimExecutionDispatch(ctx, operationID, owner, fence, at)
}

func (s *executionTerminalWriteStore) ConfirmExecutionDispatch(ctx context.Context, operationID, owner, fence string) (executionjournal.Transition, error) {
	if s.confirmDispatch != nil {
		return s.confirmDispatch(ctx, operationID, owner, fence)
	}
	return s.Store.ConfirmExecutionDispatch(ctx, operationID, owner, fence)
}

func (s *executionTerminalWriteStore) RecordExecutionResult(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
	if s.recordResult != nil {
		return s.recordResult(ctx, completion, owner, fence)
	}
	return s.Store.RecordExecutionResult(ctx, completion, owner, fence)
}

func (s *executionTerminalWriteStore) RecordExecutionAmbiguous(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
	if s.recordAmbiguous != nil {
		return s.recordAmbiguous(ctx, completion, owner, fence)
	}
	return s.Store.RecordExecutionAmbiguous(ctx, completion, owner, fence)
}

func TestExecutionJournalExactRetryAndConflict(t *testing.T) {
	store := newTestStore(t)
	dispatcher := &executionDispatchFake{}
	service := newExecutionService(t, store, dispatcher, "owner-retry-a", 1, nil)
	raw := launchExecutionRequest("external-retry", "launch-retry", `{"count":1,"message":"café","newField":true}`)
	first, err := service.Execute(context.Background(), raw)
	if err != nil || first.State != domain.ExecutionResult || first.Replayed || dispatcher.count() != 1 {
		t.Fatalf("first result=%#v err=%v calls=%d", first, err, dispatcher.count())
	}

	alias := []byte(`{"request":{"newField":true,"message":"caf\u00e9","count":1.0},"idempotencyKey":"launch-retry","operation":"launch","externalRunId":"external-retry","version":1e0}`)
	replay, err := service.Execute(context.Background(), alias)
	if err != nil || !replay.Replayed || !bytes.Equal(replay.ResultJSON, first.ResultJSON) || dispatcher.count() != 1 {
		t.Fatalf("replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
	}

	conflict := launchExecutionRequest("external-retry", "launch-retry", `{"count":2,"message":"café","newField":true}`)
	if _, err := service.Execute(context.Background(), conflict); executionErrorCode(err) != executionjournal.CodeIdempotencyConflict || dispatcher.count() != 1 {
		t.Fatalf("conflicting reuse err=%v calls=%d", err, dispatcher.count())
	}
	crossOperation := mutationExecutionRequest("external-retry", first.RunID, "send", "launch-retry", 1, `{}`)
	if _, err := service.Execute(context.Background(), crossOperation); executionErrorCode(err) != executionjournal.CodeIdempotencyConflict || dispatcher.count() != 1 {
		t.Fatalf("cross-operation key reuse err=%v calls=%d", err, dispatcher.count())
	}
	secondLaunch := launchExecutionRequest("external-retry", "launch-new-key", `{}`)
	if _, err := service.Execute(context.Background(), secondLaunch); executionErrorCode(err) != executionjournal.CodeRunConflict || dispatcher.count() != 1 {
		t.Fatalf("second launch err=%v calls=%d", err, dispatcher.count())
	}
	otherExternal, err := service.Execute(context.Background(), launchExecutionRequest("external-retry-other", "launch-retry", `{"count":1}`))
	if err != nil || otherExternal.State != domain.ExecutionResult || dispatcher.count() != 2 {
		t.Fatalf("same key on other external run=%#v err=%v calls=%d", otherExternal, err, dispatcher.count())
	}
}

func TestExecutionJournalGenerationFenceRunsBeforeDispatch(t *testing.T) {
	store := newTestStore(t)
	dispatcher := &executionDispatchFake{}
	service := newExecutionService(t, store, dispatcher, "owner-generation-a", 2, nil)
	launch, err := service.Execute(context.Background(), launchExecutionRequest("external-generation", "launch-generation", `{}`))
	if err != nil || launch.ProcessGeneration != 1 {
		t.Fatalf("launch=%#v err=%v", launch, err)
	}
	stopRaw := mutationExecutionRequest("external-generation", launch.RunID, "stop", "stop-generation", 1, `{}`)
	stopped, err := service.Execute(context.Background(), stopRaw)
	if err != nil || stopped.ProcessGeneration != 1 {
		t.Fatalf("stop=%#v err=%v", stopped, err)
	}
	resumeRaw := mutationExecutionRequest("external-generation", launch.RunID, "resume", "resume-generation", 1, `{"message":"continue"}`)
	resume, err := service.Execute(context.Background(), resumeRaw)
	if err != nil || resume.ProcessGeneration != 2 {
		t.Fatalf("resume=%#v err=%v", resume, err)
	}
	before := dispatcher.count()
	stale := mutationExecutionRequest("external-generation", launch.RunID, "send", "send-stale", 1, `{"message":"late"}`)
	_, err = service.Execute(context.Background(), stale)
	var operationError *executionjournal.OperationError
	if !errors.As(err, &operationError) || operationError.Code != executionjournal.CodeProcessGenerationMismatch || operationError.ExpectedProcessGeneration != 1 || operationError.ActualProcessGeneration != 2 {
		t.Fatalf("stale generation error=%#v", err)
	}
	if dispatcher.count() != before {
		t.Fatalf("stale generation crossed dispatcher: before=%d after=%d", before, dispatcher.count())
	}

	wrongRun := mutationExecutionRequest("external-generation", "ao-wrong-run", "send", "send-wrong-run", 2, `{}`)
	if _, err := service.Execute(context.Background(), wrongRun); executionErrorCode(err) != executionjournal.CodeRunConflict || dispatcher.count() != before {
		t.Fatalf("wrong run err=%v calls=%d", err, dispatcher.count())
	}
	send := mutationExecutionRequest("external-generation", launch.RunID, "send", "send-current", 2, `{"message":"now"}`)
	result, err := service.Execute(context.Background(), send)
	if err != nil || result.ProcessGeneration != 2 || dispatcher.count() != before+1 {
		t.Fatalf("current send=%#v err=%v calls=%d", result, err, dispatcher.count())
	}
	commands := dispatcher.commands()
	if commands[0].ProcessGeneration != 1 || commands[1].ProcessGeneration != 1 || commands[2].ProcessGeneration != 2 || commands[3].ProcessGeneration != 2 {
		t.Fatalf("backend-assigned command generations=%v,%v,%v,%v", commands[0].ProcessGeneration, commands[1].ProcessGeneration, commands[2].ProcessGeneration, commands[3].ProcessGeneration)
	}
}

func TestExecutionJournalOperationMatrixAndTerminalTombstone(t *testing.T) {
	store := newTestStore(t)
	dispatcher := &executionDispatchFake{}
	service := newExecutionService(t, store, dispatcher, "owner-matrix-a", 10, nil)
	ctx := context.Background()
	external := "external-matrix"

	execute := func(raw []byte, wantGeneration int64) executionjournal.Result {
		t.Helper()
		first, err := service.Execute(ctx, raw)
		if err != nil || first.State != domain.ExecutionResult || first.ProcessGeneration != wantGeneration || first.Replayed {
			t.Fatalf("first=%#v err=%v want generation=%d", first, err, wantGeneration)
		}
		calls := dispatcher.count()
		replay, err := service.Execute(ctx, raw)
		if err != nil || !replay.Replayed || replay.State != domain.ExecutionResult || replay.ProcessGeneration != wantGeneration || !bytes.Equal(first.ResultJSON, replay.ResultJSON) || dispatcher.count() != calls {
			t.Fatalf("replay=%#v err=%v calls=%d want=%d", replay, err, dispatcher.count(), calls)
		}
		return first
	}

	launchRaw := launchExecutionRequest(external, "matrix-launch", `{}`)
	launch := execute(launchRaw, 1)
	sendRaw := mutationExecutionRequest(external, launch.RunID, "send", "matrix-send", 1, `{"message":"one"}`)
	sendResult := execute(sendRaw, 1)
	execute(mutationExecutionRequest(external, launch.RunID, "interrupt", "matrix-interrupt", 1, `{}`), 1)
	execute(mutationExecutionRequest(external, launch.RunID, "stop", "matrix-stop-1", 1, `{}`), 1)
	assertExecutionBindingState(t, store, external, domain.ExecutionBindingStopped, 1)
	if _, err := service.Execute(ctx, mutationExecutionRequest(external, launch.RunID, "send", "matrix-send-stopped", 1, `{}`)); executionErrorCode(err) != executionjournal.CodeRunConflict {
		t.Fatalf("send while stopped err=%v", err)
	}
	execute(mutationExecutionRequest(external, launch.RunID, "resume", "matrix-resume", 1, `{"message":"continue"}`), 2)
	assertExecutionBindingState(t, store, external, domain.ExecutionBindingActive, 2)
	execute(mutationExecutionRequest(external, launch.RunID, "stop", "matrix-stop-2", 2, `{}`), 2)
	execute(mutationExecutionRequest(external, launch.RunID, "restore", "matrix-restore", 2, `{"checkpoint":"latest"}`), 3)
	assertExecutionBindingState(t, store, external, domain.ExecutionBindingActive, 3)
	execute(mutationExecutionRequest(external, launch.RunID, "stop", "matrix-stop-3", 3, `{}`), 3)
	execute(mutationExecutionRequest(external, launch.RunID, "cleanup", "matrix-cleanup", 3, `{}`), 3)
	assertExecutionBindingState(t, store, external, domain.ExecutionBindingCleaned, 3)

	before := dispatcher.count()
	historicalLaunch, err := service.Execute(ctx, launchRaw)
	if err != nil || !historicalLaunch.Replayed || !bytes.Equal(historicalLaunch.ResultJSON, launch.ResultJSON) {
		t.Fatalf("historical launch replay=%#v err=%v", historicalLaunch, err)
	}
	historicalSend, err := service.Execute(ctx, sendRaw)
	if err != nil || !historicalSend.Replayed || !bytes.Equal(historicalSend.ResultJSON, sendResult.ResultJSON) {
		t.Fatalf("historical send replay=%#v err=%v", historicalSend, err)
	}
	if _, err := service.Execute(ctx, mutationExecutionRequest(external, launch.RunID, "resume", "matrix-resume-cleaned", 3, `{"message":"again"}`)); executionErrorCode(err) != executionjournal.CodeRunConflict {
		t.Fatalf("resume after cleanup err=%v", err)
	}
	if _, err := service.Execute(ctx, launchExecutionRequest(external, "matrix-relaunch", `{}`)); executionErrorCode(err) != executionjournal.CodeRunConflict {
		t.Fatalf("relaunch after cleanup err=%v", err)
	}
	if dispatcher.count() != before || before != 9 {
		t.Fatalf("operation matrix dispatcher calls=%d, want 9", dispatcher.count())
	}
}

func TestExecutionJournalTwoStoresClaimOneSideEffect(t *testing.T) {
	storeA, storeB := newExecutionStorePair(t)
	dispatcherA := &executionDispatchFake{entered: make(chan struct{}), release: make(chan struct{})}
	dispatcherB := &executionDispatchFake{}
	serviceA := newExecutionService(t, storeA, dispatcherA, "owner-race-a", 3, nil)
	serviceB := newExecutionService(t, storeB, dispatcherB, "owner-race-b", 4, nil)
	raw := launchExecutionRequest("external-race", "launch-race", `{"message":"race"}`)
	type response struct {
		result executionjournal.Result
		err    error
	}
	resultA := make(chan response, 1)
	go func() {
		result, err := serviceA.Execute(context.Background(), raw)
		resultA <- response{result: result, err: err}
	}()
	select {
	case <-dispatcherA.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first service did not cross the durable dispatch fence")
	}
	if _, err := serviceB.Execute(context.Background(), raw); executionErrorCode(err) != executionjournal.CodeReconciliationRequired {
		t.Fatalf("second service error=%v, want reconciliation_required", err)
	}
	if dispatcherB.count() != 0 {
		t.Fatalf("second service dispatched %d side effects", dispatcherB.count())
	}
	close(dispatcherA.release)
	select {
	case response := <-resultA:
		if response.err != nil || response.result.State != domain.ExecutionResult {
			t.Fatalf("first service result=%#v err=%v", response.result, response.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first service did not finish")
	}
	replay, err := serviceB.Execute(context.Background(), raw)
	if err != nil || !replay.Replayed || dispatcherA.count() != 1 || dispatcherB.count() != 0 {
		t.Fatalf("terminal replay=%#v err=%v callsA=%d callsB=%d", replay, err, dispatcherA.count(), dispatcherB.count())
	}
}

func TestExecutionJournalForeignReconciliationCannotOvertakeLiveDispatch(t *testing.T) {
	storeA, storeB := newExecutionStorePair(t)
	launchDispatcher := &executionDispatchFake{}
	launchService := newExecutionService(t, storeA, launchDispatcher, "owner-live-launch", 11, nil)
	launch, err := launchService.Execute(context.Background(), launchExecutionRequest("external-live-fence", "launch-live-fence", `{}`))
	if err != nil {
		t.Fatalf("launch: %v", err)
	}

	dispatcherA := &executionDispatchFake{entered: make(chan struct{}), release: make(chan struct{})}
	dispatcherB := &executionDispatchFake{}
	serviceA := newExecutionService(t, storeA, dispatcherA, "owner-live-mutation-a", 12, nil)
	serviceB := newExecutionService(t, storeB, dispatcherB, "owner-live-mutation-b", 13, nil)
	raw := mutationExecutionRequest("external-live-fence", launch.RunID, "send", "send-live-fence", 1, `{"message":"in flight"}`)
	type response struct {
		result executionjournal.Result
		err    error
	}
	resultA := make(chan response, 1)
	go func() {
		result, err := serviceA.Execute(context.Background(), raw)
		resultA <- response{result: result, err: err}
	}()
	select {
	case <-dispatcherA.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("owner did not enter dispatcher")
	}

	if _, err := serviceB.RefineExactResult(context.Background(), raw, executionjournal.DispatchReceipt{RunID: launch.RunID, ResultJSON: []byte(`{"proof":"foreign"}`)}); executionErrorCode(err) != executionjournal.CodeReconciliationRequired {
		t.Fatalf("foreign service refinement err=%v", err)
	}
	request, err := executionjournal.ParseRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	journal, found, err := storeB.GetExecutionOperation(context.Background(), request.OperationID)
	if err != nil || !found || journal.State != domain.ExecutionDispatched {
		t.Fatalf("live journal=%#v found=%v err=%v", journal, found, err)
	}
	foreignResult := []byte(`{"proof":"foreign"}`)
	foreignHash := sha256.Sum256(foreignResult)
	transition, err := storeB.RecordReconciledExecutionResult(context.Background(), executionjournal.Completion{
		OperationID: request.OperationID, ExternalRunID: request.ExternalRunID,
		RunID: launch.RunID, ProcessGeneration: 1, ResultJSON: foreignResult,
		ResultHash: foreignHash[:], CompletedAt: time.Now().UTC(),
	})
	if err != nil || transition.Changed || transition.Journal.State != domain.ExecutionDispatched || transition.Binding.State != domain.ExecutionBindingBusy {
		t.Fatalf("foreign store refinement=%#v err=%v", transition, err)
	}
	transition, err = storeB.RecordExecutionResult(context.Background(), executionjournal.Completion{
		OperationID: request.OperationID, ExternalRunID: request.ExternalRunID,
		RunID: launch.RunID, ProcessGeneration: 1, ResultJSON: foreignResult,
		ResultHash: foreignHash[:], CompletedAt: time.Now().UTC(),
	}, journal.DispatchOwner, strings.Repeat("e", 32))
	if err != nil || transition.Changed || transition.Journal.State != domain.ExecutionDispatched || transition.Binding.State != domain.ExecutionBindingBusy {
		t.Fatalf("foreign guessed-fence result=%#v err=%v", transition, err)
	}
	transition, err = storeB.RecordExecutionAmbiguous(context.Background(), executionjournal.Completion{
		OperationID: request.OperationID, ExternalRunID: request.ExternalRunID,
		ResultJSON: foreignResult, ResultHash: foreignHash[:], CompletedAt: time.Now().UTC(),
	}, journal.DispatchOwner, strings.Repeat("f", 32))
	if err != nil || transition.Changed || transition.Journal.State != domain.ExecutionDispatched || transition.Binding.State != domain.ExecutionBindingBusy {
		t.Fatalf("foreign guessed-fence ambiguity=%#v err=%v", transition, err)
	}
	overtake := mutationExecutionRequest("external-live-fence", launch.RunID, "interrupt", "interrupt-overtake", 1, `{}`)
	if _, err := serviceB.Execute(context.Background(), overtake); executionErrorCode(err) != executionjournal.CodeRunBusy || dispatcherB.count() != 0 {
		t.Fatalf("overtaking mutation err=%v calls=%d", err, dispatcherB.count())
	}
	if _, err := serviceB.Execute(context.Background(), raw); executionErrorCode(err) != executionjournal.CodeReconciliationRequired || dispatcherB.count() != 0 {
		t.Fatalf("identical live retry err=%v calls=%d", err, dispatcherB.count())
	}

	close(dispatcherA.release)
	select {
	case response := <-resultA:
		if response.err != nil || response.result.State != domain.ExecutionResult {
			t.Fatalf("owner result=%#v err=%v", response.result, response.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner did not finish")
	}
	replay, err := serviceB.Execute(context.Background(), raw)
	if err != nil || !replay.Replayed || replay.State != domain.ExecutionResult || dispatcherA.count() != 1 || dispatcherB.count() != 0 {
		t.Fatalf("owner terminal replay=%#v err=%v callsA=%d callsB=%d", replay, err, dispatcherA.count(), dispatcherB.count())
	}
}

func TestExecutionJournalCrashSeamsSurviveReopen(t *testing.T) {
	for _, test := range []struct {
		name  string
		point executionjournal.FaultPoint
	}{
		{name: "after-accept", point: executionjournal.FaultAfterAccept},
		{name: "after-dispatch", point: executionjournal.FaultAfterDispatch},
		{name: "after-side-effect", point: executionjournal.FaultAfterSideEffect},
		{name: "after-result", point: executionjournal.FaultAfterResult},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataDir := t.TempDir()
			firstStore := openExecutionStore(t, dataDir)
			dispatcher := &executionDispatchFake{}
			crash := errors.New("injected process loss")
			service := newExecutionService(t, firstStore, dispatcher, "owner-crash-a", 5, &executionFault{point: test.point, err: crash})
			raw := launchExecutionRequest("external-"+test.name, "launch-"+test.name, `{"message":"crash seam"}`)
			if _, err := service.Execute(context.Background(), raw); !errors.Is(err, crash) {
				t.Fatalf("initial error=%v, want injected crash", err)
			}
			if err := firstStore.Close(); err != nil {
				t.Fatalf("close first store: %v", err)
			}

			reopened := openExecutionStore(t, dataDir)
			defer func() { _ = reopened.Close() }()
			recovery := newExecutionService(t, reopened, dispatcher, "owner-crash-b", 6, nil)
			request, err := executionjournal.ParseRequest(raw)
			if err != nil {
				t.Fatal(err)
			}
			journal, found, err := reopened.GetExecutionOperation(context.Background(), request.OperationID)
			if err != nil || !found {
				t.Fatalf("reopened journal found=%v err=%v", found, err)
			}
			switch test.point {
			case executionjournal.FaultAfterAccept:
				if journal.State != domain.ExecutionAccepted || dispatcher.count() != 0 {
					t.Fatalf("accepted seam journal=%#v calls=%d", journal, dispatcher.count())
				}
				result, err := recovery.Execute(context.Background(), raw)
				if err != nil || result.State != domain.ExecutionResult || dispatcher.count() != 1 {
					t.Fatalf("accepted recovery=%#v err=%v calls=%d", result, err, dispatcher.count())
				}
			case executionjournal.FaultAfterDispatch:
				if journal.State != domain.ExecutionDispatched || dispatcher.count() != 0 {
					t.Fatalf("dispatch seam journal=%#v calls=%d", journal, dispatcher.count())
				}
				if _, err := recovery.Execute(context.Background(), raw); executionErrorCode(err) != executionjournal.CodeReconciliationRequired || dispatcher.count() != 0 {
					t.Fatalf("dispatch retry err=%v calls=%d", err, dispatcher.count())
				}
				if _, err := recovery.RefineExactResult(context.Background(), raw, executionjournal.DispatchReceipt{RunID: "ao-external-" + test.name, ResultJSON: []byte(`{"proof":"untrusted"}`)}); executionErrorCode(err) != executionjournal.CodeReconciliationRequired {
					t.Fatalf("foreign exact refinement error=%v", err)
				}
			case executionjournal.FaultAfterSideEffect:
				if journal.State != domain.ExecutionDispatched || dispatcher.count() != 1 {
					t.Fatalf("side-effect seam journal=%#v calls=%d", journal, dispatcher.count())
				}
				if _, err := recovery.Execute(context.Background(), raw); executionErrorCode(err) != executionjournal.CodeReconciliationRequired || dispatcher.count() != 1 {
					t.Fatalf("side-effect retry err=%v calls=%d", err, dispatcher.count())
				}
				if _, err := recovery.RefineExactResult(context.Background(), raw, executionjournal.DispatchReceipt{RunID: "ao-external-" + test.name, ResultJSON: []byte(`{"proof":"untrusted"}`)}); executionErrorCode(err) != executionjournal.CodeReconciliationRequired || dispatcher.count() != 1 {
					t.Fatalf("foreign exact refinement error=%v calls=%d", err, dispatcher.count())
				}
			case executionjournal.FaultAfterResult:
				if journal.State != domain.ExecutionResult || dispatcher.count() != 1 {
					t.Fatalf("result seam journal=%#v calls=%d", journal, dispatcher.count())
				}
				replay, err := recovery.Execute(context.Background(), raw)
				if err != nil || replay.State != domain.ExecutionResult || !replay.Replayed || dispatcher.count() != 1 {
					t.Fatalf("result recovery=%#v err=%v calls=%d", replay, err, dispatcher.count())
				}
			}
		})
	}
}

func TestExecutionJournalAmbiguityNeverRedispatchesAndCanRefine(t *testing.T) {
	store := newTestStore(t)
	dispatchFailure := errors.New("response lost")
	dispatcher := &executionDispatchFake{handler: func(executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error) {
		return executionjournal.DispatchReceipt{}, dispatchFailure
	}}
	service := newExecutionService(t, store, dispatcher, "owner-ambiguous-a", 7, nil)
	raw := launchExecutionRequest("external-ambiguous", "launch-ambiguous", `{}`)
	ambiguous, err := service.Execute(context.Background(), raw)
	if err != nil || ambiguous.State != domain.ExecutionAmbiguous || dispatcher.count() != 1 {
		t.Fatalf("ambiguity=%#v err=%v calls=%d", ambiguous, err, dispatcher.count())
	}
	binding, found, bindingErr := store.GetExecutionRunBinding(context.Background(), "external-ambiguous")
	if bindingErr != nil || !found || binding.State != domain.ExecutionBindingAmbiguous || binding.PendingOperationID == "" {
		t.Fatalf("owner ambiguity binding=%#v found=%v err=%v", binding, found, bindingErr)
	}
	replay, err := service.Execute(context.Background(), raw)
	if err != nil || replay.State != domain.ExecutionAmbiguous || !replay.Replayed || dispatcher.count() != 1 {
		t.Fatalf("ambiguous replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
	}
	refined, err := service.RefineExactResult(context.Background(), raw, executionjournal.DispatchReceipt{
		RunID: "ao-external-ambiguous", ResultJSON: []byte(`{"proof":"authoritative"}`),
	})
	if err != nil || refined.State != domain.ExecutionResult || dispatcher.count() != 1 {
		t.Fatalf("refined=%#v err=%v calls=%d", refined, err, dispatcher.count())
	}
	assertExecutionBindingState(t, store, "external-ambiguous", domain.ExecutionBindingActive, 1)
	terminal, err := service.Execute(context.Background(), raw)
	if err != nil || terminal.State != domain.ExecutionResult || string(terminal.ResultJSON) != `{"proof":"authoritative"}` || dispatcher.count() != 1 {
		t.Fatalf("terminal=%#v err=%v calls=%d", terminal, err, dispatcher.count())
	}
}

func TestExecutionJournalFinalizesAfterCallerCancellation(t *testing.T) {
	for _, test := range []struct {
		name      string
		dispatch  func(executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error)
		wantState domain.ExecutionJournalState
		wantBind  domain.ExecutionRunBindingState
	}{
		{
			name: "dispatcher-error",
			dispatch: func(executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error) {
				return executionjournal.DispatchReceipt{}, context.Canceled
			},
			wantState: domain.ExecutionAmbiguous,
			wantBind:  domain.ExecutionBindingAmbiguous,
		},
		{
			name: "successful-receipt",
			dispatch: func(command executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error) {
				return executionReceipt(command), nil
			},
			wantState: domain.ExecutionResult,
			wantBind:  domain.ExecutionBindingActive,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			const contextValue = "request-scope-value"
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), executionFinalizationContextKey{}, contextValue))
			defer cancel()
			observedFinalizationContext := false
			checkFinalizationContext := func(ctx context.Context) error {
				observedFinalizationContext = true
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("finalization inherited caller cancellation: %w", err)
				}
				if value := ctx.Value(executionFinalizationContextKey{}); value != contextValue {
					return fmt.Errorf("finalization lost caller context value: %v", value)
				}
				if _, ok := ctx.Deadline(); !ok {
					return errors.New("finalization context has no service deadline")
				}
				return nil
			}
			finalizationStore := &executionTerminalWriteStore{Store: store}
			finalizationStore.recordResult = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
				if err := checkFinalizationContext(ctx); err != nil {
					return executionjournal.Transition{}, err
				}
				return store.RecordExecutionResult(ctx, completion, owner, fence)
			}
			finalizationStore.recordAmbiguous = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
				if err := checkFinalizationContext(ctx); err != nil {
					return executionjournal.Transition{}, err
				}
				return store.RecordExecutionAmbiguous(ctx, completion, owner, fence)
			}
			dispatcher := &executionDispatchFake{handler: func(command executionjournal.DispatchCommand) (executionjournal.DispatchReceipt, error) {
				cancel()
				return test.dispatch(command)
			}}
			service := newExecutionServiceWithFinalizationTimeout(t, finalizationStore, dispatcher, "owner-cancel-"+test.name, 21, nil, time.Second)
			raw := launchExecutionRequest("external-cancel-"+test.name, "launch-cancel-"+test.name, `{}`)

			first, err := service.Execute(ctx, raw)
			if err != nil || first.State != test.wantState || first.Replayed || dispatcher.count() != 1 {
				t.Fatalf("first=%#v err=%v calls=%d", first, err, dispatcher.count())
			}
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf("caller context error=%v, want canceled", ctx.Err())
			}
			if !observedFinalizationContext {
				t.Fatal("terminal store did not observe a finalization context")
			}
			if test.wantState == domain.ExecutionAmbiguous {
				assertExecutionPendingBindingState(t, store, "external-cancel-"+test.name, test.wantBind, 1)
			} else {
				assertExecutionBindingState(t, store, "external-cancel-"+test.name, test.wantBind, 1)
			}

			replay, err := service.Execute(context.Background(), raw)
			if err != nil || replay.State != test.wantState || !replay.Replayed || !bytes.Equal(replay.ResultJSON, first.ResultJSON) || dispatcher.count() != 1 {
				t.Fatalf("replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
			}
		})
	}
}

func TestExecutionJournalPostClaimCancellationUsesDetachedConfirmation(t *testing.T) {
	for _, test := range []struct {
		name       string
		isDeadline bool
		isMutation bool
		wantErr    error
	}{
		{name: "canceled", wantErr: context.Canceled},
		{name: "deadline-exceeded", isDeadline: true, wantErr: context.DeadlineExceeded},
		{name: "canceled-mutation", isMutation: true, wantErr: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			externalRunID := "external-post-claim-" + test.name
			raw := launchExecutionRequest(externalRunID, "launch-post-claim-"+test.name, `{}`)
			if test.isMutation {
				seedDispatcher := &executionDispatchFake{}
				seedService := newExecutionService(t, store, seedDispatcher, "owner-post-claim-seed", 27, nil)
				launch, err := seedService.Execute(context.Background(), raw)
				if err != nil || launch.State != domain.ExecutionResult || seedDispatcher.count() != 1 {
					t.Fatalf("seed launch=%#v err=%v calls=%d", launch, err, seedDispatcher.count())
				}
				raw = mutationExecutionRequest(externalRunID, launch.RunID, "send", "mutation-post-claim-"+test.name, 1, `{}`)
			}
			const contextValue = "post-claim-request-scope"
			base := context.WithValue(context.Background(), executionFinalizationContextKey{}, contextValue)
			var callerCtx context.Context
			var expireCaller func()
			if test.isDeadline {
				deadlineCtx := newExecutionControlledDeadlineContext(base)
				callerCtx = deadlineCtx
				expireCaller = deadlineCtx.expire
			} else {
				cancelCtx, cancel := context.WithCancel(base)
				callerCtx = cancelCtx
				expireCaller = cancel
				defer cancel()
			}

			claimCalls, confirmationCalls, ambiguityCalls := 0, 0, 0
			var confirmationContextErr error
			var ambiguityContextErr error
			faultStore := &executionTerminalWriteStore{Store: store}
			faultStore.claimDispatch = func(ctx context.Context, operationID, owner, fence string, at time.Time) (executionjournal.Transition, error) {
				claimCalls++
				transition, err := store.ClaimExecutionDispatch(ctx, operationID, owner, fence, at)
				if err == nil && transition.Changed {
					expireCaller()
				}
				return transition, err
			}
			faultStore.confirmDispatch = func(ctx context.Context, operationID, owner, fence string) (executionjournal.Transition, error) {
				confirmationCalls++
				if err := detachedContextError(ctx, contextValue); err != nil {
					confirmationContextErr = err
					return executionjournal.Transition{}, err
				}
				return store.ConfirmExecutionDispatch(ctx, operationID, owner, fence)
			}
			faultStore.recordAmbiguous = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
				ambiguityCalls++
				if err := detachedContextError(ctx, contextValue); err != nil {
					ambiguityContextErr = err
					return executionjournal.Transition{}, err
				}
				return store.RecordExecutionAmbiguous(ctx, completion, owner, fence)
			}

			// Confirmation is durably proven despite caller cancellation, so the
			// dispatcher receives exactly one attempt with the canceled caller
			// context; its refusal is then durably closed as ambiguous.
			dispatcher := &executionDispatchFake{release: make(chan struct{})}
			service := newExecutionServiceWithFinalizationTimeout(t, faultStore, dispatcher, "owner-post-claim-"+test.name, 24, nil, time.Second)

			first, err := service.Execute(callerCtx, raw)
			if err != nil || first.State != domain.ExecutionAmbiguous || first.Replayed || dispatcher.count() != 1 {
				t.Fatalf("first=%#v err=%v calls=%d", first, err, dispatcher.count())
			}
			if !errors.Is(callerCtx.Err(), test.wantErr) {
				t.Fatalf("caller context error=%v, want %v", callerCtx.Err(), test.wantErr)
			}
			if confirmationContextErr != nil || ambiguityContextErr != nil {
				t.Fatalf("detached contexts: confirmation=%v ambiguity=%v", confirmationContextErr, ambiguityContextErr)
			}
			if claimCalls != 1 || confirmationCalls != 1 || ambiguityCalls != 1 {
				t.Fatalf("transition calls: claim=%d confirmation=%d ambiguity=%d", claimCalls, confirmationCalls, ambiguityCalls)
			}
			assertExecutionPendingBindingState(t, store, externalRunID, domain.ExecutionBindingAmbiguous, 1)

			replay, err := service.Execute(context.Background(), raw)
			if err != nil || replay.State != domain.ExecutionAmbiguous || !replay.Replayed || !bytes.Equal(replay.ResultJSON, first.ResultJSON) || dispatcher.count() != 1 {
				t.Fatalf("replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
			}
			if claimCalls != 1 || confirmationCalls != 1 || ambiguityCalls != 1 {
				t.Fatalf("exact retry attempted transitions: claim=%d confirmation=%d ambiguity=%d", claimCalls, confirmationCalls, ambiguityCalls)
			}
		})
	}
}

func TestExecutionJournalConfirmationUncertaintyClosesWithOwnerFence(t *testing.T) {
	for _, test := range []struct {
		name                string
		postCommit          bool
		notProven           bool
		returnedState       domain.ExecutionJournalState
		preexistingTerminal bool
		exhaustConfirmation bool
		wantReplay          bool
	}{
		{name: "pre-commit-error"},
		{name: "post-commit-ack-lost", postCommit: true},
		{name: "confirmation-not-proven", notProven: true},
		{name: "confirmation-not-proven-invalid-state", notProven: true, returnedState: domain.ExecutionAccepted},
		{name: "confirmation-changed-invalid-state", returnedState: domain.ExecutionAccepted},
		{name: "confirmation-error-replays-durable-terminal", preexistingTerminal: true, wantReplay: true},
		{name: "confirmation-timeout-fresh-ambiguity-window", exhaustConfirmation: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			confirmationCalls, ambiguityCalls := 0, 0
			confirmationErr := errors.New("dispatch confirmation unavailable")
			var ambiguityContextErr error
			externalRunID := "external-confirm-" + test.name
			raw := launchExecutionRequest(externalRunID, "launch-confirm-"+test.name, `{}`)
			faultStore := &executionTerminalWriteStore{Store: store}
			faultStore.confirmDispatch = func(ctx context.Context, operationID, owner, fence string) (executionjournal.Transition, error) {
				confirmationCalls++
				if test.exhaustConfirmation {
					<-ctx.Done()
					return executionjournal.Transition{}, ctx.Err()
				}
				if test.postCommit || test.notProven || test.returnedState != "" || test.preexistingTerminal {
					transition, err := store.ConfirmExecutionDispatch(ctx, operationID, owner, fence)
					if err != nil {
						return executionjournal.Transition{}, err
					}
					if test.preexistingTerminal {
						canonical := []byte(`{"status":"ambiguous"}`)
						hash := sha256.Sum256(canonical)
						if _, err := store.RecordExecutionAmbiguous(ctx, executionjournal.Completion{
							OperationID: operationID, ExternalRunID: externalRunID,
							ResultJSON: canonical, ResultHash: hash[:], CompletedAt: time.Now().UTC(),
						}, owner, fence); err != nil {
							return executionjournal.Transition{}, err
						}
						return executionjournal.Transition{}, confirmationErr
					}
					if test.returnedState != "" {
						transition.Journal.State = test.returnedState
					}
					if test.notProven {
						transition.Changed = false
						return transition, nil
					}
					if test.returnedState != "" {
						return transition, nil
					}
				}
				return executionjournal.Transition{}, confirmationErr
			}
			faultStore.recordAmbiguous = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
				ambiguityCalls++
				if err := detachedContextError(ctx, ""); err != nil {
					ambiguityContextErr = err
					return executionjournal.Transition{}, err
				}
				return store.RecordExecutionAmbiguous(ctx, completion, owner, fence)
			}
			dispatcher := &executionDispatchFake{}
			service := newExecutionServiceWithFinalizationTimeout(t, faultStore, dispatcher, "owner-confirm-"+test.name, 25, nil, 25*time.Millisecond)

			first, err := service.Execute(context.Background(), raw)
			if err != nil || first.State != domain.ExecutionAmbiguous || first.Replayed != test.wantReplay || dispatcher.count() != 0 {
				t.Fatalf("first=%#v err=%v calls=%d", first, err, dispatcher.count())
			}
			if ambiguityContextErr != nil {
				t.Fatalf("fresh ambiguity context=%v", ambiguityContextErr)
			}
			if confirmationCalls != 1 || ambiguityCalls != 1 {
				t.Fatalf("transition calls: confirmation=%d ambiguity=%d", confirmationCalls, ambiguityCalls)
			}
			assertExecutionPendingBindingState(t, store, externalRunID, domain.ExecutionBindingAmbiguous, 1)

			replay, err := service.Execute(context.Background(), raw)
			if err != nil || replay.State != domain.ExecutionAmbiguous || !replay.Replayed || !bytes.Equal(replay.ResultJSON, first.ResultJSON) || dispatcher.count() != 0 {
				t.Fatalf("replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
			}
			if confirmationCalls != 1 || ambiguityCalls != 1 {
				t.Fatalf("exact retry attempted transitions: confirmation=%d ambiguity=%d", confirmationCalls, ambiguityCalls)
			}
		})
	}
}

func TestExecutionJournalConfirmationAmbiguityFailureIsBoundedAndFailsClosed(t *testing.T) {
	store := newTestStore(t)
	confirmationCalls, ambiguityCalls := 0, 0
	confirmationUnavailable := errors.New("confirmation database unavailable")
	ambiguityUnavailable := errors.New("ambiguity database unavailable")
	faultStore := &executionTerminalWriteStore{Store: store}
	faultStore.confirmDispatch = func(ctx context.Context, _, _, _ string) (executionjournal.Transition, error) {
		confirmationCalls++
		<-ctx.Done()
		return executionjournal.Transition{}, errors.Join(confirmationUnavailable, ctx.Err())
	}
	faultStore.recordAmbiguous = func(ctx context.Context, _ executionjournal.Completion, _, _ string) (executionjournal.Transition, error) {
		ambiguityCalls++
		if err := ctx.Err(); err != nil {
			return executionjournal.Transition{}, fmt.Errorf("ambiguity context began canceled: %w", err)
		}
		if _, ok := ctx.Deadline(); !ok {
			return executionjournal.Transition{}, errors.New("ambiguity context has no service deadline")
		}
		<-ctx.Done()
		return executionjournal.Transition{}, errors.Join(ambiguityUnavailable, ctx.Err())
	}
	dispatcher := &executionDispatchFake{}
	service := newExecutionServiceWithFinalizationTimeout(t, faultStore, dispatcher, "owner-confirm-db-unavailable", 26, nil, 20*time.Millisecond)
	raw := launchExecutionRequest("external-confirm-db-unavailable", "launch-confirm-db-unavailable", `{}`)

	started := time.Now()
	_, err := service.Execute(context.Background(), raw)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("bounded confirmation recovery took %s", elapsed)
	}
	if code, reason, ok := executionjournal.ErrorInfo(err); !ok || code != executionjournal.CodeStorageFailure || reason != "dispatch_confirmation_and_ambiguity_failed" {
		t.Fatalf("recovery error=%v code=%q reason=%q", err, code, reason)
	}
	if !errors.Is(err, confirmationUnavailable) || !errors.Is(err, ambiguityUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recovery error lost joined causes: %v", err)
	}
	if confirmationCalls != 1 || ambiguityCalls != 1 || dispatcher.count() != 0 {
		t.Fatalf("calls: confirmation=%d ambiguity=%d dispatch=%d", confirmationCalls, ambiguityCalls, dispatcher.count())
	}
	request, parseErr := executionjournal.ParseRequest(raw)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	journal, found, journalErr := store.GetExecutionOperation(context.Background(), request.OperationID)
	if journalErr != nil || !found || journal.State != domain.ExecutionDispatched {
		t.Fatalf("journal=%#v found=%v err=%v", journal, found, journalErr)
	}
	assertExecutionPendingBindingState(t, store, "external-confirm-db-unavailable", domain.ExecutionBindingLaunching, 1)

	if _, retryErr := service.Execute(context.Background(), raw); executionErrorCode(retryErr) != executionjournal.CodeReconciliationRequired || dispatcher.count() != 0 {
		t.Fatalf("exact retry err=%v dispatch calls=%d", retryErr, dispatcher.count())
	}
	if confirmationCalls != 1 || ambiguityCalls != 1 {
		t.Fatalf("exact retry attempted recovery: confirmation=%d ambiguity=%d", confirmationCalls, ambiguityCalls)
	}
}

func TestExecutionJournalConfirmationAmbiguityAckLossReplaysDurableTerminal(t *testing.T) {
	store := newTestStore(t)
	confirmationCalls, ambiguityCalls := 0, 0
	confirmationUnavailable := errors.New("confirmation database unavailable")
	ambiguityAckLost := errors.New("ambiguity acknowledgement lost")
	faultStore := &executionTerminalWriteStore{Store: store}
	faultStore.confirmDispatch = func(context.Context, string, string, string) (executionjournal.Transition, error) {
		confirmationCalls++
		return executionjournal.Transition{}, confirmationUnavailable
	}
	faultStore.recordAmbiguous = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
		ambiguityCalls++
		if err := detachedContextError(ctx, ""); err != nil {
			return executionjournal.Transition{}, err
		}
		if _, err := store.RecordExecutionAmbiguous(ctx, completion, owner, fence); err != nil {
			return executionjournal.Transition{}, err
		}
		return executionjournal.Transition{}, ambiguityAckLost
	}
	dispatcher := &executionDispatchFake{}
	service := newExecutionServiceWithFinalizationTimeout(t, faultStore, dispatcher, "owner-confirm-ambiguity-ack", 28, nil, time.Second)
	raw := launchExecutionRequest("external-confirm-ambiguity-ack", "launch-confirm-ambiguity-ack", `{}`)

	_, err := service.Execute(context.Background(), raw)
	if code, reason, ok := executionjournal.ErrorInfo(err); !ok || code != executionjournal.CodeStorageFailure || reason != "dispatch_confirmation_and_ambiguity_failed" {
		t.Fatalf("ack-loss error=%v code=%q reason=%q", err, code, reason)
	}
	if !errors.Is(err, confirmationUnavailable) || !errors.Is(err, ambiguityAckLost) {
		t.Fatalf("ack-loss error lost joined causes: %v", err)
	}
	if confirmationCalls != 1 || ambiguityCalls != 1 || dispatcher.count() != 0 {
		t.Fatalf("calls: confirmation=%d ambiguity=%d dispatch=%d", confirmationCalls, ambiguityCalls, dispatcher.count())
	}
	assertExecutionPendingBindingState(t, store, "external-confirm-ambiguity-ack", domain.ExecutionBindingAmbiguous, 1)

	replay, err := service.Execute(context.Background(), raw)
	if err != nil || replay.State != domain.ExecutionAmbiguous || !replay.Replayed || dispatcher.count() != 0 {
		t.Fatalf("replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
	}
	if confirmationCalls != 1 || ambiguityCalls != 1 {
		t.Fatalf("exact retry attempted recovery: confirmation=%d ambiguity=%d", confirmationCalls, ambiguityCalls)
	}
}

func TestExecutionJournalResultWriteUncertaintyClosesWithOwnerFence(t *testing.T) {
	for _, test := range []struct {
		name       string
		postCommit bool
		wantState  domain.ExecutionJournalState
		wantBind   domain.ExecutionRunBindingState
		wantReplay bool
	}{
		{name: "pre-commit-error", wantState: domain.ExecutionAmbiguous, wantBind: domain.ExecutionBindingAmbiguous},
		{name: "post-commit-ack-lost", postCommit: true, wantState: domain.ExecutionResult, wantBind: domain.ExecutionBindingActive, wantReplay: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			resultCalls, ambiguityCalls := 0, 0
			ackLost := errors.New("terminal write acknowledgement lost")
			faultStore := &executionTerminalWriteStore{Store: store}
			faultStore.recordResult = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
				resultCalls++
				if test.postCommit {
					if _, err := store.RecordExecutionResult(ctx, completion, owner, fence); err != nil {
						return executionjournal.Transition{}, err
					}
				}
				return executionjournal.Transition{}, ackLost
			}
			faultStore.recordAmbiguous = func(ctx context.Context, completion executionjournal.Completion, owner, fence string) (executionjournal.Transition, error) {
				ambiguityCalls++
				return store.RecordExecutionAmbiguous(ctx, completion, owner, fence)
			}
			dispatcher := &executionDispatchFake{}
			service := newExecutionServiceWithFinalizationTimeout(t, faultStore, dispatcher, "owner-uncertain-"+test.name, 22, nil, time.Second)
			raw := launchExecutionRequest("external-uncertain-"+test.name, "launch-uncertain-"+test.name, `{}`)

			first, err := service.Execute(context.Background(), raw)
			if err != nil || first.State != test.wantState || first.Replayed != test.wantReplay || dispatcher.count() != 1 {
				t.Fatalf("first=%#v err=%v calls=%d", first, err, dispatcher.count())
			}
			if resultCalls != 1 || ambiguityCalls != 1 {
				t.Fatalf("terminal write calls: result=%d ambiguity=%d", resultCalls, ambiguityCalls)
			}
			if test.wantState == domain.ExecutionAmbiguous {
				assertExecutionPendingBindingState(t, store, "external-uncertain-"+test.name, test.wantBind, 1)
			} else {
				assertExecutionBindingState(t, store, "external-uncertain-"+test.name, test.wantBind, 1)
			}

			replay, err := service.Execute(context.Background(), raw)
			if err != nil || replay.State != test.wantState || !replay.Replayed || !bytes.Equal(replay.ResultJSON, first.ResultJSON) || dispatcher.count() != 1 {
				t.Fatalf("replay=%#v err=%v calls=%d", replay, err, dispatcher.count())
			}
			if resultCalls != 1 || ambiguityCalls != 1 {
				t.Fatalf("exact retry attempted terminal writes: result=%d ambiguity=%d", resultCalls, ambiguityCalls)
			}
		})
	}
}

func TestExecutionJournalFinalizationTimeoutFailsClosed(t *testing.T) {
	store := newTestStore(t)
	resultCalls, ambiguityCalls := 0, 0
	resultHadDeadline, ambiguityHadDeadline := false, false
	blockUntilDeadline := func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("finalization context has no deadline")
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("finalization context began canceled: %w", err)
		}
		<-ctx.Done()
		return ctx.Err()
	}
	faultStore := &executionTerminalWriteStore{Store: store}
	faultStore.recordResult = func(ctx context.Context, _ executionjournal.Completion, _, _ string) (executionjournal.Transition, error) {
		resultCalls++
		_, resultHadDeadline = ctx.Deadline()
		return executionjournal.Transition{}, blockUntilDeadline(ctx)
	}
	faultStore.recordAmbiguous = func(ctx context.Context, _ executionjournal.Completion, _, _ string) (executionjournal.Transition, error) {
		ambiguityCalls++
		_, ambiguityHadDeadline = ctx.Deadline()
		return executionjournal.Transition{}, blockUntilDeadline(ctx)
	}
	dispatcher := &executionDispatchFake{}
	service := newExecutionServiceWithFinalizationTimeout(t, faultStore, dispatcher, "owner-timeout-a", 23, nil, 20*time.Millisecond)
	raw := launchExecutionRequest("external-timeout", "launch-timeout", `{}`)

	started := time.Now()
	_, err := service.Execute(context.Background(), raw)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("bounded finalization took %s", elapsed)
	}
	if code, reason, ok := executionjournal.ErrorInfo(err); !ok || code != executionjournal.CodeStorageFailure || reason != "record_result_and_ambiguity_failed" {
		t.Fatalf("timeout error=%v code=%q reason=%q", err, code, reason)
	}
	if resultCalls != 1 || ambiguityCalls != 1 || dispatcher.count() != 1 {
		t.Fatalf("calls: result=%d ambiguity=%d dispatch=%d", resultCalls, ambiguityCalls, dispatcher.count())
	}
	if !resultHadDeadline || !ambiguityHadDeadline {
		t.Fatalf("service deadlines: result=%v ambiguity=%v", resultHadDeadline, ambiguityHadDeadline)
	}
	request, parseErr := executionjournal.ParseRequest(raw)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	journal, found, journalErr := store.GetExecutionOperation(context.Background(), request.OperationID)
	if journalErr != nil || !found || journal.State != domain.ExecutionDispatched {
		t.Fatalf("journal=%#v found=%v err=%v", journal, found, journalErr)
	}
	assertExecutionPendingBindingState(t, store, "external-timeout", domain.ExecutionBindingLaunching, 1)

	if _, retryErr := service.Execute(context.Background(), raw); executionErrorCode(retryErr) != executionjournal.CodeReconciliationRequired || dispatcher.count() != 1 {
		t.Fatalf("exact retry err=%v dispatch calls=%d", retryErr, dispatcher.count())
	}
	if _, refineErr := service.RefineExactResult(context.Background(), raw, executionjournal.DispatchReceipt{RunID: "ao-external-timeout", ResultJSON: []byte(`{"proof":"foreign"}`)}); executionErrorCode(refineErr) != executionjournal.CodeReconciliationRequired {
		t.Fatalf("foreign refinement err=%v", refineErr)
	}
}

func TestExecutionJournalConcurrentSQLiteStress(t *testing.T) {
	storeA, storeB := newExecutionStorePair(t)
	dispatcher := &executionDispatchFake{}
	serviceA := newExecutionService(t, storeA, dispatcher, "owner-stress-a", 8, nil)
	serviceB := newExecutionService(t, storeB, dispatcher, "owner-stress-b", 9, nil)
	const operations = 32
	errorsSeen := make(chan error, operations)
	var wg sync.WaitGroup
	for i := 0; i < operations; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			service := serviceA
			if i%2 != 0 {
				service = serviceB
			}
			external := fmt.Sprintf("external-stress-%02d", i)
			raw := launchExecutionRequest(external, fmt.Sprintf("launch-stress-%02d", i), fmt.Sprintf(`{"ordinal":%d}`, i))
			first, err := service.Execute(context.Background(), raw)
			if err != nil || first.State != domain.ExecutionResult {
				errorsSeen <- fmt.Errorf("first %d: result=%#v err=%w", i, first, err)
				return
			}
			replay, err := service.Execute(context.Background(), raw)
			if err != nil || !replay.Replayed || !bytes.Equal(first.ResultJSON, replay.ResultJSON) {
				errorsSeen <- fmt.Errorf("replay %d: result=%#v err=%w", i, replay, err)
			}
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	if dispatcher.count() != operations {
		t.Fatalf("dispatcher calls=%d, want %d", dispatcher.count(), operations)
	}
}

func TestExecutionJournalSQLiteUsesByteLimitsAndRollsBack(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	oversized := []byte(`{"value":"` + strings.Repeat("é", 524_288) + `"}`)
	if len(oversized) <= 1<<20 || len([]rune(string(oversized))) >= 1<<20 {
		t.Fatalf("fixture bytes=%d runes=%d", len(oversized), len([]rune(string(oversized))))
	}
	hash := sha256.Sum256(oversized)
	journal := domain.ExecutionOperationJournal{
		OperationID: strings.Repeat("a", 64), ExternalRunID: "external-byte-request",
		Operation: domain.ExecutionLaunch, IdempotencyKey: "launch-byte-request",
		RequestHash: hash[:], RequestJSON: oversized, TargetProcessGeneration: 1,
		State: domain.ExecutionAccepted, AcceptedAt: now,
	}
	if _, err := store.AcceptExecutionOperation(ctx, journal); err == nil {
		t.Fatal("multibyte request exceeding the byte limit was stored")
	}
	if binding, found, err := store.GetExecutionRunBinding(ctx, journal.ExternalRunID); err != nil || found {
		t.Fatalf("failed request left binding=%#v found=%v err=%v", binding, found, err)
	}

	small := []byte(`{"request":{}}`)
	smallHash := sha256.Sum256(small)
	journal = domain.ExecutionOperationJournal{
		OperationID: strings.Repeat("b", 64), ExternalRunID: "external-byte-result",
		Operation: domain.ExecutionLaunch, IdempotencyKey: "launch-byte-result",
		RequestHash: smallHash[:], RequestJSON: small, TargetProcessGeneration: 1,
		State: domain.ExecutionAccepted, AcceptedAt: now,
	}
	accepted, err := store.AcceptExecutionOperation(ctx, journal)
	if err != nil || accepted.Status != executionjournal.AcceptCreated {
		t.Fatalf("accept small journal=%#v err=%v", accepted, err)
	}
	owner, fence := "owner-byte-result", strings.Repeat("c", 32)
	claimed, err := store.ClaimExecutionDispatch(ctx, journal.OperationID, owner, fence, now.Add(time.Second))
	if err != nil || !claimed.Changed {
		t.Fatalf("claim=%#v err=%v", claimed, err)
	}
	resultHash := sha256.Sum256(oversized)
	if _, err := store.RecordExecutionResult(ctx, executionjournal.Completion{
		OperationID: journal.OperationID, ExternalRunID: journal.ExternalRunID,
		RunID: "ao-byte-result", ProcessGeneration: 1, ResultJSON: oversized,
		ResultHash: resultHash[:], CompletedAt: now.Add(2 * time.Second),
	}, owner, fence); err == nil {
		t.Fatal("multibyte result exceeding the byte limit was stored")
	}
	after, found, err := store.GetExecutionOperation(ctx, journal.OperationID)
	if err != nil || !found || after.State != domain.ExecutionDispatched {
		t.Fatalf("failed result transition journal=%#v found=%v err=%v", after, found, err)
	}
	binding, found, err := store.GetExecutionRunBinding(ctx, journal.ExternalRunID)
	if err != nil || !found || binding.State != domain.ExecutionBindingLaunching || binding.PendingOperationID != journal.OperationID {
		t.Fatalf("failed result transition binding=%#v found=%v err=%v", binding, found, err)
	}
}

func executionReceipt(command executionjournal.DispatchCommand) executionjournal.DispatchReceipt {
	runID := command.RunID
	if command.Operation == domain.ExecutionLaunch {
		runID = "ao-" + command.ExternalRunID
	}
	return executionjournal.DispatchReceipt{
		RunID:      runID,
		ResultJSON: []byte(fmt.Sprintf(`{"generation":%d,"ok":true,"operation":%q}`, command.ProcessGeneration, command.Operation)),
	}
}

func launchExecutionRequest(externalRunID, key, payload string) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"externalRunId":%q,"operation":"launch","idempotencyKey":%q,"request":%s}`, externalRunID, key, payload))
}

func mutationExecutionRequest(externalRunID, runID, operation, key string, generation int64, payload string) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"externalRunId":%q,"runId":%q,"operation":%q,"idempotencyKey":%q,"expectedProcessGeneration":%d,"request":%s}`, externalRunID, runID, operation, key, generation, payload))
}

func newExecutionService(t *testing.T, store executionjournal.Store, dispatcher executionjournal.Dispatcher, owner string, seed byte, faults executionjournal.FaultInjector) *executionjournal.Service {
	t.Helper()
	return newExecutionServiceWithFinalizationTimeout(t, store, dispatcher, owner, seed, faults, 0)
}

func newExecutionServiceWithFinalizationTimeout(t *testing.T, store executionjournal.Store, dispatcher executionjournal.Dispatcher, owner string, seed byte, faults executionjournal.FaultInjector, finalizationTimeout time.Duration) *executionjournal.Service {
	t.Helper()
	service, err := executionjournal.New(executionjournal.Deps{
		Store: store, Dispatcher: dispatcher, DispatchOwner: owner,
		Now:    func() time.Time { return time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC) },
		Random: bytes.NewReader(bytes.Repeat([]byte{seed}, 4096)), Faults: faults, FinalizationTimeout: finalizationTimeout,
	})
	if err != nil {
		t.Fatalf("new execution service: %v", err)
	}
	return service
}

func newExecutionStorePair(t *testing.T) (*sqlite.Store, *sqlite.Store) {
	t.Helper()
	dataDir := t.TempDir()
	first := openExecutionStore(t, dataDir)
	second := openExecutionStore(t, dataDir)
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })
	return first, second
}

func openExecutionStore(t *testing.T, dataDir string) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatalf("open execution store: %v", err)
	}
	return store
}

func executionErrorCode(err error) executionjournal.ErrorCode {
	code, _, _ := executionjournal.ErrorInfo(err)
	return code
}

func assertExecutionBindingState(t *testing.T, store executionjournal.Store, externalRunID string, state domain.ExecutionRunBindingState, generation int64) {
	t.Helper()
	binding, found, err := store.GetExecutionRunBinding(context.Background(), externalRunID)
	if err != nil || !found || binding.State != state || binding.ProcessGeneration != generation || binding.PendingOperationID != "" {
		t.Fatalf("binding=%#v found=%v err=%v want state=%s generation=%d", binding, found, err, state, generation)
	}
}

func assertExecutionPendingBindingState(t *testing.T, store executionjournal.Store, externalRunID string, state domain.ExecutionRunBindingState, generation int64) {
	t.Helper()
	binding, found, err := store.GetExecutionRunBinding(context.Background(), externalRunID)
	if err != nil || !found || binding.State != state || binding.ProcessGeneration != generation || binding.PendingOperationID == "" {
		t.Fatalf("binding=%#v found=%v err=%v want pending state=%s generation=%d", binding, found, err, state, generation)
	}
}
