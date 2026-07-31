package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	executionjournal "github.com/aoagents/agent-orchestrator/backend/internal/service/executionjournal"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ executionjournal.Store = (*Store)(nil)

// AcceptExecutionOperation atomically creates the immutable journal row and
// reserves the run's single mutation slot. The first write is a SQLite writer
// barrier so independently-opened Store instances observe one serial order.
func (s *Store) AcceptExecutionOperation(ctx context.Context, journal domain.ExecutionOperationJournal) (executionjournal.Acceptance, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var accepted executionjournal.Acceptance
	err := s.inTx(ctx, "accept execution operation", func(q *gen.Queries) error {
		if _, err := q.ExecutionWriterBarrier(ctx, journal.ExternalRunID); err != nil {
			return err
		}
		if existing, found, err := getExecutionOperationByIdentity(ctx, q, journal.ExternalRunID, journal.IdempotencyKey); err != nil {
			return err
		} else if found {
			binding, bindingFound, err := getExecutionRunBinding(ctx, q, journal.ExternalRunID)
			if err != nil {
				return err
			}
			if !bindingFound {
				return fmt.Errorf("existing execution operation has no run binding")
			}
			accepted = executionjournal.Acceptance{Status: executionjournal.AcceptExisting, Journal: existing, Binding: binding, ActualProcessGeneration: binding.ProcessGeneration}
			return nil
		}

		binding, bindingFound, err := getExecutionRunBinding(ctx, q, journal.ExternalRunID)
		if err != nil {
			return err
		}
		if journal.Operation == domain.ExecutionLaunch {
			if bindingFound {
				accepted = executionjournal.Acceptance{Status: executionjournal.AcceptRunConflict, Binding: binding, ActualProcessGeneration: binding.ProcessGeneration}
				return nil
			}
			rows, err := q.InsertExecutionLaunchBinding(ctx, gen.InsertExecutionLaunchBindingParams{
				ExternalRunID: journal.ExternalRunID, LaunchOperationID: journal.OperationID,
				LaunchIdempotencyKey: journal.IdempotencyKey,
				LaunchRequestHash:    append([]byte(nil), journal.RequestHash...),
				PendingOperationID:   optionalString(journal.OperationID),
				CreatedAt:            journal.AcceptedAt, UpdatedAt: journal.AcceptedAt,
			})
			if err != nil {
				return err
			}
			if rows != 1 {
				return fmt.Errorf("launch binding insert lost after writer barrier")
			}
		} else {
			if !bindingFound {
				accepted = executionjournal.Acceptance{Status: executionjournal.AcceptRunNotFound}
				return nil
			}
			if binding.RunID != journal.RunID {
				accepted = executionjournal.Acceptance{Status: executionjournal.AcceptRunConflict, Binding: binding, ActualProcessGeneration: binding.ProcessGeneration}
				return nil
			}
			if binding.ProcessGeneration != journal.ExpectedProcessGeneration {
				accepted = executionjournal.Acceptance{Status: executionjournal.AcceptGenerationMismatch, Binding: binding, ActualProcessGeneration: binding.ProcessGeneration}
				return nil
			}
			if binding.PendingOperationID != "" || binding.State == domain.ExecutionBindingLaunching || binding.State == domain.ExecutionBindingBusy || binding.State == domain.ExecutionBindingAmbiguous {
				accepted = executionjournal.Acceptance{Status: executionjournal.AcceptRunBusy, Binding: binding, ActualProcessGeneration: binding.ProcessGeneration}
				return nil
			}
			requiredState, allowed := requiredExecutionBindingState(journal.Operation)
			if !allowed || binding.State != requiredState {
				accepted = executionjournal.Acceptance{Status: executionjournal.AcceptRunConflict, Binding: binding, ActualProcessGeneration: binding.ProcessGeneration}
				return nil
			}
			rows, err := q.AcquireExecutionMutationSlot(ctx, gen.AcquireExecutionMutationSlotParams{
				OperationID: optionalString(journal.OperationID), UpdatedAt: journal.AcceptedAt,
				ExternalRunID: journal.ExternalRunID, RunID: optionalString(journal.RunID),
				ExpectedProcessGeneration: journal.ExpectedProcessGeneration,
				ExpectedBindingState:      string(requiredState),
			})
			if err != nil {
				return err
			}
			if rows != 1 {
				return fmt.Errorf("mutation slot changed after writer barrier")
			}
		}

		if err := q.InsertExecutionOperation(ctx, gen.InsertExecutionOperationParams{
			OperationID: journal.OperationID, ExternalRunID: journal.ExternalRunID,
			RunID: optionalString(journal.RunID), Operation: string(journal.Operation),
			IdempotencyKey: journal.IdempotencyKey, RequestHash: append([]byte(nil), journal.RequestHash...),
			RequestJson:               string(journal.RequestJSON),
			ExpectedProcessGeneration: optionalInt64(journal.ExpectedProcessGeneration),
			TargetProcessGeneration:   journal.TargetProcessGeneration, AcceptedAt: journal.AcceptedAt,
		}); err != nil {
			return err
		}
		stored, err := executionOperationFromGenMust(ctx, q, journal.OperationID)
		if err != nil {
			return err
		}
		storedBinding, found, err := getExecutionRunBinding(ctx, q, journal.ExternalRunID)
		if err != nil || !found {
			if err == nil {
				err = fmt.Errorf("accepted execution operation has no binding")
			}
			return err
		}
		accepted = executionjournal.Acceptance{Status: executionjournal.AcceptCreated, Journal: stored, Binding: storedBinding, ActualProcessGeneration: storedBinding.ProcessGeneration}
		return nil
	})
	if err != nil {
		return executionjournal.Acceptance{}, err
	}
	return accepted, nil
}

func (s *Store) GetExecutionOperation(ctx context.Context, operationID string) (domain.ExecutionOperationJournal, bool, error) {
	row, err := s.qr.GetExecutionOperation(ctx, operationID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExecutionOperationJournal{}, false, nil
	}
	if err != nil {
		return domain.ExecutionOperationJournal{}, false, fmt.Errorf("get execution operation: %w", err)
	}
	journal, err := executionOperationFromGen(row)
	return journal, err == nil, err
}

func (s *Store) GetExecutionRunBinding(ctx context.Context, externalRunID string) (domain.ExecutionRunBinding, bool, error) {
	row, err := s.qr.GetExecutionRunBinding(ctx, externalRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExecutionRunBinding{}, false, nil
	}
	if err != nil {
		return domain.ExecutionRunBinding{}, false, fmt.Errorf("get execution run binding: %w", err)
	}
	binding, err := executionRunBindingFromGen(row)
	return binding, err == nil, err
}

func (s *Store) ClaimExecutionDispatch(ctx context.Context, operationID, owner, fence string, at time.Time) (executionjournal.Transition, error) {
	return s.transitionExecutionOperation(ctx, "claim execution dispatch", operationID, func(q *gen.Queries) (int64, error) {
		return q.MarkExecutionDispatched(ctx, gen.MarkExecutionDispatchedParams{
			DispatchedAt: optionalTime(at), DispatchOwner: optionalString(owner),
			DispatchFence: optionalString(fence), OperationID: operationID,
		})
	}, nil)
}

func (s *Store) ConfirmExecutionDispatch(ctx context.Context, operationID, owner, fence string) (executionjournal.Transition, error) {
	return s.transitionExecutionOperation(ctx, "confirm execution dispatch", operationID, func(q *gen.Queries) (int64, error) {
		return q.ConfirmExecutionDispatch(ctx, gen.ConfirmExecutionDispatchParams{
			OperationID: operationID, DispatchOwner: optionalString(owner), DispatchFence: optionalString(fence),
		})
	}, nil)
}

func (s *Store) RecordExecutionResult(ctx context.Context, completion executionjournal.Completion, dispatchOwner, dispatchFence string) (executionjournal.Transition, error) {
	return s.transitionExecutionOperation(ctx, "record execution result", completion.OperationID, func(q *gen.Queries) (int64, error) {
		return q.RecordExecutionFencedResult(ctx, gen.RecordExecutionFencedResultParams{
			CompletedAt: optionalTime(completion.CompletedAt), ResultRunID: optionalString(completion.RunID),
			ResultProcessGeneration: optionalInt64(completion.ProcessGeneration),
			ResultJson:              optionalString(string(completion.ResultJSON)), ResultHash: append([]byte(nil), completion.ResultHash...),
			OperationID: completion.OperationID, DispatchOwner: optionalString(dispatchOwner),
			DispatchFence: optionalString(dispatchFence),
		})
	}, completeExecutionBinding(completion))
}

func (s *Store) RecordExecutionAmbiguous(ctx context.Context, completion executionjournal.Completion, dispatchOwner, dispatchFence string) (executionjournal.Transition, error) {
	return s.transitionExecutionOperation(ctx, "record execution ambiguity", completion.OperationID, func(q *gen.Queries) (int64, error) {
		return q.RecordExecutionFencedAmbiguous(ctx, gen.RecordExecutionFencedAmbiguousParams{
			CompletedAt: optionalTime(completion.CompletedAt), ResultJson: optionalString(string(completion.ResultJSON)),
			ResultHash: append([]byte(nil), completion.ResultHash...), OperationID: completion.OperationID,
			DispatchOwner: optionalString(dispatchOwner), DispatchFence: optionalString(dispatchFence),
		})
	}, markExecutionBindingAmbiguous(completion))
}

func (s *Store) RecordReconciledExecutionResult(ctx context.Context, completion executionjournal.Completion) (executionjournal.Transition, error) {
	return s.transitionExecutionOperation(ctx, "record reconciled execution result", completion.OperationID, func(q *gen.Queries) (int64, error) {
		return q.RecordExecutionReconciledResult(ctx, gen.RecordExecutionReconciledResultParams{
			CompletedAt: optionalTime(completion.CompletedAt), ResultRunID: optionalString(completion.RunID),
			ResultProcessGeneration: optionalInt64(completion.ProcessGeneration),
			ResultJson:              optionalString(string(completion.ResultJSON)), ResultHash: append([]byte(nil), completion.ResultHash...),
			OperationID: completion.OperationID,
		})
	}, completeExecutionBinding(completion))
}

type executionBindingTransition func(context.Context, *gen.Queries) (int64, error)

func completeExecutionBinding(completion executionjournal.Completion) executionBindingTransition {
	return func(ctx context.Context, q *gen.Queries) (int64, error) {
		return q.CompleteExecutionRunBinding(ctx, gen.CompleteExecutionRunBindingParams{
			RunID: optionalString(completion.RunID), ProcessGeneration: completion.ProcessGeneration,
			UpdatedAt: completion.CompletedAt, ExternalRunID: completion.ExternalRunID,
			OperationID: completion.OperationID,
		})
	}
}

func requiredExecutionBindingState(operation domain.ExecutionOperation) (domain.ExecutionRunBindingState, bool) {
	switch operation {
	case domain.ExecutionSend, domain.ExecutionInterrupt, domain.ExecutionStop:
		return domain.ExecutionBindingActive, true
	case domain.ExecutionResume, domain.ExecutionRestore, domain.ExecutionCleanup:
		return domain.ExecutionBindingStopped, true
	default:
		return "", false
	}
}

func markExecutionBindingAmbiguous(completion executionjournal.Completion) executionBindingTransition {
	return func(ctx context.Context, q *gen.Queries) (int64, error) {
		return q.MarkExecutionRunBindingAmbiguous(ctx, gen.MarkExecutionRunBindingAmbiguousParams{
			UpdatedAt: completion.CompletedAt, ExternalRunID: completion.ExternalRunID,
			OperationID: optionalString(completion.OperationID),
		})
	}
}

func (s *Store) transitionExecutionOperation(ctx context.Context, what, operationID string, update func(*gen.Queries) (int64, error), bindingUpdate executionBindingTransition) (executionjournal.Transition, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var transition executionjournal.Transition
	err := s.inTx(ctx, what, func(q *gen.Queries) error {
		rows, err := update(q)
		if err != nil {
			return err
		}
		if rows == 1 && bindingUpdate != nil {
			bindingRows, err := bindingUpdate(ctx, q)
			if err != nil {
				return err
			}
			if bindingRows != 1 {
				return fmt.Errorf("journal transition did not match its run binding")
			}
		}
		journal, err := executionOperationFromGenMust(ctx, q, operationID)
		if err != nil {
			return err
		}
		binding, found, err := getExecutionRunBinding(ctx, q, journal.ExternalRunID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("execution operation has no run binding")
		}
		transition = executionjournal.Transition{Journal: journal, Binding: binding, Changed: rows == 1}
		return nil
	})
	if err != nil {
		return executionjournal.Transition{}, err
	}
	return transition, nil
}

func getExecutionOperationByIdentity(ctx context.Context, q *gen.Queries, externalRunID, idempotencyKey string) (domain.ExecutionOperationJournal, bool, error) {
	row, err := q.GetExecutionOperationByIdentity(ctx, gen.GetExecutionOperationByIdentityParams{ExternalRunID: externalRunID, IdempotencyKey: idempotencyKey})
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExecutionOperationJournal{}, false, nil
	}
	if err != nil {
		return domain.ExecutionOperationJournal{}, false, err
	}
	journal, err := executionOperationFromGen(row)
	return journal, err == nil, err
}

func executionOperationFromGenMust(ctx context.Context, q *gen.Queries, operationID string) (domain.ExecutionOperationJournal, error) {
	row, err := q.GetExecutionOperation(ctx, operationID)
	if err != nil {
		return domain.ExecutionOperationJournal{}, err
	}
	return executionOperationFromGen(row)
}

func getExecutionRunBinding(ctx context.Context, q *gen.Queries, externalRunID string) (domain.ExecutionRunBinding, bool, error) {
	row, err := q.GetExecutionRunBinding(ctx, externalRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExecutionRunBinding{}, false, nil
	}
	if err != nil {
		return domain.ExecutionRunBinding{}, false, err
	}
	binding, err := executionRunBindingFromGen(row)
	return binding, err == nil, err
}

func executionRunBindingFromGen(row gen.ExecutionRunBinding) (domain.ExecutionRunBinding, error) {
	if len(row.LaunchRequestHash) != sha256.Size {
		return domain.ExecutionRunBinding{}, fmt.Errorf("execution binding has invalid launch request hash")
	}
	return domain.ExecutionRunBinding{
		ExternalRunID: row.ExternalRunID, RunID: stringFromNull(row.RunID),
		State: domain.ExecutionRunBindingState(row.State), ProcessGeneration: row.ProcessGeneration,
		LaunchOperationID: row.LaunchOperationID, LaunchIdempotencyKey: row.LaunchIdempotencyKey,
		LaunchRequestHash:  append([]byte(nil), row.LaunchRequestHash...),
		PendingOperationID: stringFromNull(row.PendingOperationID), CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}, nil
}

func executionOperationFromGen(row gen.ExecutionOperationJournal) (domain.ExecutionOperationJournal, error) {
	if len(row.RequestHash) != sha256.Size {
		return domain.ExecutionOperationJournal{}, fmt.Errorf("execution journal has invalid request hash")
	}
	requestJSON := []byte(row.RequestJson)
	requestHash := sha256.Sum256(requestJSON)
	if !bytes.Equal(requestHash[:], row.RequestHash) {
		return domain.ExecutionOperationJournal{}, fmt.Errorf("execution journal request hash mismatch")
	}
	journal := domain.ExecutionOperationJournal{
		OperationID: row.OperationID, ExternalRunID: row.ExternalRunID, RunID: stringFromNull(row.RunID),
		Operation: domain.ExecutionOperation(row.Operation), IdempotencyKey: row.IdempotencyKey,
		RequestHash: append([]byte(nil), row.RequestHash...), RequestJSON: append([]byte(nil), requestJSON...),
		ExpectedProcessGeneration: int64FromNull(row.ExpectedProcessGeneration),
		TargetProcessGeneration:   row.TargetProcessGeneration,
		State:                     domain.ExecutionJournalState(row.State), AcceptedAt: row.AcceptedAt,
		DispatchedAt: timeFromNull(row.DispatchedAt), DispatchOwner: stringFromNull(row.DispatchOwner),
		CompletedAt:             timeFromNull(row.CompletedAt),
		ResultRunID:             stringFromNull(row.ResultRunID),
		ResultProcessGeneration: int64FromNull(row.ResultProcessGeneration),
		ResultJSON:              []byte(stringFromNull(row.ResultJson)), ResultHash: append([]byte(nil), row.ResultHash...),
	}
	if journal.State == domain.ExecutionResult || journal.State == domain.ExecutionAmbiguous {
		if len(journal.ResultHash) != sha256.Size {
			return domain.ExecutionOperationJournal{}, fmt.Errorf("terminal execution journal has invalid result hash")
		}
		resultHash := sha256.Sum256(journal.ResultJSON)
		if !bytes.Equal(resultHash[:], journal.ResultHash) {
			return domain.ExecutionOperationJournal{}, fmt.Errorf("execution journal result hash mismatch")
		}
	}
	return journal, nil
}

func optionalInt64(value int64) sql.NullInt64 {
	return sql.NullInt64{Int64: value, Valid: value != 0}
}

func int64FromNull(value sql.NullInt64) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}
