// Package executionapi exposes the narrow execution-journal seam used by the
// managed HTTP API. It deliberately keeps durable reads available while the
// mutating dispatcher remains unwired in production.
package executionapi

import (
	"context"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	executionjournal "github.com/aoagents/agent-orchestrator/backend/internal/service/executionjournal"
)

// ErrUnavailable means execution mutation has no accepted production
// dispatcher. Callers must fail before touching the durable journal.
var ErrUnavailable = errors.New("execution mutation unavailable")

// Reader is the read-only part of the durable execution journal.
type Reader interface {
	GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error)
	GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error)
}

// Executor is the already-hardened A1 mutation service. A real production
// adapter is intentionally not supplied by this foundation.
type Executor interface {
	Execute(context.Context, []byte) (executionjournal.Result, error)
}

// Service combines sanitized journal reads with an optional mutation seam.
type Service struct {
	reader   Reader
	executor Executor
}

func New(reader Reader, executor Executor) *Service {
	return &Service{reader: reader, executor: executor}
}

// Execute fails closed before any store access when no accepted dispatcher is
// wired. Tests may inject the A1 service with a deterministic fake dispatcher.
func (s *Service) Execute(ctx context.Context, raw []byte) (executionjournal.Result, error) {
	if s == nil || s.executor == nil {
		return executionjournal.Result{}, ErrUnavailable
	}
	return s.executor.Execute(ctx, raw)
}

func (s *Service) GetExecutionOperation(ctx context.Context, operationID string) (domain.ExecutionOperationJournal, bool, error) {
	if s == nil || s.reader == nil {
		return domain.ExecutionOperationJournal{}, false, ErrUnavailable
	}
	return s.reader.GetExecutionOperation(ctx, operationID)
}

func (s *Service) GetExecutionRunBinding(ctx context.Context, externalRunID string) (domain.ExecutionRunBinding, bool, error) {
	if s == nil || s.reader == nil {
		return domain.ExecutionRunBinding{}, false, ErrUnavailable
	}
	return s.reader.GetExecutionRunBinding(ctx, externalRunID)
}
