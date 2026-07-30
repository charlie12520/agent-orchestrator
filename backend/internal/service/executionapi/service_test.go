package executionapi

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type countingReader struct {
	operationReads int
	bindingReads   int
}

func (r *countingReader) GetExecutionOperation(context.Context, string) (domain.ExecutionOperationJournal, bool, error) {
	r.operationReads++
	return domain.ExecutionOperationJournal{}, false, nil
}

func (r *countingReader) GetExecutionRunBinding(context.Context, string) (domain.ExecutionRunBinding, bool, error) {
	r.bindingReads++
	return domain.ExecutionRunBinding{}, false, nil
}

func TestUnavailableExecuteFailsBeforeAnyStoreAccess(t *testing.T) {
	reader := &countingReader{}
	service := New(reader, nil)
	_, err := service.Execute(context.Background(), []byte(`{"version":1}`))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Execute error = %v, want ErrUnavailable", err)
	}
	if reader.operationReads != 0 || reader.bindingReads != 0 {
		t.Fatalf("reader calls = operation %d binding %d, want zero", reader.operationReads, reader.bindingReads)
	}
}

func TestNilReaderFailsClosed(t *testing.T) {
	service := New(nil, nil)
	if _, _, err := service.GetExecutionOperation(context.Background(), "id"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("GetExecutionOperation error = %v, want ErrUnavailable", err)
	}
	if _, _, err := service.GetExecutionRunBinding(context.Background(), "id"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("GetExecutionRunBinding error = %v, want ErrUnavailable", err)
	}
}
