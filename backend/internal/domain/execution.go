package domain

import "time"

// ExecutionOperation is one mutating operation covered by the durable
// execution journal. Read-only get/subscribe operations deliberately do not
// enter the mutation journal.
type ExecutionOperation string

const (
	ExecutionLaunch    ExecutionOperation = "launch"
	ExecutionSend      ExecutionOperation = "send"
	ExecutionInterrupt ExecutionOperation = "interrupt"
	ExecutionResume    ExecutionOperation = "resume"
	ExecutionRestore   ExecutionOperation = "restore"
	ExecutionStop      ExecutionOperation = "stop"
	ExecutionCleanup   ExecutionOperation = "cleanup"
)

// ExecutionJournalState is the write-ahead lifecycle surrounding one external
// side effect. A dispatched operation is never made dispatchable again.
type ExecutionJournalState string

const (
	ExecutionAccepted   ExecutionJournalState = "accepted"
	ExecutionDispatched ExecutionJournalState = "dispatched"
	ExecutionResult     ExecutionJournalState = "result"
	ExecutionAmbiguous  ExecutionJournalState = "ambiguous"
)

// ExecutionRunBindingState is the durable ownership state for an external run.
// Busy and ambiguous bindings retain PendingOperationID so another mutation
// cannot overtake an unresolved side effect.
type ExecutionRunBindingState string

const (
	ExecutionBindingLaunching ExecutionRunBindingState = "launching"
	ExecutionBindingActive    ExecutionRunBindingState = "active"
	ExecutionBindingStopped   ExecutionRunBindingState = "stopped"
	ExecutionBindingCleaned   ExecutionRunBindingState = "cleaned"
	ExecutionBindingBusy      ExecutionRunBindingState = "busy"
	ExecutionBindingAmbiguous ExecutionRunBindingState = "ambiguous"
)

// ExecutionRunBinding binds a caller-owned external run id to AO's durable run
// id and current backend-assigned process generation.
type ExecutionRunBinding struct {
	ExternalRunID        string
	RunID                string
	State                ExecutionRunBindingState
	ProcessGeneration    int64
	LaunchOperationID    string
	LaunchIdempotencyKey string
	LaunchRequestHash    []byte
	PendingOperationID   string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// ExecutionOperationJournal is the immutable request identity and durable
// result for one execution mutation. RequestJSON and ResultJSON are strict,
// canonical JSON bytes; hashes protect their exact durable identity.
type ExecutionOperationJournal struct {
	OperationID               string
	ExternalRunID             string
	RunID                     string
	Operation                 ExecutionOperation
	IdempotencyKey            string
	RequestHash               []byte
	RequestJSON               []byte
	ExpectedProcessGeneration int64
	TargetProcessGeneration   int64
	State                     ExecutionJournalState
	AcceptedAt                time.Time
	DispatchedAt              time.Time
	DispatchOwner             string
	CompletedAt               time.Time
	ResultRunID               string
	ResultProcessGeneration   int64
	ResultJSON                []byte
	ResultHash                []byte
}
