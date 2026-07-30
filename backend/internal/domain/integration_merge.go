package domain

import "time"

// MergeStrategy is the immutable repository merge method authorized by a
// SuperOrch integration lease.
type MergeStrategy string

const (
	MergeStrategySquash MergeStrategy = "squash"
	MergeStrategyMerge  MergeStrategy = "merge"
	MergeStrategyRebase MergeStrategy = "rebase"
)

// IntegrationMergeLeaseStatus is the durable lifecycle of a one-use lease.
type IntegrationMergeLeaseStatus string

const (
	IntegrationMergeLeaseActive   IntegrationMergeLeaseStatus = "active"
	IntegrationMergeLeaseConsumed IntegrationMergeLeaseStatus = "consumed"
	IntegrationMergeLeaseRevoked  IntegrationMergeLeaseStatus = "revoked"
	IntegrationMergeLeaseExpired  IntegrationMergeLeaseStatus = "expired"
)

// IntegrationMergeJournalState records the write-ahead state surrounding the
// external merge side effect. A dispatched operation is never dispatched a
// second time; it must be reconciled from authoritative broker facts.
type IntegrationMergeJournalState string

const (
	IntegrationMergeAccepted   IntegrationMergeJournalState = "accepted"
	IntegrationMergeDispatched IntegrationMergeJournalState = "dispatched"
	IntegrationMergeResult     IntegrationMergeJournalState = "result"
	IntegrationMergeAmbiguous  IntegrationMergeJournalState = "ambiguous"
)

// IntegrationMergeOutcomeStatus is the bounded terminal outcome returned to a
// caller and persisted in the idempotency journal.
type IntegrationMergeOutcomeStatus string

const (
	IntegrationMergeOutcomeMerged               IntegrationMergeOutcomeStatus = "merged"
	IntegrationMergeOutcomeRevalidationRequired IntegrationMergeOutcomeStatus = "revalidation_required"
	IntegrationMergeOutcomeAmbiguous            IntegrationMergeOutcomeStatus = "ambiguous"
)

// IntegrationCheckPolicy is the immutable repository policy snapshot carried
// by a lease. Revision is a broker-defined stable policy identity; required
// checks are also materialized so AO can evaluate exact authoritative facts.
type IntegrationCheckPolicy struct {
	Revision                  string   `json:"revision" maxLength:"128"`
	RequiredChecks            []string `json:"requiredChecks"`
	RequireAllObservedPassing bool     `json:"requireAllObservedPassing"`
}

// IntegrationReviewPolicy is the immutable review-policy snapshot carried by
// a lease and compared with the broker's fresh repository facts.
type IntegrationReviewPolicy struct {
	Revision               string   `json:"revision" maxLength:"128"`
	RequiredApprovals      int      `json:"requiredApprovals" minimum:"0" maximum:"100"`
	RequiredReviewers      []string `json:"requiredReviewers"`
	RequireResolvedThreads bool     `json:"requireResolvedThreads"`
}

// IntegrationCheckEvidence is one exact-head check run returned by the merge
// broker. Status is one of passing, pending, or failing.
type IntegrationCheckEvidence struct {
	Name        string    `json:"name" maxLength:"128"`
	HeadSHA     string    `json:"headSha" minLength:"40" maxLength:"40"`
	Status      string    `json:"status" enum:"passing,pending,failing"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
}

// IntegrationReviewEvidence is one authoritative review decision. AO only
// counts approved evidence bound to the lease's exact expected head.
type IntegrationReviewEvidence struct {
	Reviewer    string    `json:"reviewer" maxLength:"128"`
	HeadSHA     string    `json:"headSha" minLength:"40" maxLength:"40"`
	Decision    string    `json:"decision" enum:"approved,changes_requested,commented,dismissed"`
	SubmittedAt time.Time `json:"submittedAt"`
}

// IntegrationMergeCandidate is the broker's authoritative, freshly-read pull
// request view. Request claims never populate this structure.
type IntegrationMergeCandidate struct {
	Repository              string                      `json:"repository"`
	SourceRepository        string                      `json:"sourceRepository"`
	PRNumber                int                         `json:"prNumber"`
	SourceBranch            string                      `json:"sourceBranch"`
	HeadSHA                 string                      `json:"headSha"`
	BaseRepository          string                      `json:"baseRepository"`
	BaseBranch              string                      `json:"baseBranch"`
	Mergeability            string                      `json:"mergeability" enum:"clean,conflicting,behind,blocked,unknown"`
	Ambiguous               bool                        `json:"ambiguous"`
	ConfiguredStrategy      MergeStrategy               `json:"configuredStrategy"`
	CheckPolicy             IntegrationCheckPolicy      `json:"checkPolicy"`
	Checks                  []IntegrationCheckEvidence  `json:"checks"`
	ReviewPolicy            IntegrationReviewPolicy     `json:"reviewPolicy"`
	Reviews                 []IntegrationReviewEvidence `json:"reviews"`
	UnresolvedReviewThreads int                         `json:"unresolvedReviewThreads"`
	Merged                  bool                        `json:"merged"`
	MergedExpectedHeadSHA   string                      `json:"mergedExpectedHeadSha,omitempty"`
	MergeCommitSHA          string                      `json:"mergeCommitSha,omitempty"`
	MergeStrategyEvidence   MergeStrategy               `json:"mergeStrategyEvidence,omitempty"`
	MergedAt                time.Time                   `json:"mergedAt,omitempty"`
}

// IntegrationMergeLease is the durable immutable authorization record. Only
// CapabilityDigest is stored; the random plaintext capability is returned once
// by the issuance surface and must never be persisted or logged.
type IntegrationMergeLease struct {
	ID                     string
	Repository             string
	SourceRepository       string
	PRNumber               int
	SourceBranch           string
	ExpectedHeadSHA        string
	BaseRepository         string
	BaseBranch             string
	Strategy               MergeStrategy
	CheckPolicy            IntegrationCheckPolicy
	ReviewPolicy           IntegrationReviewPolicy
	ManualApprovalRequired bool
	CapabilityDigest       []byte
	Status                 IntegrationMergeLeaseStatus
	CreatedAt              time.Time
	ExpiresAt              time.Time
	ConsumedAt             time.Time
	RevokedAt              time.Time
}

// IntegrationMergeOutcome is the complete durable evidence for a terminal
// merge operation. It intentionally contains no capability material.
type IntegrationMergeOutcome struct {
	Version                 int                           `json:"version"`
	Status                  IntegrationMergeOutcomeStatus `json:"status" enum:"merged,revalidation_required,ambiguous"`
	Reason                  string                        `json:"reason,omitempty" maxLength:"128"`
	IdempotencyKey          string                        `json:"idempotencyKey"`
	Repository              string                        `json:"repository"`
	SourceRepository        string                        `json:"sourceRepository"`
	PRNumber                int                           `json:"prNumber"`
	SourceBranch            string                        `json:"sourceBranch"`
	ExpectedHeadSHA         string                        `json:"expectedHeadSha"`
	BaseRepository          string                        `json:"baseRepository"`
	BaseBranch              string                        `json:"baseBranch"`
	Strategy                MergeStrategy                 `json:"mergeStrategy"`
	CheckPolicy             IntegrationCheckPolicy        `json:"checkPolicy"`
	Checks                  []IntegrationCheckEvidence    `json:"checks"`
	ReviewPolicy            IntegrationReviewPolicy       `json:"reviewPolicy"`
	Reviews                 []IntegrationReviewEvidence   `json:"reviews"`
	UnresolvedReviewThreads int                           `json:"unresolvedReviewThreads"`
	MergeCommitSHA          string                        `json:"mergeCommitSha,omitempty"`
	AcceptedAt              time.Time                     `json:"acceptedAt"`
	DispatchedAt            time.Time                     `json:"dispatchedAt,omitempty"`
	CompletedAt             time.Time                     `json:"completedAt"`
}

// IntegrationMergeJournal is the durable idempotency record.
type IntegrationMergeJournal struct {
	IdempotencyKey string
	RequestHash    []byte
	LeaseID        string
	State          IntegrationMergeJournalState
	AcceptedAt     time.Time
	DispatchedAt   time.Time
	DispatchOwner  string
	DispatchFence  string
	CompletedAt    time.Time
	Outcome        *IntegrationMergeOutcome
}

// IntegrationBrokerMergeResult is the SHA- and strategy-bound receipt from a
// successful broker merge call.
type IntegrationBrokerMergeResult struct {
	Repository      string
	PRNumber        int
	ExpectedHeadSHA string
	Strategy        MergeStrategy
	MergeCommitSHA  string
	MergedAt        time.Time
}
