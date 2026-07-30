package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	integrationmerge "github.com/aoagents/agent-orchestrator/backend/internal/service/integrationmerge"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

var _ integrationmerge.Store = (*Store)(nil)

// CreateIntegrationMergeLease expires stale active rows and inserts a lease in
// one transaction. The partial unique index is a database-level backstop for
// the explicit active-row check.
func (s *Store) CreateIntegrationMergeLease(ctx context.Context, lease domain.IntegrationMergeLease, now time.Time) (bool, error) {
	checkPolicy, reviewPolicy, err := marshalIntegrationPolicies(lease)
	if err != nil {
		return false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	created := false
	err = s.inTx(ctx, "create integration merge lease", func(q *gen.Queries) error {
		if _, err := q.ExpireIntegrationMergeLeases(ctx, now); err != nil {
			return err
		}
		count, err := q.CountActiveIntegrationMergeLeases(ctx)
		if err != nil {
			return err
		}
		if count != 0 {
			return nil
		}
		if err := q.InsertIntegrationMergeLease(ctx, gen.InsertIntegrationMergeLeaseParams{
			ID: lease.ID, Repository: lease.Repository, SourceRepository: lease.SourceRepository,
			PRNumber: int64(lease.PRNumber), SourceBranch: lease.SourceBranch,
			ExpectedHeadSha: lease.ExpectedHeadSHA, BaseRepository: lease.BaseRepository,
			BaseBranch: lease.BaseBranch, MergeStrategy: string(lease.Strategy),
			CheckPolicyJson: checkPolicy, ReviewPolicyJson: reviewPolicy,
			ManualApprovalRequired: lease.ManualApprovalRequired,
			CapabilityDigest:       append([]byte(nil), lease.CapabilityDigest...), Status: string(lease.Status),
			CreatedAt: lease.CreatedAt, ExpiresAt: lease.ExpiresAt,
			ConsumedAt: optionalTime(lease.ConsumedAt), RevokedAt: optionalTime(lease.RevokedAt),
		}); err != nil {
			return err
		}
		created = true
		return nil
	})
	return created, err
}

func (s *Store) GetIntegrationMergeLease(ctx context.Context, id string) (domain.IntegrationMergeLease, bool, error) {
	row, err := s.qr.GetIntegrationMergeLease(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.IntegrationMergeLease{}, false, nil
	}
	if err != nil {
		return domain.IntegrationMergeLease{}, false, fmt.Errorf("get integration merge lease: %w", err)
	}
	lease, err := integrationMergeLeaseFromGen(row)
	return lease, err == nil, err
}

// RevokeIntegrationMergeLease is an exact-once active->revoked transition. It
// returns the current durable row when no transition occurred.
func (s *Store) RevokeIntegrationMergeLease(ctx context.Context, id string, now time.Time) (domain.IntegrationMergeLease, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var (
		lease   domain.IntegrationMergeLease
		exists  bool
		revoked bool
	)
	err := s.inTx(ctx, "revoke integration merge lease", func(q *gen.Queries) error {
		if _, err := q.ExpireIntegrationMergeLeases(ctx, now); err != nil {
			return err
		}
		rows, err := q.RevokeIntegrationMergeLease(ctx, gen.RevokeIntegrationMergeLeaseParams{
			RevokedAt: optionalTime(now), ID: id, ExpiresAt: now,
		})
		if err != nil {
			return err
		}
		row, err := q.GetIntegrationMergeLease(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		lease, err = integrationMergeLeaseFromGen(row)
		if err != nil {
			return err
		}
		exists = true
		revoked = rows == 1
		return nil
	})
	if err != nil {
		return domain.IntegrationMergeLease{}, false, err
	}
	if !exists {
		return domain.IntegrationMergeLease{}, false, nil
	}
	return lease, revoked, nil
}

func (s *Store) GetIntegrationMergeJournal(ctx context.Context, key string) (domain.IntegrationMergeJournal, bool, error) {
	row, err := s.qr.GetIntegrationMergeJournal(ctx, key)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.IntegrationMergeJournal{}, false, nil
	}
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, fmt.Errorf("get integration merge journal: %w", err)
	}
	journal, err := integrationMergeJournalFromGen(row)
	return journal, err == nil, err
}

// AcceptIntegrationMerge atomically consumes the active lease and creates the
// accepted write-ahead journal record. Neither mutation can commit alone.
func (s *Store) AcceptIntegrationMerge(ctx context.Context, journal domain.IntegrationMergeJournal, now time.Time) (bool, domain.IntegrationMergeLeaseStatus, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	accepted := false
	status := domain.IntegrationMergeLeaseStatus("")
	err := s.inTx(ctx, "accept integration merge", func(q *gen.Queries) error {
		if _, err := q.ExpireIntegrationMergeLeases(ctx, now); err != nil {
			return err
		}
		rows, err := q.ConsumeIntegrationMergeLease(ctx, gen.ConsumeIntegrationMergeLeaseParams{
			ConsumedAt: optionalTime(now), ID: journal.LeaseID, ExpiresAt: now,
		})
		if err != nil {
			return err
		}
		if rows != 1 {
			row, getErr := q.GetIntegrationMergeLease(ctx, journal.LeaseID)
			if errors.Is(getErr, sql.ErrNoRows) {
				return nil
			}
			if getErr != nil {
				return getErr
			}
			status = domain.IntegrationMergeLeaseStatus(row.Status)
			return nil
		}
		if err := q.InsertIntegrationMergeJournal(ctx, gen.InsertIntegrationMergeJournalParams{
			IdempotencyKey: journal.IdempotencyKey,
			RequestHash:    append([]byte(nil), journal.RequestHash...),
			LeaseID:        journal.LeaseID,
			AcceptedAt:     journal.AcceptedAt,
		}); err != nil {
			return err
		}
		accepted = true
		status = domain.IntegrationMergeLeaseConsumed
		return nil
	})
	return accepted, status, err
}

func (s *Store) MarkIntegrationMergeDispatched(ctx context.Context, key, owner, fence string, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "mark integration merge dispatched", key, func(q *gen.Queries) (int64, error) {
		return q.MarkIntegrationMergeDispatched(ctx, gen.MarkIntegrationMergeDispatchedParams{
			DispatchedAt: optionalTime(at), DispatchOwner: optionalString(owner),
			DispatchFence: optionalString(fence), IdempotencyKey: key,
		})
	})
}

// ConfirmIntegrationMergeDispatch atomically proves that the caller still
// owns the exact durable dispatch fence immediately before the broker call.
func (s *Store) ConfirmIntegrationMergeDispatch(ctx context.Context, key, owner, fence string) (domain.IntegrationMergeJournal, bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "confirm integration merge dispatch", key, func(q *gen.Queries) (int64, error) {
		return q.ConfirmIntegrationMergeDispatch(ctx, gen.ConfirmIntegrationMergeDispatchParams{
			IdempotencyKey: key, DispatchOwner: optionalString(owner), DispatchFence: optionalString(fence),
		})
	})
}

func (s *Store) RecordIntegrationMergePreDispatchResult(ctx context.Context, key string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	encoded, err := marshalIntegrationMergeOutcome(outcome)
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "record integration merge pre-dispatch result", key, func(q *gen.Queries) (int64, error) {
		return q.RecordIntegrationMergePreDispatchResult(ctx, gen.RecordIntegrationMergePreDispatchResultParams{
			CompletedAt: optionalTime(at), OutcomeJson: encoded,
			IdempotencyKey: key,
		})
	})
}

// RecordIntegrationMergeFencedResult terminalizes only the exact recorded
// dispatch owner/fence. It is used both by the owner and by a reconciler that
// is fencing an authoritatively invalid candidate.
func (s *Store) RecordIntegrationMergeFencedResult(ctx context.Context, key, owner, fence string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	encoded, err := marshalIntegrationMergeOutcome(outcome)
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "record integration merge fenced result", key, func(q *gen.Queries) (int64, error) {
		return q.RecordIntegrationMergeFencedResult(ctx, gen.RecordIntegrationMergeFencedResultParams{
			CompletedAt: optionalTime(at), OutcomeJson: encoded, IdempotencyKey: key,
			DispatchOwner: optionalString(owner), DispatchFence: optionalString(fence),
		})
	})
}

// RecordIntegrationMergeReconciledResult permits a non-owner to persist only
// the exact success proven by fresh authoritative broker facts.
func (s *Store) RecordIntegrationMergeReconciledResult(ctx context.Context, key string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	encoded, err := marshalIntegrationMergeOutcome(outcome)
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "record integration merge reconciled result", key, func(q *gen.Queries) (int64, error) {
		return q.RecordIntegrationMergeReconciledResult(ctx, gen.RecordIntegrationMergeReconciledResultParams{
			CompletedAt: optionalTime(at), OutcomeJson: encoded, IdempotencyKey: key,
		})
	})
}

// RecordIntegrationMergeRefinedResult upgrades a durable ambiguity only after
// the service has obtained fresh authoritative proof of the exact operation.
// It cannot reopen dispatch or cause another broker call.
func (s *Store) RecordIntegrationMergeRefinedResult(ctx context.Context, key string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	encoded, err := marshalIntegrationMergeOutcome(outcome)
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "record integration merge refined result", key, func(q *gen.Queries) (int64, error) {
		return q.RecordIntegrationMergeRefinedResult(ctx, gen.RecordIntegrationMergeRefinedResultParams{
			CompletedAt: optionalTime(at), OutcomeJson: encoded, IdempotencyKey: key,
		})
	})
}

func (s *Store) RecordIntegrationMergeFencedAmbiguous(ctx context.Context, key, owner, fence string, outcome domain.IntegrationMergeOutcome, at time.Time) (domain.IntegrationMergeJournal, bool, error) {
	encoded, err := marshalIntegrationMergeOutcome(outcome)
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.transitionIntegrationMergeJournal(ctx, "record integration merge fenced ambiguity", key, func(q *gen.Queries) (int64, error) {
		return q.RecordIntegrationMergeFencedAmbiguous(ctx, gen.RecordIntegrationMergeFencedAmbiguousParams{
			CompletedAt: optionalTime(at), OutcomeJson: encoded, IdempotencyKey: key,
			DispatchOwner: optionalString(owner), DispatchFence: optionalString(fence),
		})
	})
}

func (s *Store) transitionIntegrationMergeJournal(ctx context.Context, what, key string, update func(*gen.Queries) (int64, error)) (domain.IntegrationMergeJournal, bool, error) {
	var (
		journal domain.IntegrationMergeJournal
		changed bool
	)
	err := s.inTx(ctx, what, func(q *gen.Queries) error {
		rows, err := update(q)
		if err != nil {
			return err
		}
		row, err := q.GetIntegrationMergeJournal(ctx, key)
		if err != nil {
			return err
		}
		journal, err = integrationMergeJournalFromGen(row)
		if err != nil {
			return err
		}
		changed = rows == 1
		return nil
	})
	if err != nil {
		return domain.IntegrationMergeJournal{}, false, err
	}
	return journal, changed, nil
}

func marshalIntegrationPolicies(lease domain.IntegrationMergeLease) (string, string, error) {
	checks, err := json.Marshal(lease.CheckPolicy)
	if err != nil {
		return "", "", fmt.Errorf("encode integration check policy: %w", err)
	}
	reviews, err := json.Marshal(lease.ReviewPolicy)
	if err != nil {
		return "", "", fmt.Errorf("encode integration review policy: %w", err)
	}
	return string(checks), string(reviews), nil
}

func marshalIntegrationMergeOutcome(outcome domain.IntegrationMergeOutcome) (sql.NullString, error) {
	encoded, err := json.Marshal(outcome)
	if err != nil {
		return sql.NullString{}, fmt.Errorf("encode integration merge outcome: %w", err)
	}
	return sql.NullString{String: string(encoded), Valid: true}, nil
}

func integrationMergeLeaseFromGen(row gen.IntegrationMergeLease) (domain.IntegrationMergeLease, error) {
	var checks domain.IntegrationCheckPolicy
	if err := json.Unmarshal([]byte(row.CheckPolicyJson), &checks); err != nil {
		return domain.IntegrationMergeLease{}, fmt.Errorf("decode integration check policy: %w", err)
	}
	var reviews domain.IntegrationReviewPolicy
	if err := json.Unmarshal([]byte(row.ReviewPolicyJson), &reviews); err != nil {
		return domain.IntegrationMergeLease{}, fmt.Errorf("decode integration review policy: %w", err)
	}
	return domain.IntegrationMergeLease{
		ID: row.ID, Repository: row.Repository, SourceRepository: row.SourceRepository,
		PRNumber: int(row.PRNumber), SourceBranch: row.SourceBranch,
		ExpectedHeadSHA: row.ExpectedHeadSha, BaseRepository: row.BaseRepository,
		BaseBranch: row.BaseBranch, Strategy: domain.MergeStrategy(row.MergeStrategy),
		CheckPolicy: checks, ReviewPolicy: reviews,
		ManualApprovalRequired: row.ManualApprovalRequired,
		CapabilityDigest:       append([]byte(nil), row.CapabilityDigest...),
		Status:                 domain.IntegrationMergeLeaseStatus(row.Status), CreatedAt: row.CreatedAt,
		ExpiresAt: row.ExpiresAt, ConsumedAt: timeFromNull(row.ConsumedAt),
		RevokedAt: timeFromNull(row.RevokedAt),
	}, nil
}

func integrationMergeJournalFromGen(row gen.IntegrationMergeJournal) (domain.IntegrationMergeJournal, error) {
	journal := domain.IntegrationMergeJournal{
		IdempotencyKey: row.IdempotencyKey, RequestHash: append([]byte(nil), row.RequestHash...),
		LeaseID: row.LeaseID, State: domain.IntegrationMergeJournalState(row.State),
		AcceptedAt: row.AcceptedAt, DispatchedAt: timeFromNull(row.DispatchedAt),
		DispatchOwner: stringFromNull(row.DispatchOwner), DispatchFence: stringFromNull(row.DispatchFence),
		CompletedAt: timeFromNull(row.CompletedAt),
	}
	if row.OutcomeJson.Valid {
		var outcome domain.IntegrationMergeOutcome
		if err := json.Unmarshal([]byte(row.OutcomeJson.String), &outcome); err != nil {
			return domain.IntegrationMergeJournal{}, fmt.Errorf("decode integration merge outcome: %w", err)
		}
		journal.Outcome = &outcome
	}
	return journal, nil
}

func optionalTime(value time.Time) sql.NullTime {
	return sql.NullTime{Time: value, Valid: !value.IsZero()}
}

func optionalString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func stringFromNull(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
