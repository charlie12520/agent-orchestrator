-- name: ExpireIntegrationMergeLeases :execrows
UPDATE integration_merge_leases
SET status = 'expired'
WHERE status = 'active' AND expires_at <= ?;

-- name: CountActiveIntegrationMergeLeases :one
SELECT count(*) FROM integration_merge_leases WHERE status = 'active';

-- name: InsertIntegrationMergeLease :exec
INSERT INTO integration_merge_leases (
    id, repository, source_repository, pr_number, source_branch,
    expected_head_sha, base_repository, base_branch, merge_strategy,
    check_policy_json, review_policy_json, manual_approval_required,
    capability_digest, status, created_at, expires_at, consumed_at, revoked_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetIntegrationMergeLease :one
SELECT id, repository, source_repository, pr_number, source_branch,
    expected_head_sha, base_repository, base_branch, merge_strategy,
    check_policy_json, review_policy_json, manual_approval_required,
    capability_digest, status, created_at, expires_at, consumed_at, revoked_at
FROM integration_merge_leases
WHERE id = ?;

-- name: RevokeIntegrationMergeLease :execrows
UPDATE integration_merge_leases
SET status = 'revoked', revoked_at = ?
WHERE id = ? AND status = 'active' AND expires_at > ?;

-- name: ConsumeIntegrationMergeLease :execrows
UPDATE integration_merge_leases
SET status = 'consumed', consumed_at = ?
WHERE id = ? AND status = 'active' AND expires_at > ?;

-- name: InsertIntegrationMergeJournal :exec
INSERT INTO integration_merge_journal (
    idempotency_key, request_hash, lease_id, state, accepted_at,
    dispatched_at, dispatch_owner, dispatch_fence, completed_at, outcome_json
) VALUES (?, ?, ?, 'accepted', ?, NULL, NULL, NULL, NULL, NULL);

-- name: GetIntegrationMergeJournal :one
SELECT idempotency_key, request_hash, lease_id, state, accepted_at,
    dispatched_at, dispatch_owner, dispatch_fence, completed_at, outcome_json
FROM integration_merge_journal
WHERE idempotency_key = ?;

-- name: MarkIntegrationMergeDispatched :execrows
UPDATE integration_merge_journal
SET state = 'dispatched', dispatched_at = ?, dispatch_owner = ?, dispatch_fence = ?
WHERE idempotency_key = ? AND state = 'accepted';

-- name: ConfirmIntegrationMergeDispatch :execrows
UPDATE integration_merge_journal
SET dispatch_fence = dispatch_fence
WHERE idempotency_key = ? AND state = 'dispatched'
    AND dispatch_owner = ? AND dispatch_fence = ?;

-- name: RecordIntegrationMergePreDispatchResult :execrows
UPDATE integration_merge_journal
SET state = 'result', completed_at = ?, outcome_json = ?
WHERE idempotency_key = ? AND state = 'accepted';

-- name: RecordIntegrationMergeFencedResult :execrows
UPDATE integration_merge_journal
SET state = 'result', completed_at = ?, outcome_json = ?
WHERE idempotency_key = ? AND state = 'dispatched'
    AND dispatch_owner = ? AND dispatch_fence = ?;

-- name: RecordIntegrationMergeReconciledResult :execrows
UPDATE integration_merge_journal
SET state = 'result', completed_at = ?, outcome_json = ?
WHERE idempotency_key = ? AND state = 'dispatched';

-- name: RecordIntegrationMergeRefinedResult :execrows
UPDATE integration_merge_journal
SET state = 'result', completed_at = ?, outcome_json = ?
WHERE idempotency_key = ? AND state = 'ambiguous';

-- name: RecordIntegrationMergeFencedAmbiguous :execrows
UPDATE integration_merge_journal
SET state = 'ambiguous', completed_at = ?, outcome_json = ?
WHERE idempotency_key = ? AND state = 'dispatched'
    AND dispatch_owner = ? AND dispatch_fence = ?;
