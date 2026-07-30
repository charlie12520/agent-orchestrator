-- +goose Up
-- +goose StatementBegin
CREATE TABLE integration_merge_leases (
    id TEXT PRIMARY KEY CHECK (length(id) BETWEEN 16 AND 64),
    repository TEXT NOT NULL CHECK (length(repository) BETWEEN 20 AND 256),
    source_repository TEXT NOT NULL CHECK (length(source_repository) BETWEEN 20 AND 256),
    pr_number INTEGER NOT NULL CHECK (pr_number BETWEEN 1 AND 1000000000),
    source_branch TEXT NOT NULL CHECK (length(source_branch) BETWEEN 1 AND 255),
    expected_head_sha TEXT NOT NULL CHECK (length(expected_head_sha) = 40),
    base_repository TEXT NOT NULL CHECK (length(base_repository) BETWEEN 20 AND 256),
    base_branch TEXT NOT NULL CHECK (length(base_branch) BETWEEN 1 AND 255),
    merge_strategy TEXT NOT NULL CHECK (merge_strategy IN ('squash', 'merge', 'rebase')),
    check_policy_json TEXT NOT NULL CHECK (length(check_policy_json) BETWEEN 2 AND 32768),
    review_policy_json TEXT NOT NULL CHECK (length(review_policy_json) BETWEEN 2 AND 32768),
    manual_approval_required BOOLEAN NOT NULL,
    capability_digest BLOB NOT NULL CHECK (length(capability_digest) = 32),
    status TEXT NOT NULL CHECK (status IN ('active', 'consumed', 'revoked', 'expired')),
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    consumed_at TIMESTAMP,
    revoked_at TIMESTAMP,
    CHECK (expires_at > created_at),
    CHECK ((status = 'consumed') = (consumed_at IS NOT NULL)),
    CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

-- The integration gate is globally serialized: there can be only one active
-- candidate lease. Expired rows are transitioned before issuance.
CREATE UNIQUE INDEX idx_integration_merge_single_active
    ON integration_merge_leases((1))
    WHERE status = 'active';

CREATE TABLE integration_merge_journal (
    idempotency_key TEXT PRIMARY KEY CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    lease_id TEXT NOT NULL REFERENCES integration_merge_leases(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('accepted', 'dispatched', 'result', 'ambiguous')),
    accepted_at TIMESTAMP NOT NULL,
    dispatched_at TIMESTAMP,
    completed_at TIMESTAMP,
    outcome_json TEXT CHECK (outcome_json IS NULL OR length(outcome_json) BETWEEN 2 AND 131072),
    CHECK (state NOT IN ('dispatched', 'ambiguous') OR dispatched_at IS NOT NULL),
    CHECK (state != 'accepted' OR dispatched_at IS NULL),
    CHECK ((state IN ('result', 'ambiguous')) = (completed_at IS NOT NULL)),
    CHECK ((state IN ('result', 'ambiguous')) = (outcome_json IS NOT NULL))
);

CREATE INDEX idx_integration_merge_journal_lease
    ON integration_merge_journal(lease_id, accepted_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_integration_merge_journal_lease;
DROP TABLE IF EXISTS integration_merge_journal;
DROP INDEX IF EXISTS idx_integration_merge_single_active;
DROP TABLE IF EXISTS integration_merge_leases;
-- +goose StatementEnd
