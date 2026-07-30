-- +goose Up
-- +goose StatementBegin
CREATE TABLE execution_run_bindings (
    external_run_id TEXT PRIMARY KEY
        CHECK (length(external_run_id) BETWEEN 1 AND 128)
        CHECK (substr(external_run_id, 1, 1) GLOB '[A-Za-z0-9]')
        CHECK (external_run_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    run_id TEXT UNIQUE
        CHECK (run_id IS NULL OR (
            length(run_id) BETWEEN 1 AND 128
            AND substr(run_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND run_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        )),
    state TEXT NOT NULL CHECK (state IN ('launching', 'active', 'stopped', 'cleaned', 'busy', 'ambiguous')),
    process_generation INTEGER NOT NULL
        CHECK (process_generation BETWEEN 1 AND 9007199254740991),
    launch_operation_id TEXT NOT NULL UNIQUE
        CHECK (length(launch_operation_id) = 64)
        CHECK (launch_operation_id NOT GLOB '*[^0-9a-f]*'),
    launch_idempotency_key TEXT NOT NULL
        CHECK (length(launch_idempotency_key) BETWEEN 1 AND 128)
        CHECK (substr(launch_idempotency_key, 1, 1) GLOB '[A-Za-z0-9]')
        CHECK (launch_idempotency_key NOT GLOB '*[^A-Za-z0-9._:-]*'),
    launch_request_hash BLOB NOT NULL CHECK (length(launch_request_hash) = 32),
    pending_operation_id TEXT
        CHECK (pending_operation_id IS NULL OR (
            length(pending_operation_id) = 64
            AND pending_operation_id NOT GLOB '*[^0-9a-f]*'
        )),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CHECK (
        (state = 'launching' AND run_id IS NULL AND pending_operation_id = launch_operation_id)
        OR (state IN ('active', 'stopped', 'cleaned') AND run_id IS NOT NULL AND pending_operation_id IS NULL)
        OR (state = 'busy' AND run_id IS NOT NULL AND pending_operation_id IS NOT NULL)
        OR (state = 'ambiguous' AND pending_operation_id IS NOT NULL)
    )
);

CREATE TABLE execution_operation_journal (
    operation_id TEXT PRIMARY KEY
        CHECK (length(operation_id) = 64)
        CHECK (operation_id NOT GLOB '*[^0-9a-f]*'),
    external_run_id TEXT NOT NULL
        CHECK (length(external_run_id) BETWEEN 1 AND 128)
        CHECK (substr(external_run_id, 1, 1) GLOB '[A-Za-z0-9]')
        CHECK (external_run_id NOT GLOB '*[^A-Za-z0-9._:-]*'),
    run_id TEXT
        CHECK (run_id IS NULL OR (
            length(run_id) BETWEEN 1 AND 128
            AND substr(run_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND run_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        )),
    operation TEXT NOT NULL
        CHECK (operation IN ('launch', 'send', 'interrupt', 'resume', 'restore', 'stop', 'cleanup')),
    idempotency_key TEXT NOT NULL
        CHECK (length(idempotency_key) BETWEEN 1 AND 128)
        CHECK (substr(idempotency_key, 1, 1) GLOB '[A-Za-z0-9]')
        CHECK (idempotency_key NOT GLOB '*[^A-Za-z0-9._:-]*'),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    request_json TEXT NOT NULL
        CHECK (length(CAST(request_json AS BLOB)) BETWEEN 2 AND 1048576)
        CHECK (json_valid(request_json)),
    expected_process_generation INTEGER
        CHECK (expected_process_generation IS NULL OR expected_process_generation BETWEEN 1 AND 9007199254740991),
    target_process_generation INTEGER NOT NULL
        CHECK (target_process_generation BETWEEN 1 AND 9007199254740991),
    state TEXT NOT NULL CHECK (state IN ('accepted', 'dispatched', 'result', 'ambiguous')),
    accepted_at TIMESTAMP NOT NULL,
    dispatched_at TIMESTAMP,
    dispatch_owner TEXT
        CHECK (dispatch_owner IS NULL OR length(dispatch_owner) BETWEEN 8 AND 128),
    dispatch_fence TEXT
        CHECK (dispatch_fence IS NULL OR (
            length(dispatch_fence) = 32
            AND dispatch_fence NOT GLOB '*[^0-9a-f]*'
        )),
    completed_at TIMESTAMP,
    result_run_id TEXT
        CHECK (result_run_id IS NULL OR (
            length(result_run_id) BETWEEN 1 AND 128
            AND substr(result_run_id, 1, 1) GLOB '[A-Za-z0-9]'
            AND result_run_id NOT GLOB '*[^A-Za-z0-9._:-]*'
        )),
    result_process_generation INTEGER
        CHECK (result_process_generation IS NULL OR result_process_generation BETWEEN 1 AND 9007199254740991),
    result_json TEXT
        CHECK (result_json IS NULL OR (
            length(CAST(result_json AS BLOB)) BETWEEN 2 AND 1048576
            AND json_valid(result_json)
        )),
    result_hash BLOB CHECK (result_hash IS NULL OR length(result_hash) = 32),
    UNIQUE (external_run_id, idempotency_key),
    CHECK ((operation = 'launch') = (run_id IS NULL)),
    CHECK ((operation = 'launch') = (expected_process_generation IS NULL)),
    CHECK ((dispatch_owner IS NULL) = (dispatch_fence IS NULL)),
    CHECK ((dispatched_at IS NULL) = (dispatch_owner IS NULL)),
    CHECK (
        (state = 'accepted' AND dispatched_at IS NULL AND completed_at IS NULL
            AND result_json IS NULL AND result_hash IS NULL
            AND result_run_id IS NULL AND result_process_generation IS NULL)
        OR (state = 'dispatched' AND dispatched_at IS NOT NULL AND completed_at IS NULL
            AND result_json IS NULL AND result_hash IS NULL
            AND result_run_id IS NULL AND result_process_generation IS NULL)
        OR (state = 'result' AND dispatched_at IS NOT NULL AND completed_at IS NOT NULL
            AND result_json IS NOT NULL AND result_hash IS NOT NULL
            AND result_run_id IS NOT NULL AND result_process_generation IS NOT NULL)
        OR (state = 'ambiguous' AND dispatched_at IS NOT NULL AND completed_at IS NOT NULL
            AND result_json IS NOT NULL AND result_hash IS NOT NULL
            AND result_run_id IS NULL AND result_process_generation IS NULL)
    )
);

CREATE INDEX idx_execution_operation_journal_run_state
    ON execution_operation_journal(external_run_id, state, accepted_at);

CREATE INDEX idx_execution_operation_journal_dispatch
    ON execution_operation_journal(state, dispatch_owner, dispatched_at)
    WHERE state = 'dispatched';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_execution_operation_journal_dispatch;
DROP INDEX IF EXISTS idx_execution_operation_journal_run_state;
DROP TABLE IF EXISTS execution_operation_journal;
DROP TABLE IF EXISTS execution_run_bindings;
-- +goose StatementEnd
