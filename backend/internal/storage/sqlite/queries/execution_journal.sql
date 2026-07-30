-- name: ExecutionWriterBarrier :execrows
UPDATE execution_run_bindings
SET updated_at = updated_at
WHERE external_run_id = sqlc.arg(external_run_id);

-- name: GetExecutionRunBinding :one
SELECT external_run_id, run_id, state, process_generation,
    launch_operation_id, launch_idempotency_key, launch_request_hash,
    pending_operation_id, created_at, updated_at
FROM execution_run_bindings
WHERE external_run_id = ?;

-- name: GetExecutionOperation :one
SELECT operation_id, external_run_id, run_id, operation, idempotency_key,
    request_hash, request_json, expected_process_generation,
    target_process_generation, state, accepted_at, dispatched_at,
    dispatch_owner, dispatch_fence, completed_at, result_run_id,
    result_process_generation, result_json, result_hash
FROM execution_operation_journal
WHERE operation_id = ?;

-- name: GetExecutionOperationByIdentity :one
SELECT operation_id, external_run_id, run_id, operation, idempotency_key,
    request_hash, request_json, expected_process_generation,
    target_process_generation, state, accepted_at, dispatched_at,
    dispatch_owner, dispatch_fence, completed_at, result_run_id,
    result_process_generation, result_json, result_hash
FROM execution_operation_journal
WHERE external_run_id = sqlc.arg(external_run_id)
    AND idempotency_key = sqlc.arg(idempotency_key);

-- name: InsertExecutionLaunchBinding :execrows
INSERT OR IGNORE INTO execution_run_bindings (
    external_run_id, run_id, state, process_generation,
    launch_operation_id, launch_idempotency_key, launch_request_hash,
    pending_operation_id, created_at, updated_at
) VALUES (
    sqlc.arg(external_run_id), NULL, 'launching', 1,
    sqlc.arg(launch_operation_id), sqlc.arg(launch_idempotency_key),
    sqlc.arg(launch_request_hash), sqlc.arg(pending_operation_id),
    sqlc.arg(created_at), sqlc.arg(updated_at)
);

-- name: AcquireExecutionMutationSlot :execrows
UPDATE execution_run_bindings
SET state = 'busy', pending_operation_id = sqlc.arg(operation_id),
    updated_at = sqlc.arg(updated_at)
WHERE external_run_id = sqlc.arg(external_run_id)
    AND run_id = sqlc.arg(run_id)
    AND process_generation = sqlc.arg(expected_process_generation)
    AND state = sqlc.arg(expected_binding_state)
    AND pending_operation_id IS NULL;

-- name: InsertExecutionOperation :exec
INSERT INTO execution_operation_journal (
    operation_id, external_run_id, run_id, operation, idempotency_key,
    request_hash, request_json, expected_process_generation,
    target_process_generation, state, accepted_at, dispatched_at,
    dispatch_owner, dispatch_fence, completed_at, result_run_id,
    result_process_generation, result_json, result_hash
) VALUES (
    sqlc.arg(operation_id), sqlc.arg(external_run_id), sqlc.narg(run_id),
    sqlc.arg(operation), sqlc.arg(idempotency_key), sqlc.arg(request_hash),
    sqlc.arg(request_json), sqlc.narg(expected_process_generation),
    sqlc.arg(target_process_generation), 'accepted', sqlc.arg(accepted_at),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);

-- name: MarkExecutionDispatched :execrows
UPDATE execution_operation_journal
SET state = 'dispatched', dispatched_at = sqlc.arg(dispatched_at),
    dispatch_owner = sqlc.arg(dispatch_owner),
    dispatch_fence = sqlc.arg(dispatch_fence)
WHERE operation_id = sqlc.arg(operation_id) AND state = 'accepted';

-- name: ConfirmExecutionDispatch :execrows
UPDATE execution_operation_journal
SET dispatch_fence = dispatch_fence
WHERE operation_id = sqlc.arg(operation_id) AND state = 'dispatched'
    AND dispatch_owner = sqlc.arg(dispatch_owner)
    AND dispatch_fence = sqlc.arg(dispatch_fence);

-- name: RecordExecutionFencedResult :execrows
UPDATE execution_operation_journal
SET state = 'result', completed_at = sqlc.arg(completed_at),
    result_run_id = sqlc.arg(result_run_id),
    result_process_generation = sqlc.arg(result_process_generation),
    result_json = sqlc.arg(result_json), result_hash = sqlc.arg(result_hash)
WHERE operation_id = sqlc.arg(operation_id) AND state = 'dispatched'
    AND dispatch_owner = sqlc.arg(dispatch_owner)
    AND dispatch_fence = sqlc.arg(dispatch_fence);

-- name: RecordExecutionFencedAmbiguous :execrows
UPDATE execution_operation_journal
SET state = 'ambiguous', completed_at = sqlc.arg(completed_at),
    result_json = sqlc.arg(result_json), result_hash = sqlc.arg(result_hash)
WHERE operation_id = sqlc.arg(operation_id) AND state = 'dispatched'
    AND dispatch_owner = sqlc.arg(dispatch_owner)
    AND dispatch_fence = sqlc.arg(dispatch_fence);

-- name: RecordExecutionReconciledResult :execrows
UPDATE execution_operation_journal
SET state = 'result', completed_at = sqlc.arg(completed_at),
    result_run_id = sqlc.arg(result_run_id),
    result_process_generation = sqlc.arg(result_process_generation),
    result_json = sqlc.arg(result_json), result_hash = sqlc.arg(result_hash)
WHERE operation_id = sqlc.arg(operation_id)
    AND state = 'ambiguous';

-- name: CompleteExecutionRunBinding :execrows
UPDATE execution_run_bindings
SET run_id = sqlc.arg(run_id),
    state = CASE (
        SELECT operation FROM execution_operation_journal
        WHERE operation_id = sqlc.arg(operation_id)
    )
        WHEN 'stop' THEN 'stopped'
        WHEN 'cleanup' THEN 'cleaned'
        ELSE 'active'
    END,
    process_generation = sqlc.arg(process_generation),
    pending_operation_id = NULL, updated_at = sqlc.arg(updated_at)
WHERE execution_run_bindings.external_run_id = sqlc.arg(external_run_id)
    AND execution_run_bindings.pending_operation_id = sqlc.arg(operation_id)
    AND execution_run_bindings.state IN ('launching', 'busy', 'ambiguous');

-- name: MarkExecutionRunBindingAmbiguous :execrows
UPDATE execution_run_bindings
SET state = 'ambiguous', updated_at = sqlc.arg(updated_at)
WHERE external_run_id = sqlc.arg(external_run_id)
    AND pending_operation_id = sqlc.arg(operation_id)
    AND state IN ('launching', 'busy');
