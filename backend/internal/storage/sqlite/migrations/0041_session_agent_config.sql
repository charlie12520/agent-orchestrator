-- +goose Up
-- +goose StatementBegin
ALTER TABLE sessions ADD COLUMN agent_config_set BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE sessions ADD COLUMN agent_model TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN agent_permissions TEXT NOT NULL DEFAULT ''
    CHECK (agent_permissions IN ('', 'default', 'accept-edits', 'auto', 'bypass-permissions'));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE sessions DROP COLUMN agent_permissions;
ALTER TABLE sessions DROP COLUMN agent_model;
ALTER TABLE sessions DROP COLUMN agent_config_set;
-- +goose StatementEnd
