-- +goose Up

-- Reusable webhook delivery endpoints.
CREATE TABLE webhook_endpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id),
    name VARCHAR(100) NOT NULL DEFAULT '',
    url TEXT NOT NULL,
    output_format VARCHAR(20) NOT NULL DEFAULT 'markdown',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhook_endpoints_account ON webhook_endpoints(account_id);

-- Notification configs: what to watch and which endpoint to deliver to.
CREATE TABLE notification_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id),
    endpoint_id UUID NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    source_type VARCHAR(50) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    filters JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notification_configs_account ON notification_configs(account_id);
CREATE INDEX idx_notification_configs_enabled ON notification_configs(enabled) WHERE enabled = true;
CREATE INDEX idx_notification_configs_endpoint ON notification_configs(endpoint_id);

-- Tracks the last processed event per account per source type.
CREATE TABLE notification_checkpoints (
    account_id UUID NOT NULL REFERENCES accounts(id),
    source_type VARCHAR(50) NOT NULL,
    last_event_ts TIMESTAMPTZ NOT NULL,
    last_slot BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, source_type)
);

-- +goose Down
DROP TABLE IF EXISTS notification_checkpoints;
DROP TABLE IF EXISTS notification_configs;
DROP TABLE IF EXISTS webhook_endpoints;
