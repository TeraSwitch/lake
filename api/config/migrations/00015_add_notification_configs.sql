-- +goose Up

-- Notification configurations owned by individual accounts.
-- source_type: what to watch (escrow_events, ...)
-- channel_type: where to deliver (slack, webhook, ...)
-- destination: channel-specific config (e.g. {"team_id":"T123","channel_id":"C456"} for slack)
-- filters: source-specific exclusions (e.g. {"exclude_signers":["key1","key2"]} for escrow_events)
--
-- For Slack channels, destination.team_id references the Slack installation.
-- On installation takeover, configs using that team_id are transferred to the new installer.
CREATE TABLE notification_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id),
    source_type VARCHAR(50) NOT NULL,
    channel_type VARCHAR(50) NOT NULL,
    destination JSONB NOT NULL DEFAULT '{}',
    output_format VARCHAR(20) NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    filters JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notification_configs_account ON notification_configs(account_id);
CREATE INDEX idx_notification_configs_enabled ON notification_configs(enabled) WHERE enabled = true;

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
