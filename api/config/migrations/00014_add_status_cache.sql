-- +goose Up
CREATE TABLE IF NOT EXISTS status_cache (
    cache_key TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS status_cache;
