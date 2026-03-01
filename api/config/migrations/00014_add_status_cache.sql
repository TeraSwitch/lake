-- +goose Up
CREATE TABLE IF NOT EXISTS page_cache (
    cache_key TEXT PRIMARY KEY,
    data JSONB NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS page_cache;
