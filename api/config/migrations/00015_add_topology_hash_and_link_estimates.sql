-- +goose Up

ALTER TABLE rewards_simulations ADD COLUMN topology_hash TEXT NOT NULL DEFAULT '';

CREATE TABLE rewards_link_estimates (
    id SERIAL PRIMARY KEY,
    operator TEXT NOT NULL,
    topology_hash TEXT NOT NULL,
    results JSONB NOT NULL DEFAULT '[]',
    total_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(operator, topology_hash)
);

-- +goose Down

DROP TABLE IF EXISTS rewards_link_estimates;
ALTER TABLE rewards_simulations DROP COLUMN IF EXISTS topology_hash;
