-- +goose Up

-- Stores rewards simulation results computed by the Temporal scheduled workflow.
-- Only the most recent row matters; older rows are kept for history.
CREATE TABLE rewards_simulations (
    id SERIAL PRIMARY KEY,
    epoch BIGINT NOT NULL DEFAULT 0,
    results JSONB NOT NULL DEFAULT '[]',
    total_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    live_network JSONB NOT NULL DEFAULT '{}',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rewards_simulations_computed_at ON rewards_simulations(computed_at DESC);

-- +goose Down

DROP TABLE IF EXISTS rewards_simulations;
