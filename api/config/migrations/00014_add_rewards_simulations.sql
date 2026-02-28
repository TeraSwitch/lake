-- +goose Up

CREATE TABLE rewards_simulations (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    params JSONB NOT NULL DEFAULT '{}',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_rewards_simulations_status ON rewards_simulations(status);

CREATE TABLE rewards_simulation_results (
    id SERIAL PRIMARY KEY,
    simulation_id TEXT NOT NULL REFERENCES rewards_simulations(id) ON DELETE CASCADE,
    operator TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    proportion DOUBLE PRECISION NOT NULL
);

CREATE INDEX idx_rewards_simulation_results_sim_id ON rewards_simulation_results(simulation_id);

-- +goose Down

DROP TABLE IF EXISTS rewards_simulation_results;
DROP TABLE IF EXISTS rewards_simulations;
