-- +goose Up
-- Remove distributed workflow claiming columns (replaced by Temporal)
DROP INDEX IF EXISTS idx_workflow_runs_claimable;
ALTER TABLE workflow_runs DROP COLUMN IF EXISTS claimed_by;
ALTER TABLE workflow_runs DROP COLUMN IF EXISTS claimed_at;

-- Add follow-up questions column for Temporal polling
ALTER TABLE workflow_runs ADD COLUMN follow_up_questions JSONB NOT NULL DEFAULT '[]';

-- +goose Down
ALTER TABLE workflow_runs DROP COLUMN IF EXISTS follow_up_questions;
ALTER TABLE workflow_runs ADD COLUMN claimed_by TEXT;
ALTER TABLE workflow_runs ADD COLUMN claimed_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_workflow_runs_claimable ON workflow_runs(status, claimed_at)
    WHERE status = 'running';
