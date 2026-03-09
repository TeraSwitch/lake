package incidents

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// RegisterWorkflows registers all incident workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(DetectIncidentsWorkflow)
	w.RegisterWorkflow(BackfillIncidentsWorkflow)
}

// DetectIncidentsWorkflow is the regularly scheduled workflow (every 60s).
// It detects current incidents, diffs against previous state, and writes changes.
func DetectIncidentsWorkflow(ctx temporalworkflow.Context) error {
	detectOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, detectOpts)

	// Step 1: Query previous state from ClickHouse
	var previous []Incident
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).QueryPreviousState).Get(ctx, &previous); err != nil {
		return err
	}

	// Step 2: Compute current incidents from raw telemetry
	var current []Incident
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).ComputeCurrentState).Get(ctx, &current); err != nil {
		return err
	}

	// Step 3: Diff and write
	input := DiffAndWriteInput{
		Current:  current,
		Previous: previous,
	}
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).DiffAndWrite, input).Get(ctx, nil)
}

// BackfillInput configures a backfill run.
type BackfillInput struct {
	StartTime time.Time
	EndTime   time.Time
	ChunkSize time.Duration // default 24h
}

// BackfillIncidentsWorkflow processes historical data in time chunks.
// It only populates the incidents table (no events emitted).
func BackfillIncidentsWorkflow(ctx temporalworkflow.Context, input BackfillInput) error {
	if input.ChunkSize == 0 {
		input.ChunkSize = 24 * time.Hour
	}

	chunkOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, chunkOpts)

	// Process in chunks from StartTime to EndTime
	chunkStart := input.StartTime
	for chunkStart.Before(input.EndTime) {
		chunkEnd := chunkStart.Add(input.ChunkSize)
		if chunkEnd.After(input.EndTime) {
			chunkEnd = input.EndTime
		}

		chunk := BackfillChunkInput{
			WindowStart: chunkStart,
			WindowEnd:   chunkEnd,
		}
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).BackfillChunk, chunk).Get(ctx, nil); err != nil {
			return err
		}

		chunkStart = chunkEnd
	}

	return nil
}
