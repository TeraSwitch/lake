package incidents

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const (
	// pollInterval is how often the detection workflow checks for state changes.
	pollInterval = 30 * time.Second

	// continueAsNewThreshold is the number of iterations before the workflow
	// uses continue-as-new to reset history and avoid unbounded growth.
	continueAsNewThreshold = 60
)

// RegisterWorkflows registers all incident workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(DetectIncidentsWorkflow)
	w.RegisterWorkflow(BackfillIncidentsWorkflow)
}

// DetectIncidentsWorkflow is a long-running workflow that detects incident state
// transitions every 30 seconds. It uses continue-as-new after 60 iterations
// (~30 min) to keep workflow history bounded.
//
// Activity failures are logged and the workflow continues to the next iteration
// rather than failing, so the detection loop runs indefinitely.
func DetectIncidentsWorkflow(ctx temporalworkflow.Context, iteration int) error {
	logger := temporalworkflow.GetLogger(ctx)

	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).DetectAndWriteEvents).Get(ctx, nil); err != nil {
			logger.Error("incident detection failed", "error", err)
		}

		iteration++

		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, DetectIncidentsWorkflow, 0)
}

// BackfillIncidentsWorkflow processes historical rollup data in time chunks
// to reconstruct past incidents. Triggered manually, not auto-started.
func BackfillIncidentsWorkflow(ctx temporalworkflow.Context, input BackfillInput) error {
	if input.ChunkSize == 0 {
		input.ChunkSize = 24 * time.Hour
	}

	logger := temporalworkflow.GetLogger(ctx)

	chunkOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, chunkOpts)

	// Clean existing events in the time range if requested.
	if input.Clean {
		cleanInput := CleanTimeRangeInput{StartTime: input.StartTime, EndTime: input.EndTime}
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).CleanLinkIncidents, cleanInput).Get(ctx, nil); err != nil {
			logger.Error("clean link incidents failed", "error", err)
			return err
		}
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).CleanDeviceIncidents, cleanInput).Get(ctx, nil); err != nil {
			logger.Error("clean device incidents failed", "error", err)
			return err
		}
	}

	chunkStart := input.StartTime
	for chunkStart.Before(input.EndTime) {
		chunkEnd := chunkStart.Add(input.ChunkSize)
		if chunkEnd.After(input.EndTime) {
			chunkEnd = input.EndTime
		}

		chunk := BackfillChunkInput{
			WindowStart: chunkStart,
			WindowEnd:   chunkEnd,
			Overwrite:   input.Overwrite,
		}

		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).BackfillLinkChunk, chunk).Get(ctx, nil); err != nil {
			logger.Error("backfill link chunk failed", "error", err, "start", chunkStart, "end", chunkEnd)
		}

		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).BackfillDeviceChunk, chunk).Get(ctx, nil); err != nil {
			logger.Error("backfill device chunk failed", "error", err, "start", chunkStart, "end", chunkEnd)
		}

		chunkStart = chunkEnd
	}

	return nil
}
