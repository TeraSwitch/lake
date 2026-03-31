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

	// maxAutoBackfill is the maximum gap the detection loop will
	// automatically backfill. Gaps larger than this are capped and a warning
	// is logged suggesting manual backfill for the remainder.
	maxAutoBackfill = 24 * time.Hour

	// autoChunkSize is the chunk size for automatic gap recovery.
	autoChunkSize = 1 * time.Hour
)

// RegisterWorkflows registers all incident workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(DetectIncidentsWorkflow)
	w.RegisterWorkflow(BackfillIncidentsWorkflow)
}

// DetectIncidentsWorkflow is a long-running workflow that processes incident
// events from rollup data using a watermark. Each iteration:
//  1. Checks rollup freshness (latest bucket_ts)
//  2. Processes from watermark → latest rollup data using backfill chunks
//  3. Advances the watermark
//
// After a gap (e.g., indexer was down), the workflow automatically bacfkills
// the gap in 1-hour chunks before resuming steady-state detection.
func DetectIncidentsWorkflow(ctx temporalworkflow.Context, state DetectionState) error {
	logger := temporalworkflow.GetLogger(ctx)

	shortOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	chunkOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}

	// Cold start: derive watermark from existing events or default to now.
	if state.Watermark.IsZero() {
		var watermark time.Time
		wCtx := temporalworkflow.WithActivityOptions(ctx, shortOpts)
		if err := temporalworkflow.ExecuteActivity(wCtx, (*Activities).DeriveWatermark).Get(ctx, &watermark); err != nil {
			logger.Error("failed to derive watermark, starting from now", "error", err)
		}
		if watermark.IsZero() {
			// No existing events — start from now.
			watermark = temporalworkflow.Now(ctx)
		}
		state.Watermark = watermark
		logger.Info("incidents: initialized watermark", "watermark", state.Watermark)
	}

	for state.Iteration < continueAsNewThreshold {
		// Check how fresh the rollup data is.
		var freshness RollupFreshness
		fCtx := temporalworkflow.WithActivityOptions(ctx, shortOpts)
		if err := temporalworkflow.ExecuteActivity(fCtx, (*Activities).CheckRollupFreshness).Get(ctx, &freshness); err != nil {
			logger.Error("failed to check rollup freshness", "error", err)
			state.Iteration++
			if state.Iteration < continueAsNewThreshold {
				if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
					return err
				}
			}
			continue
		}

		// Use the latest bucket timestamp directly as the window end.
		// The backfill SQL already adds 5 minutes to max(bucket_ts) for
		// symptom ended_at, so no additional margin is needed.
		windowEnd := freshness.LatestBucket

		if !windowEnd.After(state.Watermark) {
			// No new rollup data since last processing — skip.
			state.Iteration++
			if state.Iteration < continueAsNewThreshold {
				if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
					return err
				}
			}
			continue
		}

		// Cap the gap at maxAutoBackfill.
		gap := windowEnd.Sub(state.Watermark)
		if gap > maxAutoBackfill {
			logger.Warn("incidents: large gap detected, capping auto-backfill",
				"gap", gap, "max", maxAutoBackfill,
				"watermark", state.Watermark, "window_end", windowEnd)
			state.Watermark = windowEnd.Add(-maxAutoBackfill)
		}

		// Process from watermark → windowEnd in chunks.
		chunkCtx := temporalworkflow.WithActivityOptions(ctx, chunkOpts)
		chunkStart := state.Watermark
		for chunkStart.Before(windowEnd) {
			chunkEnd := chunkStart.Add(autoChunkSize)
			if chunkEnd.After(windowEnd) {
				chunkEnd = windowEnd
			}

			chunk := BackfillChunkInput{
				WindowStart: chunkStart,
				WindowEnd:   chunkEnd,
			}

			if err := temporalworkflow.ExecuteActivity(chunkCtx, (*Activities).BackfillLinkChunk, chunk).Get(ctx, nil); err != nil {
				logger.Error("incidents: link chunk failed, will retry next cycle",
					"error", err, "start", chunkStart, "end", chunkEnd)
				break // stop chunking, watermark stays at last success
			}

			if err := temporalworkflow.ExecuteActivity(chunkCtx, (*Activities).BackfillDeviceChunk, chunk).Get(ctx, nil); err != nil {
				logger.Error("incidents: device chunk failed, will retry next cycle",
					"error", err, "start", chunkStart, "end", chunkEnd)
				break
			}

			chunkStart = chunkEnd
			state.Watermark = chunkEnd
		}

		state.Iteration++
		if state.Iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, DetectIncidentsWorkflow, state.resetIteration())
}

// resetIteration returns a copy of the state with iteration reset to 0
// for ContinueAsNew.
func (s DetectionState) resetIteration() DetectionState {
	return DetectionState{Watermark: s.Watermark}
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
