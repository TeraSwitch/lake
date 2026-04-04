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
// events from rollup data using per-pipeline watermarks. Each iteration:
//  1. Checks rollup freshness per pipeline (latency, traffic)
//  2. Processes from min(watermarks) → max(freshness) in chunks
//  3. Advances each watermark to its pipeline's freshness
//
// When one pipeline falls behind, the other continues detecting. When the
// lagging pipeline catches up, its gap is reprocessed with Overwrite so that
// incidents in the catch-up zone get the full symptom set.
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

	// Cold start: derive watermarks from existing events or default to now.
	if state.LatencyWatermark.IsZero() && state.TrafficWatermark.IsZero() {
		var watermark time.Time
		wCtx := temporalworkflow.WithActivityOptions(ctx, shortOpts)
		if err := temporalworkflow.ExecuteActivity(wCtx, (*Activities).DeriveWatermark).Get(ctx, &watermark); err != nil {
			logger.Error("failed to derive watermark, starting from now", "error", err)
		}
		if watermark.IsZero() {
			watermark = temporalworkflow.Now(ctx)
		}
		state.LatencyWatermark = watermark
		state.TrafficWatermark = watermark
		logger.Info("incidents: initialized watermarks",
			"latency_watermark", state.LatencyWatermark,
			"traffic_watermark", state.TrafficWatermark)
	}

	for state.Iteration < continueAsNewThreshold {
		// Check per-pipeline freshness.
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

		// Determine processing range: from the earliest watermark to the
		// latest fresh data across both pipelines.
		windowStart := minTime(state.LatencyWatermark, state.TrafficWatermark)
		windowEnd := freshness.LatestBucket

		if !windowEnd.After(windowStart) {
			state.Iteration++
			if state.Iteration < continueAsNewThreshold {
				if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
					return err
				}
			}
			continue
		}

		// Cap the gap at maxAutoBackfill.
		gap := windowEnd.Sub(windowStart)
		if gap > maxAutoBackfill {
			logger.Warn("incidents: large gap detected, capping auto-backfill",
				"gap", gap, "max", maxAutoBackfill,
				"window_start", windowStart, "window_end", windowEnd)
			windowStart = windowEnd.Add(-maxAutoBackfill)
			// Bring both watermarks forward if they're behind the cap.
			if state.LatencyWatermark.Before(windowStart) {
				state.LatencyWatermark = windowStart
			}
			if state.TrafficWatermark.Before(windowStart) {
				state.TrafficWatermark = windowStart
			}
		}

		// The catch-up boundary: chunks before max(watermarks) overlap with
		// previously processed windows where one pipeline was behind. These
		// need Overwrite to regenerate incidents with the full symptom set.
		catchUpBoundary := maxTime(state.LatencyWatermark, state.TrafficWatermark)

		// Process from windowStart → windowEnd in chunks.
		chunkCtx := temporalworkflow.WithActivityOptions(ctx, chunkOpts)
		chunkStart := windowStart
		failed := false
		for chunkStart.Before(windowEnd) {
			chunkEnd := chunkStart.Add(autoChunkSize)
			if chunkEnd.After(windowEnd) {
				chunkEnd = windowEnd
			}

			chunk := BackfillChunkInput{
				WindowStart:       chunkStart,
				WindowEnd:         chunkEnd,
				LatencyFreshUntil: freshness.LatencyFreshUntil,
				TrafficFreshUntil: freshness.TrafficFreshUntil,
				Overwrite:         chunkStart.Before(catchUpBoundary),
			}

			if err := temporalworkflow.ExecuteActivity(chunkCtx, (*Activities).BackfillLinkChunk, chunk).Get(ctx, nil); err != nil {
				logger.Error("incidents: link chunk failed, will retry next cycle",
					"error", err, "start", chunkStart, "end", chunkEnd)
				failed = true
				break
			}

			if err := temporalworkflow.ExecuteActivity(chunkCtx, (*Activities).BackfillDeviceChunk, chunk).Get(ctx, nil); err != nil {
				logger.Error("incidents: device chunk failed, will retry next cycle",
					"error", err, "start", chunkStart, "end", chunkEnd)
				failed = true
				break
			}

			chunkStart = chunkEnd
		}

		// Advance each watermark independently to its pipeline's freshness,
		// but only for the range we successfully processed.
		if !failed {
			if freshness.LatencyFreshUntil.After(state.LatencyWatermark) {
				state.LatencyWatermark = freshness.LatencyFreshUntil
			}
			if freshness.TrafficFreshUntil.After(state.TrafficWatermark) {
				state.TrafficWatermark = freshness.TrafficFreshUntil
			}
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
	return DetectionState{
		LatencyWatermark: s.LatencyWatermark,
		TrafficWatermark: s.TrafficWatermark,
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
