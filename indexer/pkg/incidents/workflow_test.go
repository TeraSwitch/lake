package incidents

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// newWorkflowEnv creates a test workflow environment with all incident
// workflows and activities registered.
func newWorkflowEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(DetectIncidentsWorkflow)
	env.RegisterActivity((*Activities).DeriveWatermark)
	env.RegisterActivity((*Activities).CheckRollupFreshness)
	env.RegisterActivity((*Activities).BackfillLinkChunk)
	env.RegisterActivity((*Activities).BackfillDeviceChunk)
	return env
}

// getContinueAsNewState extracts the DetectionState from a ContinueAsNewError.
func getContinueAsNewState(t *testing.T, err error) DetectionState {
	t.Helper()
	var continueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &continueAsNew)
	var state DetectionState
	dc := converter.GetDefaultDataConverter()
	require.NoError(t, dc.FromPayloads(continueAsNew.Input, &state))
	return state
}

func TestDetectIncidentsWorkflow_ColdStart(t *testing.T) {
	env := newWorkflowEnv(t)

	watermark := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)

	env.OnActivity((*Activities).DeriveWatermark, mock.Anything, mock.Anything).Return(watermark, nil).Once()
	// Return freshness equal to watermark — nothing to process.
	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      watermark,
		LatencyFreshUntil: watermark,
		TrafficFreshUntil: watermark,
	}, nil)

	env.ExecuteWorkflow(DetectIncidentsWorkflow, DetectionState{})

	env.AssertActivityCalled(t, "DeriveWatermark", mock.Anything, mock.Anything)

	// Both watermarks should initialize to the derived watermark.
	state := getContinueAsNewState(t, env.GetWorkflowError())
	assert.Equal(t, watermark, state.LatencyWatermark)
	assert.Equal(t, watermark, state.TrafficWatermark)
}

func TestDetectIncidentsWorkflow_BothPipelinesCaughtUp(t *testing.T) {
	env := newWorkflowEnv(t)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	state := DetectionState{
		LatencyWatermark: base,
		TrafficWatermark: base,
	}
	freshUntil := base.Add(1 * time.Hour)

	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      freshUntil,
		LatencyFreshUntil: freshUntil,
		TrafficFreshUntil: freshUntil,
	}, nil).Once()
	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      freshUntil,
		LatencyFreshUntil: freshUntil,
		TrafficFreshUntil: freshUntil,
	}, nil)

	env.OnActivity((*Activities).BackfillLinkChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity((*Activities).BackfillDeviceChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DetectIncidentsWorkflow, state)

	env.AssertActivityCalled(t, "BackfillLinkChunk", mock.Anything, mock.Anything, mock.Anything)
	env.AssertActivityCalled(t, "BackfillDeviceChunk", mock.Anything, mock.Anything, mock.Anything)

	// Both watermarks advance to the same freshness.
	newState := getContinueAsNewState(t, env.GetWorkflowError())
	assert.Equal(t, freshUntil, newState.LatencyWatermark)
	assert.Equal(t, freshUntil, newState.TrafficWatermark)
}

func TestDetectIncidentsWorkflow_WatermarksAdvanceIndependently(t *testing.T) {
	env := newWorkflowEnv(t)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	state := DetectionState{
		LatencyWatermark: base,
		TrafficWatermark: base,
	}

	latencyFresh := base.Add(1 * time.Hour)
	trafficFresh := base.Add(20 * time.Minute) // behind

	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      latencyFresh,
		LatencyFreshUntil: latencyFresh,
		TrafficFreshUntil: trafficFresh,
	}, nil).Once()
	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      latencyFresh,
		LatencyFreshUntil: latencyFresh,
		TrafficFreshUntil: trafficFresh,
	}, nil)

	env.OnActivity((*Activities).BackfillLinkChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity((*Activities).BackfillDeviceChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DetectIncidentsWorkflow, state)

	newState := getContinueAsNewState(t, env.GetWorkflowError())
	assert.Equal(t, latencyFresh, newState.LatencyWatermark,
		"latency watermark should advance to latency freshness")
	assert.Equal(t, trafficFresh, newState.TrafficWatermark,
		"traffic watermark should advance to traffic freshness (behind latency)")
}

func TestDetectIncidentsWorkflow_FailedChunk_WatermarksDontAdvance(t *testing.T) {
	env := newWorkflowEnv(t)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	state := DetectionState{
		LatencyWatermark: base,
		TrafficWatermark: base,
	}

	freshUntil := base.Add(1 * time.Hour)

	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      freshUntil,
		LatencyFreshUntil: freshUntil,
		TrafficFreshUntil: freshUntil,
	}, nil).Once()
	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      freshUntil,
		LatencyFreshUntil: freshUntil,
		TrafficFreshUntil: freshUntil,
	}, nil)

	// Link chunk fails on first iteration.
	env.OnActivity((*Activities).BackfillLinkChunk, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()
	env.OnActivity((*Activities).BackfillLinkChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity((*Activities).BackfillDeviceChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DetectIncidentsWorkflow, state)

	newState := getContinueAsNewState(t, env.GetWorkflowError())
	// First iteration fails, so watermarks don't advance for that cycle.
	// Subsequent iterations succeed with the same freshness, so watermarks
	// eventually advance. Verify they reach freshUntil (recovery worked).
	assert.Equal(t, freshUntil, newState.LatencyWatermark, "should recover and advance after retry")
	assert.Equal(t, freshUntil, newState.TrafficWatermark, "should recover and advance after retry")
}

func TestDetectIncidentsWorkflow_NoNewData_SkipsProcessing(t *testing.T) {
	env := newWorkflowEnv(t)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	state := DetectionState{
		LatencyWatermark: base,
		TrafficWatermark: base,
	}

	// Freshness at or behind watermarks — no new data.
	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      base,
		LatencyFreshUntil: base,
		TrafficFreshUntil: base,
	}, nil)

	env.ExecuteWorkflow(DetectIncidentsWorkflow, state)

	// Backfill activities should never be called.
	env.AssertActivityNotCalled(t, "BackfillLinkChunk", mock.Anything, mock.Anything, mock.Anything)
	env.AssertActivityNotCalled(t, "BackfillDeviceChunk", mock.Anything, mock.Anything, mock.Anything)

	newState := getContinueAsNewState(t, env.GetWorkflowError())
	assert.Equal(t, base, newState.LatencyWatermark, "should not advance without new data")
	assert.Equal(t, base, newState.TrafficWatermark, "should not advance without new data")
}

func TestDetectIncidentsWorkflow_CatchUpProcessesFromMinWatermark(t *testing.T) {
	env := newWorkflowEnv(t)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	// Latency ahead, traffic behind.
	state := DetectionState{
		LatencyWatermark: base.Add(1 * time.Hour),
		TrafficWatermark: base.Add(20 * time.Minute),
	}

	freshUntil := base.Add(2 * time.Hour)

	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      freshUntil,
		LatencyFreshUntil: freshUntil,
		TrafficFreshUntil: freshUntil,
	}, nil).Once()
	env.OnActivity((*Activities).CheckRollupFreshness, mock.Anything, mock.Anything).Return(RollupFreshness{
		LatestBucket:      freshUntil,
		LatencyFreshUntil: freshUntil,
		TrafficFreshUntil: freshUntil,
	}, nil)

	var linkCallCount int
	env.OnActivity((*Activities).BackfillLinkChunk, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { linkCallCount++ }).Return(nil)
	env.OnActivity((*Activities).BackfillDeviceChunk, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(DetectIncidentsWorkflow, state)

	// Should process from min(watermarks)=base+20m to freshUntil=base+2h = 100 min.
	// At 1h chunks, that's 2 chunks.
	assert.Equal(t, 2, linkCallCount, "should process 2 chunks from min(watermarks) to freshness")

	// Both watermarks advance to freshUntil.
	newState := getContinueAsNewState(t, env.GetWorkflowError())
	assert.Equal(t, freshUntil, newState.LatencyWatermark)
	assert.Equal(t, freshUntil, newState.TrafficWatermark)
}
