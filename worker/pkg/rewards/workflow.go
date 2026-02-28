package rewards

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// RewardsSimulation is the Temporal workflow that computes Shapley value reward allocations.
// It reuses the existing api/rewards package for topology fetching and shapley-cli execution.
func RewardsSimulation(ctx workflow.Context, req SimulationRequest) (SimulationResult, error) {
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Step 1: Fetch topology and run Shapley simulation
	if err := workflow.ExecuteActivity(ctx, (*Activities).RunSimulation, req).Get(ctx, nil); err != nil {
		return SimulationResult{
			ID:     req.ID,
			Status: "failed",
			Error:  err.Error(),
		}, err
	}

	return SimulationResult{
		ID:     req.ID,
		Status: "completed",
	}, nil
}
