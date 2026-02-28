package rewards

import (
	"encoding/json"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	apirewards "github.com/malbeclabs/lake/api/rewards"
)

// defaultActivityOpts is used for fast activities (single simulation, ~1-2 min).
var defaultActivityOpts = workflow.ActivityOptions{
	StartToCloseTimeout: 5 * time.Minute,
	RetryPolicy: &temporal.RetryPolicy{
		MaximumAttempts: 3,
	},
}

// longActivityOpts is used for slow activities (compare, link estimate) that
// may run many simulations and can take a long time for large operators.
var longActivityOpts = workflow.ActivityOptions{
	StartToCloseTimeout: 20 * time.Minute,
	RetryPolicy: &temporal.RetryPolicy{
		MaximumAttempts: 1, // no retries — too expensive to restart from scratch
	},
}

// SimulateWorkflow runs a Shapley simulation on the given input.
func SimulateWorkflow(ctx workflow.Context, input apirewards.ShapleyInput) ([]apirewards.OperatorValue, error) {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOpts)

	var results []apirewards.OperatorValue
	err := workflow.ExecuteActivity(ctx, (*Activities).Simulate, input).Get(ctx, &results)
	return results, err
}

// SimulateCronWorkflow fetches the live network, runs a Shapley simulation,
// and stores the results in PostgreSQL. Triggered on-demand when the topology
// hash is stale. Uses a deterministic workflow ID (rewards-simulate-{hash})
// so concurrent requests coalesce onto the same run.
func SimulateCronWorkflow(ctx workflow.Context) error {
	fastCtx := workflow.WithActivityOptions(ctx, defaultActivityOpts)

	// Step 1: Fetch live network topology from ClickHouse
	var liveNet apirewards.LiveNetworkResponse
	if err := workflow.ExecuteActivity(fastCtx, (*Activities).FetchTopology).Get(fastCtx, &liveNet); err != nil {
		return err
	}

	// Step 2: Hash topology
	var hash string
	if err := workflow.ExecuteActivity(fastCtx, (*Activities).HashTopology, liveNet.Network).Get(fastCtx, &hash); err != nil {
		return err
	}

	// Step 3: Collapse small operators and run Shapley simulation
	const collapseThreshold = 5
	collapsed := apirewards.CollapseSmallOperators(liveNet.Network, collapseThreshold)

	var results []apirewards.OperatorValue
	if err := workflow.ExecuteActivity(fastCtx, (*Activities).Simulate, collapsed).Get(fastCtx, &results); err != nil {
		return err
	}

	// Step 4: Compute total and store in PostgreSQL
	var total float64
	for _, r := range results {
		total += r.Value
	}

	storeInput := StoreCacheInput{
		Results:      results,
		TotalValue:   total,
		LiveNetwork:  &liveNet,
		TopologyHash: hash,
	}
	return workflow.ExecuteActivity(fastCtx, (*Activities).StoreCache, storeInput).Get(fastCtx, nil)
}

// CompareWorkflow runs baseline and modified simulations and returns the comparison.
func CompareWorkflow(ctx workflow.Context, input CompareInput) (*apirewards.CompareResult, error) {
	ctx = workflow.WithActivityOptions(ctx, longActivityOpts)

	var result apirewards.CompareResult
	err := workflow.ExecuteActivity(ctx, (*Activities).Compare, input).Get(ctx, &result)
	return &result, err
}

// LinkEstimateWorkflow computes per-link Shapley values for a specific operator.
// Uses a deterministic workflow ID (rewards-link-estimate-{operator}-{hash})
// so concurrent requests for the same operator+topology coalesce.
// If TopologyHash is set, the result is persisted for cache lookups.
func LinkEstimateWorkflow(ctx workflow.Context, input LinkEstimateInput) (*apirewards.LinkEstimateResult, error) {
	slowCtx := workflow.WithActivityOptions(ctx, longActivityOpts)
	fastCtx := workflow.WithActivityOptions(ctx, defaultActivityOpts)

	var result apirewards.LinkEstimateResult
	if err := workflow.ExecuteActivity(slowCtx, (*Activities).LinkEstimate, input).Get(slowCtx, &result); err != nil {
		return nil, err
	}

	// Store result if we have a topology hash
	if input.TopologyHash != "" {
		resultsJSON, err := json.Marshal(result.Results)
		if err == nil {
			slInput := StoreLinkEstimateInput{
				Operator:     input.Operator,
				TopologyHash: input.TopologyHash,
				Results:      resultsJSON,
				TotalValue:   result.TotalValue,
			}
			// Best-effort store — don't fail the workflow if this fails
			_ = workflow.ExecuteActivity(fastCtx, (*Activities).StoreLinkEstimate, slInput).Get(fastCtx, nil)
		}
	}

	return &result, nil
}
