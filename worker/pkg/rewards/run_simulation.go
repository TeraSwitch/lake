package rewards

import (
	"context"
	"fmt"
	"time"

	apirewards "github.com/malbeclabs/lake/api/rewards"
)

// RunSimulation fetches the live network topology, runs the Shapley computation,
// and stores the results in PostgreSQL. It reuses the existing api/rewards package.
func (a *Activities) RunSimulation(ctx context.Context, req SimulationRequest) error {
	// Fetch live network topology from ClickHouse (reuses existing queries)
	liveNet, err := apirewards.FetchLiveNetwork(ctx, a.ClickHouse)
	if err != nil {
		return fmt.Errorf("fetch live network: %w", err)
	}

	// Override parameters from request
	network := liveNet.Network
	network.OperatorUptime = req.OperatorUptime
	network.ContiguityBonus = req.ContiguityBonus
	network.DemandMultiplier = req.DemandMultiplier

	// Collapse small operators for tractable computation
	const collapseThreshold = 5
	collapsed := apirewards.CollapseSmallOperators(network, collapseThreshold)

	// Run shapley-cli
	results, err := apirewards.Simulate(ctx, collapsed)
	if err != nil {
		return fmt.Errorf("simulate: %w", err)
	}

	// Store results in PostgreSQL
	return a.storeResults(ctx, req.ID, results)
}

func (a *Activities) storeResults(ctx context.Context, simID string, results []apirewards.OperatorValue) error {
	now := time.Now().UTC()

	// Update simulation status
	_, err := a.PgPool.Exec(ctx, `
		UPDATE rewards_simulations
		SET status = 'completed', completed_at = $1
		WHERE id = $2
	`, now, simID)
	if err != nil {
		return fmt.Errorf("update simulation: %w", err)
	}

	// Insert result rows
	for _, ov := range results {
		_, err := a.PgPool.Exec(ctx, `
			INSERT INTO rewards_simulation_results (simulation_id, operator, value, proportion)
			VALUES ($1, $2, $3, $4)
		`, simID, ov.Operator, ov.Value, ov.Proportion)
		if err != nil {
			return fmt.Errorf("insert result for operator %s: %w", ov.Operator, err)
		}
	}

	return nil
}
