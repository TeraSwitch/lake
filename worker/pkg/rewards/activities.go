package rewards

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	apirewards "github.com/malbeclabs/lake/api/rewards"
)

// Activities holds dependencies for reward simulation activities.
type Activities struct {
	ClickHouse driver.Conn
	PgPool     *pgxpool.Pool
}

// Simulate runs the Shapley computation on the given input via shapley-cli.
func (a *Activities) Simulate(ctx context.Context, input apirewards.ShapleyInput) ([]apirewards.OperatorValue, error) {
	return apirewards.Simulate(ctx, input)
}

// FetchTopology fetches the live network topology from ClickHouse.
func (a *Activities) FetchTopology(ctx context.Context) (*apirewards.LiveNetworkResponse, error) {
	return apirewards.FetchLiveNetwork(ctx, a.ClickHouse)
}

// HashTopology computes a deterministic hash of the ShapleyInput.
func (a *Activities) HashTopology(ctx context.Context, input apirewards.ShapleyInput) (string, error) {
	return apirewards.TopologyHash(input), nil
}

// StoreCache writes simulation results and live network to PostgreSQL.
func (a *Activities) StoreCache(ctx context.Context, input StoreCacheInput) error {
	resultsJSON, err := json.Marshal(input.Results)
	if err != nil {
		return fmt.Errorf("marshal results: %w", err)
	}
	liveNetJSON, err := json.Marshal(input.LiveNetwork)
	if err != nil {
		return fmt.Errorf("marshal live network: %w", err)
	}

	_, err = a.PgPool.Exec(ctx, `
		INSERT INTO rewards_simulations (epoch, results, total_value, live_network, topology_hash)
		VALUES ($1, $2, $3, $4, $5)
	`, input.Epoch, resultsJSON, input.TotalValue, liveNetJSON, input.TopologyHash)
	if err != nil {
		return fmt.Errorf("insert rewards cache: %w", err)
	}

	return nil
}

// StoreLinkEstimate persists a link estimate result to PostgreSQL.
func (a *Activities) StoreLinkEstimate(ctx context.Context, input StoreLinkEstimateInput) error {
	_, err := a.PgPool.Exec(ctx, `
		INSERT INTO rewards_link_estimates (operator, topology_hash, results, total_value)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (operator, topology_hash)
		DO UPDATE SET results = $3, total_value = $4, computed_at = now()
	`, input.Operator, input.TopologyHash, input.Results, input.TotalValue)
	return err
}

// Compare runs baseline and modified simulations and returns the comparison.
// When both simulations are needed, they run in parallel.
func (a *Activities) Compare(ctx context.Context, input CompareInput) (*apirewards.CompareResult, error) {
	const collapseThreshold = 5

	var baselineResults, modifiedResults []apirewards.OperatorValue

	if len(input.CachedBaseline) > 0 {
		// Cached baseline — only need to run modified
		baselineResults = input.CachedBaseline
		modified := apirewards.CollapseSmallOperators(input.Modified, collapseThreshold)
		var err error
		modifiedResults, err = apirewards.Simulate(ctx, modified)
		if err != nil {
			return nil, fmt.Errorf("modified simulation: %w", err)
		}
	} else {
		// Run both in parallel
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			baseline := apirewards.CollapseSmallOperators(input.Baseline, collapseThreshold)
			var err error
			baselineResults, err = apirewards.Simulate(gctx, baseline)
			if err != nil {
				return fmt.Errorf("baseline simulation: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			modified := apirewards.CollapseSmallOperators(input.Modified, collapseThreshold)
			var err error
			modifiedResults, err = apirewards.Simulate(gctx, modified)
			if err != nil {
				return fmt.Errorf("modified simulation: %w", err)
			}
			return nil
		})
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}

	return apirewards.BuildCompareResult(baselineResults, modifiedResults), nil
}

// LinkEstimate computes per-link Shapley values for a specific operator.
func (a *Activities) LinkEstimate(ctx context.Context, input LinkEstimateInput) (*apirewards.LinkEstimateResult, error) {
	return apirewards.LinkEstimate(ctx, input.Operator, input.Network, input.CachedBaseline)
}
