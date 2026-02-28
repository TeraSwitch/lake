package rewards

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CachedSimulation holds the latest simulation results from PostgreSQL.
type CachedSimulation struct {
	Results      []OperatorValue      `json:"results"`
	TotalValue   float64              `json:"total_value"`
	LiveNetwork  *LiveNetworkResponse `json:"live_network"`
	Epoch        int64                `json:"epoch"`
	ComputedAt   time.Time            `json:"computed_at"`
	TopologyHash string               `json:"topology_hash"`
}

// FetchLatestSimulation reads the most recent simulation results from PostgreSQL.
// Returns nil if no results exist yet.
func FetchLatestSimulation(ctx context.Context, pool *pgxpool.Pool) (*CachedSimulation, error) {
	var resultsJSON, liveNetJSON []byte
	var cs CachedSimulation

	err := pool.QueryRow(ctx, `
		SELECT epoch, results, total_value, live_network, computed_at, topology_hash
		FROM rewards_simulations
		ORDER BY computed_at DESC
		LIMIT 1
	`).Scan(&cs.Epoch, &resultsJSON, &cs.TotalValue, &liveNetJSON, &cs.ComputedAt, &cs.TopologyHash)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(resultsJSON, &cs.Results); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(liveNetJSON, &cs.LiveNetwork); err != nil {
		return nil, err
	}

	return &cs, nil
}

// FetchCachedLinkEstimate reads a cached link estimate for an operator+hash combo.
// Returns nil, nil if no cached result exists.
func FetchCachedLinkEstimate(ctx context.Context, pool *pgxpool.Pool, operator, topologyHash string) (*LinkEstimateResult, error) {
	var resultsJSON []byte
	var result LinkEstimateResult

	err := pool.QueryRow(ctx, `
		SELECT results, total_value
		FROM rewards_link_estimates
		WHERE operator = $1 AND topology_hash = $2
	`, operator, topologyHash).Scan(&resultsJSON, &result.TotalValue)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(resultsJSON, &result.Results); err != nil {
		return nil, err
	}

	return &result, nil
}

// StoreLinkEstimate upserts a link estimate result for an operator+hash combo.
func StoreLinkEstimate(ctx context.Context, pool *pgxpool.Pool, operator, topologyHash string, result *LinkEstimateResult) error {
	resultsJSON, err := json.Marshal(result.Results)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO rewards_link_estimates (operator, topology_hash, results, total_value)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (operator, topology_hash)
		DO UPDATE SET results = $3, total_value = $4, computed_at = now()
	`, operator, topologyHash, resultsJSON, result.TotalValue)
	return err
}
