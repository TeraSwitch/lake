package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/api/rewards"
	workerrewards "github.com/malbeclabs/lake/worker/pkg/rewards"
	"golang.org/x/sync/errgroup"
)

// rewardsCache holds the background-computed Shapley results.
var rewardsCache *rewards.RewardsCache

// SetRewardsCache sets the global rewards cache instance.
func SetRewardsCache(rc *rewards.RewardsCache) {
	rewardsCache = rc
}

// maxRewardsBody limits request body size for rewards POST endpoints (5 MB).
const maxRewardsBody = 5 * 1024 * 1024

// GetRewardsSimulate handles GET /api/rewards/simulate.
// Returns pre-computed Shapley results from the background cache.
func GetRewardsSimulate(w http.ResponseWriter, r *http.Request) {
	if rewardsCache == nil || !rewardsCache.IsReady() {
		http.Error(w, "rewards simulation is computing, please try again shortly", http.StatusServiceUnavailable)
		return
	}

	results, total, computedAt, epoch := rewardsCache.GetSimulation()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"results":     results,
		"total_value": total,
		"computed_at": computedAt.UTC().Format(time.RFC3339),
		"epoch":       epoch,
	})
}

// PostRewardsCompare handles POST /api/rewards/compare.
// Body: { "baseline": <ShapleyInput>, "modified": <ShapleyInput> }
// Uses cached baseline results when available (skips one ~2min simulation).
func PostRewardsCompare(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRewardsBody)

	var req struct {
		Baseline rewards.ShapleyInput `json:"baseline"`
		Modified rewards.ShapleyInput `json:"modified"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	var baselineResults, modifiedResults []rewards.OperatorValue

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Use cached baseline when available (the frontend always sends the
		// unmodified live network as baseline, which matches the cache).
		if rewardsCache != nil && rewardsCache.IsReady() {
			cachedResults, _, _, _ := rewardsCache.GetSimulation()
			baselineResults = cachedResults
			return nil
		}
		const collapseThreshold = 5
		baseline := rewards.CollapseSmallOperators(req.Baseline, collapseThreshold)
		var err error
		baselineResults, err = rewards.Simulate(gctx, baseline)
		if err != nil {
			return fmt.Errorf("baseline simulation: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		const collapseThreshold = 5
		modified := rewards.CollapseSmallOperators(req.Modified, collapseThreshold)
		var err error
		modifiedResults, err = rewards.Simulate(gctx, modified)
		if err != nil {
			return fmt.Errorf("modified simulation: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Printf("rewards: compare: %v", err)
		http.Error(w, "comparison failed", http.StatusInternalServerError)
		return
	}

	result := rewards.BuildCompareResult(baselineResults, modifiedResults)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// PostRewardsLinkEstimate handles POST /api/rewards/link-estimate.
// Body: { "operator": "name", "network": <ShapleyInput> }
func PostRewardsLinkEstimate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRewardsBody)

	var req struct {
		Operator string               `json:"operator"`
		Network  rewards.ShapleyInput `json:"network"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Operator == "" {
		http.Error(w, "operator is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	// Pass cached baseline results to skip redundant baseline simulation (approx path)
	var cachedBaseline []rewards.OperatorValue
	if rewardsCache != nil && rewardsCache.IsReady() {
		cachedBaseline, _, _, _ = rewardsCache.GetSimulation()
	}

	result, err := rewards.LinkEstimate(ctx, req.Operator, req.Network, cachedBaseline)
	if err != nil {
		log.Printf("rewards: link estimate: %v", err)
		http.Error(w, "link estimate failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// GetRewardsLiveNetwork handles GET /api/rewards/live-network.
// Returns the current network topology, preferring the cache.
func GetRewardsLiveNetwork(w http.ResponseWriter, r *http.Request) {
	// Serve from cache if available
	if rewardsCache != nil {
		if cached := rewardsCache.GetLiveNetwork(); cached != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(cached)
			return
		}
	}

	// Fall back to direct ClickHouse query
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	liveNet, err := rewards.FetchLiveNetwork(ctx, envDB(ctx))
	metrics.RecordClickHouseQuery(time.Since(start), err)
	if err != nil {
		log.Printf("rewards: fetch live network: %v", err)
		http.Error(w, dberror.UserMessage(err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(liveNet)
}

// --- Temporal-based simulation endpoints ---

// StartSimulationRequest is the JSON body for POST /api/rewards/simulations.
type StartSimulationRequest struct {
	OperatorUptime   float64 `json:"operator_uptime"`
	ContiguityBonus  float64 `json:"contiguity_bonus"`
	DemandMultiplier float64 `json:"demand_multiplier"`
}

// SimulationStatusResponse is returned by both POST and GET simulation endpoints.
type SimulationStatusResponse struct {
	ID          string                  `json:"id"`
	WorkflowID  string                  `json:"workflow_id"`
	RunID       string                  `json:"run_id"`
	Status      string                  `json:"status"`
	Params      *StartSimulationRequest `json:"params,omitempty"`
	Results     []rewards.OperatorValue `json:"results,omitempty"`
	Error       string                  `json:"error,omitempty"`
	CreatedAt   time.Time               `json:"created_at"`
	CompletedAt *time.Time              `json:"completed_at,omitempty"`
}

// PostStartSimulation starts a new rewards simulation via Temporal workflow.
func PostStartSimulation(w http.ResponseWriter, r *http.Request) {
	if config.TemporalClient == nil {
		http.Error(w, `{"error":"temporal not available"}`, http.StatusServiceUnavailable)
		return
	}

	var req StartSimulationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	simID := uuid.New().String()

	workflowOpts := client.StartWorkflowOptions{
		ID:        "rewards-simulation-" + simID,
		TaskQueue: workerrewards.TaskQueue,
	}

	simReq := workerrewards.SimulationRequest{
		ID:               simID,
		OperatorUptime:   req.OperatorUptime,
		ContiguityBonus:  req.ContiguityBonus,
		DemandMultiplier: req.DemandMultiplier,
	}

	run, err := config.TemporalClient.ExecuteWorkflow(r.Context(), workflowOpts, workerrewards.RewardsSimulation, simReq)
	if err != nil {
		log.Printf("Failed to start rewards simulation workflow: %v", err)
		http.Error(w, `{"error":"failed to start simulation"}`, http.StatusInternalServerError)
		return
	}

	// Record in PostgreSQL
	params, _ := json.Marshal(req)
	_, dbErr := config.PgPool.Exec(r.Context(), `
		INSERT INTO rewards_simulations (id, workflow_id, run_id, status, params)
		VALUES ($1, $2, $3, $4, $5)
	`, simID, run.GetID(), run.GetRunID(), "running", params)
	if dbErr != nil {
		log.Printf("Failed to record simulation: %v", dbErr)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(SimulationStatusResponse{
		ID:         simID,
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
		Status:     "running",
		Params:     &req,
		CreatedAt:  time.Now().UTC(),
	})
}

// GetSimulationStatus returns the status and results of a rewards simulation.
func GetSimulationStatus(w http.ResponseWriter, r *http.Request) {
	simID := chi.URLParam(r, "id")
	if simID == "" {
		http.Error(w, `{"error":"missing simulation id"}`, http.StatusBadRequest)
		return
	}

	var resp SimulationStatusResponse
	var paramsJSON []byte
	var errStr *string

	err := config.PgPool.QueryRow(r.Context(), `
		SELECT id, workflow_id, run_id, status, params, error, created_at, completed_at
		FROM rewards_simulations
		WHERE id = $1
	`, simID).Scan(
		&resp.ID, &resp.WorkflowID, &resp.RunID, &resp.Status,
		&paramsJSON, &errStr, &resp.CreatedAt, &resp.CompletedAt,
	)
	if err != nil {
		http.Error(w, `{"error":"simulation not found"}`, http.StatusNotFound)
		return
	}

	if errStr != nil {
		resp.Error = *errStr
	}

	var params StartSimulationRequest
	if err := json.Unmarshal(paramsJSON, &params); err == nil {
		resp.Params = &params
	}

	// If completed, fetch results
	if resp.Status == "completed" {
		rows, err := config.PgPool.Query(r.Context(), `
			SELECT operator, value, proportion
			FROM rewards_simulation_results
			WHERE simulation_id = $1
			ORDER BY proportion DESC
		`, simID)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var ov rewards.OperatorValue
				if err := rows.Scan(&ov.Operator, &ov.Value, &ov.Proportion); err == nil {
					resp.Results = append(resp.Results, ov)
				}
			}
		}
	}

	// If still running, check Temporal for latest status
	if resp.Status == "running" && config.TemporalClient != nil {
		desc, err := config.TemporalClient.DescribeWorkflowExecution(r.Context(), resp.WorkflowID, resp.RunID)
		if err == nil && desc.WorkflowExecutionInfo != nil {
			switch desc.WorkflowExecutionInfo.Status.String() {
			case "Completed":
				resp.Status = "completed"
			case "Failed", "Terminated", "TimedOut", "Canceled":
				resp.Status = "failed"
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
