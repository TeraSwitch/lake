package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/sdk/client"

	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers/dberror"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/api/rewards"
	workerrewards "github.com/malbeclabs/lake/worker/pkg/rewards"
)

// maxRewardsBody limits request body size for rewards POST endpoints (5 MB).
const maxRewardsBody = 5 * 1024 * 1024

// WorkflowResponse is returned by POST endpoints that start a Temporal workflow.
type WorkflowResponse struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

// GetRewardsSimulate handles GET /api/rewards/simulate.
// Returns the latest Shapley results from PostgreSQL.
// If no result exists or the cached topology hash is stale vs the current
// live network, triggers SimulateCronWorkflow on-demand using a deterministic
// workflow ID so concurrent requests coalesce.
func GetRewardsSimulate(w http.ResponseWriter, r *http.Request) {
	if config.PgPool == nil {
		http.Error(w, "database not available", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	cached, err := rewards.FetchLatestSimulation(ctx, config.PgPool)
	if err != nil {
		// No cached result at all — trigger computation if Temporal is available
		if config.TemporalClient != nil {
			triggerBaselineSimulation(r.Context())
		}
		http.Error(w, "rewards simulation is computing, please try again shortly", http.StatusServiceUnavailable)
		return
	}

	refreshing := false

	// Check if the cached topology hash is stale vs the current live network
	if config.TemporalClient != nil {
		liveCtx, liveCancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer liveCancel()

		liveNet, err := rewards.FetchLiveNetwork(liveCtx, envDB(liveCtx))
		if err == nil {
			currentHash := rewards.TopologyHash(liveNet.Network)
			if cached.TopologyHash == "" || currentHash != cached.TopologyHash {
				refreshing = true
				triggerBaselineSimulation(r.Context())
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"results":     cached.Results,
		"total_value": cached.TotalValue,
		"computed_at": cached.ComputedAt.UTC().Format(time.RFC3339),
		"epoch":       cached.Epoch,
		"refreshing":  refreshing,
	})
}

// triggerBaselineSimulation fires off a SimulateCronWorkflow with a fixed
// workflow ID so that concurrent callers coalesce onto the same run.
func triggerBaselineSimulation(_ context.Context) {
	go func() {
		triggerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := config.TemporalClient.ExecuteWorkflow(triggerCtx, client.StartWorkflowOptions{
			ID:        "rewards-simulate",
			TaskQueue: workerrewards.TaskQueue,
		}, workerrewards.SimulateCronWorkflow)
		if err != nil {
			log.Printf("rewards: on-demand simulate trigger: %v", err)
		}
	}()
}

// PostRewardsCompare handles POST /api/rewards/compare.
// Body: { "baseline": <ShapleyInput>, "modified": <ShapleyInput> }
// Starts a Temporal workflow and returns the workflow ID immediately.
func PostRewardsCompare(w http.ResponseWriter, r *http.Request) {
	if config.TemporalClient == nil {
		http.Error(w, "temporal not available", http.StatusServiceUnavailable)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRewardsBody)

	var req struct {
		Baseline rewards.ShapleyInput `json:"baseline"`
		Modified rewards.ShapleyInput `json:"modified"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	input := workerrewards.CompareInput{
		Baseline: req.Baseline,
		Modified: req.Modified,
	}

	// Use cached baseline from PG if available
	if config.PgPool != nil {
		if cached, err := rewards.FetchLatestSimulation(ctx, config.PgPool); err == nil {
			input.CachedBaseline = cached.Results
		}
	}

	// Compare uses a random ID — inputs are user-modified networks so no dedup
	run, err := config.TemporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "rewards-compare-" + rewards.TopologyHash(req.Modified)[:12],
		TaskQueue: workerrewards.TaskQueue,
	}, workerrewards.CompareWorkflow, input)
	if err != nil {
		log.Printf("rewards: compare: failed to start workflow: %v", err)
		http.Error(w, "comparison failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(WorkflowResponse{
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
	})
}

// PostRewardsLinkEstimate handles POST /api/rewards/link-estimate.
// Body: { "operator": "name", "network": <ShapleyInput> }
// Checks the PG cache first; if a cached result exists for the topology hash,
// returns it immediately. Otherwise starts a Temporal workflow with a
// deterministic ID so concurrent requests for the same operator+topology
// coalesce onto one run.
func PostRewardsLinkEstimate(w http.ResponseWriter, r *http.Request) {
	if config.TemporalClient == nil {
		http.Error(w, "temporal not available", http.StatusServiceUnavailable)
		return
	}

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

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Check PG cache by topology hash
	topologyHash := rewards.TopologyHash(req.Network)
	if config.PgPool != nil {
		cached, err := rewards.FetchCachedLinkEstimate(ctx, config.PgPool, req.Operator, topologyHash)
		if err == nil && cached != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "completed",
				"result": cached,
			})
			return
		}
	}

	// No cache hit — start workflow with deterministic ID for dedup
	input := workerrewards.LinkEstimateInput{
		Operator:     req.Operator,
		Network:      req.Network,
		TopologyHash: topologyHash,
	}

	// Use cached baseline from PG if available
	if config.PgPool != nil {
		if cached, err := rewards.FetchLatestSimulation(ctx, config.PgPool); err == nil {
			input.CachedBaseline = cached.Results
		}
	}

	workflowID := "rewards-link-estimate-" + req.Operator + "-" + topologyHash[:12]
	run, err := config.TemporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: workerrewards.TaskQueue,
	}, workerrewards.LinkEstimateWorkflow, input)
	if err != nil {
		log.Printf("rewards: link estimate: failed to start workflow: %v", err)
		http.Error(w, "link estimate failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(WorkflowResponse{
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
	})
}

// GetRewardsWorkflowResult handles GET /api/rewards/workflows/{id}.
// Polls the Temporal workflow for status and returns results when complete.
func GetRewardsWorkflowResult(w http.ResponseWriter, r *http.Request) {
	if config.TemporalClient == nil {
		http.Error(w, "temporal not available", http.StatusServiceUnavailable)
		return
	}

	workflowID := chi.URLParam(r, "id")
	if workflowID == "" {
		http.Error(w, "missing workflow id", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Describe the workflow to get its status
	desc, err := config.TemporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		log.Printf("rewards: workflow %s: describe failed: %v", workflowID, err)
		http.Error(w, "workflow not found", http.StatusNotFound)
		return
	}

	status := desc.WorkflowExecutionInfo.Status.String()

	switch status {
	case "Running":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":      "running",
			"workflow_id": workflowID,
		})

	case "Completed":
		// Fetch the result. We need to know the workflow type to deserialize properly.
		// Use the workflow type name to determine what to decode into.
		typeName := desc.WorkflowExecutionInfo.Type.Name
		run := config.TemporalClient.GetWorkflow(ctx, workflowID, "")

		var result any
		switch typeName {
		case "CompareWorkflow":
			var r rewards.CompareResult
			if err := run.Get(ctx, &r); err != nil {
				log.Printf("rewards: workflow %s: get result failed: %v", workflowID, err)
				http.Error(w, "failed to get result", http.StatusInternalServerError)
				return
			}
			result = &r
		case "LinkEstimateWorkflow":
			var r rewards.LinkEstimateResult
			if err := run.Get(ctx, &r); err != nil {
				log.Printf("rewards: workflow %s: get result failed: %v", workflowID, err)
				http.Error(w, "failed to get result", http.StatusInternalServerError)
				return
			}
			result = &r
		default:
			http.Error(w, "unsupported workflow type: "+typeName, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":      "completed",
			"workflow_id": workflowID,
			"result":      result,
		})

	default: // Failed, Terminated, TimedOut, Canceled
		// Try to get the error message
		run := config.TemporalClient.GetWorkflow(ctx, workflowID, "")
		errMsg := "workflow " + status
		if err := run.Get(ctx, nil); err != nil {
			errMsg = err.Error()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":      "failed",
			"workflow_id": workflowID,
			"error":       errMsg,
		})
	}
}

// GetRewardsLiveNetwork handles GET /api/rewards/live-network.
// Returns the current network topology, preferring the cached version from PG.
func GetRewardsLiveNetwork(w http.ResponseWriter, r *http.Request) {
	// Try cached version from PG first
	if config.PgPool != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		if cached, err := rewards.FetchLatestSimulation(ctx, config.PgPool); err == nil && cached.LiveNetwork != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(cached.LiveNetwork)
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
