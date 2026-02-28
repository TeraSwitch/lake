package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/malbeclabs/lake/api/config"
)

const pollInterval = 200 * time.Millisecond

// pollWorkflowSSE polls the workflow_runs table for progress and streams SSE events.
// This replaces in-memory event subscriptions with PG polling.
func pollWorkflowSSE(ctx context.Context, workflowRunID uuid.UUID, sendEvent func(string, any)) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	var lastStepCount int

	for {
		select {
		case <-ticker.C:
			run, err := GetWorkflowRun(ctx, workflowRunID)
			if err != nil {
				slog.Error("Failed to get workflow run during polling", "workflow_id", workflowRunID, "error", err)
				continue
			}
			if run == nil {
				continue
			}

			// Parse steps
			var steps []WorkflowStep
			if err := json.Unmarshal(run.Steps, &steps); err != nil {
				// Try parsing - may be null/empty initially
				steps = nil
			}

			// Emit new steps since last poll
			for i := lastStepCount; i < len(steps); i++ {
				emitStepAsSSE(sendEvent, steps[i])
			}
			lastStepCount = len(steps)

			// Check terminal states
			switch run.Status {
			case "completed":
				sendDoneEvent(sendEvent, run)
				return
			case "failed":
				errorMsg := "Workflow failed"
				if run.Error != nil {
					errorMsg = *run.Error
				}
				sendEvent("error", map[string]string{"error": errorMsg})
				return
			case "cancelled":
				sendEvent("error", map[string]string{"error": "Workflow was cancelled"})
				return
			}

		case <-heartbeat.C:
			sendEvent("heartbeat", map[string]string{})

		case <-ctx.Done():
			// Client disconnected - workflow continues in Temporal
			slog.Info("Client disconnected during workflow polling",
				"workflow_id", workflowRunID)
			return
		}
	}
}

// emitStepAsSSE sends a single workflow step as an SSE event.
func emitStepAsSSE(sendEvent func(string, any), step WorkflowStep) {
	stepID := step.ID
	if stepID == "" {
		stepID = uuid.New().String()
	}

	switch step.Type {
	case "thinking":
		sendEvent("thinking", map[string]string{"id": stepID, "content": step.Content})
	case "sql_query":
		sendEvent("sql_done", map[string]any{
			"id":       stepID,
			"question": step.Question,
			"sql":      step.SQL,
			"rows":     step.Count,
			"error":    step.Error,
		})
	case "cypher_query":
		sendEvent("cypher_done", map[string]any{
			"id":       stepID,
			"question": step.Question,
			"cypher":   step.Cypher,
			"rows":     step.Count,
			"error":    step.Error,
		})
	case "read_docs":
		sendEvent("read_docs_done", map[string]any{
			"id":      stepID,
			"page":    step.Page,
			"content": step.Content,
			"error":   step.Error,
		})
	case "query":
		// Legacy type - treat as SQL
		sendEvent("sql_done", map[string]any{
			"id":       stepID,
			"question": step.Question,
			"sql":      step.SQL,
			"rows":     step.Count,
			"error":    step.Error,
		})
	}
}

// sendDoneEvent sends the final done event with the full response.
func sendDoneEvent(sendEvent func(string, any), run *WorkflowRun) {
	// Parse legacy arrays for building response
	var thinkingSteps []string
	_ = json.Unmarshal(run.ThinkingSteps, &thinkingSteps)
	var executedQueries []workflow.ExecutedQuery
	_ = json.Unmarshal(run.ExecutedQueries, &executedQueries)
	var steps []WorkflowStep
	_ = json.Unmarshal(run.Steps, &steps)

	response := ChatResponse{
		Answer:        "",
		ThinkingSteps: thinkingSteps,
		Steps:         steps,
	}
	if run.FinalAnswer != nil {
		response.Answer = *run.FinalAnswer
	}

	// Extract executed queries for response
	for _, eq := range executedQueries {
		response.ExecutedQueries = append(response.ExecutedQueries, ExecutedQueryResponse{
			Question: eq.GeneratedQuery.DataQuestion.Question,
			SQL:      eq.Result.SQL,
			Cypher:   eq.Result.Cypher,
			Columns:  eq.Result.Columns,
			Rows:     convertRowsToArray(eq.Result),
			Count:    eq.Result.Count,
			Error:    eq.Result.Error,
		})
	}

	sendEvent("done", response)
}

// CancelChatWorkflow cancels a running chat workflow via Temporal.
func CancelChatWorkflow(workflowRunID uuid.UUID) error {
	temporalWorkflowID := "chat-" + workflowRunID.String()
	return config.TemporalClient.CancelWorkflow(context.Background(), temporalWorkflowID, "")
}
