package bot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/api/handlers"
	"github.com/malbeclabs/lake/worker/pkg/chat"
	temporalclient "go.temporal.io/sdk/client"
)

const slackPollInterval = 500 * time.Millisecond

// ChatStreamResult holds the result from a chat workflow.
type ChatStreamResult struct {
	Answer          string
	Classification  workflow.Classification
	DataQuestions   []workflow.DataQuestion
	ExecutedQueries []workflow.ExecutedQuery
	SessionID       string
}

// ChatRunner runs chat workflows and returns results.
type ChatRunner interface {
	ChatStream(
		ctx context.Context,
		message string,
		history []workflow.ConversationMessage,
		sessionID string,
		onProgress func(workflow.Progress),
	) (ChatStreamResult, error)
}

// TemporalChatRunner runs chat workflows via Temporal.
type TemporalChatRunner struct {
	log *slog.Logger
}

// NewTemporalChatRunner creates a new Temporal-based chat runner.
func NewTemporalChatRunner(log *slog.Logger) *TemporalChatRunner {
	return &TemporalChatRunner{log: log}
}

// ChatStream starts a Temporal chat workflow and polls PG for progress.
func (r *TemporalChatRunner) ChatStream(
	ctx context.Context,
	message string,
	history []workflow.ConversationMessage,
	sessionID string,
	onProgress func(workflow.Progress),
) (ChatStreamResult, error) {
	if sessionID == "" {
		sessionID = uuid.New().String()
	}
	sessionUUID, err := uuid.Parse(sessionID)
	if err != nil {
		return ChatStreamResult{}, fmt.Errorf("invalid session ID: %w", err)
	}

	// Ensure a session row exists (Slack sessions are ephemeral)
	_, err = config.PgPool.Exec(ctx, `
		INSERT INTO sessions (id, type, name, content)
		VALUES ($1, 'chat', 'Slack', '[]')
		ON CONFLICT (id) DO NOTHING
	`, sessionUUID)
	if err != nil {
		return ChatStreamResult{}, fmt.Errorf("failed to ensure session: %w", err)
	}

	// Create workflow run in PG
	run, err := handlers.CreateWorkflowRun(ctx, sessionUUID, message, "mainnet-beta")
	if err != nil {
		return ChatStreamResult{}, fmt.Errorf("failed to create workflow run: %w", err)
	}

	// Start Temporal workflow
	temporalWorkflowID := "chat-" + run.ID.String()
	_, err = config.TemporalClient.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        temporalWorkflowID,
		TaskQueue: chat.TaskQueue,
	}, chat.ChatWorkflow, chat.ChatWorkflowInput{
		WorkflowRunID: run.ID.String(),
		SessionID:     sessionUUID.String(),
		Question:      message,
		History:       history,
		Format:        "slack",
		Env:           "mainnet-beta",
	})
	if err != nil {
		return ChatStreamResult{}, fmt.Errorf("failed to start Temporal workflow: %w", err)
	}

	// Poll PG for progress, translating steps into onProgress callbacks
	result, err := r.pollForResult(ctx, run.ID, onProgress)
	if err != nil {
		return ChatStreamResult{}, err
	}

	result.SessionID = sessionID
	return result, nil
}

// pollForResult polls workflow_runs for step progress and the final result.
func (r *TemporalChatRunner) pollForResult(
	ctx context.Context,
	workflowRunID uuid.UUID,
	onProgress func(workflow.Progress),
) (ChatStreamResult, error) {
	ticker := time.NewTicker(slackPollInterval)
	defer ticker.Stop()

	var lastStepCount int
	var dataQuestions []workflow.DataQuestion
	var queriesTotal, queriesDone int

	for {
		select {
		case <-ctx.Done():
			return ChatStreamResult{}, ctx.Err()
		case <-ticker.C:
			run, err := handlers.GetWorkflowRun(ctx, workflowRunID)
			if err != nil {
				r.log.Warn("Failed to poll workflow run", "id", workflowRunID, "error", err)
				continue
			}
			if run == nil {
				continue
			}

			// Parse steps
			var steps []handlers.WorkflowStep
			if err := json.Unmarshal(run.Steps, &steps); err != nil {
				steps = nil
			}

			// Emit progress for new steps
			for i := lastStepCount; i < len(steps); i++ {
				step := steps[i]
				switch step.Type {
				case "thinking":
					onProgress(workflow.Progress{
						Stage:           workflow.StageThinking,
						ThinkingContent: step.Content,
						DataQuestions:   dataQuestions,
						QueriesTotal:    queriesTotal,
						QueriesDone:     queriesDone,
					})
				case "sql_query":
					queriesTotal++
					dataQuestions = append(dataQuestions, workflow.DataQuestion{
						Question: step.Question,
					})
					queriesDone++
					onProgress(workflow.Progress{
						Stage:         workflow.StageExecuting,
						DataQuestions: dataQuestions,
						QueriesTotal:  queriesTotal,
						QueriesDone:   queriesDone,
					})
				case "cypher_query":
					queriesTotal++
					dataQuestions = append(dataQuestions, workflow.DataQuestion{
						Question: step.Question,
					})
					queriesDone++
					onProgress(workflow.Progress{
						Stage:         workflow.StageExecuting,
						DataQuestions: dataQuestions,
						QueriesTotal:  queriesTotal,
						QueriesDone:   queriesDone,
					})
				case "read_docs":
					queriesTotal++
					dataQuestions = append(dataQuestions, workflow.DataQuestion{
						Question:  "Reading " + step.Page,
						Rationale: "doc_read",
					})
					queriesDone++
					onProgress(workflow.Progress{
						Stage:         workflow.StageExecuting,
						DataQuestions: dataQuestions,
						QueriesTotal:  queriesTotal,
						QueriesDone:   queriesDone,
					})
				}
			}
			lastStepCount = len(steps)

			// Check terminal states
			switch run.Status {
			case "completed":
				classification := workflow.ClassificationConversational
				if queriesTotal > 0 {
					classification = workflow.ClassificationDataAnalysis
				}

				onProgress(workflow.Progress{
					Stage:          workflow.StageComplete,
					Classification: classification,
					DataQuestions:  dataQuestions,
					QueriesTotal:   queriesTotal,
					QueriesDone:    queriesDone,
				})

				result := ChatStreamResult{
					Answer:         "",
					Classification: classification,
				}
				if run.FinalAnswer != nil {
					result.Answer = *run.FinalAnswer
				}

				// Parse executed queries from steps for the result
				var executedQueries []workflow.ExecutedQuery
				_ = json.Unmarshal(run.ExecutedQueries, &executedQueries)
				result.ExecutedQueries = executedQueries
				result.DataQuestions = dataQuestions

				return result, nil

			case "failed":
				errorMsg := "workflow failed"
				if run.Error != nil {
					errorMsg = *run.Error
				}
				return ChatStreamResult{}, fmt.Errorf("%s", errorMsg)

			case "cancelled":
				return ChatStreamResult{}, fmt.Errorf("workflow was cancelled")
			}
		}
	}
}
