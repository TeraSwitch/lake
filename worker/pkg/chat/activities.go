package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	driver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/anthropics/anthropic-sdk-go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	v3 "github.com/malbeclabs/lake/agent/pkg/workflow/v3"
	neo4jpkg "github.com/malbeclabs/lake/indexer/pkg/neo4j"
	"github.com/malbeclabs/lake/internal/querier"
	"go.temporal.io/sdk/activity"
)

// Activities holds dependencies for chat workflow activities.
type Activities struct {
	ClickHouse driver.Conn
	PgPool     *pgxpool.Pool
	Neo4j      neo4jpkg.Client // optional, nil if not configured
	Database   string          // ClickHouse mainnet database name (e.g. "default")
}

// RunAgent executes the agent loop as a Temporal activity.
func (a *Activities) RunAgent(ctx context.Context, input ChatWorkflowInput) (*ChatWorkflowOutput, error) {
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow run ID: %w", err)
	}

	prompts, err := v3.LoadPrompts()
	if err != nil {
		return nil, fmt.Errorf("failed to load prompts: %w", err)
	}

	llm := workflow.NewAnthropicLLMClient(anthropic.ModelClaudeHaiku4_5, 4096)
	q := querier.NewClickHouseQuerier(a.ClickHouse)
	sf := querier.NewClickHouseSchemaFetcher(a.ClickHouse, a.Database)

	cfg := &workflow.Config{
		Logger:        slog.Default(),
		LLM:           llm,
		Querier:       q,
		SchemaFetcher: sf,
		Prompts:       prompts,
		MaxTokens:     4096,
	}

	if input.Format == "slack" {
		cfg.FormatContext = prompts.Slack
	}

	if a.Neo4j != nil && input.Env == "mainnet-beta" {
		cfg.GraphQuerier = querier.NewNeo4jQuerier(a.Neo4j)
		cfg.GraphSchemaFetcher = querier.NewNeo4jSchemaFetcher(a.Neo4j)
	}

	cfg.EnvContext = buildEnvContext(input.Env, a.Database)

	wf, err := v3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	// Track steps
	var steps []stepRecord
	sqlStepIDs := make(map[string]string)
	cypherStepIDs := make(map[string]string)
	docsStepIDs := make(map[string]string)
	var lastLLMCalls, lastInputTokens, lastOutputTokens int

	onProgress := func(progress workflow.Progress) {
		switch progress.Stage {
		case workflow.StageThinking:
			stepID := uuid.New().String()
			steps = append(steps, stepRecord{
				ID: stepID, Type: "thinking", Content: progress.ThinkingContent,
			})
		case workflow.StageSQLStarted:
			sqlStepIDs[progress.SQL] = uuid.New().String()
		case workflow.StageSQLComplete:
			stepID := sqlStepIDs[progress.SQL]
			status := "completed"
			if progress.SQLError != "" {
				status = "error"
			}
			steps = append(steps, stepRecord{
				ID: stepID, Type: "sql_query", Question: progress.SQLQuestion,
				SQL: progress.SQL, Status: status, Count: progress.SQLRows,
				Error: progress.SQLError, Env: input.Env,
			})
		case workflow.StageCypherStarted:
			cypherStepIDs[progress.Cypher] = uuid.New().String()
		case workflow.StageCypherComplete:
			stepID := cypherStepIDs[progress.Cypher]
			status := "completed"
			if progress.CypherError != "" {
				status = "error"
			}
			steps = append(steps, stepRecord{
				ID: stepID, Type: "cypher_query", Question: progress.CypherQuestion,
				Cypher: progress.Cypher, Status: status, Count: progress.CypherRows,
				Error: progress.CypherError, Env: input.Env,
			})
		case workflow.StageReadDocsStarted:
			docsStepIDs[progress.DocsPage] = uuid.New().String()
		case workflow.StageReadDocsComplete:
			stepID := docsStepIDs[progress.DocsPage]
			status := "completed"
			if progress.DocsError != "" {
				status = "error"
			}
			steps = append(steps, stepRecord{
				ID: stepID, Type: "read_docs", Page: progress.DocsPage,
				Status: status, Content: progress.DocsContent, Error: progress.DocsError,
			})
		case workflow.StageQueryStarted:
			sqlStepIDs[progress.QuerySQL] = uuid.New().String()
		case workflow.StageQueryComplete:
			stepID := sqlStepIDs[progress.QuerySQL]
			status := "completed"
			if progress.QueryError != "" {
				status = "error"
			}
			steps = append(steps, stepRecord{
				ID: stepID, Type: "sql_query", Question: progress.QueryQuestion,
				SQL: progress.QuerySQL, Status: status, Count: progress.QueryRows,
				Error: progress.QueryError, Env: input.Env,
			})
		}

		// Write steps to PG for SSE polling
		writeStepsToPG(ctx, a.PgPool, workflowRunID, steps)

		// Temporal heartbeat (enables cancellation detection)
		activity.RecordHeartbeat(ctx, len(steps))
	}

	onCheckpoint := func(state *v3.CheckpointState) error {
		lastLLMCalls = state.Metrics.LLMCalls
		lastInputTokens = state.Metrics.InputTokens
		lastOutputTokens = state.Metrics.OutputTokens

		return updateWorkflowCheckpoint(ctx, a.PgPool, workflowRunID, state, steps)
	}

	// Check for existing checkpoint (retry case)
	existingCheckpoint := loadCheckpointFromPG(ctx, a.PgPool, workflowRunID)

	var result *workflow.WorkflowResult
	if existingCheckpoint != nil {
		result, err = wf.ResumeFromCheckpoint(ctx, input.Question, existingCheckpoint, onProgress, onCheckpoint)
	} else {
		result, err = wf.RunWithCheckpoint(ctx, input.Question, input.History, onProgress, onCheckpoint)
	}

	if err != nil {
		if ctx.Err() != nil {
			// Cancelled — mark in PG
			_, _ = a.PgPool.Exec(context.Background(),
				`UPDATE workflow_runs SET status = 'cancelled', completed_at = NOW(), updated_at = NOW() WHERE id = $1`,
				workflowRunID)
			return nil, fmt.Errorf("workflow cancelled: %w", err)
		}
		// Failed — mark in PG
		_, _ = a.PgPool.Exec(context.Background(),
			`UPDATE workflow_runs SET status = 'failed', error = $2, completed_at = NOW(), updated_at = NOW() WHERE id = $1`,
			workflowRunID, err.Error())
		return nil, err
	}

	// Use metrics from last checkpoint if available (they accumulate)
	llmCalls := lastLLMCalls
	inputTokens := lastInputTokens
	outputTokens := lastOutputTokens

	return &ChatWorkflowOutput{
		Answer:            result.Answer,
		FollowUpQuestions: result.FollowUpQuestions,
		LLMCalls:          llmCalls,
		InputTokens:       inputTokens,
		OutputTokens:      outputTokens,
	}, nil
}

// FinalizeChat marks the workflow complete and updates the session.
func (a *Activities) FinalizeChat(ctx context.Context, input FinalizeInput) error {
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return fmt.Errorf("invalid workflow run ID: %w", err)
	}

	// Marshal follow-up questions
	followUpJSON, err := json.Marshal(input.Result.FollowUpQuestions)
	if err != nil {
		followUpJSON = []byte("[]")
	}

	// Mark workflow_runs as completed
	_, err = a.PgPool.Exec(ctx, `
		UPDATE workflow_runs
		SET status = 'completed', final_answer = $2, follow_up_questions = $3, completed_at = NOW(), updated_at = NOW(),
		    llm_calls = $4, input_tokens = $5, output_tokens = $6
		WHERE id = $1
	`, workflowRunID, input.Result.Answer, followUpJSON, input.Result.LLMCalls, input.Result.InputTokens, input.Result.OutputTokens)
	if err != nil {
		return fmt.Errorf("failed to complete workflow run: %w", err)
	}

	// Update session content so reloading the page shows the conversation
	sessionID, err := uuid.Parse(input.SessionID)
	if err != nil {
		return fmt.Errorf("invalid session ID: %w", err)
	}

	// Read current session content
	var content json.RawMessage
	err = a.PgPool.QueryRow(ctx, `SELECT content FROM sessions WHERE id = $1`, sessionID).Scan(&content)
	if err != nil {
		slog.Warn("Failed to read session content for finalization", "session_id", input.SessionID, "error", err)
		return nil // non-fatal
	}

	var messages []json.RawMessage
	if err := json.Unmarshal(content, &messages); err != nil {
		messages = nil
	}

	// Filter out any streaming placeholder for this workflow
	var filtered []json.RawMessage
	for _, msg := range messages {
		var m map[string]any
		if err := json.Unmarshal(msg, &m); err == nil {
			if m["status"] == "streaming" && m["workflowId"] == workflowRunID.String() {
				continue
			}
		}
		filtered = append(filtered, msg)
	}

	// Read steps from workflow_runs for the workflow data
	var stepsJSON json.RawMessage
	_ = a.PgPool.QueryRow(ctx, `SELECT steps FROM workflow_runs WHERE id = $1`, workflowRunID).Scan(&stepsJSON)

	// Build workflow data for the assistant message
	workflowData := map[string]any{
		"followUpQuestions": input.Result.FollowUpQuestions,
	}
	if stepsJSON != nil {
		var steps []json.RawMessage
		if json.Unmarshal(stepsJSON, &steps) == nil {
			workflowData["processingSteps"] = steps
		}
	}

	// Add user message
	userMsg, _ := json.Marshal(map[string]any{
		"id":      uuid.NewString(),
		"role":    "user",
		"content": input.Question,
		"env":     input.Env,
	})
	filtered = append(filtered, userMsg)

	// Add assistant message
	assistantMsg, _ := json.Marshal(map[string]any{
		"id":           uuid.NewString(),
		"role":         "assistant",
		"content":      input.Result.Answer,
		"status":       "complete",
		"workflowId":   workflowRunID.String(),
		"workflowData": workflowData,
	})
	filtered = append(filtered, assistantMsg)

	updatedContent, _ := json.Marshal(filtered)
	_, err = a.PgPool.Exec(ctx, `UPDATE sessions SET content = $2, updated_at = NOW() WHERE id = $1`, sessionID, updatedContent)
	if err != nil {
		slog.Warn("Failed to update session content", "session_id", input.SessionID, "error", err)
	}

	return nil
}

// stepRecord is the step format written to workflow_runs.steps in PG.
type stepRecord struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Content  string `json:"content,omitempty"`
	Question string `json:"question,omitempty"`
	SQL      string `json:"sql,omitempty"`
	Cypher   string `json:"cypher,omitempty"`
	Status   string `json:"status,omitempty"`
	Count    int    `json:"count,omitempty"`
	Error    string `json:"error,omitempty"`
	Page     string `json:"page,omitempty"`
	Env      string `json:"env,omitempty"`
}

func writeStepsToPG(ctx context.Context, pool *pgxpool.Pool, workflowRunID uuid.UUID, steps []stepRecord) {
	stepsJSON, err := json.Marshal(steps)
	if err != nil {
		slog.Warn("Failed to marshal steps", "error", err)
		return
	}
	_, err = pool.Exec(ctx,
		`UPDATE workflow_runs SET steps = $2, updated_at = NOW() WHERE id = $1`,
		workflowRunID, stepsJSON)
	if err != nil {
		slog.Warn("Failed to write steps to PG", "error", err)
	}
}

func updateWorkflowCheckpoint(ctx context.Context, pool *pgxpool.Pool, workflowRunID uuid.UUID, state *v3.CheckpointState, steps []stepRecord) error {
	messagesJSON, err := json.Marshal(state.Messages)
	if err != nil {
		return fmt.Errorf("failed to marshal messages: %w", err)
	}
	thinkingJSON, err := json.Marshal(state.ThinkingSteps)
	if err != nil {
		return fmt.Errorf("failed to marshal thinking steps: %w", err)
	}
	queriesJSON, err := json.Marshal(state.ExecutedQueries)
	if err != nil {
		return fmt.Errorf("failed to marshal executed queries: %w", err)
	}
	stepsJSON, err := json.Marshal(steps)
	if err != nil {
		return fmt.Errorf("failed to marshal steps: %w", err)
	}

	_, err = pool.Exec(ctx, `
		UPDATE workflow_runs
		SET iteration = $2, messages = $3, thinking_steps = $4, executed_queries = $5, steps = $6,
		    llm_calls = $7, input_tokens = $8, output_tokens = $9, updated_at = NOW()
		WHERE id = $1
	`, workflowRunID, state.Iteration, messagesJSON, thinkingJSON, queriesJSON, stepsJSON,
		state.Metrics.LLMCalls, state.Metrics.InputTokens, state.Metrics.OutputTokens)
	if err != nil {
		return fmt.Errorf("failed to update workflow checkpoint: %w", err)
	}
	return nil
}

func loadCheckpointFromPG(ctx context.Context, pool *pgxpool.Pool, workflowRunID uuid.UUID) *v3.CheckpointState {
	var iteration int
	var messagesJSON, thinkingJSON, queriesJSON json.RawMessage
	var llmCalls, inputTokens, outputTokens int

	err := pool.QueryRow(ctx, `
		SELECT iteration, messages, thinking_steps, executed_queries, llm_calls, input_tokens, output_tokens
		FROM workflow_runs WHERE id = $1 AND iteration > 0
	`, workflowRunID).Scan(&iteration, &messagesJSON, &thinkingJSON, &queriesJSON, &llmCalls, &inputTokens, &outputTokens)
	if err != nil {
		return nil
	}

	var messages []workflow.ToolMessage
	if err := json.Unmarshal(messagesJSON, &messages); err != nil {
		return nil
	}

	var thinkingSteps []string
	if err := json.Unmarshal(thinkingJSON, &thinkingSteps); err != nil {
		return nil
	}

	var executedQueries []workflow.ExecutedQuery
	if err := json.Unmarshal(queriesJSON, &executedQueries); err != nil {
		return nil
	}

	return &v3.CheckpointState{
		Iteration:       iteration,
		Messages:        messages,
		ThinkingSteps:   thinkingSteps,
		ExecutedQueries: executedQueries,
		Metrics: &v3.WorkflowMetrics{
			LLMCalls:     llmCalls,
			InputTokens:  inputTokens,
			OutputTokens: outputTokens,
		},
	}
}

func buildEnvContext(env string, mainnetDB string) string {
	if env == "mainnet-beta" {
		return fmt.Sprintf("You are querying the mainnet-beta environment (database: `%s`). Other DZ environments are available: devnet (`lake_devnet`), testnet (`lake_testnet`). To query these, use fully-qualified `database.table` syntax (e.g., `lake_devnet.dim_devices_current`).", mainnetDB)
	}

	envDB := "lake_" + env
	return fmt.Sprintf(`The user is viewing the %s environment. You MUST prefix all table names with the database name "%s." to query %s data.

Example: Instead of "SELECT * FROM dim_devices_current", write "SELECT * FROM %s.dim_devices_current"

Queries without the "%s." prefix will return mainnet-beta data. This is incorrect UNLESS the user explicitly asks for mainnet or mainnet-beta data.

Note: Neo4j graph queries, Solana validator data, and GeoIP location data are only available on mainnet-beta.`, env, envDB, env, envDB, envDB)
}
