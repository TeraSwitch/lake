package chat

import (
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// ChatWorkflow orchestrates a chat agent run as a Temporal workflow.
func ChatWorkflow(ctx temporalworkflow.Context, input ChatWorkflowInput) (*ChatWorkflowOutput, error) {
	// Activity 1: Run agent (long timeout, retry from checkpoint)
	agentCtx := temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})
	var result ChatWorkflowOutput
	if err := temporalworkflow.ExecuteActivity(agentCtx, (*Activities).RunAgent, input).Get(agentCtx, &result); err != nil {
		return nil, err
	}

	// Activity 2: Finalize (update session, mark complete)
	fastCtx := temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})
	finalizeInput := FinalizeInput{
		WorkflowRunID: input.WorkflowRunID,
		SessionID:     input.SessionID,
		Question:      input.Question,
		Env:           input.Env,
		Result:        result,
	}
	_ = temporalworkflow.ExecuteActivity(fastCtx, (*Activities).FinalizeChat, finalizeInput).Get(fastCtx, nil)

	return &result, nil
}
