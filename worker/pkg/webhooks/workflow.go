package webhooks

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// RegisterWorkflows registers all webhook workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(PollAndNotifyWorkflow)
	w.RegisterWorkflow(DeliverWebhookWorkflow)
}

// PollAndNotifyWorkflow polls for new incident events and fans out webhook deliveries.
func PollAndNotifyWorkflow(ctx temporalworkflow.Context) error {
	pollOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, pollOpts)

	// Step 1: Poll for new events since last cursor
	var events []IncidentEventRow
	if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).PollNewEvents).Get(ctx, &events); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	// Step 2: Fan out deliveries for each event
	fanOutOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, fanOutOpts)

	for _, evt := range events {
		if err := temporalworkflow.ExecuteActivity(ctx, (*Activities).FanOutDeliveries, evt).Get(ctx, nil); err != nil {
			// Log but continue with remaining events
			temporalworkflow.GetLogger(ctx).Error("failed to fan out deliveries", "event_id", evt.EventID, "error", err)
		}
	}

	return nil
}

// DeliverWebhookWorkflow handles a single webhook delivery with retries.
func DeliverWebhookWorkflow(ctx temporalworkflow.Context, input DeliverWebhookInput) error {
	deliverOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    5,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, deliverOpts)

	return temporalworkflow.ExecuteActivity(ctx, (*Activities).DeliverWebhook, input).Get(ctx, nil)
}
