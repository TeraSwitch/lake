package statuscache

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// RegisterWorkflows registers all status cache workflows with the given worker.
func RegisterWorkflows(w worker.Worker) {
	w.RegisterWorkflow(RefreshStatusWorkflow)
	w.RegisterWorkflow(RefreshTimelineWorkflow)
	w.RegisterWorkflow(RefreshOutagesWorkflow)
	w.RegisterWorkflow(RefreshLinkHistoryWorkflow)
	w.RegisterWorkflow(RefreshDeviceHistoryWorkflow)
	w.RegisterWorkflow(RefreshLatencyComparisonWorkflow)
	w.RegisterWorkflow(RefreshMetroPathLatencyWorkflow)
	w.RegisterWorkflow(CleanupWorkflow)
}

var defaultOpts = temporalworkflow.ActivityOptions{
	StartToCloseTimeout: 60 * time.Second,
	RetryPolicy: &temporal.RetryPolicy{
		MaximumAttempts: 2,
	},
}

func RefreshStatusWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, defaultOpts)
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshStatus).Get(ctx, nil)
}

func RefreshTimelineWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, defaultOpts)
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshTimeline).Get(ctx, nil)
}

func RefreshOutagesWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, defaultOpts)
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshOutages).Get(ctx, nil)
}

func RefreshLinkHistoryWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, defaultOpts)
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshLinkHistory).Get(ctx, nil)
}

func RefreshDeviceHistoryWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, defaultOpts)
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshDeviceHistory).Get(ctx, nil)
}

func RefreshLatencyComparisonWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 120 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshLatencyComparison).Get(ctx, nil)
}

func RefreshMetroPathLatencyWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 180 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RefreshMetroPathLatency).Get(ctx, nil)
}

func CleanupWorkflow(ctx temporalworkflow.Context) error {
	ctx = temporalworkflow.WithActivityOptions(ctx, defaultOpts)
	return temporalworkflow.ExecuteActivity(ctx, (*Activities).RunCleanup).Get(ctx, nil)
}
