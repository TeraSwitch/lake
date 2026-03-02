package pagecache

import (
	"context"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

type scheduleSpec struct {
	ID       string
	Workflow any
	Interval time.Duration
}

// EnsureSchedules creates or updates all page cache Temporal schedules.
func EnsureSchedules(ctx context.Context, tc client.Client) error {
	sc := tc.ScheduleClient()
	schedules := []scheduleSpec{
		{"status-cache-refresh", RefreshStatusWorkflow, 2 * time.Minute},
		{"timeline-cache-refresh", RefreshTimelineWorkflow, 2 * time.Minute},
		{"outages-cache-refresh", RefreshOutagesWorkflow, 5 * time.Minute},
		{"link-history-cache-refresh", RefreshLinkHistoryWorkflow, 5 * time.Minute},
		{"device-history-cache-refresh", RefreshDeviceHistoryWorkflow, 5 * time.Minute},
		{"latency-cache-refresh", RefreshLatencyComparisonWorkflow, 5 * time.Minute},
		{"metro-path-cache-refresh", RefreshMetroPathLatencyWorkflow, 5 * time.Minute},
		{"db-cleanup", CleanupWorkflow, 1 * time.Hour},
	}

	for _, s := range schedules {
		handle := sc.GetHandle(ctx, s.ID)

		// Try to update existing schedule
		err := handle.Update(ctx, client.ScheduleUpdateOptions{
			DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
				input.Description.Schedule.Spec = &client.ScheduleSpec{
					Intervals: []client.ScheduleIntervalSpec{
						{Every: s.Interval},
					},
				}
				input.Description.Schedule.Action = &client.ScheduleWorkflowAction{
					ID:        s.ID + "-run",
					Workflow:  s.Workflow,
					TaskQueue: TaskQueue,
				}
				return &client.ScheduleUpdate{
					Schedule: &input.Description.Schedule,
				}, nil
			},
		})

		if err != nil {
			// Schedule doesn't exist — create it
			_, err = sc.Create(ctx, client.ScheduleOptions{
				ID: s.ID,
				Spec: client.ScheduleSpec{
					Intervals: []client.ScheduleIntervalSpec{
						{Every: s.Interval},
					},
				},
				Action: &client.ScheduleWorkflowAction{
					ID:                 s.ID + "-run",
					Workflow:           s.Workflow,
					TaskQueue:          TaskQueue,
					WorkflowRunTimeout: 5 * time.Minute,
				},
			})
			if err != nil {
				return err
			}
			log.Printf("Created Temporal schedule: %s (every %v)", s.ID, s.Interval)
		} else {
			log.Printf("Updated Temporal schedule: %s (every %v)", s.ID, s.Interval)
		}
	}

	return nil
}
