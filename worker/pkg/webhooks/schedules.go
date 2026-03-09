package webhooks

import (
	"context"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

// EnsureSchedules creates or updates the webhook polling Temporal schedule.
func EnsureSchedules(ctx context.Context, tc client.Client) error {
	sc := tc.ScheduleClient()
	scheduleID := "webhook-poll"

	handle := sc.GetHandle(ctx, scheduleID)

	err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Spec = &client.ScheduleSpec{
				Intervals: []client.ScheduleIntervalSpec{
					{Every: 1 * time.Minute},
				},
			}
			input.Description.Schedule.Action = &client.ScheduleWorkflowAction{
				ID:        scheduleID + "-run",
				Workflow:  PollAndNotifyWorkflow,
				TaskQueue: TaskQueue,
			}
			return &client.ScheduleUpdate{
				Schedule: &input.Description.Schedule,
			}, nil
		},
	})

	if err != nil {
		_, err = sc.Create(ctx, client.ScheduleOptions{
			ID: scheduleID,
			Spec: client.ScheduleSpec{
				Intervals: []client.ScheduleIntervalSpec{
					{Every: 1 * time.Minute},
				},
			},
			Action: &client.ScheduleWorkflowAction{
				ID:                 scheduleID + "-run",
				Workflow:           PollAndNotifyWorkflow,
				TaskQueue:          TaskQueue,
				WorkflowRunTimeout: 5 * time.Minute,
			},
		})
		if err != nil {
			return err
		}
		log.Printf("Created Temporal schedule: %s (every 1m)", scheduleID)
	} else {
		log.Printf("Updated Temporal schedule: %s (every 1m)", scheduleID)
	}

	return nil
}
