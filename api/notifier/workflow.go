package notifier

import (
	"context"
	"log/slog"
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const (
	TaskQueue  = "api-notifier"
	WorkflowID = "api-notifier"

	pollInterval           = 30 * time.Second
	continueAsNewThreshold = 60 // ~30 min at 30s intervals
)

// Activities holds dependencies for the notifier activity.
type Activities struct {
	Log      *slog.Logger
	Store    *ConfigStore
	Sources  map[string]Source
	Channels map[string]Channel
}

// CheckAndNotify polls all sources for enabled configs and delivers notifications.
func (a *Activities) CheckAndNotify(ctx context.Context) error {
	configs, err := a.Store.ListEnabled(ctx)
	if err != nil {
		a.Log.Error("failed to list enabled configs", "error", err)
		return nil // don't fail the workflow
	}

	// Group configs by account+source so we poll each source once per account.
	type pollKey struct {
		accountID  string
		sourceType string
	}
	type delivery struct {
		config NotificationConfig
		groups []EventGroup
	}

	polled := make(map[pollKey][]EventGroup)
	var deliveries []delivery

	for _, cfg := range configs {
		source, ok := a.Sources[cfg.SourceType]
		if !ok {
			a.Log.Warn("unknown source type", "source_type", cfg.SourceType, "config_id", cfg.ID)
			continue
		}

		pk := pollKey{accountID: cfg.AccountID, sourceType: cfg.SourceType}

		groups, exists := polled[pk]
		if !exists {
			cp, err := a.Store.GetCheckpoint(ctx, cfg.AccountID, cfg.SourceType)
			if err != nil {
				a.Log.Error("failed to get checkpoint", "error", err, "config_id", cfg.ID)
				continue
			}

			var newCP Checkpoint
			groups, newCP, err = source.Poll(ctx, cp)
			if err != nil {
				a.Log.Error("source poll failed", "error", err, "source_type", cfg.SourceType, "account_id", cfg.AccountID)
				continue
			}

			polled[pk] = groups

			// Save checkpoint even if no events (advances the high-water mark).
			if newCP.LastEventTS.After(cp.LastEventTS) || newCP.LastSlot > cp.LastSlot {
				if err := a.Store.SaveCheckpoint(ctx, cfg.AccountID, cfg.SourceType, newCP); err != nil {
					a.Log.Error("failed to save checkpoint", "error", err, "config_id", cfg.ID)
				}
			}
		}

		if len(groups) > 0 {
			// Apply source-specific filters to get the relevant groups for this config.
			filtered := source.Filter(groups, cfg.Filters)
			if len(filtered) > 0 {
				deliveries = append(deliveries, delivery{config: cfg, groups: filtered})
			}
		}
	}

	// Deliver notifications.
	for _, d := range deliveries {
		ch, ok := a.Channels[d.config.ChannelType]
		if !ok {
			a.Log.Warn("unknown channel type", "channel_type", d.config.ChannelType, "config_id", d.config.ID)
			continue
		}

		if err := ch.Send(ctx, d.config.Destination, d.groups); err != nil {
			a.Log.Error("notification delivery failed",
				"error", err,
				"channel_type", d.config.ChannelType,
				"config_id", d.config.ID,
			)
		}
	}

	return nil
}

// NotifierWorkflow is a long-running workflow that polls for new events and
// delivers notifications. Uses continue-as-new after 60 iterations (~30 min).
func NotifierWorkflow(ctx temporalworkflow.Context, iteration int) error {
	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		_ = temporalworkflow.ExecuteActivity(ctx, (*Activities).CheckAndNotify).Get(ctx, nil)

		iteration++
		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, NotifierWorkflow, 0)
}
