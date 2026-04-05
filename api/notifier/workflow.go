package notifier

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// extractEndpointURL resolves the delivery URL from the endpoint type and config.
func extractEndpointURL(endpointType string, config json.RawMessage) string {
	switch endpointType {
	case EndpointTypeWebhook:
		var c struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(config, &c) == nil {
			return c.URL
		}
	}
	return ""
}

const (
	TaskQueue  = "api-notifier"
	WorkflowID = "api-notifier"

	pollInterval           = 30 * time.Second
	continueAsNewThreshold = 60 // ~30 min at 30s intervals
)

// Activities holds dependencies for the notifier activity.
type Activities struct {
	Log     *slog.Logger
	Store   *ConfigStore
	Sources map[string]Source
}

// CheckAndDeliver polls all sources for enabled configs and delivers via webhooks.
func (a *Activities) CheckAndDeliver(ctx context.Context) error {
	configs, err := a.Store.ListEnabledWithEndpoints(ctx)
	if err != nil {
		a.Log.Error("failed to list enabled configs", "error", err)
		return nil
	}

	type pollKey struct {
		accountID  string
		sourceType string
	}
	type delivery struct {
		config ConfigWithEndpoint
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

			if newCP.LastEventTS.After(cp.LastEventTS) || newCP.LastSlot > cp.LastSlot {
				if err := a.Store.SaveCheckpoint(ctx, cfg.AccountID, cfg.SourceType, newCP); err != nil {
					a.Log.Error("failed to save checkpoint", "error", err, "config_id", cfg.ID)
				}
			}
		}

		if len(groups) > 0 {
			filtered := source.Filter(groups, cfg.Filters)
			if len(filtered) > 0 {
				deliveries = append(deliveries, delivery{config: cfg, groups: filtered})
			}
		}
	}

	for _, d := range deliveries {
		format := d.config.OutputFormat
		if format == "" {
			format = FormatMarkdown
		}
		url := extractEndpointURL(d.config.EndpointType, d.config.EndpointConfig)
		if url == "" {
			a.Log.Warn("endpoint has no delivery URL", "config_id", d.config.ID, "endpoint_type", d.config.EndpointType)
			continue
		}
		if err := Deliver(ctx, url, d.groups, format); err != nil {
			a.Log.Error("delivery failed",
				"error", err,
				"endpoint_type", d.config.EndpointType,
				"config_id", d.config.ID,
			)
		}
	}

	return nil
}

// NotifierWorkflow is a long-running workflow that polls for new events and
// delivers via webhooks. Uses continue-as-new after 60 iterations (~30 min).
func NotifierWorkflow(ctx temporalworkflow.Context, iteration int) error {
	actOpts := temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = temporalworkflow.WithActivityOptions(ctx, actOpts)

	for iteration < continueAsNewThreshold {
		_ = temporalworkflow.ExecuteActivity(ctx, (*Activities).CheckAndDeliver).Get(ctx, nil)

		iteration++
		if iteration < continueAsNewThreshold {
			if err := temporalworkflow.Sleep(ctx, pollInterval); err != nil {
				return err
			}
		}
	}

	return temporalworkflow.NewContinueAsNewError(ctx, NotifierWorkflow, 0)
}
