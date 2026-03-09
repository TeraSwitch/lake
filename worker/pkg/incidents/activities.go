package incidents

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.temporal.io/sdk/activity"
)

// Activities holds dependencies for incident detection activities.
type Activities struct {
	ClickHouse driver.Conn
	Log        *slog.Logger
}

// QueryPreviousState reads the current incidents table from ClickHouse.
func (a *Activities) QueryPreviousState(ctx context.Context) ([]Incident, error) {
	a.Log.Info("querying previous incident state")
	incidents, err := QueryPreviousIncidents(ctx, a.ClickHouse)
	if err != nil {
		return nil, err
	}
	a.Log.Info("loaded previous state", "count", len(incidents))
	return incidents, nil
}

// ComputeCurrentState runs detection queries against raw telemetry.
func (a *Activities) ComputeCurrentState(ctx context.Context) ([]Incident, error) {
	a.Log.Info("computing current incident state from telemetry")
	params := DefaultDetectionParams()
	duration := 24 * time.Hour

	var allIncidents []Incident

	linkIncidents, err := DetectLinkIncidents(ctx, a.ClickHouse, duration, params)
	if err != nil {
		a.Log.Error("link detection failed", "error", err)
		return nil, err
	}
	allIncidents = append(allIncidents, linkIncidents...)

	deviceIncidents, err := DetectDeviceIncidents(ctx, a.ClickHouse, duration, params)
	if err != nil {
		a.Log.Error("device detection failed", "error", err)
		return nil, err
	}
	allIncidents = append(allIncidents, deviceIncidents...)

	a.Log.Info("detection complete", "link_incidents", len(linkIncidents), "device_incidents", len(deviceIncidents))
	return allIncidents, nil
}

// DiffAndWriteInput is the input for the DiffAndWrite activity.
type DiffAndWriteInput struct {
	Current  []Incident
	Previous []Incident
}

// DiffAndWrite diffs current vs previous state, upserts incidents, and inserts events.
func (a *Activities) DiffAndWrite(ctx context.Context, input DiffAndWriteInput) error {
	now := time.Now()

	// Diff to produce events (nil previous = first run, no events emitted)
	var events []IncidentEvent
	if input.Previous != nil {
		events = DiffIncidents(input.Current, input.Previous, now)
	}

	// Upsert all current incidents
	if err := UpsertIncidents(ctx, a.ClickHouse, input.Current); err != nil {
		return err
	}
	a.Log.Info("upserted incidents", "count", len(input.Current))

	// Insert events
	if len(events) > 0 {
		if err := InsertEvents(ctx, a.ClickHouse, events); err != nil {
			return err
		}
		a.Log.Info("inserted events", "count", len(events))
		for _, evt := range events {
			a.Log.Info("event", "type", evt.EventType, "entity", evt.EntityCode, "incident_type", ptrStr(evt.IncidentType))
		}
	}

	return nil
}

// BackfillChunkInput configures a single backfill chunk.
type BackfillChunkInput struct {
	WindowStart time.Time
	WindowEnd   time.Time
}

// BackfillChunk detects incidents for a specific time window and upserts them.
// No events are emitted during backfill.
func (a *Activities) BackfillChunk(ctx context.Context, input BackfillChunkInput) error {
	activity.RecordHeartbeat(ctx, input.WindowStart.Format(time.RFC3339))

	duration := input.WindowEnd.Sub(input.WindowStart)
	params := DefaultDetectionParams()

	a.Log.Info("backfilling chunk", "start", input.WindowStart, "end", input.WindowEnd, "duration", duration)

	var allIncidents []Incident

	linkIncidents, err := DetectLinkIncidents(ctx, a.ClickHouse, duration, params)
	if err != nil {
		return err
	}
	allIncidents = append(allIncidents, linkIncidents...)

	deviceIncidents, err := DetectDeviceIncidents(ctx, a.ClickHouse, duration, params)
	if err != nil {
		return err
	}
	allIncidents = append(allIncidents, deviceIncidents...)

	if err := UpsertIncidents(ctx, a.ClickHouse, allIncidents); err != nil {
		return err
	}

	a.Log.Info("backfill chunk complete", "incidents", len(allIncidents))
	return nil
}
