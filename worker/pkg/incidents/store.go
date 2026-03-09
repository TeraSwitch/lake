package incidents

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// QueryPreviousIncidents reads the current state from the incidents table.
func QueryPreviousIncidents(ctx context.Context, conn driver.Conn) ([]Incident, error) {
	query := `
		SELECT
			entity_type, entity_pk, incident_type, started_at,
			ended_at, is_ongoing, confirmed, severity, is_drained,
			entity_code, link_type, side_a_metro, side_z_metro,
			contributor_code, metro, device_type, drain_status,
			threshold_pct, peak_loss_pct, threshold_count, peak_count,
			affected_interfaces, duration_seconds, updated_at
		FROM incidents FINAL
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query incidents: %w", err)
	}
	defer rows.Close()

	var incidents []Incident
	for rows.Next() {
		var inc Incident
		if err := rows.Scan(
			&inc.EntityType, &inc.EntityPK, &inc.IncidentType, &inc.StartedAt,
			&inc.EndedAt, &inc.IsOngoing, &inc.Confirmed, &inc.Severity, &inc.IsDrained,
			&inc.EntityCode, &inc.LinkType, &inc.SideAMetro, &inc.SideZMetro,
			&inc.ContributorCode, &inc.Metro, &inc.DeviceType, &inc.DrainStatus,
			&inc.ThresholdPct, &inc.PeakLossPct, &inc.ThresholdCount, &inc.PeakCount,
			&inc.AffectedInterfaces, &inc.DurationSeconds, &inc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		incidents = append(incidents, inc)
	}
	return incidents, nil
}

// UpsertIncidents batch inserts/updates incidents into the incidents table.
// ReplacingMergeTree deduplicates by the ORDER BY key using updated_at as version.
func UpsertIncidents(ctx context.Context, conn driver.Conn, incidents []Incident) error {
	if len(incidents) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO incidents (
			entity_type, entity_pk, incident_type, started_at,
			ended_at, is_ongoing, confirmed, severity, is_drained,
			entity_code, link_type, side_a_metro, side_z_metro,
			contributor_code, metro, device_type, drain_status,
			threshold_pct, peak_loss_pct, threshold_count, peak_count,
			affected_interfaces, duration_seconds, updated_at
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	now := time.Now()
	for _, inc := range incidents {
		if err := batch.Append(
			inc.EntityType, inc.EntityPK, inc.IncidentType, inc.StartedAt,
			inc.EndedAt, inc.IsOngoing, inc.Confirmed, inc.Severity, inc.IsDrained,
			inc.EntityCode, inc.LinkType, inc.SideAMetro, inc.SideZMetro,
			inc.ContributorCode, inc.Metro, inc.DeviceType, inc.DrainStatus,
			inc.ThresholdPct, inc.PeakLossPct, inc.ThresholdCount, inc.PeakCount,
			inc.AffectedInterfaces, inc.DurationSeconds, now,
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}

	return batch.Send()
}

// InsertEvents batch inserts incident events into the incident_events table.
// ReplacingMergeTree deduplicates by event_id.
func InsertEvents(ctx context.Context, conn driver.Conn, events []IncidentEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := conn.PrepareBatch(ctx, `
		INSERT INTO incident_events (
			event_id, event_type, event_ts,
			entity_type, entity_pk, entity_code,
			incident_type, severity, old_severity,
			drain_status, old_drain_status, readiness, old_readiness,
			link_type, side_a_metro, side_z_metro, contributor_code,
			metro, device_type,
			threshold_pct, peak_loss_pct, threshold_count, peak_count,
			incident_started_at, incident_ended_at, duration_seconds,
			payload
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, evt := range events {
		if err := batch.Append(
			evt.EventID, evt.EventType, evt.EventTS,
			evt.EntityType, evt.EntityPK, evt.EntityCode,
			evt.IncidentType, evt.Severity, evt.OldSeverity,
			evt.DrainStatus, evt.OldDrainStatus, evt.Readiness, evt.OldReadiness,
			evt.LinkType, evt.SideAMetro, evt.SideZMetro, evt.ContributorCode,
			evt.Metro, evt.DeviceType,
			evt.ThresholdPct, evt.PeakLossPct, evt.ThresholdCount, evt.PeakCount,
			evt.IncidentStartedAt, evt.IncidentEndedAt, evt.DurationSeconds,
			evt.Payload,
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}

	return batch.Send()
}
