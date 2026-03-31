package incidents

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Activities holds dependencies for incident detection activities.
type Activities struct {
	ClickHouse          driver.Conn
	Log                 *slog.Logger
	CoalesceGap         time.Duration
	EscalationThreshold time.Duration
}

// DeriveWatermark returns the latest event timestamp across both incident
// event tables. Returns zero time if no events exist (cold start).
func (a *Activities) DeriveWatermark(ctx context.Context) (time.Time, error) {
	var linkMax, deviceMax time.Time

	row := a.ClickHouse.QueryRow(ctx, `SELECT max(event_ts) FROM link_incident_events`)
	if err := row.Scan(&linkMax); err != nil {
		return time.Time{}, fmt.Errorf("link watermark: %w", err)
	}

	row = a.ClickHouse.QueryRow(ctx, `SELECT max(event_ts) FROM device_incident_events`)
	if err := row.Scan(&deviceMax); err != nil {
		return time.Time{}, fmt.Errorf("device watermark: %w", err)
	}

	if deviceMax.After(linkMax) {
		return deviceMax, nil
	}
	return linkMax, nil
}

// CheckRollupFreshness returns the latest bucket timestamp across rollup
// tables. The query is bounded to the last 25 hours for performance.
func (a *Activities) CheckRollupFreshness(ctx context.Context) (RollupFreshness, error) {
	var latencyMax, trafficMax time.Time

	row := a.ClickHouse.QueryRow(ctx,
		`SELECT max(bucket_ts) FROM link_rollup_5m WHERE bucket_ts >= now() - INTERVAL 25 HOUR`)
	if err := row.Scan(&latencyMax); err != nil {
		return RollupFreshness{}, fmt.Errorf("latency freshness: %w", err)
	}

	row = a.ClickHouse.QueryRow(ctx,
		`SELECT max(bucket_ts) FROM device_interface_rollup_5m WHERE bucket_ts >= now() - INTERVAL 25 HOUR`)
	if err := row.Scan(&trafficMax); err != nil {
		return RollupFreshness{}, fmt.Errorf("traffic freshness: %w", err)
	}

	latest := latencyMax
	if trafficMax.After(latest) {
		latest = trafficMax
	}

	return RollupFreshness{LatestBucket: latest}, nil
}

// --- Event writers ---

func (a *Activities) writeLinkEvents(ctx context.Context, events []LinkIncidentEvent) error {
	batch, err := a.ClickHouse.PrepareBatch(ctx, `
		INSERT INTO link_incident_events (
			incident_id, link_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			link_code, link_type, side_a_metro, side_z_metro,
			contributor_code, status, provisioning
		)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.IncidentID, e.LinkPK, string(e.EventType), e.EventTS, e.StartedAt,
			e.ActiveSymptoms, e.Symptoms, string(e.Severity), e.PeakValues,
			e.LinkCode, e.LinkType, e.SideAMetro, e.SideZMetro,
			e.ContributorCode, e.Status, e.Provisioning,
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}

	return batch.Send()
}

func (a *Activities) writeDeviceEvents(ctx context.Context, events []DeviceIncidentEvent) error {
	batch, err := a.ClickHouse.PrepareBatch(ctx, `
		INSERT INTO device_incident_events (
			incident_id, device_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			device_code, device_type, metro, contributor_code, status
		)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.IncidentID, e.DevicePK, string(e.EventType), e.EventTS, e.StartedAt,
			e.ActiveSymptoms, e.Symptoms, string(e.Severity), e.PeakValues,
			e.DeviceCode, e.DeviceType, e.Metro, e.ContributorCode, e.Status,
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}

	return batch.Send()
}

// --- Severity ---

func (a *Activities) computeSeverity(symptoms []string, peakValues map[string]float64, startedAt, now time.Time) Severity {
	duration := now.Sub(startedAt)
	severities := make([]Severity, 0, len(symptoms))
	for _, sym := range symptoms {
		severities = append(severities, symptomSeverity(sym, peakValues[sym], duration, a.EscalationThreshold))
	}
	return maxSeverity(severities...)
}

// --- Helpers ---

func marshalPeakValues(m map[string]float64) string {
	data, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(data)
}
