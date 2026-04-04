package incidents

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckRollupFreshness(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	ctx := context.Background()

	t.Run("empty ingestion logs returns zero freshness", func(t *testing.T) {
		freshness, err := a.CheckRollupFreshness(ctx)
		require.NoError(t, err)
		assert.True(t, freshness.LatencyFreshUntil.IsZero())
		assert.True(t, freshness.TrafficFreshUntil.IsZero())
		assert.True(t, freshness.LatestBucket.IsZero())
	})

	t.Run("returns per-pipeline freshness from ingestion logs", func(t *testing.T) {
		latencyTS := time.Now().UTC().Truncate(time.Millisecond).Add(-10 * time.Minute)
		trafficTS := time.Now().UTC().Truncate(time.Millisecond).Add(-5 * time.Minute)

		// Seed ingestion log entries for both rollup activities.
		err := conn.Exec(ctx, `INSERT INTO log_ingestion_runs
			(run_id, workflow, activity, network, status, started_at, finished_at, duration_ms, source_min_event_ts, source_max_event_ts)
			VALUES (generateUUIDv4(), 'rollup', 'RollupLinks', 'mainnet-beta', 'success', now(), now(), 100, $1, $1)`,
			latencyTS)
		require.NoError(t, err)

		err = conn.Exec(ctx, `INSERT INTO log_ingestion_runs
			(run_id, workflow, activity, network, status, started_at, finished_at, duration_ms, source_min_event_ts, source_max_event_ts)
			VALUES (generateUUIDv4(), 'rollup', 'RollupDeviceInterfaces', 'mainnet-beta', 'success', now(), now(), 100, $1, $1)`,
			trafficTS)
		require.NoError(t, err)

		freshness, err := a.CheckRollupFreshness(ctx)
		require.NoError(t, err)

		// Allow 1-second tolerance for ClickHouse rounding.
		assert.WithinDuration(t, latencyTS, freshness.LatencyFreshUntil, time.Second)
		assert.WithinDuration(t, trafficTS, freshness.TrafficFreshUntil, time.Second)
		assert.WithinDuration(t, trafficTS, freshness.LatestBucket, time.Second, "LatestBucket should be max of both")
	})

	t.Run("ignores failed runs", func(t *testing.T) {
		// Insert a failed run with a newer timestamp — should be ignored.
		err := conn.Exec(ctx, `INSERT INTO log_ingestion_runs
			(run_id, workflow, activity, network, status, started_at, finished_at, duration_ms, source_min_event_ts, source_max_event_ts)
			VALUES (generateUUIDv4(), 'rollup', 'RollupLinks', 'mainnet-beta', 'error', now(), now(), 100, now(), now())`)
		require.NoError(t, err)

		freshness, err := a.CheckRollupFreshness(ctx)
		require.NoError(t, err)

		// Should still match the previous successful run, not the failed one.
		assert.True(t, freshness.LatencyFreshUntil.Before(time.Now().Add(-1*time.Minute)),
			"should not pick up the failed run's timestamp")
	})
}

func TestSymptomSeverity(t *testing.T) {
	t.Parallel()
	threshold := 30 * time.Minute
	short := 10 * time.Minute
	long := 45 * time.Minute

	tests := []struct {
		name     string
		symptom  string
		peak     float64
		duration time.Duration
		expected Severity
	}{
		{"isis_down short", SymptomISISDown, 1, short, SeverityCritical},
		{"isis_down long", SymptomISISDown, 1, long, SeverityCritical},
		{"high loss short", SymptomPacketLoss, 15, short, SeverityWarning},
		{"high loss long", SymptomPacketLoss, 15, long, SeverityCritical},
		{"low loss short", SymptomPacketLoss, 5, short, SeverityWarning},
		{"low loss long", SymptomPacketLoss, 5, long, SeverityWarning},
		{"carrier short", SymptomCarrier, 3, short, SeverityWarning},
		{"carrier long", SymptomCarrier, 3, long, SeverityCritical},
		{"errors short", SymptomErrors, 10, short, SeverityWarning},
		{"errors long", SymptomErrors, 10, long, SeverityWarning},
		{"discards long", SymptomDiscards, 10, long, SeverityWarning},
		{"no_latency_data short", SymptomNoLatencyData, 1, short, SeverityWarning},
		{"no_latency_data long", SymptomNoLatencyData, 1, long, SeverityCritical},
		{"no_traffic_data short", SymptomNoTrafficData, 1, short, SeverityWarning},
		{"no_traffic_data long", SymptomNoTrafficData, 1, long, SeverityCritical},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := symptomSeverity(tt.symptom, tt.peak, tt.duration, threshold)
			assert.Equal(t, tt.expected, got)
		})
	}
}
