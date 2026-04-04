package incidents

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhousetesting "github.com/malbeclabs/lake/indexer/pkg/clickhouse/testing"
	laketesting "github.com/malbeclabs/lake/utils/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openRawConn(t *testing.T, db *clickhousetesting.DB, database string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{db.Addr()},
		Auth: clickhouse.Auth{
			Database: database,
			Username: db.Username(),
			Password: db.Password(),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func setupTestDB(t *testing.T) clickhouse.Conn {
	t.Helper()
	info := laketesting.NewClientWithInfo(t, sharedDB)
	return openRawConn(t, sharedDB, info.Database)
}

func newTestBackfillActivities(conn clickhouse.Conn) *Activities {
	return &Activities{
		ClickHouse:          conn,
		Log:                 slog.Default(),
		CoalesceGap:         30 * time.Minute,
		EscalationThreshold: 30 * time.Minute,
	}
}

// insertLinkRollup inserts a link rollup row for testing.
func insertLinkRollup(t *testing.T, conn clickhouse.Conn, linkPK string, bucketTS time.Time, aLossPct, zLossPct float64, isisDown bool) {
	t.Helper()
	err := conn.Exec(context.Background(), `
		INSERT INTO link_rollup_5m (
			bucket_ts, link_pk, ingested_at,
			a_avg_rtt_us, a_min_rtt_us, a_p50_rtt_us, a_p90_rtt_us, a_p95_rtt_us, a_p99_rtt_us, a_max_rtt_us, a_loss_pct, a_samples,
			a_avg_jitter_us, a_min_jitter_us, a_p50_jitter_us, a_p90_jitter_us, a_p95_jitter_us, a_p99_jitter_us, a_max_jitter_us,
			z_avg_rtt_us, z_min_rtt_us, z_p50_rtt_us, z_p90_rtt_us, z_p95_rtt_us, z_p99_rtt_us, z_max_rtt_us, z_loss_pct, z_samples,
			z_avg_jitter_us, z_min_jitter_us, z_p50_jitter_us, z_p90_jitter_us, z_p95_jitter_us, z_p99_jitter_us, z_max_jitter_us,
			status, provisioning, isis_down
		) VALUES (
			$1, $2, now(),
			100, 50, 90, 120, 150, 200, 250, $3, 100,
			10, 5, 8, 12, 15, 20, 25,
			110, 60, 100, 130, 160, 210, 260, $4, 100,
			10, 5, 8, 12, 15, 20, 25,
			'activated', false, $5
		)`,
		bucketTS, linkPK, aLossPct, zLossPct, isisDown,
	)
	require.NoError(t, err)
}

// insertLinkDimension inserts a link dimension row for metadata enrichment.
// Writes to dim_dz_links_history which backs the dz_links_current view.
func insertLinkDimension(t *testing.T, conn clickhouse.Conn, pk, code, linkType string) {
	t.Helper()
	err := conn.Exec(context.Background(), `
		INSERT INTO dim_dz_links_history (
			entity_id, pk, status, code, link_type, committed_rtt_ns, committed_jitter_ns,
			bandwidth_bps, isis_delay_override_ns, side_a_pk, side_z_pk, contributor_pk,
			snapshot_ts, ingested_at, op_id, is_deleted
		) VALUES (
			$1, $1, 'activated', $2, $3, 500000000, 0,
			10000000000, 0, '', '', '',
			now(), now(), generateUUIDv4(), 0
		)`,
		pk, code, linkType,
	)
	require.NoError(t, err)
}

// queryLinkEvents returns all link incident events ordered by started_at, event_ts.
func queryLinkEvents(t *testing.T, conn clickhouse.Conn) []LinkIncidentEvent {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT incident_id, link_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			link_code, link_type, side_a_metro, side_z_metro,
			contributor_code, status, provisioning
		FROM link_incident_events ORDER BY started_at, event_ts
	`)
	require.NoError(t, err)
	defer rows.Close()

	var events []LinkIncidentEvent
	for rows.Next() {
		var e LinkIncidentEvent
		var eventType, severity string
		err := rows.Scan(
			&e.IncidentID, &e.LinkPK, &eventType, &e.EventTS, &e.StartedAt,
			&e.ActiveSymptoms, &e.Symptoms, &severity, &e.PeakValues,
			&e.LinkCode, &e.LinkType, &e.SideAMetro, &e.SideZMetro,
			&e.ContributorCode, &e.Status, &e.Provisioning,
		)
		require.NoError(t, err)
		e.EventType = EventType(eventType)
		e.Severity = Severity(severity)
		events = append(events, e)
	}
	return events
}

// bucket returns a time for the given hour and 5-minute bucket index.
func bucket(base time.Time, hourOffset, bucketIdx int) time.Time {
	return base.Add(time.Duration(hourOffset)*time.Hour + time.Duration(bucketIdx)*5*time.Minute)
}

// fillLinkRollup inserts zero-loss rollup rows for the entire time range to
// prevent false positive no-data incidents in tests. Aligns to 5-minute
// bucket boundaries like the real rollup writer.
func fillLinkRollup(t *testing.T, conn clickhouse.Conn, linkPK string, start, end time.Time) {
	t.Helper()
	// Align start down to 5-minute boundary.
	aligned := start.Truncate(5 * time.Minute)
	for ts := aligned; ts.Before(end); ts = ts.Add(5 * time.Minute) {
		insertLinkRollup(t, conn, linkPK, ts, 0, 0, false)
	}
}

// runBackfillChunks runs the backfill in hourly chunks over the given range.
// Callers should ensure the end time is far enough past the last data point
// (at least coalesce_gap + 1 chunk) to trigger resolution of open incidents.
func runBackfillChunks(t *testing.T, a *Activities, start, end time.Time) {
	t.Helper()
	chunkSize := 1 * time.Hour
	for cs := start; cs.Before(end); cs = cs.Add(chunkSize) {
		ce := cs.Add(chunkSize)
		if ce.After(end) {
			ce = end
		}
		err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
			WindowStart: cs,
			WindowEnd:   ce,
		})
		require.NoError(t, err)
	}
}

func TestBackfillLinkChunk_SingleIncident(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-single"
	insertLinkDimension(t, conn, linkPK, "test-single", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(4*time.Hour))
	// Overwrite 30 minutes at hour 1 with packet loss.
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 5.0, 3.0, false)
	}

	runBackfillChunks(t, a, base, base.Add(4*time.Hour))

	events := queryLinkEvents(t, conn)
	require.Len(t, events, 2, "expect opened + resolved")
	assert.Equal(t, EventOpened, events[0].EventType)
	assert.Equal(t, EventResolved, events[1].EventType)
	assert.Equal(t, events[0].IncidentID, events[1].IncidentID)
	assert.Contains(t, events[0].Symptoms, "packet_loss")
	assert.Equal(t, "test-single", events[0].LinkCode)
}

func TestBackfillLinkChunk_ContinuousAcrossFullWindow(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-continuous"
	insertLinkDimension(t, conn, linkPK, "test-continuous", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(14*time.Hour))
	// 12 hours of continuous 100% loss (144 buckets).
	for h := range 12 {
		for b := range 12 {
			insertLinkRollup(t, conn, linkPK, bucket(base, h, b), 100, 100, false)
		}
	}

	runBackfillChunks(t, a, base, base.Add(14*time.Hour))

	events := queryLinkEvents(t, conn)
	require.Len(t, events, 2, "expect opened + resolved")
	assert.Equal(t, EventOpened, events[0].EventType)
	assert.Equal(t, EventResolved, events[1].EventType)
	assert.Equal(t, events[0].IncidentID, events[1].IncidentID, "should be same incident")
}

func TestBackfillLinkChunk_TwoSeparateIncidents(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-two"
	insertLinkDimension(t, conn, linkPK, "test-two", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(6*time.Hour))
	// Incident 1: 30 min of loss at hour 1.
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 10, 10, false)
	}
	// Gap of 2 hours (well beyond 30m coalesce gap).
	// Incident 2: 30 min of loss at hour 4.
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 4, i), 10, 10, false)
	}

	runBackfillChunks(t, a, base, base.Add(6*time.Hour))

	events := queryLinkEvents(t, conn)
	require.Len(t, events, 4, "expect 2 incidents: 2x (opened + resolved)")

	// Two distinct incident IDs.
	ids := make(map[string]bool)
	for _, e := range events {
		ids[e.IncidentID] = true
	}
	assert.Len(t, ids, 2, "should be two different incidents")
}

func TestBackfillLinkChunk_CoalesceWithinGap(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-coalesce"
	insertLinkDimension(t, conn, linkPK, "test-coalesce", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(4*time.Hour))
	// First burst: 15 min of loss.
	for i := range 3 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 10, 10, false)
	}
	// Gap of 20 min (within 30m coalesce gap).
	// Second burst: 15 min of loss starting at 1h40m.
	for i := range 3 {
		insertLinkRollup(t, conn, linkPK, base.Add(1*time.Hour+40*time.Minute+time.Duration(i)*5*time.Minute), 10, 10, false)
	}

	runBackfillChunks(t, a, base, base.Add(4*time.Hour))

	events := queryLinkEvents(t, conn)
	// Two bursts within coalesce gap = one incident, but with intermediate
	// events showing the gap between bursts (matching live detector behavior).
	require.Len(t, events, 5, "opened + symptom_resolved + symptom_added + symptom_resolved + resolved")
	assert.Equal(t, EventOpened, events[0].EventType)
	assert.Equal(t, EventSymptomResolved, events[1].EventType)
	assert.Equal(t, EventSymptomAdded, events[2].EventType)
	assert.Equal(t, EventSymptomResolved, events[3].EventType)
	assert.Equal(t, EventResolved, events[4].EventType)
	assert.Equal(t, events[0].IncidentID, events[4].IncidentID)
}

func TestBackfillLinkChunk_MultipleSymptoms(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-multi"
	insertLinkDimension(t, conn, linkPK, "test-multi", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(4*time.Hour))
	// Loss + ISIS down overlapping — should be one incident with both symptoms.
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 50, 50, true)
	}

	runBackfillChunks(t, a, base, base.Add(4*time.Hour))

	events := queryLinkEvents(t, conn)
	require.Len(t, events, 2, "single incident with multiple symptoms: opened + resolved")
	assert.Contains(t, events[0].Symptoms, "packet_loss")
	assert.Contains(t, events[0].Symptoms, "isis_down")
	assert.Equal(t, Severity("critical"), events[0].Severity, "ISIS down = critical")
}

func TestBackfillLinkChunk_OngoingAtEndOfWindow(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-ongoing"
	insertLinkDimension(t, conn, linkPK, "test-ongoing", "WAN")

	// Loss at end of window — should not get a resolved event.
	windowEnd := base.Add(6 * time.Hour)
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, windowEnd.Add(-30*time.Minute+time.Duration(i)*5*time.Minute), 10, 10, false)
	}

	err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
		WindowStart: base,
		WindowEnd:   windowEnd,
	})
	require.NoError(t, err)

	events := queryLinkEvents(t, conn)
	require.Len(t, events, 1, "only opened, no resolved (ongoing)")
	assert.Equal(t, EventOpened, events[0].EventType)
}

func TestBackfillLinkChunk_CrossChunkContinuation(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-cross"
	insertLinkDimension(t, conn, linkPK, "test-cross", "WAN")

	// Continuous loss across 2 hours.
	for h := range 2 {
		for b := range 12 {
			insertLinkRollup(t, conn, linkPK, bucket(base, h, b), 100, 100, false)
		}
	}

	// Process in two 1-hour chunks.
	err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
		WindowStart: base,
		WindowEnd:   base.Add(1 * time.Hour),
	})
	require.NoError(t, err)

	err = a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
		WindowStart: base.Add(1 * time.Hour),
		WindowEnd:   base.Add(2 * time.Hour),
	})
	require.NoError(t, err)

	events := queryLinkEvents(t, conn)

	// Should be exactly 1 incident (same ID across both chunks).
	ids := make(map[string]bool)
	for _, e := range events {
		ids[e.IncidentID] = true
	}
	assert.Len(t, ids, 1, "continuous loss across chunks should be ONE incident, got %d", len(ids))

	// Should have exactly 1 opened event.
	openedCount := 0
	for _, e := range events {
		if e.EventType == EventOpened {
			openedCount++
		}
	}
	assert.Equal(t, 1, openedCount, "should only have 1 opened event")
}

func TestBackfillLinkChunk_CrossChunkResolution(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-cross-resolve"
	insertLinkDimension(t, conn, linkPK, "test-cross-resolve", "WAN")

	// Loss in chunk 1 (0:00-1:00): buckets at 0:40, 0:45, 0:50, 0:55
	// Ends at 1:00, so gap to chunk 2's first bucket (1:05) is only 5 min.
	for i := 8; i <= 11; i++ {
		insertLinkRollup(t, conn, linkPK, bucket(base, 0, i), 10, 10, false)
	}
	// Loss in chunk 2 (1:00-2:00): bucket at 1:05 (5 min after chunk 1's last)
	insertLinkRollup(t, conn, linkPK, bucket(base, 1, 1), 10, 10, false)
	// Then no more loss — incident should resolve in chunk 3

	// Process in three 1-hour chunks.
	for h := range 3 {
		err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
			WindowStart: base.Add(time.Duration(h) * time.Hour),
			WindowEnd:   base.Add(time.Duration(h+1) * time.Hour),
		})
		require.NoError(t, err)
	}

	events := queryLinkEvents(t, conn)

	// Should be 1 incident with opened + resolved.
	ids := make(map[string]bool)
	for _, e := range events {
		ids[e.IncidentID] = true
	}
	assert.Len(t, ids, 1, "should be one incident across chunks")

	hasOpened := false
	hasResolved := false
	for _, e := range events {
		if e.EventType == EventOpened {
			hasOpened = true
		}
		if e.EventType == EventResolved {
			hasResolved = true
		}
	}
	assert.True(t, hasOpened, "should have opened event")
	assert.True(t, hasResolved, "should have resolved event after loss stops")
}

func TestBackfillLinkChunk_IntermittentLossMultiChunk(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-intermittent"
	insertLinkDimension(t, conn, linkPK, "test-intermittent", "WAN")

	// Intermittent loss with gaps < 30m, spanning 4 hours.
	// Each bucket produces a window of (ts, ts+5m). Gap = next_ts - (prev_ts+5m).
	// Buckets at: 0:10, 0:30, 0:55, 1:20, 1:45, 2:10, 2:35, 3:00
	// Gaps:       15m    20m    20m    20m    20m    20m    20m   — all < 30m
	times := []time.Duration{
		10 * time.Minute,
		30 * time.Minute,
		55 * time.Minute,
		80 * time.Minute,
		105 * time.Minute,
		130 * time.Minute,
		155 * time.Minute,
		180 * time.Minute,
	}
	for _, d := range times {
		insertLinkRollup(t, conn, linkPK, base.Add(d), 5, 5, false)
	}

	// Process in 1-hour chunks.
	for h := range 5 {
		err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
			WindowStart: base.Add(time.Duration(h) * time.Hour),
			WindowEnd:   base.Add(time.Duration(h+1) * time.Hour),
		})
		require.NoError(t, err)
	}

	events := queryLinkEvents(t, conn)

	// All gaps are < 30m, so should be ONE incident.
	ids := make(map[string]bool)
	for _, e := range events {
		ids[e.IncidentID] = true
	}
	assert.Len(t, ids, 1, "intermittent loss with <30m gaps should be one incident, got %d", len(ids))

	hasResolved := false
	for _, e := range events {
		if e.EventType == EventResolved {
			hasResolved = true
		}
	}
	assert.True(t, hasResolved, "should have resolved after loss stops")
}

func TestBackfillLinkChunk_Idempotent(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-idempotent"
	insertLinkDimension(t, conn, linkPK, "test-idempotent", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(4*time.Hour))
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 10, 10, false)
	}

	// Run twice.
	runBackfillChunks(t, a, base, base.Add(4*time.Hour))
	runBackfillChunks(t, a, base, base.Add(4*time.Hour))

	events := queryLinkEvents(t, conn)
	// Should still be just 2 events (opened + resolved), not duplicated.
	// No symptom_resolved because the incident resolves within the same chunk.
	assert.Len(t, events, 2, "second run should not create duplicates")
}

func TestBackfillLinkChunk_OverwriteMode(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-overwrite"
	insertLinkDimension(t, conn, linkPK, "test-overwrite", "WAN")

	// Fill the entire range with zero-loss data to prevent no-data false positives.
	fillLinkRollup(t, conn, linkPK, base, base.Add(5*time.Hour))
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 10, 10, false)
	}

	runBackfillChunks(t, a, base, base.Add(4*time.Hour))

	// Count events before overwrite.
	eventsBefore := queryLinkEvents(t, conn)

	// Run again with overwrite on each chunk.
	for h := range 5 {
		err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
			WindowStart: base.Add(time.Duration(h) * time.Hour),
			WindowEnd:   base.Add(time.Duration(h+1) * time.Hour),
			Overwrite:   true,
		})
		require.NoError(t, err)
	}

	eventsAfter := queryLinkEvents(t, conn)
	assert.Len(t, eventsAfter, len(eventsBefore), "overwrite should produce same event count")
}

func TestBackfillLinkChunk_NoLatencyData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-nolatency"
	insertLinkDimension(t, conn, linkPK, "test-nolatency", "WAN")

	// Link reported latency in the 24h before the backfill window...
	for i := range 12 {
		insertLinkRollup(t, conn, linkPK, base.Add(-24*time.Hour+time.Duration(i)*5*time.Minute), 0, 0, false)
	}
	// ...but has NO data in the backfill window itself.
	// This should produce a no_latency_data incident.

	runBackfillChunks(t, a, base, base.Add(2*time.Hour))

	events := queryLinkEvents(t, conn)
	require.NotEmpty(t, events, "should detect no_latency_data incident")

	hasNoLatency := false
	for _, e := range events {
		if e.EventType == EventOpened {
			for _, s := range e.Symptoms {
				if s == "no_latency_data" {
					hasNoLatency = true
				}
			}
		}
	}
	assert.True(t, hasNoLatency, "should have no_latency_data symptom")
}

func TestBackfillLinkChunk_NoTrafficData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-notraffic"
	insertLinkDimension(t, conn, linkPK, "test-notraffic", "WAN")

	// Link had traffic data in the 24h before the window...
	for i := range 12 {
		err := conn.Exec(context.Background(), `
			INSERT INTO device_interface_rollup_5m (
				bucket_ts, device_pk, intf, ingested_at,
				link_pk, link_side,
				in_errors, out_errors, in_fcs_errors, in_discards, out_discards, carrier_transitions,
				avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
				avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
				avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
				avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps,
				status, isis_overload, isis_unreachable, user_tunnel_id, user_pk
			) VALUES (
				$1, 'dev-1', 'eth0', now(),
				$2, 'A',
				0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				'activated', false, false, NULL, ''
			)`,
			base.Add(-24*time.Hour+time.Duration(i)*5*time.Minute),
			linkPK,
		)
		require.NoError(t, err)
	}
	// ...but has NO traffic data in the backfill window.

	runBackfillChunks(t, a, base, base.Add(2*time.Hour))

	events := queryLinkEvents(t, conn)
	require.NotEmpty(t, events, "should detect no_traffic_data incident")

	hasNoTraffic := false
	for _, e := range events {
		if e.EventType == EventOpened {
			for _, s := range e.Symptoms {
				if s == "no_traffic_data" {
					hasNoTraffic = true
				}
			}
		}
	}
	assert.True(t, hasNoTraffic, "should have no_traffic_data symptom")
}

func TestBackfillLinkChunk_NoFalsePositiveNoDataUnalignedStart(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	// Use a non-5-minute-aligned start time (like --start-time-ago produces).
	base := time.Date(2026, 3, 28, 0, 7, 0, 0, time.UTC) // 00:07, not aligned to 5m
	linkPK := "test-link-nofalse-unaligned"
	insertLinkDimension(t, conn, linkPK, "test-nofalse-unaligned", "WAN")

	// Continuous data across the full range — no gaps.
	fillLinkRollup(t, conn, linkPK, base.Add(-1*time.Hour), base.Add(3*time.Hour))

	runBackfillChunks(t, a, base, base.Add(2*time.Hour))

	events := queryLinkEvents(t, conn)
	assert.Empty(t, events, "healthy link with unaligned start should not have any incidents")
}

func TestBackfillLinkChunk_NoLatencyDataUnaligned(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	// Unaligned start time.
	base := time.Date(2026, 3, 28, 0, 3, 0, 0, time.UTC) // 00:03
	linkPK := "test-link-nolatency-unaligned"
	insertLinkDimension(t, conn, linkPK, "test-nolatency-unaligned", "WAN")

	// Link reported in the hour before — aligned to 5m buckets.
	for i := range 12 {
		insertLinkRollup(t, conn, linkPK, base.Truncate(5*time.Minute).Add(-1*time.Hour+time.Duration(i)*5*time.Minute), 0, 0, false)
	}
	// No data in the backfill window.

	runBackfillChunks(t, a, base, base.Add(2*time.Hour))

	events := queryLinkEvents(t, conn)
	require.NotEmpty(t, events, "should detect no_latency_data incident")

	hasNoLatency := false
	for _, e := range events {
		if e.EventType == EventOpened {
			for _, s := range e.Symptoms {
				if s == "no_latency_data" {
					hasNoLatency = true
				}
			}
		}
	}
	assert.True(t, hasNoLatency, "should have no_latency_data symptom")
}

func TestFilterStaleNoDataWindows(t *testing.T) {
	t.Parallel()

	cutoff := time.Date(2026, 3, 28, 1, 0, 0, 0, time.UTC)
	before := cutoff.Add(-10 * time.Minute) // within freshness
	after := cutoff.Add(10 * time.Minute)   // past freshness

	makeWindow := func(symptom string, startedAt time.Time) backfillSymptomWindow {
		return backfillSymptomWindow{
			Symptom:   symptom,
			StartedAt: startedAt,
			EndedAt:   startedAt.Add(5 * time.Minute),
		}
	}

	t.Run("zero cutoffs pass everything through", func(t *testing.T) {
		windows := []backfillSymptomWindow{
			makeWindow(SymptomNoLatencyData, after),
			makeWindow(SymptomNoTrafficData, after),
		}
		result := filterStaleNoDataWindows(windows, time.Time{}, time.Time{})
		assert.Len(t, result, 2)
	})

	t.Run("no_latency_data past cutoff is filtered", func(t *testing.T) {
		windows := []backfillSymptomWindow{
			makeWindow(SymptomNoLatencyData, before),
			makeWindow(SymptomNoLatencyData, after),
		}
		result := filterStaleNoDataWindows(windows, cutoff, time.Time{})
		require.Len(t, result, 1)
		assert.Equal(t, before, result[0].StartedAt)
	})

	t.Run("no_traffic_data past cutoff is filtered", func(t *testing.T) {
		windows := []backfillSymptomWindow{
			makeWindow(SymptomNoTrafficData, before),
			makeWindow(SymptomNoTrafficData, after),
		}
		result := filterStaleNoDataWindows(windows, time.Time{}, cutoff)
		require.Len(t, result, 1)
		assert.Equal(t, before, result[0].StartedAt)
	})

	t.Run("non-no_data symptoms are never filtered", func(t *testing.T) {
		windows := []backfillSymptomWindow{
			makeWindow(SymptomPacketLoss, after),
			makeWindow(SymptomISISDown, after),
			makeWindow(SymptomErrors, after),
		}
		result := filterStaleNoDataWindows(windows, cutoff, cutoff)
		assert.Len(t, result, 3)
	})

	t.Run("exact cutoff time is filtered", func(t *testing.T) {
		windows := []backfillSymptomWindow{
			makeWindow(SymptomNoLatencyData, cutoff),
		}
		result := filterStaleNoDataWindows(windows, cutoff, time.Time{})
		assert.Empty(t, result)
	})

	t.Run("mixed symptoms only filters no_data past cutoff", func(t *testing.T) {
		windows := []backfillSymptomWindow{
			makeWindow(SymptomPacketLoss, after),
			makeWindow(SymptomNoLatencyData, after),
			makeWindow(SymptomNoTrafficData, before),
			makeWindow(SymptomISISDown, after),
		}
		result := filterStaleNoDataWindows(windows, cutoff, cutoff)
		require.Len(t, result, 3)
		for _, w := range result {
			assert.NotEqual(t, SymptomNoLatencyData, w.Symptom)
		}
	})
}

func TestBackfillLinkChunk_NoLatencyDataSuppressedByFreshness(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-suppress-latency"
	insertLinkDimension(t, conn, linkPK, "test-suppress-latency", "WAN")

	// Link reported latency in the 24h before — so it's "expected" to have data.
	for i := range 12 {
		insertLinkRollup(t, conn, linkPK, base.Add(-24*time.Hour+time.Duration(i)*5*time.Minute), 0, 0, false)
	}
	// No data in the backfill window — would normally produce no_latency_data.

	// Process with a latency freshness cutoff AT the window start,
	// meaning the latency pipeline hasn't caught up to this window at all.
	err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
		WindowStart:       base,
		WindowEnd:         base.Add(2 * time.Hour),
		LatencyFreshUntil: base, // pipeline only fresh up to window start
	})
	require.NoError(t, err)

	events := queryLinkEvents(t, conn)
	assert.Empty(t, events, "no_latency_data should be suppressed when latency pipeline is behind")
}

func TestBackfillLinkChunk_NoTrafficDataSuppressedByFreshness(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-suppress-traffic"
	insertLinkDimension(t, conn, linkPK, "test-suppress-traffic", "WAN")

	// Link had traffic data in the 24h before the window.
	for i := range 12 {
		err := conn.Exec(context.Background(), `
			INSERT INTO device_interface_rollup_5m (
				bucket_ts, device_pk, intf, ingested_at,
				link_pk, link_side,
				in_errors, out_errors, in_fcs_errors, in_discards, out_discards, carrier_transitions,
				avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
				avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
				avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
				avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps,
				status, isis_overload, isis_unreachable, user_tunnel_id, user_pk
			) VALUES (
				$1, 'dev-suppress', 'eth0', now(),
				$2, 'A',
				0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0,
				'activated', false, false, NULL, ''
			)`,
			base.Add(-24*time.Hour+time.Duration(i)*5*time.Minute),
			linkPK,
		)
		require.NoError(t, err)
	}
	// No traffic data in the window — would normally produce no_traffic_data.

	// Process with traffic freshness cutoff at window start.
	err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
		WindowStart:       base,
		WindowEnd:         base.Add(2 * time.Hour),
		TrafficFreshUntil: base,
	})
	require.NoError(t, err)

	events := queryLinkEvents(t, conn)
	assert.Empty(t, events, "no_traffic_data should be suppressed when traffic pipeline is behind")
}

func TestBackfillLinkChunk_RealSymptomsNotSuppressedByFreshness(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-real-not-suppressed"
	insertLinkDimension(t, conn, linkPK, "test-real-not-suppressed", "WAN")

	// Fill with zero-loss data, then add real packet loss.
	fillLinkRollup(t, conn, linkPK, base, base.Add(4*time.Hour))
	for i := range 6 {
		insertLinkRollup(t, conn, linkPK, bucket(base, 1, i), 10, 10, false)
	}

	// Even with both freshness cutoffs set to window start (maximally restrictive),
	// real symptoms like packet_loss should still be detected.
	runBackfillChunksWithFreshness(t, a, base, base.Add(4*time.Hour), base, base)

	events := queryLinkEvents(t, conn)
	require.NotEmpty(t, events, "real symptoms should not be suppressed by freshness cutoffs")
	assert.Contains(t, events[0].Symptoms, "packet_loss")
}

// runBackfillChunksWithFreshness is like runBackfillChunks but passes freshness cutoffs.
func runBackfillChunksWithFreshness(t *testing.T, a *Activities, start, end time.Time, latencyCutoff, trafficCutoff time.Time) {
	t.Helper()
	chunkSize := 1 * time.Hour
	for cs := start; cs.Before(end); cs = cs.Add(chunkSize) {
		ce := cs.Add(chunkSize)
		if ce.After(end) {
			ce = end
		}
		err := a.BackfillLinkChunk(context.Background(), BackfillChunkInput{
			WindowStart:       cs,
			WindowEnd:         ce,
			LatencyFreshUntil: latencyCutoff,
			TrafficFreshUntil: trafficCutoff,
		})
		require.NoError(t, err)
	}
}

func TestBackfillLinkChunk_NoFalsePositiveNoData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := newTestBackfillActivities(conn)

	base := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC)
	linkPK := "test-link-nofalse"
	insertLinkDimension(t, conn, linkPK, "test-nofalse", "WAN")

	// Link has continuous latency data through the entire window.
	for h := range 3 {
		for b := range 12 {
			insertLinkRollup(t, conn, linkPK, bucket(base, h, b), 0, 0, false)
		}
	}

	runBackfillChunks(t, a, base, base.Add(3*time.Hour))

	events := queryLinkEvents(t, conn)
	// Should have NO incidents — the link is healthy.
	assert.Empty(t, events, "healthy link should not have any incidents")
}
