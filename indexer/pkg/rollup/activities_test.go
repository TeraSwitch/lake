package rollup

import (
	"context"
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

// --- Link rollup tests ---

func TestWriteLinkBuckets(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	ctx := context.Background()

	buckets := []LinkBucket{{
		BucketTS:   time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		LinkPK:     "link-1",
		IngestedAt: time.Now().Truncate(time.Millisecond),
		A: LinkLatencyStats{
			AvgRttUs: 100.5, MinRttUs: 50, P50RttUs: 90, P90RttUs: 120,
			P95RttUs: 150, P99RttUs: 200, MaxRttUs: 250,
			LossPct: 1.0, Samples: 500,
		},
		Z: LinkLatencyStats{
			AvgRttUs: 110.5, MinRttUs: 60, P50RttUs: 100, P90RttUs: 130,
			P95RttUs: 160, P99RttUs: 210, MaxRttUs: 260,
			LossPct: 1.5, Samples: 500,
		},
	}}

	require.NoError(t, a.WriteLinkBuckets(ctx, buckets))

	var aAvg, aMin, aP95, aMax, aLoss float64
	var aSamples uint32
	var zAvg, zP95, zLoss float64
	var zSamples uint32
	err := conn.QueryRow(ctx, `
		SELECT a_avg_rtt_us, a_min_rtt_us, a_p95_rtt_us, a_max_rtt_us, a_loss_pct, a_samples,
		       z_avg_rtt_us, z_p95_rtt_us, z_loss_pct, z_samples
		FROM link_rollup_5m FINAL WHERE link_pk = 'link-1'
	`).Scan(&aAvg, &aMin, &aP95, &aMax, &aLoss, &aSamples, &zAvg, &zP95, &zLoss, &zSamples)
	require.NoError(t, err)

	assert.InDelta(t, 100.5, aAvg, 0.01)
	assert.InDelta(t, 50.0, aMin, 0.01)
	assert.InDelta(t, 150.0, aP95, 0.01)
	assert.InDelta(t, 250.0, aMax, 0.01)
	assert.InDelta(t, 1.0, aLoss, 0.01)
	assert.Equal(t, uint32(500), aSamples)
	assert.InDelta(t, 110.5, zAvg, 0.01)
	assert.InDelta(t, 160.0, zP95, 0.01)
	assert.InDelta(t, 1.5, zLoss, 0.01)
	assert.Equal(t, uint32(500), zSamples)
}

func TestWriteLinkBuckets_Empty(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	require.NoError(t, a.WriteLinkBuckets(context.Background(), nil))
}

func TestComputeLinkRollup_EmptyTables(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeLinkRollup(context.Background(), BackfillChunkInput{
		WindowStart: time.Now().Add(-1 * time.Hour),
		WindowEnd:   time.Now(),
	})
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

func TestComputeLinkRollup_WithData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed dim_dz_links_history (dz_links_current is a view over this)
	err := conn.Exec(ctx, `INSERT INTO dim_dz_links_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, pk, status, side_a_pk, side_z_pk, bandwidth_bps) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		"link-entity-1", now, now, "00000000-0000-0000-0000-000000000001", uint8(0), "link-1", "activated", "device-a", "device-z", int64(10_000_000_000))
	require.NoError(t, err)

	// Seed latency samples for both sides within the same 5m bucket
	for i := range 20 {
		ts := bucketStart.Add(time.Duration(i) * time.Second)
		// Side A probes: RTT 100-290us
		err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ts, ts, int64(1), int32(i), "device-a", "device-z", "link-1", int64(100+i*10), false)
		require.NoError(t, err)
		// Side Z probes: RTT 200-390us (higher)
		err = conn.Exec(ctx, `INSERT INTO fact_dz_device_link_latency (event_ts, ingested_at, epoch, sample_index, origin_device_pk, target_device_pk, link_pk, rtt_us, loss) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ts, ts, int64(1), int32(i), "device-z", "device-a", "link-1", int64(200+i*10), false)
		require.NoError(t, err)
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeLinkRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	b := buckets[0]
	assert.Equal(t, "link-1", b.LinkPK)

	// Direction A should have lower RTT than direction Z
	assert.Greater(t, b.A.AvgRttUs, float64(0))
	assert.Greater(t, b.Z.AvgRttUs, float64(0))
	assert.Less(t, b.A.AvgRttUs, b.Z.AvgRttUs)

	// Full percentile spectrum for each direction
	assert.GreaterOrEqual(t, b.A.MaxRttUs, b.A.P99RttUs)
	assert.GreaterOrEqual(t, b.A.P99RttUs, b.A.P95RttUs)
	assert.GreaterOrEqual(t, b.A.P95RttUs, b.A.P50RttUs)
	assert.GreaterOrEqual(t, b.A.P50RttUs, b.A.MinRttUs)

	assert.GreaterOrEqual(t, b.Z.MaxRttUs, b.Z.P99RttUs)
	assert.GreaterOrEqual(t, b.Z.P99RttUs, b.Z.P95RttUs)
	assert.GreaterOrEqual(t, b.Z.P95RttUs, b.Z.P50RttUs)
	assert.GreaterOrEqual(t, b.Z.P50RttUs, b.Z.MinRttUs)

	assert.Equal(t, uint32(20), b.A.Samples)
	assert.Equal(t, uint32(20), b.Z.Samples)

	// Write back and verify
	require.NoError(t, a.WriteLinkBuckets(ctx, buckets))
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM link_rollup_5m").Scan(&count))
	assert.Equal(t, uint64(1), count)
}

// --- Device interface rollup tests ---

func TestWriteDeviceInterfaceBuckets(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	ctx := context.Background()

	buckets := []DeviceInterfaceBucket{{
		BucketTS:           time.Date(2026, 3, 21, 12, 0, 0, 0, time.UTC),
		DevicePK:           "device-1",
		Intf:               "Ethernet1/1",
		IngestedAt:         time.Now().Truncate(time.Millisecond),
		InErrors:           100,
		OutErrors:          50,
		InFcsErrors:        10,
		InDiscards:         20,
		OutDiscards:        15,
		CarrierTransitions: 3,
		InBps:              InterfaceRateStats{Avg: 1_000_000, P95: 1_500_000, Max: 2_000_000},
		OutBps:             InterfaceRateStats{Avg: 500_000},
		InPps:              InterfaceRateStats{Avg: 1500},
		OutPps:             InterfaceRateStats{Avg: 750},
	}}

	require.NoError(t, a.WriteDeviceInterfaceBuckets(ctx, buckets))

	var inErr, outErr, fcsErr uint64
	var avgInBps, p95InBps, maxInBps float64
	err := conn.QueryRow(ctx, `
		SELECT in_errors, out_errors, in_fcs_errors, avg_in_bps, p95_in_bps, max_in_bps
		FROM device_interface_rollup_5m FINAL
		WHERE device_pk = 'device-1' AND intf = 'Ethernet1/1'
	`).Scan(&inErr, &outErr, &fcsErr, &avgInBps, &p95InBps, &maxInBps)
	require.NoError(t, err)

	assert.Equal(t, uint64(100), inErr)
	assert.Equal(t, uint64(50), outErr)
	assert.Equal(t, uint64(10), fcsErr)
	assert.InDelta(t, 1_000_000, avgInBps, 0.01)
	assert.InDelta(t, 1_500_000, p95InBps, 0.01)
	assert.InDelta(t, 2_000_000, maxInBps, 0.01)
}

func TestWriteDeviceInterfaceBuckets_Empty(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}
	require.NoError(t, a.WriteDeviceInterfaceBuckets(context.Background(), nil))
}

func TestComputeDeviceInterfaceRollup_EmptyTables(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeDeviceInterfaceRollup(context.Background(), BackfillChunkInput{
		WindowStart: time.Now().Add(-1 * time.Hour),
		WindowEnd:   time.Now(),
	})
	require.NoError(t, err)
	assert.Empty(t, buckets)
}

func TestComputeDeviceInterfaceRollup_WithData(t *testing.T) {
	t.Parallel()
	conn := setupTestDB(t)
	ctx := context.Background()

	now := time.Now().Truncate(5 * time.Minute)
	bucketStart := now.Add(-5 * time.Minute)

	// Seed multiple counter snapshots for the same interface
	for i := range 5 {
		ts := bucketStart.Add(time.Duration(i) * time.Minute)
		err := conn.Exec(ctx, `INSERT INTO fact_dz_device_interface_counters (event_ts, ingested_at, device_pk, host, intf, link_pk, link_side, in_errors_delta, out_errors_delta, in_fcs_errors_delta, in_discards_delta, out_discards_delta, carrier_transitions_delta, in_octets_delta, out_octets_delta, in_pkts_delta, out_pkts_delta, delta_duration) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
			ts, ts, "device-1", "host-1", "Ethernet1/1", "link-1", "A",
			int64(10), int64(5), int64(1), int64(2), int64(1), int64(0),
			int64(125_000*(i+1)), int64(62_500*(i+1)), int64(100*(i+1)), int64(50*(i+1)), float64(1.0))
		require.NoError(t, err)
	}

	a := &Activities{ClickHouse: conn, Log: laketesting.NewLogger()}

	buckets, err := a.ComputeDeviceInterfaceRollup(ctx, BackfillChunkInput{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(5 * time.Minute),
	})
	require.NoError(t, err)
	require.Len(t, buckets, 1)

	b := buckets[0]
	assert.Equal(t, "device-1", b.DevicePK)
	assert.Equal(t, "Ethernet1/1", b.Intf)

	// Counters are summed
	assert.Equal(t, uint64(50), b.InErrors)
	assert.Equal(t, uint64(25), b.OutErrors)

	// BPS percentiles (5 snapshots with increasing rates)
	assert.Greater(t, b.InBps.Avg, float64(0))
	assert.Greater(t, b.InBps.Min, float64(0))
	assert.Greater(t, b.InBps.Max, float64(0))
	assert.GreaterOrEqual(t, b.InBps.Max, b.InBps.P99)
	assert.GreaterOrEqual(t, b.InBps.P99, b.InBps.P95)
	assert.GreaterOrEqual(t, b.InBps.P95, b.InBps.Min)

	// PPS
	assert.Greater(t, b.InPps.Avg, float64(0))
	assert.Greater(t, b.OutPps.Avg, float64(0))

	// Write back and verify
	require.NoError(t, a.WriteDeviceInterfaceBuckets(ctx, buckets))
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM device_interface_rollup_5m").Scan(&count))
	assert.Equal(t, uint64(1), count)
}
