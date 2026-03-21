package rollup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.temporal.io/sdk/activity"
)

// Activities holds dependencies for rollup activities.
type Activities struct {
	ClickHouse driver.Conn
	Log        *slog.Logger
}

// safeHeartbeat records a heartbeat if running inside a Temporal activity context.
func safeHeartbeat(ctx context.Context, details ...any) {
	defer func() { recover() }() //nolint:errcheck
	activity.RecordHeartbeat(ctx, details...)
}

// ComputeLinkRollup queries probe telemetry for the given time window and returns
// link rollup buckets with per-direction latency percentiles and loss.
func (a *Activities) ComputeLinkRollup(ctx context.Context, input BackfillChunkInput) ([]LinkBucket, error) {
	safeHeartbeat(ctx, "computing link rollup")
	start := time.Now()

	latencyQuery := `
		WITH loss_sub AS (
			SELECT
				f.link_pk,
				toStartOfFiveMinutes(f.ingested_at) as bucket,
				if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') as direction,
				countIf(f.loss OR f.rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency f
			JOIN dz_links_current l ON f.link_pk = l.pk
			LEFT JOIN (
				SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
					   latest_sample_index, sampling_interval_us
				FROM fact_dz_device_link_latency_sample_header
			) h ON f.origin_device_pk = h.origin_device_pk
				AND f.target_device_pk = h.target_device_pk
				AND f.link_pk = h._hdr_link_pk
				AND f.epoch = h.epoch
			WHERE f.ingested_at >= $1 AND f.ingested_at < $2
			GROUP BY f.link_pk, bucket, direction
		)
		SELECT
			f.link_pk,
			toStartOfFiveMinutes(f.ingested_at) as bucket,
			if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') as direction,
			avg(f.rtt_us) as avg_rtt,
			toFloat64(min(f.rtt_us)) as min_rtt,
			quantile(0.50)(f.rtt_us) as p50_rtt,
			quantile(0.90)(f.rtt_us) as p90_rtt,
			quantile(0.95)(f.rtt_us) as p95_rtt,
			quantile(0.99)(f.rtt_us) as p99_rtt,
			toFloat64(max(f.rtt_us)) as max_rtt,
			max(ls.loss_pct) as loss_pct,
			toUInt32(count(*)) as samples
		FROM fact_dz_device_link_latency f
		JOIN dz_links_current l ON f.link_pk = l.pk
		LEFT JOIN (
			SELECT origin_device_pk, target_device_pk, link_pk AS _hdr_link_pk, epoch,
				   latest_sample_index, sampling_interval_us
			FROM fact_dz_device_link_latency_sample_header
		) h ON f.origin_device_pk = h.origin_device_pk
			AND f.target_device_pk = h.target_device_pk
			AND f.link_pk = h._hdr_link_pk
			AND f.epoch = h.epoch
		LEFT JOIN loss_sub ls ON f.link_pk = ls.link_pk
			AND toStartOfFiveMinutes(f.ingested_at) = ls.bucket
			AND if(f.origin_device_pk = l.side_a_pk, 'A', 'Z') = ls.direction
		WHERE f.ingested_at >= $1 AND f.ingested_at < $2
		GROUP BY f.link_pk, bucket, direction
		ORDER BY f.link_pk, bucket, direction
	`

	rows, err := a.ClickHouse.Query(ctx, latencyQuery, input.WindowStart, input.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("link latency query: %w", err)
	}
	defer rows.Close()

	type bucketKey struct {
		linkPK string
		bucket time.Time
	}
	type bucketData struct {
		a *LinkLatencyStats
		z *LinkLatencyStats
	}
	bucketMap := make(map[bucketKey]*bucketData)

	for rows.Next() {
		var linkPK, direction string
		var bucket time.Time
		var d LinkLatencyStats
		if err := rows.Scan(&linkPK, &bucket, &direction,
			&d.AvgRttUs, &d.MinRttUs, &d.P50RttUs, &d.P90RttUs, &d.P95RttUs, &d.P99RttUs, &d.MaxRttUs,
			&d.LossPct, &d.Samples); err != nil {
			return nil, fmt.Errorf("link latency scan: %w", err)
		}
		bk := bucketKey{linkPK: linkPK, bucket: bucket.UTC()}
		if bucketMap[bk] == nil {
			bucketMap[bk] = &bucketData{}
		}
		ds := d
		if direction == "A" {
			bucketMap[bk].a = &ds
		} else {
			bucketMap[bk].z = &ds
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("link latency rows: %w", err)
	}

	now := time.Now()
	var buckets []LinkBucket

	for bk, bd := range bucketMap {
		b := LinkBucket{
			BucketTS:   bk.bucket,
			LinkPK:     bk.linkPK,
			IngestedAt: now,
		}
		if bd.a != nil {
			b.A = *bd.a
		}
		if bd.z != nil {
			b.Z = *bd.z
		}
		buckets = append(buckets, b)
	}

	a.Log.Info("computed link rollup buckets",
		"count", len(buckets),
		"window", fmt.Sprintf("[%s, %s)", input.WindowStart.Format(time.RFC3339), input.WindowEnd.Format(time.RFC3339)),
		"duration", time.Since(start))

	return buckets, nil
}

// ComputeDeviceInterfaceRollup queries interface counters and traffic rates for
// the given time window and returns per-device, per-interface rollup buckets.
func (a *Activities) ComputeDeviceInterfaceRollup(ctx context.Context, input BackfillChunkInput) ([]DeviceInterfaceBucket, error) {
	safeHeartbeat(ctx, "computing device interface rollup")
	start := time.Now()

	query := `
		SELECT
			device_pk,
			intf,
			toStartOfFiveMinutes(event_ts) as bucket,
			-- Error/discard counters
			toUInt64(SUM(greatest(0, in_errors_delta))) as in_errors,
			toUInt64(SUM(greatest(0, out_errors_delta))) as out_errors,
			toUInt64(SUM(greatest(0, in_fcs_errors_delta))) as in_fcs_errors,
			toUInt64(SUM(greatest(0, in_discards_delta))) as in_discards,
			toUInt64(SUM(greatest(0, out_discards_delta))) as out_discards,
			toUInt64(SUM(greatest(0, carrier_transitions_delta))) as carrier_transitions,
			-- In BPS percentiles
			avgIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as avg_in_bps,
			minIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as min_in_bps,
			quantileIf(0.50)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p50_in_bps,
			quantileIf(0.90)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p90_in_bps,
			quantileIf(0.95)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p95_in_bps,
			quantileIf(0.99)(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as p99_in_bps,
			maxIf(in_octets_delta * 8 / delta_duration, delta_duration > 0 AND in_octets_delta >= 0) as max_in_bps,
			-- Out BPS percentiles
			avgIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as avg_out_bps,
			minIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as min_out_bps,
			quantileIf(0.50)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p50_out_bps,
			quantileIf(0.90)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p90_out_bps,
			quantileIf(0.95)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p95_out_bps,
			quantileIf(0.99)(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as p99_out_bps,
			maxIf(out_octets_delta * 8 / delta_duration, delta_duration > 0 AND out_octets_delta >= 0) as max_out_bps,
			-- In PPS percentiles
			avgIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as avg_in_pps,
			minIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as min_in_pps,
			quantileIf(0.50)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p50_in_pps,
			quantileIf(0.90)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p90_in_pps,
			quantileIf(0.95)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p95_in_pps,
			quantileIf(0.99)(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as p99_in_pps,
			maxIf(in_pkts_delta / delta_duration, delta_duration > 0 AND in_pkts_delta >= 0) as max_in_pps,
			-- Out PPS percentiles
			avgIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as avg_out_pps,
			minIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as min_out_pps,
			quantileIf(0.50)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p50_out_pps,
			quantileIf(0.90)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p90_out_pps,
			quantileIf(0.95)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p95_out_pps,
			quantileIf(0.99)(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as p99_out_pps,
			maxIf(out_pkts_delta / delta_duration, delta_duration > 0 AND out_pkts_delta >= 0) as max_out_pps
		FROM fact_dz_device_interface_counters
		WHERE event_ts >= $1 AND event_ts < $2
		GROUP BY device_pk, intf, bucket
		ORDER BY device_pk, intf, bucket
	`

	rows, err := a.ClickHouse.Query(ctx, query, input.WindowStart, input.WindowEnd)
	if err != nil {
		return nil, fmt.Errorf("device interface query: %w", err)
	}
	defer rows.Close()

	now := time.Now()
	var buckets []DeviceInterfaceBucket

	for rows.Next() {
		var b DeviceInterfaceBucket
		b.IngestedAt = now
		if err := rows.Scan(
			&b.DevicePK, &b.Intf, &b.BucketTS,
			&b.InErrors, &b.OutErrors, &b.InFcsErrors, &b.InDiscards, &b.OutDiscards, &b.CarrierTransitions,
			&b.InBps.Avg, &b.InBps.Min, &b.InBps.P50, &b.InBps.P90, &b.InBps.P95, &b.InBps.P99, &b.InBps.Max,
			&b.OutBps.Avg, &b.OutBps.Min, &b.OutBps.P50, &b.OutBps.P90, &b.OutBps.P95, &b.OutBps.P99, &b.OutBps.Max,
			&b.InPps.Avg, &b.InPps.Min, &b.InPps.P50, &b.InPps.P90, &b.InPps.P95, &b.InPps.P99, &b.InPps.Max,
			&b.OutPps.Avg, &b.OutPps.Min, &b.OutPps.P50, &b.OutPps.P90, &b.OutPps.P95, &b.OutPps.P99, &b.OutPps.Max,
		); err != nil {
			return nil, fmt.Errorf("device interface scan: %w", err)
		}
		b.BucketTS = b.BucketTS.UTC()
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("device interface rows: %w", err)
	}

	a.Log.Info("computed device interface rollup buckets",
		"count", len(buckets),
		"window", fmt.Sprintf("[%s, %s)", input.WindowStart.Format(time.RFC3339), input.WindowEnd.Format(time.RFC3339)),
		"duration", time.Since(start))

	return buckets, nil
}

// WriteLinkBuckets batch inserts link rollup buckets into ClickHouse.
func (a *Activities) WriteLinkBuckets(ctx context.Context, buckets []LinkBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	safeHeartbeat(ctx, fmt.Sprintf("writing %d link buckets", len(buckets)))

	batch, err := a.ClickHouse.PrepareBatch(ctx, `INSERT INTO link_rollup_5m (
		bucket_ts, link_pk, ingested_at,
		a_avg_rtt_us, a_min_rtt_us, a_p50_rtt_us, a_p90_rtt_us, a_p95_rtt_us, a_p99_rtt_us, a_max_rtt_us, a_loss_pct, a_samples,
		z_avg_rtt_us, z_min_rtt_us, z_p50_rtt_us, z_p90_rtt_us, z_p95_rtt_us, z_p99_rtt_us, z_max_rtt_us, z_loss_pct, z_samples
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, b := range buckets {
		if err := batch.Append(
			b.BucketTS, b.LinkPK, b.IngestedAt,
			b.A.AvgRttUs, b.A.MinRttUs, b.A.P50RttUs, b.A.P90RttUs, b.A.P95RttUs, b.A.P99RttUs, b.A.MaxRttUs, b.A.LossPct, b.A.Samples,
			b.Z.AvgRttUs, b.Z.MinRttUs, b.Z.P50RttUs, b.Z.P90RttUs, b.Z.P95RttUs, b.Z.P99RttUs, b.Z.MaxRttUs, b.Z.LossPct, b.Z.Samples,
		); err != nil {
			return fmt.Errorf("append batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	a.Log.Info("wrote link rollup buckets", "count", len(buckets))
	return nil
}

// WriteDeviceInterfaceBuckets batch inserts device interface rollup buckets into ClickHouse.
func (a *Activities) WriteDeviceInterfaceBuckets(ctx context.Context, buckets []DeviceInterfaceBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	safeHeartbeat(ctx, fmt.Sprintf("writing %d device interface buckets", len(buckets)))

	batch, err := a.ClickHouse.PrepareBatch(ctx, `INSERT INTO device_interface_rollup_5m (
		bucket_ts, device_pk, intf, ingested_at,
		in_errors, out_errors, in_fcs_errors, in_discards, out_discards, carrier_transitions,
		avg_in_bps, min_in_bps, p50_in_bps, p90_in_bps, p95_in_bps, p99_in_bps, max_in_bps,
		avg_out_bps, min_out_bps, p50_out_bps, p90_out_bps, p95_out_bps, p99_out_bps, max_out_bps,
		avg_in_pps, min_in_pps, p50_in_pps, p90_in_pps, p95_in_pps, p99_in_pps, max_in_pps,
		avg_out_pps, min_out_pps, p50_out_pps, p90_out_pps, p95_out_pps, p99_out_pps, max_out_pps
	)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, b := range buckets {
		if err := batch.Append(
			b.BucketTS, b.DevicePK, b.Intf, b.IngestedAt,
			b.InErrors, b.OutErrors, b.InFcsErrors, b.InDiscards, b.OutDiscards, b.CarrierTransitions,
			b.InBps.Avg, b.InBps.Min, b.InBps.P50, b.InBps.P90, b.InBps.P95, b.InBps.P99, b.InBps.Max,
			b.OutBps.Avg, b.OutBps.Min, b.OutBps.P50, b.OutBps.P90, b.OutBps.P95, b.OutBps.P99, b.OutBps.Max,
			b.InPps.Avg, b.InPps.Min, b.InPps.P50, b.InPps.P90, b.InPps.P95, b.InPps.P99, b.InPps.Max,
			b.OutPps.Avg, b.OutPps.Min, b.OutPps.P50, b.OutPps.P90, b.OutPps.P95, b.OutPps.P99, b.OutPps.Max,
		); err != nil {
			return fmt.Errorf("append batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	a.Log.Info("wrote device interface rollup buckets", "count", len(buckets))
	return nil
}

// RollupLinks computes link rollup buckets and writes them to ClickHouse in a
// single activity, avoiding large payloads flowing through Temporal.
func (a *Activities) RollupLinks(ctx context.Context, input BackfillChunkInput) (int, error) {
	buckets, err := a.ComputeLinkRollup(ctx, input)
	if err != nil {
		return 0, err
	}
	if err := a.WriteLinkBuckets(ctx, buckets); err != nil {
		return 0, err
	}
	return len(buckets), nil
}

// RollupDeviceInterfaces computes device interface rollup buckets and writes
// them to ClickHouse in a single activity.
func (a *Activities) RollupDeviceInterfaces(ctx context.Context, input BackfillChunkInput) (int, error) {
	buckets, err := a.ComputeDeviceInterfaceRollup(ctx, input)
	if err != nil {
		return 0, err
	}
	if err := a.WriteDeviceInterfaceBuckets(ctx, buckets); err != nil {
		return 0, err
	}
	return len(buckets), nil
}
