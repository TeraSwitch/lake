-- +goose Up

-- Link rollup: per-direction latency/loss from probe data (fact_dz_device_link_latency).
-- Direction A = probes originating from side_a, direction Z = probes originating from side_z.
-- Interface counters and traffic rates live in device_interface_rollup_5m.
CREATE TABLE IF NOT EXISTS link_rollup_5m (
    bucket_ts DateTime,
    link_pk String,
    ingested_at DateTime64(3),

    -- Direction A→Z (probes from side_a to side_z)
    a_avg_rtt_us Float64,
    a_min_rtt_us Float64,
    a_p50_rtt_us Float64,
    a_p90_rtt_us Float64,
    a_p95_rtt_us Float64,
    a_p99_rtt_us Float64,
    a_max_rtt_us Float64,
    a_loss_pct Float64,
    a_samples UInt32,

    -- Direction Z→A (probes from side_z to side_a)
    z_avg_rtt_us Float64,
    z_min_rtt_us Float64,
    z_p50_rtt_us Float64,
    z_p90_rtt_us Float64,
    z_p95_rtt_us Float64,
    z_p99_rtt_us Float64,
    z_max_rtt_us Float64,
    z_loss_pct Float64,
    z_samples UInt32
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (bucket_ts, link_pk);

-- Device interface rollup: counters and traffic from fact_dz_device_interface_counters.
-- Keyed by (device_pk, intf) so consumers can join to links via link_pk/link_side,
-- or aggregate across all interfaces for a device-level view.
CREATE TABLE IF NOT EXISTS device_interface_rollup_5m (
    bucket_ts DateTime,
    device_pk String,
    intf String,
    ingested_at DateTime64(3),

    -- Error/discard counters
    in_errors UInt64,
    out_errors UInt64,
    in_fcs_errors UInt64,
    in_discards UInt64,
    out_discards UInt64,
    carrier_transitions UInt64,

    -- Traffic rates: bits per second
    avg_in_bps Float64,
    min_in_bps Float64,
    p50_in_bps Float64,
    p90_in_bps Float64,
    p95_in_bps Float64,
    p99_in_bps Float64,
    max_in_bps Float64,
    avg_out_bps Float64,
    min_out_bps Float64,
    p50_out_bps Float64,
    p90_out_bps Float64,
    p95_out_bps Float64,
    p99_out_bps Float64,
    max_out_bps Float64,

    -- Traffic rates: packets per second
    avg_in_pps Float64,
    min_in_pps Float64,
    p50_in_pps Float64,
    p90_in_pps Float64,
    p95_in_pps Float64,
    p99_in_pps Float64,
    max_in_pps Float64,
    avg_out_pps Float64,
    min_out_pps Float64,
    p50_out_pps Float64,
    p90_out_pps Float64,
    p95_out_pps Float64,
    p99_out_pps Float64,
    max_out_pps Float64
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (bucket_ts, device_pk, intf);

-- +goose Down

DROP TABLE IF EXISTS link_rollup_5m;
DROP TABLE IF EXISTS device_interface_rollup_5m;
