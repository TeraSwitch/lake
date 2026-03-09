-- +goose Up
-- +goose StatementBegin

-- incidents: one row per incident, upserted each detection cycle.
-- ReplacingMergeTree deduplicates by updated_at so re-inserts are idempotent.
CREATE TABLE IF NOT EXISTS incidents
(
    -- Identity (dedup key)
    entity_type      LowCardinality(String),   -- link, device
    entity_pk        String,
    incident_type    LowCardinality(String),    -- packet_loss, errors, discards, carrier, no_data
    started_at       DateTime64(3),

    -- Current state (updated each cycle)
    ended_at         Nullable(DateTime64(3)),
    is_ongoing       Bool                       DEFAULT true,
    confirmed        Bool                       DEFAULT false,
    severity         LowCardinality(String)     DEFAULT '',    -- degraded, incident
    is_drained       Bool                       DEFAULT false,

    -- Entity context
    entity_code      String                     DEFAULT '',
    link_type        Nullable(String),
    side_a_metro     Nullable(String),
    side_z_metro     Nullable(String),
    contributor_code Nullable(String),
    metro            Nullable(String),          -- for devices
    device_type      Nullable(String),          -- for devices
    drain_status     Nullable(String),          -- soft-drained, hard-drained

    -- Metrics
    threshold_pct    Nullable(Float64),
    peak_loss_pct    Nullable(Float64),
    threshold_count  Nullable(Int64),
    peak_count       Nullable(Int64),
    affected_interfaces Array(String),

    -- Duration
    duration_seconds Nullable(Int64),

    -- Versioning for ReplacingMergeTree
    updated_at       DateTime64(3)              DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (entity_type, entity_pk, incident_type, started_at);

-- +goose StatementEnd

-- +goose StatementBegin

-- incident_events: append-only state transitions, deduped by deterministic event_id.
-- ReplacingMergeTree collapses duplicate inserts from detection restarts.
CREATE TABLE IF NOT EXISTS incident_events
(
    -- Deterministic identity (hash of transition details)
    event_id         String,
    event_type       LowCardinality(String),    -- incident.started, incident.cleared, incident.severity_changed,
                                                -- link.drained, link.undrained, link.readiness_changed
    event_ts         DateTime64(3),

    -- Entity
    entity_type      LowCardinality(String),    -- link, device
    entity_pk        String,
    entity_code      String                     DEFAULT '',

    -- Incident context (for incident.* events)
    incident_type    Nullable(String),           -- packet_loss, errors, discards, carrier, no_data
    severity         Nullable(String),
    old_severity     Nullable(String),

    -- Drain context (for link.* events)
    drain_status     Nullable(String),
    old_drain_status Nullable(String),
    readiness        Nullable(String),
    old_readiness    Nullable(String),

    -- Entity context
    link_type        Nullable(String),
    side_a_metro     Nullable(String),
    side_z_metro     Nullable(String),
    contributor_code Nullable(String),
    metro            Nullable(String),
    device_type      Nullable(String),

    -- Metrics snapshot
    threshold_pct    Nullable(Float64),
    peak_loss_pct    Nullable(Float64),
    threshold_count  Nullable(Int64),
    peak_count       Nullable(Int64),

    -- Timing
    incident_started_at Nullable(DateTime64(3)),
    incident_ended_at   Nullable(DateTime64(3)),
    duration_seconds    Nullable(Int64),

    -- Full payload for webhook delivery
    payload          String                     DEFAULT '',

    ingested_at      DateTime64(3)              DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (event_id);

-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS incident_events;
DROP TABLE IF EXISTS incidents;
