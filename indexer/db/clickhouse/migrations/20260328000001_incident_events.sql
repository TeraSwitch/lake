-- +goose Up

-- link_incident_events: append-only event stream for link incidents.
-- Each incident groups all symptoms for a single link. Events track the
-- lifecycle: opened, symptom changes, and resolved.
CREATE TABLE IF NOT EXISTS link_incident_events (
    incident_id      String,
    link_pk          String,
    event_type       Enum8('opened' = 1, 'symptom_added' = 2, 'symptom_resolved' = 3, 'resolved' = 4),
    event_ts         DateTime,
    started_at       DateTime,
    active_symptoms  Array(String),
    symptoms         Array(String),
    severity         String,
    peak_values      String,
    link_code        String,
    link_type        String,
    side_a_metro     String,
    side_z_metro     String,
    contributor_code String,
    status           String,
    provisioning     Bool
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (incident_id, event_ts);

-- device_incident_events: append-only event stream for device incidents.
CREATE TABLE IF NOT EXISTS device_incident_events (
    incident_id      String,
    device_pk        String,
    event_type       Enum8('opened' = 1, 'symptom_added' = 2, 'symptom_resolved' = 3, 'resolved' = 4),
    event_ts         DateTime,
    started_at       DateTime,
    active_symptoms  Array(String),
    symptoms         Array(String),
    severity         String,
    peak_values      String,
    device_code      String,
    device_type      String,
    metro            String,
    contributor_code String,
    status           String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_ts)
ORDER BY (incident_id, event_ts);

-- +goose Down

DROP TABLE IF EXISTS link_incident_events;
DROP TABLE IF EXISTS device_incident_events;
