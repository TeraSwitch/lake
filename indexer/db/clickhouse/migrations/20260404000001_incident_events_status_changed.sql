-- +goose Up

-- Add status_changed event type and previous/new status columns for tracking
-- entity status transitions (e.g., drained/undrained) during open incidents.

ALTER TABLE link_incident_events
    MODIFY COLUMN event_type Enum8('opened' = 1, 'symptom_added' = 2, 'symptom_resolved' = 3, 'resolved' = 4, 'status_changed' = 5);

-- +goose StatementBegin
ALTER TABLE link_incident_events
    ADD COLUMN IF NOT EXISTS previous_status String DEFAULT '',
    ADD COLUMN IF NOT EXISTS new_status String DEFAULT '';
-- +goose StatementEnd

ALTER TABLE device_incident_events
    MODIFY COLUMN event_type Enum8('opened' = 1, 'symptom_added' = 2, 'symptom_resolved' = 3, 'resolved' = 4, 'status_changed' = 5);

-- +goose StatementBegin
ALTER TABLE device_incident_events
    ADD COLUMN IF NOT EXISTS previous_status String DEFAULT '',
    ADD COLUMN IF NOT EXISTS new_status String DEFAULT '';
-- +goose StatementEnd

-- +goose Down

ALTER TABLE link_incident_events
    DROP COLUMN IF EXISTS new_status,
    DROP COLUMN IF EXISTS previous_status;

ALTER TABLE link_incident_events
    MODIFY COLUMN event_type Enum8('opened' = 1, 'symptom_added' = 2, 'symptom_resolved' = 3, 'resolved' = 4);

ALTER TABLE device_incident_events
    DROP COLUMN IF EXISTS new_status,
    DROP COLUMN IF EXISTS previous_status;

ALTER TABLE device_incident_events
    MODIFY COLUMN event_type Enum8('opened' = 1, 'symptom_added' = 2, 'symptom_resolved' = 3, 'resolved' = 4);
