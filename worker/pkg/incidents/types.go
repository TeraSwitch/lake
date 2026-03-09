package incidents

import "time"

// Incident represents a single incident stored in the ClickHouse incidents table.
type Incident struct {
	EntityType   string    `json:"entity_type"` // link, device
	EntityPK     string    `json:"entity_pk"`
	IncidentType string    `json:"incident_type"` // packet_loss, errors, discards, carrier, no_data
	StartedAt    time.Time `json:"started_at"`

	EndedAt   *time.Time `json:"ended_at,omitempty"`
	IsOngoing bool       `json:"is_ongoing"`
	Confirmed bool       `json:"confirmed"`
	Severity  string     `json:"severity"` // degraded, incident
	IsDrained bool       `json:"is_drained"`

	EntityCode      string  `json:"entity_code"`
	LinkType        *string `json:"link_type,omitempty"`
	SideAMetro      *string `json:"side_a_metro,omitempty"`
	SideZMetro      *string `json:"side_z_metro,omitempty"`
	ContributorCode *string `json:"contributor_code,omitempty"`
	Metro           *string `json:"metro,omitempty"`
	DeviceType      *string `json:"device_type,omitempty"`
	DrainStatus     *string `json:"drain_status,omitempty"`

	ThresholdPct       *float64 `json:"threshold_pct,omitempty"`
	PeakLossPct        *float64 `json:"peak_loss_pct,omitempty"`
	ThresholdCount     *int64   `json:"threshold_count,omitempty"`
	PeakCount          *int64   `json:"peak_count,omitempty"`
	AffectedInterfaces []string `json:"affected_interfaces,omitempty"`

	DurationSeconds *int64    `json:"duration_seconds,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// IncidentKey returns the unique identity of this incident for diffing.
func (i Incident) IncidentKey() string {
	return i.EntityType + ":" + i.EntityPK + ":" + i.IncidentType + ":" + i.StartedAt.UTC().Format(time.RFC3339)
}

// Event types for incident_events.
const (
	EventIncidentStarted         = "incident.started"
	EventIncidentCleared         = "incident.cleared"
	EventIncidentSeverityChanged = "incident.severity_changed"
	EventLinkDrained             = "link.drained"
	EventLinkUndrained           = "link.undrained"
	EventLinkReadinessChanged    = "link.readiness_changed"
)

// IncidentEvent represents a state transition stored in the incident_events table.
type IncidentEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	EventTS   time.Time `json:"event_ts"`

	EntityType string `json:"entity_type"`
	EntityPK   string `json:"entity_pk"`
	EntityCode string `json:"entity_code"`

	IncidentType *string `json:"incident_type,omitempty"`
	Severity     *string `json:"severity,omitempty"`
	OldSeverity  *string `json:"old_severity,omitempty"`

	DrainStatus    *string `json:"drain_status,omitempty"`
	OldDrainStatus *string `json:"old_drain_status,omitempty"`
	Readiness      *string `json:"readiness,omitempty"`
	OldReadiness   *string `json:"old_readiness,omitempty"`

	LinkType        *string `json:"link_type,omitempty"`
	SideAMetro      *string `json:"side_a_metro,omitempty"`
	SideZMetro      *string `json:"side_z_metro,omitempty"`
	ContributorCode *string `json:"contributor_code,omitempty"`
	Metro           *string `json:"metro,omitempty"`
	DeviceType      *string `json:"device_type,omitempty"`

	ThresholdPct   *float64 `json:"threshold_pct,omitempty"`
	PeakLossPct    *float64 `json:"peak_loss_pct,omitempty"`
	ThresholdCount *int64   `json:"threshold_count,omitempty"`
	PeakCount      *int64   `json:"peak_count,omitempty"`

	IncidentStartedAt *time.Time `json:"incident_started_at,omitempty"`
	IncidentEndedAt   *time.Time `json:"incident_ended_at,omitempty"`
	DurationSeconds   *int64     `json:"duration_seconds,omitempty"`

	Payload string `json:"payload"`
}

// DetectionParams holds configurable detection parameters.
type DetectionParams struct {
	MinDuration time.Duration // minimum consecutive duration above threshold (default 30m)
	CoalesceGap time.Duration // gap between incidents to merge (default 720m/12h)

	PacketLossThreshold float64 // default 10.0
	ErrorsThreshold     int64   // default 10
	DiscardsThreshold   int64   // default 10
	CarrierThreshold    int64   // default 1
}

// DefaultDetectionParams returns default detection parameters.
func DefaultDetectionParams() DetectionParams {
	return DetectionParams{
		MinDuration:         30 * time.Minute,
		CoalesceGap:         720 * time.Minute,
		PacketLossThreshold: 10.0,
		ErrorsThreshold:     10,
		DiscardsThreshold:   10,
		CarrierThreshold:    1,
	}
}

const bucketInterval = 5 * time.Minute

// MinBuckets returns the minimum number of consecutive 5-min buckets for the configured duration.
func (p DetectionParams) MinBuckets() int {
	n := int(p.MinDuration / bucketInterval)
	if n < 1 {
		return 1
	}
	return n
}

// linkMeta contains link info for enriching detected incidents.
type linkMeta struct {
	LinkPK          string
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
	Status          string
}

// deviceMeta contains device info for enriching detected incidents.
type deviceMeta struct {
	DevicePK        string
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
}

// lossBucket is a 5-minute aggregation of packet loss data.
type lossBucket struct {
	LinkPK      string
	Bucket      time.Time
	LossPct     float64
	SampleCount uint64
}

// counterBucket is a 5-minute aggregation of counter metrics.
type counterBucket struct {
	EntityPK string
	Bucket   time.Time
	Value    int64
}

// drainedPeriod represents a time period when a link/device was in drained state.
type drainedPeriod struct {
	Start time.Time
	End   *time.Time // nil if still drained
}
