// Package incidents runs a Temporal workflow that detects incident state
// transitions from ClickHouse rollup data and writes append-only events
// to the link_incident_events and device_incident_events tables. It is
// designed to be embedded in the indexer process alongside the rollup worker.
package incidents

import (
	"slices"
	"time"
)

const (
	// TaskQueue is the Temporal task queue for incident detection workflows.
	TaskQueue = "indexer-incidents"
	// WorkflowID is the Temporal workflow ID for the long-running detection workflow.
	WorkflowID = "indexer-incidents"
)

// EventType represents a lifecycle event in an incident.
type EventType string

const (
	EventOpened          EventType = "opened"
	EventSymptomAdded    EventType = "symptom_added"
	EventSymptomResolved EventType = "symptom_resolved"
	EventResolved        EventType = "resolved"
	EventStatusChanged   EventType = "status_changed"
)

// Severity levels for incidents.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
)

// --- Link types ---

// LinkIncidentEvent represents a single row in the link_incident_events table.
type LinkIncidentEvent struct {
	IncidentID      string
	LinkPK          string
	EventType       EventType
	EventTS         time.Time
	StartedAt       time.Time
	ActiveSymptoms  []string // currently active symptoms
	Symptoms        []string // every symptom ever seen in this incident
	Severity        Severity
	PeakValues      string // JSON
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
	Status          string
	Provisioning    bool
	PreviousStatus  string // only for status_changed events
	NewStatus       string // only for status_changed events
}

// --- Device types ---

// DeviceIncidentEvent represents a single row in the device_incident_events table.
type DeviceIncidentEvent struct {
	IncidentID      string
	DevicePK        string
	EventType       EventType
	EventTS         time.Time
	StartedAt       time.Time
	ActiveSymptoms  []string // currently active symptoms
	Symptoms        []string // every symptom ever seen in this incident
	Severity        Severity
	PeakValues      string // JSON
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
	PreviousStatus  string // only for status_changed events
	NewStatus       string // only for status_changed events
}

// --- Detection workflow state ---

// DetectionState is passed through Temporal ContinueAsNew to track the
// detection loop's position. Zero watermarks mean "cold start".
type DetectionState struct {
	LatencyWatermark time.Time // how far latency-based detection has been processed
	TrafficWatermark time.Time // how far traffic-based detection has been processed
	Iteration        int
}

// RollupFreshness reports per-pipeline freshness from ingestion logs.
type RollupFreshness struct {
	LatestBucket      time.Time // max of both, used for watermark advancement
	LatencyFreshUntil time.Time // latest source_max_event_ts for RollupLinks
	TrafficFreshUntil time.Time // latest source_max_event_ts for RollupDeviceInterfaces
}

// eventDelta is a state transition produced by the backfill event generator.
type eventDelta struct {
	IncidentID     string
	EntityPK       string
	EventType      EventType
	EventTS        time.Time
	StartedAt      time.Time
	ActiveSymptoms []string
	Symptoms       []string // every symptom ever seen
	Severity       Severity
	PeakValues     string // JSON
	PreviousStatus string // only for status_changed events
	NewStatus      string // only for status_changed events
}

// statusTransition represents an entity status change detected from history tables.
type statusTransition struct {
	PreviousStatus string
	NewStatus      string
	ChangedTS      time.Time
}

// SymptomType constants match the incident_type values from the ClickHouse data.
const (
	SymptomPacketLoss      = "packet_loss"
	SymptomISISDown        = "isis_down"
	SymptomISISOverload    = "isis_overload"
	SymptomISISUnreachable = "isis_unreachable"
	SymptomNoLatencyData   = "no_latency_data"
	SymptomNoTrafficData   = "no_traffic_data"
	SymptomErrors          = "errors"
	SymptomFCS             = "fcs"
	SymptomDiscards        = "discards"
	SymptomCarrier         = "carrier"
)

// symptomSeverity returns the severity for a symptom based on type, magnitude,
// and how long it has been active.
func symptomSeverity(symptomType string, peakValue float64, duration time.Duration, escalationThreshold time.Duration) Severity {
	sustained := duration >= escalationThreshold

	switch symptomType {
	case SymptomISISDown, SymptomISISOverload, SymptomISISUnreachable:
		return SeverityCritical
	case SymptomPacketLoss:
		if peakValue >= 10 && sustained {
			return SeverityCritical
		}
		return SeverityWarning
	case SymptomCarrier, SymptomFCS, SymptomNoLatencyData, SymptomNoTrafficData:
		if sustained {
			return SeverityCritical
		}
		return SeverityWarning
	default:
		return SeverityWarning
	}
}

// maxSeverity returns the highest severity from a list.
func maxSeverity(severities ...Severity) Severity {
	if slices.Contains(severities, SeverityCritical) {
		return SeverityCritical
	}
	return SeverityWarning
}
