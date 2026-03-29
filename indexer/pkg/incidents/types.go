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
)

// Severity levels for incidents.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
)

// --- Link types ---

// LinkSymptom represents a currently active symptom on a link, as detected
// from rollup tables.
type LinkSymptom struct {
	LinkPK       string
	IncidentType string
	PeakValue    float64
	StartedAt    time.Time

	// Metadata snapshot
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
	Status          string
	Provisioning    bool
}

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
}

// OpenLinkIncident represents a non-resolved link incident reconstructed
// from the latest event in the link_incident_events table.
type OpenLinkIncident struct {
	IncidentID     string
	LinkPK         string
	StartedAt      time.Time
	ActiveSymptoms []string
	Symptoms       []string
	Severity       Severity
	LastEventTS    time.Time

	// Metadata from the latest event (carried forward for resolved/clearing events)
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
	Status          string
	Provisioning    bool
}

// --- Device types ---

// DeviceSymptom represents a currently active symptom on a device.
type DeviceSymptom struct {
	DevicePK     string
	IncidentType string
	PeakValue    float64
	StartedAt    time.Time

	// Metadata snapshot
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
}

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
}

// OpenDeviceIncident represents a non-resolved device incident.
type OpenDeviceIncident struct {
	IncidentID     string
	DevicePK       string
	StartedAt      time.Time
	ActiveSymptoms []string
	Symptoms       []string
	Severity       Severity
	LastEventTS    time.Time

	// Metadata from the latest event
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
}

// --- Internal diff types ---
// These are used by the shared diff algorithm. Entity-specific code maps
// to/from these before calling diff.

// symptomInput is a single active symptom for the diff algorithm.
type symptomInput struct {
	EntityPK     string
	IncidentType string
	PeakValue    float64
	StartedAt    time.Time
}

// openState is the current state of an open incident for the diff algorithm.
type openState struct {
	IncidentID     string
	EntityPK       string
	StartedAt      time.Time
	ActiveSymptoms []string
	Symptoms       []string // every symptom ever seen
	Severity       Severity
	LastEventTS    time.Time
}

// eventDelta is a state transition produced by the diff algorithm.
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
