package notifier

import (
	"context"
	"encoding/json"
	"time"
)

// Checkpoint tracks polling progress for a source.
type Checkpoint struct {
	LastEventTS time.Time
	LastSlot    uint64
}

// EventGroup is a set of related events (e.g. from the same transaction)
// that should be delivered as a single notification.
type EventGroup struct {
	Key     string  `json:"key"`     // grouping key (e.g. tx_signature)
	Summary string  `json:"summary"` // human-readable summary line
	Events  []Event `json:"events"`  // individual events in the group
}

// Event is a single event within a group.
type Event struct {
	Type    string         `json:"type"`
	Details map[string]any `json:"details"`
}

// Source polls for new events from a data source.
type Source interface {
	// Type returns the source type identifier (e.g. "escrow_events").
	Type() string

	// Poll returns new event groups since the given checkpoint.
	Poll(ctx context.Context, cp Checkpoint) ([]EventGroup, Checkpoint, error)

	// Filter applies source-specific exclusion filters to event groups.
	// The filters JSON schema is defined by each source implementation.
	Filter(groups []EventGroup, filters json.RawMessage) []EventGroup
}
