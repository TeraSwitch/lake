package incidents

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"
)

// DiffIncidents compares current incidents against previous state and generates events.
// If previous is nil (first run / cold start), no events are emitted.
func DiffIncidents(current, previous []Incident, now time.Time) []IncidentEvent {
	if previous == nil {
		return nil
	}

	prevMap := make(map[string]Incident, len(previous))
	for _, inc := range previous {
		prevMap[inc.IncidentKey()] = inc
	}

	currMap := make(map[string]Incident, len(current))
	for _, inc := range current {
		currMap[inc.IncidentKey()] = inc
	}

	var events []IncidentEvent

	// New or changed incidents
	for key, curr := range currMap {
		prev, existed := prevMap[key]

		if !existed {
			// incident.started
			events = append(events, newIncidentEvent(EventIncidentStarted, curr, now))
		} else {
			// Check for severity change
			if prev.Severity != curr.Severity && prev.Severity != "" && curr.Severity != "" {
				events = append(events, severityChangedEvent(curr, prev.Severity, now))
			}

			// Check for drain status change
			prevDrained := prev.IsDrained
			currDrained := curr.IsDrained
			if !prevDrained && currDrained {
				events = append(events, drainEvent(EventLinkDrained, curr, now))
			} else if prevDrained && !currDrained {
				events = append(events, drainEvent(EventLinkUndrained, curr, now))
			}
		}
	}

	// Cleared incidents (in previous but not in current as ongoing)
	for key, prev := range prevMap {
		if !prev.IsOngoing {
			continue
		}
		curr, exists := currMap[key]
		if !exists || !curr.IsOngoing {
			events = append(events, clearedEvent(prev, curr, now))
		}
	}

	return events
}

func newIncidentEvent(eventType string, inc Incident, now time.Time) IncidentEvent {
	payload, _ := json.Marshal(inc)
	return IncidentEvent{
		EventID:    deterministicEventID(inc.EntityPK, inc.IncidentType, inc.StartedAt.Format(time.RFC3339), eventType),
		EventType:  eventType,
		EventTS:    now,
		EntityType: inc.EntityType,
		EntityPK:   inc.EntityPK,
		EntityCode: inc.EntityCode,

		IncidentType: &inc.IncidentType,
		Severity:     strPtrNonEmpty(inc.Severity),

		LinkType:        inc.LinkType,
		SideAMetro:      inc.SideAMetro,
		SideZMetro:      inc.SideZMetro,
		ContributorCode: inc.ContributorCode,
		Metro:           inc.Metro,
		DeviceType:      inc.DeviceType,

		ThresholdPct:   inc.ThresholdPct,
		PeakLossPct:    inc.PeakLossPct,
		ThresholdCount: inc.ThresholdCount,
		PeakCount:      inc.PeakCount,

		IncidentStartedAt: &inc.StartedAt,
		IncidentEndedAt:   inc.EndedAt,
		DurationSeconds:   inc.DurationSeconds,

		Payload: string(payload),
	}
}

func clearedEvent(prev, curr Incident, now time.Time) IncidentEvent {
	// Use curr if available (has end time), otherwise prev
	inc := prev
	if curr.EntityPK != "" {
		inc = curr
	}

	endedAtStr := ""
	if inc.EndedAt != nil {
		endedAtStr = inc.EndedAt.Format(time.RFC3339)
	}

	payload, _ := json.Marshal(inc)
	return IncidentEvent{
		EventID:    deterministicEventID(inc.EntityPK, inc.IncidentType, inc.StartedAt.Format(time.RFC3339), EventIncidentCleared, endedAtStr),
		EventType:  EventIncidentCleared,
		EventTS:    now,
		EntityType: inc.EntityType,
		EntityPK:   inc.EntityPK,
		EntityCode: inc.EntityCode,

		IncidentType: &inc.IncidentType,
		Severity:     strPtrNonEmpty(inc.Severity),

		LinkType:        inc.LinkType,
		SideAMetro:      inc.SideAMetro,
		SideZMetro:      inc.SideZMetro,
		ContributorCode: inc.ContributorCode,
		Metro:           inc.Metro,
		DeviceType:      inc.DeviceType,

		ThresholdPct:   inc.ThresholdPct,
		PeakLossPct:    inc.PeakLossPct,
		ThresholdCount: inc.ThresholdCount,
		PeakCount:      inc.PeakCount,

		IncidentStartedAt: &inc.StartedAt,
		IncidentEndedAt:   inc.EndedAt,
		DurationSeconds:   inc.DurationSeconds,

		Payload: string(payload),
	}
}

func severityChangedEvent(inc Incident, oldSeverity string, now time.Time) IncidentEvent {
	payload, _ := json.Marshal(inc)
	return IncidentEvent{
		EventID:    deterministicEventID(inc.EntityPK, inc.IncidentType, inc.StartedAt.Format(time.RFC3339), EventIncidentSeverityChanged, oldSeverity, inc.Severity),
		EventType:  EventIncidentSeverityChanged,
		EventTS:    now,
		EntityType: inc.EntityType,
		EntityPK:   inc.EntityPK,
		EntityCode: inc.EntityCode,

		IncidentType: &inc.IncidentType,
		Severity:     strPtrNonEmpty(inc.Severity),
		OldSeverity:  &oldSeverity,

		LinkType:        inc.LinkType,
		SideAMetro:      inc.SideAMetro,
		SideZMetro:      inc.SideZMetro,
		ContributorCode: inc.ContributorCode,
		Metro:           inc.Metro,
		DeviceType:      inc.DeviceType,

		ThresholdPct:   inc.ThresholdPct,
		PeakLossPct:    inc.PeakLossPct,
		ThresholdCount: inc.ThresholdCount,
		PeakCount:      inc.PeakCount,

		IncidentStartedAt: &inc.StartedAt,
		DurationSeconds:   inc.DurationSeconds,

		Payload: string(payload),
	}
}

func drainEvent(eventType string, inc Incident, now time.Time) IncidentEvent {
	payload, _ := json.Marshal(inc)
	return IncidentEvent{
		EventID:    deterministicEventID(inc.EntityPK, eventType, ptrStr(inc.DrainStatus), inc.StartedAt.Format(time.RFC3339)),
		EventType:  eventType,
		EventTS:    now,
		EntityType: inc.EntityType,
		EntityPK:   inc.EntityPK,
		EntityCode: inc.EntityCode,

		IncidentType: &inc.IncidentType,
		DrainStatus:  inc.DrainStatus,

		LinkType:        inc.LinkType,
		SideAMetro:      inc.SideAMetro,
		SideZMetro:      inc.SideZMetro,
		ContributorCode: inc.ContributorCode,
		Metro:           inc.Metro,
		DeviceType:      inc.DeviceType,

		IncidentStartedAt: &inc.StartedAt,

		Payload: string(payload),
	}
}

// deterministicEventID creates a deterministic hash-based event ID from the given components.
func deterministicEventID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:32]
}

func strPtrNonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func ptrStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
