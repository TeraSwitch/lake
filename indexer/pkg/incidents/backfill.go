package incidents

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/sdk/activity"
)

// safeHeartbeat records a heartbeat if running inside a Temporal activity context.
func safeHeartbeat(ctx context.Context, details ...any) {
	defer func() { recover() }() //nolint:errcheck
	activity.RecordHeartbeat(ctx, details...)
}

// BackfillInput configures a backfill run.
type BackfillInput struct {
	StartTime time.Time
	EndTime   time.Time
	ChunkSize time.Duration // default 1 day
	Overwrite bool          // if true, delete existing events in range before inserting
	Clean     bool          // if true, delete all events in the time range before backfilling
}

// BackfillChunkInput configures a single backfill chunk.
type BackfillChunkInput struct {
	WindowStart       time.Time
	WindowEnd         time.Time
	Overwrite         bool
	LatencyFreshUntil time.Time // zero = no suppression
	TrafficFreshUntil time.Time // zero = no suppression
}

const (
	// backfillCoalesceGapMinutes is the coalesce gap used for backfill (30 min).
	backfillCoalesceGapMinutes = 30
)

// CleanTimeRange deletes all incident events where started_at falls within
// the given range. Used before backfilling to ensure a clean slate.
type CleanTimeRangeInput struct {
	StartTime time.Time
	EndTime   time.Time
}

func (a *Activities) CleanLinkIncidents(ctx context.Context, input CleanTimeRangeInput) error {
	safeHeartbeat(ctx, "clean link incidents", input.StartTime, input.EndTime)
	// Align start down to 5-minute boundary to cover any events created from
	// aligned bucket windows (the backfill rounds down to 5-minute boundaries).
	query := `ALTER TABLE link_incident_events DELETE WHERE started_at >= toStartOfFiveMinutes(toDateTime($1)) AND started_at < $2`
	if err := a.ClickHouse.Exec(ctx, query, input.StartTime, input.EndTime); err != nil {
		return fmt.Errorf("clean link incidents: %w", err)
	}
	return nil
}

func (a *Activities) CleanDeviceIncidents(ctx context.Context, input CleanTimeRangeInput) error {
	safeHeartbeat(ctx, "clean device incidents", input.StartTime, input.EndTime)
	query := `ALTER TABLE device_incident_events DELETE WHERE started_at >= toStartOfFiveMinutes(toDateTime($1)) AND started_at < $2`
	if err := a.ClickHouse.Exec(ctx, query, input.StartTime, input.EndTime); err != nil {
		return fmt.Errorf("clean device incidents: %w", err)
	}
	return nil
}

// --- Backfill symptom window types ---

// backfillSymptomWindow represents a single symptom's coalesced time window
// within an incident, as returned by the backfill SQL query.
type backfillSymptomWindow struct {
	IncidentID    string
	EntityPK      string
	Symptom       string
	StartedAt     time.Time
	EndedAt       time.Time
	PeakValue     float64
	IncidentStart time.Time
	IncidentEnd   time.Time
	IsExisting    bool // true if this incident continues an existing open incident
}

// backfillLinkMeta holds entity metadata for a backfilled link incident.
type backfillLinkMeta struct {
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
	Status          string
	Provisioning    bool
}

// backfillDeviceMeta holds entity metadata for a backfilled device incident.
type backfillDeviceMeta struct {
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
}

// --- Event generation from symptom windows ---

// generateTransitionEvents walks symptom windows for a single incident
// chronologically and produces events at each state transition.
func (a *Activities) generateTransitionEvents(
	incidentID, entityPK string,
	incidentStart, incidentEnd time.Time,
	windows []backfillSymptomWindow,
	isExisting bool,
	chunkEnd time.Time,
) []eventDelta {
	if len(windows) == 0 {
		return nil
	}

	// Collect all unique transition timestamps.
	// Exclude timestamps at or beyond the chunk end — those are boundary
	// effects from the SQL clipping, not real symptom transitions.
	tsSet := make(map[time.Time]struct{})
	for _, w := range windows {
		tsSet[w.StartedAt] = struct{}{}
		if w.EndedAt.Before(chunkEnd) {
			tsSet[w.EndedAt] = struct{}{}
		}
	}
	timestamps := make([]time.Time, 0, len(tsSet))
	for ts := range tsSet {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i].Before(timestamps[j]) })

	var events []eventDelta
	activeSymptoms := make(map[string]struct{})
	allSymptoms := make(map[string]struct{})
	peakValues := make(map[string]float64)
	isFirst := !isExisting

	for _, ts := range timestamps {
		// Find symptoms ending at this timestamp.
		var removed []string
		for _, w := range windows {
			if w.EndedAt.Equal(ts) {
				delete(activeSymptoms, w.Symptom)
				removed = append(removed, w.Symptom)
			}
		}

		// Find symptoms starting at this timestamp.
		var added []string
		for _, w := range windows {
			if w.StartedAt.Equal(ts) {
				activeSymptoms[w.Symptom] = struct{}{}
				allSymptoms[w.Symptom] = struct{}{}
				if w.PeakValue > peakValues[w.Symptom] {
					peakValues[w.Symptom] = w.PeakValue
				}
				added = append(added, w.Symptom)
			}
		}

		if len(removed) == 0 && len(added) == 0 {
			continue
		}

		// Build sorted slices.
		activeSorted := sortedKeys(activeSymptoms)
		allSorted := sortedKeys(allSymptoms)
		severity := a.computeSeverity(activeSorted, peakValues, incidentStart, ts)
		if len(activeSorted) == 0 {
			// Use all symptoms for severity when resolving (matches live detector).
			severity = a.computeSeverity(allSorted, peakValues, incidentStart, ts)
		}
		peakJSON := marshalPeakValues(peakValues)

		// Determine event type.
		if isFirst && len(added) > 0 {
			// First event for a new incident.
			events = append(events, eventDelta{
				IncidentID:     incidentID,
				EntityPK:       entityPK,
				EventType:      EventOpened,
				EventTS:        ts,
				StartedAt:      incidentStart,
				ActiveSymptoms: activeSorted,
				Symptoms:       allSorted,
				Severity:       severity,
				PeakValues:     peakJSON,
			})
			isFirst = false
			continue
		}

		// Emit symptom_resolved if symptoms were removed.
		if len(removed) > 0 {
			// Check if this is the final resolution: all symptoms gone, at the
			// incident end time, and the coalesce gap has elapsed before chunk end.
			resolvedEnough := !incidentEnd.Add(time.Duration(backfillCoalesceGapMinutes) * time.Minute).After(chunkEnd)
			if len(activeSorted) == 0 && ts.Equal(incidentEnd) && resolvedEnough {
				// Cap the resolved timestamp to chunkEnd so it never
				// exceeds the processing window.
				resolvedTS := ts
				if resolvedTS.After(chunkEnd) {
					resolvedTS = chunkEnd
				}
				events = append(events, eventDelta{
					IncidentID:     incidentID,
					EntityPK:       entityPK,
					EventType:      EventResolved,
					EventTS:        resolvedTS,
					StartedAt:      incidentStart,
					ActiveSymptoms: activeSorted,
					Symptoms:       allSorted,
					Severity:       severity,
					PeakValues:     peakJSON,
				})
				continue
			}
			events = append(events, eventDelta{
				IncidentID:     incidentID,
				EntityPK:       entityPK,
				EventType:      EventSymptomResolved,
				EventTS:        ts,
				StartedAt:      incidentStart,
				ActiveSymptoms: activeSorted,
				Symptoms:       allSorted,
				Severity:       severity,
				PeakValues:     peakJSON,
			})
		}

		// Emit symptom_added if symptoms were added.
		if len(added) > 0 {
			events = append(events, eventDelta{
				IncidentID:     incidentID,
				EntityPK:       entityPK,
				EventType:      EventSymptomAdded,
				EventTS:        ts,
				StartedAt:      incidentStart,
				ActiveSymptoms: activeSorted,
				Symptoms:       allSorted,
				Severity:       severity,
				PeakValues:     peakJSON,
			})
		}
	}

	return events
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// filterStaleNoDataWindows removes no_latency_data and no_traffic_data symptom
// windows whose start time is at or past the respective pipeline's freshness
// cutoff. This prevents false missing-data incidents when a pipeline is behind.
// Zero cutoff means no suppression for that symptom type.
func filterStaleNoDataWindows(windows []backfillSymptomWindow, latencyCutoff, trafficCutoff time.Time) []backfillSymptomWindow {
	if latencyCutoff.IsZero() && trafficCutoff.IsZero() {
		return windows
	}
	filtered := make([]backfillSymptomWindow, 0, len(windows))
	for _, w := range windows {
		switch w.Symptom {
		case SymptomNoLatencyData:
			if !latencyCutoff.IsZero() && !w.StartedAt.Before(latencyCutoff) {
				continue
			}
		case SymptomNoTrafficData:
			if !trafficCutoff.IsZero() && !w.StartedAt.Before(trafficCutoff) {
				continue
			}
		}
		filtered = append(filtered, w)
	}
	return filtered
}

// --- Link backfill ---

// BackfillLinkChunk reconstructs historical link incidents from rollup data,
// generating full event timelines (opened, symptom_added, symptom_resolved, resolved).
func (a *Activities) BackfillLinkChunk(ctx context.Context, input BackfillChunkInput) error {
	safeHeartbeat(ctx, "backfill link chunk", input.WindowStart, input.WindowEnd)

	if input.Overwrite {
		deleteQuery := `ALTER TABLE link_incident_events DELETE WHERE started_at >= $1 AND started_at < $2`
		if err := a.ClickHouse.Exec(ctx, deleteQuery, input.WindowStart, input.WindowEnd); err != nil {
			return fmt.Errorf("delete existing link events: %w", err)
		}
		a.Log.Info("backfill: deleted existing link events", "start", input.WindowStart, "end", input.WindowEnd)
	}

	// Query symptom windows per incident using the same CTE chain as before,
	// but SELECT into Go instead of INSERT.
	query := linkSymptomWindowsQuery
	rows, err := a.ClickHouse.Query(ctx, query, input.WindowStart, input.WindowEnd, backfillCoalesceGapMinutes)
	if err != nil {
		return fmt.Errorf("query link symptom windows: %w", err)
	}
	defer rows.Close()

	safeHeartbeat(ctx, "parsing link symptom windows")

	// Parse results, grouped by incident_id.
	type linkIncident struct {
		windows []backfillSymptomWindow
		meta    backfillLinkMeta
	}
	incidents := make(map[string]*linkIncident)
	var incidentOrder []string

	for rows.Next() {
		var w backfillSymptomWindow
		var meta backfillLinkMeta
		if err := rows.Scan(
			&w.IncidentID, &w.EntityPK, &w.Symptom,
			&w.StartedAt, &w.EndedAt, &w.PeakValue,
			&w.IncidentStart, &w.IncidentEnd, &w.IsExisting,
			&meta.LinkCode, &meta.LinkType, &meta.SideAMetro, &meta.SideZMetro,
			&meta.ContributorCode, &meta.Status, &meta.Provisioning,
		); err != nil {
			return fmt.Errorf("scan link symptom window: %w", err)
		}
		w.StartedAt = w.StartedAt.UTC()
		w.EndedAt = w.EndedAt.UTC()
		w.IncidentStart = w.IncidentStart.UTC()
		w.IncidentEnd = w.IncidentEnd.UTC()

		inc, ok := incidents[w.IncidentID]
		if !ok {
			inc = &linkIncident{meta: meta}
			incidents[w.IncidentID] = inc
			incidentOrder = append(incidentOrder, w.IncidentID)
		}
		inc.windows = append(inc.windows, w)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate link symptom windows: %w", err)
	}

	// Filter out no_*_data symptoms for windows past the pipeline freshness cutoff.
	for _, inc := range incidents {
		inc.windows = filterStaleNoDataWindows(inc.windows, input.LatencyFreshUntil, input.TrafficFreshUntil)
	}

	safeHeartbeat(ctx, fmt.Sprintf("generating events for %d link incidents", len(incidents)))

	// Check which incidents already have events (idempotency).
	existingIDs, err := a.existingIncidentIDs(ctx, "link_incident_events", input.WindowStart)
	if err != nil {
		return fmt.Errorf("check existing link incidents: %w", err)
	}

	// Generate events for each incident.
	var allEvents []LinkIncidentEvent
	for _, incID := range incidentOrder {
		inc := incidents[incID]
		if len(inc.windows) == 0 {
			continue
		}

		w0 := inc.windows[0]

		// Skip if this incident already has events (unless overwrite).
		if !input.Overwrite && existingIDs[incID] {
			continue
		}

		deltas := a.generateTransitionEvents(
			incID, w0.EntityPK,
			w0.IncidentStart, w0.IncidentEnd,
			inc.windows, w0.IsExisting,
			input.WindowEnd,
		)

		for _, d := range deltas {
			allEvents = append(allEvents, LinkIncidentEvent{
				IncidentID:      d.IncidentID,
				LinkPK:          d.EntityPK,
				EventType:       d.EventType,
				EventTS:         d.EventTS,
				StartedAt:       d.StartedAt,
				ActiveSymptoms:  d.ActiveSymptoms,
				Symptoms:        d.Symptoms,
				Severity:        d.Severity,
				PeakValues:      d.PeakValues,
				LinkCode:        inc.meta.LinkCode,
				LinkType:        inc.meta.LinkType,
				SideAMetro:      inc.meta.SideAMetro,
				SideZMetro:      inc.meta.SideZMetro,
				ContributorCode: inc.meta.ContributorCode,
				Status:          inc.meta.Status,
				Provisioning:    inc.meta.Provisioning,
			})
		}
	}

	if len(allEvents) > 0 {
		if err := a.writeLinkEvents(ctx, allEvents); err != nil {
			return fmt.Errorf("write link backfill events: %w", err)
		}
		a.Log.Info("backfill: wrote link events", "count", len(allEvents), "incidents", len(incidents))
	}

	// Resolve stale open incidents that have no symptoms in this chunk.
	if err := a.resolveStaleLinks(ctx, input); err != nil {
		return err
	}

	a.Log.Info("backfill: link chunk complete", "start", input.WindowStart, "end", input.WindowEnd)
	return nil
}

func (a *Activities) resolveStaleLinks(ctx context.Context, input BackfillChunkInput) error {
	resolveStaleQuery := `
		INSERT INTO link_incident_events (
			incident_id, link_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			link_code, link_type, side_a_metro, side_z_metro,
			contributor_code, status, provisioning
		)
		SELECT
			ie.incident_id, ie.link_pk, 'resolved',
			least(toDateTime($1) + INTERVAL $3 MINUTE, toDateTime($2)), ie.started_at,
			arrayResize(emptyArrayString(), 0), ie.symptoms, ie.severity, ie.peak_values,
			ie.link_code, ie.link_type, ie.side_a_metro, ie.side_z_metro,
			ie.contributor_code, ie.status, ie.provisioning
		FROM link_incident_events ie
		INNER JOIN (
			SELECT incident_id, max(event_ts) AS max_ts
			FROM link_incident_events GROUP BY incident_id
		) latest ON ie.incident_id = latest.incident_id AND ie.event_ts = latest.max_ts
		WHERE ie.event_type != 'resolved'
		  AND ie.event_ts < toDateTime($1)
		  AND ie.link_pk NOT IN (
			SELECT DISTINCT link_pk FROM link_rollup_5m FINAL
			WHERE bucket_ts >= $1 AND bucket_ts < $2
			  AND greatest(a_loss_pct, z_loss_pct) > 0
		  )
		  AND ie.link_pk NOT IN (
			SELECT DISTINCT link_pk FROM link_rollup_5m FINAL
			WHERE bucket_ts >= $1 AND bucket_ts < $2
			  AND isis_down = true
		  )
		  AND ie.link_pk NOT IN (
			SELECT DISTINCT link_pk FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= $1 AND bucket_ts < $2
			  AND link_pk != ''
			  AND (in_errors + out_errors >= 1 OR in_fcs_errors >= 1
			    OR in_discards + out_discards >= 1 OR carrier_transitions >= 1)
		  )
	`
	if err := a.ClickHouse.Exec(ctx, resolveStaleQuery, input.WindowStart, input.WindowEnd, backfillCoalesceGapMinutes); err != nil {
		return fmt.Errorf("resolve stale link incidents: %w", err)
	}
	return nil
}

// --- Device backfill ---

// BackfillDeviceChunk reconstructs historical device incidents from rollup data.
func (a *Activities) BackfillDeviceChunk(ctx context.Context, input BackfillChunkInput) error {
	safeHeartbeat(ctx, "backfill device chunk", input.WindowStart, input.WindowEnd)

	if input.Overwrite {
		deleteQuery := `ALTER TABLE device_incident_events DELETE WHERE started_at >= $1 AND started_at < $2`
		if err := a.ClickHouse.Exec(ctx, deleteQuery, input.WindowStart, input.WindowEnd); err != nil {
			return fmt.Errorf("delete existing device events: %w", err)
		}
	}

	query := deviceSymptomWindowsQuery
	rows, err := a.ClickHouse.Query(ctx, query, input.WindowStart, input.WindowEnd, backfillCoalesceGapMinutes)
	if err != nil {
		return fmt.Errorf("query device symptom windows: %w", err)
	}
	defer rows.Close()

	safeHeartbeat(ctx, "parsing device symptom windows")

	type deviceIncident struct {
		windows []backfillSymptomWindow
		meta    backfillDeviceMeta
	}
	incidents := make(map[string]*deviceIncident)
	var incidentOrder []string

	for rows.Next() {
		var w backfillSymptomWindow
		var meta backfillDeviceMeta
		if err := rows.Scan(
			&w.IncidentID, &w.EntityPK, &w.Symptom,
			&w.StartedAt, &w.EndedAt, &w.PeakValue,
			&w.IncidentStart, &w.IncidentEnd, &w.IsExisting,
			&meta.DeviceCode, &meta.DeviceType, &meta.Metro,
			&meta.ContributorCode, &meta.Status,
		); err != nil {
			return fmt.Errorf("scan device symptom window: %w", err)
		}
		w.StartedAt = w.StartedAt.UTC()
		w.EndedAt = w.EndedAt.UTC()
		w.IncidentStart = w.IncidentStart.UTC()
		w.IncidentEnd = w.IncidentEnd.UTC()

		inc, ok := incidents[w.IncidentID]
		if !ok {
			inc = &deviceIncident{meta: meta}
			incidents[w.IncidentID] = inc
			incidentOrder = append(incidentOrder, w.IncidentID)
		}
		inc.windows = append(inc.windows, w)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate device symptom windows: %w", err)
	}

	// Filter out no_*_data symptoms for windows past the pipeline freshness cutoff.
	for _, inc := range incidents {
		inc.windows = filterStaleNoDataWindows(inc.windows, input.LatencyFreshUntil, input.TrafficFreshUntil)
	}

	safeHeartbeat(ctx, fmt.Sprintf("generating events for %d device incidents", len(incidents)))

	existingIDs, err := a.existingIncidentIDs(ctx, "device_incident_events", input.WindowStart)
	if err != nil {
		return fmt.Errorf("check existing device incidents: %w", err)
	}

	var allEvents []DeviceIncidentEvent
	for _, incID := range incidentOrder {
		inc := incidents[incID]
		if len(inc.windows) == 0 {
			continue
		}

		w0 := inc.windows[0]

		if !input.Overwrite && existingIDs[incID] {
			continue
		}

		deltas := a.generateTransitionEvents(
			incID, w0.EntityPK,
			w0.IncidentStart, w0.IncidentEnd,
			inc.windows, w0.IsExisting,
			input.WindowEnd,
		)

		for _, d := range deltas {
			allEvents = append(allEvents, DeviceIncidentEvent{
				IncidentID:      d.IncidentID,
				DevicePK:        d.EntityPK,
				EventType:       d.EventType,
				EventTS:         d.EventTS,
				StartedAt:       d.StartedAt,
				ActiveSymptoms:  d.ActiveSymptoms,
				Symptoms:        d.Symptoms,
				Severity:        d.Severity,
				PeakValues:      d.PeakValues,
				DeviceCode:      inc.meta.DeviceCode,
				DeviceType:      inc.meta.DeviceType,
				Metro:           inc.meta.Metro,
				ContributorCode: inc.meta.ContributorCode,
				Status:          inc.meta.Status,
			})
		}
	}

	if len(allEvents) > 0 {
		if err := a.writeDeviceEvents(ctx, allEvents); err != nil {
			return fmt.Errorf("write device backfill events: %w", err)
		}
		a.Log.Info("backfill: wrote device events", "count", len(allEvents), "incidents", len(incidents))
	}

	if err := a.resolveStaleDevices(ctx, input); err != nil {
		return err
	}

	a.Log.Info("backfill: device chunk complete", "start", input.WindowStart, "end", input.WindowEnd)
	return nil
}

func (a *Activities) resolveStaleDevices(ctx context.Context, input BackfillChunkInput) error {
	resolveStaleQuery := `
		INSERT INTO device_incident_events (
			incident_id, device_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			device_code, device_type, metro, contributor_code, status
		)
		SELECT
			ie.incident_id, ie.device_pk, 'resolved',
			least(toDateTime($1) + INTERVAL $3 MINUTE, toDateTime($2)), ie.started_at,
			arrayResize(emptyArrayString(), 0), ie.symptoms, ie.severity, ie.peak_values,
			ie.device_code, ie.device_type, ie.metro, ie.contributor_code, ie.status
		FROM device_incident_events ie
		INNER JOIN (
			SELECT incident_id, max(event_ts) AS max_ts
			FROM device_incident_events GROUP BY incident_id
		) latest ON ie.incident_id = latest.incident_id AND ie.event_ts = latest.max_ts
		WHERE ie.event_type != 'resolved'
		  AND ie.event_ts < toDateTime($1)
		  AND ie.device_pk NOT IN (
			SELECT DISTINCT device_pk FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= $1 AND bucket_ts < $2
			  AND link_pk = ''
			  AND (in_errors + out_errors >= 1 OR in_fcs_errors >= 1
			    OR in_discards + out_discards >= 1 OR carrier_transitions >= 1
			    OR isis_overload = true OR isis_unreachable = true)
		  )
	`
	if err := a.ClickHouse.Exec(ctx, resolveStaleQuery, input.WindowStart, input.WindowEnd, backfillCoalesceGapMinutes); err != nil {
		return fmt.Errorf("resolve stale device incidents: %w", err)
	}
	return nil
}

// --- Shared helpers ---

// existingIncidentIDs returns the set of incident_ids that already have events
// in the table. The query is scoped to incidents that could overlap the given
// window to avoid scanning the full table on small detection windows.
func (a *Activities) existingIncidentIDs(ctx context.Context, table string, windowStart time.Time) (map[string]bool, error) {
	query := fmt.Sprintf("SELECT DISTINCT incident_id FROM %s WHERE started_at >= $1", table)
	rows, err := a.ClickHouse.Query(ctx, query, windowStart.Add(-25*time.Hour))
	if err != nil {
		return nil, fmt.Errorf("query existing incidents: %w", err)
	}
	defer rows.Close()

	result := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result[id] = true
	}
	return result, rows.Err()
}
