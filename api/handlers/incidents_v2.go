package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-chi/chi/v5"
)

// --- Link incidents v2 types ---

// LinkIncidentV2 represents an incident on a link with grouped symptoms.
type LinkIncidentV2 struct {
	IncidentID      string             `json:"incident_id"`
	LinkPK          string             `json:"link_pk"`
	Severity        string             `json:"severity"`
	Status          string             `json:"status"` // ongoing, pending_resolution, resolved
	StartedAt       string             `json:"started_at"`
	EndedAt         *string            `json:"ended_at,omitempty"`
	DurationSeconds int64              `json:"duration_seconds"`
	ActiveSymptoms  []string           `json:"active_symptoms"`
	Symptoms        []string           `json:"symptoms"`
	PeakValues      map[string]float64 `json:"peak_values"`
	LastEventTS     string             `json:"last_event_ts"`
	LinkCode        string             `json:"link_code"`
	LinkType        string             `json:"link_type"`
	SideAMetro      string             `json:"side_a_metro"`
	SideZMetro      string             `json:"side_z_metro"`
	ContributorCode string             `json:"contributor_code"`
	EntityStatus    string             `json:"entity_status"`
	Provisioning    bool               `json:"provisioning"`
	IsDrained       bool               `json:"is_drained"`
}

// DrainedLinkInfoV2 represents a drained link with incidents from the events table.
type DrainedLinkInfoV2 struct {
	LinkPK          string           `json:"link_pk"`
	LinkCode        string           `json:"link_code"`
	LinkType        string           `json:"link_type"`
	SideAMetro      string           `json:"side_a_metro"`
	SideZMetro      string           `json:"side_z_metro"`
	ContributorCode string           `json:"contributor_code"`
	DrainStatus     string           `json:"drain_status"`
	DrainedSince    string           `json:"drained_since"`
	ActiveIncidents []LinkIncidentV2 `json:"active_incidents"`
	RecentIncidents []LinkIncidentV2 `json:"recent_incidents"`
	LastIncidentEnd *string          `json:"last_incident_end,omitempty"`
	ClearForSeconds *int64           `json:"clear_for_seconds,omitempty"`
	Readiness       string           `json:"readiness"`
}

// LinkIncidentsV2Response is the v2 API response for link incidents.
type LinkIncidentsV2Response struct {
	Incidents      []LinkIncidentV2    `json:"incidents"`
	Summary        IncidentsV2Summary  `json:"summary"`
	Drained        []DrainedLinkInfoV2 `json:"drained"`
	DrainedSummary DrainedSummary      `json:"drained_summary"`
}

// --- Device incidents v2 types ---

// DeviceIncidentV2 represents an incident on a device with grouped symptoms.
type DeviceIncidentV2 struct {
	IncidentID      string             `json:"incident_id"`
	DevicePK        string             `json:"device_pk"`
	Severity        string             `json:"severity"`
	Status          string             `json:"status"`
	StartedAt       string             `json:"started_at"`
	EndedAt         *string            `json:"ended_at,omitempty"`
	DurationSeconds int64              `json:"duration_seconds"`
	ActiveSymptoms  []string           `json:"active_symptoms"`
	Symptoms        []string           `json:"symptoms"`
	PeakValues      map[string]float64 `json:"peak_values"`
	LastEventTS     string             `json:"last_event_ts"`
	DeviceCode      string             `json:"device_code"`
	DeviceType      string             `json:"device_type"`
	Metro           string             `json:"metro"`
	ContributorCode string             `json:"contributor_code"`
	EntityStatus    string             `json:"entity_status"`
	IsDrained       bool               `json:"is_drained"`
}

// DrainedDeviceInfoV2 represents a drained device with incidents from events.
type DrainedDeviceInfoV2 struct {
	DevicePK        string             `json:"device_pk"`
	DeviceCode      string             `json:"device_code"`
	DeviceType      string             `json:"device_type"`
	Metro           string             `json:"metro"`
	ContributorCode string             `json:"contributor_code"`
	DrainStatus     string             `json:"drain_status"`
	DrainedSince    string             `json:"drained_since"`
	ActiveIncidents []DeviceIncidentV2 `json:"active_incidents"`
	RecentIncidents []DeviceIncidentV2 `json:"recent_incidents"`
	LastIncidentEnd *string            `json:"last_incident_end,omitempty"`
	ClearForSeconds *int64             `json:"clear_for_seconds,omitempty"`
	Readiness       string             `json:"readiness"`
}

// DeviceIncidentsV2Response is the v2 API response for device incidents.
type DeviceIncidentsV2Response struct {
	Incidents      []DeviceIncidentV2    `json:"incidents"`
	Summary        IncidentsV2Summary    `json:"summary"`
	Drained        []DrainedDeviceInfoV2 `json:"drained"`
	DrainedSummary DrainedSummary        `json:"drained_summary"`
}

// --- Shared types ---

// IncidentsV2Summary contains aggregate counts for v2 incidents.
type IncidentsV2Summary struct {
	Total     int            `json:"total"`
	Ongoing   int            `json:"ongoing"`
	Critical  int            `json:"critical"`
	Warning   int            `json:"warning"`
	BySymptom map[string]int `json:"by_symptom"`
}

// incidentsV2Params holds parsed query parameters for v2 endpoints.
type incidentsV2Params struct {
	Duration time.Duration
	Severity string // "all", "critical", "warning"
	Symptoms []string
	Status   string // "all", "ongoing", "resolved"
	Filters  []IncidentFilter
}

func parseIncidentsV2Params(r *http.Request) incidentsV2Params {
	q := r.URL.Query()

	timeRange := q.Get("range")
	if timeRange == "" {
		timeRange = "24h"
	}

	severity := q.Get("severity")
	if severity == "" {
		severity = "all"
	}

	status := q.Get("status")
	if status == "" {
		status = "all"
	}

	var symptoms []string
	if s := q.Get("symptom"); s != "" {
		for _, sym := range strings.Split(s, ",") {
			sym = strings.TrimSpace(sym)
			if sym != "" {
				symptoms = append(symptoms, sym)
			}
		}
	}

	return incidentsV2Params{
		Duration: parseTimeRange(timeRange),
		Severity: severity,
		Symptoms: symptoms,
		Status:   status,
		Filters:  parseIncidentFilters(q.Get("filter")),
	}
}

// --- Link incidents v2 handler ---

// GetLinkIncidentsV2 returns link incidents from the link_incident_events table.
func (a *API) GetLinkIncidentsV2(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) && isDefaultIncidentsV2Request(r) {
		if data, err := a.readPageCache(r.Context(), "link_incidents_v2"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	params := parseIncidentsV2Params(r)

	ctx, cancel := statusContext(r, 30*time.Second)
	defer cancel()

	resp, err := fetchLinkIncidentsV2(ctx, a.envDB(ctx), params)
	if err != nil {
		slog.Error("failed to fetch v2 link incidents", "error", err)
		http.Error(w, fmt.Sprintf("Failed to fetch incidents: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func fetchLinkIncidentsV2(ctx context.Context, conn driver.Conn, params incidentsV2Params) (*LinkIncidentsV2Response, error) {
	rangeSecs := int64(params.Duration.Seconds())

	query := `
		WITH latest AS (
			SELECT *, row_number() OVER (PARTITION BY incident_id ORDER BY event_ts DESC) as rn
			FROM link_incident_events
		)
		SELECT
			incident_id, link_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			link_code, link_type, side_a_metro, side_z_metro,
			contributor_code, status, provisioning
		FROM latest
		WHERE rn = 1
		  AND (
			-- Ongoing/pending: always show
			event_type != 'resolved'
			-- Resolved: only if the incident was active during the time range
			OR event_ts >= now() - INTERVAL $1 SECOND
			OR started_at >= now() - INTERVAL $1 SECOND
		  )
	`

	rows, err := conn.Query(ctx, query, rangeSecs)
	if err != nil {
		return nil, fmt.Errorf("query link incidents: %w", err)
	}
	defer rows.Close()

	var allIncidents []LinkIncidentV2
	now := time.Now()

	for rows.Next() {
		var (
			incidentID, linkPK, severity, peakValuesJSON string
			linkCode, linkType, sideAMetro, sideZMetro   string
			contributorCode, status                      string
			eventTypeStr                                 string
			provisioning                                 bool
			eventTS, startedAt                           time.Time
			activeSymptoms, symptoms                     []string
		)

		if err := rows.Scan(
			&incidentID, &linkPK, &eventTypeStr, &eventTS, &startedAt,
			&activeSymptoms, &symptoms, &severity, &peakValuesJSON,
			&linkCode, &linkType, &sideAMetro, &sideZMetro,
			&contributorCode, &status, &provisioning,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		// Derive status from event state (uses active symptoms).
		incStatus := deriveIncidentStatus(eventTypeStr, activeSymptoms)

		// Compute duration and ended_at.
		var endedAt *string
		var durationSecs int64
		if incStatus == "resolved" {
			end := eventTS.UTC().Format(time.RFC3339)
			endedAt = &end
			durationSecs = int64(eventTS.Sub(startedAt).Seconds())
		} else {
			durationSecs = int64(now.Sub(startedAt).Seconds())
		}

		peakValues := parsePeakValues(peakValuesJSON)

		if activeSymptoms == nil {
			activeSymptoms = []string{}
		}
		if symptoms == nil {
			symptoms = []string{}
		}

		inc := LinkIncidentV2{
			IncidentID:      incidentID,
			LinkPK:          linkPK,
			Severity:        severity,
			Status:          incStatus,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			EndedAt:         endedAt,
			DurationSeconds: durationSecs,
			ActiveSymptoms:  activeSymptoms,
			Symptoms:        symptoms,
			PeakValues:      peakValues,
			LastEventTS:     eventTS.UTC().Format(time.RFC3339),
			LinkCode:        linkCode,
			LinkType:        linkType,
			SideAMetro:      sideAMetro,
			SideZMetro:      sideZMetro,
			ContributorCode: contributorCode,
			EntityStatus:    status,
			Provisioning:    provisioning,
			IsDrained:       status == "soft-drained" || status == "hard-drained",
		}

		allIncidents = append(allIncidents, inc)
	}

	// Apply filters.
	filtered := filterLinkIncidentsV2(allIncidents, params)

	// Build response.
	return buildLinkIncidentsV2Response(ctx, conn, filtered, params.Filters), nil
}

func filterLinkIncidentsV2(incidents []LinkIncidentV2, params incidentsV2Params) []LinkIncidentV2 {
	var result []LinkIncidentV2
	for _, inc := range incidents {
		if params.Severity != "all" && inc.Severity != params.Severity {
			continue
		}
		if params.Status != "all" {
			if params.Status == "ongoing" && inc.Status != "ongoing" && inc.Status != "pending_resolution" {
				continue
			}
			if params.Status == "resolved" && inc.Status != "resolved" {
				continue
			}
		}
		if len(params.Symptoms) > 0 && !hasAnySymptom(inc.Symptoms, params.Symptoms) {
			continue // filter against all symptoms (complete picture)
		}
		if !matchesLinkFiltersV2(inc, params.Filters) {
			continue
		}
		result = append(result, inc)
	}
	return result
}

func matchesLinkFiltersV2(inc LinkIncidentV2, filters []IncidentFilter) bool {
	for _, f := range filters {
		switch f.Type {
		case "metro":
			if inc.SideAMetro != f.Value && inc.SideZMetro != f.Value {
				return false
			}
		case "link":
			if inc.LinkCode != f.Value {
				return false
			}
		case "contributor":
			if inc.ContributorCode != f.Value {
				return false
			}
		}
	}
	return true
}

func buildLinkIncidentsV2Response(ctx context.Context, conn driver.Conn, incidents []LinkIncidentV2, filters []IncidentFilter) *LinkIncidentsV2Response {
	// Split active vs drained.
	var active []LinkIncidentV2
	drainedByLink := make(map[string][]LinkIncidentV2)

	for _, inc := range incidents {
		if inc.IsDrained {
			drainedByLink[inc.LinkPK] = append(drainedByLink[inc.LinkPK], inc)
		} else {
			active = append(active, inc)
		}
	}

	// Sort active by start time descending.
	sort.Slice(active, func(i, j int) bool {
		return active[i].StartedAt > active[j].StartedAt
	})

	// Build drained view.
	linkMeta, err := fetchLinkMetadataWithStatus(ctx, conn, filters)
	if err != nil {
		slog.Warn("v2: failed to fetch link metadata", "error", err)
		linkMeta = make(map[string]linkMetadataWithStatus)
	}
	drainedSince := fetchDrainedSince(ctx, conn, linkMeta)
	drainedLinks := buildDrainedLinksV2(linkMeta, drainedByLink, drainedSince)

	// Build summary.
	summary := buildIncidentsV2Summary(active)

	drainedSummary := DrainedSummary{Total: len(drainedLinks)}
	for _, dl := range drainedLinks {
		if len(dl.ActiveIncidents) > 0 {
			drainedSummary.WithIncidents++
			drainedSummary.NotReady++
		} else if dl.Readiness == "green" {
			drainedSummary.Ready++
		} else {
			drainedSummary.NotReady++
		}
	}

	if active == nil {
		active = []LinkIncidentV2{}
	}
	if drainedLinks == nil {
		drainedLinks = []DrainedLinkInfoV2{}
	}

	return &LinkIncidentsV2Response{
		Incidents:      active,
		Summary:        summary,
		Drained:        drainedLinks,
		DrainedSummary: drainedSummary,
	}
}

func buildDrainedLinksV2(linkMeta map[string]linkMetadataWithStatus, incidentsByLink map[string][]LinkIncidentV2, drainedSince map[string]time.Time) []DrainedLinkInfoV2 {
	var result []DrainedLinkInfoV2

	for linkPK, meta := range linkMeta {
		if meta.Status != "soft-drained" && meta.Status != "hard-drained" {
			continue
		}

		incidents := incidentsByLink[linkPK]
		var activeIncs, recentIncs []LinkIncidentV2
		var lastEndedAt *time.Time

		for _, inc := range incidents {
			if inc.Status == "ongoing" || inc.Status == "pending_resolution" {
				activeIncs = append(activeIncs, inc)
			} else if inc.EndedAt != nil {
				recentIncs = append(recentIncs, inc)
				endTime, err := time.Parse(time.RFC3339, *inc.EndedAt)
				if err == nil && (lastEndedAt == nil || endTime.After(*lastEndedAt)) {
					lastEndedAt = &endTime
				}
			}
		}

		sort.Slice(recentIncs, func(i, j int) bool {
			return recentIncs[i].StartedAt > recentIncs[j].StartedAt
		})

		if activeIncs == nil {
			activeIncs = []LinkIncidentV2{}
		}
		if recentIncs == nil {
			recentIncs = []LinkIncidentV2{}
		}

		dl := DrainedLinkInfoV2{
			LinkPK:          linkPK,
			LinkCode:        meta.LinkCode,
			LinkType:        meta.LinkType,
			SideAMetro:      meta.SideAMetro,
			SideZMetro:      meta.SideZMetro,
			ContributorCode: meta.ContributorCode,
			DrainStatus:     meta.Status,
			DrainedSince:    drainedSinceStr(drainedSince, linkPK),
			ActiveIncidents: activeIncs,
			RecentIncidents: recentIncs,
			Readiness:       computeReadiness(len(activeIncs) > 0, lastEndedAt),
		}

		if lastEndedAt != nil {
			clearFor := int64(time.Since(*lastEndedAt).Seconds())
			dl.ClearForSeconds = &clearFor
			end := lastEndedAt.UTC().Format(time.RFC3339)
			dl.LastIncidentEnd = &end
		}

		result = append(result, dl)
	}

	readinessOrder := map[string]int{"red": 0, "yellow": 1, "green": 2, "gray": 3}
	sort.Slice(result, func(i, j int) bool {
		oi, oj := readinessOrder[result[i].Readiness], readinessOrder[result[j].Readiness]
		if oi != oj {
			return oi < oj
		}
		return result[i].LinkCode < result[j].LinkCode
	})

	return result
}

// --- Device incidents v2 handler ---

// GetDeviceIncidentsV2 returns device incidents from the device_incident_events table.
func (a *API) GetDeviceIncidentsV2(w http.ResponseWriter, r *http.Request) {
	if isMainnet(r.Context()) && isDefaultIncidentsV2Request(r) {
		if data, err := a.readPageCache(r.Context(), "device_incidents_v2"); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write(data)
			return
		}
	}

	params := parseIncidentsV2Params(r)

	ctx, cancel := statusContext(r, 30*time.Second)
	defer cancel()

	resp, err := fetchDeviceIncidentsV2(ctx, a.envDB(ctx), params)
	if err != nil {
		slog.Error("failed to fetch v2 device incidents", "error", err)
		http.Error(w, fmt.Sprintf("Failed to fetch incidents: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func fetchDeviceIncidentsV2(ctx context.Context, conn driver.Conn, params incidentsV2Params) (*DeviceIncidentsV2Response, error) {
	rangeSecs := int64(params.Duration.Seconds())

	query := `
		WITH latest AS (
			SELECT *, row_number() OVER (PARTITION BY incident_id ORDER BY event_ts DESC) as rn
			FROM device_incident_events
		)
		SELECT
			incident_id, device_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			device_code, device_type, metro, contributor_code, status
		FROM latest
		WHERE rn = 1
		  AND (
			event_type != 'resolved'
			OR event_ts >= now() - INTERVAL $1 SECOND
			OR started_at >= now() - INTERVAL $1 SECOND
		  )
	`

	rows, err := conn.Query(ctx, query, rangeSecs)
	if err != nil {
		return nil, fmt.Errorf("query device incidents: %w", err)
	}
	defer rows.Close()

	var allIncidents []DeviceIncidentV2
	now := time.Now()

	for rows.Next() {
		var (
			incidentID, devicePK, severity, peakValuesJSON string
			deviceCode, deviceType, metro                  string
			contributorCode, status, eventTypeStr          string
			eventTS, startedAt                             time.Time
			activeSymptoms, symptoms                       []string
		)

		if err := rows.Scan(
			&incidentID, &devicePK, &eventTypeStr, &eventTS, &startedAt,
			&activeSymptoms, &symptoms, &severity, &peakValuesJSON,
			&deviceCode, &deviceType, &metro, &contributorCode, &status,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		incStatus := deriveIncidentStatus(eventTypeStr, activeSymptoms)

		var endedAt *string
		var durationSecs int64
		if incStatus == "resolved" {
			end := eventTS.UTC().Format(time.RFC3339)
			endedAt = &end
			durationSecs = int64(eventTS.Sub(startedAt).Seconds())
		} else {
			durationSecs = int64(now.Sub(startedAt).Seconds())
		}

		peakValues := parsePeakValues(peakValuesJSON)

		if activeSymptoms == nil {
			activeSymptoms = []string{}
		}
		if symptoms == nil {
			symptoms = []string{}
		}

		inc := DeviceIncidentV2{
			IncidentID:      incidentID,
			DevicePK:        devicePK,
			Severity:        severity,
			Status:          incStatus,
			StartedAt:       startedAt.UTC().Format(time.RFC3339),
			EndedAt:         endedAt,
			DurationSeconds: durationSecs,
			ActiveSymptoms:  activeSymptoms,
			Symptoms:        symptoms,
			PeakValues:      peakValues,
			LastEventTS:     eventTS.UTC().Format(time.RFC3339),
			DeviceCode:      deviceCode,
			DeviceType:      deviceType,
			Metro:           metro,
			ContributorCode: contributorCode,
			EntityStatus:    status,
			IsDrained:       isDeviceDrained(status),
		}

		allIncidents = append(allIncidents, inc)
	}

	filtered := filterDeviceIncidentsV2(allIncidents, params)

	return buildDeviceIncidentsV2Response(ctx, conn, filtered, params.Filters), nil
}

func filterDeviceIncidentsV2(incidents []DeviceIncidentV2, params incidentsV2Params) []DeviceIncidentV2 {
	var result []DeviceIncidentV2
	for _, inc := range incidents {
		if params.Severity != "all" && inc.Severity != params.Severity {
			continue
		}
		if params.Status != "all" {
			if params.Status == "ongoing" && inc.Status != "ongoing" && inc.Status != "pending_resolution" {
				continue
			}
			if params.Status == "resolved" && inc.Status != "resolved" {
				continue
			}
		}
		if len(params.Symptoms) > 0 && !hasAnySymptom(inc.Symptoms, params.Symptoms) {
			continue // filter against all symptoms (complete picture)
		}
		if !matchesDeviceFiltersV2(inc, params.Filters) {
			continue
		}
		result = append(result, inc)
	}
	return result
}

func matchesDeviceFiltersV2(inc DeviceIncidentV2, filters []IncidentFilter) bool {
	for _, f := range filters {
		switch f.Type {
		case "metro":
			if inc.Metro != f.Value {
				return false
			}
		case "device":
			if inc.DeviceCode != f.Value {
				return false
			}
		case "contributor":
			if inc.ContributorCode != f.Value {
				return false
			}
		}
	}
	return true
}

func buildDeviceIncidentsV2Response(ctx context.Context, conn driver.Conn, incidents []DeviceIncidentV2, filters []IncidentFilter) *DeviceIncidentsV2Response {
	var active []DeviceIncidentV2
	drainedByDevice := make(map[string][]DeviceIncidentV2)

	for _, inc := range incidents {
		if inc.IsDrained {
			drainedByDevice[inc.DevicePK] = append(drainedByDevice[inc.DevicePK], inc)
		} else {
			active = append(active, inc)
		}
	}

	sort.Slice(active, func(i, j int) bool {
		return active[i].StartedAt > active[j].StartedAt
	})

	deviceMeta, err := fetchDeviceMetadata(ctx, conn, filters)
	if err != nil {
		slog.Warn("v2: failed to fetch device metadata", "error", err)
		deviceMeta = make(map[string]deviceMetadata)
	}
	drainedDevices := buildDrainedDevicesV2(deviceMeta, drainedByDevice)

	summary := buildDeviceIncidentsV2Summary(active)

	drainedSummary := DrainedSummary{Total: len(drainedDevices)}
	for _, dd := range drainedDevices {
		if len(dd.ActiveIncidents) > 0 {
			drainedSummary.WithIncidents++
			drainedSummary.NotReady++
		} else if dd.Readiness == "green" {
			drainedSummary.Ready++
		} else {
			drainedSummary.NotReady++
		}
	}

	if active == nil {
		active = []DeviceIncidentV2{}
	}
	if drainedDevices == nil {
		drainedDevices = []DrainedDeviceInfoV2{}
	}

	return &DeviceIncidentsV2Response{
		Incidents:      active,
		Summary:        summary,
		Drained:        drainedDevices,
		DrainedSummary: drainedSummary,
	}
}

func buildDrainedDevicesV2(deviceMeta map[string]deviceMetadata, incidentsByDevice map[string][]DeviceIncidentV2) []DrainedDeviceInfoV2 {
	var result []DrainedDeviceInfoV2

	for devicePK, meta := range deviceMeta {
		if !isDeviceDrained(meta.Status) {
			continue
		}

		incidents := incidentsByDevice[devicePK]
		var activeIncs, recentIncs []DeviceIncidentV2
		var lastEndedAt *time.Time

		for _, inc := range incidents {
			if inc.Status == "ongoing" || inc.Status == "pending_resolution" {
				activeIncs = append(activeIncs, inc)
			} else if inc.EndedAt != nil {
				recentIncs = append(recentIncs, inc)
				endTime, err := time.Parse(time.RFC3339, *inc.EndedAt)
				if err == nil && (lastEndedAt == nil || endTime.After(*lastEndedAt)) {
					lastEndedAt = &endTime
				}
			}
		}

		sort.Slice(recentIncs, func(i, j int) bool {
			return recentIncs[i].StartedAt > recentIncs[j].StartedAt
		})

		if activeIncs == nil {
			activeIncs = []DeviceIncidentV2{}
		}
		if recentIncs == nil {
			recentIncs = []DeviceIncidentV2{}
		}

		dd := DrainedDeviceInfoV2{
			DevicePK:        devicePK,
			DeviceCode:      meta.DeviceCode,
			DeviceType:      meta.DeviceType,
			Metro:           meta.Metro,
			ContributorCode: meta.ContributorCode,
			DrainStatus:     meta.Status,
			ActiveIncidents: activeIncs,
			RecentIncidents: recentIncs,
			Readiness:       computeReadiness(len(activeIncs) > 0, lastEndedAt),
		}

		if lastEndedAt != nil {
			clearFor := int64(time.Since(*lastEndedAt).Seconds())
			dd.ClearForSeconds = &clearFor
			end := lastEndedAt.UTC().Format(time.RFC3339)
			dd.LastIncidentEnd = &end
		}

		result = append(result, dd)
	}

	readinessOrder := map[string]int{"red": 0, "yellow": 1, "green": 2, "gray": 3}
	sort.Slice(result, func(i, j int) bool {
		oi, oj := readinessOrder[result[i].Readiness], readinessOrder[result[j].Readiness]
		if oi != oj {
			return oi < oj
		}
		return result[i].DeviceCode < result[j].DeviceCode
	})

	return result
}

// --- Shared helpers ---

func deriveIncidentStatus(eventType string, symptoms []string) string {
	if eventType == "resolved" {
		return "resolved"
	}
	if len(symptoms) == 0 {
		return "pending_resolution"
	}
	return "ongoing"
}

func hasAnySymptom(incidentSymptoms, filterSymptoms []string) bool {
	for _, fs := range filterSymptoms {
		for _, is := range incidentSymptoms {
			if is == fs {
				return true
			}
		}
	}
	return false
}

func parsePeakValues(jsonStr string) map[string]float64 {
	if jsonStr == "" || jsonStr == "{}" {
		return map[string]float64{}
	}
	var m map[string]float64
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		return map[string]float64{}
	}
	return m
}

func computeReadiness(hasActive bool, lastEndedAt *time.Time) string {
	if hasActive {
		return "red"
	}
	if lastEndedAt != nil {
		if time.Since(*lastEndedAt).Seconds() >= 1800 {
			return "green"
		}
		return "yellow"
	}
	return "gray"
}

func buildIncidentsV2Summary(incidents []LinkIncidentV2) IncidentsV2Summary {
	s := IncidentsV2Summary{BySymptom: make(map[string]int)}
	s.Total = len(incidents)
	for _, inc := range incidents {
		if inc.Status == "ongoing" || inc.Status == "pending_resolution" {
			s.Ongoing++
		}
		if inc.Severity == "critical" {
			s.Critical++
		} else {
			s.Warning++
		}
		for _, sym := range inc.Symptoms {
			s.BySymptom[sym]++
		}
	}
	return s
}

func buildDeviceIncidentsV2Summary(incidents []DeviceIncidentV2) IncidentsV2Summary {
	s := IncidentsV2Summary{BySymptom: make(map[string]int)}
	s.Total = len(incidents)
	for _, inc := range incidents {
		if inc.Status == "ongoing" || inc.Status == "pending_resolution" {
			s.Ongoing++
		}
		if inc.Severity == "critical" {
			s.Critical++
		} else {
			s.Warning++
		}
		for _, sym := range inc.Symptoms {
			s.BySymptom[sym]++
		}
	}
	return s
}

func isDefaultIncidentsV2Request(r *http.Request) bool {
	q := r.URL.Query()
	rangeParam := q.Get("range")
	if rangeParam != "" && rangeParam != "24h" {
		return false
	}
	if q.Get("severity") != "" && q.Get("severity") != "all" {
		return false
	}
	if q.Get("symptom") != "" {
		return false
	}
	if q.Get("status") != "" && q.Get("status") != "all" {
		return false
	}
	if q.Get("filter") != "" {
		return false
	}
	return true
}

// --- Cache functions ---

// FetchDefaultLinkIncidentsV2Data fetches link incidents with default params for caching.
func (a *API) FetchDefaultLinkIncidentsV2Data(ctx context.Context) *LinkIncidentsV2Response {
	params := incidentsV2Params{
		Duration: 24 * time.Hour,
		Severity: "all",
		Status:   "all",
	}
	resp, err := fetchLinkIncidentsV2(ctx, a.envDB(ctx), params)
	if err != nil {
		slog.Info("cache: link incidents v2 fetch unsuccessful", "detail", err)
		return nil
	}
	return resp
}

// --- Incident detail types and handlers ---

// IncidentEventV2 represents a single event in an incident's timeline.
type IncidentEventV2 struct {
	EventType      string             `json:"event_type"`
	EventTS        string             `json:"event_ts"`
	ActiveSymptoms []string           `json:"active_symptoms"`
	Symptoms       []string           `json:"symptoms"`
	Severity       string             `json:"severity"`
	PeakValues     map[string]float64 `json:"peak_values"`
}

// LinkIncidentDetailResponse is the API response for a single link incident.
type LinkIncidentDetailResponse struct {
	IncidentID      string               `json:"incident_id"`
	LinkPK          string               `json:"link_pk"`
	Severity        string               `json:"severity"`
	Status          string               `json:"status"`
	StartedAt       string               `json:"started_at"`
	EndedAt         *string              `json:"ended_at,omitempty"`
	DurationSeconds int64                `json:"duration_seconds"`
	ActiveSymptoms  []string             `json:"active_symptoms"`
	Symptoms        []string             `json:"symptoms"`
	PeakValues      map[string]float64   `json:"peak_values"`
	LinkCode        string               `json:"link_code"`
	LinkType        string               `json:"link_type"`
	SideAMetro      string               `json:"side_a_metro"`
	SideZMetro      string               `json:"side_z_metro"`
	ContributorCode string               `json:"contributor_code"`
	EntityStatus    string               `json:"entity_status"`
	Provisioning    bool                 `json:"provisioning"`
	Events          []IncidentEventV2    `json:"events"`
	StatusChanges   []EntityStatusChange `json:"status_changes"`
}

// DeviceIncidentDetailResponse is the API response for a single device incident.
type DeviceIncidentDetailResponse struct {
	IncidentID      string               `json:"incident_id"`
	DevicePK        string               `json:"device_pk"`
	Severity        string               `json:"severity"`
	Status          string               `json:"status"`
	StartedAt       string               `json:"started_at"`
	EndedAt         *string              `json:"ended_at,omitempty"`
	DurationSeconds int64                `json:"duration_seconds"`
	ActiveSymptoms  []string             `json:"active_symptoms"`
	Symptoms        []string             `json:"symptoms"`
	PeakValues      map[string]float64   `json:"peak_values"`
	DeviceCode      string               `json:"device_code"`
	DeviceType      string               `json:"device_type"`
	Metro           string               `json:"metro"`
	ContributorCode string               `json:"contributor_code"`
	EntityStatus    string               `json:"entity_status"`
	Events          []IncidentEventV2    `json:"events"`
	StatusChanges   []EntityStatusChange `json:"status_changes"`
}

// GetLinkIncidentDetail returns the full event timeline for a single link incident.
func (a *API) GetLinkIncidentDetail(w http.ResponseWriter, r *http.Request) {
	incidentID := chi.URLParam(r, "id")
	if incidentID == "" {
		http.Error(w, "incident ID is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	query := `
		SELECT event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			link_pk, link_code, link_type, side_a_metro, side_z_metro,
			contributor_code, status, provisioning
		FROM link_incident_events
		WHERE incident_id = $1
		ORDER BY event_ts ASC
	`

	rows, err := a.envDB(ctx).Query(ctx, query, incidentID)
	if err != nil {
		slog.Error("failed to fetch link incident detail", "error", err, "id", incidentID)
		http.Error(w, "Failed to fetch incident", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var events []IncidentEventV2
	var resp LinkIncidentDetailResponse
	resp.IncidentID = incidentID
	now := time.Now()

	for rows.Next() {
		var (
			eventTypeStr, severity, peakValuesJSON string
			linkPK, linkCode, linkType, sideAMetro string
			sideZMetro, contributorCode, status    string
			provisioning                           bool
			eventTS, startedAt                     time.Time
			activeSymptoms, symptoms               []string
		)

		if err := rows.Scan(
			&eventTypeStr, &eventTS, &startedAt,
			&activeSymptoms, &symptoms, &severity, &peakValuesJSON,
			&linkPK, &linkCode, &linkType, &sideAMetro, &sideZMetro,
			&contributorCode, &status, &provisioning,
		); err != nil {
			http.Error(w, "Failed to scan event", http.StatusInternalServerError)
			return
		}

		if activeSymptoms == nil {
			activeSymptoms = []string{}
		}
		if symptoms == nil {
			symptoms = []string{}
		}

		events = append(events, IncidentEventV2{
			EventType:      eventTypeStr,
			EventTS:        eventTS.UTC().Format(time.RFC3339),
			ActiveSymptoms: activeSymptoms,
			Symptoms:       symptoms,
			Severity:       severity,
			PeakValues:     parsePeakValues(peakValuesJSON),
		})

		// Last row = latest state
		resp.LinkPK = linkPK
		resp.LinkCode = linkCode
		resp.LinkType = linkType
		resp.SideAMetro = sideAMetro
		resp.SideZMetro = sideZMetro
		resp.ContributorCode = contributorCode
		resp.EntityStatus = status
		resp.Provisioning = provisioning
		resp.StartedAt = startedAt.UTC().Format(time.RFC3339)
		resp.Severity = severity
		resp.ActiveSymptoms = activeSymptoms
		resp.Symptoms = symptoms
		resp.PeakValues = parsePeakValues(peakValuesJSON)
		resp.Status = deriveIncidentStatus(eventTypeStr, activeSymptoms)

		if resp.Status == "resolved" {
			end := eventTS.UTC().Format(time.RFC3339)
			resp.EndedAt = &end
			resp.DurationSeconds = int64(eventTS.Sub(startedAt).Seconds())
		} else {
			resp.DurationSeconds = int64(now.Sub(startedAt).Seconds())
		}
	}

	if len(events) == 0 {
		http.Error(w, "Incident not found", http.StatusNotFound)
		return
	}

	resp.Events = events

	// Fetch status changes for this link during the incident.
	resp.StatusChanges = fetchLinkStatusChanges(ctx, a.envDB(ctx), resp.LinkPK, resp.StartedAt, resp.EndedAt)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// GetDeviceIncidentDetail returns the full event timeline for a single device incident.
func (a *API) GetDeviceIncidentDetail(w http.ResponseWriter, r *http.Request) {
	incidentID := chi.URLParam(r, "id")
	if incidentID == "" {
		http.Error(w, "incident ID is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := statusContext(r, 15*time.Second)
	defer cancel()

	query := `
		SELECT event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			device_pk, device_code, device_type, metro,
			contributor_code, status
		FROM device_incident_events
		WHERE incident_id = $1
		ORDER BY event_ts ASC
	`

	rows, err := a.envDB(ctx).Query(ctx, query, incidentID)
	if err != nil {
		slog.Error("failed to fetch device incident detail", "error", err, "id", incidentID)
		http.Error(w, "Failed to fetch incident", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var events []IncidentEventV2
	var resp DeviceIncidentDetailResponse
	resp.IncidentID = incidentID
	now := time.Now()

	for rows.Next() {
		var (
			eventTypeStr, severity, peakValuesJSON string
			devicePK, deviceCode, deviceType       string
			metro, contributorCode, status         string
			eventTS, startedAt                     time.Time
			activeSymptoms, symptoms               []string
		)

		if err := rows.Scan(
			&eventTypeStr, &eventTS, &startedAt,
			&activeSymptoms, &symptoms, &severity, &peakValuesJSON,
			&devicePK, &deviceCode, &deviceType, &metro,
			&contributorCode, &status,
		); err != nil {
			http.Error(w, "Failed to scan event", http.StatusInternalServerError)
			return
		}

		if activeSymptoms == nil {
			activeSymptoms = []string{}
		}
		if symptoms == nil {
			symptoms = []string{}
		}

		events = append(events, IncidentEventV2{
			EventType:      eventTypeStr,
			EventTS:        eventTS.UTC().Format(time.RFC3339),
			ActiveSymptoms: activeSymptoms,
			Symptoms:       symptoms,
			Severity:       severity,
			PeakValues:     parsePeakValues(peakValuesJSON),
		})

		resp.DevicePK = devicePK
		resp.DeviceCode = deviceCode
		resp.DeviceType = deviceType
		resp.Metro = metro
		resp.ContributorCode = contributorCode
		resp.EntityStatus = status
		resp.StartedAt = startedAt.UTC().Format(time.RFC3339)
		resp.Severity = severity
		resp.ActiveSymptoms = activeSymptoms
		resp.Symptoms = symptoms
		resp.PeakValues = parsePeakValues(peakValuesJSON)
		resp.Status = deriveIncidentStatus(eventTypeStr, activeSymptoms)

		if resp.Status == "resolved" {
			end := eventTS.UTC().Format(time.RFC3339)
			resp.EndedAt = &end
			resp.DurationSeconds = int64(eventTS.Sub(startedAt).Seconds())
		} else {
			resp.DurationSeconds = int64(now.Sub(startedAt).Seconds())
		}
	}

	if len(events) == 0 {
		http.Error(w, "Incident not found", http.StatusNotFound)
		return
	}

	resp.Events = events

	// Fetch status changes for this device during the incident.
	resp.StatusChanges = fetchDeviceStatusChanges(ctx, a.envDB(ctx), resp.DevicePK, resp.StartedAt, resp.EndedAt)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// FetchDefaultDeviceIncidentsV2Data fetches device incidents with default params for caching.
func (a *API) FetchDefaultDeviceIncidentsV2Data(ctx context.Context) *DeviceIncidentsV2Response {
	params := incidentsV2Params{
		Duration: 24 * time.Hour,
		Severity: "all",
		Status:   "all",
	}
	resp, err := fetchDeviceIncidentsV2(ctx, a.envDB(ctx), params)
	if err != nil {
		slog.Info("cache: device incidents v2 fetch unsuccessful", "detail", err)
		return nil
	}
	return resp
}


