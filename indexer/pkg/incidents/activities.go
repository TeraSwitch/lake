package incidents

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Activities holds dependencies for incident detection activities.
type Activities struct {
	ClickHouse          driver.Conn
	Log                 *slog.Logger
	CoalesceGap         time.Duration
	EscalationThreshold time.Duration
}

// symptomLookback is how far back we check rollup data to determine if a
// symptom is currently active. 30 minutes accounts for the natural lag
// between rollup bucket timestamps and when the data is available (~10 min),
// providing 4 consecutive 5-minute bucket windows of coverage.
const symptomLookback = 30 * time.Minute

// recentDataWindow is how far back we look to determine if an entity has ever
// reported data (for no-data detection). If an entity reported within this
// window but not in the last symptomLookback, it gets a no-data symptom.
const recentDataWindow = 24 * time.Hour

// provisioningSentinel is the committed_rtt_ns value that indicates a link is provisioning.
const provisioningSentinel int64 = 1_000_000_000

// DetectAndWriteEvents is the main activity. It detects symptoms from rollup
// tables, compares against open incidents, and writes events for state changes.
// Links and devices are processed independently with separate tables.
func (a *Activities) DetectAndWriteEvents(ctx context.Context) error {
	start := time.Now()
	now := time.Now()
	var totalEvents int

	// Process link incidents.
	linkEvents, err := a.detectLinkEvents(ctx, now)
	if err != nil {
		return fmt.Errorf("detect link events: %w", err)
	}
	if len(linkEvents) > 0 {
		if err := a.writeLinkEvents(ctx, linkEvents); err != nil {
			return fmt.Errorf("write link events: %w", err)
		}
		totalEvents += len(linkEvents)
	}

	// Process device incidents.
	deviceEvents, err := a.detectDeviceEvents(ctx, now)
	if err != nil {
		return fmt.Errorf("detect device events: %w", err)
	}
	if len(deviceEvents) > 0 {
		if err := a.writeDeviceEvents(ctx, deviceEvents); err != nil {
			return fmt.Errorf("write device events: %w", err)
		}
		totalEvents += len(deviceEvents)
	}

	if totalEvents > 0 {
		a.Log.Info("incidents: wrote events",
			"link_events", len(linkEvents),
			"device_events", len(deviceEvents),
			"duration", time.Since(start).Round(time.Millisecond))
	} else {
		a.Log.Debug("incidents: no state changes",
			"duration", time.Since(start).Round(time.Millisecond))
	}

	return nil
}

// --- Link detection ---

func (a *Activities) detectLinkEvents(ctx context.Context, now time.Time) ([]LinkIncidentEvent, error) {
	symptoms, err := a.fetchLinkSymptoms(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch link symptoms: %w", err)
	}

	counterSymptoms, err := a.fetchLinkCounterSymptoms(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch link counter symptoms: %w", err)
	}
	symptoms = append(symptoms, counterSymptoms...)

	noData, err := a.fetchLinkNoDataSymptoms(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch link no-data: %w", err)
	}
	symptoms = append(symptoms, noData...)

	open, err := a.fetchOpenLinkIncidents(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch open link incidents: %w", err)
	}

	// Convert to diff inputs.
	var inputs []symptomInput
	symptomsByEntity := make(map[string]LinkSymptom) // first symptom per entity for metadata
	for _, s := range symptoms {
		inputs = append(inputs, symptomInput{
			EntityPK: s.LinkPK, IncidentType: s.IncidentType,
			PeakValue: s.PeakValue, StartedAt: s.StartedAt,
		})
		if _, ok := symptomsByEntity[s.LinkPK]; !ok {
			symptomsByEntity[s.LinkPK] = s
		}
	}

	var openStates []openState
	for _, o := range open {
		openStates = append(openStates, openState{
			IncidentID: o.IncidentID, EntityPK: o.LinkPK,
			StartedAt: o.StartedAt, ActiveSymptoms: o.ActiveSymptoms,
			Symptoms: o.Symptoms, Severity: o.Severity, LastEventTS: o.LastEventTS,
		})
	}

	deltas := a.diff(now, inputs, openStates)

	// Index open incidents by entity for metadata fallback.
	openByEntity := make(map[string]OpenLinkIncident)
	for _, o := range open {
		openByEntity[o.LinkPK] = o
	}

	// Convert deltas to LinkIncidentEvents with metadata.
	events := make([]LinkIncidentEvent, 0, len(deltas))
	for _, d := range deltas {
		e := LinkIncidentEvent{
			IncidentID: d.IncidentID, LinkPK: d.EntityPK,
			EventType: d.EventType, EventTS: d.EventTS,
			StartedAt: d.StartedAt, ActiveSymptoms: d.ActiveSymptoms,
			Symptoms: d.Symptoms, Severity: d.Severity, PeakValues: d.PeakValues,
		}
		if s, ok := symptomsByEntity[d.EntityPK]; ok {
			// Use current symptom metadata (freshest).
			e.LinkCode = s.LinkCode
			e.LinkType = s.LinkType
			e.SideAMetro = s.SideAMetro
			e.SideZMetro = s.SideZMetro
			e.ContributorCode = s.ContributorCode
			e.Status = s.Status
			e.Provisioning = s.Provisioning
		} else if o, ok := openByEntity[d.EntityPK]; ok {
			// Fallback: use metadata from the open incident's last event.
			e.LinkCode = o.LinkCode
			e.LinkType = o.LinkType
			e.SideAMetro = o.SideAMetro
			e.SideZMetro = o.SideZMetro
			e.ContributorCode = o.ContributorCode
			e.Status = o.Status
			e.Provisioning = o.Provisioning
		}
		events = append(events, e)
	}

	return events, nil
}

func (a *Activities) fetchLinkSymptoms(ctx context.Context) ([]LinkSymptom, error) {
	query := `
		WITH recent AS (
			SELECT
				r.link_pk,
				greatest(r.a_loss_pct, r.z_loss_pct) AS loss_pct,
				r.isis_down,
				COALESCE(l.code, '') AS link_code,
				COALESCE(l.link_type, '') AS link_type,
				COALESCE(ma.code, '') AS side_a_metro,
				COALESCE(mz.code, '') AS side_z_metro,
				COALESCE(c.code, '') AS contributor_code,
				COALESCE(l.status, '') AS entity_status,
				r.provisioning AS is_provisioning
			FROM link_rollup_5m r FINAL
			LEFT JOIN dz_links_current l ON r.link_pk = l.pk
			LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
			LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
			WHERE r.bucket_ts >= now() - INTERVAL $1 SECOND
		)
		SELECT link_pk, 'packet_loss' AS incident_type,
			max(loss_pct) AS peak_value, now() AS started_at,
			any(link_code), any(link_type), any(side_a_metro), any(side_z_metro),
			any(contributor_code), any(entity_status), any(is_provisioning)
		FROM recent WHERE loss_pct > 0
		GROUP BY link_pk

		UNION ALL

		SELECT link_pk, 'isis_down',
			toFloat64(1), now(),
			any(link_code), any(link_type), any(side_a_metro), any(side_z_metro),
			any(contributor_code), any(entity_status), any(is_provisioning)
		FROM recent WHERE isis_down = true
		GROUP BY link_pk
	`

	return a.queryLinkSymptoms(ctx, query, int64(symptomLookback.Seconds()))
}

func (a *Activities) fetchLinkCounterSymptoms(ctx context.Context) ([]LinkSymptom, error) {
	// Counter symptoms from device_interface_rollup_5m grouped by link_pk.
	query := `
		WITH recent AS (
			SELECT
				r.link_pk,
				r.in_errors + r.out_errors AS errors,
				r.in_fcs_errors AS fcs,
				r.in_discards + r.out_discards AS discards,
				r.carrier_transitions AS carrier,
				COALESCE(l.code, '') AS link_code,
				COALESCE(l.link_type, '') AS link_type,
				COALESCE(ma.code, '') AS side_a_metro,
				COALESCE(mz.code, '') AS side_z_metro,
				COALESCE(c.code, '') AS contributor_code,
				COALESCE(l.status, '') AS entity_status,
				l.committed_rtt_ns = $2 AS is_provisioning
			FROM device_interface_rollup_5m r FINAL
			LEFT JOIN dz_links_current l ON r.link_pk = l.pk
			LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
			LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
			LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
			LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
			LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
			WHERE r.bucket_ts >= now() - INTERVAL $1 SECOND
			  AND r.link_pk != ''
		)
		SELECT link_pk, 'errors', toFloat64(sum(errors)), now(),
			any(link_code), any(link_type), any(side_a_metro), any(side_z_metro),
			any(contributor_code), any(entity_status), any(is_provisioning)
		FROM recent WHERE errors > 0 GROUP BY link_pk HAVING toFloat64(sum(errors)) >= 1

		UNION ALL

		SELECT link_pk, 'fcs', toFloat64(sum(fcs)), now(),
			any(link_code), any(link_type), any(side_a_metro), any(side_z_metro),
			any(contributor_code), any(entity_status), any(is_provisioning)
		FROM recent WHERE fcs > 0 GROUP BY link_pk HAVING toFloat64(sum(fcs)) >= 1

		UNION ALL

		SELECT link_pk, 'discards', toFloat64(sum(discards)), now(),
			any(link_code), any(link_type), any(side_a_metro), any(side_z_metro),
			any(contributor_code), any(entity_status), any(is_provisioning)
		FROM recent WHERE discards > 0 GROUP BY link_pk HAVING toFloat64(sum(discards)) >= 1

		UNION ALL

		SELECT link_pk, 'carrier', toFloat64(sum(carrier)), now(),
			any(link_code), any(link_type), any(side_a_metro), any(side_z_metro),
			any(contributor_code), any(entity_status), any(is_provisioning)
		FROM recent WHERE carrier > 0 GROUP BY link_pk HAVING toFloat64(sum(carrier)) >= 1
	`

	return a.queryLinkSymptoms(ctx, query, int64(symptomLookback.Seconds()), provisioningSentinel)
}

func (a *Activities) fetchLinkNoDataSymptoms(ctx context.Context) ([]LinkSymptom, error) {
	lookbackSecs := int64(symptomLookback.Seconds())
	recentSecs := int64(recentDataWindow.Seconds())

	// Links that reported in the last 24h but not in the last 15m.
	noLatencyQuery := `
		SELECT l.pk, 'no_latency_data', toFloat64(1), now(),
			COALESCE(l.code, ''), COALESCE(l.link_type, ''),
			COALESCE(ma.code, ''), COALESCE(mz.code, ''),
			COALESCE(c.code, ''), COALESCE(l.status, ''),
			l.committed_rtt_ns = $3 AS is_provisioning
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
		WHERE l.pk IN (
			SELECT DISTINCT link_pk FROM link_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL $1 SECOND
		)
		AND l.pk NOT IN (
			SELECT DISTINCT link_pk FROM link_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL $2 SECOND
		)
	`

	noLatency, err := a.queryLinkSymptoms(ctx, noLatencyQuery, recentSecs, lookbackSecs, provisioningSentinel)
	if err != nil {
		return nil, fmt.Errorf("link no_latency_data: %w", err)
	}

	noTrafficQuery := `
		SELECT l.pk, 'no_traffic_data', toFloat64(1), now(),
			COALESCE(l.code, ''), COALESCE(l.link_type, ''),
			COALESCE(ma.code, ''), COALESCE(mz.code, ''),
			COALESCE(c.code, ''), COALESCE(l.status, ''),
			l.committed_rtt_ns = $3 AS is_provisioning
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
		WHERE l.pk IN (
			SELECT DISTINCT link_pk FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL $1 SECOND AND link_pk != ''
		)
		AND l.pk NOT IN (
			SELECT DISTINCT link_pk FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL $2 SECOND AND link_pk != ''
		)
	`

	noTraffic, err := a.queryLinkSymptoms(ctx, noTrafficQuery, recentSecs, lookbackSecs, provisioningSentinel)
	if err != nil {
		return nil, fmt.Errorf("link no_traffic_data: %w", err)
	}

	return append(noLatency, noTraffic...), nil
}

func (a *Activities) queryLinkSymptoms(ctx context.Context, query string, args ...any) ([]LinkSymptom, error) {
	rows, err := a.ClickHouse.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []LinkSymptom
	for rows.Next() {
		var s LinkSymptom
		if err := rows.Scan(
			&s.LinkPK, &s.IncidentType, &s.PeakValue, &s.StartedAt,
			&s.LinkCode, &s.LinkType, &s.SideAMetro, &s.SideZMetro,
			&s.ContributorCode, &s.Status, &s.Provisioning,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		result = append(result, s)
	}
	return result, nil
}

func (a *Activities) fetchOpenLinkIncidents(ctx context.Context) ([]OpenLinkIncident, error) {
	query := `
		SELECT ie.incident_id, ie.link_pk, ie.started_at,
			ie.active_symptoms, ie.symptoms, ie.severity, ie.event_ts,
			ie.link_code, ie.link_type, ie.side_a_metro, ie.side_z_metro,
			ie.contributor_code, ie.status, ie.provisioning
		FROM link_incident_events ie
		INNER JOIN (
			SELECT incident_id, max(event_ts) AS max_ts
			FROM link_incident_events GROUP BY incident_id
		) latest ON ie.incident_id = latest.incident_id AND ie.event_ts = latest.max_ts
		WHERE ie.event_type != 'resolved'
	`

	rows, err := a.ClickHouse.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []OpenLinkIncident
	for rows.Next() {
		var inc OpenLinkIncident
		var severity string
		if err := rows.Scan(
			&inc.IncidentID, &inc.LinkPK, &inc.StartedAt,
			&inc.ActiveSymptoms, &inc.Symptoms, &severity, &inc.LastEventTS,
			&inc.LinkCode, &inc.LinkType, &inc.SideAMetro, &inc.SideZMetro,
			&inc.ContributorCode, &inc.Status, &inc.Provisioning,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		inc.Severity = Severity(severity)
		result = append(result, inc)
	}
	return result, nil
}

func (a *Activities) writeLinkEvents(ctx context.Context, events []LinkIncidentEvent) error {
	batch, err := a.ClickHouse.PrepareBatch(ctx, `
		INSERT INTO link_incident_events (
			incident_id, link_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			link_code, link_type, side_a_metro, side_z_metro,
			contributor_code, status, provisioning
		)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.IncidentID, e.LinkPK, string(e.EventType), e.EventTS, e.StartedAt,
			e.ActiveSymptoms, e.Symptoms, string(e.Severity), e.PeakValues,
			e.LinkCode, e.LinkType, e.SideAMetro, e.SideZMetro,
			e.ContributorCode, e.Status, e.Provisioning,
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}

	return batch.Send()
}

// --- Device detection ---

func (a *Activities) detectDeviceEvents(ctx context.Context, now time.Time) ([]DeviceIncidentEvent, error) {
	symptoms, err := a.fetchDeviceSymptoms(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch device symptoms: %w", err)
	}

	noData, err := a.fetchDeviceNoDataSymptoms(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch device no-data: %w", err)
	}
	symptoms = append(symptoms, noData...)

	open, err := a.fetchOpenDeviceIncidents(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch open device incidents: %w", err)
	}

	// Convert to diff inputs.
	var inputs []symptomInput
	symptomsByEntity := make(map[string]DeviceSymptom)
	for _, s := range symptoms {
		inputs = append(inputs, symptomInput{
			EntityPK: s.DevicePK, IncidentType: s.IncidentType,
			PeakValue: s.PeakValue, StartedAt: s.StartedAt,
		})
		if _, ok := symptomsByEntity[s.DevicePK]; !ok {
			symptomsByEntity[s.DevicePK] = s
		}
	}

	var openStates []openState
	for _, o := range open {
		openStates = append(openStates, openState{
			IncidentID: o.IncidentID, EntityPK: o.DevicePK,
			StartedAt: o.StartedAt, ActiveSymptoms: o.ActiveSymptoms,
			Symptoms: o.Symptoms, Severity: o.Severity, LastEventTS: o.LastEventTS,
		})
	}

	deltas := a.diff(now, inputs, openStates)

	// Index open incidents by entity for metadata fallback.
	openByEntity := make(map[string]OpenDeviceIncident)
	for _, o := range open {
		openByEntity[o.DevicePK] = o
	}

	events := make([]DeviceIncidentEvent, 0, len(deltas))
	for _, d := range deltas {
		e := DeviceIncidentEvent{
			IncidentID: d.IncidentID, DevicePK: d.EntityPK,
			EventType: d.EventType, EventTS: d.EventTS,
			StartedAt: d.StartedAt, ActiveSymptoms: d.ActiveSymptoms,
			Symptoms: d.Symptoms, Severity: d.Severity, PeakValues: d.PeakValues,
		}
		if s, ok := symptomsByEntity[d.EntityPK]; ok {
			e.DeviceCode = s.DeviceCode
			e.DeviceType = s.DeviceType
			e.Metro = s.Metro
			e.ContributorCode = s.ContributorCode
			e.Status = s.Status
		} else if o, ok := openByEntity[d.EntityPK]; ok {
			e.DeviceCode = o.DeviceCode
			e.DeviceType = o.DeviceType
			e.Metro = o.Metro
			e.ContributorCode = o.ContributorCode
			e.Status = o.Status
		}
		events = append(events, e)
	}

	return events, nil
}

func (a *Activities) fetchDeviceSymptoms(ctx context.Context) ([]DeviceSymptom, error) {
	query := `
		WITH recent AS (
			SELECT
				r.device_pk,
				r.in_errors + r.out_errors AS errors,
				r.in_fcs_errors AS fcs,
				r.in_discards + r.out_discards AS discards,
				r.carrier_transitions AS carrier,
				r.isis_overload, r.isis_unreachable,
				COALESCE(d.code, '') AS device_code,
				COALESCE(d.device_type, '') AS device_type,
				COALESCE(m.code, '') AS metro,
				COALESCE(c.code, '') AS contributor_code,
				COALESCE(d.status, '') AS device_status
			FROM device_interface_rollup_5m r FINAL
			LEFT JOIN dz_devices_current d ON r.device_pk = d.pk
			LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
			LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
			WHERE r.bucket_ts >= now() - INTERVAL $1 SECOND
			  AND r.link_pk = ''
		)
		SELECT device_pk, 'errors', toFloat64(sum(errors)), now(),
			any(device_code), any(device_type), any(metro), any(contributor_code), any(device_status)
		FROM recent WHERE errors > 0 GROUP BY device_pk HAVING toFloat64(sum(errors)) >= 1

		UNION ALL
		SELECT device_pk, 'fcs', toFloat64(sum(fcs)), now(),
			any(device_code), any(device_type), any(metro), any(contributor_code), any(device_status)
		FROM recent WHERE fcs > 0 GROUP BY device_pk HAVING toFloat64(sum(fcs)) >= 1

		UNION ALL
		SELECT device_pk, 'discards', toFloat64(sum(discards)), now(),
			any(device_code), any(device_type), any(metro), any(contributor_code), any(device_status)
		FROM recent WHERE discards > 0 GROUP BY device_pk HAVING toFloat64(sum(discards)) >= 1

		UNION ALL
		SELECT device_pk, 'carrier', toFloat64(sum(carrier)), now(),
			any(device_code), any(device_type), any(metro), any(contributor_code), any(device_status)
		FROM recent WHERE carrier > 0 GROUP BY device_pk HAVING toFloat64(sum(carrier)) >= 1

		UNION ALL
		SELECT device_pk, 'isis_overload', toFloat64(1), now(),
			any(device_code), any(device_type), any(metro), any(contributor_code), any(device_status)
		FROM recent WHERE isis_overload = true GROUP BY device_pk

		UNION ALL
		SELECT device_pk, 'isis_unreachable', toFloat64(1), now(),
			any(device_code), any(device_type), any(metro), any(contributor_code), any(device_status)
		FROM recent WHERE isis_unreachable = true GROUP BY device_pk
	`

	return a.queryDeviceSymptoms(ctx, query, int64(symptomLookback.Seconds()))
}

func (a *Activities) fetchDeviceNoDataSymptoms(ctx context.Context) ([]DeviceSymptom, error) {
	lookbackSecs := int64(symptomLookback.Seconds())
	recentSecs := int64(recentDataWindow.Seconds())

	// Devices that reported traffic in the last 24h but not in the last 15m.
	noTrafficQuery := `
		SELECT d.pk, 'no_traffic_data', toFloat64(1), now(),
			COALESCE(d.code, ''), COALESCE(d.device_type, ''),
			COALESCE(m.code, ''), COALESCE(c.code, ''), COALESCE(d.status, '')
		FROM dz_devices_current d
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		WHERE d.pk IN (
			SELECT DISTINCT device_pk FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL $1 SECOND AND link_pk = ''
		)
		AND d.pk NOT IN (
			SELECT DISTINCT device_pk FROM device_interface_rollup_5m
			WHERE bucket_ts >= now() - INTERVAL $2 SECOND AND link_pk = ''
		)
	`

	noTraffic, err := a.queryDeviceSymptoms(ctx, noTrafficQuery, recentSecs, lookbackSecs)
	if err != nil {
		return nil, fmt.Errorf("device no_traffic_data: %w", err)
	}

	// Devices that had links reporting latency in the last 24h but not in the last 15m.
	noLatencyQuery := `
		WITH device_links AS (
			SELECT da.pk AS device_pk, l.pk AS link_pk
			FROM dz_links_current l
			JOIN dz_devices_current da ON l.side_a_pk = da.pk
			UNION ALL
			SELECT dz.pk AS device_pk, l.pk AS link_pk
			FROM dz_links_current l
			JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		),
		devices_reported_recently AS (
			SELECT DISTINCT dl.device_pk FROM device_links dl
			WHERE dl.link_pk IN (
				SELECT DISTINCT link_pk FROM link_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL $1 SECOND
			)
		),
		devices_reporting_now AS (
			SELECT DISTINCT dl.device_pk FROM device_links dl
			WHERE dl.link_pk IN (
				SELECT DISTINCT link_pk FROM link_rollup_5m
				WHERE bucket_ts >= now() - INTERVAL $2 SECOND
			)
		)
		SELECT d.pk, 'no_latency_data', toFloat64(1), now(),
			COALESCE(d.code, ''), COALESCE(d.device_type, ''),
			COALESCE(m.code, ''), COALESCE(c.code, ''), COALESCE(d.status, '')
		FROM dz_devices_current d
		INNER JOIN devices_reported_recently drr ON d.pk = drr.device_pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
		WHERE d.pk NOT IN (SELECT device_pk FROM devices_reporting_now)
	`

	noLatency, err := a.queryDeviceSymptoms(ctx, noLatencyQuery, recentSecs, lookbackSecs)
	if err != nil {
		return nil, fmt.Errorf("device no_latency_data: %w", err)
	}

	return append(noTraffic, noLatency...), nil
}

func (a *Activities) queryDeviceSymptoms(ctx context.Context, query string, args ...any) ([]DeviceSymptom, error) {
	rows, err := a.ClickHouse.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []DeviceSymptom
	for rows.Next() {
		var s DeviceSymptom
		if err := rows.Scan(
			&s.DevicePK, &s.IncidentType, &s.PeakValue, &s.StartedAt,
			&s.DeviceCode, &s.DeviceType, &s.Metro, &s.ContributorCode, &s.Status,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		result = append(result, s)
	}
	return result, nil
}

func (a *Activities) fetchOpenDeviceIncidents(ctx context.Context) ([]OpenDeviceIncident, error) {
	query := `
		SELECT ie.incident_id, ie.device_pk, ie.started_at,
			ie.active_symptoms, ie.symptoms, ie.severity, ie.event_ts,
			ie.device_code, ie.device_type, ie.metro, ie.contributor_code, ie.status
		FROM device_incident_events ie
		INNER JOIN (
			SELECT incident_id, max(event_ts) AS max_ts
			FROM device_incident_events GROUP BY incident_id
		) latest ON ie.incident_id = latest.incident_id AND ie.event_ts = latest.max_ts
		WHERE ie.event_type != 'resolved'
	`

	rows, err := a.ClickHouse.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var result []OpenDeviceIncident
	for rows.Next() {
		var inc OpenDeviceIncident
		var severity string
		if err := rows.Scan(
			&inc.IncidentID, &inc.DevicePK, &inc.StartedAt,
			&inc.ActiveSymptoms, &inc.Symptoms, &severity, &inc.LastEventTS,
			&inc.DeviceCode, &inc.DeviceType, &inc.Metro, &inc.ContributorCode, &inc.Status,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		inc.Severity = Severity(severity)
		result = append(result, inc)
	}
	return result, nil
}

func (a *Activities) writeDeviceEvents(ctx context.Context, events []DeviceIncidentEvent) error {
	batch, err := a.ClickHouse.PrepareBatch(ctx, `
		INSERT INTO device_incident_events (
			incident_id, device_pk, event_type, event_ts, started_at,
			active_symptoms, symptoms, severity, peak_values,
			device_code, device_type, metro, contributor_code, status
		)`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, e := range events {
		if err := batch.Append(
			e.IncidentID, e.DevicePK, string(e.EventType), e.EventTS, e.StartedAt,
			e.ActiveSymptoms, e.Symptoms, string(e.Severity), e.PeakValues,
			e.DeviceCode, e.DeviceType, e.Metro, e.ContributorCode, e.Status,
		); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}

	return batch.Send()
}

// --- Shared diff logic ---

// diff computes the events needed to reconcile current symptoms with open
// incidents. It operates on entity-agnostic internal types.
func (a *Activities) diff(now time.Time, symptoms []symptomInput, openIncidents []openState) []eventDelta {
	// Group current symptoms by entity.
	currentByEntity := make(map[string][]symptomInput)
	for _, s := range symptoms {
		currentByEntity[s.EntityPK] = append(currentByEntity[s.EntityPK], s)
	}

	// Index open incidents by entity, keeping only the most recent one.
	// If an entity has multiple open incidents (e.g., from backfill overlap),
	// resolve the older ones.
	openByEntity := make(map[string]openState)
	var staleIncidents []openState
	for _, inc := range openIncidents {
		if existing, ok := openByEntity[inc.EntityPK]; ok {
			// Keep the more recent one, mark the older as stale.
			if inc.LastEventTS.After(existing.LastEventTS) {
				staleIncidents = append(staleIncidents, existing)
				openByEntity[inc.EntityPK] = inc
			} else {
				staleIncidents = append(staleIncidents, inc)
			}
		} else {
			openByEntity[inc.EntityPK] = inc
		}
	}

	var events []eventDelta

	// Process entities with current symptoms.
	for entityPK, entitySymptoms := range currentByEntity {
		symptomSet := make([]string, 0, len(entitySymptoms))
		peakMap := make(map[string]float64)
		for _, s := range entitySymptoms {
			if !slices.Contains(symptomSet, s.IncidentType) {
				symptomSet = append(symptomSet, s.IncidentType)
			}
			if existing, ok := peakMap[s.IncidentType]; !ok || s.PeakValue > existing {
				peakMap[s.IncidentType] = s.PeakValue
			}
		}
		sort.Strings(symptomSet)
		peakJSON := marshalPeakValues(peakMap)

		if open, exists := openByEntity[entityPK]; exists {
			prevSet := open.ActiveSymptoms
			sort.Strings(prevSet)

			added := setDiff(symptomSet, prevSet)
			removed := setDiff(prevSet, symptomSet)

			// allSymptoms = union of previously seen symptoms and current active
			allSymptoms := unionSorted(open.Symptoms, symptomSet)

			if len(removed) > 0 {
				sev := a.computeSeverity(symptomSet, peakMap, open.StartedAt, now)
				events = append(events, eventDelta{
					IncidentID: open.IncidentID, EntityPK: entityPK,
					EventType: EventSymptomResolved, EventTS: now,
					StartedAt: open.StartedAt, ActiveSymptoms: symptomSet,
					Symptoms: allSymptoms, Severity: sev, PeakValues: peakJSON,
				})
			}

			if len(added) > 0 {
				sev := a.computeSeverity(symptomSet, peakMap, open.StartedAt, now)
				events = append(events, eventDelta{
					IncidentID: open.IncidentID, EntityPK: entityPK,
					EventType: EventSymptomAdded, EventTS: now,
					StartedAt: open.StartedAt, ActiveSymptoms: symptomSet,
					Symptoms: allSymptoms, Severity: sev, PeakValues: peakJSON,
				})
			}

			// Severity upgrade without symptom changes.
			if len(added) == 0 && len(removed) == 0 {
				newSev := a.computeSeverity(symptomSet, peakMap, open.StartedAt, now)
				if newSev != open.Severity {
					events = append(events, eventDelta{
						IncidentID: open.IncidentID, EntityPK: entityPK,
						EventType: EventSymptomAdded, EventTS: now,
						StartedAt: open.StartedAt, ActiveSymptoms: symptomSet,
						Symptoms: allSymptoms, Severity: newSev, PeakValues: peakJSON,
					})
				}
			}

			delete(openByEntity, entityPK)
		} else {
			// New incident — all symptoms equals active symptoms initially.
			startedAt := earliestStartedAt(entitySymptoms)
			incidentID := generateIncidentID(entityPK, startedAt)
			sev := a.computeSeverity(symptomSet, peakMap, startedAt, now)

			events = append(events, eventDelta{
				IncidentID: incidentID, EntityPK: entityPK,
				EventType: EventOpened, EventTS: now,
				StartedAt: startedAt, ActiveSymptoms: symptomSet,
				Symptoms: symptomSet, Severity: sev, PeakValues: peakJSON,
			})
		}
	}

	// Open incidents with no current symptoms.
	for entityPK, open := range openByEntity {
		if len(open.ActiveSymptoms) == 0 {
			if now.Sub(open.LastEventTS) >= a.CoalesceGap {
				events = append(events, eventDelta{
					IncidentID: open.IncidentID, EntityPK: entityPK,
					EventType: EventResolved, EventTS: now,
					StartedAt: open.StartedAt, ActiveSymptoms: []string{},
					Symptoms: open.Symptoms, Severity: open.Severity, PeakValues: "{}",
				})
			}
		} else {
			events = append(events, eventDelta{
				IncidentID: open.IncidentID, EntityPK: entityPK,
				EventType: EventSymptomResolved, EventTS: now,
				StartedAt: open.StartedAt, ActiveSymptoms: []string{},
				Symptoms: open.Symptoms, Severity: open.Severity, PeakValues: "{}",
			})
		}
	}

	// Resolve stale incidents (duplicate open incidents for the same entity).
	for _, stale := range staleIncidents {
		events = append(events, eventDelta{
			IncidentID: stale.IncidentID, EntityPK: stale.EntityPK,
			EventType: EventResolved, EventTS: now,
			StartedAt: stale.StartedAt, ActiveSymptoms: []string{},
			Symptoms: stale.Symptoms, Severity: stale.Severity, PeakValues: "{}",
		})
	}

	return events
}

func (a *Activities) computeSeverity(symptoms []string, peakValues map[string]float64, startedAt, now time.Time) Severity {
	duration := now.Sub(startedAt)
	severities := make([]Severity, 0, len(symptoms))
	for _, sym := range symptoms {
		severities = append(severities, symptomSeverity(sym, peakValues[sym], duration, a.EscalationThreshold))
	}
	return maxSeverity(severities...)
}

// --- Helpers ---

func generateIncidentID(entityPK string, startedAt time.Time) string {
	h := sha256.Sum256([]byte(entityPK + "|" + strconv.FormatInt(startedAt.Unix(), 10)))
	return fmt.Sprintf("%x", h[:8])
}

func earliestStartedAt(symptoms []symptomInput) time.Time {
	earliest := symptoms[0].StartedAt
	for _, s := range symptoms[1:] {
		if s.StartedAt.Before(earliest) {
			earliest = s.StartedAt
		}
	}
	return earliest
}

func unionSorted(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, v := range a {
		seen[v] = struct{}{}
	}
	for _, v := range b {
		seen[v] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for v := range seen {
		result = append(result, v)
	}
	sort.Strings(result)
	return result
}

func setDiff(a, b []string) []string {
	var diff []string
	for _, v := range a {
		if !slices.Contains(b, v) {
			diff = append(diff, v)
		}
	}
	return diff
}

func marshalPeakValues(m map[string]float64) string {
	data, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(data)
}
