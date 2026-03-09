package incidents

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
)

// DetectLinkIncidents runs all link incident detection queries and returns a unified list.
func DetectLinkIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, params DetectionParams) ([]Incident, error) {
	linkMeta, err := fetchLinkMeta(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("fetch link metadata: %w", err)
	}

	var allIncidents []Incident
	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		incidents, err := detectPacketLossIncidents(gCtx, conn, duration, params.PacketLossThreshold, linkMeta)
		if err != nil {
			return fmt.Errorf("packet loss: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	counterTypes := []struct {
		name      string
		threshold int64
		metricSQL string
	}{
		{"errors", params.ErrorsThreshold, "sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))"},
		{"discards", params.DiscardsThreshold, "sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))"},
		{"carrier", params.CarrierThreshold, "sum(greatest(0, coalesce(carrier_transitions_delta, 0)))"},
	}

	for _, ct := range counterTypes {
		g.Go(func() error {
			incidents, err := detectCounterIncidents(gCtx, conn, duration, ct.threshold, ct.metricSQL, ct.name, linkMeta, params)
			if err != nil {
				return fmt.Errorf("%s: %w", ct.name, err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	g.Go(func() error {
		incidents, err := detectNoDataIncidents(gCtx, conn, duration, linkMeta)
		if err != nil {
			return fmt.Errorf("no_data: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Filter by min duration and set confirmed
	now := time.Now()
	minDurationSecs := int64(params.MinDuration.Seconds())
	filtered := allIncidents[:0]
	for i := range allIncidents {
		inc := &allIncidents[i]
		if inc.IsOngoing {
			inc.Confirmed = now.Sub(inc.StartedAt) >= params.MinDuration
			filtered = append(filtered, *inc)
		} else if inc.DurationSeconds == nil || *inc.DurationSeconds >= minDurationSecs {
			inc.Confirmed = true
			filtered = append(filtered, *inc)
		}
	}

	// Enrich counter incidents with affected interfaces
	enrichLinkIncidentsWithInterfaces(ctx, conn, filtered)

	return filtered, nil
}

// DetectDeviceIncidents runs all device incident detection queries.
func DetectDeviceIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, params DetectionParams) ([]Incident, error) {
	devMeta, err := fetchDeviceMeta(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("fetch device metadata: %w", err)
	}

	var allIncidents []Incident
	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)

	counterTypes := []struct {
		name      string
		threshold int64
		metricSQL string
	}{
		{"errors", params.ErrorsThreshold, "sum(greatest(0, coalesce(in_errors_delta, 0))) + sum(greatest(0, coalesce(out_errors_delta, 0)))"},
		{"discards", params.DiscardsThreshold, "sum(greatest(0, coalesce(in_discards_delta, 0))) + sum(greatest(0, coalesce(out_discards_delta, 0)))"},
		{"carrier", params.CarrierThreshold, "sum(greatest(0, coalesce(carrier_transitions_delta, 0)))"},
	}

	for _, ct := range counterTypes {
		g.Go(func() error {
			incidents, err := detectDeviceCounterIncidents(gCtx, conn, duration, ct.threshold, ct.metricSQL, ct.name, devMeta, params)
			if err != nil {
				return fmt.Errorf("device %s: %w", ct.name, err)
			}
			mu.Lock()
			allIncidents = append(allIncidents, incidents...)
			mu.Unlock()
			return nil
		})
	}

	g.Go(func() error {
		incidents, err := detectDeviceNoDataIncidents(gCtx, conn, duration, devMeta)
		if err != nil {
			return fmt.Errorf("device no_data: %w", err)
		}
		mu.Lock()
		allIncidents = append(allIncidents, incidents...)
		mu.Unlock()
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	now := time.Now()
	minDurationSecs := int64(params.MinDuration.Seconds())
	filtered := allIncidents[:0]
	for i := range allIncidents {
		inc := &allIncidents[i]
		if inc.IsOngoing {
			inc.Confirmed = now.Sub(inc.StartedAt) >= params.MinDuration
			filtered = append(filtered, *inc)
		} else if inc.DurationSeconds == nil || *inc.DurationSeconds >= minDurationSecs {
			inc.Confirmed = true
			filtered = append(filtered, *inc)
		}
	}

	enrichDeviceIncidentsWithInterfaces(ctx, conn, filtered)

	return filtered, nil
}

// fetchLinkMeta fetches all link metadata with current status.
func fetchLinkMeta(ctx context.Context, conn driver.Conn) (map[string]linkMeta, error) {
	query := `
		SELECT
			l.pk,
			l.code,
			l.link_type,
			COALESCE(ma.code, '') AS side_a_metro,
			COALESCE(mz.code, '') AS side_z_metro,
			COALESCE(c.code, '') AS contributor_code,
			l.status
		FROM dz_links_current l
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]linkMeta)
	for rows.Next() {
		var lm linkMeta
		if err := rows.Scan(&lm.LinkPK, &lm.LinkCode, &lm.LinkType, &lm.SideAMetro, &lm.SideZMetro, &lm.ContributorCode, &lm.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[lm.LinkPK] = lm
	}
	return result, nil
}

// fetchDeviceMeta fetches all device metadata with current status.
func fetchDeviceMeta(ctx context.Context, conn driver.Conn) (map[string]deviceMeta, error) {
	query := `
		SELECT
			d.pk,
			d.code,
			d.device_type,
			COALESCE(m.code, '') AS metro,
			COALESCE(c.code, '') AS contributor_code,
			d.status
		FROM dz_devices_current d
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
	`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]deviceMeta)
	for rows.Next() {
		var dm deviceMeta
		if err := rows.Scan(&dm.DevicePK, &dm.DeviceCode, &dm.DeviceType, &dm.Metro, &dm.ContributorCode, &dm.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[dm.DevicePK] = dm
	}
	return result, nil
}

// linkIncidentToIncident converts link detection results to the unified Incident type.
func linkIncidentToIncident(lm linkMeta, incidentType string, startedAt time.Time, endedAt *time.Time, isOngoing bool, severity string, thresholdPct *float64, peakLossPct *float64, thresholdCount *int64, peakCount *int64, durationSecs *int64) Incident {
	isDrained := lm.Status == "soft-drained" || lm.Status == "hard-drained"
	inc := Incident{
		EntityType:      "link",
		EntityPK:        lm.LinkPK,
		IncidentType:    incidentType,
		StartedAt:       startedAt,
		EndedAt:         endedAt,
		IsOngoing:       isOngoing,
		Severity:        severity,
		IsDrained:       isDrained,
		EntityCode:      lm.LinkCode,
		LinkType:        strPtr(lm.LinkType),
		SideAMetro:      strPtr(lm.SideAMetro),
		SideZMetro:      strPtr(lm.SideZMetro),
		ContributorCode: strPtr(lm.ContributorCode),
		ThresholdPct:    thresholdPct,
		PeakLossPct:     peakLossPct,
		ThresholdCount:  thresholdCount,
		PeakCount:       peakCount,
		DurationSeconds: durationSecs,
		UpdatedAt:       time.Now(),
	}
	if isDrained {
		inc.DrainStatus = strPtr(lm.Status)
	}
	return inc
}

// deviceIncidentToIncident converts device detection results to the unified Incident type.
func deviceIncidentToIncident(dm deviceMeta, incidentType string, startedAt time.Time, endedAt *time.Time, isOngoing bool, severity string, thresholdCount *int64, peakCount *int64, durationSecs *int64) Incident {
	isDrained := dm.Status == "soft-drained" || dm.Status == "hard-drained"
	inc := Incident{
		EntityType:      "device",
		EntityPK:        dm.DevicePK,
		IncidentType:    incidentType,
		StartedAt:       startedAt,
		EndedAt:         endedAt,
		IsOngoing:       isOngoing,
		Severity:        severity,
		IsDrained:       isDrained,
		EntityCode:      dm.DeviceCode,
		DeviceType:      strPtr(dm.DeviceType),
		Metro:           strPtr(dm.Metro),
		ContributorCode: strPtr(dm.ContributorCode),
		ThresholdCount:  thresholdCount,
		PeakCount:       peakCount,
		DurationSeconds: durationSecs,
		UpdatedAt:       time.Now(),
	}
	if isDrained {
		inc.DrainStatus = strPtr(dm.Status)
	}
	return inc
}

// packetLossSeverity returns severity based on peak loss percentage.
func packetLossSeverity(peakLossPct float64) string {
	if peakLossPct >= 10.0 {
		return "incident"
	}
	return "degraded"
}

// incidentSeverity returns severity based on incident type.
func incidentSeverity(incidentType string, peakLossPct float64) string {
	switch incidentType {
	case "packet_loss":
		return packetLossSeverity(peakLossPct)
	case "carrier":
		return "incident"
	default:
		return "degraded"
	}
}

// counterTypeFilter returns the SQL WHERE clause for non-zero counter activity.
func counterTypeFilter(incidentType string) string {
	switch incidentType {
	case "errors":
		return "(coalesce(in_errors_delta, 0) > 0 OR coalesce(out_errors_delta, 0) > 0)"
	case "discards":
		return "(coalesce(in_discards_delta, 0) > 0 OR coalesce(out_discards_delta, 0) > 0)"
	case "carrier":
		return "(coalesce(carrier_transitions_delta, 0) > 0)"
	default:
		return "1=0"
	}
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// detectPacketLossIncidents detects packet loss incidents on links.
func detectPacketLossIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, threshold float64, meta map[string]linkMeta) ([]Incident, error) {
	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil
	}

	// Find currently high-loss links (ongoing)
	ongoingIncidents, ongoingLinks, err := fetchCurrentHighLossLinks(ctx, conn, threshold, meta)
	if err != nil {
		return nil, err
	}

	// Fetch historical buckets
	query := `
		WITH buckets AS (
			SELECT
				lat.link_pk,
				toStartOfInterval(lat.event_ts, INTERVAL 5 MINUTE) as bucket,
				countIf(lat.loss = true OR lat.rtt_us = 0) * 100.0 / count(*) as loss_pct,
				count(*) as sample_count
			FROM fact_dz_device_link_latency lat
			WHERE lat.event_ts >= now() - INTERVAL $1 SECOND
			  AND lat.link_pk IN ($2)
			GROUP BY lat.link_pk, bucket
			HAVING count(*) >= 3
		)
		SELECT b.link_pk, b.bucket, b.loss_pct, b.sample_count
		FROM buckets b
		ORDER BY b.link_pk, b.bucket
	`

	lookbackSecs := int64((duration + 24*time.Hour).Seconds())
	rows, err := conn.Query(ctx, query, lookbackSecs, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query historical buckets: %w", err)
	}
	defer rows.Close()

	var buckets []lossBucket
	for rows.Next() {
		var lb lossBucket
		if err := rows.Scan(&lb.LinkPK, &lb.Bucket, &lb.LossPct, &lb.SampleCount); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		buckets = append(buckets, lb)
	}

	completedIncidents := pairPacketLossCompleted(buckets, meta, threshold, ongoingLinks)
	all := append(ongoingIncidents, completedIncidents...)
	all = coalescePacketLossIncidents(all)
	return all, nil
}

// fetchCurrentHighLossLinks finds links with current high packet loss.
func fetchCurrentHighLossLinks(ctx context.Context, conn driver.Conn, threshold float64, meta map[string]linkMeta) ([]Incident, map[string]bool, error) {
	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil, nil
	}

	query := `
		WITH recent_buckets AS (
			SELECT
				lat.link_pk,
				toStartOfInterval(lat.event_ts, INTERVAL 5 MINUTE) as bucket,
				countIf(lat.loss = true OR lat.rtt_us = 0) * 100.0 / count(*) as loss_pct,
				count(*) as sample_count
			FROM fact_dz_device_link_latency lat
			WHERE lat.event_ts >= now() - INTERVAL 15 MINUTE
			  AND lat.link_pk IN ($2)
			GROUP BY lat.link_pk, bucket
			HAVING count(*) >= 3
		),
		ranked AS (
			SELECT link_pk, bucket, loss_pct,
				ROW_NUMBER() OVER (PARTITION BY link_pk ORDER BY bucket DESC) AS rn
			FROM recent_buckets
		)
		SELECT link_pk, bucket, loss_pct, rn
		FROM ranked
		WHERE rn <= 3
		ORDER BY link_pk, rn
	`

	rows, err := conn.Query(ctx, query, threshold, linkPKs)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	type recentBucket struct {
		Bucket  time.Time
		LossPct float64
		Rank    uint64
	}
	linkBuckets := make(map[string][]recentBucket)

	for rows.Next() {
		var linkPK string
		var bucket time.Time
		var lossPct float64
		var rn uint64
		if err := rows.Scan(&linkPK, &bucket, &lossPct, &rn); err != nil {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}
		linkBuckets[linkPK] = append(linkBuckets[linkPK], recentBucket{bucket, lossPct, rn})
	}

	var incidents []Incident
	ongoingLinks := make(map[string]bool)

	for linkPK, buckets := range linkBuckets {
		lm, ok := meta[linkPK]
		if !ok {
			continue
		}

		consecutiveAbove := 0
		for _, b := range buckets {
			if b.LossPct >= threshold {
				consecutiveAbove++
			} else {
				break
			}
		}
		if consecutiveAbove < 2 {
			continue
		}

		startedAt, peakLoss, err := findPacketLossEventStart(ctx, conn, linkPK, threshold)
		if err != nil {
			startedAt = buckets[0].Bucket.Add(-10 * time.Minute)
			peakLoss = buckets[0].LossPct
		}

		thresholdPct := threshold
		incidents = append(incidents, linkIncidentToIncident(lm, "packet_loss", startedAt, nil, true,
			packetLossSeverity(peakLoss), &thresholdPct, &peakLoss, nil, nil, nil))

		ongoingLinks[lm.LinkCode] = true
	}

	return incidents, ongoingLinks, nil
}

// findPacketLossEventStart looks back in history to find when the current event started.
func findPacketLossEventStart(ctx context.Context, conn driver.Conn, linkPK string, threshold float64) (time.Time, float64, error) {
	query := `
		WITH buckets AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
				countIf(loss = true OR rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency
			WHERE link_pk = $1
			  AND event_ts >= now() - INTERVAL 365 DAY
			GROUP BY bucket
			HAVING count(*) >= 3
			ORDER BY bucket DESC
		),
		above AS (
			SELECT
				bucket, loss_pct,
				lagInFrame(bucket, 1) OVER (ORDER BY bucket ASC) as prev_bucket
			FROM buckets
			WHERE loss_pct >= $2
		)
		SELECT bucket, loss_pct
		FROM above
		WHERE prev_bucket IS NULL OR dateDiff('minute', prev_bucket, bucket) > 15
		ORDER BY bucket DESC
		LIMIT 1
	`

	var startBucket time.Time
	var peakLoss float64
	if err := conn.QueryRow(ctx, query, linkPK, threshold).Scan(&startBucket, &peakLoss); err != nil {
		return time.Time{}, 0, err
	}

	peakQuery := `
		SELECT max(loss_pct) as peak_loss FROM (
			SELECT countIf(loss = true OR rtt_us = 0) * 100.0 / count(*) as loss_pct
			FROM fact_dz_device_link_latency
			WHERE link_pk = $1
			  AND event_ts >= $2
			GROUP BY toStartOfInterval(event_ts, INTERVAL 5 MINUTE)
			HAVING count(*) >= 3
		)
	`

	var peak float64
	if err := conn.QueryRow(ctx, peakQuery, linkPK, startBucket).Scan(&peak); err == nil && peak > peakLoss {
		peakLoss = peak
	}

	return startBucket, peakLoss, nil
}

// pairPacketLossCompleted finds completed packet loss events from historical buckets.
func pairPacketLossCompleted(buckets []lossBucket, meta map[string]linkMeta, threshold float64, excludeLinks map[string]bool) []Incident {
	var incidents []Incident

	byLink := make(map[string][]lossBucket)
	for _, b := range buckets {
		byLink[b.LinkPK] = append(byLink[b.LinkPK], b)
	}

	for linkPK, linkBuckets := range byLink {
		lm, ok := meta[linkPK]
		if !ok || excludeLinks[lm.LinkCode] {
			continue
		}

		sort.Slice(linkBuckets, func(i, j int) bool {
			return linkBuckets[i].Bucket.Before(linkBuckets[j].Bucket)
		})

		var activeStart *time.Time
		var peakLoss float64
		var consecutiveBuckets int

		for i, b := range linkBuckets {
			aboveThreshold := b.LossPct >= threshold

			if i == 0 {
				if aboveThreshold {
					t := b.Bucket
					activeStart = &t
					peakLoss = b.LossPct
					consecutiveBuckets = 1
				}
				continue
			}

			prevAbove := linkBuckets[i-1].LossPct >= threshold

			if aboveThreshold && !prevAbove {
				t := b.Bucket
				activeStart = &t
				peakLoss = b.LossPct
				consecutiveBuckets = 1
			} else if !aboveThreshold && prevAbove && activeStart != nil {
				if consecutiveBuckets >= 2 {
					endTime := linkBuckets[i-1].Bucket.Add(5 * time.Minute)
					durationSecs := int64(endTime.Sub(*activeStart).Seconds())
					thresholdPct := threshold
					incidents = append(incidents, linkIncidentToIncident(lm, "packet_loss", *activeStart, &endTime, false,
						packetLossSeverity(peakLoss), &thresholdPct, &peakLoss, nil, nil, &durationSecs))
				}
				activeStart = nil
				peakLoss = 0
				consecutiveBuckets = 0
			} else if aboveThreshold && activeStart != nil {
				consecutiveBuckets++
				if b.LossPct > peakLoss {
					peakLoss = b.LossPct
				}
			}
		}

		if activeStart != nil && consecutiveBuckets >= 2 {
			lastBucket := linkBuckets[len(linkBuckets)-1]
			endTime := lastBucket.Bucket.Add(5 * time.Minute)
			durationSecs := int64(endTime.Sub(*activeStart).Seconds())
			thresholdPct := threshold
			incidents = append(incidents, linkIncidentToIncident(lm, "packet_loss", *activeStart, &endTime, false,
				packetLossSeverity(peakLoss), &thresholdPct, &peakLoss, nil, nil, &durationSecs))
		}
	}

	return incidents
}

// coalescePacketLossIncidents merges nearby packet loss incidents.
func coalescePacketLossIncidents(incidents []Incident) []Incident {
	return coalesceIncidents(incidents, 15*time.Minute)
}

// coalesceIncidents merges incidents on the same entity separated by less than the gap.
func coalesceIncidents(incidents []Incident, gap time.Duration) []Incident {
	if len(incidents) <= 1 {
		return incidents
	}

	type key struct {
		EntityPK     string
		IncidentType string
	}
	byKey := make(map[key][]Incident)
	for _, inc := range incidents {
		k := key{inc.EntityPK, inc.IncidentType}
		byKey[k] = append(byKey[k], inc)
	}

	var result []Incident
	for _, group := range byKey {
		if len(group) <= 1 {
			result = append(result, group...)
			continue
		}

		sort.Slice(group, func(i, j int) bool {
			return group[i].StartedAt.Before(group[j].StartedAt)
		})

		merged := group[0]
		for i := 1; i < len(group); i++ {
			curr := group[i]

			if merged.IsOngoing {
				if curr.PeakLossPct != nil && (merged.PeakLossPct == nil || *curr.PeakLossPct > *merged.PeakLossPct) {
					merged.PeakLossPct = curr.PeakLossPct
				}
				if curr.PeakCount != nil && (merged.PeakCount == nil || *curr.PeakCount > *merged.PeakCount) {
					merged.PeakCount = curr.PeakCount
				}
				continue
			}

			if merged.EndedAt == nil {
				result = append(result, merged)
				merged = curr
				continue
			}

			if curr.StartedAt.Sub(*merged.EndedAt) < gap {
				if curr.IsOngoing {
					merged.EndedAt = nil
					merged.DurationSeconds = nil
					merged.IsOngoing = true
				} else {
					merged.EndedAt = curr.EndedAt
					if curr.EndedAt != nil {
						d := int64(curr.EndedAt.Sub(merged.StartedAt).Seconds())
						merged.DurationSeconds = &d
					}
				}
				if curr.PeakLossPct != nil && (merged.PeakLossPct == nil || *curr.PeakLossPct > *merged.PeakLossPct) {
					merged.PeakLossPct = curr.PeakLossPct
				}
				if curr.PeakCount != nil && (merged.PeakCount == nil || *curr.PeakCount > *merged.PeakCount) {
					merged.PeakCount = curr.PeakCount
				}
			} else {
				result = append(result, merged)
				merged = curr
			}
		}
		result = append(result, merged)
	}

	return result
}

// detectCounterIncidents detects counter-based incidents (errors, discards, carrier) on links.
func detectCounterIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, threshold int64, metricSQL, incidentType string, meta map[string]linkMeta, params DetectionParams) ([]Incident, error) {
	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil
	}

	// Current high-counter links
	ongoingIncidents, ongoingLinks, err := fetchCurrentHighCounterLinks(ctx, conn, threshold, metricSQL, incidentType, meta, params)
	if err != nil {
		return nil, err
	}

	// Historical buckets
	query := fmt.Sprintf(`
		SELECT
			ic.link_pk,
			toStartOfInterval(ic.event_ts, INTERVAL 5 MINUTE) as bucket,
			%s as metric_value
		FROM fact_dz_device_interface_counters ic
		WHERE ic.event_ts >= now() - INTERVAL $1 SECOND
		  AND ic.link_pk IN ($2)
		GROUP BY ic.link_pk, bucket
		ORDER BY ic.link_pk, bucket
	`, metricSQL)

	lookbackSecs := int64((duration + 24*time.Hour).Seconds())
	rows, err := conn.Query(ctx, query, lookbackSecs, linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query historical buckets: %w", err)
	}
	defer rows.Close()

	var buckets []counterBucket
	for rows.Next() {
		var cb counterBucket
		if err := rows.Scan(&cb.EntityPK, &cb.Bucket, &cb.Value); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		buckets = append(buckets, cb)
	}

	completed := pairCounterIncidentsCompleted(buckets, meta, threshold, incidentType, ongoingLinks, params)
	all := append(ongoingIncidents, completed...)
	return coalesceIncidents(all, params.CoalesceGap), nil
}

// fetchCurrentHighCounterLinks finds links currently experiencing counter metrics above threshold.
func fetchCurrentHighCounterLinks(ctx context.Context, conn driver.Conn, threshold int64, metricSQL, incidentType string, meta map[string]linkMeta, params DetectionParams) ([]Incident, map[string]bool, error) {
	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil, nil
	}

	query := fmt.Sprintf(`
		WITH recent_buckets AS (
			SELECT
				ic.link_pk,
				toStartOfInterval(ic.event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters ic
			WHERE ic.event_ts >= now() - INTERVAL 15 MINUTE
			  AND ic.link_pk IN ($1)
			GROUP BY ic.link_pk, bucket
		),
		ranked AS (
			SELECT link_pk, bucket, metric_value,
				ROW_NUMBER() OVER (PARTITION BY link_pk ORDER BY bucket DESC) AS rn
			FROM recent_buckets
		)
		SELECT link_pk, bucket, metric_value, rn
		FROM ranked
		WHERE rn <= 3
		ORDER BY link_pk, rn
	`, metricSQL)

	rows, err := conn.Query(ctx, query, linkPKs)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	type recentBucket struct {
		Bucket time.Time
		Value  int64
		Rank   uint64
	}
	linkBuckets := make(map[string][]recentBucket)

	for rows.Next() {
		var linkPK string
		var bucket time.Time
		var value int64
		var rn uint64
		if err := rows.Scan(&linkPK, &bucket, &value, &rn); err != nil {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}
		linkBuckets[linkPK] = append(linkBuckets[linkPK], recentBucket{bucket, value, rn})
	}

	var incidents []Incident
	ongoingLinks := make(map[string]bool)

	for linkPK, buckets := range linkBuckets {
		lm, ok := meta[linkPK]
		if !ok {
			continue
		}

		consecutiveAbove := 0
		for _, b := range buckets {
			if b.Value >= threshold {
				consecutiveAbove++
			} else {
				break
			}
		}
		if consecutiveAbove < params.MinBuckets() {
			continue
		}

		startedAt, peakValue, err := findCounterIncidentStart(ctx, conn, linkPK, threshold, metricSQL)
		if err != nil {
			startedAt = buckets[0].Bucket.Add(-10 * time.Minute)
			peakValue = buckets[0].Value
		}

		thresholdCount := threshold
		incidents = append(incidents, linkIncidentToIncident(lm, incidentType, startedAt, nil, true,
			incidentSeverity(incidentType, 0), nil, nil, &thresholdCount, &peakValue, nil))

		ongoingLinks[lm.LinkCode] = true
	}

	return incidents, ongoingLinks, nil
}

// findCounterIncidentStart looks back in history to find when the current counter incident started.
func findCounterIncidentStart(ctx context.Context, conn driver.Conn, entityPK string, threshold int64, metricSQL string) (time.Time, int64, error) {
	query := fmt.Sprintf(`
		WITH buckets AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE link_pk = $1
			  AND event_ts >= now() - INTERVAL 7 DAY
			GROUP BY bucket
			ORDER BY bucket DESC
		),
		above AS (
			SELECT bucket, metric_value,
				lagInFrame(bucket, 1) OVER (ORDER BY bucket ASC) as prev_bucket
			FROM buckets
			WHERE metric_value >= $2
		)
		SELECT bucket, metric_value
		FROM above
		WHERE prev_bucket IS NULL OR dateDiff('minute', prev_bucket, bucket) > 15
		ORDER BY bucket DESC
		LIMIT 1
	`, metricSQL)

	var startBucket time.Time
	var value int64
	if err := conn.QueryRow(ctx, query, entityPK, threshold).Scan(&startBucket, &value); err != nil {
		return time.Time{}, 0, err
	}

	peakQuery := fmt.Sprintf(`
		SELECT max(metric_value) FROM (
			SELECT %s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE link_pk = $1
			  AND event_ts >= $2
			GROUP BY toStartOfInterval(event_ts, INTERVAL 5 MINUTE)
		)
	`, metricSQL)

	var peak int64
	if err := conn.QueryRow(ctx, peakQuery, entityPK, startBucket).Scan(&peak); err == nil && peak > value {
		value = peak
	}

	return startBucket, value, nil
}

// pairCounterIncidentsCompleted finds completed counter incidents from historical buckets.
func pairCounterIncidentsCompleted(buckets []counterBucket, meta map[string]linkMeta, threshold int64, incidentType string, excludeLinks map[string]bool, params DetectionParams) []Incident {
	var incidents []Incident

	byEntity := make(map[string][]counterBucket)
	for _, b := range buckets {
		byEntity[b.EntityPK] = append(byEntity[b.EntityPK], b)
	}

	for entityPK, entityBuckets := range byEntity {
		lm, ok := meta[entityPK]
		if !ok || excludeLinks[lm.LinkCode] {
			continue
		}

		sort.Slice(entityBuckets, func(i, j int) bool {
			return entityBuckets[i].Bucket.Before(entityBuckets[j].Bucket)
		})

		var activeStart *time.Time
		var peakValue int64
		var consecutiveBuckets int

		for i, b := range entityBuckets {
			aboveThreshold := b.Value >= threshold

			if i == 0 {
				if aboveThreshold {
					t := b.Bucket
					activeStart = &t
					peakValue = b.Value
					consecutiveBuckets = 1
				}
				continue
			}

			prevAbove := entityBuckets[i-1].Value >= threshold

			if aboveThreshold && !prevAbove {
				t := b.Bucket
				activeStart = &t
				peakValue = b.Value
				consecutiveBuckets = 1
			} else if !aboveThreshold && prevAbove && activeStart != nil {
				if consecutiveBuckets >= params.MinBuckets() {
					endTime := entityBuckets[i-1].Bucket.Add(5 * time.Minute)
					durationSecs := int64(endTime.Sub(*activeStart).Seconds())
					thresholdCount := threshold
					incidents = append(incidents, linkIncidentToIncident(lm, incidentType, *activeStart, &endTime, false,
						incidentSeverity(incidentType, 0), nil, nil, &thresholdCount, &peakValue, &durationSecs))
				}
				activeStart = nil
				peakValue = 0
				consecutiveBuckets = 0
			} else if aboveThreshold && activeStart != nil {
				consecutiveBuckets++
				if b.Value > peakValue {
					peakValue = b.Value
				}
			}
		}

		if activeStart != nil && consecutiveBuckets >= params.MinBuckets() {
			lastBucket := entityBuckets[len(entityBuckets)-1]
			endTime := lastBucket.Bucket.Add(5 * time.Minute)
			durationSecs := int64(endTime.Sub(*activeStart).Seconds())
			thresholdCount := threshold
			incidents = append(incidents, linkIncidentToIncident(lm, incidentType, *activeStart, &endTime, false,
				incidentSeverity(incidentType, 0), nil, nil, &thresholdCount, &peakValue, &durationSecs))
		}
	}

	return incidents
}

// detectNoDataIncidents detects no-data incidents on links.
func detectNoDataIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, meta map[string]linkMeta) ([]Incident, error) {
	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil
	}

	// Current no-data links
	currentIncidents, ongoingLinks, err := fetchCurrentNoDataLinks(ctx, conn, meta)
	if err != nil {
		return nil, err
	}

	// Historical no-data events
	completedIncidents, err := findCompletedNoDataEvents(ctx, conn, duration, meta, ongoingLinks)
	if err != nil {
		return nil, err
	}

	return append(currentIncidents, completedIncidents...), nil
}

// fetchCurrentNoDataLinks finds links currently not reporting data.
func fetchCurrentNoDataLinks(ctx context.Context, conn driver.Conn, meta map[string]linkMeta) ([]Incident, map[string]bool, error) {
	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil, nil
	}

	query := `
		WITH link_last_seen AS (
			SELECT link_pk, max(event_ts) as last_seen
			FROM fact_dz_device_link_latency
			WHERE event_ts >= now() - INTERVAL 30 DAY
			  AND link_pk IN ($1)
			GROUP BY link_pk
		)
		SELECT lls.link_pk, lls.last_seen
		FROM link_last_seen lls
		WHERE lls.last_seen < now() - INTERVAL 15 MINUTE
		  AND lls.last_seen >= now() - INTERVAL 30 DAY
	`

	rows, err := conn.Query(ctx, query, linkPKs)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var incidents []Incident
	ongoingLinks := make(map[string]bool)

	for rows.Next() {
		var linkPK string
		var lastSeen time.Time
		if err := rows.Scan(&linkPK, &lastSeen); err != nil {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}

		lm, ok := meta[linkPK]
		if !ok {
			continue
		}

		startedAt := lastSeen.Add(5 * time.Minute)
		incidents = append(incidents, linkIncidentToIncident(lm, "no_data", startedAt, nil, true,
			"incident", nil, nil, nil, nil, nil))

		ongoingLinks[lm.LinkCode] = true
	}

	return incidents, ongoingLinks, nil
}

const noDataGapThreshold = 15 * time.Minute

// findCompletedNoDataEvents finds gaps in data within the time range.
func findCompletedNoDataEvents(ctx context.Context, conn driver.Conn, duration time.Duration, meta map[string]linkMeta, excludeLinks map[string]bool) ([]Incident, error) {
	drainedPeriods, err := fetchDrainedPeriods(ctx, conn, duration)
	if err != nil {
		return nil, fmt.Errorf("fetch drained periods: %w", err)
	}

	linkPKs := linkPKList(meta)
	if len(linkPKs) == 0 {
		return nil, nil
	}

	query := `
		SELECT link_pk, toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket
		FROM fact_dz_device_link_latency
		WHERE event_ts >= now() - INTERVAL $1 SECOND
		  AND link_pk IN ($2)
		GROUP BY link_pk, bucket
		ORDER BY link_pk, bucket
	`

	rows, err := conn.Query(ctx, query, int64(duration.Seconds()), linkPKs)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	byLink := make(map[string][]time.Time)
	for rows.Next() {
		var linkPK string
		var bucket time.Time
		if err := rows.Scan(&linkPK, &bucket); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		byLink[linkPK] = append(byLink[linkPK], bucket)
	}

	var incidents []Incident
	for linkPK, linkBuckets := range byLink {
		lm, ok := meta[linkPK]
		if !ok || excludeLinks[lm.LinkCode] {
			continue
		}

		sort.Slice(linkBuckets, func(i, j int) bool {
			return linkBuckets[i].Before(linkBuckets[j])
		})

		for i := 1; i < len(linkBuckets); i++ {
			gap := linkBuckets[i].Sub(linkBuckets[i-1])
			if gap >= noDataGapThreshold {
				gapStart := linkBuckets[i-1].Add(5 * time.Minute)
				gapEnd := linkBuckets[i]

				if periods, ok := drainedPeriods[linkPK]; ok {
					if gapOverlapsDrainedPeriod(gapStart, gapEnd, periods) {
						continue
					}
				}

				durationSecs := int64(gapEnd.Sub(gapStart).Seconds())
				incidents = append(incidents, linkIncidentToIncident(lm, "no_data", gapStart, &gapEnd, false,
					"incident", nil, nil, nil, nil, &durationSecs))
			}
		}
	}

	return incidents, nil
}

// fetchDrainedPeriods gets all periods when links were drained within the time range.
func fetchDrainedPeriods(ctx context.Context, conn driver.Conn, duration time.Duration) (map[string][]drainedPeriod, error) {
	query := `
		SELECT link_pk, previous_status, new_status, changed_ts
		FROM dz_link_status_changes
		WHERE changed_ts >= now() - INTERVAL $1 SECOND
		ORDER BY link_pk, changed_ts
	`

	rows, err := conn.Query(ctx, query, int64(duration.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	type statusChange struct {
		LinkPK, PrevStatus, NewStatus string
		ChangedTS                     time.Time
	}

	byLink := make(map[string][]statusChange)
	for rows.Next() {
		var sc statusChange
		if err := rows.Scan(&sc.LinkPK, &sc.PrevStatus, &sc.NewStatus, &sc.ChangedTS); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		byLink[sc.LinkPK] = append(byLink[sc.LinkPK], sc)
	}

	result := make(map[string][]drainedPeriod)
	for linkPK, changes := range byLink {
		var periods []drainedPeriod
		var active *drainedPeriod

		for _, c := range changes {
			isDrained := c.NewStatus == "soft-drained" || c.NewStatus == "hard-drained"
			isRecovery := c.NewStatus == "activated" && (c.PrevStatus == "soft-drained" || c.PrevStatus == "hard-drained")

			if isDrained && active == nil {
				active = &drainedPeriod{Start: c.ChangedTS}
			} else if isRecovery && active != nil {
				t := c.ChangedTS
				active.End = &t
				periods = append(periods, *active)
				active = nil
			}
		}
		if active != nil {
			periods = append(periods, *active)
		}
		if len(periods) > 0 {
			result[linkPK] = periods
		}
	}

	return result, nil
}

func gapOverlapsDrainedPeriod(gapStart, gapEnd time.Time, periods []drainedPeriod) bool {
	for _, p := range periods {
		periodEnd := time.Now().Add(time.Hour)
		if p.End != nil {
			periodEnd = *p.End
		}
		if gapStart.Before(periodEnd) && gapEnd.After(p.Start) {
			return true
		}
	}
	return false
}

// detectDeviceCounterIncidents detects counter-based incidents on devices.
func detectDeviceCounterIncidents(ctx context.Context, conn driver.Conn, duration time.Duration, threshold int64, metricSQL, incidentType string, meta map[string]deviceMeta, params DetectionParams) ([]Incident, error) {
	devicePKs := devicePKList(meta)
	if len(devicePKs) == 0 {
		return nil, nil
	}

	// Current high-counter devices
	ongoingIncidents, ongoingDevices, err := fetchCurrentHighCounterDevices(ctx, conn, threshold, metricSQL, incidentType, meta, params)
	if err != nil {
		return nil, err
	}

	// Historical buckets
	query := fmt.Sprintf(`
		SELECT
			ic.device_pk,
			toStartOfInterval(ic.event_ts, INTERVAL 5 MINUTE) as bucket,
			%s as metric_value
		FROM fact_dz_device_interface_counters ic
		WHERE ic.event_ts >= now() - INTERVAL $1 SECOND
		  AND ic.device_pk IN ($2)
		GROUP BY ic.device_pk, bucket
		ORDER BY ic.device_pk, bucket
	`, metricSQL)

	lookbackSecs := int64((duration + 24*time.Hour).Seconds())
	rows, err := conn.Query(ctx, query, lookbackSecs, devicePKs)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var buckets []counterBucket
	for rows.Next() {
		var cb counterBucket
		if err := rows.Scan(&cb.EntityPK, &cb.Bucket, &cb.Value); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		buckets = append(buckets, cb)
	}

	completed := pairDeviceCounterCompleted(buckets, meta, threshold, incidentType, ongoingDevices, params)
	all := append(ongoingIncidents, completed...)
	return coalesceIncidents(all, params.CoalesceGap), nil
}

// fetchCurrentHighCounterDevices finds devices currently experiencing counter metrics above threshold.
func fetchCurrentHighCounterDevices(ctx context.Context, conn driver.Conn, threshold int64, metricSQL, incidentType string, meta map[string]deviceMeta, params DetectionParams) ([]Incident, map[string]bool, error) {
	devicePKs := devicePKList(meta)
	if len(devicePKs) == 0 {
		return nil, nil, nil
	}

	query := fmt.Sprintf(`
		WITH recent_buckets AS (
			SELECT
				ic.device_pk,
				toStartOfInterval(ic.event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters ic
			WHERE ic.event_ts >= now() - INTERVAL 15 MINUTE
			  AND ic.device_pk IN ($1)
			GROUP BY ic.device_pk, bucket
		),
		ranked AS (
			SELECT device_pk, bucket, metric_value,
				ROW_NUMBER() OVER (PARTITION BY device_pk ORDER BY bucket DESC) AS rn
			FROM recent_buckets
		)
		SELECT device_pk, bucket, metric_value, rn
		FROM ranked
		WHERE rn <= 3
		ORDER BY device_pk, rn
	`, metricSQL)

	rows, err := conn.Query(ctx, query, devicePKs)
	if err != nil {
		return nil, nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	type recentBucket struct {
		Bucket time.Time
		Value  int64
		Rank   uint64
	}
	devBuckets := make(map[string][]recentBucket)

	for rows.Next() {
		var devicePK string
		var bucket time.Time
		var value int64
		var rn uint64
		if err := rows.Scan(&devicePK, &bucket, &value, &rn); err != nil {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}
		devBuckets[devicePK] = append(devBuckets[devicePK], recentBucket{bucket, value, rn})
	}

	var incidents []Incident
	ongoingDevices := make(map[string]bool)

	for devicePK, buckets := range devBuckets {
		dm, ok := meta[devicePK]
		if !ok {
			continue
		}

		consecutiveAbove := 0
		for _, b := range buckets {
			if b.Value >= threshold {
				consecutiveAbove++
			} else {
				break
			}
		}
		if consecutiveAbove < params.MinBuckets() {
			continue
		}

		startedAt, peakValue, err := findDeviceCounterStart(ctx, conn, devicePK, threshold, metricSQL)
		if err != nil {
			startedAt = buckets[0].Bucket.Add(-10 * time.Minute)
			peakValue = buckets[0].Value
		}

		thresholdCount := threshold
		incidents = append(incidents, deviceIncidentToIncident(dm, incidentType, startedAt, nil, true,
			incidentSeverity(incidentType, 0), &thresholdCount, &peakValue, nil))

		ongoingDevices[dm.DeviceCode] = true
	}

	return incidents, ongoingDevices, nil
}

// findDeviceCounterStart looks back to find when a device counter incident started.
func findDeviceCounterStart(ctx context.Context, conn driver.Conn, devicePK string, threshold int64, metricSQL string) (time.Time, int64, error) {
	query := fmt.Sprintf(`
		WITH buckets AS (
			SELECT
				toStartOfInterval(event_ts, INTERVAL 5 MINUTE) as bucket,
				%s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE device_pk = $1
			  AND event_ts >= now() - INTERVAL 7 DAY
			GROUP BY bucket
			ORDER BY bucket DESC
		),
		above AS (
			SELECT bucket, metric_value,
				lagInFrame(bucket, 1) OVER (ORDER BY bucket ASC) as prev_bucket
			FROM buckets
			WHERE metric_value >= $2
		)
		SELECT bucket, metric_value
		FROM above
		WHERE prev_bucket IS NULL OR dateDiff('minute', prev_bucket, bucket) > 15
		ORDER BY bucket DESC
		LIMIT 1
	`, metricSQL)

	var startBucket time.Time
	var value int64
	if err := conn.QueryRow(ctx, query, devicePK, threshold).Scan(&startBucket, &value); err != nil {
		return time.Time{}, 0, err
	}

	peakQuery := fmt.Sprintf(`
		SELECT max(metric_value) FROM (
			SELECT %s as metric_value
			FROM fact_dz_device_interface_counters
			WHERE device_pk = $1
			  AND event_ts >= $2
			GROUP BY toStartOfInterval(event_ts, INTERVAL 5 MINUTE)
		)
	`, metricSQL)

	var peak int64
	if err := conn.QueryRow(ctx, peakQuery, devicePK, startBucket).Scan(&peak); err == nil && peak > value {
		value = peak
	}

	return startBucket, value, nil
}

// pairDeviceCounterCompleted finds completed counter incidents on devices.
func pairDeviceCounterCompleted(buckets []counterBucket, meta map[string]deviceMeta, threshold int64, incidentType string, excludeDevices map[string]bool, params DetectionParams) []Incident {
	var incidents []Incident

	byEntity := make(map[string][]counterBucket)
	for _, b := range buckets {
		byEntity[b.EntityPK] = append(byEntity[b.EntityPK], b)
	}

	for entityPK, entityBuckets := range byEntity {
		dm, ok := meta[entityPK]
		if !ok || excludeDevices[dm.DeviceCode] {
			continue
		}

		sort.Slice(entityBuckets, func(i, j int) bool {
			return entityBuckets[i].Bucket.Before(entityBuckets[j].Bucket)
		})

		var activeStart *time.Time
		var peakValue int64
		var consecutiveBuckets int

		for i, b := range entityBuckets {
			aboveThreshold := b.Value >= threshold

			if i == 0 {
				if aboveThreshold {
					t := b.Bucket
					activeStart = &t
					peakValue = b.Value
					consecutiveBuckets = 1
				}
				continue
			}

			prevAbove := entityBuckets[i-1].Value >= threshold

			if aboveThreshold && !prevAbove {
				t := b.Bucket
				activeStart = &t
				peakValue = b.Value
				consecutiveBuckets = 1
			} else if !aboveThreshold && prevAbove && activeStart != nil {
				if consecutiveBuckets >= params.MinBuckets() {
					endTime := entityBuckets[i-1].Bucket.Add(5 * time.Minute)
					durationSecs := int64(endTime.Sub(*activeStart).Seconds())
					thresholdCount := threshold
					incidents = append(incidents, deviceIncidentToIncident(dm, incidentType, *activeStart, &endTime, false,
						incidentSeverity(incidentType, 0), &thresholdCount, &peakValue, &durationSecs))
				}
				activeStart = nil
				peakValue = 0
				consecutiveBuckets = 0
			} else if aboveThreshold && activeStart != nil {
				consecutiveBuckets++
				if b.Value > peakValue {
					peakValue = b.Value
				}
			}
		}

		if activeStart != nil && consecutiveBuckets >= params.MinBuckets() {
			lastBucket := entityBuckets[len(entityBuckets)-1]
			endTime := lastBucket.Bucket.Add(5 * time.Minute)
			durationSecs := int64(endTime.Sub(*activeStart).Seconds())
			thresholdCount := threshold
			incidents = append(incidents, deviceIncidentToIncident(dm, incidentType, *activeStart, &endTime, false,
				incidentSeverity(incidentType, 0), &thresholdCount, &peakValue, &durationSecs))
		}
	}

	return incidents
}

// detectDeviceNoDataIncidents detects no-data incidents on devices.
func detectDeviceNoDataIncidents(ctx context.Context, conn driver.Conn, _ time.Duration, meta map[string]deviceMeta) ([]Incident, error) {
	devicePKs := devicePKList(meta)
	if len(devicePKs) == 0 {
		return nil, nil
	}

	// Current no-data devices — use link latency with UNION of origin/target device
	query := `
		WITH device_last_seen AS (
			SELECT device_pk, max(last_seen) as last_seen FROM (
				SELECT origin_device_pk as device_pk, max(event_ts) as last_seen
				FROM fact_dz_device_link_latency
				WHERE event_ts >= now() - INTERVAL 30 DAY
				  AND origin_device_pk IN ($1)
				GROUP BY origin_device_pk
				UNION ALL
				SELECT target_device_pk as device_pk, max(event_ts) as last_seen
				FROM fact_dz_device_link_latency
				WHERE event_ts >= now() - INTERVAL 30 DAY
				  AND target_device_pk IN ($1)
				GROUP BY target_device_pk
			)
			GROUP BY device_pk
		)
		SELECT dls.device_pk, dls.last_seen
		FROM device_last_seen dls
		WHERE dls.last_seen < now() - INTERVAL 15 MINUTE
		  AND dls.last_seen >= now() - INTERVAL 30 DAY
	`

	rows, err := conn.Query(ctx, query, devicePKs)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var incidents []Incident
	for rows.Next() {
		var devicePK string
		var lastSeen time.Time
		if err := rows.Scan(&devicePK, &lastSeen); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		dm, ok := meta[devicePK]
		if !ok {
			continue
		}

		startedAt := lastSeen.Add(5 * time.Minute)
		incidents = append(incidents, deviceIncidentToIncident(dm, "no_data", startedAt, nil, true,
			"incident", nil, nil, nil))
	}

	return incidents, nil
}

// enrichLinkIncidentsWithInterfaces populates AffectedInterfaces for counter incidents.
func enrichLinkIncidentsWithInterfaces(ctx context.Context, conn driver.Conn, incidents []Incident) {
	type key struct {
		entityPK, incidentType string
	}
	type window struct {
		start, end time.Time
	}

	windows := make(map[key]window)
	for _, inc := range incidents {
		if inc.IncidentType == "packet_loss" || inc.IncidentType == "no_data" || inc.EntityType != "link" {
			continue
		}
		end := time.Now()
		if inc.EndedAt != nil {
			end = *inc.EndedAt
		}
		k := key{inc.EntityPK, inc.IncidentType}
		if w, ok := windows[k]; ok {
			if inc.StartedAt.Before(w.start) {
				w.start = inc.StartedAt
			}
			if end.After(w.end) {
				w.end = end
			}
			windows[k] = w
		} else {
			windows[k] = window{inc.StartedAt, end}
		}
	}

	if len(windows) == 0 {
		return
	}

	interfacesByKey := make(map[key][]string)
	byType := make(map[string][]struct {
		entityPK   string
		start, end time.Time
	})

	for k, w := range windows {
		byType[k.incidentType] = append(byType[k.incidentType], struct {
			entityPK   string
			start, end time.Time
		}{k.entityPK, w.start, w.end})
	}

	for incType, typeWindows := range byType {
		filter := counterTypeFilter(incType)
		var entityPKs []string
		earliest := typeWindows[0].start
		latest := typeWindows[0].end
		for _, w := range typeWindows {
			entityPKs = append(entityPKs, w.entityPK)
			if w.start.Before(earliest) {
				earliest = w.start
			}
			if w.end.After(latest) {
				latest = w.end
			}
		}

		q := fmt.Sprintf(`
			SELECT link_pk, intf
			FROM fact_dz_device_interface_counters
			WHERE link_pk IN ($1)
			  AND event_ts >= $2
			  AND event_ts <= $3
			  AND %s
			GROUP BY link_pk, intf
			ORDER BY link_pk, intf
		`, filter)

		rows, err := conn.Query(ctx, q, entityPKs, earliest, latest)
		if err != nil {
			continue
		}
		for rows.Next() {
			var pk, intf string
			if err := rows.Scan(&pk, &intf); err != nil {
				continue
			}
			k := key{pk, incType}
			interfacesByKey[k] = append(interfacesByKey[k], intf)
		}
		rows.Close()
	}

	for i := range incidents {
		inc := &incidents[i]
		if inc.IncidentType == "packet_loss" || inc.IncidentType == "no_data" || inc.EntityType != "link" {
			continue
		}
		k := key{inc.EntityPK, inc.IncidentType}
		if intfs, ok := interfacesByKey[k]; ok {
			inc.AffectedInterfaces = intfs
		}
	}
}

// enrichDeviceIncidentsWithInterfaces populates AffectedInterfaces for device counter incidents.
func enrichDeviceIncidentsWithInterfaces(ctx context.Context, conn driver.Conn, incidents []Incident) {
	type key struct {
		entityPK, incidentType string
	}
	type window struct {
		start, end time.Time
	}

	windows := make(map[key]window)
	for _, inc := range incidents {
		if inc.IncidentType == "no_data" || inc.EntityType != "device" {
			continue
		}
		end := time.Now()
		if inc.EndedAt != nil {
			end = *inc.EndedAt
		}
		k := key{inc.EntityPK, inc.IncidentType}
		if w, ok := windows[k]; ok {
			if inc.StartedAt.Before(w.start) {
				w.start = inc.StartedAt
			}
			if end.After(w.end) {
				w.end = end
			}
			windows[k] = w
		} else {
			windows[k] = window{inc.StartedAt, end}
		}
	}

	if len(windows) == 0 {
		return
	}

	interfacesByKey := make(map[key][]string)
	byType := make(map[string][]struct {
		entityPK   string
		start, end time.Time
	})

	for k, w := range windows {
		byType[k.incidentType] = append(byType[k.incidentType], struct {
			entityPK   string
			start, end time.Time
		}{k.entityPK, w.start, w.end})
	}

	for incType, typeWindows := range byType {
		filter := counterTypeFilter(incType)
		var entityPKs []string
		earliest := typeWindows[0].start
		latest := typeWindows[0].end
		for _, w := range typeWindows {
			entityPKs = append(entityPKs, w.entityPK)
			if w.start.Before(earliest) {
				earliest = w.start
			}
			if w.end.After(latest) {
				latest = w.end
			}
		}

		q := fmt.Sprintf(`
			SELECT device_pk, intf
			FROM fact_dz_device_interface_counters
			WHERE device_pk IN ($1)
			  AND event_ts >= $2
			  AND event_ts <= $3
			  AND %s
			GROUP BY device_pk, intf
			ORDER BY device_pk, intf
		`, filter)

		rows, err := conn.Query(ctx, q, entityPKs, earliest, latest)
		if err != nil {
			continue
		}
		for rows.Next() {
			var pk, intf string
			if err := rows.Scan(&pk, &intf); err != nil {
				continue
			}
			k := key{pk, incType}
			interfacesByKey[k] = append(interfacesByKey[k], intf)
		}
		rows.Close()
	}

	for i := range incidents {
		inc := &incidents[i]
		if inc.IncidentType == "no_data" || inc.EntityType != "device" {
			continue
		}
		k := key{inc.EntityPK, inc.IncidentType}
		if intfs, ok := interfacesByKey[k]; ok {
			inc.AffectedInterfaces = intfs
		}
	}
}

func linkPKList(meta map[string]linkMeta) []string {
	pks := make([]string, 0, len(meta))
	for pk := range meta {
		pks = append(pks, pk)
	}
	return pks
}

func devicePKList(meta map[string]deviceMeta) []string {
	pks := make([]string, 0, len(meta))
	for pk := range meta {
		pks = append(pks, pk)
	}
	return pks
}
