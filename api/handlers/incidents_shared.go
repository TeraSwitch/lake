package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DrainedSummary contains aggregate counts for drained entities.
type DrainedSummary struct {
	Total         int `json:"total"`
	WithIncidents int `json:"with_incidents"`
	Ready         int `json:"ready"`
	NotReady      int `json:"not_ready"`
}

type linkMetadata struct {
	LinkPK          string
	LinkCode        string
	LinkType        string
	SideAMetro      string
	SideZMetro      string
	ContributorCode string
}

type linkMetadataWithStatus struct {
	linkMetadata
	Status string
}

func fetchLinkMetadataWithStatus(ctx context.Context, conn driver.Conn, filters []IncidentFilter) (map[string]linkMetadataWithStatus, error) {
	var qb strings.Builder
	qb.WriteString(`
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
		WHERE l.committed_rtt_ns != $1
	`)

	var args []any
	args = append(args, committedRttProvisioningNs)
	argIdx := 2

	for _, f := range filters {
		switch f.Type {
		case "metro":
			fmt.Fprintf(&qb, " AND (ma.code = $%d OR mz.code = $%d)", argIdx, argIdx)
			args = append(args, f.Value)
			argIdx++
		case "link":
			fmt.Fprintf(&qb, " AND l.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "contributor":
			fmt.Fprintf(&qb, " AND c.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "device":
			fmt.Fprintf(&qb, " AND (da.code = $%d OR dz.code = $%d)", argIdx, argIdx)
			args = append(args, f.Value)
			argIdx++
		}
	}

	rows, err := conn.Query(ctx, qb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]linkMetadataWithStatus)
	for rows.Next() {
		var lm linkMetadataWithStatus
		if err := rows.Scan(&lm.LinkPK, &lm.LinkCode, &lm.LinkType, &lm.SideAMetro, &lm.SideZMetro, &lm.ContributorCode, &lm.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[lm.LinkPK] = lm
	}

	return result, nil
}

func drainedSinceStr(drainedSince map[string]time.Time, pk string) string {
	if t, ok := drainedSince[pk]; ok {
		return t.UTC().Format(time.RFC3339)
	}
	return ""
}

// fetchDrainedSince finds when each drained link entered its current drain state.
func fetchDrainedSince(ctx context.Context, conn driver.Conn, linkMeta map[string]linkMetadataWithStatus) map[string]time.Time {
	drainedPKs := make([]string, 0)
	for pk, meta := range linkMeta {
		if meta.Status == "soft-drained" || meta.Status == "hard-drained" {
			drainedPKs = append(drainedPKs, pk)
		}
	}
	if len(drainedPKs) == 0 {
		return nil
	}

	query := `
		SELECT sc.link_pk, max(sc.changed_ts) as drained_at
		FROM dz_link_status_changes sc
		WHERE sc.link_pk IN ($1)
		  AND sc.new_status IN ('soft-drained', 'hard-drained')
		GROUP BY sc.link_pk
	`

	rows, err := conn.Query(ctx, query, drainedPKs)
	if err != nil {
		return nil
	}
	defer rows.Close()

	result := make(map[string]time.Time)
	for rows.Next() {
		var linkPK string
		var drainedAt time.Time
		if err := rows.Scan(&linkPK, &drainedAt); err != nil {
			continue
		}
		result[linkPK] = drainedAt
	}
	return result
}

// deviceMetadata contains device info for enriching incidents.
type deviceMetadata struct {
	DevicePK        string
	DeviceCode      string
	DeviceType      string
	Metro           string
	ContributorCode string
	Status          string
}

func isDeviceDrained(status string) bool {
	return status == "soft-drained" || status == "hard-drained" || status == "suspended"
}

func fetchDeviceMetadata(ctx context.Context, conn driver.Conn, filters []IncidentFilter) (map[string]deviceMetadata, error) {
	var qb strings.Builder
	qb.WriteString(`
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
		WHERE 1=1
	`)

	var args []any
	argIdx := 1

	for _, f := range filters {
		switch f.Type {
		case "metro":
			fmt.Fprintf(&qb, " AND m.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "device":
			fmt.Fprintf(&qb, " AND d.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		case "contributor":
			fmt.Fprintf(&qb, " AND c.code = $%d", argIdx)
			args = append(args, f.Value)
			argIdx++
		}
	}

	rows, err := conn.Query(ctx, qb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]deviceMetadata)
	for rows.Next() {
		var dm deviceMetadata
		if err := rows.Scan(&dm.DevicePK, &dm.DeviceCode, &dm.DeviceType, &dm.Metro, &dm.ContributorCode, &dm.Status); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		result[dm.DevicePK] = dm
	}

	return result, nil
}
