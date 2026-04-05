package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/malbeclabs/lake/api/notifier"
)

const (
	SourceTypeUserActivity = "user_activity"

	maxUserEventsPerPoll = 200
)

// UserActivitySource polls ClickHouse for user connection and disconnection
// events in the serviceability dimension table.
type UserActivitySource struct {
	DB       driver.Conn
	Database string
}

func (s *UserActivitySource) Type() string {
	return SourceTypeUserActivity
}

type userEventRow struct {
	PK          string
	OwnerPubkey string
	Status      string
	Kind        string
	ClientIP    string
	DZIP        string
	DevicePK    string
	TenantPK    string
	EventType   string // "connected" or "disconnected"
	EventTS     time.Time
}

// userActivityFilters is the JSON schema for user activity filters.
type userActivityFilters struct {
	ExcludeOwners []string `json:"exclude_owners"`
}

func (s *UserActivitySource) Poll(ctx context.Context, cp notifier.Checkpoint) ([]notifier.EventGroup, notifier.Checkpoint, error) {
	// Detect new connections: users whose earliest snapshot is after checkpoint.
	// Detect disconnections: users whose latest is_deleted=1 snapshot is after checkpoint.
	query := fmt.Sprintf(`
		SELECT pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk, event_type, event_ts
		FROM (
			SELECT
				pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk,
				'connected' AS event_type,
				min(snapshot_ts) AS event_ts
			FROM %[1]s.dim_dz_users_history
			WHERE is_deleted = 0
			GROUP BY pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk
			HAVING event_ts > $1

			UNION ALL

			SELECT
				pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk,
				'disconnected' AS event_type,
				max(snapshot_ts) AS event_ts
			FROM %[1]s.dim_dz_users_history
			WHERE is_deleted = 1
			GROUP BY pk, owner_pubkey, status, kind, client_ip, dz_ip, device_pk, tenant_pk
			HAVING event_ts > $1
		)
		ORDER BY event_ts ASC
		LIMIT %d`, s.Database, maxUserEventsPerPoll)

	rows, err := s.DB.Query(ctx, query, cp.LastEventTS)
	if err != nil {
		return nil, cp, fmt.Errorf("user activity query: %w", err)
	}
	defer rows.Close()

	var events []userEventRow
	for rows.Next() {
		var e userEventRow
		if err := rows.Scan(&e.PK, &e.OwnerPubkey, &e.Status, &e.Kind,
			&e.ClientIP, &e.DZIP, &e.DevicePK, &e.TenantPK, &e.EventType, &e.EventTS); err != nil {
			return nil, cp, fmt.Errorf("user activity scan: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, cp, fmt.Errorf("user activity rows: %w", err)
	}

	if len(events) == 0 {
		return nil, cp, nil
	}

	groups := buildUserActivityGroups(events)

	last := events[len(events)-1]
	newCP := notifier.Checkpoint{
		LastEventTS: last.EventTS,
	}

	return groups, newCP, nil
}

// Filter excludes event groups where the user's owner_pubkey is in the exclude list.
func (s *UserActivitySource) Filter(groups []notifier.EventGroup, filtersRaw json.RawMessage) []notifier.EventGroup {
	if len(filtersRaw) == 0 || string(filtersRaw) == "{}" {
		return groups
	}

	var f userActivityFilters
	if err := json.Unmarshal(filtersRaw, &f); err != nil || len(f.ExcludeOwners) == 0 {
		return groups
	}

	excluded := make(map[string]bool, len(f.ExcludeOwners))
	for _, o := range f.ExcludeOwners {
		excluded[o] = true
	}

	var filtered []notifier.EventGroup
	for _, g := range groups {
		exclude := false
		for _, e := range g.Events {
			if owner, ok := e.Details["owner_pubkey"].(string); ok && excluded[owner] {
				exclude = true
				break
			}
		}
		if !exclude {
			filtered = append(filtered, g)
		}
	}
	return filtered
}

func buildUserActivityGroups(events []userEventRow) []notifier.EventGroup {
	groups := make([]notifier.EventGroup, 0, len(events))

	for _, e := range events {
		details := map[string]any{
			"pk":           e.PK,
			"owner_pubkey": e.OwnerPubkey,
			"status":       e.Status,
			"kind":         e.Kind,
			"client_ip":    e.ClientIP,
			"dz_ip":        e.DZIP,
			"device_pk":    e.DevicePK,
			"tenant_pk":    e.TenantPK,
			"event_ts":     e.EventTS,
		}

		var summary string
		kindLabel := "User"
		if e.Kind != "" {
			kindLabel = e.Kind + " User"
		}

		switch e.EventType {
		case "connected":
			summary = kindLabel + " Connected"
		case "disconnected":
			summary = kindLabel + " Disconnected"
		default:
			summary = kindLabel + " Activity"
		}

		groups = append(groups, notifier.EventGroup{
			Key:     e.PK,
			Summary: summary,
			Events: []notifier.Event{{
				Type:    e.EventType,
				Details: details,
			}},
		})
	}

	return groups
}
