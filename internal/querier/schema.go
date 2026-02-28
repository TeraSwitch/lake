package querier

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	driver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/malbeclabs/lake/api/metrics"
)

// Cached schema to avoid querying ClickHouse system tables on every agent request.
var (
	schemaCache    string
	schemaCacheAt  time.Time
	schemaCacheMu  sync.RWMutex
	schemaCacheTTL = 60 * time.Second
)

// ClickHouseSchemaFetcher implements workflow.SchemaFetcher using an injected connection.
type ClickHouseSchemaFetcher struct {
	conn     driver.Conn
	database string
}

// NewClickHouseSchemaFetcher creates a new ClickHouseSchemaFetcher with the given connection and database name.
func NewClickHouseSchemaFetcher(conn driver.Conn, database string) *ClickHouseSchemaFetcher {
	return &ClickHouseSchemaFetcher{conn: conn, database: database}
}

// FetchSchema retrieves table columns and view definitions from ClickHouse.
// Results are cached for 60 seconds to avoid redundant queries under load.
func (f *ClickHouseSchemaFetcher) FetchSchema(ctx context.Context) (string, error) {
	// Check cache first
	schemaCacheMu.RLock()
	if schemaCache != "" && time.Since(schemaCacheAt) < schemaCacheTTL {
		cached := schemaCache
		schemaCacheMu.RUnlock()
		return cached, nil
	}
	schemaCacheMu.RUnlock()

	// Fetch columns
	start := time.Now()
	rows, err := f.conn.Query(ctx, `
		SELECT
			table,
			name,
			type
		FROM system.columns
		WHERE database = $1
		  AND table NOT LIKE 'stg_%'
		  AND table != '_env_lock'
		ORDER BY table, position
	`, f.database)
	duration := time.Since(start)
	if err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return "", fmt.Errorf("failed to fetch columns: %w", err)
	}
	defer rows.Close()
	metrics.RecordClickHouseQuery(duration, nil)

	type columnInfo struct {
		Table string
		Name  string
		Type  string
	}
	var columns []columnInfo
	for rows.Next() {
		var c columnInfo
		if err := rows.Scan(&c.Table, &c.Name, &c.Type); err != nil {
			return "", err
		}
		columns = append(columns, c)
	}

	// Fetch view definitions
	start = time.Now()
	viewRows, err := f.conn.Query(ctx, `
		SELECT
			name,
			as_select
		FROM system.tables
		WHERE database = $1
		  AND engine = 'View'
		  AND name NOT LIKE 'stg_%'
		  AND name != '_env_lock'
	`, f.database)
	duration = time.Since(start)
	if err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return "", fmt.Errorf("failed to fetch views: %w", err)
	}
	defer viewRows.Close()
	metrics.RecordClickHouseQuery(duration, nil)

	viewDefs := make(map[string]string)
	for viewRows.Next() {
		var name, asSelect string
		if err := viewRows.Scan(&name, &asSelect); err != nil {
			return "", err
		}
		viewDefs[name] = asSelect
	}

	// Format schema as readable text
	var sb strings.Builder
	currentTable := ""
	for _, col := range columns {
		if col.Table != currentTable {
			if currentTable != "" {
				if def, ok := viewDefs[currentTable]; ok {
					sb.WriteString("  Definition: " + def + "\n")
				}
				sb.WriteString("\n")
			}
			currentTable = col.Table
			if _, isView := viewDefs[col.Table]; isView {
				sb.WriteString(col.Table + " (VIEW):\n")
			} else {
				sb.WriteString(col.Table + ":\n")
			}
		}
		sb.WriteString("  - " + col.Name + " (" + col.Type + ")\n")
	}
	if def, ok := viewDefs[currentTable]; ok {
		sb.WriteString("  Definition: " + def + "\n")
	}

	result := sb.String()

	// Update cache
	schemaCacheMu.Lock()
	schemaCache = result
	schemaCacheAt = time.Now()
	schemaCacheMu.Unlock()

	return result, nil
}
