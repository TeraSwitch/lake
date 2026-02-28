package querier

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	driver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/malbeclabs/lake/api/metrics"
)

// ClickHouseQuerier implements workflow.Querier using an injected connection.
type ClickHouseQuerier struct {
	conn driver.Conn
}

// NewClickHouseQuerier creates a new ClickHouseQuerier with the given connection.
func NewClickHouseQuerier(conn driver.Conn) *ClickHouseQuerier {
	return &ClickHouseQuerier{conn: conn}
}

// Query executes a SQL query and returns the result.
func (q *ClickHouseQuerier) Query(ctx context.Context, sql string) (workflow.QueryResult, error) {
	sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")

	start := time.Now()
	rows, err := q.conn.Query(ctx, sql)
	duration := time.Since(start)
	if err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return workflow.QueryResult{SQL: sql, Error: err.Error()}, nil
	}
	defer rows.Close()

	columnTypes := rows.ColumnTypes()
	columns := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ct.Name()
	}

	var resultRows []map[string]any
	for rows.Next() {
		values := make([]any, len(columnTypes))
		for i, ct := range columnTypes {
			values[i] = reflect.New(ct.ScanType()).Interface()
		}

		if err := rows.Scan(values...); err != nil {
			metrics.RecordClickHouseQuery(duration, err)
			return workflow.QueryResult{SQL: sql, Error: fmt.Sprintf("scan error: %v", err)}, nil
		}

		row := make(map[string]any)
		for i, col := range columns {
			row[col] = reflect.ValueOf(values[i]).Elem().Interface()
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		metrics.RecordClickHouseQuery(duration, err)
		return workflow.QueryResult{SQL: sql, Error: err.Error()}, nil
	}

	metrics.RecordClickHouseQuery(duration, nil)
	workflow.SanitizeRows(resultRows)

	result := workflow.QueryResult{
		SQL:     sql,
		Columns: columns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}
	result.Formatted = FormatQueryResult(result)

	return result, nil
}

// FormatQueryResult creates a human-readable format of the query result.
func FormatQueryResult(result workflow.QueryResult) string {
	if result.Error != "" {
		return fmt.Sprintf("Error: %s", result.Error)
	}

	if len(result.Rows) == 0 {
		return "Query returned no results."
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Results (%d rows):\n", len(result.Rows)))
	sb.WriteString("Columns: " + strings.Join(result.Columns, " | ") + "\n")
	sb.WriteString(strings.Repeat("-", 40) + "\n")

	maxRows := min(50, len(result.Rows))
	for i := range maxRows {
		row := result.Rows[i]
		var values []string
		for _, col := range result.Columns {
			values = append(values, workflow.FormatValue(row[col]))
		}
		sb.WriteString(strings.Join(values, " | ") + "\n")
	}

	if len(result.Rows) > 50 {
		sb.WriteString(fmt.Sprintf("... and %d more rows\n", len(result.Rows)-50))
	}

	return sb.String()
}
