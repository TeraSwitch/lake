package handlers

import (
	"context"

	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/internal/querier"
)

// DBQuerier implements workflow.Querier using the global connection pool.
type DBQuerier struct {
	inner *querier.ClickHouseQuerier
}

// NewDBQuerier creates a new DBQuerier.
func NewDBQuerier() *DBQuerier {
	return &DBQuerier{inner: querier.NewClickHouseQuerier(config.DB)}
}

// Query executes a SQL query and returns the result.
func (q *DBQuerier) Query(ctx context.Context, sql string) (workflow.QueryResult, error) {
	return q.inner.Query(ctx, sql)
}

// DBSchemaFetcher implements workflow.SchemaFetcher using the global connection pool.
type DBSchemaFetcher struct {
	inner *querier.ClickHouseSchemaFetcher
}

// NewDBSchemaFetcher creates a new DBSchemaFetcher.
func NewDBSchemaFetcher() *DBSchemaFetcher {
	return &DBSchemaFetcher{inner: querier.NewClickHouseSchemaFetcher(config.DB, config.Database())}
}

// FetchSchema retrieves table columns and view definitions from ClickHouse.
func (f *DBSchemaFetcher) FetchSchema(ctx context.Context) (string, error) {
	return f.inner.FetchSchema(ctx)
}
