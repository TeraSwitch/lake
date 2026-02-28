package handlers

import (
	"context"

	"github.com/malbeclabs/lake/agent/pkg/workflow"
	"github.com/malbeclabs/lake/api/config"
	"github.com/malbeclabs/lake/internal/querier"
)

// Neo4jQuerier implements workflow.Querier for Neo4j graph queries.
type Neo4jQuerier struct {
	inner *querier.Neo4jQuerier
}

// NewNeo4jQuerier creates a new Neo4jQuerier.
func NewNeo4jQuerier() *Neo4jQuerier {
	return &Neo4jQuerier{inner: querier.NewNeo4jQuerier(config.Neo4jClient)}
}

// Query executes a Cypher query and returns formatted results.
func (q *Neo4jQuerier) Query(ctx context.Context, cypher string) (workflow.QueryResult, error) {
	return q.inner.Query(ctx, cypher)
}

// Neo4jSchemaFetcher implements workflow.SchemaFetcher for Neo4j.
type Neo4jSchemaFetcher struct {
	inner *querier.Neo4jSchemaFetcher
}

// NewNeo4jSchemaFetcher creates a new Neo4jSchemaFetcher.
func NewNeo4jSchemaFetcher() *Neo4jSchemaFetcher {
	return &Neo4jSchemaFetcher{inner: querier.NewNeo4jSchemaFetcher(config.Neo4jClient)}
}

// FetchSchema returns a formatted string describing the Neo4j graph schema.
func (f *Neo4jSchemaFetcher) FetchSchema(ctx context.Context) (string, error) {
	return f.inner.FetchSchema(ctx)
}

// convertNeo4jValue converts Neo4j types to standard Go types.
func convertNeo4jValue(val any) any {
	return querier.ConvertNeo4jValue(val)
}
