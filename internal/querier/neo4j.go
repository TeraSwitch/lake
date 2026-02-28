package querier

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/malbeclabs/lake/agent/pkg/workflow"
	neo4jpkg "github.com/malbeclabs/lake/indexer/pkg/neo4j"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4jQuerier implements workflow.Querier using an injected Neo4j client.
type Neo4jQuerier struct {
	client neo4jpkg.Client
}

// NewNeo4jQuerier creates a new Neo4jQuerier with the given client.
func NewNeo4jQuerier(client neo4jpkg.Client) *Neo4jQuerier {
	return &Neo4jQuerier{client: client}
}

// Query executes a Cypher query and returns formatted results.
func (q *Neo4jQuerier) Query(ctx context.Context, cypher string) (workflow.QueryResult, error) {
	session, err := q.client.Session(ctx)
	if err != nil {
		return workflow.QueryResult{Cypher: cypher, Error: err.Error()}, nil
	}
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4jpkg.Transaction) (any, error) {
		res, err := tx.Run(ctx, cypher, nil)
		if err != nil {
			return nil, err
		}

		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}

		var columns []string
		if len(records) > 0 {
			columns = records[0].Keys
		}

		rows := make([]map[string]any, 0, len(records))
		for _, record := range records {
			row := make(map[string]any)
			for _, key := range record.Keys {
				val, _ := record.Get(key)
				row[key] = ConvertNeo4jValue(val)
			}
			rows = append(rows, row)
		}

		return workflow.QueryResult{
			Cypher:    cypher,
			Columns:   columns,
			Rows:      rows,
			Count:     len(rows),
			Formatted: FormatCypherResult(columns, rows),
		}, nil
	})

	if err != nil {
		return workflow.QueryResult{Cypher: cypher, Error: err.Error()}, nil
	}

	return result.(workflow.QueryResult), nil
}

// Neo4jSchemaFetcher implements workflow.SchemaFetcher for Neo4j using an injected client.
type Neo4jSchemaFetcher struct {
	client neo4jpkg.Client
}

// NewNeo4jSchemaFetcher creates a new Neo4jSchemaFetcher with the given client.
func NewNeo4jSchemaFetcher(client neo4jpkg.Client) *Neo4jSchemaFetcher {
	return &Neo4jSchemaFetcher{client: client}
}

// FetchSchema returns a formatted string describing the Neo4j graph schema.
func (f *Neo4jSchemaFetcher) FetchSchema(ctx context.Context) (string, error) {
	session, err := f.client.Session(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close(ctx)

	var sb strings.Builder
	sb.WriteString("## Graph Database Schema (Neo4j)\n\n")

	labels, err := getNodeLabels(ctx, session)
	if err != nil {
		return "", fmt.Errorf("failed to get node labels: %w", err)
	}

	if len(labels) > 0 {
		sb.WriteString("### Node Labels\n\n")
		for _, label := range labels {
			fmt.Fprintf(&sb, "**%s**\n", label.Name)
			if len(label.Properties) > 0 {
				sb.WriteString("Properties:\n")
				for _, prop := range label.Properties {
					fmt.Fprintf(&sb, "- `%s` (%s)\n", prop.Name, prop.Type)
				}
			}
			sb.WriteString("\n")
		}
	}

	relTypes, err := getRelationshipTypes(ctx, session)
	if err != nil {
		return "", fmt.Errorf("failed to get relationship types: %w", err)
	}

	if len(relTypes) > 0 {
		sb.WriteString("### Relationship Types\n\n")
		for _, rel := range relTypes {
			fmt.Fprintf(&sb, "- `%s`", rel.Name)
			if len(rel.Properties) > 0 {
				propNames := make([]string, len(rel.Properties))
				for i, p := range rel.Properties {
					propNames[i] = p.Name
				}
				fmt.Fprintf(&sb, " (properties: %s)", strings.Join(propNames, ", "))
			}
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}

	return sb.String(), nil
}

type labelInfo struct {
	Name       string
	Properties []propertyInfo
}

type propertyInfo struct {
	Name string
	Type string
}

type relTypeInfo struct {
	Name       string
	Properties []propertyInfo
}

func getNodeLabels(ctx context.Context, session neo4jpkg.Session) ([]labelInfo, error) {
	labelsResult, err := session.ExecuteRead(ctx, func(tx neo4jpkg.Transaction) (any, error) {
		res, err := tx.Run(ctx, "CALL db.labels()", nil)
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		labels := make([]string, 0, len(records))
		for _, record := range records {
			if label, ok := record.Values[0].(string); ok {
				labels = append(labels, label)
			}
		}
		return labels, nil
	})
	if err != nil {
		return nil, err
	}

	labels := labelsResult.([]string)

	propsResult, _ := session.ExecuteRead(ctx, func(tx neo4jpkg.Transaction) (any, error) {
		res, err := tx.Run(ctx, "CALL db.schema.nodeTypeProperties()", nil)
		if err != nil {
			return nil, nil
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, nil
		}

		propMap := make(map[string][]propertyInfo)
		for _, record := range records {
			nodeLabels, _ := record.Get("nodeLabels")
			propName, _ := record.Get("propertyName")
			propTypes, _ := record.Get("propertyTypes")

			if labelsArr, ok := nodeLabels.([]any); ok && len(labelsArr) > 0 {
				labelName := fmt.Sprintf("%v", labelsArr[0])
				propNameStr := fmt.Sprintf("%v", propName)
				propTypeStr := "any"
				if typesArr, ok := propTypes.([]any); ok && len(typesArr) > 0 {
					propTypeStr = fmt.Sprintf("%v", typesArr[0])
				}
				propMap[labelName] = append(propMap[labelName], propertyInfo{
					Name: propNameStr,
					Type: propTypeStr,
				})
			}
		}
		return propMap, nil
	})

	propMap := make(map[string][]propertyInfo)
	if propsResult != nil {
		propMap = propsResult.(map[string][]propertyInfo)
	}

	result := make([]labelInfo, 0, len(labels))
	for _, label := range labels {
		result = append(result, labelInfo{
			Name:       label,
			Properties: propMap[label],
		})
	}

	return result, nil
}

func getRelationshipTypes(ctx context.Context, session neo4jpkg.Session) ([]relTypeInfo, error) {
	typesResult, err := session.ExecuteRead(ctx, func(tx neo4jpkg.Transaction) (any, error) {
		res, err := tx.Run(ctx, "CALL db.relationshipTypes()", nil)
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		types := make([]string, 0, len(records))
		for _, record := range records {
			if relType, ok := record.Values[0].(string); ok {
				types = append(types, relType)
			}
		}
		return types, nil
	})
	if err != nil {
		return nil, err
	}

	relTypes := typesResult.([]string)

	propsResult, _ := session.ExecuteRead(ctx, func(tx neo4jpkg.Transaction) (any, error) {
		res, err := tx.Run(ctx, "CALL db.schema.relTypeProperties()", nil)
		if err != nil {
			return nil, nil
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, nil
		}

		propMap := make(map[string][]propertyInfo)
		for _, record := range records {
			relType, _ := record.Get("relType")
			propName, _ := record.Get("propertyName")
			propTypes, _ := record.Get("propertyTypes")

			relTypeStr := strings.TrimPrefix(fmt.Sprintf("%v", relType), ":`")
			relTypeStr = strings.TrimSuffix(relTypeStr, "`")
			propNameStr := fmt.Sprintf("%v", propName)
			propTypeStr := "any"
			if typesArr, ok := propTypes.([]any); ok && len(typesArr) > 0 {
				propTypeStr = fmt.Sprintf("%v", typesArr[0])
			}
			propMap[relTypeStr] = append(propMap[relTypeStr], propertyInfo{
				Name: propNameStr,
				Type: propTypeStr,
			})
		}
		return propMap, nil
	})

	propMap := make(map[string][]propertyInfo)
	if propsResult != nil {
		propMap = propsResult.(map[string][]propertyInfo)
	}

	result := make([]relTypeInfo, 0, len(relTypes))
	for _, relType := range relTypes {
		result = append(result, relTypeInfo{
			Name:       relType,
			Properties: propMap[relType],
		})
	}

	return result, nil
}

// ConvertNeo4jValue converts Neo4j types to standard Go types.
func ConvertNeo4jValue(val any) any {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case neo4jdriver.Node:
		props := make(map[string]any)
		for k, pv := range v.Props {
			props[k] = ConvertNeo4jValue(pv)
		}
		return map[string]any{
			"_labels":     v.Labels,
			"_properties": props,
		}
	case neo4jdriver.Relationship:
		props := make(map[string]any)
		for k, pv := range v.Props {
			props[k] = ConvertNeo4jValue(pv)
		}
		return map[string]any{
			"_type":       v.Type,
			"_properties": props,
		}
	case neo4jdriver.Path:
		nodes := make([]any, len(v.Nodes))
		for i, n := range v.Nodes {
			nodes[i] = ConvertNeo4jValue(n)
		}
		rels := make([]any, len(v.Relationships))
		for i, r := range v.Relationships {
			rels[i] = ConvertNeo4jValue(r)
		}
		return map[string]any{
			"_nodes":         nodes,
			"_relationships": rels,
		}
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = ConvertNeo4jValue(item)
		}
		return result
	case map[string]any:
		result := make(map[string]any)
		for k, mv := range v {
			result[k] = ConvertNeo4jValue(mv)
		}
		return result
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		return v
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil
		}
		return v
	default:
		return v
	}
}

// FormatCypherResult formats query results for display.
func FormatCypherResult(columns []string, rows []map[string]any) string {
	if len(rows) == 0 {
		return "(no results)"
	}

	var sb strings.Builder

	sb.WriteString("| ")
	for i, col := range columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(col)
	}
	sb.WriteString(" |\n")

	sb.WriteString("|")
	for range columns {
		sb.WriteString("---|")
	}
	sb.WriteString("\n")

	maxRows := 50
	for i, row := range rows {
		if i >= maxRows {
			fmt.Fprintf(&sb, "\n... and %d more rows", len(rows)-maxRows)
			break
		}
		sb.WriteString("| ")
		for j, col := range columns {
			if j > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(formatNeo4jValue(row[col]))
		}
		sb.WriteString(" |\n")
	}

	return sb.String()
}

func formatNeo4jValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		if len(val) > 50 {
			return val[:47] + "..."
		}
		return val
	case []any:
		if len(val) == 0 {
			return "[]"
		}
		parts := make([]string, 0, len(val))
		for _, item := range val {
			parts = append(parts, formatNeo4jValue(item))
		}
		result := "[" + strings.Join(parts, ", ") + "]"
		if len(result) > 80 {
			return result[:77] + "..."
		}
		return result
	case map[string]any:
		if labels, ok := val["_labels"]; ok {
			return formatNeo4jNode(val, labels)
		}
		if relType, ok := val["_type"]; ok {
			return formatNeo4jRelationship(val, relType)
		}
		if nodes, ok := val["_nodes"]; ok {
			if rels, ok := val["_relationships"]; ok {
				return formatNeo4jPath(nodes, rels)
			}
		}
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func formatNeo4jNode(val map[string]any, labels any) string {
	label := "Node"
	if labelsArr, ok := labels.([]any); ok && len(labelsArr) > 0 {
		label = fmt.Sprintf("%v", labelsArr[0])
	}
	identifier := getNodeIdentifier(val)
	if identifier != "" {
		return fmt.Sprintf("%s(%s)", label, identifier)
	}
	return fmt.Sprintf("%s(?)", label)
}

func formatNeo4jRelationship(val map[string]any, relType any) string {
	typeStr := fmt.Sprintf("%v", relType)
	props, _ := val["_properties"].(map[string]any)
	if len(props) == 0 {
		return fmt.Sprintf("[:%s]", typeStr)
	}
	propParts := make([]string, 0, 2)
	priorityKeys := []string{"metric", "weight", "cost", "name", "type"}
	for _, key := range priorityKeys {
		if v, ok := props[key]; ok && len(propParts) < 2 {
			propParts = append(propParts, fmt.Sprintf("%s: %v", key, v))
		}
	}
	if len(propParts) > 0 {
		return fmt.Sprintf("[:%s {%s}]", typeStr, strings.Join(propParts, ", "))
	}
	return fmt.Sprintf("[:%s]", typeStr)
}

func formatNeo4jPath(nodes any, rels any) string {
	nodesArr, ok1 := nodes.([]any)
	relsArr, ok2 := rels.([]any)
	if !ok1 || !ok2 || len(nodesArr) == 0 {
		return "[empty path]"
	}

	parts := make([]string, 0, len(nodesArr)*2)
	for i, node := range nodesArr {
		nodeMap, ok := node.(map[string]any)
		if ok {
			labels := nodeMap["_labels"]
			parts = append(parts, formatNeo4jNode(nodeMap, labels))
		} else {
			parts = append(parts, formatNeo4jValue(node))
		}
		if i < len(relsArr) {
			relMap, ok := relsArr[i].(map[string]any)
			if ok {
				relType := relMap["_type"]
				parts = append(parts, fmt.Sprintf("-[:%v]->", relType))
			} else {
				parts = append(parts, "->")
			}
		}
	}

	return "[" + strings.Join(parts, " ") + "]"
}

func getNodeIdentifier(val map[string]any) string {
	props, _ := val["_properties"].(map[string]any)
	if props == nil {
		return ""
	}
	candidates := []string{"code", "name", "pk", "id"}
	for _, key := range candidates {
		if v, ok := props[key]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
	}
	for _, v := range props {
		if v != nil {
			s := fmt.Sprintf("%v", v)
			if len(s) > 30 {
				return s[:27] + "..."
			}
			return s
		}
	}
	return ""
}
