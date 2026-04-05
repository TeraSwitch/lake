package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Output format constants.
const (
	FormatMarkdown  = "markdown"
	FormatPlaintext = "plaintext"
)

// Endpoint type constants.
const (
	EndpointTypeWebhook = "webhook"
)

// NotificationEndpoint is a reusable delivery target.
type NotificationEndpoint struct {
	ID           string          `json:"id"`
	AccountID    string          `json:"account_id"`
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Config       json.RawMessage `json:"config"`
	OutputFormat string          `json:"output_format"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// NotificationConfig defines what to watch and which endpoint to deliver to.
type NotificationConfig struct {
	ID         string          `json:"id"`
	AccountID  string          `json:"account_id"`
	EndpointID string          `json:"endpoint_id"`
	SourceType string          `json:"source_type"`
	Enabled    bool            `json:"enabled"`
	Filters    json.RawMessage `json:"filters"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// ConfigStore provides CRUD operations for webhook endpoints, notification
// configs, and checkpoints.
type ConfigStore struct {
	Pool *pgxpool.Pool
}

// --- Endpoints ---

func (s *ConfigStore) ListEndpoints(ctx context.Context, accountID string) ([]NotificationEndpoint, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, account_id, name, type, config, output_format, created_at, updated_at
		 FROM notification_endpoints WHERE account_id = $1 ORDER BY created_at DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var endpoints []NotificationEndpoint
	for rows.Next() {
		var e NotificationEndpoint
		if err := rows.Scan(&e.ID, &e.AccountID, &e.Name, &e.Type, &e.Config, &e.OutputFormat, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		endpoints = append(endpoints, e)
	}
	return endpoints, rows.Err()
}

func (s *ConfigStore) GetEndpoint(ctx context.Context, id string) (*NotificationEndpoint, error) {
	var e NotificationEndpoint
	err := s.Pool.QueryRow(ctx,
		`SELECT id, account_id, name, type, config, output_format, created_at, updated_at
		 FROM notification_endpoints WHERE id = $1`, id,
	).Scan(&e.ID, &e.AccountID, &e.Name, &e.Type, &e.Config, &e.OutputFormat, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (s *ConfigStore) CreateEndpoint(ctx context.Context, e *NotificationEndpoint) error {
	return s.Pool.QueryRow(ctx,
		`INSERT INTO notification_endpoints (account_id, name, type, config, output_format)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, created_at, updated_at`,
		e.AccountID, e.Name, e.Type, e.Config, e.OutputFormat,
	).Scan(&e.ID, &e.CreatedAt, &e.UpdatedAt)
}

func (s *ConfigStore) UpdateEndpoint(ctx context.Context, id, accountID string, e *NotificationEndpoint) error {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE notification_endpoints SET name = $1, type = $2, config = $3, output_format = $4, updated_at = NOW()
		 WHERE id = $5 AND account_id = $6`,
		e.Name, e.Type, e.Config, e.OutputFormat, id, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("endpoint not found")
	}
	return nil
}

func (s *ConfigStore) DeleteEndpoint(ctx context.Context, id, accountID string) error {
	tag, err := s.Pool.Exec(ctx,
		`DELETE FROM notification_endpoints WHERE id = $1 AND account_id = $2`, id, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("endpoint not found")
	}
	return nil
}

// --- Notification Configs ---

// ConfigWithEndpoint is a notification config joined with its endpoint,
// used by the workflow to resolve delivery targets.
type ConfigWithEndpoint struct {
	NotificationConfig
	EndpointType   string
	EndpointConfig json.RawMessage
	OutputFormat   string
}

func (s *ConfigStore) ListEnabledWithEndpoints(ctx context.Context) ([]ConfigWithEndpoint, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT c.id, c.account_id, c.endpoint_id, c.source_type, c.enabled, c.filters, c.created_at, c.updated_at,
		        e.type, e.config, e.output_format
		 FROM notification_configs c
		 JOIN notification_endpoints e ON e.id = c.endpoint_id
		 WHERE c.enabled = true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []ConfigWithEndpoint
	for rows.Next() {
		var c ConfigWithEndpoint
		if err := rows.Scan(&c.ID, &c.AccountID, &c.EndpointID, &c.SourceType, &c.Enabled, &c.Filters,
			&c.CreatedAt, &c.UpdatedAt, &c.EndpointType, &c.EndpointConfig, &c.OutputFormat); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}
	return configs, rows.Err()
}

func (s *ConfigStore) ListConfigsByAccount(ctx context.Context, accountID string) ([]NotificationConfig, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, account_id, endpoint_id, source_type, enabled, filters, created_at, updated_at
		 FROM notification_configs WHERE account_id = $1 ORDER BY created_at DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []NotificationConfig
	for rows.Next() {
		var c NotificationConfig
		if err := rows.Scan(&c.ID, &c.AccountID, &c.EndpointID, &c.SourceType, &c.Enabled, &c.Filters, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}
	return configs, rows.Err()
}

func (s *ConfigStore) CreateConfig(ctx context.Context, c *NotificationConfig) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx,
		`INSERT INTO notification_configs (account_id, endpoint_id, source_type, enabled, filters)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, created_at, updated_at`,
		c.AccountID, c.EndpointID, c.SourceType, c.Enabled, c.Filters,
	).Scan(&c.ID, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return err
	}

	// Initialize checkpoint to now so we only notify on future events.
	_, err = tx.Exec(ctx,
		`INSERT INTO notification_checkpoints (account_id, source_type, last_event_ts, last_slot, updated_at)
		 VALUES ($1, $2, NOW(), 0, NOW())
		 ON CONFLICT (account_id, source_type) DO NOTHING`,
		c.AccountID, c.SourceType,
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *ConfigStore) UpdateConfig(ctx context.Context, id, accountID string, c *NotificationConfig) error {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE notification_configs
		 SET endpoint_id = $1, enabled = $2, filters = $3, updated_at = NOW()
		 WHERE id = $4 AND account_id = $5`,
		c.EndpointID, c.Enabled, c.Filters, id, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification config not found")
	}
	return nil
}

func (s *ConfigStore) DeleteConfig(ctx context.Context, id, accountID string) error {
	tag, err := s.Pool.Exec(ctx,
		`DELETE FROM notification_configs WHERE id = $1 AND account_id = $2`, id, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification config not found")
	}
	return nil
}

// --- Checkpoints ---

func (s *ConfigStore) GetCheckpoint(ctx context.Context, accountID, sourceType string) (Checkpoint, error) {
	var cp Checkpoint
	err := s.Pool.QueryRow(ctx,
		`SELECT last_event_ts, last_slot FROM notification_checkpoints
		 WHERE account_id = $1 AND source_type = $2`,
		accountID, sourceType,
	).Scan(&cp.LastEventTS, &cp.LastSlot)
	if err != nil {
		return Checkpoint{}, nil
	}
	return cp, nil
}

func (s *ConfigStore) SaveCheckpoint(ctx context.Context, accountID, sourceType string, cp Checkpoint) error {
	_, err := s.Pool.Exec(ctx,
		`INSERT INTO notification_checkpoints (account_id, source_type, last_event_ts, last_slot, updated_at)
		 VALUES ($1, $2, $3, $4, NOW())
		 ON CONFLICT (account_id, source_type) DO UPDATE
		 SET last_event_ts = EXCLUDED.last_event_ts, last_slot = EXCLUDED.last_slot, updated_at = NOW()`,
		accountID, sourceType, cp.LastEventTS, cp.LastSlot)
	return err
}
