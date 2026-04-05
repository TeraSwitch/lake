package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NotificationConfig is a stored notification configuration.
type NotificationConfig struct {
	ID          string          `json:"id"`
	AccountID   string          `json:"account_id"`
	SourceType  string          `json:"source_type"`
	ChannelType string          `json:"channel_type"`
	Destination json.RawMessage `json:"destination"`
	Enabled     bool            `json:"enabled"`
	Filters     json.RawMessage `json:"filters"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// ConfigStore provides CRUD operations for notification configs and checkpoints.
type ConfigStore struct {
	Pool *pgxpool.Pool
}

// ListEnabled returns all enabled notification configs.
func (s *ConfigStore) ListEnabled(ctx context.Context) ([]NotificationConfig, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, account_id, source_type, channel_type, destination, enabled, filters, created_at, updated_at
		 FROM notification_configs WHERE enabled = true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []NotificationConfig
	for rows.Next() {
		var c NotificationConfig
		if err := rows.Scan(&c.ID, &c.AccountID, &c.SourceType, &c.ChannelType, &c.Destination, &c.Enabled, &c.Filters, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}
	return configs, rows.Err()
}

// ListByAccount returns all notification configs for an account.
func (s *ConfigStore) ListByAccount(ctx context.Context, accountID string) ([]NotificationConfig, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT id, account_id, source_type, channel_type, destination, enabled, filters, created_at, updated_at
		 FROM notification_configs WHERE account_id = $1 ORDER BY created_at DESC`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []NotificationConfig
	for rows.Next() {
		var c NotificationConfig
		if err := rows.Scan(&c.ID, &c.AccountID, &c.SourceType, &c.ChannelType, &c.Destination, &c.Enabled, &c.Filters, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}
	return configs, rows.Err()
}

// Create inserts a new notification config and initializes the checkpoint to
// now so that only future events are delivered (not historical data).
func (s *ConfigStore) Create(ctx context.Context, c *NotificationConfig) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx,
		`INSERT INTO notification_configs (account_id, source_type, channel_type, destination, enabled, filters)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at, updated_at`,
		c.AccountID, c.SourceType, c.ChannelType, c.Destination, c.Enabled, c.Filters,
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

// Update modifies an existing notification config.
func (s *ConfigStore) Update(ctx context.Context, id, accountID string, c *NotificationConfig) error {
	tag, err := s.Pool.Exec(ctx,
		`UPDATE notification_configs
		 SET channel_type = $1, destination = $2, enabled = $3, filters = $4, updated_at = NOW()
		 WHERE id = $5 AND account_id = $6`,
		c.ChannelType, c.Destination, c.Enabled, c.Filters, id, accountID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification config not found")
	}
	return nil
}

// Delete removes a notification config.
func (s *ConfigStore) Delete(ctx context.Context, id, accountID string) error {
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

// TransferSlackConfigs transfers notification configs that use a Slack channel
// with the given team_id to a new account. Called during installation takeover.
func (s *ConfigStore) TransferSlackConfigs(ctx context.Context, teamID, newAccountID string) error {
	_, err := s.Pool.Exec(ctx,
		`UPDATE notification_configs
		 SET account_id = $1, updated_at = NOW()
		 WHERE channel_type = 'slack' AND destination->>'team_id' = $2`,
		newAccountID, teamID)
	return err
}

// GetCheckpoint returns the checkpoint for an account + source type.
func (s *ConfigStore) GetCheckpoint(ctx context.Context, accountID, sourceType string) (Checkpoint, error) {
	var cp Checkpoint
	err := s.Pool.QueryRow(ctx,
		`SELECT last_event_ts, last_slot FROM notification_checkpoints
		 WHERE account_id = $1 AND source_type = $2`,
		accountID, sourceType,
	).Scan(&cp.LastEventTS, &cp.LastSlot)
	if err != nil {
		// Return zero checkpoint on not found — will poll from beginning
		return Checkpoint{}, nil
	}
	return cp, nil
}

// SaveCheckpoint upserts the checkpoint for an account + source type.
func (s *ConfigStore) SaveCheckpoint(ctx context.Context, accountID, sourceType string, cp Checkpoint) error {
	_, err := s.Pool.Exec(ctx,
		`INSERT INTO notification_checkpoints (account_id, source_type, last_event_ts, last_slot, updated_at)
		 VALUES ($1, $2, $3, $4, NOW())
		 ON CONFLICT (account_id, source_type) DO UPDATE
		 SET last_event_ts = EXCLUDED.last_event_ts, last_slot = EXCLUDED.last_slot, updated_at = NOW()`,
		accountID, sourceType, cp.LastEventTS, cp.LastSlot)
	return err
}
