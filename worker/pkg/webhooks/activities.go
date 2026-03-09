package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	temporalclient "go.temporal.io/sdk/client"
)

// Activities holds dependencies for webhook delivery activities.
type Activities struct {
	ClickHouse     driver.Conn
	PgPool         *pgxpool.Pool
	TemporalClient temporalclient.Client
	Log            *slog.Logger
}

// PollNewEvents reads new incident events from ClickHouse since the last cursor.
func (a *Activities) PollNewEvents(ctx context.Context) ([]IncidentEventRow, error) {
	// Read cursor
	var lastEventTS time.Time
	err := a.PgPool.QueryRow(ctx,
		`SELECT last_event_ts FROM webhook_cursor WHERE id = 1`,
	).Scan(&lastEventTS)
	if err != nil {
		return nil, fmt.Errorf("read cursor: %w", err)
	}

	// Query new events
	rows, err := a.ClickHouse.Query(ctx, `
		SELECT event_id, event_type, event_ts, payload
		FROM incident_events FINAL
		WHERE ingested_at > $1
		ORDER BY ingested_at
		LIMIT 100
	`, lastEventTS)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []IncidentEventRow
	var maxTS time.Time
	for rows.Next() {
		var evt IncidentEventRow
		if err := rows.Scan(&evt.EventID, &evt.EventType, &evt.EventTS, &evt.Payload); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		events = append(events, evt)
		if evt.EventTS.After(maxTS) {
			maxTS = evt.EventTS
		}
	}

	// Advance cursor
	if len(events) > 0 {
		_, err = a.PgPool.Exec(ctx,
			`UPDATE webhook_cursor SET last_event_ts = $1, updated_at = NOW() WHERE id = 1`,
			maxTS)
		if err != nil {
			return nil, fmt.Errorf("update cursor: %w", err)
		}
	}

	a.Log.Info("polled events", "count", len(events))
	return events, nil
}

// FanOutDeliveries finds matching subscriptions for an event and starts delivery workflows.
func (a *Activities) FanOutDeliveries(ctx context.Context, evt IncidentEventRow) error {
	// Query active subscriptions that match this event type
	rows, err := a.PgPool.Query(ctx, `
		SELECT id, account_id, url, secret, event_types
		FROM webhook_subscriptions
		WHERE is_active = true
		  AND (event_types = '{}' OR $1 = ANY(event_types))
	`, evt.EventType)
	if err != nil {
		return fmt.Errorf("query subscriptions: %w", err)
	}
	defer rows.Close()

	var subs []Subscription
	for rows.Next() {
		var s Subscription
		if err := rows.Scan(&s.ID, &s.AccountID, &s.URL, &s.Secret, &s.EventTypes); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		subs = append(subs, s)
	}

	if len(subs) == 0 {
		return nil
	}

	a.Log.Info("fan out deliveries", "event_id", evt.EventID, "subscriptions", len(subs))

	for _, sub := range subs {
		deliveryID := uuid.New()

		// Create delivery record
		_, err := a.PgPool.Exec(ctx, `
			INSERT INTO webhook_deliveries (id, subscription_id, event_id, event_type, status)
			VALUES ($1, $2, $3, $4, 'pending')
		`, deliveryID, sub.ID, evt.EventID, evt.EventType)
		if err != nil {
			a.Log.Error("create delivery record", "error", err)
			continue
		}

		// Start delivery workflow
		input := DeliverWebhookInput{
			DeliveryID:     deliveryID,
			SubscriptionID: sub.ID,
			EventID:        evt.EventID,
			EventType:      evt.EventType,
			URL:            sub.URL,
			Secret:         sub.Secret,
			Payload:        []byte(evt.Payload),
		}

		workflowID := fmt.Sprintf("webhook-deliver-%s-%s", evt.EventID[:8], sub.ID.String()[:8])
		_, err = a.TemporalClient.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: TaskQueue,
		}, DeliverWebhookWorkflow, input)
		if err != nil {
			a.Log.Error("start delivery workflow", "error", err)
			// Update delivery status to failed
			_, _ = a.PgPool.Exec(ctx, `
				UPDATE webhook_deliveries SET status = 'failed', error = $1, updated_at = NOW()
				WHERE id = $2
			`, err.Error(), deliveryID)
		}
	}

	return nil
}

// DeliverWebhook sends the HTTP POST with HMAC signature and updates the delivery record.
func (a *Activities) DeliverWebhook(ctx context.Context, input DeliverWebhookInput) error {
	// Compute HMAC-SHA256 signature
	mac := hmac.New(sha256.New, []byte(input.Secret))
	mac.Write(input.Payload)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, "POST", input.URL, bytes.NewReader(input.Payload))
	if err != nil {
		a.updateDeliveryFailed(ctx, input.DeliveryID, 0, err.Error())
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DZ-Signature", signature)
	req.Header.Set("X-DZ-Event", input.EventType)
	req.Header.Set("X-DZ-Delivery", input.DeliveryID.String())
	req.Header.Set("User-Agent", "DoubleZero-Webhooks/1.0")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		a.updateDeliveryFailed(ctx, input.DeliveryID, 0, err.Error())
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))

	if resp.StatusCode >= 400 {
		errMsg := fmt.Sprintf("status %d: %s", resp.StatusCode, string(body))
		a.updateDeliveryFailed(ctx, input.DeliveryID, resp.StatusCode, errMsg)
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	// Success
	_, _ = a.PgPool.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = 'success', attempts = attempts + 1, last_attempt_at = NOW(),
			response_status = $1, updated_at = NOW()
		WHERE id = $2
	`, resp.StatusCode, input.DeliveryID)

	a.Log.Info("webhook delivered", "delivery_id", input.DeliveryID, "status", resp.StatusCode)
	return nil
}

func (a *Activities) updateDeliveryFailed(ctx context.Context, deliveryID uuid.UUID, statusCode int, errMsg string) {
	var responseStatus *int
	if statusCode > 0 {
		responseStatus = &statusCode
	}
	_, _ = a.PgPool.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = 'failed', attempts = attempts + 1, last_attempt_at = NOW(),
			response_status = $1, error = $2, updated_at = NOW()
		WHERE id = $3
	`, responseStatus, errMsg, deliveryID)
}
