package webhooks

import (
	"time"

	"github.com/google/uuid"
)

// IncidentEventRow represents an event read from the ClickHouse incident_events table.
type IncidentEventRow struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	EventTS   time.Time `json:"event_ts"`
	Payload   string    `json:"payload"`
}

// Subscription represents a webhook subscription from Postgres.
type Subscription struct {
	ID         uuid.UUID `json:"id"`
	AccountID  uuid.UUID `json:"account_id"`
	URL        string    `json:"url"`
	Secret     string    `json:"secret"`
	EventTypes []string  `json:"event_types"`
}

// DeliverWebhookInput is the input for a single webhook delivery.
type DeliverWebhookInput struct {
	DeliveryID     uuid.UUID `json:"delivery_id"`
	SubscriptionID uuid.UUID `json:"subscription_id"`
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	URL            string    `json:"url"`
	Secret         string    `json:"secret"`
	Payload        []byte    `json:"payload"`
}
