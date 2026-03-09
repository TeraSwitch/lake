package handlers

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/malbeclabs/lake/api/config"
)

// WebhookSubscription represents a webhook subscription.
type WebhookSubscription struct {
	ID         uuid.UUID `json:"id"`
	AccountID  uuid.UUID `json:"account_id"`
	Name       string    `json:"name"`
	URL        string    `json:"url"`
	Secret     string    `json:"secret,omitempty"` // only returned on create/rotate
	EventTypes []string  `json:"event_types"`
	IsActive   bool      `json:"is_active"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// WebhookDelivery represents a delivery attempt.
type WebhookDelivery struct {
	ID             uuid.UUID  `json:"id"`
	SubscriptionID uuid.UUID  `json:"subscription_id"`
	EventID        string     `json:"event_id"`
	EventType      string     `json:"event_type"`
	Status         string     `json:"status"`
	Attempts       int        `json:"attempts"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	ResponseStatus *int       `json:"response_status,omitempty"`
	Error          *string    `json:"error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

type createWebhookRequest struct {
	Name       string   `json:"name"`
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
}

type updateWebhookRequest struct {
	Name       *string  `json:"name,omitempty"`
	URL        *string  `json:"url,omitempty"`
	EventTypes []string `json:"event_types,omitempty"`
	IsActive   *bool    `json:"is_active,omitempty"`
}

func generateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ListWebhooks returns all webhook subscriptions for the authenticated user.
func ListWebhooks(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	rows, err := config.PgPool.Query(r.Context(),
		`SELECT id, account_id, name, url, event_types, is_active, created_at, updated_at
		 FROM webhook_subscriptions WHERE account_id = $1 ORDER BY created_at DESC`,
		account.ID)
	if err != nil {
		http.Error(w, "Failed to query webhooks", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var subs []WebhookSubscription
	for rows.Next() {
		var s WebhookSubscription
		if err := rows.Scan(&s.ID, &s.AccountID, &s.Name, &s.URL, &s.EventTypes, &s.IsActive, &s.CreatedAt, &s.UpdatedAt); err != nil {
			http.Error(w, "Failed to scan webhook", http.StatusInternalServerError)
			return
		}
		subs = append(subs, s)
	}

	if subs == nil {
		subs = []WebhookSubscription{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(subs)
}

// CreateWebhook creates a new webhook subscription.
func CreateWebhook(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req createWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.URL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}

	secret, err := generateSecret()
	if err != nil {
		http.Error(w, "Failed to generate secret", http.StatusInternalServerError)
		return
	}

	if req.EventTypes == nil {
		req.EventTypes = []string{}
	}

	var s WebhookSubscription
	err = config.PgPool.QueryRow(r.Context(),
		`INSERT INTO webhook_subscriptions (account_id, name, url, secret, event_types)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, account_id, name, url, event_types, is_active, created_at, updated_at`,
		account.ID, req.Name, req.URL, secret, req.EventTypes,
	).Scan(&s.ID, &s.AccountID, &s.Name, &s.URL, &s.EventTypes, &s.IsActive, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create webhook: %v", err), http.StatusInternalServerError)
		return
	}

	s.Secret = secret // Return secret only on creation

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(s)
}

// GetWebhook returns a single webhook subscription.
func GetWebhook(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var s WebhookSubscription
	err := config.PgPool.QueryRow(r.Context(),
		`SELECT id, account_id, name, url, event_types, is_active, created_at, updated_at
		 FROM webhook_subscriptions WHERE id = $1 AND account_id = $2`,
		id, account.ID,
	).Scan(&s.ID, &s.AccountID, &s.Name, &s.URL, &s.EventTypes, &s.IsActive, &s.CreatedAt, &s.UpdatedAt)
	if err != nil {
		http.Error(w, "Webhook not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}

// UpdateWebhook updates a webhook subscription.
func UpdateWebhook(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var req updateWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Verify ownership
	var exists bool
	err := config.PgPool.QueryRow(r.Context(),
		`SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1 AND account_id = $2)`,
		id, account.ID,
	).Scan(&exists)
	if err != nil || !exists {
		http.Error(w, "Webhook not found", http.StatusNotFound)
		return
	}

	if req.Name != nil {
		_, _ = config.PgPool.Exec(r.Context(),
			`UPDATE webhook_subscriptions SET name = $1, updated_at = NOW() WHERE id = $2`, *req.Name, id)
	}
	if req.URL != nil {
		_, _ = config.PgPool.Exec(r.Context(),
			`UPDATE webhook_subscriptions SET url = $1, updated_at = NOW() WHERE id = $2`, *req.URL, id)
	}
	if req.EventTypes != nil {
		_, _ = config.PgPool.Exec(r.Context(),
			`UPDATE webhook_subscriptions SET event_types = $1, updated_at = NOW() WHERE id = $2`, req.EventTypes, id)
	}
	if req.IsActive != nil {
		_, _ = config.PgPool.Exec(r.Context(),
			`UPDATE webhook_subscriptions SET is_active = $1, updated_at = NOW() WHERE id = $2`, *req.IsActive, id)
	}

	// Return updated
	var s WebhookSubscription
	_ = config.PgPool.QueryRow(r.Context(),
		`SELECT id, account_id, name, url, event_types, is_active, created_at, updated_at
		 FROM webhook_subscriptions WHERE id = $1`,
		id,
	).Scan(&s.ID, &s.AccountID, &s.Name, &s.URL, &s.EventTypes, &s.IsActive, &s.CreatedAt, &s.UpdatedAt)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}

// DeleteWebhook deletes a webhook subscription.
func DeleteWebhook(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	result, err := config.PgPool.Exec(r.Context(),
		`DELETE FROM webhook_subscriptions WHERE id = $1 AND account_id = $2`,
		id, account.ID)
	if err != nil {
		http.Error(w, "Failed to delete webhook", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected() == 0 {
		http.Error(w, "Webhook not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// RotateWebhookSecret generates a new secret for a webhook subscription.
func RotateWebhookSecret(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	secret, err := generateSecret()
	if err != nil {
		http.Error(w, "Failed to generate secret", http.StatusInternalServerError)
		return
	}

	result, err := config.PgPool.Exec(r.Context(),
		`UPDATE webhook_subscriptions SET secret = $1, updated_at = NOW()
		 WHERE id = $2 AND account_id = $3`,
		secret, id, account.ID)
	if err != nil {
		http.Error(w, "Failed to rotate secret", http.StatusInternalServerError)
		return
	}
	if result.RowsAffected() == 0 {
		http.Error(w, "Webhook not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"secret": secret})
}

// ListWebhookDeliveries returns delivery attempts for a webhook subscription.
func ListWebhookDeliveries(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	// Verify ownership
	var exists bool
	err := config.PgPool.QueryRow(r.Context(),
		`SELECT EXISTS(SELECT 1 FROM webhook_subscriptions WHERE id = $1 AND account_id = $2)`,
		id, account.ID,
	).Scan(&exists)
	if err != nil || !exists {
		http.Error(w, "Webhook not found", http.StatusNotFound)
		return
	}

	rows, err := config.PgPool.Query(r.Context(),
		`SELECT id, subscription_id, event_id, event_type, status, attempts, last_attempt_at, response_status, error, created_at
		 FROM webhook_deliveries WHERE subscription_id = $1 ORDER BY created_at DESC LIMIT 100`,
		id)
	if err != nil {
		http.Error(w, "Failed to query deliveries", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var deliveries []WebhookDelivery
	for rows.Next() {
		var d WebhookDelivery
		if err := rows.Scan(&d.ID, &d.SubscriptionID, &d.EventID, &d.EventType, &d.Status, &d.Attempts, &d.LastAttemptAt, &d.ResponseStatus, &d.Error, &d.CreatedAt); err != nil {
			http.Error(w, "Failed to scan delivery", http.StatusInternalServerError)
			return
		}
		deliveries = append(deliveries, d)
	}

	if deliveries == nil {
		deliveries = []WebhookDelivery{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(deliveries)
}

// TestWebhook sends a test event to a webhook subscription.
func TestWebhook(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")

	var url, secret string
	err := config.PgPool.QueryRow(r.Context(),
		`SELECT url, secret FROM webhook_subscriptions WHERE id = $1 AND account_id = $2`,
		id, account.ID,
	).Scan(&url, &secret)
	if err != nil {
		http.Error(w, "Webhook not found", http.StatusNotFound)
		return
	}

	// Send a test event payload
	testPayload := map[string]any{
		"event_id":   "test-" + uuid.New().String()[:8],
		"event_type": "test",
		"event_ts":   time.Now().UTC().Format(time.RFC3339),
		"data": map[string]string{
			"message": "This is a test event from DoubleZero Data.",
		},
	}

	payloadBytes, _ := json.Marshal(testPayload)
	statusCode, err := deliverWebhookHTTP(url, secret, "test", payloadBytes)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":         true,
		"response_status": statusCode,
	})
}
