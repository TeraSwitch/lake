package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/malbeclabs/lake/api/notifier"
)

func (a *API) notificationStore() *notifier.ConfigStore {
	return &notifier.ConfigStore{Pool: a.PgPool}
}

// --- Webhook Endpoints ---

func (a *API) ListWebhookEndpoints(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	endpoints, err := a.notificationStore().ListEndpoints(r.Context(), account.ID.String())
	if err != nil {
		logError("failed to list webhook endpoints", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if endpoints == nil {
		endpoints = []notifier.WebhookEndpoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(endpoints)
}

type createWebhookEndpointRequest struct {
	Name         string `json:"name"`
	URL          string `json:"url"`
	OutputFormat string `json:"output_format"`
}

func (a *API) CreateWebhookEndpoint(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	var req createWebhookEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.URL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}

	outputFormat := req.OutputFormat
	if outputFormat == "" {
		outputFormat = notifier.FormatMarkdown
	}

	e := &notifier.WebhookEndpoint{
		AccountID:    account.ID.String(),
		Name:         req.Name,
		URL:          req.URL,
		OutputFormat: outputFormat,
	}

	if err := a.notificationStore().CreateEndpoint(r.Context(), e); err != nil {
		logError("failed to create webhook endpoint", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(e)
}

type updateWebhookEndpointRequest struct {
	Name         string `json:"name"`
	URL          string `json:"url"`
	OutputFormat string `json:"output_format"`
}

func (a *API) UpdateWebhookEndpoint(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	var req updateWebhookEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	e := &notifier.WebhookEndpoint{
		Name:         req.Name,
		URL:          req.URL,
		OutputFormat: req.OutputFormat,
	}

	if err := a.notificationStore().UpdateEndpoint(r.Context(), id, account.ID.String(), e); err != nil {
		logError("failed to update webhook endpoint", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *API) DeleteWebhookEndpoint(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	if err := a.notificationStore().DeleteEndpoint(r.Context(), id, account.ID.String()); err != nil {
		logError("failed to delete webhook endpoint", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Notification Configs ---

func (a *API) ListNotificationConfigs(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	configs, err := a.notificationStore().ListConfigsByAccount(r.Context(), account.ID.String())
	if err != nil {
		logError("failed to list notification configs", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if configs == nil {
		configs = []notifier.NotificationConfig{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(configs)
}

type createNotificationConfigRequest struct {
	EndpointID string          `json:"endpoint_id"`
	SourceType string          `json:"source_type"`
	Enabled    *bool           `json:"enabled"`
	Filters    json.RawMessage `json:"filters"`
}

func (a *API) CreateNotificationConfig(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	var req createNotificationConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.EndpointID == "" || req.SourceType == "" {
		http.Error(w, "endpoint_id and source_type are required", http.StatusBadRequest)
		return
	}

	// Verify the endpoint belongs to this account.
	ep, err := a.notificationStore().GetEndpoint(r.Context(), req.EndpointID)
	if err != nil || ep.AccountID != account.ID.String() {
		http.Error(w, "webhook endpoint not found", http.StatusBadRequest)
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	filters := req.Filters
	if filters == nil {
		filters = json.RawMessage("{}")
	}

	cfg := &notifier.NotificationConfig{
		AccountID:  account.ID.String(),
		EndpointID: req.EndpointID,
		SourceType: req.SourceType,
		Enabled:    enabled,
		Filters:    filters,
	}

	if err := a.notificationStore().CreateConfig(r.Context(), cfg); err != nil {
		logError("failed to create notification config", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(cfg)
}

type updateNotificationConfigRequest struct {
	EndpointID string          `json:"endpoint_id"`
	Enabled    *bool           `json:"enabled"`
	Filters    json.RawMessage `json:"filters"`
}

func (a *API) UpdateNotificationConfig(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	var req updateNotificationConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	filters := req.Filters
	if filters == nil {
		filters = json.RawMessage("{}")
	}

	endpointID := req.EndpointID
	if endpointID != "" {
		ep, err := a.notificationStore().GetEndpoint(r.Context(), endpointID)
		if err != nil || ep.AccountID != account.ID.String() {
			http.Error(w, "webhook endpoint not found", http.StatusBadRequest)
			return
		}
	}

	cfg := &notifier.NotificationConfig{
		EndpointID: endpointID,
		Enabled:    enabled,
		Filters:    filters,
	}

	if err := a.notificationStore().UpdateConfig(r.Context(), id, account.ID.String(), cfg); err != nil {
		logError("failed to update notification config", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *API) DeleteNotificationConfig(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	if err := a.notificationStore().DeleteConfig(r.Context(), id, account.ID.String()); err != nil {
		logError("failed to delete notification config", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
