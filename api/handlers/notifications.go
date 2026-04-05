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

// --- Notification Endpoints ---

func (a *API) ListNotificationEndpoints(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	endpoints, err := a.notificationStore().ListEndpoints(r.Context(), account.ID.String())
	if err != nil {
		logError("failed to list notification endpoints", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	if endpoints == nil {
		endpoints = []notifier.NotificationEndpoint{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(endpoints)
}

type createEndpointRequest struct {
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Config       json.RawMessage `json:"config"`
	OutputFormat string          `json:"output_format"`
}

func (a *API) CreateNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	var req createEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}

	if err := validateEndpointConfig(req.Type, req.Config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	outputFormat := req.OutputFormat
	if outputFormat == "" {
		outputFormat = notifier.FormatMarkdown
	}
	config := req.Config
	if config == nil {
		config = json.RawMessage("{}")
	}

	e := &notifier.NotificationEndpoint{
		AccountID:    account.ID.String(),
		Name:         req.Name,
		Type:         req.Type,
		Config:       config,
		OutputFormat: outputFormat,
	}

	if err := a.notificationStore().CreateEndpoint(r.Context(), e); err != nil {
		logError("failed to create notification endpoint", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(e)
}

type updateEndpointRequest struct {
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Config       json.RawMessage `json:"config"`
	OutputFormat string          `json:"output_format"`
}

func (a *API) UpdateNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
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

	var req updateEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Type != "" {
		if err := validateEndpointConfig(req.Type, req.Config); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	config := req.Config
	if config == nil {
		config = json.RawMessage("{}")
	}

	e := &notifier.NotificationEndpoint{
		Name:         req.Name,
		Type:         req.Type,
		Config:       config,
		OutputFormat: req.OutputFormat,
	}

	if err := a.notificationStore().UpdateEndpoint(r.Context(), id, account.ID.String(), e); err != nil {
		logError("failed to update notification endpoint", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *API) DeleteNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
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
		logError("failed to delete notification endpoint", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func validateEndpointConfig(endpointType string, config json.RawMessage) error {
	switch endpointType {
	case notifier.EndpointTypeWebhook:
		var c struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(config, &c); err != nil || c.URL == "" {
			return &validationError{"webhook config requires a url"}
		}
	default:
		return &validationError{"unsupported endpoint type: " + endpointType}
	}
	return nil
}

type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }

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

	ep, err := a.notificationStore().GetEndpoint(r.Context(), req.EndpointID)
	if err != nil || ep.AccountID != account.ID.String() {
		http.Error(w, "endpoint not found", http.StatusBadRequest)
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

	if req.EndpointID != "" {
		ep, err := a.notificationStore().GetEndpoint(r.Context(), req.EndpointID)
		if err != nil || ep.AccountID != account.ID.String() {
			http.Error(w, "endpoint not found", http.StatusBadRequest)
			return
		}
	}

	cfg := &notifier.NotificationConfig{
		EndpointID: req.EndpointID,
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
