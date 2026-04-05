package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/malbeclabs/lake/api/notifier"
)

// notificationConfigStore returns a ConfigStore backed by the API's PgPool.
func (a *API) notificationConfigStore() *notifier.ConfigStore {
	return &notifier.ConfigStore{Pool: a.PgPool}
}

// ListNotificationConfigs returns all notification configs for the authenticated user.
func (a *API) ListNotificationConfigs(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	configs, err := a.notificationConfigStore().ListByAccount(r.Context(), account.ID.String())
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

// createNotificationConfigRequest is the JSON body for creating a notification config.
type createNotificationConfigRequest struct {
	SourceType   string          `json:"source_type"`
	ChannelType  string          `json:"channel_type"`
	Destination  json.RawMessage `json:"destination"`
	OutputFormat string          `json:"output_format"`
	Enabled      *bool           `json:"enabled"`
	Filters      json.RawMessage `json:"filters"`
}

// CreateNotificationConfig creates a new notification config.
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

	if req.SourceType == "" || req.ChannelType == "" {
		http.Error(w, "source_type and channel_type are required", http.StatusBadRequest)
		return
	}

	if err := a.validateNotificationConfig(r, req.ChannelType, req.Destination); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	destination := req.Destination
	if destination == nil {
		destination = json.RawMessage("{}")
	}
	filters := req.Filters
	if filters == nil {
		filters = json.RawMessage("{}")
	}

	cfg := &notifier.NotificationConfig{
		AccountID:    account.ID.String(),
		SourceType:   req.SourceType,
		ChannelType:  req.ChannelType,
		Destination:  destination,
		OutputFormat: req.OutputFormat,
		Enabled:      enabled,
		Filters:      filters,
	}

	if err := a.notificationConfigStore().Create(r.Context(), cfg); err != nil {
		logError("failed to create notification config", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(cfg)
}

// updateNotificationConfigRequest is the JSON body for updating a notification config.
type updateNotificationConfigRequest struct {
	ChannelType  string          `json:"channel_type"`
	Destination  json.RawMessage `json:"destination"`
	OutputFormat string          `json:"output_format"`
	Enabled      *bool           `json:"enabled"`
	Filters      json.RawMessage `json:"filters"`
}

// UpdateNotificationConfig updates an existing notification config.
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

	if req.ChannelType != "" {
		if err := a.validateNotificationConfig(r, req.ChannelType, req.Destination); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	destination := req.Destination
	if destination == nil {
		destination = json.RawMessage("{}")
	}
	filters := req.Filters
	if filters == nil {
		filters = json.RawMessage("{}")
	}

	cfg := &notifier.NotificationConfig{
		ChannelType:  req.ChannelType,
		Destination:  destination,
		OutputFormat: req.OutputFormat,
		Enabled:      enabled,
		Filters:      filters,
	}

	if err := a.notificationConfigStore().Update(r.Context(), id, account.ID.String(), cfg); err != nil {
		logError("failed to update notification config", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DeleteNotificationConfig deletes a notification config.
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

	if err := a.notificationConfigStore().Delete(r.Context(), id, account.ID.String()); err != nil {
		logError("failed to delete notification config", "error", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// validateNotificationConfig validates channel-specific constraints.
func (a *API) validateNotificationConfig(r *http.Request, channelType string, destination json.RawMessage) error {
	switch channelType {
	case "slack":
		var dest struct {
			TeamID    string `json:"team_id"`
			ChannelID string `json:"channel_id"`
		}
		if err := json.Unmarshal(destination, &dest); err != nil {
			return &validationError{"invalid slack destination"}
		}
		if dest.TeamID == "" || dest.ChannelID == "" {
			return &validationError{"slack destination requires team_id and channel_id"}
		}
		// Verify the caller owns the Slack installation.
		account := GetAccountFromContext(r.Context())
		inst, err := a.GetSlackInstallationByTeamID(r.Context(), dest.TeamID)
		if err != nil {
			return &validationError{"Slack installation not found for this workspace"}
		}
		if inst.InstalledBy == nil || *inst.InstalledBy != account.ID.String() {
			return &validationError{"you must be the Slack installer for this workspace"}
		}
	case "webhook":
		var dest struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(destination, &dest); err != nil || dest.URL == "" {
			return &validationError{"webhook destination requires a url"}
		}
	default:
		return &validationError{"unsupported channel_type: " + channelType}
	}
	return nil
}

type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }
