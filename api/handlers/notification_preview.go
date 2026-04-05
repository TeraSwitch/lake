package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/malbeclabs/lake/api/notifier"
)

// PreviewNotifications streams a live preview of notification events via SSE.
// Uses an ephemeral in-memory checkpoint (starts at now minus lookback window)
// so no config or persistent state is needed.
func (a *API) PreviewNotifications(w http.ResponseWriter, r *http.Request) {
	account := GetAccountFromContext(r.Context())
	if account == nil {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	sourceType := r.URL.Query().Get("source_type")
	if sourceType == "" {
		http.Error(w, "source_type is required", http.StatusBadRequest)
		return
	}

	source, ok := a.NotificationSources[sourceType]
	if !ok {
		http.Error(w, "unknown source_type", http.StatusBadRequest)
		return
	}

	// SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	sendEvent := func(eventType string, data any) {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
		flusher.Flush()
	}

	// Start with a lookback window to show recent events immediately.
	cp := notifier.Checkpoint{
		LastEventTS: time.Now().Add(-5 * time.Minute),
	}

	// Initial poll for recent events.
	groups, newCP, err := source.Poll(r.Context(), cp)
	if err != nil {
		sendEvent("error", map[string]string{"message": "failed to poll: " + err.Error()})
		return
	}
	if newCP.LastEventTS.After(cp.LastEventTS) || newCP.LastSlot > cp.LastSlot {
		cp = newCP
	}

	for _, g := range groups {
		sendEvent("event_group", g)
	}

	sendEvent("caught_up", map[string]bool{"caught_up": true})

	// Poll for new events while the connection is open.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			sendEvent("heartbeat", map[string]int64{"ts": time.Now().Unix()})
		case <-ticker.C:
			groups, newCP, err := source.Poll(r.Context(), cp)
			if err != nil {
				sendEvent("error", map[string]string{"message": "poll failed: " + err.Error()})
				continue
			}
			if newCP.LastEventTS.After(cp.LastEventTS) || newCP.LastSlot > cp.LastSlot {
				cp = newCP
			}
			for _, g := range groups {
				sendEvent("event_group", g)
			}
		}
	}
}
