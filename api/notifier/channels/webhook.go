package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/malbeclabs/lake/api/notifier"
)

const ChannelTypeWebhook = "webhook"

// WebhookDestination is the JSON schema for webhook channel destinations.
type WebhookDestination struct {
	URL string `json:"url"`
}

// WebhookChannel delivers notifications via HTTP POST to a configured URL.
type WebhookChannel struct{}

func (c *WebhookChannel) Type() string {
	return ChannelTypeWebhook
}

func (c *WebhookChannel) Send(ctx context.Context, destination json.RawMessage, groups []notifier.EventGroup, outputFormat string) error {
	var dest WebhookDestination
	if err := json.Unmarshal(destination, &dest); err != nil {
		return fmt.Errorf("invalid webhook destination: %w", err)
	}
	if dest.URL == "" {
		return fmt.Errorf("webhook destination requires a url")
	}

	// Handle summary mode for large backlogs.
	if len(groups) > maxWebhookNotifications {
		return c.post(ctx, dest.URL, renderBody(groups, outputFormat, true))
	}

	for _, group := range groups {
		body := renderBody([]notifier.EventGroup{group}, outputFormat, false)
		if err := c.post(ctx, dest.URL, body); err != nil {
			return err
		}
	}
	return nil
}

const maxWebhookNotifications = 10

func renderBody(groups []notifier.EventGroup, format string, summary bool) []byte {
	var payload struct {
		Format string `json:"format"`
		Text   string `json:"text"`
	}
	payload.Format = format

	if summary {
		payload.Text = notifier.RenderSummaryMarkdown(groups)
	} else {
		switch format {
		case notifier.FormatPlaintext:
			payload.Text = notifier.RenderPlaintext(groups)
		default:
			payload.Text = notifier.RenderMarkdown(groups)
		}
	}

	data, _ := json.Marshal(payload)
	return data
}

func (c *WebhookChannel) post(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook delivery: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
