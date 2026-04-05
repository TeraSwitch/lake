package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

const maxGroupsPerDelivery = 10

// Deliver sends event groups to a webhook endpoint. If there are more than
// maxGroupsPerDelivery groups, a summary is sent instead.
func Deliver(ctx context.Context, url string, groups []EventGroup, outputFormat string) error {
	if len(groups) > maxGroupsPerDelivery {
		return post(ctx, url, renderBody(groups, outputFormat, true))
	}

	for _, group := range groups {
		body := renderBody([]EventGroup{group}, outputFormat, false)
		if err := post(ctx, url, body); err != nil {
			return err
		}
	}
	return nil
}

func renderBody(groups []EventGroup, format string, summary bool) []byte {
	var payload struct {
		Format string `json:"format"`
		Text   string `json:"text"`
	}
	payload.Format = format

	if summary {
		payload.Text = RenderSummaryMarkdown(groups)
	} else {
		switch format {
		case FormatPlaintext:
			payload.Text = RenderPlaintext(groups)
		default:
			payload.Text = RenderMarkdown(groups)
		}
	}

	data, _ := json.Marshal(payload)
	return data
}

func post(ctx context.Context, url string, body []byte) error {
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
