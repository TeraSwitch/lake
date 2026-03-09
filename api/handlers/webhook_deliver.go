package handlers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"
)

// deliverWebhookHTTP sends an HTTP POST to the webhook URL with HMAC-SHA256 signature.
// Returns the HTTP status code and any error.
func deliverWebhookHTTP(url, secret, eventType string, payload []byte) (int, error) {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DZ-Signature", signature)
	req.Header.Set("X-DZ-Event", eventType)
	req.Header.Set("User-Agent", "DoubleZero-Webhooks/1.0")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(io.LimitReader(resp.Body, 1024))

	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return resp.StatusCode, nil
}
