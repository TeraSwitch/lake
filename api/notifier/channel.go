package notifier

import (
	"context"
	"encoding/json"
)

// Channel delivers notifications to an external destination.
type Channel interface {
	// Type returns the channel type identifier (e.g. "slack", "webhook").
	Type() string

	// Send delivers event groups to the destination. The outputFormat determines
	// how groups are rendered (e.g. "markdown", "plaintext", "blocks").
	Send(ctx context.Context, destination json.RawMessage, groups []EventGroup, outputFormat string) error
}
