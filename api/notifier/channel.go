package notifier

import (
	"context"
	"encoding/json"
)

// Channel delivers notifications to an external destination.
type Channel interface {
	// Type returns the channel type identifier (e.g. "slack", "webhook").
	Type() string

	// Send delivers event groups to the destination described by the JSON config.
	Send(ctx context.Context, destination json.RawMessage, groups []EventGroup) error
}
