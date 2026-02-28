package config

import (
	"log"

	temporalclient "go.temporal.io/sdk/client"

	lktemporal "github.com/malbeclabs/lake/worker/pkg/temporal"
)

// TemporalClient is the global Temporal client (nil if unavailable).
var TemporalClient temporalclient.Client

// LoadTemporal initializes the Temporal client.
// It logs a warning and returns nil error if the connection fails,
// so the API can still start without Temporal.
func LoadTemporal() error {
	c, err := lktemporal.NewClient()
	if err != nil {
		log.Printf("Warning: Temporal not available: %v", err)
		return nil
	}
	TemporalClient = c
	log.Printf("Connected to Temporal: host=%s, namespace=%s", lktemporal.HostPort(), lktemporal.Namespace())
	return nil
}

// CloseTemporal closes the Temporal client if it was initialized.
func CloseTemporal() {
	if TemporalClient != nil {
		TemporalClient.Close()
	}
}
