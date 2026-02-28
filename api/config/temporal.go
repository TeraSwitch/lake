package config

import (
	"fmt"
	"log"

	temporalclient "go.temporal.io/sdk/client"

	lktemporal "github.com/malbeclabs/lake/worker/pkg/temporal"
)

// TemporalClient is the global Temporal client.
var TemporalClient temporalclient.Client

// LoadTemporal initializes the Temporal client.
func LoadTemporal() error {
	c, err := lktemporal.NewClient()
	if err != nil {
		return fmt.Errorf("temporal client: %w", err)
	}
	TemporalClient = c
	log.Printf("Connected to Temporal: host=%s, namespace=%s", lktemporal.HostPort(), lktemporal.Namespace())
	return nil
}

// CloseTemporal closes the Temporal client.
func CloseTemporal() {
	if TemporalClient != nil {
		TemporalClient.Close()
	}
}
