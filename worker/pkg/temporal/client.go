package temporal

import (
	"os"

	"go.temporal.io/sdk/client"
)

const (
	DefaultHostPort  = "localhost:7233"
	DefaultNamespace = "default"
)

// HostPort returns the Temporal server address from env or default.
func HostPort() string {
	if v := os.Getenv("TEMPORAL_HOST_PORT"); v != "" {
		return v
	}
	return DefaultHostPort
}

// Namespace returns the Temporal namespace from env or default.
func Namespace() string {
	if v := os.Getenv("TEMPORAL_NAMESPACE"); v != "" {
		return v
	}
	return DefaultNamespace
}

// NewClient creates a Temporal client using env-based configuration.
func NewClient() (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:  HostPort(),
		Namespace: Namespace(),
	})
}
