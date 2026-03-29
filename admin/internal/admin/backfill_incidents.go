package admin

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/malbeclabs/lake/indexer/pkg/incidents"
	"go.temporal.io/sdk/client"
)

// BackfillIncidentsConfig holds configuration for the backfill-incidents command.
type BackfillIncidentsConfig struct {
	StartTime     time.Time
	EndTime       time.Time
	ChunkInterval time.Duration
	Overwrite     bool
	Clean         bool
}

// BackfillIncidents starts the BackfillIncidentsWorkflow on Temporal.
func BackfillIncidents(log *slog.Logger, cfg BackfillIncidentsConfig) error {
	if cfg.ChunkInterval == 0 {
		cfg.ChunkInterval = 24 * time.Hour
	}

	temporalAddr := os.Getenv("TEMPORAL_HOST_PORT")
	if temporalAddr == "" {
		temporalAddr = "localhost:7233"
	}
	temporalNamespace := os.Getenv("TEMPORAL_NAMESPACE")
	if temporalNamespace == "" {
		temporalNamespace = "default"
	}

	c, err := client.Dial(client.Options{
		HostPort:  temporalAddr,
		Namespace: temporalNamespace,
		Logger:    log,
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	input := incidents.BackfillInput{
		StartTime: cfg.StartTime,
		EndTime:   cfg.EndTime,
		ChunkSize: cfg.ChunkInterval,
		Overwrite: cfg.Overwrite,
		Clean:     cfg.Clean,
	}

	workflowID := fmt.Sprintf("incidents-backfill-%d", time.Now().Unix())
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: incidents.TaskQueue,
	}, incidents.BackfillIncidentsWorkflow, input)
	if err != nil {
		return fmt.Errorf("start backfill workflow: %w", err)
	}

	log.Info("started incidents backfill workflow",
		"workflow_id", run.GetID(),
		"run_id", run.GetRunID(),
		"start", cfg.StartTime.Format(time.RFC3339),
		"end", cfg.EndTime.Format(time.RFC3339),
		"chunk", cfg.ChunkInterval,
		"overwrite", cfg.Overwrite,
		"clean", cfg.Clean,
	)

	log.Info("waiting for completion...")
	if err := run.Get(context.Background(), nil); err != nil {
		return fmt.Errorf("backfill workflow failed: %w", err)
	}

	log.Info("incidents backfill complete")
	return nil
}
