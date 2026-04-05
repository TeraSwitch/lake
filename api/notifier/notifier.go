package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Config configures the notifier worker.
type Config struct {
	Log      *slog.Logger
	PgPool   *pgxpool.Pool
	Sources  map[string]Source
	Channels map[string]Channel
}

// Start connects to Temporal, registers workflows and activities, then runs
// the notifier worker. It blocks until ctx is cancelled or an error occurs.
func Start(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	temporalHost := envOrDefault("TEMPORAL_HOST_PORT", "localhost:7233")
	temporalNS := envOrDefault("TEMPORAL_NAMESPACE", "default")
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  temporalHost,
		Namespace: temporalNS,
		Logger:    newTemporalLogger(log),
	})
	if err != nil {
		return fmt.Errorf("notifier: temporal dial: %w", err)
	}
	defer tc.Close()
	log.Info("notifier: temporal connected", "host", temporalHost, "namespace", temporalNS)

	store := &ConfigStore{Pool: cfg.PgPool}
	activities := &Activities{
		Log:      log.With("component", "notifier"),
		Store:    store,
		Sources:  cfg.Sources,
		Channels: cfg.Channels,
	}

	w := worker.New(tc, TaskQueue, worker.Options{})
	w.RegisterWorkflow(NotifierWorkflow)
	w.RegisterActivity(activities)

	// Terminate any existing workflow from a previous deploy, then start fresh.
	_ = tc.TerminateWorkflow(ctx, WorkflowID, "", "restarting on deploy")
	run, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        WorkflowID,
		TaskQueue: TaskQueue,
	}, NotifierWorkflow, 0)
	if err != nil {
		return fmt.Errorf("notifier: failed to start workflow: %w", err)
	}
	log.Info("notifier: workflow started", "id", WorkflowID)

	go func() {
		if err := run.Get(ctx, nil); err != nil && ctx.Err() == nil && !isWorkflowTerminated(err) {
			log.Error("notifier: workflow failed", "id", WorkflowID, "error", err)
		}
	}()

	log.Info("notifier: starting worker", "task_queue", TaskQueue)

	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(worker.InterruptCh()) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {}
func (l *temporalLogger) Info(msg string, keyvals ...any)  { l.log.Info(msg, keyvals...) }
func (l *temporalLogger) Warn(msg string, keyvals ...any)  { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) { l.log.Error(msg, keyvals...) }

func isWorkflowTerminated(err error) bool {
	return err != nil && strings.Contains(err.Error(), "terminated")
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
