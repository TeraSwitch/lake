package incidents

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Config configures the incidents detection worker.
type Config struct {
	Log *slog.Logger

	// ClickHouse connection parameters.
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
	ClickHouseSecure   bool

	// CoalesceGap is how long all symptoms must be clear before an incident
	// is considered resolved. Default: 30 minutes.
	CoalesceGap time.Duration

	// EscalationThreshold is how long a symptom must persist before it can
	// escalate from warning to critical severity. Default: 30 minutes.
	EscalationThreshold time.Duration
}

// Start connects to ClickHouse and Temporal, then begins processing incident
// detection workflows. It blocks until ctx is cancelled or an error occurs.
func Start(ctx context.Context, cfg Config) error {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	if cfg.CoalesceGap == 0 {
		cfg.CoalesceGap = 30 * time.Minute
	}
	if cfg.EscalationThreshold == 0 {
		cfg.EscalationThreshold = 30 * time.Minute
	}

	// Open a dedicated ClickHouse connection for the incidents worker.
	chOpts := &clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUsername,
			Password: cfg.ClickHousePassword,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 120,
		},
		DialTimeout: 5 * time.Second,
	}
	if cfg.ClickHouseSecure {
		chOpts.TLS = &tls.Config{}
	}

	chConn, err := clickhouse.Open(chOpts)
	if err != nil {
		return fmt.Errorf("incidents: clickhouse open: %w", err)
	}
	if err := chConn.Ping(ctx); err != nil {
		chConn.Close()
		return fmt.Errorf("incidents: clickhouse ping: %w", err)
	}
	defer chConn.Close()
	log.Info("incidents: clickhouse connected", "addr", cfg.ClickHouseAddr, "database", cfg.ClickHouseDatabase)

	// Connect to Temporal
	temporalHost := envOrDefault("TEMPORAL_HOST_PORT", "localhost:7233")
	temporalNS := envOrDefault("TEMPORAL_NAMESPACE", "default")
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  temporalHost,
		Namespace: temporalNS,
		Logger:    newTemporalLogger(log),
	})
	if err != nil {
		return fmt.Errorf("incidents: temporal dial: %w", err)
	}
	defer tc.Close()
	log.Info("incidents: temporal connected", "host", temporalHost, "namespace", temporalNS)

	// Register workflows and activities
	activities := &Activities{
		ClickHouse:          chConn,
		Log:                 log.With("component", "incidents"),
		CoalesceGap:         cfg.CoalesceGap,
		EscalationThreshold: cfg.EscalationThreshold,
	}

	w := worker.New(tc, TaskQueue, worker.Options{})
	RegisterWorkflows(w)
	w.RegisterActivity(activities)

	// Terminate any existing workflow from a previous deploy, then start fresh.
	_ = tc.TerminateWorkflow(ctx, WorkflowID, "", "restarting on deploy")
	run, err := tc.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        WorkflowID,
		TaskQueue: TaskQueue,
	}, DetectIncidentsWorkflow, 0)
	if err != nil {
		return fmt.Errorf("incidents: failed to start workflow: %w", err)
	}
	log.Info("incidents: workflow started", "id", WorkflowID,
		"coalesce_gap", cfg.CoalesceGap,
		"escalation_threshold", cfg.EscalationThreshold)

	// Watch the workflow in the background so failures surface in logs.
	go func() {
		if err := run.Get(ctx, nil); err != nil && ctx.Err() == nil {
			log.Error("incidents: workflow failed", "id", WorkflowID, "error", err)
		}
	}()

	log.Info("incidents: starting worker", "task_queue", TaskQueue)

	// Run blocks until ctx is cancelled or worker error
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(worker.InterruptCh()) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(_ string, _ ...any)         {} // no-op to avoid noisy workflow logs
func (l *temporalLogger) Info(msg string, keyvals ...any)  { l.log.Info(msg, keyvals...) }
func (l *temporalLogger) Warn(msg string, keyvals ...any)  { l.log.Warn(msg, keyvals...) }
func (l *temporalLogger) Error(msg string, keyvals ...any) { l.log.Error(msg, keyvals...) }

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
