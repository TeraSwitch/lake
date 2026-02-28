package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	flag "github.com/spf13/pflag"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	apirewards "github.com/malbeclabs/lake/api/rewards"
	lktemporal "github.com/malbeclabs/lake/worker/pkg/temporal"

	"github.com/malbeclabs/lake/worker/pkg/rewards"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	verboseFlag := flag.Bool("verbose", false, "enable verbose (debug) logging")
	flag.Parse()

	_ = godotenv.Load()
	_ = godotenv.Load("api/.env")

	level := slog.LevelInfo
	if *verboseFlag {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	log.Info("lake-worker starting", "version", version, "commit", commit, "date", date)

	// Signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", "signal", sig.String())
		cancel()
	}()

	// Configure shapley-cli binary path
	if shapleyBin := os.Getenv("SHAPLEY_CLI_PATH"); shapleyBin != "" {
		apirewards.SetBinaryPath(shapleyBin)
	}

	// Initialize ClickHouse
	chAddr := envOrDefault("CLICKHOUSE_ADDR_TCP", "localhost:9100")
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{
			Database: envOrDefault("CLICKHOUSE_DATABASE", "default"),
			Username: envOrDefault("CLICKHOUSE_USERNAME", "default"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
	if err != nil {
		return fmt.Errorf("clickhouse: %w", err)
	}
	if err := chConn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}
	defer chConn.Close()
	log.Info("clickhouse connected", "addr", chAddr)

	// Initialize PostgreSQL
	pgConnStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		envOrDefault("POSTGRES_USER", "lakedev"),
		envOrDefault("POSTGRES_PASSWORD", "lakedev"),
		envOrDefault("POSTGRES_HOST", "localhost"),
		envOrDefault("POSTGRES_PORT", "5432"),
		envOrDefault("POSTGRES_DB", "lakedev"),
		envOrDefault("POSTGRES_SSLMODE", "disable"),
	)
	poolCfg, err := pgxpool.ParseConfig(pgConnStr)
	if err != nil {
		return fmt.Errorf("postgres config: %w", err)
	}
	poolCfg.MaxConns = 5
	poolCfg.MaxConnLifetime = time.Hour

	pgPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("postgres pool: %w", err)
	}
	if err := pgPool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping: %w", err)
	}
	defer pgPool.Close()
	log.Info("postgres connected")

	// Initialize Temporal client
	tc, err := temporalclient.Dial(temporalclient.Options{
		HostPort:  lktemporal.HostPort(),
		Namespace: lktemporal.Namespace(),
		Logger:    newTemporalLogger(log),
	})
	if err != nil {
		return fmt.Errorf("temporal client: %w", err)
	}
	defer tc.Close()
	log.Info("temporal connected", "host", lktemporal.HostPort(), "namespace", lktemporal.Namespace())

	// Create activities with all dependencies
	activities := &rewards.Activities{
		ClickHouse:     chConn,
		PgPool:         pgPool,
		ShapleyBinPath: envOrDefault("SHAPLEY_CLI_PATH", "./shapley-cli/target/release/shapley-cli"),
	}

	// Create and start worker
	w := worker.New(tc, rewards.TaskQueue, worker.Options{})

	// Register rewards workflow domain
	w.RegisterWorkflow(rewards.RewardsSimulation)
	w.RegisterActivity(activities)

	log.Info("starting worker", "task_queue", rewards.TaskQueue)

	if err := w.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("worker: %w", err)
	}

	return nil
}

func envOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// temporalLogger adapts slog to Temporal's log interface.
type temporalLogger struct {
	log *slog.Logger
}

func newTemporalLogger(log *slog.Logger) *temporalLogger {
	return &temporalLogger{log: log.With("component", "temporal")}
}

func (l *temporalLogger) Debug(msg string, keyvals ...any) {
	l.log.Debug(msg, keyvals...)
}

func (l *temporalLogger) Info(msg string, keyvals ...any) {
	l.log.Info(msg, keyvals...)
}

func (l *temporalLogger) Warn(msg string, keyvals ...any) {
	l.log.Warn(msg, keyvals...)
}

func (l *temporalLogger) Error(msg string, keyvals ...any) {
	l.log.Error(msg, keyvals...)
}
