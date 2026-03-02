package pagecache

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/malbeclabs/lake/api/handlers"
)

// Activities holds dependencies for page cache activities.
// These run in the API process's embedded worker.
type Activities struct {
	PgPool *pgxpool.Pool
}

func (a *Activities) RefreshStatus(ctx context.Context) error {
	resp := handlers.FetchStatusData(ctx)
	if resp.Error != "" {
		log.Printf("Page cache refresh error: %v", resp.Error)
		return nil // don't fail the activity, just skip
	}
	return a.upsertCache(ctx, "status", resp)
}

func (a *Activities) RefreshTimeline(ctx context.Context) error {
	resp := handlers.FetchDefaultTimelineData(ctx)
	return a.upsertCache(ctx, "timeline", resp)
}

func (a *Activities) RefreshOutages(ctx context.Context) error {
	resp := handlers.FetchDefaultOutagesData(ctx)
	return a.upsertCache(ctx, "outages", resp)
}

func (a *Activities) RefreshLinkHistory(ctx context.Context) error {
	resp, err := handlers.FetchLinkHistoryData(ctx, "24h", 72)
	if err != nil {
		log.Printf("Link history cache refresh error: %v", err)
		return nil
	}
	return a.upsertCache(ctx, "link-history:24h:72", resp)
}

func (a *Activities) RefreshDeviceHistory(ctx context.Context) error {
	resp, err := handlers.FetchDeviceHistoryData(ctx, "24h", 72)
	if err != nil {
		log.Printf("Device history cache refresh error: %v", err)
		return nil
	}
	return a.upsertCache(ctx, "device-history:24h:72", resp)
}

func (a *Activities) RefreshLatencyComparison(ctx context.Context) error {
	resp, err := handlers.FetchLatencyComparisonData(ctx)
	if err != nil {
		log.Printf("Latency comparison cache refresh error: %v", err)
		return nil
	}
	return a.upsertCache(ctx, "latency-comparison", resp)
}

func (a *Activities) RefreshMetroPathLatency(ctx context.Context) error {
	strategies := []string{"latency", "hops", "bandwidth"}
	for _, strategy := range strategies {
		resp, err := handlers.FetchMetroPathLatencyData(ctx, strategy)
		if err != nil {
			log.Printf("Metro path latency cache refresh error (optimize=%s): %v", strategy, err)
			continue
		}
		if err := a.upsertCache(ctx, "metro-path-latency:"+strategy, resp); err != nil {
			return err
		}
	}
	return nil
}

func (a *Activities) RunCleanup(ctx context.Context) error {
	handlers.RunCleanupTasks(ctx)
	return nil
}

func (a *Activities) upsertCache(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = a.PgPool.Exec(ctx,
		`INSERT INTO page_cache (cache_key, data, refreshed_at) VALUES ($1, $2, NOW())
		 ON CONFLICT (cache_key) DO UPDATE SET data = $2, refreshed_at = NOW()`,
		key, data)
	return err
}
