package handlers

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const pgCacheTTL = 5 * time.Second

// PGStatusCache provides a PG-backed status cache with a thin in-memory TTL layer.
// Temporal schedules refresh the PG table; this cache reads from PG and keeps
// results in memory for pgCacheTTL to avoid repeated PG reads on every request.
type PGStatusCache struct {
	pool *pgxpool.Pool

	mu    sync.RWMutex
	cache map[string]*pgCacheEntry
}

type pgCacheEntry struct {
	data      json.RawMessage
	fetchedAt time.Time
}

// NewPGStatusCache creates a new PG-backed status cache.
func NewPGStatusCache(pool *pgxpool.Pool) *PGStatusCache {
	return &PGStatusCache{
		pool:  pool,
		cache: make(map[string]*pgCacheEntry),
	}
}

// get retrieves a cached value, reading from PG if the in-memory entry is stale.
func (c *PGStatusCache) get(key string, dest any) bool {
	// Check in-memory cache first
	c.mu.RLock()
	entry := c.cache[key]
	c.mu.RUnlock()

	if entry != nil && time.Since(entry.fetchedAt) < pgCacheTTL {
		return json.Unmarshal(entry.data, dest) == nil
	}

	// Read from PG
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var data json.RawMessage
	err := c.pool.QueryRow(ctx,
		`SELECT data FROM status_cache WHERE cache_key = $1`, key).Scan(&data)
	if err != nil {
		return false
	}

	// Update in-memory cache
	c.mu.Lock()
	c.cache[key] = &pgCacheEntry{data: data, fetchedAt: time.Now()}
	c.mu.Unlock()

	return json.Unmarshal(data, dest) == nil
}

// IsReady returns true if the required cache keys exist in PG.
func (c *PGStatusCache) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := c.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM status_cache WHERE cache_key IN ('status', 'timeline', 'outages')`).Scan(&count)
	if err != nil {
		return false
	}
	return count >= 3
}

// GetStatus returns the cached status response.
func (c *PGStatusCache) GetStatus() *StatusResponse {
	var resp StatusResponse
	if c.get("status", &resp) {
		return &resp
	}
	return nil
}

// GetLinkHistory returns the cached link history response.
func (c *PGStatusCache) GetLinkHistory(timeRange string, buckets int) *LinkHistoryResponse {
	var resp LinkHistoryResponse
	if c.get("link-history:"+linkHistoryCacheKey(timeRange, buckets), &resp) {
		return &resp
	}
	return nil
}

// GetDeviceHistory returns the cached device history response.
func (c *PGStatusCache) GetDeviceHistory(timeRange string, buckets int) *DeviceHistoryResponse {
	var resp DeviceHistoryResponse
	if c.get("device-history:"+deviceHistoryCacheKey(timeRange, buckets), &resp) {
		return &resp
	}
	return nil
}

// GetTimeline returns the cached default timeline response.
func (c *PGStatusCache) GetTimeline() *TimelineResponse {
	var resp TimelineResponse
	if c.get("timeline", &resp) {
		return &resp
	}
	return nil
}

// GetOutages returns the cached default outages response.
func (c *PGStatusCache) GetOutages() *LinkOutagesResponse {
	var resp LinkOutagesResponse
	if c.get("outages", &resp) {
		return &resp
	}
	return nil
}

// GetLatencyComparison returns the cached latency comparison response.
func (c *PGStatusCache) GetLatencyComparison() *LatencyComparisonResponse {
	var resp LatencyComparisonResponse
	if c.get("latency-comparison", &resp) {
		return &resp
	}
	return nil
}

// GetMetroPathLatency returns the cached metro path latency for the given strategy.
func (c *PGStatusCache) GetMetroPathLatency(optimize string) *MetroPathLatencyResponse {
	var resp MetroPathLatencyResponse
	if c.get("metro-path-latency:"+optimize, &resp) {
		return &resp
	}
	return nil
}

// WarmCache performs a synchronous refresh of all cache entries directly (without Temporal).
// Used during startup to ensure cache is warm before serving traffic.
func (c *PGStatusCache) WarmCache(ctx context.Context) {
	log.Println("Warming PG status cache...")
	start := time.Now()

	// Refresh status
	if resp := FetchStatusData(ctx); resp.Error == "" {
		c.upsert(ctx, "status", resp)
	}

	// Refresh timeline
	if resp := FetchDefaultTimelineData(ctx); ctx.Err() == nil {
		c.upsert(ctx, "timeline", resp)
	}

	// Refresh outages
	if resp := FetchDefaultOutagesData(ctx); ctx.Err() == nil {
		c.upsert(ctx, "outages", resp)
	}

	// Refresh link history (default config)
	for _, cfg := range linkHistoryConfigs {
		if resp, err := FetchLinkHistoryData(ctx, cfg.timeRange, cfg.buckets); err == nil {
			c.upsert(ctx, "link-history:"+linkHistoryCacheKey(cfg.timeRange, cfg.buckets), resp)
		}
	}

	// Refresh device history (default config)
	for _, cfg := range deviceHistoryConfigs {
		if resp, err := FetchDeviceHistoryData(ctx, cfg.timeRange, cfg.buckets); err == nil {
			c.upsert(ctx, "device-history:"+deviceHistoryCacheKey(cfg.timeRange, cfg.buckets), resp)
		}
	}

	// Refresh latency comparison
	if resp, err := FetchLatencyComparisonData(ctx); err == nil {
		c.upsert(ctx, "latency-comparison", resp)
	}

	// Refresh metro path latency
	for _, strategy := range []string{"latency", "hops", "bandwidth"} {
		if resp, err := FetchMetroPathLatencyData(ctx, strategy); err == nil {
			c.upsert(ctx, "metro-path-latency:"+strategy, resp)
		}
	}

	log.Printf("PG status cache warmed in %v", time.Since(start))
}

func (c *PGStatusCache) upsert(ctx context.Context, key string, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		log.Printf("Failed to marshal cache value for key %s: %v", key, err)
		return
	}

	_, err = c.pool.Exec(ctx,
		`INSERT INTO status_cache (cache_key, data, refreshed_at) VALUES ($1, $2, NOW())
		 ON CONFLICT (cache_key) DO UPDATE SET data = $2, refreshed_at = NOW()`,
		key, data)
	if err != nil {
		log.Printf("Failed to upsert cache key %s: %v", key, err)
	}
}
