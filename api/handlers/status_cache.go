package handlers

import (
	"strconv"
)

// Link history configuration to pre-cache (default only)
var linkHistoryConfigs = []struct {
	timeRange string
	buckets   int
}{
	{"24h", 72}, // 24-hour view (default)
}

// Device history configuration to pre-cache (default only)
var deviceHistoryConfigs = []struct {
	timeRange string
	buckets   int
}{
	{"24h", 72}, // 24-hour view (default)
}

// StatusCacheProvider defines the interface for accessing cached status data.
type StatusCacheProvider interface {
	IsReady() bool
	GetStatus() *StatusResponse
	GetLinkHistory(timeRange string, buckets int) *LinkHistoryResponse
	GetDeviceHistory(timeRange string, buckets int) *DeviceHistoryResponse
	GetTimeline() *TimelineResponse
	GetOutages() *LinkOutagesResponse
	GetLatencyComparison() *LatencyComparisonResponse
	GetMetroPathLatency(optimize string) *MetroPathLatencyResponse
}

// Global cache instance
var statusCache StatusCacheProvider

// SetStatusCache sets the global status cache implementation.
func SetStatusCache(cache StatusCacheProvider) {
	statusCache = cache
}

// IsStatusCacheReady returns true if the status cache is initialized and populated.
func IsStatusCacheReady() bool {
	return statusCache != nil && statusCache.IsReady()
}

func linkHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}

func deviceHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}
