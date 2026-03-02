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

// PageCacheProvider defines the interface for accessing cached page data.
type PageCacheProvider interface {
	GetStatus() *StatusResponse
	GetLinkHistory(timeRange string, buckets int) *LinkHistoryResponse
	GetDeviceHistory(timeRange string, buckets int) *DeviceHistoryResponse
	GetTimeline() *TimelineResponse
	GetOutages() *LinkOutagesResponse
	GetLatencyComparison() *LatencyComparisonResponse
	GetMetroPathLatency(optimize string) *MetroPathLatencyResponse
}

// Global cache instance
var pageCache PageCacheProvider

// SetPageCache sets the global page cache implementation.
func SetPageCache(cache PageCacheProvider) {
	pageCache = cache
}

func linkHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}

func deviceHistoryCacheKey(timeRange string, buckets int) string {
	return timeRange + ":" + strconv.Itoa(buckets)
}
