package incidents

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSymptomSeverity(t *testing.T) {
	t.Parallel()
	threshold := 30 * time.Minute
	short := 10 * time.Minute
	long := 45 * time.Minute

	tests := []struct {
		name     string
		symptom  string
		peak     float64
		duration time.Duration
		expected Severity
	}{
		{"isis_down short", SymptomISISDown, 1, short, SeverityCritical},
		{"isis_down long", SymptomISISDown, 1, long, SeverityCritical},
		{"high loss short", SymptomPacketLoss, 15, short, SeverityWarning},
		{"high loss long", SymptomPacketLoss, 15, long, SeverityCritical},
		{"low loss short", SymptomPacketLoss, 5, short, SeverityWarning},
		{"low loss long", SymptomPacketLoss, 5, long, SeverityWarning},
		{"carrier short", SymptomCarrier, 3, short, SeverityWarning},
		{"carrier long", SymptomCarrier, 3, long, SeverityCritical},
		{"errors short", SymptomErrors, 10, short, SeverityWarning},
		{"errors long", SymptomErrors, 10, long, SeverityWarning},
		{"discards long", SymptomDiscards, 10, long, SeverityWarning},
		{"no_latency_data short", SymptomNoLatencyData, 1, short, SeverityWarning},
		{"no_latency_data long", SymptomNoLatencyData, 1, long, SeverityCritical},
		{"no_traffic_data short", SymptomNoTrafficData, 1, short, SeverityWarning},
		{"no_traffic_data long", SymptomNoTrafficData, 1, long, SeverityCritical},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := symptomSeverity(tt.symptom, tt.peak, tt.duration, threshold)
			assert.Equal(t, tt.expected, got)
		})
	}
}
