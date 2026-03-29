package incidents

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestActivities() *Activities {
	return &Activities{
		Log:                 slog.Default(),
		CoalesceGap:         30 * time.Minute,
		EscalationThreshold: 30 * time.Minute,
	}
}

func TestDiff_NewIncident(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)

	symptoms := []symptomInput{{
		EntityPK:     "link-1",
		IncidentType: SymptomPacketLoss,
		PeakValue:    15.0,
		StartedAt:    now.Add(-10 * time.Minute),
	}}

	events := a.diff(now, symptoms, nil)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, EventOpened, e.EventType)
	assert.Equal(t, "link-1", e.EntityPK)
	assert.Equal(t, []string{SymptomPacketLoss}, e.ActiveSymptoms)
	assert.Equal(t, []string{SymptomPacketLoss}, e.Symptoms, "all symptoms should equal active on open")
	assert.Equal(t, SeverityWarning, e.Severity)
	assert.Equal(t, now.Add(-10*time.Minute), e.StartedAt)
	assert.NotEmpty(t, e.IncidentID)
}

func TestDiff_NewIncidentMultipleSymptoms(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-5 * time.Minute)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomPacketLoss, PeakValue: 12.0, StartedAt: start},
		{EntityPK: "link-1", IncidentType: SymptomErrors, PeakValue: 5, StartedAt: start.Add(2 * time.Minute)},
	}

	events := a.diff(now, symptoms, nil)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, EventOpened, e.EventType)
	assert.Equal(t, start, e.StartedAt)
	assert.Contains(t, e.ActiveSymptoms, SymptomPacketLoss)
	assert.Contains(t, e.ActiveSymptoms, SymptomErrors)
	assert.Equal(t, e.ActiveSymptoms, e.Symptoms, "all symptoms should equal active on open")
}

func TestDiff_SymptomAdded(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-20 * time.Minute)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomPacketLoss, PeakValue: 12.0, StartedAt: start},
		{EntityPK: "link-1", IncidentType: SymptomCarrier, PeakValue: 3, StartedAt: now.Add(-5 * time.Minute)},
	}

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{SymptomPacketLoss},
		Symptoms:       []string{SymptomPacketLoss},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-30 * time.Second),
	}}

	events := a.diff(now, symptoms, open)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, EventSymptomAdded, e.EventType)
	assert.Equal(t, "abc123", e.IncidentID)
	assert.Contains(t, e.ActiveSymptoms, SymptomPacketLoss)
	assert.Contains(t, e.ActiveSymptoms, SymptomCarrier)
	assert.Contains(t, e.Symptoms, SymptomPacketLoss)
	assert.Contains(t, e.Symptoms, SymptomCarrier)
}

func TestDiff_SymptomResolved(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomPacketLoss, PeakValue: 12.0, StartedAt: start},
	}

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{SymptomCarrier, SymptomPacketLoss},
		Symptoms:       []string{SymptomCarrier, SymptomPacketLoss},
		Severity:       SeverityCritical,
		LastEventTS:    now.Add(-30 * time.Second),
	}}

	events := a.diff(now, symptoms, open)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, EventSymptomResolved, e.EventType)
	assert.Equal(t, "abc123", e.IncidentID)
	assert.Equal(t, []string{SymptomPacketLoss}, e.ActiveSymptoms)
	assert.Equal(t, []string{SymptomCarrier, SymptomPacketLoss}, e.Symptoms, "all symptoms retains resolved symptom")
}

func TestDiff_AllSymptomsCleared(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{SymptomPacketLoss},
		Symptoms:       []string{SymptomPacketLoss},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-30 * time.Second),
	}}

	events := a.diff(now, nil, open)

	require.Len(t, events, 1)
	e := events[0]
	assert.Equal(t, EventSymptomResolved, e.EventType)
	assert.Equal(t, "abc123", e.IncidentID)
	assert.Empty(t, e.ActiveSymptoms)
	assert.Equal(t, []string{SymptomPacketLoss}, e.Symptoms, "all symptoms preserved when active cleared")
}

func TestDiff_ResolvedAfterCoalesceGap(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-2 * time.Hour)

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{},
		Symptoms:       []string{SymptomPacketLoss},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-31 * time.Minute),
	}}

	events := a.diff(now, nil, open)

	require.Len(t, events, 1)
	assert.Equal(t, EventResolved, events[0].EventType)
	assert.Equal(t, "abc123", events[0].IncidentID)
	assert.Equal(t, []string{SymptomPacketLoss}, events[0].Symptoms, "all symptoms preserved on resolve")
}

func TestDiff_NotResolvedWithinCoalesceGap(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-2 * time.Hour)

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{},
		Symptoms:       []string{SymptomPacketLoss},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-10 * time.Minute),
	}}

	events := a.diff(now, nil, open)

	assert.Empty(t, events, "should not resolve within coalesce gap")
}

func TestDiff_SymptomsReturnCancelsPendingResolution(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-2 * time.Hour)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomErrors, PeakValue: 10, StartedAt: now.Add(-3 * time.Minute)},
	}

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{},
		Symptoms:       []string{SymptomPacketLoss},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-15 * time.Minute),
	}}

	events := a.diff(now, symptoms, open)

	require.Len(t, events, 1)
	assert.Equal(t, EventSymptomAdded, events[0].EventType)
	assert.Equal(t, "abc123", events[0].IncidentID)
	assert.Equal(t, []string{SymptomErrors}, events[0].ActiveSymptoms)
	assert.Equal(t, []string{SymptomErrors, SymptomPacketLoss}, events[0].Symptoms, "all symptoms includes previous")
}

func TestDiff_SeverityEscalation(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-45 * time.Minute)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomCarrier, PeakValue: 5, StartedAt: start},
	}

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{SymptomCarrier},
		Symptoms:       []string{SymptomCarrier},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-30 * time.Second),
	}}

	events := a.diff(now, symptoms, open)

	require.Len(t, events, 1)
	assert.Equal(t, SeverityCritical, events[0].Severity)
}

func TestDiff_NoChangeNoEvent(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)
	start := now.Add(-10 * time.Minute)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomPacketLoss, PeakValue: 12.0, StartedAt: start},
	}

	open := []openState{{
		IncidentID:     "abc123",
		EntityPK:       "link-1",
		StartedAt:      start,
		ActiveSymptoms: []string{SymptomPacketLoss},
		Symptoms:       []string{SymptomPacketLoss},
		Severity:       SeverityWarning,
		LastEventTS:    now.Add(-30 * time.Second),
	}}

	events := a.diff(now, symptoms, open)

	assert.Empty(t, events, "no state change should produce no events")
}

func TestDiff_ISISAlwaysCritical(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomISISDown, PeakValue: 1, StartedAt: now.Add(-5 * time.Minute)},
	}

	events := a.diff(now, symptoms, nil)

	require.Len(t, events, 1)
	assert.Equal(t, SeverityCritical, events[0].Severity)
}

func TestDiff_MultipleEntities(t *testing.T) {
	t.Parallel()
	a := newTestActivities()
	now := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)

	symptoms := []symptomInput{
		{EntityPK: "link-1", IncidentType: SymptomPacketLoss, PeakValue: 12.0, StartedAt: now.Add(-5 * time.Minute)},
		{EntityPK: "link-2", IncidentType: SymptomCarrier, PeakValue: 2, StartedAt: now.Add(-3 * time.Minute)},
	}

	events := a.diff(now, symptoms, nil)

	assert.Len(t, events, 2)
	for _, e := range events {
		assert.Equal(t, EventOpened, e.EventType)
	}
}

func TestGenerateIncidentID_Deterministic(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC)

	id1 := generateIncidentID("link-1", ts)
	id2 := generateIncidentID("link-1", ts)
	id3 := generateIncidentID("link-2", ts)

	assert.Equal(t, id1, id2, "same inputs should produce same ID")
	assert.NotEqual(t, id1, id3, "different entity should produce different ID")
	assert.Len(t, id1, 16, "ID should be 16 hex chars")
}

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
