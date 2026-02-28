package rewards

import "time"

// SimulationRequest is the workflow input.
type SimulationRequest struct {
	ID               string  `json:"id"`
	OperatorUptime   float64 `json:"operator_uptime"`
	ContiguityBonus  float64 `json:"contiguity_bonus"`
	DemandMultiplier float64 `json:"demand_multiplier"`
}

// SimulationResult is the final workflow output stored in PostgreSQL.
type SimulationResult struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}
