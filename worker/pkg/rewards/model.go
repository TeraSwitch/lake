package rewards

import apirewards "github.com/malbeclabs/lake/api/rewards"

// CompareInput is the input for the Compare workflow.
type CompareInput struct {
	Baseline       apirewards.ShapleyInput    `json:"baseline"`
	Modified       apirewards.ShapleyInput    `json:"modified"`
	CachedBaseline []apirewards.OperatorValue `json:"cached_baseline,omitempty"`
}

// LinkEstimateInput is the input for the LinkEstimate workflow.
type LinkEstimateInput struct {
	Operator       string                     `json:"operator"`
	Network        apirewards.ShapleyInput    `json:"network"`
	TopologyHash   string                     `json:"topology_hash,omitempty"`
	CachedBaseline []apirewards.OperatorValue `json:"cached_baseline,omitempty"`
}

// StoreCacheInput is the input for the StoreCache activity.
type StoreCacheInput struct {
	Epoch        int64                           `json:"epoch"`
	Results      []apirewards.OperatorValue      `json:"results"`
	TotalValue   float64                         `json:"total_value"`
	LiveNetwork  *apirewards.LiveNetworkResponse `json:"live_network"`
	TopologyHash string                          `json:"topology_hash"`
}

// StoreLinkEstimateInput is the input for the StoreLinkEstimate activity.
type StoreLinkEstimateInput struct {
	Operator     string  `json:"operator"`
	TopologyHash string  `json:"topology_hash"`
	Results      []byte  `json:"results"`
	TotalValue   float64 `json:"total_value"`
}
