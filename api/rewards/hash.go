package rewards

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
)

// TopologyHash computes a deterministic SHA-256 hash of the full ShapleyInput.
// All slices are sorted before marshaling to ensure stable output regardless of
// the order data arrives from ClickHouse.
//
// Demands are included because they directly affect the Shapley LP — validator
// counts set flow requirements and leader slots weight the objective function.
// The 5-minute demandCache in FetchLiveNetwork keeps the hash stable within
// that window.
func TopologyHash(input ShapleyInput) string {
	sorted := input

	// Sort devices by code
	sorted.Devices = make([]Device, len(input.Devices))
	copy(sorted.Devices, input.Devices)
	sort.Slice(sorted.Devices, func(i, j int) bool {
		return sorted.Devices[i].Device < sorted.Devices[j].Device
	})

	// Sort private links by device1+device2
	sorted.PrivateLinks = make([]PrivateLink, len(input.PrivateLinks))
	copy(sorted.PrivateLinks, input.PrivateLinks)
	sort.Slice(sorted.PrivateLinks, func(i, j int) bool {
		if sorted.PrivateLinks[i].Device1 != sorted.PrivateLinks[j].Device1 {
			return sorted.PrivateLinks[i].Device1 < sorted.PrivateLinks[j].Device1
		}
		return sorted.PrivateLinks[i].Device2 < sorted.PrivateLinks[j].Device2
	})

	// Sort public links by city1+city2
	sorted.PublicLinks = make([]PublicLink, len(input.PublicLinks))
	copy(sorted.PublicLinks, input.PublicLinks)
	sort.Slice(sorted.PublicLinks, func(i, j int) bool {
		if sorted.PublicLinks[i].City1 != sorted.PublicLinks[j].City1 {
			return sorted.PublicLinks[i].City1 < sorted.PublicLinks[j].City1
		}
		return sorted.PublicLinks[i].City2 < sorted.PublicLinks[j].City2
	})

	// Sort demands by start+end+type
	sorted.Demands = make([]Demand, len(input.Demands))
	copy(sorted.Demands, input.Demands)
	sort.Slice(sorted.Demands, func(i, j int) bool {
		if sorted.Demands[i].Start != sorted.Demands[j].Start {
			return sorted.Demands[i].Start < sorted.Demands[j].Start
		}
		if sorted.Demands[i].End != sorted.Demands[j].End {
			return sorted.Demands[i].End < sorted.Demands[j].End
		}
		return sorted.Demands[i].Type < sorted.Demands[j].Type
	})

	data, _ := json.Marshal(sorted)
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}
