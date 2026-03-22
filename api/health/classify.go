// Package health provides shared health classification functions used by both
// the API handlers and the health bucket worker.
package health

// Latency/loss thresholds for link health classification.
const (
	LatencyWarningPct  = 20.0 // 20% over committed RTT
	LatencyCriticalPct = 50.0 // 50% over committed RTT
	LossWarningPct     = 1.0  // 1% - Moderate (degraded)
	LossCriticalPct    = 10.0 // 10% - Severe (unhealthy)
	UtilWarningPct     = 70.0
	UtilCriticalPct    = 90.0
)

// Device health thresholds.
const (
	DeviceUnhealthyThreshold uint64 = 100
)

// CommittedRttProvisioningNs is the sentinel committed_rtt_ns value (1000ms)
// that indicates a link is still being provisioned and not yet operational.
const CommittedRttProvisioningNs = 1_000_000_000

// ClassifyLinkStatus classifies link health based on latency overage and loss.
// avgLatency is avg RTT in microseconds, committedRttUs is committed RTT in microseconds.
func ClassifyLinkStatus(avgLatency, lossPct, committedRttUs float64) string {
	var latencyOveragePct float64
	if committedRttUs > 0 && avgLatency > 0 {
		latencyOveragePct = ((avgLatency - committedRttUs) / committedRttUs) * 100
	}

	if lossPct >= LossCriticalPct || latencyOveragePct >= LatencyCriticalPct {
		return "unhealthy"
	}
	if lossPct >= LossWarningPct || latencyOveragePct >= LatencyWarningPct {
		return "degraded"
	}
	return "healthy"
}

// ClassifyDeviceStatus classifies device health based on interface counters.
func ClassifyDeviceStatus(totalErrors, totalDiscards, carrierTransitions uint64) string {
	if totalErrors >= DeviceUnhealthyThreshold || totalDiscards >= DeviceUnhealthyThreshold || carrierTransitions >= DeviceUnhealthyThreshold {
		return "unhealthy"
	}
	if totalErrors > 0 || totalDiscards > 0 || carrierTransitions > 0 {
		return "degraded"
	}
	return "healthy"
}

// IsDrainedStatus returns true if the status represents a drained link or device.
func IsDrainedStatus(status string) bool {
	return status == "soft-drained" || status == "hard-drained" || status == "suspended"
}
