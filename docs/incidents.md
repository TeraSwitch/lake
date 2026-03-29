# Incident Model

This document describes how DoubleZero Data detects, tracks, and classifies network incidents.

## What is an incident?

An incident represents a period where something is wrong with a network entity (a link or a device). A single incident can involve multiple concurrent symptoms — for example, a link might have both packet loss and carrier transitions at the same time. These are tracked as symptoms within one incident rather than as separate incidents.

An incident is uniquely identified by the entity it affects and when it started. The start time is pinned when the first symptom is detected and never changes, even if additional symptoms appear or resolve during the incident.

## Symptoms

A symptom is a specific type of anomaly detected on an entity. Each symptom is detected independently from 5-minute rollup data.

### Link symptoms

| Symptom | Trigger | Description |
|---|---|---|
| `packet_loss` | Loss > 0% on either direction | Any packet loss is anomalous on the DZ network |
| `isis_down` | IS-IS adjacency marked deleted | Link is out of the routing topology |
| `no_latency_data` | No rows in link_rollup_5m in lookback window | Latency probes have stopped |
| `no_traffic_data` | No rows in device_interface_rollup_5m for link interfaces | Interface counter collection has stopped |
| `errors` | in_errors + out_errors ≥ 1 | Interface error counters |
| `fcs` | in_fcs_errors ≥ 1 | Frame check sequence errors (layer 1/2 issue) |
| `discards` | in_discards + out_discards ≥ 1 | Interface discard counters |
| `carrier` | carrier_transitions ≥ 1 | Interface carrier state changes |

### Device symptoms

| Symptom | Trigger | Description |
|---|---|---|
| `errors` | in_errors + out_errors ≥ 1 | Interface error counters (non-link interfaces) |
| `fcs` | in_fcs_errors ≥ 1 | Frame check sequence errors |
| `discards` | in_discards + out_discards ≥ 1 | Interface discard counters |
| `carrier` | carrier_transitions ≥ 1 | Interface carrier state changes |
| `no_latency_data` | No rows in link_rollup_5m for any of the device's links | Device is not sending latency probes |
| `no_traffic_data` | No rows in device_interface_rollup_5m for non-link interfaces | Device is not reporting interface counters |
| `isis_overload` | IS-IS overload bit set | Device is signaling IS-IS overload |
| `isis_unreachable` | IS-IS unreachable | Device is unreachable via IS-IS |

## Severity

Incidents have two severity levels: **critical** and **warning**. Severity is determined by the combination of symptom type and duration.

| Symptom | Short-lived (< 30 min) | Sustained (≥ 30 min) |
|---|---|---|
| `isis_down` / `isis_overload` / `isis_unreachable` | critical | critical |
| `packet_loss` ≥ 10% | warning | critical |
| `packet_loss` > 0% < 10% | warning | warning |
| `carrier` | warning | critical |
| `fcs` | warning | critical |
| `no_latency_data` | warning | critical |
| `no_traffic_data` | warning | critical |
| `errors` | warning | warning |
| `discards` | warning | warning |

IS-IS symptoms are always critical because they indicate the entity is out of the routing topology — even briefly, this is operationally significant.

The incident's overall severity is the maximum across all active symptoms. Severity can upgrade over time as symptoms persist past the escalation threshold (default: 30 minutes).

## Lifecycle

An incident progresses through these events:

1. **opened** — First symptom detected. The incident start time is pinned.
2. **symptom_added** — A new symptom type appears on the same entity while the incident is active.
3. **symptom_resolved** — A symptom type clears (no longer detected in rollup data).
4. **resolved** — All symptoms have been clear for the coalesce gap (default: 30 minutes).

The coalesce gap prevents flapping — if a symptom briefly clears and returns within 30 minutes, it remains part of the same incident rather than creating a new one.

### Example timeline

```
10:00  opened          symptoms: [packet_loss]           severity: warning
10:05  symptom_added   symptoms: [carrier, packet_loss]  severity: warning
10:30  (severity escalates due to duration)              severity: critical
10:45  symptom_resolved symptoms: [packet_loss]          severity: critical
11:00  symptom_resolved symptoms: []                     severity: critical
11:30  resolved         symptoms: []                     severity: critical
```

## Expected-to-report entities

No-data symptoms apply to entities that have recently reported data but stopped. An entity is considered "expected to report" if it produced rollup data in the last 24 hours but has none in the last 15 minutes. This avoids flagging entities that have never reported (e.g., freshly provisioned links).

## Detection

Incidents are detected by a background Temporal workflow that runs every 30 seconds. Each cycle:

1. Queries the ClickHouse rollup tables (`link_rollup_5m`, `device_interface_rollup_5m`) for symptoms active in the last 15 minutes
2. Compares against the known set of open incidents (from the `incident_events` table)
3. Writes events for any state transitions (new symptoms, cleared symptoms, severity changes)

The detection workflow queries rollup tables directly rather than using the `link_incidents_v` / `device_incidents_v` views. This allows independent thresholds (e.g., detecting any packet loss > 0%) and keeps the query lightweight since it only scans the most recent data.

Events are stored in separate ClickHouse tables per entity kind — `link_incident_events` and `device_incident_events` — as append-only logs with typed metadata columns. The full history of an incident can be reconstructed by reading all events for a given incident ID.

## Backfill

The detection workflow only tracks incidents going forward from when it starts. To populate historical incidents from existing rollup data, use the backfill command:

```bash
# Backfill the last 7 days of incidents
go run ./admin/cmd/admin --start-backfill-incidents --start-time-ago 168h

# Backfill a specific time range
go run ./admin/cmd/admin --start-backfill-incidents \
  --start-time 2026-03-01T00:00:00Z \
  --end-time 2026-03-28T00:00:00Z

# Overwrite existing events in the range
go run ./admin/cmd/admin --start-backfill-incidents \
  --start-time-ago 168h \
  --backfill-incidents-overwrite
```

The backfill processes data in day-sized chunks (configurable via `--chunk-interval`) and generates simplified events — just `opened` and `resolved` per incident, without intermediate symptom transitions. It uses the same gap-and-island analysis and coalesce gap as the live detection.

Rollup data must exist before backfilling incidents. If needed, backfill rollups first:

```bash
go run ./admin/cmd/admin --start-backfill-rollup --start-time-ago 168h --source-database lake
```

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `--incidents-coalesce-gap` | 30m | How long all symptoms must clear before resolved |
| `--incidents-escalation-threshold` | 30m | How long a symptom must persist to escalate severity |
| `--no-incidents` | false | Disable the incidents detection worker |
