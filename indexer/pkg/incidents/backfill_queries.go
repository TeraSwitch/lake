package incidents

// linkSymptomWindowsQuery returns per-symptom-window rows for each incident,
// using the same CTE chain as the original backfill but SELECT-ing into Go
// instead of INSERT-ing directly. Each row represents one coalesced symptom
// window mapped to its parent incident.
const linkSymptomWindowsQuery = `
	WITH
	above AS (
		SELECT r.link_pk, r.bucket_ts,
			greatest(r.a_loss_pct, r.z_loss_pct) AS peak_value,
			'packet_loss' AS symptom
		FROM link_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND greatest(r.a_loss_pct, r.z_loss_pct) > 0

		UNION ALL

		SELECT r.link_pk, r.bucket_ts,
			toFloat64(1) AS peak_value,
			'isis_down' AS symptom
		FROM link_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.isis_down = true

		UNION ALL

		SELECT r.link_pk, r.bucket_ts,
			toFloat64(sum(r.in_errors + r.out_errors)) AS peak_value,
			'errors' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk != ''
		GROUP BY r.link_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.link_pk, r.bucket_ts,
			toFloat64(sum(r.in_fcs_errors)) AS peak_value,
			'fcs' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk != ''
		GROUP BY r.link_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.link_pk, r.bucket_ts,
			toFloat64(sum(r.in_discards + r.out_discards)) AS peak_value,
			'discards' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk != ''
		GROUP BY r.link_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.link_pk, r.bucket_ts,
			toFloat64(sum(r.carrier_transitions)) AS peak_value,
			'carrier' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk != ''
		GROUP BY r.link_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT al.link_pk, e.bucket_ts,
			toFloat64(1) AS peak_value,
			'no_latency_data' AS symptom
		FROM (
			SELECT DISTINCT link_pk FROM link_rollup_5m FINAL
			WHERE bucket_ts >= $1 - INTERVAL 24 HOUR AND bucket_ts < $1
		) al
		CROSS JOIN (
			SELECT toStartOfFiveMinutes(toDateTime($1)) + number * 300 AS bucket_ts
			FROM numbers(toUInt64(greatest(0, dateDiff('second', toDateTime($1), toDateTime($2) - INTERVAL 30 MINUTE)) / 300))
		) e
		WHERE (al.link_pk, e.bucket_ts) NOT IN (
			SELECT link_pk, bucket_ts FROM link_rollup_5m FINAL
			WHERE bucket_ts >= toStartOfFiveMinutes(toDateTime($1)) AND bucket_ts < $2
		)

		UNION ALL

		SELECT al.link_pk, e.bucket_ts,
			toFloat64(1) AS peak_value,
			'no_traffic_data' AS symptom
		FROM (
			SELECT DISTINCT link_pk FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= $1 - INTERVAL 24 HOUR AND bucket_ts < $1
			  AND link_pk != ''
		) al
		CROSS JOIN (
			SELECT toStartOfFiveMinutes(toDateTime($1)) + number * 300 AS bucket_ts
			FROM numbers(toUInt64(greatest(0, dateDiff('second', toDateTime($1), toDateTime($2) - INTERVAL 30 MINUTE)) / 300))
		) e
		WHERE (al.link_pk, e.bucket_ts) NOT IN (
			SELECT link_pk, bucket_ts FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= toStartOfFiveMinutes(toDateTime($1)) AND bucket_ts < $2
			  AND link_pk != ''
		)
	),

	islands AS (
		SELECT link_pk, symptom, bucket_ts, peak_value,
			bucket_ts - toIntervalSecond(
				row_number() OVER (PARTITION BY link_pk, symptom ORDER BY bucket_ts) * 300
			) AS island_grp
		FROM above
	),

	raw_windows AS (
		SELECT link_pk, symptom, island_grp,
			min(bucket_ts) AS started_at,
			max(bucket_ts) + toIntervalSecond(300) AS ended_at,
			max(peak_value) AS peak_value
		FROM islands
		GROUP BY link_pk, symptom, island_grp
	),

	numbered AS (
		SELECT *,
			lagInFrame(ended_at) OVER (
				PARTITION BY link_pk, symptom ORDER BY started_at
			) AS prev_ended_at
		FROM raw_windows
	),
	coalesce_groups AS (
		SELECT *,
			sum(if(prev_ended_at IS NULL
				OR dateDiff('minute', prev_ended_at, started_at) >= $3, 1, 0))
				OVER (PARTITION BY link_pk, symptom ORDER BY started_at) AS coalesce_grp
		FROM numbered
	),
	coalesced AS (
		SELECT link_pk, symptom,
			min(started_at) AS started_at,
			max(ended_at) AS ended_at,
			max(peak_value) AS peak_value
		FROM coalesce_groups
		GROUP BY link_pk, symptom, coalesce_grp
	),

	existing_open AS (
		SELECT ie.incident_id AS existing_id, ie.link_pk, ie.started_at AS existing_start, ie.event_ts
		FROM link_incident_events ie
		INNER JOIN (
			SELECT incident_id, max(event_ts) AS max_ts
			FROM link_incident_events GROUP BY incident_id
		) latest ON ie.incident_id = latest.incident_id AND ie.event_ts = latest.max_ts
		WHERE ie.event_type != 'resolved'
	),

	entity_windows AS (
		SELECT link_pk, started_at, ended_at, symptom, peak_value
		FROM coalesced

		UNION ALL

		SELECT eo.link_pk, eo.existing_start AS started_at, toDateTime($1) AS ended_at,
			'_anchor' AS symptom, toFloat64(0) AS peak_value
		FROM existing_open eo
	),
	entity_numbered AS (
		SELECT *,
			lagInFrame(ended_at) OVER (
				PARTITION BY link_pk ORDER BY started_at
			) AS prev_ended
		FROM entity_windows
	),
	incident_groups AS (
		SELECT *,
			sum(if(prev_ended IS NULL
				OR dateDiff('minute', prev_ended, started_at) >= $3, 1, 0))
				OVER (PARTITION BY link_pk ORDER BY started_at) AS inc_grp
		FROM entity_numbered
	),
	incidents_raw AS (
		SELECT
			link_pk,
			inc_grp,
			min(started_at) AS incident_start,
			max(ended_at) AS incident_end,
			arrayDistinct(arrayFilter(x -> x != '_anchor', groupArray(symptom))) AS symptoms
		FROM incident_groups
		GROUP BY link_pk, inc_grp
		HAVING length(symptoms) > 0
	),
	incidents AS (
		SELECT i.*, eo.existing_id, eo.existing_start
		FROM incidents_raw i
		LEFT JOIN existing_open eo ON i.link_pk = eo.link_pk
			AND i.incident_start = eo.existing_start
	),
	incidents_enriched AS (
		SELECT
			i.link_pk, i.inc_grp, i.incident_start, i.incident_end, i.symptoms,
			if(i.existing_id != '',
				i.existing_id,
				lower(hex(SHA256(concat(i.link_pk, '|', toString(toUnixTimestamp(i.incident_start))))))
			) AS incident_id,
			i.existing_id != '' AS is_existing,
			COALESCE(l.code, '') AS link_code,
			COALESCE(l.link_type, '') AS link_type,
			COALESCE(ma.code, '') AS side_a_metro,
			COALESCE(mz.code, '') AS side_z_metro,
			COALESCE(c.code, '') AS contributor_code,
			COALESCE(l.status, '') AS entity_status,
			l.committed_rtt_ns = 1000000000 AS is_provisioning
		FROM incidents i
		LEFT JOIN dz_links_current l ON i.link_pk = l.pk
		LEFT JOIN dz_devices_current da ON l.side_a_pk = da.pk
		LEFT JOIN dz_devices_current dz ON l.side_z_pk = dz.pk
		LEFT JOIN dz_metros_current ma ON da.metro_pk = ma.pk
		LEFT JOIN dz_metros_current mz ON dz.metro_pk = mz.pk
		LEFT JOIN dz_contributors_current c ON l.contributor_pk = c.pk
	),

	-- Map each raw (pre-coalesce) symptom window to its parent incident.
	-- Using raw_windows instead of coalesced preserves the individual
	-- symptom spikes so the backfill generates intermediate events that
	-- match what the live detector would produce.
	symptom_windows AS (
		SELECT
			ie.incident_id,
			rw.link_pk,
			rw.symptom,
			rw.started_at AS sw_started_at,
			rw.ended_at AS sw_ended_at,
			rw.peak_value,
			ie.incident_start,
			ie.incident_end,
			ie.is_existing,
			ie.link_code, ie.link_type, ie.side_a_metro, ie.side_z_metro,
			ie.contributor_code, ie.entity_status, ie.is_provisioning
		FROM raw_windows rw
		INNER JOIN incidents_enriched ie
			ON rw.link_pk = ie.link_pk
			AND rw.started_at >= ie.incident_start
			AND rw.started_at < ie.incident_end
	)

	SELECT
		incident_id, link_pk, symptom,
		sw_started_at, sw_ended_at, peak_value,
		incident_start, incident_end, is_existing,
		link_code, link_type, side_a_metro, side_z_metro,
		contributor_code, entity_status, is_provisioning
	FROM symptom_windows
	ORDER BY incident_id, sw_started_at
`

// deviceSymptomWindowsQuery is the device equivalent of linkSymptomWindowsQuery.
const deviceSymptomWindowsQuery = `
	WITH
	above AS (
		SELECT r.device_pk, r.bucket_ts,
			toFloat64(sum(r.in_errors + r.out_errors)) AS peak_value,
			'errors' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk = ''
		GROUP BY r.device_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.device_pk, r.bucket_ts,
			toFloat64(sum(r.in_fcs_errors)) AS peak_value,
			'fcs' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk = ''
		GROUP BY r.device_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.device_pk, r.bucket_ts,
			toFloat64(sum(r.in_discards + r.out_discards)) AS peak_value,
			'discards' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk = ''
		GROUP BY r.device_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.device_pk, r.bucket_ts,
			toFloat64(sum(r.carrier_transitions)) AS peak_value,
			'carrier' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk = ''
		GROUP BY r.device_pk, r.bucket_ts
		HAVING peak_value >= 1

		UNION ALL

		SELECT r.device_pk, r.bucket_ts,
			toFloat64(1) AS peak_value,
			'isis_overload' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk = '' AND r.isis_overload = true
		GROUP BY r.device_pk, r.bucket_ts

		UNION ALL

		SELECT r.device_pk, r.bucket_ts,
			toFloat64(1) AS peak_value,
			'isis_unreachable' AS symptom
		FROM device_interface_rollup_5m r FINAL
		WHERE r.bucket_ts >= $1 AND r.bucket_ts < $2
		  AND r.link_pk = '' AND r.isis_unreachable = true
		GROUP BY r.device_pk, r.bucket_ts

		UNION ALL

		SELECT ad.device_pk, e.bucket_ts,
			toFloat64(1) AS peak_value,
			'no_traffic_data' AS symptom
		FROM (
			SELECT DISTINCT device_pk FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= $1 - INTERVAL 24 HOUR AND bucket_ts < $1
			  AND link_pk = ''
		) ad
		CROSS JOIN (
			SELECT toStartOfFiveMinutes(toDateTime($1)) + number * 300 AS bucket_ts
			FROM numbers(toUInt64(greatest(0, dateDiff('second', toDateTime($1), toDateTime($2) - INTERVAL 30 MINUTE)) / 300))
		) e
		WHERE (ad.device_pk, e.bucket_ts) NOT IN (
			SELECT device_pk, bucket_ts FROM device_interface_rollup_5m FINAL
			WHERE bucket_ts >= toStartOfFiveMinutes(toDateTime($1)) AND bucket_ts < $2
			  AND link_pk = ''
		)
	),

	islands AS (
		SELECT device_pk, symptom, bucket_ts, peak_value,
			bucket_ts - toIntervalSecond(
				row_number() OVER (PARTITION BY device_pk, symptom ORDER BY bucket_ts) * 300
			) AS island_grp
		FROM above
	),

	raw_windows AS (
		SELECT device_pk, symptom, island_grp,
			min(bucket_ts) AS started_at,
			max(bucket_ts) + toIntervalSecond(300) AS ended_at,
			max(peak_value) AS peak_value
		FROM islands
		GROUP BY device_pk, symptom, island_grp
	),

	numbered AS (
		SELECT *,
			lagInFrame(ended_at) OVER (
				PARTITION BY device_pk, symptom ORDER BY started_at
			) AS prev_ended_at
		FROM raw_windows
	),
	coalesce_groups AS (
		SELECT *,
			sum(if(prev_ended_at IS NULL
				OR dateDiff('minute', prev_ended_at, started_at) >= $3, 1, 0))
				OVER (PARTITION BY device_pk, symptom ORDER BY started_at) AS coalesce_grp
		FROM numbered
	),
	coalesced AS (
		SELECT device_pk, symptom,
			min(started_at) AS started_at,
			max(ended_at) AS ended_at,
			max(peak_value) AS peak_value
		FROM coalesce_groups
		GROUP BY device_pk, symptom, coalesce_grp
	),

	existing_open AS (
		SELECT ie.incident_id AS existing_id, ie.device_pk, ie.started_at AS existing_start, ie.event_ts
		FROM device_incident_events ie
		INNER JOIN (
			SELECT incident_id, max(event_ts) AS max_ts
			FROM device_incident_events GROUP BY incident_id
		) latest ON ie.incident_id = latest.incident_id AND ie.event_ts = latest.max_ts
		WHERE ie.event_type != 'resolved'
	),

	entity_windows AS (
		SELECT device_pk, started_at, ended_at, symptom, peak_value
		FROM coalesced

		UNION ALL

		SELECT eo.device_pk, eo.existing_start AS started_at, toDateTime($1) AS ended_at,
			'_anchor' AS symptom, toFloat64(0) AS peak_value
		FROM existing_open eo
	),
	entity_numbered AS (
		SELECT *,
			lagInFrame(ended_at) OVER (
				PARTITION BY device_pk ORDER BY started_at
			) AS prev_ended
		FROM entity_windows
	),
	incident_groups AS (
		SELECT *,
			sum(if(prev_ended IS NULL
				OR dateDiff('minute', prev_ended, started_at) >= $3, 1, 0))
				OVER (PARTITION BY device_pk ORDER BY started_at) AS inc_grp
		FROM entity_numbered
	),
	incidents_raw AS (
		SELECT
			device_pk,
			inc_grp,
			min(started_at) AS incident_start,
			max(ended_at) AS incident_end,
			arrayDistinct(arrayFilter(x -> x != '_anchor', groupArray(symptom))) AS symptoms
		FROM incident_groups
		GROUP BY device_pk, inc_grp
		HAVING length(symptoms) > 0
	),
	incidents AS (
		SELECT i.*, eo.existing_id, eo.existing_start
		FROM incidents_raw i
		LEFT JOIN existing_open eo ON i.device_pk = eo.device_pk
			AND i.incident_start = eo.existing_start
	),
	incidents_enriched AS (
		SELECT
			i.device_pk, i.inc_grp, i.incident_start, i.incident_end, i.symptoms,
			if(i.existing_id != '',
				i.existing_id,
				lower(hex(SHA256(concat(i.device_pk, '|', toString(toUnixTimestamp(i.incident_start))))))
			) AS incident_id,
			i.existing_id != '' AS is_existing,
			COALESCE(d.code, '') AS device_code,
			COALESCE(d.device_type, '') AS device_type_name,
			COALESCE(m.code, '') AS metro,
			COALESCE(c.code, '') AS contributor_code,
			COALESCE(d.status, '') AS entity_status
		FROM incidents i
		LEFT JOIN dz_devices_current d ON i.device_pk = d.pk
		LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
		LEFT JOIN dz_contributors_current c ON d.contributor_pk = c.pk
	),

	symptom_windows AS (
		SELECT
			ie.incident_id,
			rw.device_pk,
			rw.symptom,
			rw.started_at AS sw_started_at,
			rw.ended_at AS sw_ended_at,
			rw.peak_value,
			ie.incident_start,
			ie.incident_end,
			ie.is_existing,
			ie.device_code, ie.device_type_name, ie.metro,
			ie.contributor_code, ie.entity_status
		FROM raw_windows rw
		INNER JOIN incidents_enriched ie
			ON rw.device_pk = ie.device_pk
			AND rw.started_at >= ie.incident_start
			AND rw.started_at < ie.incident_end
	)

	SELECT
		incident_id, device_pk, symptom,
		sw_started_at, sw_ended_at, peak_value,
		incident_start, incident_end, is_existing,
		device_code, device_type_name, metro,
		contributor_code, entity_status
	FROM symptom_windows
	ORDER BY incident_id, sw_started_at
`
