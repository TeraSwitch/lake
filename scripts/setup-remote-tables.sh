#!/bin/bash
set -euo pipefail

# Set up remoteSecure tables in local ClickHouse pointing to ClickHouse Cloud.
# Reads CLICKHOUSE_CLOUD_REMOTE_TABLE_HOST and CLICKHOUSE_CLOUD_REMOTE_TABLE_PASSWORD from .env.
#
# Usage:
#   ./scripts/setup-remote-tables.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

if [[ -f .env ]]; then
    CLICKHOUSE_CLOUD_REMOTE_TABLE_HOST=$(grep -E '^CLICKHOUSE_CLOUD_REMOTE_TABLE_HOST=' .env | cut -d= -f2- || true)
    CLICKHOUSE_CLOUD_REMOTE_TABLE_PASSWORD=$(grep -E '^CLICKHOUSE_CLOUD_REMOTE_TABLE_PASSWORD=' .env | cut -d= -f2- || true)
fi

if [[ -z "${CLICKHOUSE_CLOUD_REMOTE_TABLE_HOST:-}" ]] || [[ -z "${CLICKHOUSE_CLOUD_REMOTE_TABLE_PASSWORD:-}" ]]; then
    echo "CLICKHOUSE_CLOUD_REMOTE_TABLE_HOST or CLICKHOUSE_CLOUD_REMOTE_TABLE_PASSWORD not set in .env, skipping"
    echo "Set both to enable querying shredder data from ClickHouse Cloud"
    exit 0
fi

# Wait for local ClickHouse to be ready
echo "Waiting for local ClickHouse..."
for i in {1..30}; do
    if curl -sf http://localhost:8123/ping > /dev/null 2>&1; then
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "ClickHouse not ready after 30s, skipping"
        exit 1
    fi
    sleep 1
done

# Add remote tables here: "local_table_name remote_db.remote_table_name"
REMOTE_TABLES=(
    "dz_publisher_shred_stats shredder.publisher_shred_stats"
)

failed=0
for entry in "${REMOTE_TABLES[@]}"; do
    local_name="${entry%% *}"
    remote_path="${entry#* }"
    if curl -sf http://localhost:8123/ -d "
        CREATE TABLE IF NOT EXISTS ${local_name}
        AS remoteSecure(
            '${CLICKHOUSE_CLOUD_REMOTE_TABLE_HOST}',
            '${remote_path}',
            'lake_dev_reader',
            '${CLICKHOUSE_CLOUD_REMOTE_TABLE_PASSWORD}'
        )
    "; then
        echo "${local_name}: ok"
    else
        echo "${local_name}: FAILED"
        ((failed++))
    fi
done

if [[ $failed -gt 0 ]]; then
    echo "${failed} remote table(s) failed to create"
    exit 1
fi
