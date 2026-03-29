#!/bin/bash
# Demo workload loop — generates data, then runs inserts + queries continuously.
#
# Data tables have 12h TTL so old rows expire automatically — no drop/recreate
# cycle needed. The workload just keeps inserting and querying.
#
# All tool config comes from environment variables set in docker-compose.yml.
# See CH_GEN_*, CH_QUERY_*, CH_MUTATION_* env vars.

set -euo pipefail

# All nodes for query distribution (tables are Distributed across shards)
CH_HOSTS=(${CH_QUERY_HOSTS:-ch-s1r1 ch-s1r2 ch-s2r1 ch-s2r2})
_host_idx=0
pick_host() {
  local host="${CH_HOSTS[$_host_idx]}"
  _host_idx=$(( (_host_idx + 1) % ${#CH_HOSTS[@]} ))
  echo "$host"
}

run() { cd /app && uv run "$@"; }

log() { echo "[workload $(date '+%Y-%m-%d %H:%M:%S')] $*"; }

wait_for_clickhouse() {
  log "Waiting for ClickHouse..."
  local retries=0
  until run python -c "
from clickhouse_driver import Client
c = Client('${CH_HOST}', port=int('${CH_PORT}'), user='${CH_USER}', password='${CH_PASSWORD}')
c.execute('SELECT 1')
" >/dev/null 2>&1; do
    retries=$((retries + 1))
    if [ $retries -ge 60 ]; then
      log "ERROR: ClickHouse not ready after 60 attempts"
      exit 1
    fi
    sleep 5
  done
  log "ClickHouse is ready"
}

# ── Insert loop — runs generate in append mode repeatedly ───────────────

insert_loop() {
  while true; do
    log "Inserting batch (append mode)..."
    run tracehouse-generate --mode append || log "WARNING: generate exited with error"
    sleep 10
  done
}

# ── Main ────────────────────────────────────────────────────────────────

wait_for_clickhouse

# Phase 1: Generate initial data (blocking — tables must exist before queries start)
# No throttle for initial load — fill tables fast, TTL handles cleanup later
log "Generating initial data (no throttle, full parallelism)..."
CH_GEN_THROTTLE_MIN=0 CH_GEN_THROTTLE_MAX=0 CH_GEN_PARALLELISM=0 \
  run tracehouse-generate || log "WARNING: generate exited with error"

# Phase 2: Continuous activity — inserts + queries + sparse mutations
# Inserts create new parts → triggers natural background merges
# Old data expires via TTL — no drop cycle needed
log "Starting continuous workload..."

insert_loop &
insert_pid=$!

# Spread queries across all nodes (one worker per node)
query_pids=()
for qhost in "${CH_HOSTS[@]}"; do
  log "Starting query worker on $qhost"
  CH_HOST=$qhost run tracehouse-queries &
  query_pids+=($!)
done

CH_HOST=$(pick_host) run tracehouse-mutations &
mutations_pid=$!

# Wait forever — TTL handles cleanup
log "Workload running (TTL handles data expiry, no cycle needed)"
wait
