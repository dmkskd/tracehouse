#!/usr/bin/env bash
#
# Setup continuous sampling for tracehouse.
#
# Auto-detects single-node vs cluster and generates the appropriate DDL:
#   - Single node:  Atomic database, plain MergeTree
#   - Cluster:      ON CLUSTER DDL, same table names (app uses clusterAllReplicas)
#
# Targets:
#   --target processes   Only set up processes_history (system.processes sampling)
#   --target merges      Only set up merges_history (system.merges sampling)
#   --target all         Set up both (default)
#
# Usage:
#   ./infra/scripts/setup_sampling.sh                                  # both targets, localhost:9000
#   ./infra/scripts/setup_sampling.sh --target processes               # processes only
#   ./infra/scripts/setup_sampling.sh --target merges                  # merges only
#   ./infra/scripts/setup_sampling.sh --host my-ch-node                # custom host
#   ./infra/scripts/setup_sampling.sh --user admin --password secret
#   ./infra/scripts/setup_sampling.sh --cluster dev                    # specify cluster for DDL
#   ./infra/scripts/setup_sampling.sh --interval 5                     # sample every 5 seconds
#   ./infra/scripts/setup_sampling.sh --dry-run                        # print SQL only
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
usage() {
  cat <<'EOF'
Usage: setup_sampling.sh [OPTIONS]

Setup continuous sampling for tracehouse. Creates refreshable materialized
views that snapshot system.processes and/or system.merges every N seconds
into tracehouse history tables.

Auto-detects single-node vs cluster topology and generates appropriate DDL.

Connection options:
  --host HOST              ClickHouse host (default: localhost)
  --port PORT              ClickHouse native port (default: 9000)
  --user USER              ClickHouse user
  --password PASSWORD      ClickHouse password

Cluster options:
  --cluster NAME           Cluster name for ON CLUSTER DDL
  --on-cluster yes|no      Use ON CLUSTER in DDL (default: yes)

Sampling options:
  --target TARGET          What to sample: processes, merges, or all (default: all)
  --interval N             Sampling interval in seconds (default: 1, minimum: 1)
  --ttl DAYS               Data retention in days (default: 7, 0 to disable)

Execution options:
  --dry-run                Print generated SQL without executing
  --yes                    Skip confirmation prompt
  --help, -h               Show this help message

Examples:
  setup_sampling.sh                                        # all targets, localhost, 1s interval
  setup_sampling.sh --target processes                     # processes only
  setup_sampling.sh --target merges --interval 5           # merges only, 5s interval
  setup_sampling.sh --host db1 --interval 5                # remote host, 5s interval
  setup_sampling.sh --cluster prod --yes                   # cluster mode, no prompt
  setup_sampling.sh --on-cluster no --host xyz.clickhouse.cloud  # skip ON CLUSTER
  setup_sampling.sh --dry-run                              # preview SQL only
EOF
  exit 0
}

# ---------------------------------------------------------------------------
# Cluster selection functions (tested via setup-sampling-script.integration.test.ts)
# ---------------------------------------------------------------------------
# Input: multi-line string, each line: cluster_name  nodes  shards  max_replica

# Select a replicated cluster (shards < nodes, with actual replicas).
# Accepts both multi-shard replicated and single-shard replicated topologies.
select_replicated_cluster() {
  echo "$1" | awk '$3 < $2 && ($3 > 1 || $4 > 1) {print $1; exit}'
}

# Select an all-sharded cluster (every node is its own shard, no replication).
select_sharded_cluster() {
  echo "$1" | awk '$3 == $2 {print $1; exit}'
}

# Allow sourcing just the functions (for tests) without running the main script.
# Usage: SETUP_SAMPLING_SOURCE_ONLY=1 source setup_sampling.sh
if [[ "${SETUP_SAMPLING_SOURCE_ONLY:-}" == "1" ]]; then
  return 0 2>/dev/null || exit 0
fi

HOST="localhost"
PORT="9000"
USER=""
PASSWORD=""
CLUSTER=""
USE_ON_CLUSTER="yes"
INTERVAL=1
TTL_DAYS=7
DRY_RUN=false
ASSUME_YES=false
TARGET="all"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)                HOST="$2"; shift 2 ;;
    --port)                PORT="$2"; shift 2 ;;
    --user)                USER="$2"; shift 2 ;;
    --password)            PASSWORD="$2"; shift 2 ;;
    --cluster)             CLUSTER="$2"; shift 2 ;;
    --on-cluster)          USE_ON_CLUSTER="$2"; shift 2 ;;
    --target)              TARGET="$2"; shift 2 ;;
    --interval)            INTERVAL="$2"; shift 2 ;;
    --ttl)                 TTL_DAYS="$2"; shift 2 ;;
    --dry-run)             DRY_RUN=true; shift ;;
    --yes|-y)              ASSUME_YES=true; shift ;;
    --help|-h)             usage ;;
    *)                     echo "Unknown option: $1" >&2; echo "Run with --help for usage." >&2; exit 1 ;;
  esac
done

# Validate target
case "$TARGET" in
  processes|merges|all) ;;
  *) echo "ERROR: --target must be 'processes', 'merges', or 'all'. Got: $TARGET" >&2; exit 1 ;;
esac

# Validate interval (must be a positive integer — ClickHouse REFRESH EVERY does not support sub-second)
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [[ "$INTERVAL" -lt 1 ]]; then
  echo "ERROR: --interval must be a positive integer (seconds). Got: $INTERVAL" >&2
  echo "       ClickHouse REFRESH EVERY does not support sub-second intervals." >&2
  exit 1
fi

# Validate TTL (must be a non-negative integer)
if ! [[ "$TTL_DAYS" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --ttl must be a non-negative integer (days). Got: $TTL_DAYS" >&2
  exit 1
fi

if [[ "$TTL_DAYS" -gt 0 ]]; then
  TTL_CLAUSE="TTL toDateTime(sample_time) + INTERVAL $TTL_DAYS DAY"
  TTL_DISPLAY="${TTL_DAYS}-day TTL"
else
  TTL_CLAUSE=""
  TTL_DISPLAY="no TTL"
fi

CH="clickhouse client --host $HOST --port $PORT"
[[ -n "$USER" ]] && CH="$CH --user $USER"
[[ -n "$PASSWORD" ]] && CH="$CH --password $PASSWORD"

run_query() {
  if $DRY_RUN; then
    echo "$1;"
    echo ""
  else
    echo "$1" | $CH --multiquery
  fi
}

# In --dry-run, log() emits SQL comments so the output stays valid SQL
# but still tells you what's happening.
if $DRY_RUN; then
  log() { echo "-- $*"; }
else
  log() { echo "$@"; }
fi

# Convenience: should we set up processes / merges?
setup_processes() { [[ "$TARGET" == "all" || "$TARGET" == "processes" ]]; }
setup_merges()    { [[ "$TARGET" == "all" || "$TARGET" == "merges" ]]; }

# ---------------------------------------------------------------------------
# Detect environment
# ---------------------------------------------------------------------------
log "Detecting cluster topology..."

if [[ -n "$CLUSTER" ]]; then
  # User explicitly specified a cluster
  CLUSTER_NAME="$CLUSTER"
  log "  Using specified cluster: $CLUSTER_NAME"
else
  # Find multi-node clusters (skip ClickHouse built-in test clusters)
  CLUSTERS=$($CH --query "
    SELECT cluster, count() AS nodes, uniq(shard_num) AS shards,
           max(replica_num) AS max_replica
    FROM system.clusters
    WHERE cluster NOT IN ('test_shard_localhost', 'test_cluster_two_shards_localhost',
                           'test_cluster_one_shard_three_replicas_localhost',
                           'test_unavailable_shard')
    GROUP BY cluster
    HAVING count() > 1
    ORDER BY count() DESC
  " || true)

  CLUSTER_COUNT=$(echo "$CLUSTERS" | grep -c . || true)

  if [[ "$CLUSTER_COUNT" -eq 0 ]]; then
    CLUSTER_NAME=""
  elif [[ "$CLUSTER_COUNT" -eq 1 ]]; then
    CLUSTER_NAME=$(echo "$CLUSTERS" | awk '{print $1}')
    log "  Found cluster: $CLUSTER_NAME"
  else
    echo ""
    echo "  Multiple clusters found:"
    echo ""
    printf "    %-20s %6s  %6s  %s\n" "CLUSTER" "NODES" "SHARDS" "TOPOLOGY"
    echo "    -------------------------------------------------------"
    # Find the best candidate for each role
    MAIN_CLUSTER=""
    SHARDED_CLUSTER=""
    echo "$CLUSTERS" | while read -r name nodes shards max_rep; do
      if [[ "$shards" -eq "$nodes" ]]; then
        topo="all-sharded (each node = 1 shard)"
      elif [[ "$shards" -eq 1 ]]; then
        topo="all-replicated (1 shard, ${nodes} replicas)"
      else
        reps=$((nodes / shards))
        topo="${shards} shards × ${reps} replicas"
      fi
      printf "    %-20s %6s  %6s  %s\n" "$name" "$nodes" "$shards" "$topo"
    done

    MAIN_CLUSTER=$(select_replicated_cluster "$CLUSTERS")
    SHARDED_CLUSTER=$(select_sharded_cluster "$CLUSTERS")

    echo ""
    if [[ -n "$MAIN_CLUSTER" ]]; then
      CLUSTER_NAME="$MAIN_CLUSTER"
      echo "  Auto-selected: --cluster $MAIN_CLUSTER"
    else
      echo "  Could not auto-detect cluster. Re-run with --cluster <name> to select one."
      exit 1
    fi
  fi
fi

if [[ -z "$CLUSTER_NAME" ]]; then
  MODE="single"
  log "  Mode:    single-node"
  ON_CLUSTER=""
else
  NODE_COUNT=$($CH --query "SELECT count() FROM system.clusters WHERE cluster = '$CLUSTER_NAME'")
  SHARD_COUNT=$($CH --query "SELECT uniq(shard_num) FROM system.clusters WHERE cluster = '$CLUSTER_NAME'")
  MODE="cluster"
  log "  Mode:    cluster"
  log "  Cluster: $CLUSTER_NAME ($NODE_COUNT nodes, $SHARD_COUNT shards)"

  if [[ "$USE_ON_CLUSTER" == "yes" ]]; then
    ON_CLUSTER=" ON CLUSTER '$CLUSTER_NAME'"
    log "  ON CLUSTER: yes"

    # Verify all cluster nodes are reachable before running DDL.
    # ON CLUSTER silently skips unreachable nodes, so we check upfront.
    log "  Checking cluster node reachability..."
    REACHABLE=$($CH --query "
      SELECT count() FROM clusterAllReplicas('$CLUSTER_NAME', system.one)
    " 2>/dev/null || echo "0")
    if [[ "$REACHABLE" -lt "$NODE_COUNT" ]]; then
      log "  WARNING: Only $REACHABLE of $NODE_COUNT nodes are reachable."
      log "           ON CLUSTER DDL may not propagate to all nodes."
      log "           Waiting 30s for nodes to come online..."
      sleep 30
      REACHABLE=$($CH --query "
        SELECT count() FROM clusterAllReplicas('$CLUSTER_NAME', system.one)
      " 2>/dev/null || echo "0")
      if [[ "$REACHABLE" -lt "$NODE_COUNT" ]]; then
        log "  WARNING: Still only $REACHABLE of $NODE_COUNT nodes reachable. Proceeding anyway."
      else
        log "  All $REACHABLE nodes are now reachable."
      fi
    else
      log "  All $REACHABLE nodes are reachable."
    fi
  else
    ON_CLUSTER=""
    log "  ON CLUSTER: no (DDL auto-replicates)"
  fi
fi

log ""

# Table name helpers — same names in both single and cluster mode.
# No Distributed table needed; the app uses clusterAllReplicas() to fan out.
proc_local_table() { echo "processes_history"; }
proc_buffer_table() { echo "processes_history_buffer"; }
merge_local_table() { echo "merges_history"; }
merge_buffer_table() { echo "merges_history_buffer"; }

# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------
if ! $DRY_RUN && ! $ASSUME_YES && [[ -t 0 ]]; then
  echo "============================================================"
  echo "Ready to execute DDL ($MODE mode, target=$TARGET, sampling every ${INTERVAL}s)"
  echo "============================================================"
  echo ""
  echo "  Host:     $HOST:$PORT"
  if [[ "$MODE" == "cluster" ]]; then
    echo "  Cluster:  $CLUSTER_NAME ($NODE_COUNT nodes, $SHARD_COUNT shards)"
    if [[ -n "$ON_CLUSTER" ]]; then
      echo "  ON CLUSTER: yes"
    else
      echo "  ON CLUSTER: no"
    fi
  fi
  echo "  Target:   $TARGET"
  echo "  Interval: every ${INTERVAL}s"
  echo "  TTL:      $TTL_DISPLAY"
  echo "  Database: tracehouse"
  echo ""
  echo "  Tables:"
  if setup_processes; then
    echo "    tracehouse.$(proc_local_table)          (MergeTree, $TTL_DISPLAY)"
    echo "    tracehouse.$(proc_buffer_table)         (Buffer)"
    echo "    tracehouse.processes_sampler            (Refreshable MV, every ${INTERVAL}s)"
  fi
  if setup_merges; then
    echo "    tracehouse.$(merge_local_table)            (MergeTree, $TTL_DISPLAY)"
    echo "    tracehouse.$(merge_buffer_table)           (Buffer)"
    echo "    tracehouse.merges_sampler                 (Refreshable MV, every ${INTERVAL}s)"
  fi
  echo ""
  printf "Proceed? [y/N] "
  read -r answer
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
  fi
  echo ""
fi

log "============================================================"
log "Generating DDL ($MODE mode, target=$TARGET, sampling every ${INTERVAL}s)..."
log "============================================================"
log ""

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
if [[ -n "$ON_CLUSTER" ]]; then
  run_query "CREATE DATABASE IF NOT EXISTS tracehouse$ON_CLUSTER ENGINE = Atomic"
else
  run_query "CREATE DATABASE IF NOT EXISTS tracehouse"
fi

# Buffer flush timing (shared by both targets)
BUF_MIN_TIME=$(( INTERVAL * 15 < 15 ? 15 : INTERVAL * 15 ))
BUF_MAX_TIME=$(( BUF_MIN_TIME * 2 ))

# ===========================================================================
# PROCESSES sampling
# ===========================================================================
if setup_processes; then
  PROC_LOCAL=$(proc_local_table)
  PROC_BUFFER=$(proc_buffer_table)

  log "--- Setting up processes_history ---"

  # Target table (MergeTree, local per node)
  run_query "CREATE TABLE IF NOT EXISTS tracehouse.$PROC_LOCAL$ON_CLUSTER
(
    -- sampling metadata
    hostname            LowCardinality(String) DEFAULT hostName(),
    sample_time         DateTime64(3) DEFAULT now64(3),

    -- query identity
    is_initial_query    UInt8,
    query_id            String,
    initial_query_id    String,
    query               String,
    normalized_query_hash UInt64,
    query_kind          LowCardinality(String),
    current_database    LowCardinality(String),

    -- user / client info
    user                String,
    initial_user        String,
    address             String,
    initial_address     String,
    interface           UInt8,
    os_user             String,
    client_hostname     String,
    client_name         String,

    -- progress
    elapsed             Float64,
    is_cancelled        UInt8,
    read_rows           UInt64,
    read_bytes          UInt64,
    written_rows        UInt64,
    written_bytes       UInt64,
    total_rows_approx   UInt64,

    -- resources
    memory_usage        Int64,
    peak_memory_usage   Int64,
    thread_ids          Array(UInt64),
    peak_threads_usage  UInt64,

    -- maps
    ProfileEvents       Map(String, UInt64),
    Settings            Map(String, String)
)
ENGINE = MergeTree
ORDER BY (query_id, sample_time)
${TTL_CLAUSE:+$TTL_CLAUSE
}SETTINGS index_granularity = 8192"

  # Buffer table
  run_query "CREATE TABLE IF NOT EXISTS tracehouse.$PROC_BUFFER$ON_CLUSTER
    AS tracehouse.$PROC_LOCAL
ENGINE = Buffer(
    'tracehouse', '$PROC_LOCAL',
    1,            -- num_layers
    $BUF_MIN_TIME, $BUF_MAX_TIME, -- min/max seconds before flush
    100, 10000,   -- min/max rows before flush
    10000, 1000000 -- min/max bytes before flush
)"

  # Refreshable materialized view
  run_query "CREATE MATERIALIZED VIEW IF NOT EXISTS tracehouse.processes_sampler$ON_CLUSTER
REFRESH EVERY $INTERVAL SECOND
APPEND
TO tracehouse.$PROC_BUFFER
AS
/* source:TraceHouse:Sampler:processes */
SELECT
    hostName()          AS hostname,
    now64(3)            AS sample_time,

    is_initial_query,
    query_id,
    initial_query_id,
    query,
    normalized_query_hash,
    query_kind,
    current_database,

    user,
    initial_user,
    toString(address)   AS address,
    toString(initial_address) AS initial_address,
    interface,
    os_user,
    client_hostname,
    client_name,

    elapsed,
    is_cancelled,
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    total_rows_approx,

    memory_usage,
    peak_memory_usage,
    thread_ids,
    peak_threads_usage,

    ProfileEvents,
    Settings
FROM system.processes
WHERE query NOT LIKE '%source:TraceHouse:%'"

  log ""
fi

# ===========================================================================
# MERGES sampling
# ===========================================================================
if setup_merges; then
  MERGE_LOCAL=$(merge_local_table)
  MERGE_BUFFER=$(merge_buffer_table)

  log "--- Setting up merges_history ---"

  # Target table (MergeTree, local per node)
  run_query "CREATE TABLE IF NOT EXISTS tracehouse.$MERGE_LOCAL$ON_CLUSTER
(
    -- sampling metadata
    hostname            LowCardinality(String) DEFAULT hostName(),
    sample_time         DateTime64(3) DEFAULT now64(3),

    -- merge identity
    database            LowCardinality(String),
    table               LowCardinality(String),
    result_part_name    String,
    partition_id        LowCardinality(String),

    -- merge properties
    elapsed             Float64,
    progress            Float64,
    num_parts           UInt64,
    is_mutation         UInt8,
    merge_type          LowCardinality(String),
    merge_algorithm     LowCardinality(String),

    -- size
    total_size_bytes_compressed   UInt64,
    total_size_bytes_uncompressed UInt64,
    total_size_marks              UInt64,

    -- I/O progress
    rows_read                     UInt64,
    bytes_read_uncompressed       UInt64,
    rows_written                  UInt64,
    bytes_written_uncompressed    UInt64,
    columns_written               UInt64,

    -- resources
    memory_usage        UInt64,
    thread_id           UInt64
)
ENGINE = MergeTree
ORDER BY (database, table, result_part_name, sample_time)
${TTL_CLAUSE:+$TTL_CLAUSE
}SETTINGS index_granularity = 8192"

  # Buffer table
  run_query "CREATE TABLE IF NOT EXISTS tracehouse.$MERGE_BUFFER$ON_CLUSTER
    AS tracehouse.$MERGE_LOCAL
ENGINE = Buffer(
    'tracehouse', '$MERGE_LOCAL',
    1,            -- num_layers
    $BUF_MIN_TIME, $BUF_MAX_TIME, -- min/max seconds before flush
    100, 10000,   -- min/max rows before flush
    10000, 1000000 -- min/max bytes before flush
)"

  # Refreshable materialized view
  # Each node's MV reads its own local system.merges — no cross-node traffic.
  run_query "CREATE MATERIALIZED VIEW IF NOT EXISTS tracehouse.merges_sampler$ON_CLUSTER
REFRESH EVERY $INTERVAL SECOND
APPEND
TO tracehouse.$MERGE_BUFFER
AS
/* source:TraceHouse:Sampler:merges */
SELECT
    hostName()          AS hostname,
    now64(3)            AS sample_time,

    database,
    table,
    result_part_name,
    partition_id,

    elapsed,
    progress,
    num_parts,
    is_mutation,
    merge_type,
    merge_algorithm,

    total_size_bytes_compressed,
    total_size_bytes_uncompressed,
    total_size_marks,

    rows_read,
    bytes_read_uncompressed,
    rows_written,
    bytes_written_uncompressed,
    columns_written,

    memory_usage,
    thread_id
FROM system.merges"

  log ""
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log ""
log "============================================================"
if $DRY_RUN; then
  log "Done. Pipe this output to: clickhouse client --multiquery"
else
  echo "Setup complete!"
  echo ""

  echo "--- Tables created ---"
  if [[ "$MODE" == "cluster" ]]; then
    $CH --query "
      SELECT hostName() AS host, name, engine
      FROM clusterAllReplicas('$CLUSTER_NAME', system.tables)
      WHERE database = 'tracehouse'
      ORDER BY host, name
    "
  else
    $CH --query "
      SELECT name, engine
      FROM system.tables
      WHERE database = 'tracehouse'
      ORDER BY name
    "
  fi

  echo ""
  echo "--- Data flow ---"
  $CH --query "
    SELECT concat(
      name, ' (', engine, ')',
      if(length(dependencies_table) > 0, ' → ' || arrayStringConcat(dependencies_table, ', '), '')
    ) AS flow
    FROM system.tables
    WHERE database = 'tracehouse'
    ORDER BY
      engine = 'MaterializedView' DESC,
      engine = 'Buffer' DESC,
      engine = 'MergeTree' DESC,
      engine = 'Distributed' DESC
  "

  echo ""
  echo "--- Sampler status ---"
  $CH --query "
    SELECT database, view, status, last_success_time, next_refresh_time, exception
    FROM system.view_refreshes
    WHERE database = 'tracehouse'
  "

  if [[ "$MODE" == "cluster" ]]; then
    echo ""
    echo "--- Cluster coverage ---"
    COVERAGE=$($CH --query "
      SELECT
        hostName() AS host,
        countIf(name = 'tracehouse') AS has_db
      FROM clusterAllReplicas('$CLUSTER_NAME', system.databases)
      GROUP BY host
      ORDER BY host
    ")
    echo "$COVERAGE"
    MISSING=$(echo "$COVERAGE" | awk '$2 == 0 {print $1}')
    if [[ -n "$MISSING" ]]; then
      echo ""
      echo "WARNING: tracehouse database is MISSING on these nodes:"
      echo "$MISSING"
      echo "Re-run this script once all nodes are ready."
    fi
  fi

  echo ""
  echo "--- Live sample (waiting 3s for data) ---"
  sleep 3
  if setup_processes; then
    $CH --query "
      SELECT
        'processes' AS target,
        hostname,
        count() AS samples,
        min(sample_time) AS first_sample,
        max(sample_time) AS last_sample
      FROM tracehouse.processes_history
      GROUP BY hostname
      ORDER BY hostname
    "
  fi
  if setup_merges; then
    $CH --query "
      SELECT
        'merges' AS target,
        hostname,
        count() AS samples,
        min(sample_time) AS first_sample,
        max(sample_time) AS last_sample
      FROM tracehouse.merges_history
      GROUP BY hostname
      ORDER BY hostname
    "
  fi
fi
