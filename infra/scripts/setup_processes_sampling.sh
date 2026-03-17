#!/usr/bin/env bash
#
# Setup process sampling for tracehouse.
#
# Auto-detects single-node vs cluster and generates the appropriate DDL:
#   - Single node:  Atomic database, plain MergeTree (processes_history)
#   - Cluster:      ON CLUSTER DDL, local table (processes_history_local),
#                   Distributed table (processes_history) for cross-node queries
#
# Usage:
#   ./infra/scripts/setup_processes_sampling.sh                          # localhost:9000
#   ./infra/scripts/setup_processes_sampling.sh --host my-ch-node        # custom host
#   ./infra/scripts/setup_processes_sampling.sh --user admin --password secret
#   ./infra/scripts/setup_processes_sampling.sh --cluster dev            # specify cluster for DDL
#   ./infra/scripts/setup_processes_sampling.sh --cluster dev --distributed-cluster all-sharded
#   ./infra/scripts/setup_processes_sampling.sh --dry-run                # print SQL only
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
HOST="localhost"
PORT="9000"
USER=""
PASSWORD=""
CLUSTER=""
DIST_CLUSTER=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)                HOST="$2"; shift 2 ;;
    --port)                PORT="$2"; shift 2 ;;
    --user)                USER="$2"; shift 2 ;;
    --password)            PASSWORD="$2"; shift 2 ;;
    --cluster)             CLUSTER="$2"; shift 2 ;;
    --distributed-cluster) DIST_CLUSTER="$2"; shift 2 ;;
    --dry-run)             DRY_RUN=true; shift ;;
    *)                     echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

CH="clickhouse client --host $HOST --port $PORT"
[[ -n "$USER" ]] && CH="$CH --user $USER"
[[ -n "$PASSWORD" ]] && CH="$CH --password $PASSWORD"

run_query() {
  echo "$1;"
  echo ""
  if ! $DRY_RUN; then
    echo "$1" | $CH --multiquery
  fi
}

# ---------------------------------------------------------------------------
# Detect environment
# ---------------------------------------------------------------------------
echo "Detecting cluster topology..."

if [[ -n "$CLUSTER" ]]; then
  # User explicitly specified a cluster
  CLUSTER_NAME="$CLUSTER"
  echo "  Using specified cluster: $CLUSTER_NAME"
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
    echo "  Found cluster: $CLUSTER_NAME"
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

    # Find a shards×replicas cluster (for --cluster / ON CLUSTER DDL)
    MAIN_CLUSTER=$(echo "$CLUSTERS" | awk '$3 < $2 && $3 > 1 {print $1; exit}')
    # Find an all-sharded cluster (for --distributed-cluster)
    SHARDED_CLUSTER=$(echo "$CLUSTERS" | awk '$3 == $2 {print $1; exit}')

    echo ""
    if [[ -n "$MAIN_CLUSTER" && -n "$SHARDED_CLUSTER" ]]; then
      echo "  Suggested command:"
      echo ""
      echo "    $0 --cluster $MAIN_CLUSTER --distributed-cluster $SHARDED_CLUSTER"
      echo ""
      echo "  --cluster $MAIN_CLUSTER"
      echo "    Your shard/replica topology. Used for ON CLUSTER DDL (creates tables on all nodes)."
      echo "  --distributed-cluster $SHARDED_CLUSTER"
      echo "    Every node as a separate shard. Used for the Distributed table so all nodes are queried."
      echo "    (Needed because each node samples its own system.processes independently.)"
    elif [[ -n "$MAIN_CLUSTER" ]]; then
      echo "  Suggested command:"
      echo ""
      echo "    $0 --cluster $MAIN_CLUSTER"
    else
      echo "  Re-run with --cluster <name> to select one."
    fi
    exit 1
  fi
fi

if [[ -z "$CLUSTER_NAME" ]]; then
  MODE="single"
  echo "  Mode:    single-node"
  ON_CLUSTER=""
  LOCAL_TABLE="processes_history"
  BUFFER_TABLE="processes_history_buffer"
  DIST_CLUSTER_NAME=""
else
  NODE_COUNT=$($CH --query "SELECT count() FROM system.clusters WHERE cluster = '$CLUSTER_NAME'")
  SHARD_COUNT=$($CH --query "SELECT uniq(shard_num) FROM system.clusters WHERE cluster = '$CLUSTER_NAME'")
  MODE="cluster"
  echo "  Mode:    cluster"
  echo "  Cluster: $CLUSTER_NAME ($NODE_COUNT nodes, $SHARD_COUNT shards)"
  ON_CLUSTER=" ON CLUSTER '$CLUSTER_NAME'"
  LOCAL_TABLE="processes_history_local"
  BUFFER_TABLE="processes_history_local_buffer"

  # Each node produces unique samples from its local system.processes.
  # The Distributed table must treat every node as a separate shard,
  # otherwise replicas within a shard are only sampled randomly (1 of N).
  HAS_REPLICAS=$($CH --query "
    SELECT max(cnt) > 1 FROM (
      SELECT shard_num, count() AS cnt
      FROM system.clusters WHERE cluster = '$CLUSTER_NAME'
      GROUP BY shard_num
    )
  ")

  if [[ -n "$DIST_CLUSTER" ]]; then
    DIST_CLUSTER_NAME="$DIST_CLUSTER"
    echo "  Distributed cluster: $DIST_CLUSTER_NAME"
  elif [[ "$HAS_REPLICAS" == "1" ]]; then
    # Find an all-sharded cluster automatically
    SHARDED_CANDIDATE=$($CH --query "
      SELECT cluster
      FROM system.clusters
      WHERE cluster NOT IN ('test_shard_localhost', 'test_cluster_two_shards_localhost',
                             'test_cluster_one_shard_three_replicas_localhost',
                             'test_unavailable_shard')
      GROUP BY cluster
      HAVING count() > 1 AND uniq(shard_num) = count()
      ORDER BY cluster
      LIMIT 1
    " || true)

    if [[ -n "$SHARDED_CANDIDATE" ]]; then
      DIST_CLUSTER_NAME="$SHARDED_CANDIDATE"
      echo "  Distributed cluster: $DIST_CLUSTER_NAME (auto-detected all-sharded cluster)"
    else
      # No all-sharded cluster found — fall back to main cluster.
      # With replicas sharing a shard, the Distributed table will only
      # query one replica per shard (not all nodes), so some samples
      # may be missed. Still better than failing entirely.
      DIST_CLUSTER_NAME="$CLUSTER_NAME"
      echo "  WARNING: No all-sharded cluster found. Using '$CLUSTER_NAME' for Distributed table."
      echo "           Samples from replica nodes sharing a shard may not all be visible."
      echo "           For full coverage, create an all-sharded cluster and re-run with:"
      echo "             $0 --cluster $CLUSTER_NAME --distributed-cluster <name>"
    fi
  else
    # No replicas — same cluster works for both DDL and Distributed
    DIST_CLUSTER_NAME="$CLUSTER_NAME"
  fi
fi

echo ""
echo "============================================================"
echo "Generating DDL ($MODE mode)..."
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
run_query "CREATE DATABASE IF NOT EXISTS tracehouse$ON_CLUSTER ENGINE = Atomic"

# ---------------------------------------------------------------------------
# Target table (MergeTree, local per node)
# ---------------------------------------------------------------------------
run_query "CREATE TABLE IF NOT EXISTS tracehouse.$LOCAL_TABLE$ON_CLUSTER
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
TTL toDateTime(sample_time) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192"

# ---------------------------------------------------------------------------
# Buffer table
# ---------------------------------------------------------------------------
# Buffer tables are always node-local (not replicated), but ON CLUSTER
# ensures the DDL runs on every node.
run_query "CREATE TABLE IF NOT EXISTS tracehouse.$BUFFER_TABLE$ON_CLUSTER
    AS tracehouse.$LOCAL_TABLE
ENGINE = Buffer(
    'tracehouse', '$LOCAL_TABLE',
    1,            -- num_layers
    15, 30,       -- min/max seconds before flush
    100, 10000,   -- min/max rows before flush
    10000, 1000000 -- min/max bytes before flush
)"

# ---------------------------------------------------------------------------
# Refreshable materialized view
# ---------------------------------------------------------------------------
# APPEND is required on replicated databases. Each node's MV reads its own
# local system.processes — no cross-node traffic.
run_query "CREATE MATERIALIZED VIEW IF NOT EXISTS tracehouse.processes_sampler$ON_CLUSTER
REFRESH EVERY 1 SECOND
APPEND
TO tracehouse.$BUFFER_TABLE
AS
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
WHERE is_initial_query = 1
  AND query NOT LIKE '%processes_history%'"

# ---------------------------------------------------------------------------
# Distributed table (cluster only)
# ---------------------------------------------------------------------------
if [[ "$MODE" == "cluster" && -n "$DIST_CLUSTER_NAME" ]]; then
  run_query "CREATE TABLE IF NOT EXISTS tracehouse.processes_history$ON_CLUSTER
    AS tracehouse.$LOCAL_TABLE
ENGINE = Distributed('$DIST_CLUSTER_NAME', 'tracehouse', '$LOCAL_TABLE', rand())"
fi

echo ""
echo "============================================================"
if $DRY_RUN; then
  echo "Dry run complete. No DDL was executed."
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

  echo ""
  echo "--- Live sample (waiting 3s for data) ---"
  sleep 3
  $CH --query "
    SELECT
      hostname,
      count() AS samples,
      min(sample_time) AS first_sample,
      max(sample_time) AS last_sample
    FROM tracehouse.processes_history
    GROUP BY hostname
    ORDER BY hostname
  "
fi
