# Generating Test Data

TraceHouse ships with scripts to generate realistic test datasets for development and demo purposes.

## Available Datasets

| Dataset | Table | Description |
|---------|-------|-------------|
| Synthetic | `synthetic_data.*` | Generated data with configurable row counts and partitions |
| NYC Taxi | `nyc_taxi.*` | New York City taxi trip records |
| UK House Prices | `uk_price_paid.*` | UK property transaction data |
| Web Analytics | `web_analytics.*` | Simulated web analytics events |

## Data Tools TUI

The TUI dashboard lets you start, stop, and monitor all data tools (generate, queries, mutations, merge-triggers, events) from a single terminal:

```bash
just data-tools-tui
```

Keyboard shortcuts: `a` start all, `s` stop all, `1`–`5` toggle individual tools, `c` copy log, `x` clear log, `q` quit. Tool `5` is the event generator described below; starting all tools intentionally generates its query failures and DDL.

The TUI forwards your `.env` / `CH_ENV_FILE` settings to all child processes and shows live progress for bulk data generation. It also includes a `.env` viewer/editor tab (`Ctrl+S` to save).

## Generating Data

### Generate All Datasets

```bash
just generate-data
```

### Generate Specific Datasets

```bash
just generate-data taxi        # NYC Taxi only
just generate-data synthetic   # Synthetic only
just generate-data uk          # UK House Prices only
just generate-data web         # Web Analytics only
```

### Quick Generate (Small Dataset)

For fast iteration during development:

```bash
just generate-data-quick
# 1M rows, small batches → many parts for merge visualization
```

### Heavy Generate (Merge Stress Test)

To generate lots of merge activity:

```bash
just generate-data-heavy
# 10M rows, small batches → triggers many merges
```

### Append Without Recreating Schemas

For a continuous workload whose tables were prepared during startup, skip the
idempotent database and table DDL on subsequent append cycles:

```bash
just generate-data all --mode append --skip-create
```

This avoids propagating redundant `CREATE ... IF NOT EXISTS` entries through
Replicated database DDL queues. The command expects every selected dataset
table to exist already; omit `--skip-create` for initial setup or recovery.

## Generating Activity

### Slow Queries

Generate query activity for the query monitor:

```bash
just run-queries

# Heavy load example
just run-queries --slow-workers 10 --s3-workers 6 --slow-interval 0.3
```

### Mutations

Generate mutation activity:

```bash
just run-mutations          # All mutation types
just run-mutations-heavy    # Heavy mutations only
just run-mutations-light    # Lightweight mutations only
```

By default mutations run in **async** mode (`mutations_sync=0`) - the script fires each mutation and returns immediately, so you can watch progress in the Merge Tracker UI. Use `--sync sync` to wait for each mutation to complete before starting the next:

```bash
just run-mutations --sync async   # Fire-and-forget (default)
just run-mutations --sync sync    # Wait for each mutation to finish
```

Or set via environment variable:

```bash
CH_MUTATION_SYNC=sync just run-mutations
```

:::tip
Lightweight `DELETE FROM` is synchronous by default in ClickHouse. The `--sync` flag overrides this with `mutations_sync=0` so all mutation types behave consistently.
:::

### Time Travel Events

Generate events that can be inspected in Time Travel and the top-level
**Events** page:

```bash
# Cover every safely generated event class
just run-events --once

# Continuous patterns, each on an independent low-frequency cadence
just run-events

# A recurring scheduled-job pattern every 15 minutes
just run-events --types oom --oom-interval 900

# Only disposable schema changes
just run-events --types ddl --ddl-interval 60

# One failed background mutation for Merge Tracker and Events
just run-events --types merge --once
```

The event workload first checks that the source log for each selected type is
visible to the connected user. Every intended event query contains a
`tracehouse-demo-event` comment, while setup and cleanup queries disable query
logging. `--once` runs `SYSTEM FLUSH LOGS` when permitted so the results appear
promptly.

| Generated event | Safety boundary | Time Travel kind |
| --- | --- | --- |
| Disposable DDL cycle | Uses `tracehouse_event_demo`; includes create, alter, rename, optimize, truncate, and drop operations; removes its tables after each cycle | `ddl` |
| Query OOM | Sets a 1 MB **query** memory limit; it is not a process/server OOM | `query_oom` |
| Query timeout | Sets a 50 ms limit on one CPU query | `query_timeout` |
| Failed merge/mutation | Runs `throwIf` inside a background mutation on a disposable table, waits for error 395, then drops the table to stop retries | `part_failure` |
| Query rejection | Uses one tiny disposable MergeTree table with a table-local parts limit | `query_rejected` |
| Query disk limit | Requires an impossible free-space threshold for one bounded external sort; it does not fill the disk | `query_resource_limit` |
| Missing Keeper | Attempts one isolated replicated table when Keeper is unavailable, then cleans it up | `error_burst` / coordination |
| Failed local connection | Connects only to unused localhost port 1 with a short timeout | `error_burst` / maintenance |

The default database and cadences can be configured with `CH_EVENT_DATABASE`,
`CH_EVENT_TYPES`, `CH_EVENT_DDL_INTERVAL`, `CH_EVENT_OOM_INTERVAL`,
`CH_EVENT_TIMEOUT_INTERVAL`, `CH_EVENT_MERGE_INTERVAL`, `CH_EVENT_REJECTED_INTERVAL`,
`CH_EVENT_RESOURCE_INTERVAL`, `CH_EVENT_COORDINATION_INTERVAL`, and
`CH_EVENT_NETWORK_INTERVAL`.

The workload only runs a type when the system log consumed by Events is
available: `system.query_log` for query/DDL events, `system.part_log` for the
failed mutation, and `system.error_log` for the operational probes. A Keeper-enabled server cannot safely manufacture
`NO_ZOOKEEPER`; in that environment the coordination probe is cleaned up and
reported as not generated.

Restarts require an action outside ClickHouse, so they are deliberately not part of the continuous workload. For a disposable local Docker environment:

```bash
docker restart tracehouse-dev
```

Allow asynchronous metrics to sample before and after the restart. Time Travel then infers the restart from the `Uptime` reset in `system.asynchronous_metric_log`.

Crashes, full disks, corrupt parts, replica data loss/read-only episodes, and
background-task failures are not automated. They require service or storage
fault injection and can damage data or destabilize a shared server, so they
should only be exercised in an isolated environment.

For the event definitions, sources, retention caveats, and filtering contract, see [Time Travel Events](https://github.com/TraceHouse/tracehouse/blob/main/docs/metrics/time-travel-events.md).

## Multi-User Simulation

All tools support `--users N` to create temporary ClickHouse users (`th_alice`, `th_bob`, `th_charlie`, ...) so that activity shows up under different usernames in `system.query_log`, `system.processes`, etc. This is useful for testing per-user dashboards and spotting "noisy neighbor" patterns.

```bash
# Run queries as 5 different users
just run-queries --users 5

# With skewed distribution (th_alice gets ~55% of traffic)
just run-queries --users 5 --user-skew 1

# Very skewed (th_alice gets ~74%)
just run-queries --users 5 --user-skew 2

# Works with all tools
just generate-data --users 3
just run-mutations --users 5 --user-skew 1
```

Or set via environment variables:

```bash
CH_USERS=5
CH_USER_SKEW=1
```

**Security:** Users are created with random passwords (fresh each run). On exit, all test users are locked with `HOST NONE` — no one can connect as them. If the script crashes, the random 128-bit password provides protection until the next run resets it.

**Skew values:**

| `--user-skew` | th_alice | th_bob | th_charlie | Effect                  |
| ------------- | -------- | ------ | ---------- | ----------------------- |
| 0 (default)   | 33%      | 33%    | 33%        | Equal                   |
| 1             | 55%      | 27%    | 18%        | Zipf — clear noisy user |
| 2             | 74%      | 18%    | 8%         | Very noisy th_alice     |

## Resetting Data

```bash
# Drop and regenerate all test data
just regenerate-data

# Drop all test tables (with confirmation)
just drop-data

# Drop without confirmation
just drop-data -y
```

## Configuration

All CLI scripts (`just generate-data`, `just run-queries`, `just run-mutations`, etc.) automatically load `.env` from the repo root if it exists. This file is **not** used by the frontend app — only by the data-utils CLI tools.

```bash
cp .env.example .env
```

To use a different env file, set `CH_ENV_FILE` or pass `--env-file`:

```bash
CH_ENV_FILE=.env.clickhouse just generate-data
# or
just generate-data --env-file .env.aiven
```

```bash
# .env — used by CLI scripts only, not the app UI
CH_HOST=your-cluster.example.com
CH_PORT=9440
CH_USER=default
CH_PASSWORD=your-password
CH_SECURE=true

# Data generation parameters
CH_GEN_ROWS=1000000
CH_GEN_PARTITIONS=1
CH_GEN_BATCH_SIZE=10000
```

See `.env.example` for the full list of available options.

CLI tools prompt for confirmation before starting. To skip the prompt, pass `-y` / `--assume-yes` or set `CH_ASSUME_YES=true` in your `.env`:

```bash
just generate-data -y
# or
CH_ASSUME_YES=true just generate-data
```

:::info
By default, all CLI tools automatically look for a `.env` file in the repo root. If no `.env` is found and no `CH_ENV_FILE` is set, the tools fall back to built-in defaults (`localhost:9000`, user `default`, no password) — which works out of the box for local Docker Compose or Local Binary setups.
:::
