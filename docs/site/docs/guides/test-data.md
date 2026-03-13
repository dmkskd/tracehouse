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

The TUI dashboard lets you start, stop, and monitor all data tools (generate, queries, mutations, merge-triggers) from a single terminal:

```bash
just data-tools-tui
```

Keyboard shortcuts: `a` start all, `s` stop all, `1`–`4` toggle individual tools, `c` copy log, `x` clear log, `q` quit.

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
