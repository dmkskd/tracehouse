# Project Structure

Detailed reference of the monorepo layout and key files.

## Root

```text
├── package.json           # npm workspaces root
├── justfile               # Development commands (run `just` to list)
├── .env                   # Environment configuration
├── .env.example           # Template for environment variables
├── .env.aiven             # Aiven ClickHouse Cloud config
├── .env.clickhouse        # ClickHouse-specific settings
└── scripts/
    └── setup.sh           # Bootstrap script
```

## Frontend (`frontend/`)

```text
frontend/
├── src/
│   ├── App.tsx                    # Root app component with routing
│   ├── components/
│   │   ├── 3d/                    # Three.js 3D visualizations
│   │   ├── analytics/             # Dashboards, charts, query explorer
│   │   ├── cluster/               # Cluster topology views
│   │   ├── common/                # Shared UI components
│   │   ├── connection/            # Connection management
│   │   ├── database/              # Database tree, table detail, parts
│   │   ├── engine-internals/      # Thread pools, memory, CPU
│   │   ├── merge/                 # Merge tracking and visualization
│   │   ├── metrics/               # Metric cards and time series
│   │   └── query/                 # Query monitoring
│   ├── pages/                     # Route-level page components
│   ├── stores/                    # Zustand state stores
│   └── services/                  # API layer
├── index.html
├── package.json
├── vite.config.ts
└── tailwind.config.ts
```

## Core Library (`packages/core/`)

```text
packages/core/
├── src/
│   ├── adapters/
│   │   ├── cluster-adapter.ts     # Multi-node cluster connections
│   │   └── host-targeted-adapter.ts  # Single-node connections
│   ├── services/                  # Domain query builders
│   └── index.ts                   # Public API
├── package.json
└── vitest.config.ts
```

## Data Utils (`tools/data-utils/`)

Python package for data loading, query workload generation, and mutation testing.

```text
tools/data-utils/
├── pyproject.toml                     # Package config + entry points
├── src/data_utils/
│   ├── capabilities.py                # ClickHouse capability probing
│   ├── env.py                         # Shared connection/env helpers
│   ├── cli/
│   │   ├── generate.py                # tracehouse-generate entry point
│   │   ├── queries.py                 # tracehouse-queries entry point
│   │   ├── mutations.py               # tracehouse-mutations entry point
│   │   └── merge_triggers.py          # tracehouse-merge-triggers entry point
│   └── tables/
│       ├── protocol.py                # Dataset Protocol + QuerySet
│       ├── synthetic_data.py          # synthetic_data.events
│       ├── nyc_taxi.py                # nyc_taxi.trips
│       ├── uk_house_prices.py         # uk_price_paid.uk_price_paid
│       ├── web_analytics.py           # web_analytics.pageviews
│       └── dimensions.py              # Dimension/lookup tables
└── tests/                             # Integration tests (testcontainers)
```

## Infrastructure (`infra/`)

```text
infra/
├── quickstart/
│   └── docker-compose.yml         # All-in-one quickstart (app + ClickHouse)
├── docker/
│   ├── docker-compose.yml         # ClickHouse + Prometheus + Grafana
│   └── custom-clickhouse/         # Custom ClickHouse Docker image
├── k8s/
│   ├── setup.sh                   # Kind cluster setup
│   └── clickhouse-cluster.yaml    # ClickHouse K8s manifests
├── local/
│   ├── setup.sh                   # Local binary setup
│   └── config/                    # ClickHouse config files
└── scripts/
    ├── setup_jaeger_export.sql    # Jaeger trace export setup
    ├── setup_read_only_user.sql   # Read-only user setup
    └── uk_house_queries.sql       # Example UK house price queries
```

## Development Commands

Run `just` to see all available commands, organized by group:

| Group | Commands |
|-------|----------|
| `services` | `start`, `stop`, `restart`, `frontend-start` |
| `docker` | `docker-start`, `docker-start-full`, `docker-stop` |
| `k8s` | `k8s-start`, `k8s-stop`, `k8s-status` |
| `local` | `local-start`, `local-stop` |
| `data` | `data-tools-tui`, `generate-data`, `run-queries`, `run-mutations`, `drop-data` |
| `test` | `test`, `test-frontend`, `test-core`, `test-core-integration`, `test-data-utils` |
| `build` | `build`, `dist-frontend`, `dist-docker-build` |
