# Project Structure

Detailed reference of the monorepo layout and key files.

## Root

```
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

```
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

```
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

## Infrastructure (`infra/`)

```
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
    ├── setup_test_data.py         # Data loading orchestrator
    ├── run_queries.py             # Query load generator
    ├── run_mutations.py           # Mutation load generator
    └── tables/                    # Per-dataset loading scripts
```

## Development Commands

Run `just` to see all available commands, organized by group:

| Group | Commands |
|-------|----------|
| `services` | `start`, `stop`, `restart`, `frontend-start` |
| `docker` | `docker-start`, `docker-start-full`, `docker-stop` |
| `k8s` | `k8s-start`, `k8s-stop`, `k8s-status` |
| `local` | `local-start`, `local-stop` |
| `data` | `load-data`, `run-queries`, `run-mutations`, `drop-data` |
| `test` | `test`, `test-frontend`, `test-core`, `test-core-integration` |
| `build` | `build`, `dist-frontend`, `dist-docker-build` |
