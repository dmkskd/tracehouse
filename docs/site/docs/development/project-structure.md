# Project Structure

TraceHouse is a monorepo with a shared TypeScript core, a React frontend, and flexible infrastructure options.

```
tracehouse/
├── frontend/              # React + Three.js + Vite frontend
│   └── src/
│       ├── components/    # UI components by domain
│       ├── pages/         # Route-level page components
│       ├── stores/        # Zustand state management
│       └── services/      # API and data fetching
├── packages/
│   ├── core/              # Shared TypeScript library
│   │   ├── adapters/      # ClickHouse connection adapters
│   │   └── services/      # Business logic and queries
│   ├── ui-shared/         # Shared UI components
│   └── proxy/             # CORS proxy (Express, stateless)
├── grafana-app-plugin/    # Grafana app plugin (full UI)
├── infra/
│   ├── quickstart/        # All-in-one quickstart (app + ClickHouse)
│   ├── docker/            # Docker Compose setup
│   ├── k8s/               # Kubernetes manifests
│   ├── local/             # Local binary setup
│   └── scripts/           # Data loading and test scripts
├── docs/                  # Documentation
└── justfile               # Development commands
```

## Core Library (`packages/core`)

The core library contains all ClickHouse query logic and data transformation. It's shared between the frontend and Grafana plugins.

Key components:
- **Adapters** - Connection management for single-node and cluster topologies
- **Services** - Domain-specific query builders (merges, queries, parts, metrics)
- **Types** - Shared TypeScript interfaces

## Frontend (`frontend/`)

Built with:
- **React 18** - UI framework
- **Vite** - Build tool and dev server
- **Three.js** (via React Three Fiber) - 3D visualizations
- **Zustand** - State management
- **Recharts** - 2D charts
- **D3** - Advanced visualizations (flame graphs, etc.)
- **Tailwind CSS** - Styling

### Component Organization

Components are organized by domain:

| Directory | Purpose |
|-----------|---------|
| `components/cluster/` | Cluster topology and overview |
| `components/database/` | Database tree, table detail, parts |
| `components/merge/` | Merge tracking and visualization |
| `components/query/` | Query monitoring and analysis |
| `components/engine-internals/` | Thread pools, memory, CPU sampling |
| `components/analytics/` | Dashboards and custom queries |
| `components/3d/` | Three.js 3D visualizations |
| `components/connection/` | Connection management UI |
