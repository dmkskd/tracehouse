# Getting Started

:::caution Early Stage
This project is under heavy development. Breaking changes are expected. Saved dashboards and queries are stored in browser localStorage and may be lost after updates.
:::

:::note Platform
This project has only been tested on macOS (Apple Silicon / ARM). It should work on other platforms but your mileage may vary.
:::

## Quick Try

The fastest way to try the app. Requires Docker.

```bash
# Clone the repo
git clone https://github.com/dmkskd/tracehouse.git && cd tracehouse
# Start the app and a local ClickHouse instance
cd infra/quickstart && docker compose up
```

This starts the app and a local ClickHouse instance. Open [http://localhost:8990](http://localhost:8990) - the connection form is pre-filled, just click Connect.

If you already have a ClickHouse instance you want to monitor instead, see [Connecting to ClickHouse](./guides/connecting).

:::note Resource Usage
The app polls ClickHouse system tables at regular intervals. You can review the cost via the built-in Self-Monitoring dashboard (Analytics > Self-Monitoring). See [Deployment - Resource Usage](./guides/deployment) for details.
:::

---

## Development Setup

For working on the code and running the full tooling. Requires Node.js, `just`, and optionally Docker for a local ClickHouse.

### Prerequisites

- **Node.js** 18+ and npm
- **just** command runner ([installation](https://github.com/casey/just#installation))
- **Docker** (optional, for local ClickHouse)
- **uv** Python package manager (for data loading scripts)

:::tip Quick Bootstrap
`./scripts/setup.sh` checks and installs `just`, `uv`, `node`, and `npm`.
Use `./scripts/setup.sh --check` for a dry-run.
:::

### 1. Clone and Install

```bash
git clone https://github.com/dmkskd/tracehouse.git
cd tracehouse
./scripts/setup.sh
```

### 2. Start the App

```bash
just frontend-start
```

Opens the Vite dev server on [http://localhost:5173](http://localhost:5173) with hot reload, and the CORS proxy on `localhost:8990`.

If you already have a ClickHouse instance, enter its host and credentials in the connection form. Otherwise, continue below.

### 3. Start a Local ClickHouse (optional)

| | Local Binary | Docker Compose | Kubernetes (Kind) |
|---|---|---|---|
| **Command** | `just local-start` | `just docker-start` | `just k8s-start` |
| **Startup time** | ~5 seconds | ~15 seconds | 3-5 minutes |
| **Dependencies**| None | Docker | Docker + Kind |
| **ClickHouse topology** | Single node | Single node | 2 shards x 2 replicas |
| **Includes Prometheus + Grafana** | No | Yes | Yes |
| **Best for** | Quick iteration | Day-to-day development | Testing cluster features |

```bash
# Recommended for development
just docker-start
```

:::tip
`just start` starts both Docker Compose infrastructure and the frontend in one go.
:::

### 4. Load Test Data

```bash
just load-data          # All test datasets
just load-data-quick    # Small dataset, faster
```

### 5. Generate Query Activity

```bash
just run-queries        # Ctrl+C to stop
```

See [Generating Activity](./guides/test-data#generating-activity) for options.

### Useful Commands

```bash
just              # List all available commands
just status       # Show status of all services
just stop         # Stop everything
just restart      # Restart all services
just test         # Run all tests
```

## Next Steps

- [Connecting to ClickHouse](./guides/connecting) - connect to existing clusters
- [Architecture](./architecture) - architecture and deployment options
- [Deployment](./guides/deployment) - build and ship for production
- [Loading Test Data](./guides/test-data) - test datasets and data generation
