# TraceHouse

An open-source tool to visually explore and monitor ClickHouse. [Documentation](https://dmkskd.github.io/tracehouse/)

> **Early stage.** This project is under heavy development. Breaking changes are expected. Saved dashboards and queries are stored in browser localStorage and may be lost after updates.

## Quickstart

Requires only Docker.

```bash
git clone https://github.com/dmkskd/tracehouse.git
cd tracehouse/infra/quickstart
docker compose up
```

Open [http://localhost:8990](http://localhost:8990) - the connection form is pre-filled, just click Connect.

## Development Setup

For working on the code. Requires Node.js 18+, [just](https://github.com/casey/just), and optionally Docker.

```bash
./scripts/setup.sh        # Bootstrap dependencies
just frontend-start       # http://localhost:5173 (hot reload)
just docker-start         # Start local ClickHouse + Prometheus + Grafana
```

See the [Getting Started guide](docs/site/docs/getting-started.md) for full setup options including Kubernetes.

## Project Structure

```text
├── frontend/         React + Three.js frontend
├── packages/core/    TypeScript core library (ClickHouse queries & services)
├── packages/proxy/   CORS proxy for ClickHouse HTTP API
├── infra/            Infrastructure (Docker Compose, K8s, quickstart)
├── docs/             Documentation site
└── justfile          Development commands (run `just` to see all)
```

## Commands

```bash
just                  # List all commands
just start            # Start infra + frontend
just stop             # Stop all services
just test             # Run tests
just generate-data    # Generate test data into ClickHouse
```
