# Local ClickHouse Setup (Single Binary)

Run ClickHouse directly on your machine without Docker or Kubernetes.

## Quick Start

```bash
# One-liner setup and start
./setup.sh
```

## Manual Installation

### macOS
```bash
# Using Homebrew
brew install clickhouse

# Or download binary directly
curl https://clickhouse.com/ | sh
```

### Linux
```bash
# Using official installer
curl https://clickhouse.com/ | sh

# Or using apt (Debian/Ubuntu)
sudo apt-get install -y apt-transport-https ca-certificates
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
sudo apt-get install -y clickhouse-server clickhouse-client
```

## Directory Structure

```
local/
├── config/           # ClickHouse config overrides (same as Docker/K8s)
│   ├── listen.xml
│   ├── opentelemetry.xml
│   ├── prometheus.xml
│   └── text_log.xml
├── data/             # ClickHouse data directory (created on first run)
├── logs/             # ClickHouse logs (created on first run)
├── setup.sh          # Setup and start script
└── stop.sh           # Stop script
```

## Configuration

The config files in `config/` are identical to those used in Docker Compose and K8s deployments:

- `listen.xml` - Listen on all interfaces (0.0.0.0)
- `opentelemetry.xml` - Enable OpenTelemetry tracing for all queries
- `prometheus.xml` - Expose Prometheus metrics on port 9363
- `text_log.xml` - Enable system.text_log and opentelemetry_span_log

## Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 9000 | Native   | ClickHouse native protocol (clickhouse-driver) |
| 8123 | HTTP     | HTTP interface |
| 9363 | HTTP     | Prometheus metrics |
| 9009 | HTTP     | Interserver (replication) |

## Usage

```bash
# Start ClickHouse
./setup.sh

# Stop ClickHouse
./stop.sh

# Connect with client
clickhouse-client

# Check status
clickhouse-client --query "SELECT 1"
```

## Data Persistence

Data is stored in `./data/` and logs in `./logs/`. These directories are gitignored.

To reset all data:
```bash
./stop.sh
rm -rf data/ logs/
./setup.sh
```
