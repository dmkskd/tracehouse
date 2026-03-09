# Deployment

:::tip Use a Read-Only Account
TraceHouse only reads from system tables. Always connect it with a read-only user to limit its access to the cluster.
:::

TraceHouse can be deployed as a standalone binary, a Docker container, a single HTML file, or a Grafana app plugin. Since the app is entirely static (no server-side state), it can be embedded almost anywhere - the single HTML file is the simplest starting point for custom integrations.

|                       | Standalone Binary      | Docker Image           | Single HTML File       | Grafana App Plugin              |
| --------------------- | ---------------------- | ---------------------- | ---------------------- | ------------------------------- |
| **CORS proxy**        | Bundled (transparent)  | Bundled (transparent)  | None (needs CORS)      | Not needed (Grafana is backend) |
| **Server required**   | No (single binary)     | Yes (Docker)           | No (`file://` works)   | Yes (Grafana)                   |
| **ClickHouse config** | None                   | None                   | CORS must be enabled   | Via Grafana data source         |
| **Connection setup**  | In-app UI              | In-app UI              | In-app UI              | Pick Grafana data source        |
| **Offline / air-gap** | Works anywhere         | Needs Docker runtime   | Works anywhere         | Needs Grafana                   |
| **Size**              | ~3 MB                  | ~200 MB                | ~1 MB                  | ~3 MB                           |
| **Best for**          | Production / sharing   | Production deployments | Embedding / sharing    | Teams already using Grafana     |

:::warning Resource Usage on Production Clusters
TraceHouse polls system tables at regular intervals to power its dashboards. While the overhead is typically small, the actual cost depends on your environment - some users are sensitive to network egress, others to CPU time or read I/O on shared clusters.

After deploying, check the **Self-Monitoring dashboard** (Analytics > Self-Monitoring > App Query Cost Details) to see exactly what the app is reading and how often. Different environments have different cost profiles, so review the numbers for your setup. We are constantly reviewing and improving the app's query efficiency.

See the [Polling Reference](/docs/reference/polling) for tuning intervals or disabling specific pollers.
:::

## Standalone Binary

A single ~3 MB binary with the frontend and CORS proxy embedded. No runtime dependencies - just copy and run. Built with Rust using the `rust-embed` crate for asset embedding.

### Build

Requires [Rust](https://rustup.rs/) and Node.js (for the frontend build):

```bash
just dist-binary
# Output: infra/binary/target/release/tracehouse
```

### Run

```bash
./tracehouse
# Available at http://localhost:8990
```

#### Options

```text
-p, --port <PORT>  Port to listen on [default: 8990]
    --insecure     Skip TLS certificate verification for upstream ClickHouse
-h, --help         Print help
-V, --version      Print version
```

The port can also be set via the `TRACEHOUSE_PORT` environment variable. The `--insecure` flag can be set via `TRACEHOUSE_INSECURE=true`.

Or use the justfile shortcut:

```bash
just dist-binary-run
# Available at http://localhost:8990

# Custom port
just dist-binary-run 3000
```

### Connecting to ClickHouse

Enter your ClickHouse host, port, and credentials in the connection form. The proxy handles connectivity - no CORS configuration needed on the ClickHouse side.

:::tip
Use `--insecure` only for self-signed certificates or development. In production, configure proper TLS certificates on your ClickHouse instance.
:::

## Docker Image

The Docker image bundles the frontend and the CORS proxy into a single container. The proxy is always active and transparent - users don't need to configure it. All ClickHouse requests are routed through the co-located proxy automatically.

This is an alternative when Docker is already part of your infrastructure.

### Building the Image

```bash
just dist-docker-build
# Tags as tracehouse:latest

# Custom tag
just dist-docker-build my-registry/tracehouse:v1.0
```

### Running the Container

The container listens on port **8990**. Map it to a host port with `-p`:

```bash
docker run --rm -p 8990:8990 tracehouse:latest
# Available at http://localhost:8990
```

Or use the justfile shortcut:

```bash
just dist-docker-run
# Available at http://localhost:8990

# Custom port
just dist-docker-run tracehouse:latest 3000
```

### Docker Networking

Enter your ClickHouse host, port, and credentials in the connection form. The proxy handles connectivity - no CORS configuration needed on the ClickHouse side.

:::note
If your ClickHouse instance is running on the **host machine** (not inside Docker), use `host.docker.internal` as the host instead of `localhost`. Inside a Docker container, `localhost` refers to the container itself, not the host machine.

The app treats `host.docker.internal` the same as `localhost` - it defaults to HTTP port 8123 without TLS.
:::

### Docker Compose (Production)

```yaml
services:
  tracehouse:
    image: tracehouse:latest
    ports:
      - "8990:8990"
```

## Single HTML File

Build the entire app as one self-contained HTML file - no server needed, works from `file://`:

```bash
just build-single
# Output: frontend/dist/tracehouse.html
```

Open the file directly in a browser. The single HTML file is the best format for embedding into other tools and workflows - it's what the standalone binary uses internally. Also useful for sharing with colleagues, offline usage, and air-gapped environments.

:::note
The single-file build connects directly from the browser (no proxy). Your ClickHouse instance must have CORS enabled, or you'll need to run the proxy separately (`just proxy-start`).
:::

### How It Works

See [Building](../development/building) for details on the build process, bundle composition, and output variants.

## Grafana App Plugin

The same app can be deployed as a Grafana app plugin. In this mode, queries are routed through the [Grafana ClickHouse data source plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) instead of connecting to ClickHouse directly - so users just pick an existing Grafana data source and Grafana handles auth, permissions, and connectivity.

### Prerequisites

- A running Grafana instance (11.x+)
- The [Grafana ClickHouse data source plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) installed and configured with at least one ClickHouse data source

### Build

```bash
# Install dependencies
just grafana-plugin-install

# Build the plugin
just grafana-plugin-build
# Output: grafana-app-plugin/dist/
```

For development with live reload:

```bash
just grafana-plugin-dev
```

### Install into Grafana

Copy the built plugin into your Grafana plugins directory:

```bash
cp -r grafana-app-plugin/dist /var/lib/grafana/plugins/tracehouse-app
```

Then restart Grafana and enable the plugin from the Grafana UI under **Administration → Plugins**.

If you're using the Docker Compose setup from this repo, Grafana is already configured to load the plugin automatically from the local build output.

## Environment Variables

Connection details (host, port, user, password) are configured in the app's connection UI, not via environment variables.

The Docker image uses these internal environment variables (you generally don't need to change them):

| Variable     | Default      | Description                                        |
| ------------ | ------------ | -------------------------------------------------- |
| `PROXY_PORT` | `8990`       | Port the bundled proxy listens on                  |
| `STATIC_DIR` | `/app/static` | Path to frontend static files (set automatically) |

## Troubleshooting

### Port Conflicts

If ports are already in use, stop conflicting services or modify the Docker Compose configuration in `infra/docker/docker-compose.yml`.

### ClickHouse Connection Issues

Ensure ClickHouse is running and accessible:

```bash
curl http://localhost:8123/ping
# Should return "Ok."
```

### Node.js Version Issues

Use a Node.js version manager like `nvm` or `fnm`:

```bash
nvm install 20
nvm use 20
```
