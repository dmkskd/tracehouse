# TraceHouse - Development Commands

# Default: show available commands
default:
    @just --list --unsorted

# ─────────────────────────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────────────────────────

# Show status of all services (Docker, K8s, Local, frontend)
[group('status')]
status:
    #!/usr/bin/env bash
    G='\033[0;32m'; R='\033[0;31m'; Y='\033[1;33m'; D='\033[0;90m'; N='\033[0m'
    up() { echo -e "  ${G}UP${N}  $1 ${D}$2${N}"; }
    down() { echo -e "  ${R}--${N}  $1 ${D}$2${N}"; }
    echo ""
    echo "=== Local Binary ==="
    if [ -f "infra/local/clickhouse.pid" ] && kill -0 $(cat infra/local/clickhouse.pid 2>/dev/null) 2>/dev/null; then
        up "ClickHouse" "localhost:9000 / :8123"
    else
        down "ClickHouse"
    fi
    echo ""
    echo "=== Docker Compose ==="
    if docker compose -f infra/docker/docker-compose.yml ps --status running 2>/dev/null | grep -q "tracehouse-dev"; then
        up "ClickHouse" "localhost:9000 / :8123"
    else
        down "ClickHouse"
    fi
    if docker compose -f infra/docker/docker-compose.yml ps --status running 2>/dev/null | grep -q "tracehouse-prometheus"; then
        up "Prometheus" "localhost:9090"
    else
        down "Prometheus"
    fi
    if docker compose -f infra/docker/docker-compose.yml ps --status running 2>/dev/null | grep -q "tracehouse-grafana"; then
        up "Grafana" "localhost:3001"
    else
        down "Grafana"
    fi
    echo ""
    echo "=== Kubernetes (Kind) ==="
    if kind get clusters 2>/dev/null | grep -q "tracehouse-dev"; then
        up "Kind cluster" "tracehouse-dev"
        CH_READY=$(kubectl get pods -n clickhouse -l app=dev-cluster-clickhouse -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{" "}{end}' 2>/dev/null)
        CH_COUNT=$(echo "$CH_READY" | wc -w | tr -d ' ')
        CH_UP=$(echo "$CH_READY" | grep -o "True" | wc -l | tr -d ' ')
        if [ "$CH_COUNT" -gt 0 ] 2>/dev/null; then
            if [ "$CH_UP" -eq "$CH_COUNT" ]; then
                up "ClickHouse" "${CH_UP}/${CH_COUNT} replicas ready"
            else
                echo -e "  ${Y}!!${N}  ClickHouse ${D}${CH_UP}/${CH_COUNT} replicas ready${N}"
            fi
        else
            down "ClickHouse" "no pods"
        fi
        if kubectl get deployment prometheus -n clickhouse &>/dev/null; then
            PROM_READY=$(kubectl get deployment prometheus -n clickhouse -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
            if [ "${PROM_READY:-0}" -gt 0 ]; then up "Prometheus" "localhost:9090"; else down "Prometheus"; fi
        else
            down "Prometheus"
        fi
        if kubectl get deployment grafana -n clickhouse &>/dev/null; then
            GRAF_READY=$(kubectl get deployment grafana -n clickhouse -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
            if [ "${GRAF_READY:-0}" -gt 0 ]; then up "Grafana" "localhost:3001"; else down "Grafana"; fi
        else
            down "Grafana"
        fi
    else
        down "Kind cluster" "not created"
    fi
    echo ""
    echo "=== App Services ==="
    if pgrep -f "vite" >/dev/null 2>&1; then
        up "Frontend" "localhost:5173"
    else
        down "Frontend"
    fi
    echo ""

# ─────────────────────────────────────────────────────────────────
# SERVICES
# ─────────────────────────────────────────────────────────────────

# Start everything: infra (Docker Compose with all profiles) + frontend
[group('services')]
start: (docker-start-full-bg) (frontend-start-bg)
    @echo ""
    @echo "All services started"
    @echo "  ClickHouse: localhost:9000 / :8123"
    @echo "  Prometheus: localhost:9090"
    @echo "  Grafana:    localhost:3001"
    @echo "  Frontend:   localhost:5173"
    @echo ""
    @echo "Open browser on http://localhost:5173 to access the app"
    @echo ""
    @echo "Status: just status"
    @echo "Stop:   just stop"

# Stop all services + infra
[group('services')]
stop: (frontend-stop) (docker-stop)
    @echo "All services stopped"

# Restart all
[group('services')]
restart: stop start

# Start frontend (foreground - see logs directly)
[group('services')]
frontend-start: install proxy-start
    cd frontend && npm run dev

# Start frontend (background)
[group('services')]
frontend-start-bg: install
    @mkdir -p logs
    @echo "Starting CORS proxy on :8990..."
    @npx tracehouse-proxy 2>&1 | while IFS= read -r line; do echo "$(date '+%Y-%m-%d %H:%M:%S') $line"; done >> logs/proxy.log &
    @echo "Starting frontend..."
    @cd frontend && npm run dev >> ../logs/frontend.log 2>&1 &

# Stop frontend only
[group('services')]
frontend-stop:
    @echo "Stopping frontend..."
    @-pkill -f "vite" 2>/dev/null || true
    @echo "Stopping CORS proxy..."
    @-pkill -f "tracehouse-proxy" 2>/dev/null || true

# Restart frontend only
[group('services')]
frontend-restart: frontend-stop frontend-start

# Start CORS proxy (background) — needed for remote ClickHouse connections
[group('services')]
proxy-start:
    @mkdir -p logs
    @echo "Starting CORS proxy on :8990..."
    @npx tracehouse-proxy 2>&1 | while IFS= read -r line; do echo "$(date '+%Y-%m-%d %H:%M:%S') $line"; done >> logs/proxy.log &

# ─────────────────────────────────────────────────────────────────
# INFRASTRUCTURE (Local Binary - No Containers)
# ─────────────────────────────────────────────────────────────────

# Start ClickHouse locally (no Docker/K8s)
[group('local')]
local-start:
    @./infra/local/setup.sh

# Stop local ClickHouse
[group('local')]
local-stop:
    @./infra/local/stop.sh

# Restart local ClickHouse
[group('local')]
local-restart: local-stop local-start


# ─────────────────────────────────────────────────────────────────
# INFRASTRUCTURE (Docker)
# ─────────────────────────────────────────────────────────────────

# Start ClickHouse + MinIO (fast, no npm builds)
[group('docker')]
docker-start:
    docker compose -f infra/docker/docker-compose.yml up

# Start ClickHouse + MinIO in background (fast, no npm builds)
[group('docker')]
docker-start-bg:
    @docker compose -f infra/docker/docker-compose.yml up -d
    @echo "Waiting for ClickHouse..."
    @sleep 3

# Start everything: ClickHouse + MinIO + Prometheus + Grafana + Tempo (builds Grafana plugin first)
[group('docker')]
docker-start-full: grafana-plugin-build
    docker compose -f infra/docker/docker-compose.yml --profile full up

# Start everything in background (builds Grafana plugin first)
[group('docker')]
docker-start-full-bg: grafana-plugin-build
    @docker compose -f infra/docker/docker-compose.yml --profile full up -d
    @echo "Waiting for ClickHouse..."
    @sleep 3

# Stop docker infrastructure (all profiles)
[group('docker')]
docker-stop:
    @docker compose -f infra/docker/docker-compose.yml --profile full down

# Restart docker infrastructure
[group('docker')]
docker-restart: docker-stop docker-start

# ─────────────────────────────────────────────────────────────────
# INFRASTRUCTURE (Kubernetes / Kind)
# ─────────────────────────────────────────────────────────────────

# Create Kind cluster with ClickHouse Cloud operator (default)
[group('k8s')]
k8s-start:
    @./infra/k8s/setup.sh

# Create Kind cluster with Altinity operator
[group('k8s')]
k8s-start-altinity:
    @./infra/k8s/setup.sh --operator altinity

# Delete Kind cluster
[group('k8s')]
k8s-stop:
    @kind delete cluster --name tracehouse-dev
    @echo "Kind cluster deleted"

# Show cluster status
[group('k8s')]
k8s-status:
    @echo "=== Cluster ===" 
    @kubectl cluster-info --context kind-tracehouse-dev 2>/dev/null || echo "Cluster not running"
    @echo ""
    @echo "=== Pods ==="
    @kubectl get pods -n clickhouse 2>/dev/null || true
    @echo ""
    @echo "=== Services ==="
    @kubectl get svc -n clickhouse 2>/dev/null || true

# Connect to ClickHouse via kubectl exec
[group('k8s')]
k8s-connect:
    @kubectl exec -it -n clickhouse $(kubectl get pod -n clickhouse -l app=dev-cluster-clickhouse -o jsonpath='{.items[0].metadata.name}') -- clickhouse client

# View ClickHouse logs
[group('k8s')]
k8s-logs:
    @kubectl logs -n clickhouse -l app=dev-cluster-clickhouse -f

# View operator logs
[group('k8s')]
k8s-operator-logs:
    @kubectl logs -n clickhouse-operator-system -l control-plane=controller-manager -f

# Restart ClickHouse pods
[group('k8s')]
k8s-restart:
    @kubectl rollout restart statefulset -n clickhouse

# Create Kind cluster with Altinity operator — large (4 shards × 2 replicas, 8 workers)
[group('k8s')]
k8s-start-altinity-large:
    @./infra/k8s/altinity-large/setup.sh

# ─────────────────────────────────────────────────────────────────
# LOGS
# ─────────────────────────────────────────────────────────────────

# View frontend logs
[group('logs')]
logs-frontend:
    @mkdir -p logs
    @touch logs/frontend.log
    @tail -f logs/frontend.log

# View all logs
[group('logs')]
logs:
    @mkdir -p logs
    @touch logs/frontend.log logs/proxy.log
    @tail -f logs/frontend.log logs/proxy.log

# ─────────────────────────────────────────────────────────────────
# TEST DATA
# ─────────────────────────────────────────────────────────────────

# Generate test data (positional args): just generate-data <rows> <partitions> <batch>
# Works with both Docker and K8s (connects via localhost:9000)
# Launch TUI to orchestrate all data tools from a single dashboard
[group('data')]
data-tools-tui *args="":
    uv run --project tools/data-utils tracehouse-data-tools-tui {{args}}

# When args are omitted, the script reads defaults from .env (CH_GEN_*)
[group('data')]
generate-data table="all" *args="":
    #!/usr/bin/env bash
    TABLE_FLAG=""
    case "{{table}}" in
        taxi) TABLE_FLAG="--taxi-only" ;;
        synthetic) TABLE_FLAG="--synthetic-only" ;;
        uk) TABLE_FLAG="--uk-only" ;;
        web) TABLE_FLAG="--web-only" ;;
    esac
    uv run --project tools/data-utils tracehouse-generate $TABLE_FLAG {{args}}

# Generate test data (quick - 1M rows, small batches for many parts)
[group('data')]
generate-data-quick:
    uv run --project tools/data-utils tracehouse-generate --rows 1000000 --partitions 1 --batch-size 10000

# Generate test data (heavy - many small batches to trigger lots of merges)
[group('data')]
generate-data-heavy:
    uv run --project tools/data-utils tracehouse-generate --rows 10000000 --partitions 1 --batch-size 10000

# Run queries to generate activity for monitoring
# When args are omitted, the script reads defaults from .env (CH_QUERY_*)
# Example for heavy load: just run-queries --slow-workers 10 --s3-workers 6 --slow-interval 0.3
[group('data')]
run-queries *args="":
    uv run --project tools/data-utils tracehouse-queries {{args}}

# Run mutations (UPDATE/DELETE) to test mutation monitoring
# When args are omitted, the script reads defaults from .env (CH_MUTATION_*)
[group('data')]
run-mutations *args="":
    uv run --project tools/data-utils tracehouse-mutations {{args}}

# Run heavy mutations only
[group('data')]
run-mutations-heavy *args="":
    uv run --project tools/data-utils tracehouse-mutations --heavy-only {{args}}

# Run lightweight mutations only
[group('data')]
run-mutations-light *args="":
    uv run --project tools/data-utils tracehouse-mutations --lightweight-only {{args}}

# Reset and regenerate test data
[group('data')]
regenerate-data *args="":
    uv run --project tools/data-utils tracehouse-generate --drop {{args}}

# Drop test tables (works with both Docker and K8s via localhost)
[group('data')]
drop-data confirm="":
    uv run --project tools/data-utils tracehouse-drop {{confirm}}

# ─────────────────────────────────────────────────────────────────
# TESTING & QUALITY
# ─────────────────────────────────────────────────────────────────

# Run all tests (frontend + core unit + core integration + scripts)
[group('test')]
test: test-frontend test-core test-core-integration test-data-utils

# Run frontend tests
[group('test')]
test-frontend:
    cd frontend && npm test

# Run core unit tests
[group('test')]
test-core:
    cd packages/core && npx vitest run

# Run core integration tests (requires Docker)
[group('test')]
test-core-integration:
    cd packages/core && npx vitest run --config vitest.integration.config.ts

# Run only tests matching a domain tag (e.g. just test-tag security)
[group('test')]
test-tag tag:
    cd packages/core && npx vitest run --tags-filter="{{tag}}"
    cd frontend && npx vitest run --tags-filter="{{tag}}"

# List all available test tags
[group('test')]
test-list-tags:
    cd packages/core && npx vitest --list-tags
    cd frontend && npx vitest --list-tags

# Print test results grouped by tag (run tests first)
[group('test')]
test-summary:
    #!/usr/bin/env node
    const fs = require('fs');
    const paths = [
      'packages/core/test-reports/results.json',
      'frontend/test-reports/results.json',
    ];
    const byTag = {};
    let total = 0, passed = 0, failed = 0;
    for (const p of paths) {
      if (!fs.existsSync(p)) continue;
      const data = JSON.parse(fs.readFileSync(p, 'utf8'));
      for (const suite of data.testResults || []) {
        for (const t of suite.assertionResults || []) {
          total++;
          if (t.status === 'passed') passed++;
          if (t.status === 'failed') failed++;
          const tags = t.tags?.length ? t.tags : ['untagged'];
          for (const tag of tags) {
            byTag[tag] = byTag[tag] || { passed: 0, failed: 0, skipped: 0 };
            byTag[tag][t.status === 'passed' ? 'passed' : t.status === 'failed' ? 'failed' : 'skipped']++;
          }
        }
      }
    }
    console.log(`\n  Total: ${passed} passed, ${failed} failed, ${total} tests\n`);
    console.log('  Tag                Passed  Failed  Total');
    console.log('  ' + '─'.repeat(45));
    for (const [tag, s] of Object.entries(byTag).sort((a, b) => b[1].passed - a[1].passed)) {
      const t = s.passed + s.failed + s.skipped;
      console.log(`  ${tag.padEnd(19)} ${String(s.passed).padStart(6)}  ${String(s.failed).padStart(6)}  ${String(t).padStart(5)}`);
    }
    console.log('');

# Open HTML test report in browser (run tests first)
[group('test')]
test-report:
    cd packages/core && npx vite preview --outDir test-reports/html --open

# Run Playwright e2e tests (starts ClickHouse + Vite dev server)
[group('test')]
e2e:
    cd packages/e2e && npx playwright test

# Run e2e tests with interactive UI
[group('test')]
e2e-ui:
    cd packages/e2e && npx playwright test --ui

# Run e2e tests with visible browser (slow=ms, default 500)
[group('test')]
e2e-headed slow="500":
    cd packages/e2e && SLOWMO={{slow}} npx playwright test --headed --workers=1

# Run Python data-utils tests (requires Docker)
[group('test')]
test-data-utils:
    cd tools/data-utils && uv run --group test python -m pytest tests/ -v

# Format code
[group('test')]
fmt:
    cd frontend && npm run lint -- --fix

# Run security scan (npm audit + semgrep)
[group('test')]
security-scan:
    #!/usr/bin/env bash
    EXIT=0
    echo "=== npm audit ==="
    npm audit || EXIT=$?
    echo ""
    echo "=== semgrep ==="
    if ! command -v semgrep &>/dev/null; then
        echo "semgrep is not installed."
        echo "Install it with: ./scripts/setup.sh --security"
        EXIT=1
    else
        semgrep --config auto packages/ frontend/src/ || EXIT=$?
    fi
    exit $EXIT

# ─────────────────────────────────────────────────────────────────
# GRAFANA APP PLUGIN
# ─────────────────────────────────────────────────────────────────

# Build the Grafana app plugin (full TraceHouse)
[group('grafana-app')]
grafana-plugin-build: install grafana-plugin-install
    @echo "Building Grafana app plugin..."
    cd grafana-app-plugin && npm run build
    @echo "Grafana app plugin built → grafana-app-plugin/dist/"

# Dev build with watch mode
[group('grafana-app')]
grafana-plugin-dev: install grafana-plugin-install
    cd grafana-app-plugin && npm run dev

# Install Grafana plugin dependencies
[group('grafana-app')]
grafana-plugin-install:
    cd grafana-app-plugin && npm install

# ─────────────────────────────────────────────────────────────────
# SINGLE FILE BUILD
# ─────────────────────────────────────────────────────────────────

# Build the entire app as a single self-contained HTML file (works from file://)
[group('dist')]
build-single:
    cd frontend && npm run build:single

# ─────────────────────────────────────────────────────────────────
# BUILD
# ─────────────────────────────────────────────────────────────────

# Build all workspaces in dependency order (core → ui-shared → frontend)
[group('build')]
build:
    npm run build --workspace=packages/core
    npm run build --workspace=packages/ui-shared
    npm run build --workspace=frontend

# ─────────────────────────────────────────────────────────────────
# DISTRIBUTION (Docker)
# ─────────────────────────────────────────────────────────────────

# Build frontend static files to frontend/dist/
[group('dist')]
dist-frontend:
    cd frontend && npm run build

# Build distributable Docker image (frontend + proxy bundled)
[group('dist')]
dist-docker-build tag="tracehouse:latest":
    docker build -f infra/docker/Dockerfile -t "{{tag}}" .

# Run distributable Docker image
[group('dist')]
dist-docker-run tag="tracehouse:latest" port="8990":
    docker run --rm -p "{{port}}:8990" "{{tag}}"

# Build + run distributable Docker image
[group('dist')]
dist-docker tag="tracehouse:latest" port="8990": (dist-docker-build tag)
    @echo "App available at http://localhost:{{port}}"
    docker run --rm -p "{{port}}:8990" "{{tag}}"

# ─────────────────────────────────────────────────────────────────
# DISTRIBUTION (Binary)
# ─────────────────────────────────────────────────────────────────

# Build the standalone binary (frontend embedded + CORS proxy)
[group('dist')]
dist-binary: dist-binary-frontend
    cd infra/binary && cargo build --release
    @echo ""
    @ls -lh infra/binary/target/release/tracehouse
    @echo ""
    @echo "Binary ready: infra/binary/target/release/tracehouse"

# Build only the frontend for binary embedding
[group('dist')]
dist-binary-frontend: build
    cd frontend && TH_BUNDLED_PROXY=true npx vite build --config vite.singlefile.config.ts

# Run the standalone binary
[group('dist')]
dist-binary-run port="8990": dist-binary
    @echo "App available at http://localhost:{{port}}"
    ./infra/binary/target/release/tracehouse --port {{port}}

# Build all release artifacts (binary + single HTML) into release/
[group('dist')]
dist-release:
    ./scripts/build-release.sh

# ─────────────────────────────────────────────────────────────────
# DOCUMENTATION
# ─────────────────────────────────────────────────────────────────

# Build the documentation site (static output in docs/site/build)
[group('docs')]
docs-build: docs-install
    cd docs/site && npm run build

# Start documentation dev server
[group('docs')]
docs-dev: docs-install
    cd docs/site && npm start

# Install documentation dependencies
[group('docs')]
docs-install:
    cd docs/site && npm install

# Serve the built documentation site locally
[group('docs')]
docs-serve: docs-install
    cd docs/site && npm run serve

# ─────────────────────────────────────────────────────────────────
# RELEASE
# ─────────────────────────────────────────────────────────────────

# Bump version in root/plugin package.json + plugin.json (e.g. `just bump 0.10.0` or `just bump 0.10.0-rc.1`)
[group('release')]
bump VERSION:
    npm pkg set version={{VERSION}}
    npm pkg set version={{VERSION}} --workspace=dmkskd-tracehouse-app
    node -e "const f='grafana-app-plugin/src/plugin.json';const fs=require('fs');const j=JSON.parse(fs.readFileSync(f));j.info.version='{{VERSION}}';fs.writeFileSync(f,JSON.stringify(j,null,2)+'\n')"
    @echo "Bumped to {{VERSION}}. Review, commit, then: git tag v{{VERSION}} && git push --tags"

# Bump main to next minor -dev (run right after tagging — e.g. after v0.9.0 → 0.10.0-dev)
[group('release')]
bump-dev:
    #!/usr/bin/env bash
    set -euo pipefail
    CURRENT=$(node -p "require('./package.json').version")
    BASE=${CURRENT%%-*}
    IFS=. read -r major minor _patch <<< "$BASE"
    NEXT="$major.$((minor+1)).0-dev"
    just bump "$NEXT"

# Verify root/plugin package.json + plugin.json all agree (use in CI / pre-commit)
[group('release')]
version-check:
    #!/usr/bin/env bash
    set -euo pipefail
    ROOT=$(node -p "require('./package.json').version")
    PLUG_PKG=$(node -p "require('./grafana-app-plugin/package.json').version")
    PLUG_JSON=$(node -p "require('./grafana-app-plugin/src/plugin.json').info.version")
    if [[ "$ROOT" != "$PLUG_PKG" || "$ROOT" != "$PLUG_JSON" ]]; then
      echo "Version mismatch: root=$ROOT plugin-pkg=$PLUG_PKG plugin.json=$PLUG_JSON" >&2
      exit 1
    fi
    echo "All versions match: $ROOT"

# ─────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────

# Install dependencies
[group('setup')]
install:
    npm install
    npm run build --workspace=packages/core
    npm run build --workspace=packages/ui-shared
