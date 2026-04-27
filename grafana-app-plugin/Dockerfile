# Multi-stage build for the TraceHouse Grafana plugin.
# Build context must be the repository root.
#
# Usage (standalone):
#   docker build -f grafana-app-plugin/Dockerfile -t tracehouse-plugin-build .
#   docker run --rm -v "$PWD/dist:/output" tracehouse-plugin-build

FROM node:22-alpine AS builder

WORKDIR /app

# Copy root workspace config
COPY package.json package-lock.json ./

# Copy the packages the plugin depends on
COPY packages/core/package.json packages/core/
COPY packages/ui-shared/package.json packages/ui-shared/

# Copy the plugin package.json
COPY grafana-app-plugin/package.json grafana-app-plugin/

# Copy frontend package.json (needed for webpack @frontend/ aliases)
COPY frontend/package.json frontend/

# Install all workspace dependencies
# Delete macOS lock file — npm/cli#4828 causes it to skip Linux optional deps (e.g. lightningcss native bindings)
RUN rm -f package-lock.json && npm install --workspaces --include-workspace-root

# Copy source files
COPY packages/core/ packages/core/
COPY packages/ui-shared/ packages/ui-shared/
COPY grafana-app-plugin/ grafana-app-plugin/
COPY frontend/src/ frontend/src/

# Build in dependency order
RUN npm run build --workspace=packages/core && \
    npm run build --workspace=packages/ui-shared && \
    NODE_OPTIONS="--import tsx" npm run build --workspace=grafana-app-plugin

# Final stage: just the built plugin
FROM alpine:3.20

COPY --from=builder /app/grafana-app-plugin/dist /dist

# Copy dist to /output volume when run
CMD ["cp", "-r", "/dist/.", "/output/"]
