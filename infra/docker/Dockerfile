# ── Build stage ──────────────────────────────────────────────────
FROM node:22-alpine AS build

WORKDIR /app

# Copy workspace root + package manifests first (layer cache)
COPY package.json package-lock.json* ./
COPY packages/core/package.json packages/core/
COPY packages/ui-shared/package.json packages/ui-shared/
COPY packages/proxy/package.json packages/proxy/
COPY frontend/package.json frontend/

# Install all workspace deps
# Delete macOS lock file — npm/cli#4828 causes it to skip Linux optional deps (e.g. rollup native bindings)
RUN rm -f package-lock.json && npm install --workspaces --include-workspace-root

# Copy source for workspace packages + frontend
COPY packages/ packages/
COPY frontend/ frontend/

# Build workspace packages (order matters: core → ui-shared → proxy)
RUN npm run build --workspace=packages/core
RUN npm run build --workspace=packages/ui-shared
RUN npm run build --workspace=@tracehouse/proxy

# Build frontend with bundled proxy flag.
# DEFAULT_CH_HOST pre-fills the connection form (e.g. 'clickhouse' in docker-compose).
ARG DEFAULT_CH_HOST=""
RUN cd frontend && VITE_BUNDLED_PROXY=true \
    VITE_DEFAULT_CH_HOST="${DEFAULT_CH_HOST}" \
    npm run build

# ── Runtime stage ────────────────────────────────────────────────
FROM node:22-alpine

WORKDIR /app

# Copy built proxy + its dependencies
COPY --from=build /app/packages/proxy/dist/ ./proxy/
COPY --from=build /app/packages/proxy/package.json ./proxy/
COPY --from=build /app/node_modules/ ./node_modules/

# Copy built frontend static files
COPY --from=build /app/frontend/dist ./static/

ENV STATIC_DIR=/app/static
ENV PROXY_PORT=8990

EXPOSE 8990

CMD ["node", "proxy/index.js"]
