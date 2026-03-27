# ── Build stage ──────────────────────────────────────────────────
FROM node:22-alpine AS build

WORKDIR /app

# Copy workspace root + package manifests first (layer cache)
COPY package.json package-lock.json* ./
COPY packages/core/package.json packages/core/
COPY packages/ui-shared/package.json packages/ui-shared/
COPY packages/proxy/ packages/proxy/
COPY frontend/package.json frontend/

# Install all workspace deps
# Delete macOS lock file — npm/cli#4828 causes it to skip Linux optional deps (e.g. rollup native bindings)
# Note: proxy source is copied above because its `prepare` script runs `tsc` during install
RUN rm -f package-lock.json && npm install --workspaces --include-workspace-root

# Copy source for workspace packages + frontend
COPY packages/ packages/
COPY frontend/ frontend/

# Build workspace packages (order matters: core → ui-shared → proxy)
RUN npm run build --workspace=packages/core
RUN npm run build --workspace=packages/ui-shared
RUN npm run build --workspace=@tracehouse/proxy

# Build frontend with bundled proxy flag.
# DEFAULT_CH_* args pre-fill the connection form and optionally auto-connect.
ARG DEFAULT_CH_HOST=""
ARG DEFAULT_CH_PORT=""
ARG DEFAULT_CH_USER=""
ARG DEFAULT_CH_PASSWORD=""
ARG DEFAULT_CH_SECURE=""
ARG DEFAULT_CH_CLUSTER=""
ARG AUTO_CONNECT=""
RUN cd frontend && VITE_BUNDLED_PROXY=true \
    VITE_DEFAULT_CH_HOST="${DEFAULT_CH_HOST}" \
    VITE_DEFAULT_CH_PORT="${DEFAULT_CH_PORT}" \
    VITE_DEFAULT_CH_USER="${DEFAULT_CH_USER}" \
    VITE_DEFAULT_CH_PASSWORD="${DEFAULT_CH_PASSWORD}" \
    VITE_DEFAULT_CH_SECURE="${DEFAULT_CH_SECURE}" \
    VITE_DEFAULT_CH_CLUSTER="${DEFAULT_CH_CLUSTER}" \
    VITE_AUTO_CONNECT="${AUTO_CONNECT}" \
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
