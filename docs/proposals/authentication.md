# Authentication hardening

Status: proposal, not built. Captured 2026-10-24.

Prompted by the Altinity SQL Browser
([blog](https://altinity.com/blog/the-altinity-sql-browser-powerful-lightweight-and-secure),
[repo](https://github.com/Altinity/altinity-sql-browser)), which runs a browser
SQL client against ClickHouse without the app ever holding a database password.

## Why

How TraceHouse authenticates today:

| Area | Current behaviour | Where |
|---|---|---|
| Credential type | ClickHouse user + password per connection profile | `packages/core/src/types/connection.ts` |
| Password storage | User picks `persistent` (plaintext in localStorage), `session` (sessionStorage) or `memory` | `frontend/src/stores/connectionStore.ts` |
| Proxy | Forwards to any host named in `x-ch-host`; password travels in `x-ch-password` on every request | `packages/proxy/src/index.ts`, `infra/binary/src/lib.rs` |
| Proxy CORS | `cors()` with no options: any origin may call the proxy | `packages/proxy/src/index.ts:41` |
| CSP | None on the served frontend | frontend, proxy, Rust binary |
| Demo credentials | `TH_DEFAULT_CH_PASSWORD` compiled into the JS bundle | `frontend/vite.buildDefines.ts:26` |
| Grafana plugin | Auth delegated to the Grafana datasource; no password in the browser | `packages/core/src/adapters/grafana-adapter.ts` |

Consequences:

1. **Shared, long-lived secrets.** Most deployments use one `monitoring_user`
   password, stored in the browser. `query_log` cannot attribute queries to a
   person, and rotating the password means re-entering it in every browser.
2. **The proxy can reach any host and any website can call it.** A deployed
   TraceHouse container forwards requests to any host it can reach, including
   internal ones, for any origin that calls it.
3. **No limit on where the page may send data.** Imported dashboards and the
   Query Explorer run user-supplied SQL, and the page holds credentials. A CSP
   `connect-src` limits what injected code could send out and to where.

## What Altinity does

- Single HTML file served from ClickHouse's own HTTP handler (`user_files`), so
  the page and the queries share one origin; no CORS proxy.
- OAuth2 Authorization Code + PKCE against the user's IdP (Google, Okta, ...),
  configured in a static `config.json`.
- Sends the id_token (or the access_token, when an audience is configured) as
  `Authorization: Bearer <jwt>`. ClickHouse validates it with
  `<token_processors>` (JWKS) or a delegated `<http_authentication_servers>`
  verifier, and runs the query as that user with their own roles and grants.
- For servers without token validation, the JWT is sent as the Basic-auth
  password and checked by a separate verifier (`ch-jwt-verify`).
- Token stored in sessionStorage.
- Strict CSP: `default-src 'none'`, `connect-src 'self'` plus IdP origins,
  `frame-ancestors 'none'`, `base-uri 'none'`, plus `X-Content-Type-Options:
  nosniff` and `Referrer-Policy: no-referrer`.

## Proposal

Ordered by value for effort. Each item can ship on its own.

### 1. Lock down the proxy

- `TH_PROXY_ALLOWED_HOSTS` (comma-separated hosts or `host:port`). When set,
  requests to other targets get `403`. Unset keeps today's behaviour, so local
  dev is unchanged.
- When the proxy also serves the frontend (`STATIC_DIR` set, the Docker image),
  restrict CORS to the same origin by default. `TH_PROXY_CORS_ORIGINS`
  overrides it.
- Implement in both proxies: Node (`packages/proxy`) and Rust (`infra/binary`).

### 2. Security headers and CSP on the served app

Served by the Docker image / Rust binary:

```
Content-Security-Policy: default-src 'self'; connect-src 'self' <configured CH hosts> <IdP origins>;
  img-src 'self' data:; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'
X-Content-Type-Options: nosniff
Referrer-Policy: no-referrer
```

`connect-src` comes from config. Connecting directly from the browser (CORS
mode) to arbitrary hosts conflicts with a fixed `connect-src`. Either the
container sets the allowed hosts once (matches item 1), or `connect-src`
falls back to `'self'` and all traffic goes through the proxy. Grafana plugin
is out of scope (Grafana sets its own headers).

### 3. Bearer token connections

New auth mode on a connection profile: `password` (today) | `bearer`.

- Phase A (small): user pastes a token; adapters send
  `Authorization: Bearer <token>` instead of user/password. Proxy forwards an
  `x-ch-authorization` header as `Authorization`. Expired-token errors map to
  the existing `authentication` error category with a "token expired" message.
- Phase B: built-in OIDC login (Authorization Code + PKCE) using a runtime
  config file (`issuer`, `client_id`, optional `audience`, preset clusters),
  similar to Altinity's `config.json`. Token kept in memory or sessionStorage
  only, never localStorage. Silent refresh if the IdP issues refresh tokens to
  public clients, otherwise re-login on expiry.

Benefit: per-user identity in `query_log`, ClickHouse RBAC applied per person,
no shared password in browsers.

Layering: token acquisition is frontend (`stores/`, `providers/`); header
construction stays in the core adapters.

### 4. Storage defaults and labelling

- Default `CredentialStorageMode` is already `session`; keep it and label
  `persistent` in the UI as "stored unencrypted in this browser".
- Document that `TH_DEFAULT_CH_PASSWORD` is visible to anyone who loads the
  page, and should only be used for demo/read-only accounts.

### Not proposed

- Serving TraceHouse from ClickHouse `user_files` / `http_handlers`. It removes
  the proxy for self-managed clusters but needs write access to server config
  and does not fit Cloud or Aiven. Worth a deployment-guide section, not code.

## Open questions

- **Server support for bearer JWTs.** Altinity's README relies on
  `<token_processors>` / `<http_authentication_servers>`. Which ClickHouse
  versions (OSS, Cloud, Altinity builds) accept `Authorization: Bearer`
  natively has not been verified yet. Needs checking against ClickHouse docs and
  the compat matrix before scoping item 3; it decides whether Phase A is useful
  on its own or needs the Basic-auth fallback.
- Does `@clickhouse/client-web` (browser adapter) accept a custom
  `Authorization` header without also sending Basic auth? Check before
  implementing item 3.
- Item 2 vs direct CORS connections: is restricting `connect-src` acceptable
  for the Docker image, given users can enter any host today?

## Rough size

| Item | Estimate |
|---|---|
| 1. Proxy allowlist + CORS (Node + Rust) | ~150 lines + integration tests |
| 2. Headers + CSP | ~60 lines + config plumbing |
| 3A. Pasted bearer token | ~200 lines (types, adapters, proxy, form) |
| 3B. OIDC + PKCE login | ~600+ lines, new runtime config |
| 4. Labels + docs | small |
