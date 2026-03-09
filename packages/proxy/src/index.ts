#!/usr/bin/env node
/**
 * ClickHouse CORS Proxy
 *
 * A lightweight local proxy that forwards browser requests to a remote
 * ClickHouse server, bypassing CORS restrictions. The frontend sends
 * requests to this proxy instead of directly to ClickHouse.
 *
 * Routes:
 *   POST /proxy/query   — forward a ClickHouse query
 *   POST /proxy/command — forward a ClickHouse command (no result)
 *   GET  /proxy/ping    — health check
 *
 * The target ClickHouse connection details are sent in request headers:
 *   x-ch-host, x-ch-port, x-ch-user, x-ch-password, x-ch-database, x-ch-secure
 *
 * This avoids storing connection state on the proxy — the frontend
 * remains the source of truth for connection profiles.
 */

import path from 'path';
import express from 'express';
import cors from 'cors';
import { createProxyMiddleware } from 'http-proxy-middleware';

const DEFAULT_PORT = 8990;

const CH_HEADERS = [
  'x-ch-host',
  'x-ch-port',
  'x-ch-user',
  'x-ch-password',
  'x-ch-database',
  'x-ch-secure',
];

export function createApp() {
  const app = express();

  app.use(cors());

  // Health endpoints — registered before the proxy middleware
  app.get('/proxy/ping', (_req, res) => {
    res.json({ status: 'ok' });
  });

  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', service: 'clickhouse-cors-proxy' });
  });

  // Serve bundled frontend static files when STATIC_DIR is set (Docker image)
  const staticDir = process.env.STATIC_DIR;
  if (staticDir) {
    const resolved = path.resolve(staticDir);
    app.use(express.static(resolved));
  }

  // Proxy middleware — streams requests/responses to ClickHouse
  app.use(
    '/proxy',
    createProxyMiddleware({
      // Suppress per-request logging from http-proxy-middleware
      logger: { info: () => {}, warn: console.warn, error: console.error } as any,
      router: (req) => {
        const host = req.headers['x-ch-host'] as string;
        if (!host) throw new Error('Missing x-ch-host header');
        const port = (req.headers['x-ch-port'] as string) || '8443';
        const secure = (req.headers['x-ch-secure'] as string) !== 'false';
        return `${secure ? 'https' : 'http'}://${host}:${port}`;
      },
      changeOrigin: true,
      // Rewrite /proxy/query?... → /?... (ClickHouse HTTP API is at the root)
      pathRewrite: (path) => {
        const qIndex = path.indexOf('?');
        return qIndex === -1 ? '/' : '/' + path.slice(qIndex);
      },
      on: {
        proxyReq: (proxyReq, req) => {
          // Auth via ClickHouse native headers (not query params)
          proxyReq.setHeader('X-ClickHouse-User', (req.headers['x-ch-user'] as string) || 'default');
          proxyReq.setHeader('X-ClickHouse-Key', (req.headers['x-ch-password'] as string) || '');
          proxyReq.setHeader('X-ClickHouse-Database', (req.headers['x-ch-database'] as string) || 'default');

          // Rename format → default_format (ClickHouse's parameter name)
          const url = new URL(proxyReq.path, 'http://placeholder');
          const format = url.searchParams.get('format');
          if (format) {
            url.searchParams.delete('format');
            url.searchParams.set('default_format', format);
          }

          proxyReq.path = url.pathname + url.search;

          // Strip proxy-specific headers — no need to send credentials to ClickHouse as headers
          for (const h of CH_HEADERS) {
            proxyReq.removeHeader(h);
          }
          // Remove client-supplied Authorization header to prevent bypassing or confusing auth
          proxyReq.removeHeader('Authorization');
        },
        error: (err, _req, res) => {
          const msg = err instanceof Error ? err.message : String(err);
          console.error('[proxy]', msg);
          (res as express.Response).status(502).json({ error: msg });
        },
      },
    }),
  );

  // SPA fallback — serve index.html for any non-API route (client-side routing)
  if (staticDir) {
    const resolved = path.resolve(staticDir);
    app.get('*', (_req, res) => {
      res.sendFile(path.join(resolved, 'index.html'));
    });
  }

  return app;
}

function main() {
  const port = parseInt(process.env.PROXY_PORT ?? String(DEFAULT_PORT), 10);
  const app = createApp();

  app.listen(port, () => {
    console.log(`[clickhouse-proxy] CORS proxy listening on http://localhost:${port}`);
    console.log(`[clickhouse-proxy] Frontend should set proxy URL to http://localhost:${port}/proxy`);
  });
}

// Only auto-start when run directly (not when imported by tests).
// Vitest sets VITEST=true; also check for common direct-run patterns.
if (!process.env.VITEST) {
  main();
}
