/**
 * Integration tests for the CORS proxy.
 *
 * Spins up a real ClickHouse via testcontainers and the proxy on an
 * ephemeral port, then verifies the full request chain:
 *   client → proxy → ClickHouse → proxy → client
 *
 * Set CH_TEST_URL to skip testcontainers and use an existing instance.
 */

import fs from 'fs';
import http from 'http';
import os from 'os';
import path from 'path';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ClickHouseContainer, type StartedClickHouseContainer } from '@testcontainers/clickhouse';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import { createApp } from '../index.js';

const CH_IMAGE = 'clickhouse/clickhouse-server:26.1-alpine';
const CONTAINER_TIMEOUT = 120_000;

// Filled in beforeAll (alongside chHost/chPort)
let chUser: string;
let chPassword: string;

/** Headers the frontend's ProxyAdapter sends on every request. */
function chHeaders(overrides: Partial<Record<string, string>> = {}) {
  return {
    'x-ch-host': chHost,
    'x-ch-port': String(chPort),
    'x-ch-user': chUser,
    'x-ch-password': chPassword,
    'x-ch-database': 'default',
    'x-ch-secure': 'false',
    ...overrides,
  };
}

/** POST to the proxy and return { resp, text } for easier debugging. */
async function proxyPost(
  path: string,
  body: string,
  headers: Record<string, string> = chHeaders(),
) {
  const resp = await fetch(`${proxyBaseUrl}${path}`, {
    method: 'POST',
    headers,
    body,
  });
  const text = await resp.text();
  return { resp, text };
}

// Filled in beforeAll
let container: StartedClickHouseContainer | null = null;
let chHost: string;
let chPort: number;
let proxyServer: Server;
let proxyBaseUrl: string;

beforeAll(async () => {
  // --- ClickHouse ---
  const externalUrl = process.env.CH_TEST_URL;
  if (externalUrl) {
    const url = new URL(externalUrl);
    chHost = url.hostname;
    chPort = parseInt(url.port || '8123', 10);
    chUser = url.username || 'default';
    chPassword = url.password || '';
  } else {
    container = await new ClickHouseContainer(CH_IMAGE).start();
    chHost = container.getHost();
    chPort = container.getMappedPort(8123);
    chUser = container.getUsername();
    chPassword = container.getPassword();
  }

  // Verify ClickHouse is reachable directly before testing the proxy
  const directResp = await fetch(`http://${chHost}:${chPort}/ping`);
  expect(directResp.ok).toBe(true);

  // --- Proxy ---
  const app = createApp();
  proxyServer = await new Promise<Server>((resolve) => {
    const s = app.listen(0, () => resolve(s)); // port 0 = ephemeral
  });
  const addr = proxyServer.address() as AddressInfo;
  proxyBaseUrl = `http://localhost:${addr.port}`;
}, CONTAINER_TIMEOUT);

afterAll(async () => {
  if (proxyServer) {
    await new Promise<void>((resolve, reject) =>
      proxyServer.close((err) => (err ? reject(err) : resolve())),
    );
  }
  if (container) {
    await container.stop();
  }
}, 30_000);

// ---------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------

describe('health checks', { tags: ['connectivity'] }, () => {
  it('GET /proxy/ping returns ok', async () => {
    const resp = await fetch(`${proxyBaseUrl}/proxy/ping`);
    expect(resp.ok).toBe(true);
    const body = await resp.json();
    expect(body).toEqual({ status: 'ok' });
  });

  it('GET /health returns ok', async () => {
    const resp = await fetch(`${proxyBaseUrl}/health`);
    expect(resp.ok).toBe(true);
    const body = await resp.json();
    expect(body).toEqual({ status: 'ok', service: 'clickhouse-cors-proxy' });
  });
});

// ---------------------------------------------------------------------------
// CORS headers
// ---------------------------------------------------------------------------

describe('CORS', { tags: ['connectivity'] }, () => {
  it('returns Access-Control-Allow-Origin on proxy responses', async () => {
    const resp = await fetch(`${proxyBaseUrl}/proxy/ping`);
    expect(resp.headers.get('access-control-allow-origin')).toBe('*');
  });
});

// ---------------------------------------------------------------------------
// Query forwarding
// ---------------------------------------------------------------------------

describe('query forwarding', { tags: ['connectivity'] }, () => {
  it('forwards a simple query with FORMAT in SQL body', async () => {
    const { resp, text } = await proxyPost(
      '/proxy/query',
      'SELECT 1 as n FORMAT JSONEachRow',
    );
    expect(resp.status, `Expected 200 but got ${resp.status}: ${text}`).toBe(200);
    const row = JSON.parse(text.trim());
    expect(row).toEqual({ n: 1 });
  });

  it('forwards ?format=JSONEachRow query param to ClickHouse', async () => {
    const { resp, text } = await proxyPost(
      '/proxy/query?format=JSONEachRow',
      'SELECT 42 as answer',
    );
    expect(resp.status, `Expected 200 but got ${resp.status}: ${text}`).toBe(200);
    const row = JSON.parse(text.trim());
    expect(row).toEqual({ answer: 42 });
  });

  it('forwards version query (same as connection test)', async () => {
    const { resp, text } = await proxyPost(
      '/proxy/query?format=JSONEachRow',
      'SELECT version() as version, timezone() as timezone, hostName() as display_name',
    );
    expect(resp.status, `Expected 200 but got ${resp.status}: ${text}`).toBe(200);
    const row = JSON.parse(text.trim());
    expect(row).toHaveProperty('version');
    expect(row).toHaveProperty('timezone');
    expect(row).toHaveProperty('display_name');
  });

  it('forwards commands (DDL/DML) via /proxy/command', async () => {
    const { resp, text } = await proxyPost(
      '/proxy/command',
      'SELECT 1',
    );
    expect(resp.status, `Expected 200 but got ${resp.status}: ${text}`).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// Auth header translation
// ---------------------------------------------------------------------------

describe('auth headers', { tags: ['connectivity'] }, () => {
  it('translates x-ch-user/password to ClickHouse auth headers', async () => {
    const { resp, text } = await proxyPost(
      '/proxy/query',
      'SELECT currentUser() as user FORMAT JSONEachRow',
    );
    expect(resp.status, `Expected 200 but got ${resp.status}: ${text}`).toBe(200);
    const row = JSON.parse(text.trim());
    expect(row.user).toBe(chUser);
  });

  it('returns error for invalid credentials', async () => {
    const { resp } = await proxyPost(
      '/proxy/query',
      'SELECT 1',
      chHeaders({
        'x-ch-user': 'nonexistent_user_12345',
        'x-ch-password': 'wrong',
      }),
    );
    expect(resp.ok).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Security: proxy headers must NOT leak to ClickHouse
// ---------------------------------------------------------------------------

describe('security', { tags: ['connectivity'] }, () => {
  it('does not require x-ch-host for health endpoints', async () => {
    const resp = await fetch(`${proxyBaseUrl}/proxy/ping`);
    expect(resp.ok).toBe(true);
  });

  it('returns error when x-ch-host is missing on proxy route', async () => {
    const resp = await fetch(`${proxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-port': '8123',
        'x-ch-secure': 'false',
      },
      body: 'SELECT 1',
    });
    expect(resp.ok).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Security: header leak verification via echo server
//
// Spins up a tiny HTTP server that records the headers it receives,
// then points the proxy at it. This lets us assert exactly which
// headers arrive at the "ClickHouse" backend.
// ---------------------------------------------------------------------------

describe('security — header leaking', { tags: ['connectivity'] }, () => {
  let echoServer: Server;
  let echoPort: number;
  let leakProxyServer: Server;
  let leakProxyBaseUrl: string;
  /** Headers received by the echo server on the most recent request. */
  let capturedHeaders: http.IncomingHttpHeaders;
  /** URL path + query string received by the echo server. */
  let capturedUrl: string;

  beforeAll(async () => {
    // Echo server — returns 200 and records headers + URL
    echoServer = http.createServer((req, res) => {
      capturedHeaders = { ...req.headers };
      capturedUrl = req.url ?? '';
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('OK');
    });
    await new Promise<void>((resolve) => echoServer.listen(0, resolve));
    echoPort = (echoServer.address() as AddressInfo).port;

    // Separate proxy instance (no ClickHouse dependency)
    const app = createApp();
    leakProxyServer = await new Promise<Server>((resolve) => {
      const s = app.listen(0, () => resolve(s));
    });
    leakProxyBaseUrl = `http://localhost:${(leakProxyServer.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    await Promise.all([
      new Promise<void>((resolve, reject) =>
        leakProxyServer?.close((err) => (err ? reject(err) : resolve())),
      ),
      new Promise<void>((resolve, reject) =>
        echoServer?.close((err) => (err ? reject(err) : resolve())),
      ),
    ]);
  });

  it('password only appears in X-ClickHouse-Key, not as x-ch-password', async () => {
    await fetch(`${leakProxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'alice',
        'x-ch-password': 'super-secret-123',
        'x-ch-database': 'mydb',
        'x-ch-secure': 'false',
      },
      body: 'SELECT 1',
    });

    // Password must be translated to X-ClickHouse-Key (ClickHouse native auth)
    expect(capturedHeaders['x-clickhouse-key']).toBe('super-secret-123');
    // But the raw x-ch-password header must NOT be forwarded
    expect(capturedHeaders['x-ch-password']).toBeUndefined();
  });

  it('does not forward any x-ch-* headers to the backend', async () => {
    await fetch(`${leakProxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'bob',
        'x-ch-password': 'another-secret',
        'x-ch-database': 'production',
        'x-ch-secure': 'false',
      },
      body: 'SELECT 1',
    });

    const headerNames = Object.keys(capturedHeaders);
    const leaked = headerNames.filter((h) => h.startsWith('x-ch-'));
    expect(leaked, `x-ch-* headers leaked to backend: ${leaked.join(', ')}`).toEqual([]);
  });

  it('translates credentials to X-ClickHouse-User/Key headers', async () => {
    await fetch(`${leakProxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'alice',
        'x-ch-password': 'secret',
        'x-ch-database': 'mydb',
        'x-ch-secure': 'false',
      },
      body: 'SELECT 1',
    });

    expect(capturedHeaders['x-clickhouse-user']).toBe('alice');
    expect(capturedHeaders['x-clickhouse-key']).toBe('secret');
    expect(capturedHeaders['x-clickhouse-database']).toBe('mydb');
  });

  it('x-ch-host value does not appear in forwarded headers', async () => {
    // The x-ch-host is used for routing but should be stripped from
    // the headers sent to the backend (it's in the Host header instead).
    await fetch(`${leakProxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'default',
        'x-ch-password': '',
        'x-ch-database': 'default',
        'x-ch-secure': 'false',
      },
      body: 'SELECT 1',
    });

    expect(capturedHeaders['x-ch-host']).toBeUndefined();
    expect(capturedHeaders['x-ch-port']).toBeUndefined();
    expect(capturedHeaders['x-ch-secure']).toBeUndefined();
  });

  it('password does not appear in the URL forwarded to the backend', async () => {
    await fetch(`${leakProxyBaseUrl}/proxy/query?format=JSONEachRow`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'alice',
        'x-ch-password': 'super-secret-123',
        'x-ch-database': 'default',
        'x-ch-secure': 'false',
      },
      body: 'SELECT 1',
    });

    // Password must not leak into the query string
    expect(capturedUrl).not.toContain('super-secret-123');
    expect(capturedUrl).not.toContain('password');
    // Only the format param should be forwarded (as default_format)
    expect(capturedUrl).toContain('default_format=JSONEachRow');
  });

  it('overwrites any incoming X-ClickHouse-* auth headers from the client', async () => {
    // If a malicious client tries to bypass proxy auth by sending ClickHouse
    // headers directly, the proxy MUST overwrite them.
    await fetch(`${leakProxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'alice',
        'x-ch-password': 'secret',
        'x-ch-database': 'mydb',
        'x-ch-secure': 'false',
        // Injected headers we don't want the proxy to trust
        'X-ClickHouse-User': 'malicious-admin',
        'X-ClickHouse-Key': 'hacked',
        'X-ClickHouse-Database': 'system',
      },
      body: 'SELECT 1',
    });

    expect(capturedHeaders['x-clickhouse-user']).toBe('alice');
    expect(capturedHeaders['x-clickhouse-key']).toBe('secret');
    expect(capturedHeaders['x-clickhouse-database']).toBe('mydb');
  });

  it('removes Authorization header to prevent bypassing or confusing ClickHouse auth', async () => {
    await fetch(`${leakProxyBaseUrl}/proxy/query`, {
      method: 'POST',
      headers: {
        'x-ch-host': 'localhost',
        'x-ch-port': String(echoPort),
        'x-ch-user': 'alice',
        'x-ch-password': 'secret',
        'x-ch-database': 'mydb',
        'x-ch-secure': 'false',
        'Authorization': 'Basic dXNlcjpwYXNz', // user:pass
      },
      body: 'SELECT 1',
    });

    // We shouldn't forward Authorization because we already authenticated with X-ClickHouse-User/Key
    // and sending both might cause ClickHouse to complain or bypass.
    expect(capturedHeaders['authorization']).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('error handling', { tags: ['connectivity'] }, () => {
  it('returns error for invalid SQL', async () => {
    const { resp } = await proxyPost(
      '/proxy/query',
      'THIS IS NOT VALID SQL',
    );
    expect(resp.ok).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Static file serving (Docker / bundled mode)
// ---------------------------------------------------------------------------

describe('static file serving (STATIC_DIR)', { tags: ['connectivity'] }, () => {
  let staticServer: Server;
  let staticBaseUrl: string;
  let tmpDir: string;

  beforeAll(async () => {
    // Create a temp directory with fake frontend files
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'proxy-static-'));
    fs.writeFileSync(path.join(tmpDir, 'index.html'), '<html><body>Click-it</body></html>');
    fs.mkdirSync(path.join(tmpDir, 'assets'));
    fs.writeFileSync(path.join(tmpDir, 'assets', 'app.js'), 'console.log("app")');

    // Set STATIC_DIR and create a fresh app instance
    process.env.STATIC_DIR = tmpDir;
    const app = createApp();
    delete process.env.STATIC_DIR;

    staticServer = await new Promise<Server>((resolve) => {
      const s = app.listen(0, () => resolve(s));
    });
    const addr = staticServer.address() as AddressInfo;
    staticBaseUrl = `http://localhost:${addr.port}`;
  });

  afterAll(async () => {
    if (staticServer) {
      await new Promise<void>((resolve, reject) =>
        staticServer.close((err) => (err ? reject(err) : resolve())),
      );
    }
    if (tmpDir) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('serves index.html at /', async () => {
    const resp = await fetch(staticBaseUrl);
    expect(resp.ok).toBe(true);
    const text = await resp.text();
    expect(text).toContain('Click-it');
  });

  it('serves static assets', async () => {
    const resp = await fetch(`${staticBaseUrl}/assets/app.js`);
    expect(resp.ok).toBe(true);
    const text = await resp.text();
    expect(text).toContain('console.log');
  });

  it('SPA fallback returns index.html for unknown routes', async () => {
    const resp = await fetch(`${staticBaseUrl}/some/deep/route`);
    expect(resp.ok).toBe(true);
    const text = await resp.text();
    expect(text).toContain('Click-it');
  });

  it('health endpoints still work alongside static serving', async () => {
    const resp = await fetch(`${staticBaseUrl}/proxy/ping`);
    expect(resp.ok).toBe(true);
    const body = await resp.json();
    expect(body).toEqual({ status: 'ok' });
  });
});
