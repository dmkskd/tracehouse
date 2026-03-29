/**
 * Integration test proving HTTP response compression between ClickHouse and the client.
 *
 * Tests both raw HTTP (to inspect headers) and the @clickhouse/client library
 * (to confirm the compression option works end-to-end as we'd use it in production).
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { CLIENT_COMPRESSION } from '../../adapters/types.js';

const CONTAINER_TIMEOUT = 120_000;

/** Query that produces a non-trivial, compressible payload (similar to metric_log). */
const QUERY = `
  SELECT
    toString(now() - INTERVAL number SECOND) AS t,
    concat('dev-cluster-clickhouse-0-0-', toString(number % 3)) AS host,
    number * 1000 AS v,
    1000 AS interval_ms
  FROM numbers(3000)
`;

describe('HTTP response compression', { tags: ['storage'] }, () => {
  let ctx: TestClickHouseContext;
  let baseUrl: string;
  let authHeader: string;

  beforeAll(async () => {
    ctx = await startClickHouse();

    let raw: string;
    if (ctx.container) {
      raw = ctx.container.getConnectionUrl();
    } else {
      raw = process.env.CH_TEST_URL!;
    }

    // Strip credentials from the URL — fetch() rejects URLs with embedded creds.
    const parsed = new URL(raw);
    const user = decodeURIComponent(parsed.username || 'default');
    const pass = decodeURIComponent(parsed.password || '');
    authHeader = `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}`;
    parsed.username = '';
    parsed.password = '';
    baseUrl = parsed.origin;
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  /** Helper to fetch from the test ClickHouse instance. */
  function chFetch(path: string, headers: Record<string, string> = {}) {
    return fetch(`${baseUrl}${path}`, {
      headers: { Authorization: authHeader, ...headers },
    });
  }

  describe('raw HTTP (proving the protocol)', () => {
    it('returns uncompressed by default', async () => {
      const queryWithFormat = `${QUERY} FORMAT JSONEachRow`;
      const resp = await chFetch(`/?query=${encodeURIComponent(queryWithFormat)}`, {
        'Accept-Encoding': 'identity',
      });
      expect(resp.ok).toBe(true);

      const body = await resp.text();
      expect(resp.headers.get('content-encoding')).toBeNull();
      expect(body.length).toBeGreaterThan(50_000);
    });

    it('returns gzip when Accept-Encoding + enable_http_compression=1', async () => {
      const queryWithFormat = `${QUERY} FORMAT JSONEachRow`;
      const resp = await chFetch(
        `/?query=${encodeURIComponent(queryWithFormat)}&enable_http_compression=1`,
        { 'Accept-Encoding': 'gzip' },
      );
      expect(resp.ok).toBe(true);
      expect(resp.headers.get('content-encoding')).toBe('gzip');
    });

    it('achieves significant size reduction', async () => {
      const queryWithFormat = `${QUERY} FORMAT JSONEachRow`;
      const plainResp = await chFetch(`/?query=${encodeURIComponent(queryWithFormat)}`, {
        'Accept-Encoding': 'identity',
      });
      const plainBody = new Uint8Array(await plainResp.arrayBuffer());
      const uncompressedSize = plainBody.byteLength;

      const { gzipSync } = await import('node:zlib');
      const compressedSize = gzipSync(plainBody).byteLength;
      const ratio = compressedSize / uncompressedSize;

      console.log(`  Uncompressed: ${(uncompressedSize / 1024).toFixed(1)} KB`);
      console.log(`  Compressed:   ${(compressedSize / 1024).toFixed(1)} KB`);
      console.log(`  Ratio:        ${(ratio * 100).toFixed(1)}% (${(1 / ratio).toFixed(1)}x reduction)`);

      expect(ratio).toBeLessThan(0.15);
    });
  });

  describe('@clickhouse/client with compression option', () => {
    let plainClient: ClickHouseClient;
    let compressedClient: ClickHouseClient;

    beforeAll(() => {
      const connectionUrl = ctx.container
        ? ctx.container.getConnectionUrl()
        : process.env.CH_TEST_URL!;

      plainClient = createClient({ url: connectionUrl });
      compressedClient = createClient({
        url: connectionUrl,
        compression: CLIENT_COMPRESSION,
      });
    });

    afterAll(async () => {
      await plainClient.close();
      await compressedClient.close();
    });

    it('both clients return identical results', async () => {
      const plainResult = await plainClient.query({ query: QUERY, format: 'JSONEachRow' });
      const plainRows = await plainResult.json();

      const compressedResult = await compressedClient.query({ query: QUERY, format: 'JSONEachRow' });
      const compressedRows = await compressedResult.json();

      expect(plainRows).toHaveLength(3000);
      expect(compressedRows).toHaveLength(3000);
      // Same data regardless of transport compression
      expect(plainRows).toEqual(compressedRows);
    });

    it('compressed client returns correct data shape', async () => {
      const result = await compressedClient.query({ query: QUERY, format: 'JSONEachRow' });
      const rows = await result.json<{ t: string; host: string; v: string; interval_ms: string }>();

      expect(rows.length).toBe(3000);
      // Spot-check structure
      const first = rows[0];
      expect(first).toHaveProperty('t');
      expect(first).toHaveProperty('host');
      expect(first).toHaveProperty('v');
      expect(first).toHaveProperty('interval_ms');
      expect(first.host).toMatch(/^dev-cluster-clickhouse-0-0-/);
    });
  });
});
