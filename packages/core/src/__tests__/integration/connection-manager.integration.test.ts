/**
 * Integration tests for ConnectionManager.testConnection against a real ClickHouse instance.
 *
 * Replaces the mock-based testConnection tests from connection-manager.test.ts.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { ConnectionManager } from '../../services/connection-manager.js';
import type { AdapterFactory } from '../../services/connection-manager.js';
import type { ConnectionConfig } from '../../types/connection.js';
import { TestAdapter } from './setup/clickhouse-container.js';
import { createClient } from '@clickhouse/client';

const CONTAINER_TIMEOUT = 120_000;

describe('ConnectionManager.testConnection integration', { tags: ['connectivity'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await stopClickHouse(ctx);
    }
  }, 30_000);

  /** Build an AdapterFactory that connects to the test container. */
  function makeFactory(urlOverride?: string): AdapterFactory {
    return (_config: ConnectionConfig) => {
      if (!urlOverride && !ctx.container) throw new Error('makeFactory() requires a testcontainer (not CH_TEST_URL mode)');
      const url = urlOverride ?? ctx.container!.getConnectionUrl();
      const client = createClient({ url });
      const adapter = new TestAdapter(client);
      return {
        adapter,
        close: () => client.close(),
      };
    };
  }

  it('returns success with server info for a valid connection', async () => {
    const mgr = new ConnectionManager(makeFactory());
    const result = await mgr.testConnection({
      host: 'localhost', port: 8123, user: 'default',
      password: '', database: 'default', secure: false,
      connect_timeout: 10, send_receive_timeout: 30,
    });

    expect(result.success).toBe(true);
    expect(result.server_version).toBeTruthy();
    expect(result.server_timezone).toBeTruthy();
    expect(result.server_display_name).toBeTruthy();
    expect(typeof result.latency_ms).toBe('number');
    expect(result.latency_ms).toBeGreaterThanOrEqual(0);
  });

  it('returns failure for a connection to a non-existent server', async () => {
    // Point to a port that's not listening
    const mgr = new ConnectionManager(makeFactory('http://127.0.0.1:19999'));
    const result = await mgr.testConnection({
      host: '127.0.0.1', port: 19999, user: 'default',
      password: '', database: 'default', secure: false,
      connect_timeout: 2, send_receive_timeout: 2,
    });

    expect(result.success).toBe(false);
    expect(result.error_message).toBeTruthy();
    expect(typeof result.latency_ms).toBe('number');
  });

  it('measures latency', async () => {
    const mgr = new ConnectionManager(makeFactory());
    const result = await mgr.testConnection({
      host: 'localhost', port: 8123, user: 'default',
      password: '', database: 'default', secure: false,
      connect_timeout: 10, send_receive_timeout: 30,
    });

    expect(result.latency_ms).toBeGreaterThanOrEqual(0);
    // Should complete in under 5 seconds against a local container
    expect(result.latency_ms).toBeLessThan(5000);
  });
});
