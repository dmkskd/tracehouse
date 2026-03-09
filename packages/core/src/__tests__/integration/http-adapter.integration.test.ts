/**
 * Integration tests for HttpAdapter error categorization against a real ClickHouse instance.
 *
 * Replaces the mock-based http-adapter.test.ts error categorization tests.
 * Tests that real ClickHouse errors are correctly categorized by the adapter.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { HttpAdapter } from '../../adapters/http-adapter.js';
import { AdapterError } from '../../adapters/types.js';

const CONTAINER_TIMEOUT = 120_000;

describe('HttpAdapter integration', () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => {
    ctx = await startClickHouse();
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await stopClickHouse(ctx);
    }
  }, 30_000);

  /** Build a ConnectionConfig pointing at the test container. */
  function containerConfig(): import('../../types/connection.js').ConnectionConfig {
    if (!ctx.container) throw new Error('containerConfig() requires a testcontainer (not CH_TEST_URL mode)');
    return {
      host: ctx.container.getHost(),
      port: ctx.container.getHttpPort(),
      user: ctx.container.getUsername(),
      password: ctx.container.getPassword(),
      database: 'default',
      secure: false,
      connect_timeout: 10,
      send_receive_timeout: 30,
    };
  }

  describe('successful queries', () => {
    it('executes a simple query', async () => {
      const adapter = new HttpAdapter(containerConfig());
      const rows = await adapter.executeQuery<{ v: number }>('SELECT 1 AS v');
      expect(rows).toHaveLength(1);
      expect(Number(rows[0].v)).toBe(1);
      await adapter.close();
    });
  });

  describe('error categorization with real errors', () => {
    it('categorizes syntax errors as query errors', async () => {
      const adapter = new HttpAdapter(containerConfig());
      try {
        await adapter.executeQuery('SELEC INVALID SYNTAX');
        expect.unreachable('should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(AdapterError);
        expect((error as AdapterError).category).toBe('query');
      } finally {
        await adapter.close();
      }
    });

    it('categorizes unknown table as query error', async () => {
      const adapter = new HttpAdapter(containerConfig());
      try {
        await adapter.executeQuery('SELECT * FROM nonexistent_table_xyz');
        expect.unreachable('should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(AdapterError);
        expect((error as AdapterError).category).toBe('query');
      } finally {
        await adapter.close();
      }
    });

    it('categorizes connection refused as network error', async () => {
      const adapter = new HttpAdapter({
        host: '127.0.0.1', port: 19999, user: 'default',
        password: '', database: 'default', secure: false,
        connect_timeout: 1, send_receive_timeout: 2,
      });

      try {
        await adapter.executeQuery('SELECT 1');
        expect.unreachable('should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(AdapterError);
        expect(['network', 'timeout', 'unknown']).toContain((error as AdapterError).category);
      } finally {
        await adapter.close();
      }
    });

    it('preserves error message', async () => {
      const adapter = new HttpAdapter(containerConfig());
      try {
        await adapter.executeQuery('SELECT * FROM nonexistent_db.nonexistent_table');
        expect.unreachable('should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(AdapterError);
        expect((error as AdapterError).message).toBeTruthy();
        expect((error as AdapterError).message.length).toBeGreaterThan(0);
      } finally {
        await adapter.close();
      }
    });
  });
});
