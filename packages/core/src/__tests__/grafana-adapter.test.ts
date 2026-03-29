import { describe, it, expect } from 'vitest';
import { GrafanaAdapter } from '../adapters/grafana-adapter.js';

// ── Helpers ───────────────────────────────────────────────────────

/** Build a minimal Grafana /api/ds/query response from column definitions and values. */
function grafanaResponse(
  fields: Array<{ name: string; type?: string }>,
  values: unknown[][],
) {
  return {
    results: {
      q: {
        status: 200,
        frames: [{
          schema: { fields },
          data: { values },
        }],
      },
    },
  };
}

/**
 * Create a GrafanaAdapter with a fake backendSrv that returns a canned response.
 * Call executeQuery to exercise framesToRows through the public API.
 */
function adapterWithResponse(response: unknown): GrafanaAdapter {
  return new GrafanaAdapter(
    { uid: 'test-ds', type: 'grafana-clickhouse-datasource' },
    () => ({ post: async <T>() => response as T }),
  );
}

// ── Tests ─────────────────────────────────────────────────────────

describe('GrafanaAdapter', { tags: ['connectivity'] }, () => {
  describe('framesToRows — DateTime normalization', () => {
    it('converts epoch-ms time fields to ISO-like strings', async () => {
      // Grafana ClickHouse datasource returns DateTime columns as epoch-ms
      // with schema type "time". This is a hardcoded sample of what Grafana returns
      // for a query like: SELECT query_id, query_start_time FROM system.query_log LIMIT 2
      const response = grafanaResponse(
        [
          { name: 'query_id', type: 'string' },
          { name: 'query_start_time', type: 'time' },
          { name: 'query_duration_ms', type: 'number' },
        ],
        [
          ['abc-123', 'def-456'],              // query_id values
          [1711440000000, 1711443600000],       // query_start_time as epoch-ms (2024-03-26 12:00:00, 13:00:00 UTC)
          [150, 320],                           // query_duration_ms
        ],
      );

      const adapter = adapterWithResponse(response);
      const rows = await adapter.executeQuery<{
        query_id: string;
        query_start_time: string;
        query_duration_ms: number;
      }>('SELECT 1');

      expect(rows).toHaveLength(2);

      // Time fields should be normalized to "YYYY-MM-DD HH:MM:SS.sss" strings (no T, no Z)
      expect(typeof rows[0].query_start_time).toBe('string');
      expect(rows[0].query_start_time).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/);
      expect(rows[0].query_start_time).toContain('2024-03-26');

      expect(typeof rows[1].query_start_time).toBe('string');
      expect(rows[1].query_start_time).toContain('2024-03-26');

      // Non-time fields should pass through unchanged
      expect(rows[0].query_id).toBe('abc-123');
      expect(rows[0].query_duration_ms).toBe(150);
    });

    it('passes through string time fields as-is', async () => {
      // If the value is already a string (unlikely but defensive), don't break it
      const response = grafanaResponse(
        [{ name: 'query_start_time', type: 'time' }],
        [['2024-03-26 12:00:00']],
      );

      const adapter = adapterWithResponse(response);
      const rows = await adapter.executeQuery<{ query_start_time: string }>('SELECT 1');

      expect(rows[0].query_start_time).toBe('2024-03-26 12:00:00');
    });

    it('passes through fields without type annotation unchanged', async () => {
      const response = grafanaResponse(
        [{ name: 'count' }],  // no type field
        [[42]],
      );

      const adapter = adapterWithResponse(response);
      const rows = await adapter.executeQuery<{ count: number }>('SELECT 1');

      expect(rows[0].count).toBe(42);
    });

    it('handles null time values', async () => {
      const response = grafanaResponse(
        [{ name: 'ts', type: 'time' }],
        [[null]],
      );

      const adapter = adapterWithResponse(response);
      const rows = await adapter.executeQuery<{ ts: string | null }>('SELECT 1');

      expect(rows[0].ts).toBeNull();
    });
  });

  describe('framesToRows — basic behavior', () => {
    it('returns empty array for empty frames', async () => {
      const response = { results: { q: { status: 200, frames: [] } } };
      const adapter = adapterWithResponse(response);
      const rows = await adapter.executeQuery('SELECT 1');
      expect(rows).toEqual([]);
    });

    it('throws on frame error', async () => {
      const response = {
        results: {
          q: {
            status: 400,
            frames: [{ schema: { fields: [] }, data: { values: [] } }],
            error: 'DB::Exception: Syntax error',
          },
        },
      };
      const adapter = adapterWithResponse(response);
      await expect(adapter.executeQuery('BAD SQL')).rejects.toThrow('DB::Exception');
    });
  });
});
