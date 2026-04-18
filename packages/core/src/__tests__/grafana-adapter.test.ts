import { describe, it, expect } from 'vitest';
import { GrafanaAdapter, type AdapterFrame } from '../adapters/grafana-adapter.js';

function frame(fields: Array<{ name: string; type?: string }>, columns: unknown[][]): AdapterFrame {
  return {
    fields: fields.map((f, i) => ({ ...f, values: columns[i] ?? [] })),
  };
}

function adapterWithFrames(frames: AdapterFrame[]): GrafanaAdapter {
  return new GrafanaAdapter(async () => frames);
}

describe('GrafanaAdapter', { tags: ['connectivity'] }, () => {
  describe('framesToRows — DateTime normalization', () => {
    it('converts epoch-ms time fields to ISO-like strings', async () => {
      // Grafana ClickHouse datasource returns DateTime columns as epoch-ms
      // with schema type "time". This is a hardcoded sample of what Grafana returns
      // for a query like: SELECT query_id, query_start_time FROM system.query_log LIMIT 2
      const f = frame(
        [
          { name: 'query_id', type: 'string' },
          { name: 'query_start_time', type: 'time' },
          { name: 'query_duration_ms', type: 'number' },
        ],
        [
          ['abc-123', 'def-456'],
          [1711440000000, 1711443600000],
          [150, 320],
        ],
      );

      const adapter = adapterWithFrames([f]);
      const rows = await adapter.executeQuery<{
        query_id: string;
        query_start_time: string;
        query_duration_ms: number;
      }>('SELECT 1');

      expect(rows).toHaveLength(2);

      expect(typeof rows[0].query_start_time).toBe('string');
      expect(rows[0].query_start_time).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/);
      expect(rows[0].query_start_time).toContain('2024-03-26');

      expect(typeof rows[1].query_start_time).toBe('string');
      expect(rows[1].query_start_time).toContain('2024-03-26');

      expect(rows[0].query_id).toBe('abc-123');
      expect(rows[0].query_duration_ms).toBe(150);
    });

    it('passes through string time fields as-is', async () => {
      const f = frame(
        [{ name: 'query_start_time', type: 'time' }],
        [['2024-03-26 12:00:00']],
      );

      const adapter = adapterWithFrames([f]);
      const rows = await adapter.executeQuery<{ query_start_time: string }>('SELECT 1');

      expect(rows[0].query_start_time).toBe('2024-03-26 12:00:00');
    });

    it('passes through fields without type annotation unchanged', async () => {
      const f = frame([{ name: 'count' }], [[42]]);

      const adapter = adapterWithFrames([f]);
      const rows = await adapter.executeQuery<{ count: number }>('SELECT 1');

      expect(rows[0].count).toBe(42);
    });

    it('handles null time values', async () => {
      const f = frame([{ name: 'ts', type: 'time' }], [[null]]);

      const adapter = adapterWithFrames([f]);
      const rows = await adapter.executeQuery<{ ts: string | null }>('SELECT 1');

      expect(rows[0].ts).toBeNull();
    });
  });

  describe('framesToRows — basic behavior', () => {
    it('returns empty array for empty frames', async () => {
      const adapter = adapterWithFrames([]);
      const rows = await adapter.executeQuery('SELECT 1');
      expect(rows).toEqual([]);
    });

    it('throws on query function rejection', async () => {
      const adapter = new GrafanaAdapter(async () => {
        throw new Error('DB::Exception: Syntax error');
      });
      await expect(adapter.executeQuery('BAD SQL')).rejects.toThrow('DB::Exception');
    });
  });
});
