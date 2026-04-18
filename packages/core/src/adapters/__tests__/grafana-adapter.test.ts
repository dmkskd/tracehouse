import { describe, it, expect, vi } from 'vitest';
import { GrafanaAdapter, type AdapterFrame, type AdapterQueryFn } from '../grafana-adapter.js';
import { AdapterError } from '../types.js';

function makeFrame(fields: Array<{ name: string; type?: string }>, columns: unknown[][]): AdapterFrame {
  return {
    fields: fields.map((f, i) => ({ ...f, values: columns[i] ?? [] })),
  };
}

function createAdapter(queryFn: AdapterQueryFn) {
  return new GrafanaAdapter(queryFn);
}

describe('GrafanaAdapter', { tags: ['connectivity'] }, () => {
  it('can be constructed with a query function', () => {
    const adapter = createAdapter(vi.fn().mockResolvedValue([]));
    expect(adapter).toBeDefined();
  });

  it('implements executeQuery method', () => {
    const adapter = createAdapter(vi.fn().mockResolvedValue([]));
    expect(typeof adapter.executeQuery).toBe('function');
  });

  describe('executeQuery', () => {
    it('invokes the query function with sql and refId', async () => {
      const queryFn = vi.fn<AdapterQueryFn>().mockResolvedValue([]);
      const adapter = createAdapter(queryFn);

      await adapter.executeQuery('SELECT 1');

      expect(queryFn).toHaveBeenCalledWith('SELECT 1', 'q');
    });

    it('returns empty array for empty frames', async () => {
      const queryFn = vi.fn<AdapterQueryFn>().mockResolvedValue([]);
      const adapter = createAdapter(queryFn);

      const result = await adapter.executeQuery('SELECT 1');
      expect(result).toEqual([]);
    });

    it('returns empty array when frame has no fields', async () => {
      const queryFn = vi.fn<AdapterQueryFn>().mockResolvedValue([{ fields: [] }]);
      const adapter = createAdapter(queryFn);

      const result = await adapter.executeQuery('SELECT 1');
      expect(result).toEqual([]);
    });

    it('converts columnar frames to row objects', async () => {
      const frame = makeFrame(
        [{ name: 'name' }, { name: 'rows' }, { name: 'bytes_on_disk' }],
        [
          ['part1', 'part2'],
          [100, 200],
          [1024, 2048],
        ],
      );
      const adapter = createAdapter(vi.fn().mockResolvedValue([frame]));

      const result = await adapter.executeQuery<{ name: string; rows: number; bytes_on_disk: number }>(
        'SELECT name, rows, bytes_on_disk FROM system.parts',
      );

      expect(result).toEqual([
        { name: 'part1', rows: 100, bytes_on_disk: 1024 },
        { name: 'part2', rows: 200, bytes_on_disk: 2048 },
      ]);
    });

    it('handles single-row response', async () => {
      const frame = makeFrame([{ name: 'version' }], [['24.3.1']]);
      const adapter = createAdapter(vi.fn().mockResolvedValue([frame]));

      const result = await adapter.executeQuery<{ version: string }>('SELECT version()');
      expect(result).toEqual([{ version: '24.3.1' }]);
    });

    it('handles null values in frame data', async () => {
      const frame = makeFrame(
        [{ name: 'name' }, { name: 'value' }],
        [['a', 'b'], [1, null]],
      );
      const adapter = createAdapter(vi.fn().mockResolvedValue([frame]));

      const result = await adapter.executeQuery('SELECT name, value');
      expect(result).toEqual([
        { name: 'a', value: 1 },
        { name: 'b', value: null },
      ]);
    });

    it('wraps query errors as AdapterError', async () => {
      const adapter = createAdapter(vi.fn().mockRejectedValue(new Error('DB::Exception: Table not found')));

      try {
        await adapter.executeQuery('SELECT * FROM bad_table');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(AdapterError);
        const ae = error as AdapterError;
        expect(ae.message).toContain('DB::Exception');
        expect(ae.category).toBe('query');
      }
    });
  });

  describe('error categorization', () => {
    function categorize(message: string): string {
      const adapter = createAdapter(vi.fn().mockResolvedValue([]));
      const wrapped = (adapter as unknown as Record<string, (e: Error) => AdapterError>)
        .wrapError(new Error(message));
      return wrapped.category;
    }

    it('categorizes Unauthorized as authentication', () => {
      expect(categorize('Unauthorized')).toBe('authentication');
    });

    it('categorizes 403 as authentication', () => {
      expect(categorize('Request failed with status 403')).toBe('authentication');
    });

    it('categorizes authentication keyword as authentication', () => {
      expect(categorize('authentication failed')).toBe('authentication');
    });

    it('categorizes timeout as timeout', () => {
      expect(categorize('Request timeout after 30s')).toBe('timeout');
    });

    it('categorizes Timeout as timeout', () => {
      expect(categorize('Timeout waiting for response')).toBe('timeout');
    });

    it('categorizes network as network', () => {
      expect(categorize('network error')).toBe('network');
    });

    it('categorizes ECONNREFUSED as network', () => {
      expect(categorize('connect ECONNREFUSED 127.0.0.1:8123')).toBe('network');
    });

    it('categorizes Failed to fetch as network', () => {
      expect(categorize('Failed to fetch')).toBe('network');
    });

    it('categorizes DB::Exception as query', () => {
      expect(categorize('DB::Exception: Unknown table')).toBe('query');
    });

    it('categorizes Syntax error as query', () => {
      expect(categorize('Syntax error at position 10')).toBe('query');
    });

    it('categorizes Code: as query', () => {
      expect(categorize('Code: 60. DB::Exception')).toBe('query');
    });

    it('categorizes unknown errors as unknown', () => {
      expect(categorize('Something unexpected')).toBe('unknown');
    });

    it('preserves original error as cause', () => {
      const adapter = createAdapter(vi.fn().mockResolvedValue([]));
      const original = new Error('test');
      const wrapped = (adapter as unknown as Record<string, (e: Error) => AdapterError>)
        .wrapError(original);
      expect(wrapped.cause).toBe(original);
    });

    it('handles non-Error values', () => {
      const adapter = createAdapter(vi.fn().mockResolvedValue([]));
      const wrapped = (adapter as unknown as Record<string, (e: unknown) => AdapterError>)
        .wrapError('string error');
      expect(wrapped.message).toBe('string error');
      expect(wrapped.cause).toBeUndefined();
      expect(wrapped.category).toBe('unknown');
    });
  });

  describe('network error wrapping', () => {
    it('wraps query function rejection as AdapterError', async () => {
      const adapter = createAdapter(vi.fn().mockRejectedValue(new Error('Failed to fetch')));

      try {
        await adapter.executeQuery('SELECT 1');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(AdapterError);
        const ae = error as AdapterError;
        expect(ae.category).toBe('network');
        expect(ae.message).toBe('Failed to fetch');
      }
    });
  });
});
