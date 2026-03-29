import { describe, it, expect, vi } from 'vitest';
import { GrafanaAdapter } from '../grafana-adapter.js';
import { AdapterError } from '../types.js';

function makeDsRef() {
  return { uid: 'test-uid', type: 'grafana-clickhouse-datasource' };
}

function makeResponse(refId: string, fields: string[], values: unknown[][]) {
  return {
    results: {
      [refId]: {
        frames: [{
          schema: { fields: fields.map(name => ({ name })) },
          data: { values },
        }],
      },
    },
  };
}

function createAdapter(postFn: (url: string, body: unknown) => Promise<unknown>) {
  return new GrafanaAdapter(makeDsRef(), () => ({ post: postFn as never }));
}

describe('GrafanaAdapter', { tags: ['connectivity'] }, () => {
  it('can be constructed with a datasource ref and getBackendSrv', () => {
    const adapter = new GrafanaAdapter(makeDsRef(), () => ({ post: vi.fn() as never }));
    expect(adapter).toBeDefined();
  });

  it('implements executeQuery method', () => {
    const adapter = new GrafanaAdapter(makeDsRef(), () => ({ post: vi.fn() as never }));
    expect(typeof adapter.executeQuery).toBe('function');
  });

  describe('executeQuery', () => {
    it('sends correct request shape to /api/ds/query', async () => {
      const postFn = vi.fn().mockResolvedValue(makeResponse('q', [], []));
      const adapter = createAdapter(postFn);

      await adapter.executeQuery('SELECT 1');

      expect(postFn).toHaveBeenCalledWith('/api/ds/query', {
        queries: [{
          refId: 'q',
          rawSql: 'SELECT 1',
          datasource: { uid: 'test-uid', type: 'grafana-clickhouse-datasource' },
          format: 1,
          maxDataPoints: 1000,
          intervalMs: 1000,
        }],
        from: 'now',
        to: 'now',
      });
    });

    it('returns empty array for empty frames', async () => {
      const postFn = vi.fn().mockResolvedValue(makeResponse('q', [], []));
      const adapter = createAdapter(postFn);

      const result = await adapter.executeQuery('SELECT 1');
      expect(result).toEqual([]);
    });

    it('returns empty array when no frames exist', async () => {
      const postFn = vi.fn().mockResolvedValue({
        results: { q: { frames: [] } },
      });
      const adapter = createAdapter(postFn);

      const result = await adapter.executeQuery('SELECT 1');
      expect(result).toEqual([]);
    });

    it('returns empty array when result is missing', async () => {
      const postFn = vi.fn().mockResolvedValue({ results: {} });
      const adapter = createAdapter(postFn);

      const result = await adapter.executeQuery('SELECT 1');
      expect(result).toEqual([]);
    });

    it('converts columnar frames to row objects', async () => {
      const response = makeResponse('q',
        ['name', 'rows', 'bytes_on_disk'],
        [
          ['part1', 'part2'],
          [100, 200],
          [1024, 2048],
        ],
      );
      const postFn = vi.fn().mockResolvedValue(response);
      const adapter = createAdapter(postFn);

      const result = await adapter.executeQuery<{ name: string; rows: number; bytes_on_disk: number }>('SELECT name, rows, bytes_on_disk FROM system.parts');

      expect(result).toEqual([
        { name: 'part1', rows: 100, bytes_on_disk: 1024 },
        { name: 'part2', rows: 200, bytes_on_disk: 2048 },
      ]);
    });

    it('handles single-row response', async () => {
      const response = makeResponse('q', ['version'], [['24.3.1']]);
      const postFn = vi.fn().mockResolvedValue(response);
      const adapter = createAdapter(postFn);

      const result = await adapter.executeQuery<{ version: string }>('SELECT version()');
      expect(result).toEqual([{ version: '24.3.1' }]);
    });

    it('handles null values in frame data', async () => {
      const response = makeResponse('q',
        ['name', 'value'],
        [['a', 'b'], [1, null]],
      );
      const postFn = vi.fn().mockResolvedValue(response);
      const adapter = createAdapter(postFn);

      const result = await adapter.executeQuery('SELECT name, value');
      expect(result).toEqual([
        { name: 'a', value: 1 },
        { name: 'b', value: null },
      ]);
    });

    it('throws AdapterError when result contains an error string', async () => {
      const postFn = vi.fn().mockResolvedValue({
        results: {
          q: {
            frames: [{
              schema: { fields: [{ name: 'x' }] },
              data: { values: [[1]] },
            }],
            error: 'DB::Exception: Table not found',
          },
        },
      });
      const adapter = createAdapter(postFn);

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
      const adapter = createAdapter(() => { throw new Error('unused'); });
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
      const adapter = createAdapter(() => { throw new Error('unused'); });
      const original = new Error('test');
      const wrapped = (adapter as unknown as Record<string, (e: Error) => AdapterError>)
        .wrapError(original);
      expect(wrapped.cause).toBe(original);
    });

    it('handles non-Error values', () => {
      const adapter = createAdapter(() => { throw new Error('unused'); });
      const wrapped = (adapter as unknown as Record<string, (e: unknown) => AdapterError>)
        .wrapError('string error');
      expect(wrapped.message).toBe('string error');
      expect(wrapped.cause).toBeUndefined();
      expect(wrapped.category).toBe('unknown');
    });
  });

  describe('network error wrapping', () => {
    it('wraps post() rejection as AdapterError', async () => {
      const postFn = vi.fn().mockRejectedValue(new Error('Failed to fetch'));
      const adapter = createAdapter(postFn);

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
