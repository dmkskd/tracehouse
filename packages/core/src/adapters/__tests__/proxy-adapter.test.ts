import { afterEach, describe, expect, it, vi } from 'vitest';
import { ProxyAdapter } from '../proxy-adapter.js';
import { tagQuery } from '../../queries/builder.js';

afterEach(() => vi.unstubAllGlobals());
describe('Proxy typed UTC output', () => {
  it.each([false, true])('forwards the opt-in UTC setting (%s) without rewriting SQL', async utcDateTimeOutput => {
    const fetchMock = vi.fn().mockImplementation(async () => new Response('{"clock":"2026-11-08T17:00:00Z"}\n'));
    vi.stubGlobal('fetch', fetchMock);
    const adapter = new ProxyAdapter({ host: 'localhost', port: 8123, user: 'default', password: '', database: 'default', secure: false, connect_timeout: 10, send_receive_timeout: 30 });
    const sql = tagQuery('SELECT now() AS clock', 'TraceHouse:Analytics:proxyTest');
    await adapter.executeQuery(sql, { utcDateTimeOutput, queryId: 'query-id' });
    const [url, request] = fetchMock.mock.calls[0];
    expect(new URL(url).searchParams.get('date_time_output_format')).toBe(utcDateTimeOutput ? 'iso' : null);
    expect(request.body).toBe(sql);
    expect(request.headers['x-ch-query-id']).toBe('query-id');
    await adapter.executeRawQuery(sql, 'fixture', { utcDateTimeOutput, queryId: 'raw-id' });
    const [rawUrl, rawRequest] = fetchMock.mock.calls[1];
    expect(new URL(rawUrl).searchParams.get('date_time_output_format')).toBe(utcDateTimeOutput ? 'iso' : null);
    expect(rawRequest.headers['x-ch-database']).toBe('fixture');
    expect(rawRequest.headers['x-ch-query-id']).toBe('raw-id');
  });
});
