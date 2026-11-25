import { act, cleanup, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { objectKey, projectCatalogGraph, type Capture } from '@tracehouse/core/services/object-browser';
import { useObjectFocus } from '../useObjectFocus';

const user = objectKey('c', 'user', 'alice'), table = objectKey('c', 'table', 'demo', 'trips');
const operations = ['SELECT'];
const catalog = (): Capture => ({ schemaVersion: 1, mode: 'catalog',
  investigation: { connection: 'c', principal: '', database: '', table: '', operation: 'SELECT' },
  startedAt: '', completedAt: '', hosts: [{ host: 'h', version: '26.9' }], operations: [{ name: 'SELECT', group: 'SELECT' }],
  verdict: { kind: 'not-checked', reason: '' }, coverage: [], relationships: [],
  observations: [{ key: user, name: 'alice', kind: 'user', source: 'users', host: 'h', attributes: {} }],
});
const sideRead = (source: 'users' | 'tables', state: 'failed' | 'not-granted' | 'truncated'): Capture => ({
  ...catalog(), mode: 'investigation', coverage: [{ source, state, hosts: [], visibility: 'unknown' }],
  observations: source === 'users' ? [{ key: table, name: 'demo.trips', kind: 'table', source: 'tables', host: 'h', attributes: { database: 'demo', name: 'trips' } }] : catalog().observations,
});
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}
afterEach(cleanup);

describe('focus side-read lifetime and partial inventory', () => {
  it.each(['failed', 'not-granted', 'truncated'] as const)('keeps the origin and incomplete answer visible when users are %s', async state => {
    const capture = catalog(), read = vi.fn().mockResolvedValue(sideRead('users', state));
    const project = (focus: string | null, from?: Capture) => projectCatalogGraph(from ?? capture, focus ?? undefined);
    const { result } = renderHook(() => useObjectFocus(project, operations, capture, read));
    act(() => result.current.openAt(user, null, table));
    await waitFor(() => expect(result.current.reading).toBe(false));
    expect(result.current.open).toBe(true);
    expect(result.current.graph?.nodes.some(node => node.key === user)).toBe(true);
    expect(result.current.answers[0].state).toBe('incomplete');
    expect(result.current.answers[0].line.text).toContain(`users ${state}`);
    expect(result.current.notFound).toEqual([]);
  });

  it('keeps an unread table unknown while displaying the source failure', async () => {
    const capture = catalog(), read = vi.fn().mockResolvedValue(sideRead('tables', 'failed'));
    const project = (focus: string | null, from?: Capture) => projectCatalogGraph(from ?? capture, focus ?? undefined);
    const { result } = renderHook(() => useObjectFocus(project, operations, capture, read));
    act(() => result.current.openAt(user, null, table));
    await waitFor(() => expect(result.current.answers[0]?.state).toBe('incomplete'));
    expect(result.current.open).toBe(true);
    expect(result.current.notFound).toEqual([]);
  });

  it('discards an outstanding side read when the catalog is refreshed', async () => {
    const first = deferred<Capture>(), second = deferred<Capture>();
    const read = vi.fn().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result, rerender } = renderHook(({ capture }) => useObjectFocus(
      (focus, from) => projectCatalogGraph(from ?? capture, focus ?? undefined), operations, capture, read,
    ), { initialProps: { capture: catalog() } });
    act(() => result.current.openAt(user, null, table));
    rerender({ capture: catalog() });
    expect(read).toHaveBeenCalledTimes(2);
    await act(async () => first.resolve(sideRead('users', 'failed')));
    expect(result.current.reading).toBe(true);
    expect(result.current.answers).toEqual([]);
    await act(async () => second.resolve(sideRead('tables', 'failed')));
    expect(result.current.reading).toBe(false);
    expect(result.current.answers[0].line.text).toContain('tables failed');
  });

  it('does not publish a result from a replaced connection reader', async () => {
    const first = deferred<Capture>(), second = deferred<Capture>(), capture = catalog();
    const oldRead = vi.fn().mockReturnValue(first.promise), newRead = vi.fn().mockReturnValue(second.promise);
    const project = (focus: string | null, from?: Capture) => projectCatalogGraph(from ?? capture, focus ?? undefined);
    const { result, rerender } = renderHook(({ read }) => useObjectFocus(project, operations, capture, read), { initialProps: { read: oldRead } });
    act(() => result.current.openAt(user, null, table));
    rerender({ read: newRead });
    await act(async () => second.resolve(sideRead('tables', 'failed')));
    await act(async () => first.resolve(sideRead('users', 'failed')));
    expect(result.current.open).toBe(true);
    expect(result.current.answers[0].line.text).toContain('tables failed');
    expect(result.current.answers[0].line.text).not.toContain('users failed');
  });
});
