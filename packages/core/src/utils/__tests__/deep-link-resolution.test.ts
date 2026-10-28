import { describe, it, expect, vi, afterEach } from 'vitest';
import { resolveDeepLink, describeLinkStatus, buildMergeLinkUrl } from '../deep-link-resolution.js';
import type { LinkStatus } from '../../services/link-resolver.js';

afterEach(() => {
  vi.useRealTimers();
});

function run(fetchResults: Array<string | null>, statuses: LinkStatus[], signal = new AbortController().signal) {
  const seen: Array<LinkStatus | null> = [];
  let fetchCall = 0;
  let diagnoseCall = 0;
  const promise = resolveDeepLink<string>({
    fetch: async () => fetchResults[Math.min(fetchCall++, fetchResults.length - 1)],
    diagnose: async () => statuses[Math.min(diagnoseCall++, statuses.length - 1)],
    onStatus: s => seen.push(s),
    signal,
    pollIntervalMs: 1_000,
    pendingFlushWindowMs: 5_000,
  });
  return { promise, seen };
}

describe('resolveDeepLink', () => {
  it('polls through running and pending flush, then returns the record', async () => {
    vi.useFakeTimers();
    const { promise, seen } = run(
      [null, null, 'record'],
      [{ state: 'running', hostname: 'h1', elapsedSec: 3 }, { state: 'not_found' }],
    );
    await vi.runAllTimersAsync();
    expect(await promise).toBe('record');
    // not_found right after running means it just finished: reported as pending flush.
    expect(seen.map(s => s?.state ?? null)).toEqual(['running', 'pending_flush', null]);
  });

  it('gives up on pending flush after the window', async () => {
    vi.useFakeTimers();
    const { promise, seen } = run([null], [{ state: 'pending_flush' }]);
    await vi.runAllTimersAsync();
    expect(await promise).toBeNull();
    expect(seen.at(-1)).toEqual({ state: 'not_found' });
    expect(seen.filter(s => s?.state === 'pending_flush')).toHaveLength(5);
  });

  it('reports a record that does not exist as not found, without polling', async () => {
    const { promise, seen } = run([null], [{ state: 'not_found' }]);
    expect(await promise).toBeNull();
    expect(seen).toEqual([{ state: 'not_found' }]);
    expect(describeLinkStatus('merge', 'web.pv all_1_2_1', { state: 'not_found' }))
      .toBe('No record of merge web.pv all_1_2_1 in system.part_log. It may be outside the log\'s retention, on another cluster, or the log may be disabled.');
  });

  it('stops polling when aborted', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const { promise, seen } = run([null], [{ state: 'running', hostname: 'h1', elapsedSec: 1 }], controller.signal);
    await vi.advanceTimersByTimeAsync(2_500);
    controller.abort();
    expect(await promise).toBeNull();
    const count = seen.length;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(seen).toHaveLength(count);
  });
});

describe('describeLinkStatus', () => {
  it('formats long elapsed times as hours and minutes', () => {
    expect(describeLinkStatus('merge', 'db.t part', { state: 'running', hostname: 'h1', elapsedSec: 10435, progress: 0.238 }))
      .toBe('Merge still running on h1 · 24% · 2h 53m. Details open when it finishes.');
  });
});

it('carries the overview attempt start and replica in merge links', () => {
  const params = new URL(buildMergeLinkUrl('db', 'tbl', 'part', 'node-1', 123456), 'https://tracehouse.local').searchParams;
  expect(params.get('md_host')).toBe('node-1');
  expect(params.get('md_start')).toBe('123456');
});
