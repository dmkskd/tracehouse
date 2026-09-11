import { describe, expect, it } from 'vitest';
import { readTimeTravelCoordinates, writeTimeTravelCoordinates } from '../timeTravelCoordinates';

describe('Time Travel URL coordinates', () => {
  it('round-trips the durable investigation state while preserving unrelated params', () => {
    const initial = new URLSearchParams('nqh=42&qd_id=query-1');
    const encoded = writeTimeTravelCoordinates(initial, {
      queryHashOnly: true,
      windowSec: 450,
      isLive: false,
      autoRefresh: true,
      customStartTime: '2026-09-10T08:00',
      customEndTime: '2026-09-10T10:00',
      viewportEndTime: '2026-09-10T09:30',
      pinnedMs: 1_789_030_800_000,
      selectedEventId: 'event-1',
      zoomRange: [1_789_030_700_000, 1_789_030_900_000],
      metricMode: 'memory',
      viewMode: '2d',
      hiddenCategories: new Set(['merge']),
      activityLimit: 250,
      selectedHosts: ['host-b', 'host-a'],
      perServerView: true,
      selectedTimeRange: 'Custom',
      sortField: 'duration',
      sortDir: 'asc',
      operationKind: 'merge',
      operationScope: 'pin',
      operationSearch: 'analytics.events',
      includeRunning: false,
      eventsVisible: false,
      navigatorShape: 'change',
      hiddenEventSeverities: new Set(['info']),
      hiddenEventCategories: new Set(['availability']),
      hiddenEventKinds: new Set(['error_burst']),
    });

    expect(encoded.get('nqh')).toBe('42');
    expect(encoded.get('qd_id')).toBe('query-1');
    expect(encoded.getAll('tt_host')).toEqual(['host-b', 'host-a']);
    const decoded = readTimeTravelCoordinates(encoded, { eventsVisible: true, navigatorShape: 'trend' });
    expect(decoded).toMatchObject({
      queryHashOnly: true,
      windowSec: 450,
      isLive: false,
      autoRefresh: true,
      metricMode: 'memory',
      perServerView: true,
      selectedTimeRange: 'Custom',
      sortField: 'duration',
      sortDir: 'asc',
      operationKind: 'merge',
      operationScope: 'pin',
      operationSearch: 'analytics.events',
      includeRunning: false,
      eventsVisible: false,
      navigatorShape: 'change',
    });
    expect([...decoded.hiddenCategories]).toEqual(['merge']);
    expect([...decoded.hiddenEventKinds]).toEqual(['error_burst']);
  });

  it('uses recipient preferences only when an older link has no visual coordinates', () => {
    const decoded = readTimeTravelCoordinates(new URLSearchParams(), {
      eventsVisible: false,
      navigatorShape: 'peaks',
    });
    expect(decoded.eventsVisible).toBe(false);
    expect(decoded.navigatorShape).toBe('peaks');
  });

  it('writes the visible activity limit even when it is the product default', () => {
    const defaults = readTimeTravelCoordinates(new URLSearchParams(), {
      eventsVisible: true,
      navigatorShape: 'peaks',
    });
    const encoded = writeTimeTravelCoordinates(new URLSearchParams(), defaults);
    expect(encoded.get('tt_limit')).toBe('100');
    expect(encoded.get('tt_window')).toBe('150');
    expect(encoded.get('tt_metric')).toBe('cpu');
    expect(encoded.get('tt_live')).toBe('1');
  });

  it('scopes the operations table to the pin when a pinned link omits the scope', () => {
    const decoded = readTimeTravelCoordinates(
      new URLSearchParams('tt_pin=1789030800000'),
      { eventsVisible: true, navigatorShape: 'trend' },
    );
    expect(decoded.operationScope).toBe('pin');
  });

  it('defaults the operations table to the window when nothing is pinned', () => {
    const decoded = readTimeTravelCoordinates(
      new URLSearchParams(''),
      { eventsVisible: true, navigatorShape: 'trend' },
    );
    expect(decoded).toMatchObject({ operationScope: 'window', operationKind: 'all', operationSearch: '' });
  });

  it('lets an explicit scope override the pin default', () => {
    const decoded = readTimeTravelCoordinates(
      new URLSearchParams('tt_pin=1789030800000&tt_scope=window'),
      { eventsVisible: true, navigatorShape: 'trend' },
    );
    expect(decoded.operationScope).toBe('window');
  });

  it('falls back for unknown filter values rather than trusting the URL', () => {
    const decoded = readTimeTravelCoordinates(
      new URLSearchParams('tt_ops=bogus&tt_scope=bogus&tt_sort=bogus'),
      { eventsVisible: true, navigatorShape: 'trend' },
    );
    expect(decoded).toMatchObject({ operationKind: 'all', operationScope: 'window', sortField: 'metric' });
  });

  it('accepts the text-sorted columns', () => {
    const decoded = readTimeTravelCoordinates(
      new URLSearchParams('tt_sort=user'),
      { eventsVisible: true, navigatorShape: 'trend' },
    );
    expect(decoded.sortField).toBe('user');
  });

  it('keeps an empty search out of the URL', () => {
    const params = writeTimeTravelCoordinates(new URLSearchParams(), {
      ...baseCoordinates(),
      operationSearch: '',
    });
    expect(params.has('tt_q')).toBe(false);
  });
});

/** Minimal valid coordinates, for cases that only care about one field. */
function baseCoordinates() {
  return {
    queryHashOnly: false,
    windowSec: 150,
    isLive: true,
    autoRefresh: false,
    customStartTime: null,
    customEndTime: null,
    viewportEndTime: null,
    pinnedMs: null,
    selectedEventId: null,
    zoomRange: null,
    metricMode: 'cpu' as const,
    viewMode: '2d' as const,
    hiddenCategories: new Set<'query' | 'merge' | 'mutation'>(),
    activityLimit: 100,
    selectedHosts: [],
    perServerView: false,
    selectedTimeRange: '1h',
    sortField: 'metric' as const,
    sortDir: 'desc' as const,
    operationKind: 'all' as const,
    operationScope: 'window' as const,
    operationSearch: '',
    includeRunning: true,
    eventsVisible: true,
    navigatorShape: 'peaks' as const,
    hiddenEventSeverities: new Set<string>(),
    hiddenEventCategories: new Set<string>(),
    hiddenEventKinds: new Set<string>(),
  };
}
