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
});
