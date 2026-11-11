import { describe, expect, it } from 'vitest';
import { relativeIntervalMs, resolveDashboardWindow, timeAxisDomain, timeAxisTicks, timeAxisValues, zoomSelection } from '../time-axis.js';

describe('timeAxisDomain and timeAxisTicks', () => {
  const window = { startMs: Date.UTC(2026, 10, 9, 13, 0, 17), endMs: Date.UTC(2026, 10, 9, 14, 0, 17) };

  it('uses the dashboard window, widened only for rows outside it', () => {
    const inside = [Date.UTC(2026, 10, 9, 13, 30)];
    const before = [Date.UTC(2026, 10, 9, 12, 0), Date.UTC(2026, 10, 9, 13, 30)];
    expect(timeAxisDomain(inside, window)).toEqual([window.startMs, window.endMs]);
    expect(timeAxisDomain(before, window)).toEqual([Date.UTC(2026, 10, 9, 12, 0), window.endMs]);
    expect(timeAxisDomain(before, null)).toEqual(before);
  });

  it('puts ticks on round times inside the domain', () => {
    expect(timeAxisTicks([window.startMs, window.endMs], 6)).toEqual(
      [10, 20, 30, 40, 50, 60].map(minute => Date.UTC(2026, 10, 9, 13, minute)),
    );
  });
});

describe('timeAxisValues', () => {
  it('keeps real spacing for irregular UTC samples', () => {
    expect(timeAxisValues(['2026-11-09T13:00:00Z', '2026-11-09T13:02:00Z', '2026-11-09T13:08:00Z']))
      .toEqual([Date.UTC(2026, 10, 9, 13, 0), Date.UTC(2026, 10, 9, 13, 2), Date.UTC(2026, 10, 9, 13, 8)]);
  });

  it('keeps a label axis for unzoned strings, non-time labels and out-of-order rows', () => {
    expect(timeAxisValues(['2026-11-09 13:00:00', '2026-11-09 13:01:00'])).toBeNull();
    expect(timeAxisValues(['default', 'analytics'])).toBeNull();
    expect(timeAxisValues(['2026-11-09T13:01:00Z', '2026-11-09T13:00:00Z'])).toBeNull();
    expect(timeAxisValues([])).toBeNull();
  });
});

describe('resolveDashboardWindow', () => {
  const nowMs = Date.UTC(2026, 10, 9, 14, 0);

  it('ends a relative interval at now', () => {
    expect(resolveDashboardWindow({ interval: '1 HOUR', absolute: null, nowMs }))
      .toEqual({ startMs: nowMs - 3_600_000, endMs: nowMs });
    expect(relativeIntervalMs('15 MINUTE')).toBe(900_000);
  });

  it('uses an absolute range as given', () => {
    expect(resolveDashboardWindow({
      interval: 'CUSTOM:2026-11-09T12:00:00.000Z,2026-11-09T13:00:00.000Z',
      absolute: { startTime: '2026-11-09T12:00:00.000Z', endTime: '2026-11-09T13:00:00.000Z' },
      nowMs,
    })).toEqual({ startMs: Date.UTC(2026, 10, 9, 12), endMs: Date.UTC(2026, 10, 9, 13) });
  });

  it('has no window for an unknown interval or none at all', () => {
    expect(resolveDashboardWindow({ interval: 'last week', absolute: null, nowMs })).toBeNull();
    expect(resolveDashboardWindow({ interval: null, absolute: null, nowMs })).toBeNull();
  });
});

describe('zoomSelection', () => {
  const domain: [number, number] = [Date.UTC(2026, 10, 11, 12, 0), Date.UTC(2026, 10, 11, 13, 0)];

  it('orders a right-to-left drag, clamps it to the axis and widens it to whole seconds', () => {
    expect(zoomSelection(Date.UTC(2026, 10, 11, 12, 50, 0, 400), Date.UTC(2026, 10, 11, 11, 30), domain))
      .toEqual({ startMs: domain[0], endMs: Date.UTC(2026, 10, 11, 12, 50, 1) });
  });

  it('rejects a selection shorter than the minimum span', () => {
    expect(zoomSelection(Date.UTC(2026, 10, 11, 12, 10), Date.UTC(2026, 10, 11, 12, 10, 30), domain)).toBeNull();
    expect(zoomSelection(Date.UTC(2026, 10, 11, 12, 10), Date.UTC(2026, 10, 11, 12, 10, 30), domain, 10_000))
      .toEqual({ startMs: Date.UTC(2026, 10, 11, 12, 10), endMs: Date.UTC(2026, 10, 11, 12, 10, 30) });
  });
});
