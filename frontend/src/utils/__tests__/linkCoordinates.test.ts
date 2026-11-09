import { describe, expect, it } from 'vitest';
import {
  DEFAULT_LINK_PAD_MS,
  MIN_LINK_PAD_MS,
  linkTimeToCustomRange,
  linkTimeWindow,
  parseLinkPad,
  parseLinkTime,
  readLinkTime,
  stripLinkTimeParams,
} from '../linkCoordinates';

const AT = Date.parse('2026-11-09T12:00:00.000Z');

describe('link time parsing', () => {
  it('reads epoch ms as Grafana passes it, and ISO dates as people type them', () => {
    expect(parseLinkTime(String(AT))).toBe(AT);
    expect(parseLinkTime('2026-11-09T12:00:00Z')).toBe(AT);
    expect(parseLinkTime('${__value.time}')).toBeNull();
    expect(parseLinkTime('')).toBeNull();
  });

  it('reads pads with units, defaulting and raising to the minimum', () => {
    expect(parseLinkPad('15m')).toBe(15 * 60_000);
    expect(parseLinkPad('1h')).toBe(3_600_000);
    expect(parseLinkPad(null)).toBe(DEFAULT_LINK_PAD_MS);
    expect(parseLinkPad('5 minutes')).toBe(DEFAULT_LINK_PAD_MS);
    expect(parseLinkPad('30s')).toBe(MIN_LINK_PAD_MS);
  });
});

describe('readLinkTime', () => {
  it('turns an instant into a window of pad on each side', () => {
    const time = readLinkTime(new URLSearchParams(`th_at=${AT}&th_pad=10m`));
    expect(time && linkTimeWindow(time)).toEqual({ startMs: AT - 600_000, endMs: AT + 600_000 });
  });

  it('keeps the range as context when an instant is also given; the instant decides the window', () => {
    const params = new URLSearchParams(`th_from=${AT - 3_600_000}&th_to=${AT + 3_600_000}&th_at=${AT}`);
    const time = readLinkTime(params);
    expect(time).toEqual({
      kind: 'instant',
      atMs: AT,
      padMs: DEFAULT_LINK_PAD_MS,
      range: { startMs: AT - 3_600_000, endMs: AT + 3_600_000 },
    });
    expect(linkTimeWindow(time!)).toEqual({ startMs: AT - DEFAULT_LINK_PAD_MS, endMs: AT + DEFAULT_LINK_PAD_MS });
  });

  it('uses the range alone, ignores a pad without an instant, and rejects an inverted range', () => {
    expect(readLinkTime(new URLSearchParams(`th_from=${AT}&th_to=${AT + 60_000}&th_pad=1h`)))
      .toEqual({ kind: 'window', startMs: AT, endMs: AT + 60_000 });
    expect(readLinkTime(new URLSearchParams(`th_from=${AT}&th_to=${AT - 60_000}`))).toBeNull();
    expect(readLinkTime(new URLSearchParams('status=error'))).toBeNull();
  });

  it('produces the CUSTOM range Queries and Merges store', () => {
    const time = readLinkTime(new URLSearchParams(`th_at=${AT}&th_pad=1m`))!;
    expect(linkTimeToCustomRange(time)).toBe('CUSTOM:2026-11-09T11:59:00.000Z,2026-11-09T12:01:00.000Z');
  });
});

describe('stripLinkTimeParams', () => {
  it('removes only the link time inputs', () => {
    const params = new URLSearchParams(`status=error&th_at=${AT}&th_pad=5m&th_from=1&th_to=2&th_ds=ch`);
    expect(stripLinkTimeParams(params).toString()).toBe('status=error&th_ds=ch');
  });
});
