/**
 * Shared time parameters for links into TraceHouse (e.g. Grafana data links).
 *
 * Every page reads the same inputs and translates them into its own URL state
 * on arrival, then strips them so they never sit next to the page's state:
 *
 *   th_from, th_to  a window                      (`${__from}`, `${__to}`)
 *   th_at           an instant, shown with a pad  (`${__value.time}`)
 *   th_pad          time on each side of th_at    (`1m`, `15m`, `1h`; default 5m, minimum 1m)
 *
 * Times are epoch ms or ISO dates. The instant is the focus and the range is
 * context: pages with one window use th_at ± th_pad and ignore the range.
 */

export const LINK_TIME_PARAMS = ['th_from', 'th_to', 'th_at', 'th_pad'] as const;

export const DEFAULT_LINK_PAD_MS = 5 * 60_000;
/** Custom range inputs are minute-precision; a smaller pad can collapse start and end. */
export const MIN_LINK_PAD_MS = 60_000;

const PAD_UNIT_MS: Record<string, number> = {
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

export interface LinkWindow {
  startMs: number;
  endMs: number;
}

export type LinkTime =
  | { kind: 'window'; startMs: number; endMs: number }
  | { kind: 'instant'; atMs: number; padMs: number; range: LinkWindow | null };

/** Epoch ms (digits only) or any date `Date.parse` accepts, e.g. ISO. */
export function parseLinkTime(raw: string | null): number | null {
  if (!raw) return null;
  const value = raw.trim();
  const ms = /^\d+$/.test(value) ? Number(value) : Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

/** `30s`, `5m`, `1.5h`, `1d`; missing or unreadable gives the default, small values the minimum. */
export function parseLinkPad(raw: string | null): number {
  const match = raw?.trim().match(/^(\d+(?:\.\d+)?)([smhd])$/);
  if (!match) return DEFAULT_LINK_PAD_MS;
  return Math.max(MIN_LINK_PAD_MS, Number(match[1]) * PAD_UNIT_MS[match[2]]);
}

export function hasLinkTimeParams(params: URLSearchParams): boolean {
  return LINK_TIME_PARAMS.some(key => params.has(key));
}

/** Read the link's time inputs; null when there are none or they are unusable. */
export function readLinkTime(params: URLSearchParams): LinkTime | null {
  const startMs = parseLinkTime(params.get('th_from'));
  const endMs = parseLinkTime(params.get('th_to'));
  const range = startMs !== null && endMs !== null && endMs > startMs
    ? { startMs, endMs }
    : null;

  const atMs = parseLinkTime(params.get('th_at'));
  if (atMs !== null) {
    return { kind: 'instant', atMs, padMs: parseLinkPad(params.get('th_pad')), range };
  }
  return range ? { kind: 'window', ...range } : null;
}

/** The single window a list page shows: the instant ± pad, or the range. */
export function linkTimeWindow(time: LinkTime): LinkWindow {
  return time.kind === 'instant'
    ? { startMs: time.atMs - time.padMs, endMs: time.atMs + time.padMs }
    : { startMs: time.startMs, endMs: time.endMs };
}

/** The `CUSTOM:<iso>,<iso>` time range that Queries and Merges store. */
export function linkTimeToCustomRange(time: LinkTime): string {
  const { startMs, endMs } = linkTimeWindow(time);
  return `CUSTOM:${new Date(startMs).toISOString()},${new Date(endMs).toISOString()}`;
}

/** Copy of `params` without the link time inputs; everything else is kept. */
export function stripLinkTimeParams(params: URLSearchParams): URLSearchParams {
  const next = new URLSearchParams(params);
  LINK_TIME_PARAMS.forEach(key => next.delete(key));
  return next;
}
