/**
 * Time breakdown — decomposing a query's duration into work and waiting.
 *
 * There are two clocks in query_log and they must never be mixed:
 *
 *   - Wall clock (query_duration_ms): what the user waited.
 *   - Thread time (ProfileEvents):    summed across every thread, so on a
 *                                     parallel query it is routinely several
 *                                     times the wall clock.
 *
 * The breakdown only exists in thread time. So this module produces *shares*
 * (0..1), never durations — callers render them as the fill of a bar whose
 * length still comes from the wall clock. That keeps thread-summed microseconds
 * off a wall-clock axis, which is the one thing that would make the bar lie.
 *
 * The residual segment ("unaccounted") is the point of doing this as one
 * composition rather than four separate metrics: lock contention, mutex waits,
 * sleeps and scheduler gaps have no counter of their own, so they only ever
 * show up as the gap between RealTimeMicroseconds and everything nameable.
 */

export type TimeBreakdownKey =
  | 'cpu'
  | 'disk_wait'
  | 'cpu_wait'
  | 'network_wait'
  | 'unaccounted';

export interface TimeBreakdownSegment {
  key: TimeBreakdownKey;
  label: string;
  /** Thread-microseconds attributed to this segment. */
  us: number;
  /** Fraction of total thread time, 0..1. */
  share: number;
}

export interface TimeBreakdown {
  /** Segments with a non-zero share, widest first (residual always last). */
  segments: TimeBreakdownSegment[];
  /** Denominator used — RealTimeMicroseconds, or the segment sum if larger. */
  totalUs: number;
  /**
   * True when the named segments summed to more than RealTimeMicroseconds and
   * were scaled to fit. ClickHouse's counters overlap (a thread can be counted
   * as both waiting and running by different accounting paths), so this is
   * expected occasionally rather than a defect. No residual is shown when set.
   */
  normalized: boolean;
  /**
   * False when OSIOWaitMicroseconds was absent from the map. ClickHouse omits
   * zero-valued events, and the counter also needs procfs/taskstats access, so
   * absence means "no disk wait recorded" — never assert "no disk wait".
   */
  diskWaitReported: boolean;
  /** False when there is not enough data to compose anything. */
  available: boolean;
}

// One label per segment, used verbatim in both the legend and the tooltip.
// Kept to a single word because the legend has to fit four of them across a
// ~250px card, and because two names for one segment reads as two segments.
const LABELS: Record<TimeBreakdownKey, string> = {
  cpu: 'CPU',
  disk_wait: 'Disk',
  cpu_wait: 'Queue',
  network_wait: 'Network',
  unaccounted: 'Unaccounted',
};

/**
 * The counter behind each segment, for display. Exported so the UI can name its
 * source rather than asking the reader to trust a label — every share here is
 * checkable against system.query_log by hand.
 */
export const TIME_BREAKDOWN_EVENTS: Record<TimeBreakdownKey, string> = {
  cpu: 'OSCPUVirtualTimeMicroseconds',
  disk_wait: 'OSIOWaitMicroseconds',
  cpu_wait: 'OSCPUWaitMicroseconds',
  network_wait: 'Network{Receive,Send}ElapsedMicroseconds',
  unaccounted: 'RealTimeMicroseconds minus the above',
};

/** Denominator for every share. */
export const TIME_BREAKDOWN_DENOMINATOR = 'RealTimeMicroseconds';

export type ProfileEventsInput = Record<string, number | string | undefined> | undefined | null;

function ev(pe: NonNullable<ProfileEventsInput>, name: string): number {
  const raw = pe[name];
  if (raw == null) return 0;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

const EMPTY: TimeBreakdown = {
  segments: [],
  totalUs: 0,
  normalized: false,
  diskWaitReported: false,
  available: false,
};

/**
 * Decompose a query's thread time from its ProfileEvents map.
 *
 * Returns shares only. Multiply by a wall-clock duration to lay it on a bar;
 * do not present the microsecond values as elapsed time.
 */
export function computeTimeBreakdown(profileEvents: ProfileEventsInput): TimeBreakdown {
  if (!profileEvents) return EMPTY;

  // OSCPUVirtualTimeMicroseconds is the direct measure; User+System is the
  // fallback for versions/deployments where it is not populated.
  const cpu = ev(profileEvents, 'OSCPUVirtualTimeMicroseconds')
    || (ev(profileEvents, 'UserTimeMicroseconds') + ev(profileEvents, 'SystemTimeMicroseconds'));
  const diskWait = ev(profileEvents, 'OSIOWaitMicroseconds');
  const cpuWait = ev(profileEvents, 'OSCPUWaitMicroseconds');
  const networkWait = ev(profileEvents, 'NetworkReceiveElapsedMicroseconds')
    + ev(profileEvents, 'NetworkSendElapsedMicroseconds');

  const named = cpu + diskWait + cpuWait + networkWait;
  const realTime = ev(profileEvents, 'RealTimeMicroseconds');

  // Without a denominator or any named time there is nothing to compose. A
  // blank bar is better than one built from a guessed total.
  const totalUs = Math.max(realTime, named);
  if (totalUs <= 0) return EMPTY;

  const normalized = named > realTime;
  const unaccounted = normalized ? 0 : totalUs - named;

  const segments: TimeBreakdownSegment[] = ([
    ['cpu', cpu],
    ['disk_wait', diskWait],
    ['cpu_wait', cpuWait],
    ['network_wait', networkWait],
    ['unaccounted', unaccounted],
  ] as [TimeBreakdownKey, number][])
    .filter(([, us]) => us > 0)
    .map(([key, us]) => ({ key, label: LABELS[key], us, share: us / totalUs }));

  // Residual last so the bar reads work -> waits -> unknown; the rest widest
  // first so the dominant cost is the first thing the eye lands on.
  segments.sort((a, b) => {
    if (a.key === 'unaccounted') return 1;
    if (b.key === 'unaccounted') return -1;
    return b.us - a.us;
  });

  return {
    segments,
    totalUs,
    normalized,
    diskWaitReported: 'OSIOWaitMicroseconds' in profileEvents,
    available: segments.length > 0,
  };
}

/** The segment that dominates, or undefined when nothing is composed. */
export function dominantSegment(breakdown: TimeBreakdown): TimeBreakdownSegment | undefined {
  return breakdown.segments.find(s => s.key !== 'unaccounted') ?? breakdown.segments[0];
}

/** Combined share of every segment that represents waiting rather than work. */
export function waitShare(breakdown: TimeBreakdown): number {
  return breakdown.segments
    .filter(s => s.key === 'disk_wait' || s.key === 'cpu_wait' || s.key === 'network_wait')
    .reduce((sum, s) => sum + s.share, 0);
}
