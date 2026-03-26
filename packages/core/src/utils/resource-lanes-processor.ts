/**
 * Pure data processing for resource lanes surface visualization.
 *
 * Extracts business logic from the React component so it can be unit tested:
 * - Lane aggregation (query + merge resources combined)
 * - Normalized stress ranking (weighted share of system)
 * - Per-lane resource breakdowns (for stacked bars)
 * - Grid building (per time-bucket normalized values)
 * - Label formatting (system: strip db prefix, table: truncate query text)
 */

import type { ResourceLanesData, ResourceLaneRow, ResourceLaneMergeRow, ResourceTotalsRow } from '../types/analytics.js';

// ─── Constants ───

export type ResourceChannel =
  | 'total_cpu_us'
  | 'total_memory'
  | 'total_read_bytes'
  | 'total_selected_marks'
  | 'total_io_wait_us'
  | 'total_duration_ms'
  | 'query_count'
  | 'total_read_rows';

export const STRESS_COMPONENTS = [
  { key: 'total_cpu_us' as ResourceChannel, weight: 0.35 },
  { key: 'total_memory' as ResourceChannel, weight: 0.25 },
  { key: 'total_read_bytes' as ResourceChannel, weight: 0.25 },
  { key: 'total_selected_marks' as ResourceChannel, weight: 0.15 },
] as const;

/** Which stress components merges contribute to (merges don't scan marks) */
export const MERGE_CHANNEL_MAP: Partial<Record<ResourceChannel, keyof ResourceLaneMergeRow>> = {
  'total_cpu_us': 'total_cpu_us',
  'total_memory': 'total_memory',
  'total_read_bytes': 'total_read_bytes',
};

export type ViewMode = 'stress' | ResourceChannel;

/**
 * How Z values are scaled for display:
 * - share:    lane / system_total_at_t — relative contribution (sums to ~1.0)
 * - load:     lane / max(system_total) — shows actual system pressure
 * - contrast: rescale so peak cell fills full height — maximizes visual diff
 */
export type StressScale = 'share' | 'load' | 'contrast';

// ─── Types ───

/** Per-lane resource breakdown (proportional shares within a lane's own usage) */
export interface LaneResourceBreakdown {
  cpu: number;
  memory: number;
  io: number;
  marks: number;
}

/** Fully processed lanes data ready for rendering */
export interface ProcessedLanes {
  /** Display grid: Z[lane][time] scaled per the chosen StressScale */
  Z: number[][];
  /** Raw share-of-system grid (unscaled, for tooltips/labels) */
  Zraw: number[][];
  laneLabels: string[];
  laneIds: string[];
  timeLabels: string[];
  systemPeak: number;
  /** Per-lane average share of system (unscaled, for labels) */
  laneAvgShare: number[];
  /** Per-lane resource breakdown for stacked bars */
  laneBreakdowns: LaneResourceBreakdown[];
}

// ─── Internal helpers ───

/** Build per-channel system totals map: ts → value */
function buildSystemTotals(totals: ResourceTotalsRow[], channel: ResourceChannel): Map<string, number> {
  const m = new Map<string, number>();
  for (const t of totals) {
    m.set(t.ts, Number(t[channel] ?? 0));
  }
  return m;
}

/** Compute a single normalized share: lane_value / system_total */
function computeShare(laneVal: number, sysTotal: number): number {
  return sysTotal > 0 ? Math.min(1, laneVal / sysTotal) : 0;
}

// ─── Exported functions ───

/** Aggregate merge totals by timestamp for quick lookup */
export function buildMergeTotalsMap(mergeTotals: ResourceLaneMergeRow[]): Map<string, ResourceLaneMergeRow> {
  const m = new Map<string, ResourceLaneMergeRow>();
  for (const r of mergeTotals) {
    const existing = m.get(r.ts);
    if (existing) {
      existing.total_cpu_us += r.total_cpu_us;
      existing.total_memory += r.total_memory;
      existing.total_read_bytes += r.total_read_bytes;
    } else {
      m.set(r.ts, { ...r });
    }
  }
  return m;
}

/**
 * Build combined system totals per channel (queries + merges).
 * Returns a map: channel → (ts → total value).
 */
export function buildCombinedSystemTotals(
  totals: ResourceTotalsRow[],
  mergeTotalsMap: Map<string, ResourceLaneMergeRow>,
): Map<ResourceChannel, Map<string, number>> {
  const sysTotalsByChannel = new Map<ResourceChannel, Map<string, number>>();
  for (const comp of STRESS_COMPONENTS) {
    const qTotals = buildSystemTotals(totals, comp.key);
    const mergeField = MERGE_CHANNEL_MAP[comp.key];
    if (mergeField) {
      for (const [ts, mrow] of mergeTotalsMap) {
        const existing = qTotals.get(ts) ?? 0;
        qTotals.set(ts, existing + Number(mrow[mergeField] ?? 0));
      }
    }
    sysTotalsByChannel.set(comp.key, qTotals);
  }
  return sysTotalsByChannel;
}

interface LaneAggEntry {
  label: string;
  channelTotals: Record<string, number>;
}

/**
 * Aggregate per-lane channel totals from query lanes + merge data.
 * At system level, merges fold into matching table lanes.
 * At table level, merges get a synthetic "__merges__" lane.
 */
export function aggregateLanes(
  lanes: ResourceLaneRow[],
  merges: ResourceLaneMergeRow[],
  isTableLevel: boolean,
): Map<string, LaneAggEntry> {
  const laneAgg = new Map<string, LaneAggEntry>();

  for (const r of lanes) {
    const existing = laneAgg.get(r.lane_id);
    const cpuVal = Number(r['total_cpu_us'] ?? 0);
    const memVal = Number(r['total_memory'] ?? 0);
    const ioVal = Number(r['total_read_bytes'] ?? 0);
    const marksVal = Number(r['total_selected_marks'] ?? 0);

    if (existing) {
      existing.channelTotals['total_cpu_us'] += cpuVal;
      existing.channelTotals['total_memory'] += memVal;
      existing.channelTotals['total_read_bytes'] += ioVal;
      existing.channelTotals['total_selected_marks'] += marksVal;
    } else {
      laneAgg.set(r.lane_id, {
        label: r.lane_label,
        channelTotals: {
          'total_cpu_us': cpuVal,
          'total_memory': memVal,
          'total_read_bytes': ioVal,
          'total_selected_marks': marksVal,
        },
      });
    }
  }

  // System level: add merge resources to matching table lanes
  if (!isTableLevel) {
    for (const mr of merges) {
      const existing = laneAgg.get(mr.lane_id);
      if (existing) {
        existing.channelTotals['total_cpu_us'] += mr.total_cpu_us;
        existing.channelTotals['total_memory'] += mr.total_memory;
        existing.channelTotals['total_read_bytes'] += mr.total_read_bytes;
      }
    }
  }

  // Table level: synthetic Merges lane
  if (isTableLevel && merges.length > 0) {
    let mergeCpu = 0, mergeMem = 0, mergeIo = 0;
    for (const mr of merges) {
      mergeCpu += mr.total_cpu_us;
      mergeMem += mr.total_memory;
      mergeIo += mr.total_read_bytes;
    }
    laneAgg.set('__merges__', {
      label: 'Merges',
      channelTotals: {
        'total_cpu_us': mergeCpu,
        'total_memory': mergeMem,
        'total_read_bytes': mergeIo,
        'total_selected_marks': 0,
      },
    });
  }

  return laneAgg;
}

/**
 * Rank lanes by normalized weighted stress (share of system, not raw values).
 * Returns entries sorted by descending stress.
 */
export function rankLanes(
  laneAgg: Map<string, LaneAggEntry>,
  sysTotalsByChannel: Map<ResourceChannel, Map<string, number>>,
): Array<{ id: string; info: LaneAggEntry; normalizedStress: number }> {
  // Sum system totals across all time buckets per channel
  const systemChannelTotals: Record<string, number> = {};
  for (const comp of STRESS_COMPONENTS) {
    const chTotals = sysTotalsByChannel.get(comp.key)!;
    let sum = 0;
    for (const v of chTotals.values()) sum += v;
    systemChannelTotals[comp.key] = sum;
  }

  return [...laneAgg.entries()]
    .map(([id, info]) => {
      let normalizedStress = 0;
      for (const comp of STRESS_COMPONENTS) {
        const laneVal = info.channelTotals[comp.key] ?? 0;
        const sysVal = systemChannelTotals[comp.key] || 1;
        normalizedStress += (laneVal / sysVal) * comp.weight;
      }
      return { id, info, normalizedStress };
    })
    .sort((a, b) => b.normalizedStress - a.normalizedStress);
}

/**
 * Compute per-lane resource breakdown (proportional shares within each lane).
 * Used for stacked bars in the UI.
 */
export function computeLaneBreakdowns(
  rankedLanes: Array<{ info: LaneAggEntry }>,
): LaneResourceBreakdown[] {
  return rankedLanes.map(r => {
    const ch = r.info.channelTotals;
    const sum = (ch['total_cpu_us'] || 0) + (ch['total_memory'] || 0) +
      (ch['total_read_bytes'] || 0) + (ch['total_selected_marks'] || 0);
    if (sum === 0) return { cpu: 0, memory: 0, io: 0, marks: 0 };
    return {
      cpu: (ch['total_cpu_us'] || 0) / sum,
      memory: (ch['total_memory'] || 0) / sum,
      io: (ch['total_read_bytes'] || 0) / sum,
      marks: (ch['total_selected_marks'] || 0) / sum,
    };
  });
}

/**
 * Format lane labels:
 * - System level: strip database prefix ("default.events" → "events")
 * - Table level: show query text truncated
 * - Merges lane: "Merges"
 */
export function formatLaneLabels(
  rankedLanes: Array<{ id: string; info: LaneAggEntry }>,
  isTableLevel: boolean,
): string[] {
  return rankedLanes.map(r => {
    if (r.id === '__merges__') return 'Merges';
    const label = r.info.label;
    if (!isTableLevel) {
      const dotIdx = label.indexOf('.');
      if (dotIdx >= 0) return label.slice(dotIdx + 1);
    }
    return label.length > 50 ? label.slice(0, 50) + '…' : label;
  });
}

/** Format timestamps to HH:MM labels */
export function formatTimeLabels(times: string[]): string[] {
  return times.map(t => {
    const match = t.match(/(\d{2}:\d{2})/);
    return match ? match[1] : t.slice(11, 16);
  });
}

/**
 * Build the normalized grid for a single resource channel.
 * Combines query + merge contributions, then normalizes each cell
 * to its share of system total at that time bucket.
 */
export function buildChannelGrid(
  lanes: ResourceLaneRow[],
  merges: ResourceLaneMergeRow[],
  channel: ResourceChannel,
  sysTotals: Map<string, number>,
  laneIdxMap: Map<string, number>,
  timeIdx: Map<string, number>,
  times: string[],
  nLanes: number,
  nTime: number,
  isTableLevel: boolean,
): number[][] {
  const mergeField = MERGE_CHANNEL_MAP[channel];
  const grid: number[][] = Array.from({ length: nLanes }, () => new Array(nTime).fill(0));

  // Query contributions
  for (const r of lanes) {
    const li = laneIdxMap.get(r.lane_id);
    const ti = timeIdx.get(r.ts);
    if (li !== undefined && ti !== undefined) {
      grid[li][ti] += Number(r[channel] ?? 0);
    }
  }

  // Merge contributions
  if (mergeField) {
    for (const mr of merges) {
      const laneId = isTableLevel ? '__merges__' : mr.lane_id;
      const li = laneIdxMap.get(laneId);
      const ti = timeIdx.get(mr.ts);
      if (li !== undefined && ti !== undefined) {
        grid[li][ti] += Number(mr[mergeField] ?? 0);
      }
    }
  }

  // Normalize to share of system
  for (let li = 0; li < nLanes; li++) {
    for (let ti = 0; ti < nTime; ti++) {
      const sysTotal = sysTotals.get(times[ti]) ?? 1;
      grid[li][ti] = computeShare(grid[li][ti], sysTotal);
    }
  }

  return grid;
}

/**
 * Apply the chosen stress scale to a share-of-system grid.
 *
 * - share:    return Zshare as-is (lane / system_total_at_t)
 * - load:     lane_raw / max(system_total) — surface drops when system is idle
 * - contrast: rescale so peak cell = 1.0 — maximizes visual differences
 */
function applyStressScale(
  Zshare: number[][],
  scale: StressScale,
  lanes: ResourceLaneRow[],
  merges: ResourceLaneMergeRow[],
  sysTotalsByChannel: Map<ResourceChannel, Map<string, number>>,
  laneIdxMap: Map<string, number>,
  timeIdx: Map<string, number>,
  times: string[],
  nLanes: number,
  nTime: number,
  isTableLevel: boolean,
): { Z: number[][]; Zraw: number[][]; systemPeak: number } {
  if (scale === 'share') {
    return { Z: Zshare, Zraw: Zshare, systemPeak: 1 };
  }

  if (scale === 'load') {
    // Build raw (unnormalized) weighted stress, then divide by max system total
    // For each component: raw_lane_value / max(system_channel_total) * weight
    const componentRawGrids = STRESS_COMPONENTS.map(comp => {
      const sysTotals = sysTotalsByChannel.get(comp.key)!;
      const maxSysTotal = Math.max(...sysTotals.values(), 1);
      const mergeField = MERGE_CHANNEL_MAP[comp.key];
      const grid: number[][] = Array.from({ length: nLanes }, () => new Array(nTime).fill(0));

      for (const r of lanes) {
        const li = laneIdxMap.get(r.lane_id);
        const ti = timeIdx.get(r.ts);
        if (li !== undefined && ti !== undefined) {
          grid[li][ti] += Number(r[comp.key] ?? 0);
        }
      }
      if (mergeField) {
        for (const mr of merges) {
          const laneId = isTableLevel ? '__merges__' : mr.lane_id;
          const li = laneIdxMap.get(laneId);
          const ti = timeIdx.get(mr.ts);
          if (li !== undefined && ti !== undefined) {
            grid[li][ti] += Number(mr[mergeField] ?? 0);
          }
        }
      }
      // Normalize by max system total (not per-bucket)
      for (let li = 0; li < nLanes; li++) {
        for (let ti = 0; ti < nTime; ti++) {
          grid[li][ti] = Math.min(1, grid[li][ti] / maxSysTotal);
        }
      }
      return { grid, weight: comp.weight };
    });

    const Z: number[][] = Array.from({ length: nLanes }, (_, li) =>
      Array.from({ length: nTime }, (_, ti) => {
        let stress = 0;
        for (const { grid, weight } of componentRawGrids) {
          stress += grid[li][ti] * weight;
        }
        return Math.min(1, stress);
      }),
    );

    let zPeak = 0;
    for (const row of Z) for (const v of row) if (v > zPeak) zPeak = v;
    return { Z, Zraw: Zshare, systemPeak: zPeak || 1 };
  }

  // contrast: rescale Zshare so peak = 1.0
  let peak = 0;
  for (const row of Zshare) for (const v of row) if (v > peak) peak = v;
  const s = peak > 0 ? 1 / peak : 1;
  const Z = Zshare.map(row => row.map(v => Math.min(1, v * s)));
  return { Z, Zraw: Zshare, systemPeak: peak || 1 };
}

/**
 * Main processing function: takes raw ResourceLanesData and produces
 * fully processed data ready for visualization.
 */
export function processLanesData(
  data: ResourceLanesData,
  viewMode: ViewMode,
  stressScale: StressScale = 'share',
): ProcessedLanes | null {
  const { lanes, totals, merges = [], mergeTotals = [] } = data;
  if (lanes.length === 0 || totals.length === 0) return null;

  const times = totals.map(r => r.ts).sort();
  if (times.length < 2) return null;
  const timeIdx = new Map(times.map((t, i) => [t, i]));
  const nTime = times.length;
  const isTableLevel = data.level === 'table';

  // Build combined system totals (queries + merges)
  const mergeTotalsMap = buildMergeTotalsMap(mergeTotals);
  const sysTotalsByChannel = buildCombinedSystemTotals(totals, mergeTotalsMap);

  // Aggregate lanes and rank by normalized stress
  const laneAgg = aggregateLanes(lanes, merges, isTableLevel);
  const ranked = rankLanes(laneAgg, sysTotalsByChannel);
  if (ranked.length === 0) return null;

  const laneIds = ranked.map(r => r.id);
  const laneLabels = formatLaneLabels(ranked, isTableLevel);
  const laneIdxMap = new Map(laneIds.map((id, i) => [id, i]));
  const nLanes = laneIds.length;
  const laneBreakdowns = computeLaneBreakdowns(ranked);
  const timeLabels = formatTimeLabels(times);

  const isStressMode = viewMode === 'stress';

  if (isStressMode) {
    // Build per-component share grids (lane / system_total_at_t), then weighted-sum
    const componentGrids = STRESS_COMPONENTS.map(comp => ({
      grid: buildChannelGrid(lanes, merges, comp.key, sysTotalsByChannel.get(comp.key)!, laneIdxMap, timeIdx, times, nLanes, nTime, isTableLevel),
      weight: comp.weight,
    }));

    // Zshare = share-of-system (always used for tooltips/labels via laneAvgShare)
    const Zshare: number[][] = Array.from({ length: nLanes }, (_, li) =>
      Array.from({ length: nTime }, (_, ti) => {
        let stress = 0;
        for (const { grid, weight } of componentGrids) {
          stress += grid[li][ti] * weight;
        }
        return Math.min(1, stress);
      }),
    );

    const laneAvgShare = Zshare.map(row => {
      const nonZero = row.filter(v => v > 0);
      return nonZero.length > 0 ? nonZero.reduce((a, b) => a + b, 0) / nonZero.length : 0;
    });

    // Apply chosen scale for the display grid (Z)
    const { Z, Zraw, systemPeak } = applyStressScale(Zshare, stressScale, lanes, merges, sysTotalsByChannel, laneIdxMap, timeIdx, times, nLanes, nTime, isTableLevel);

    return { Z, Zraw, laneLabels, laneIds, timeLabels, systemPeak, laneAvgShare, laneBreakdowns };
  } else {
    // Single channel mode
    const channel = viewMode as ResourceChannel;

    // Build combined totals for this channel
    let channelTotals = buildSystemTotals(totals, channel);
    const mergeField = MERGE_CHANNEL_MAP[channel];
    if (mergeField) {
      channelTotals = new Map(channelTotals);
      for (const [ts, mrow] of mergeTotalsMap) {
        const existing = channelTotals.get(ts) ?? 0;
        channelTotals.set(ts, existing + Number(mrow[mergeField] ?? 0));
      }
    }

    // Build raw grid (unnormalized)
    const ZrawAbs: number[][] = Array.from({ length: nLanes }, () => new Array(nTime).fill(0));
    for (const r of lanes) {
      const li = laneIdxMap.get(r.lane_id);
      const ti = timeIdx.get(r.ts);
      if (li !== undefined && ti !== undefined) {
        ZrawAbs[li][ti] = Number(r[channel] ?? 0);
      }
    }
    if (mergeField) {
      for (const mr of merges) {
        const laneId = isTableLevel ? '__merges__' : mr.lane_id;
        const li = laneIdxMap.get(laneId);
        const ti = timeIdx.get(mr.ts);
        if (li !== undefined && ti !== undefined) {
          ZrawAbs[li][ti] += Number(mr[mergeField] ?? 0);
        }
      }
    }

    // Normalize: share of system at each time bucket
    const Zshare: number[][] = ZrawAbs.map(row =>
      row.map((val, ti) => computeShare(val, channelTotals.get(times[ti]) ?? 1)),
    );

    const systemPeak = Math.max(...[...channelTotals.values()], 1);
    const laneAvgShare = Zshare.map(row => {
      const nonZero = row.filter(v => v > 0);
      return nonZero.length > 0 ? nonZero.reduce((a, b) => a + b, 0) / nonZero.length : 0;
    });

    // For single-channel: apply load/contrast scaling to Zshare
    let Z: number[][];
    if (stressScale === 'load') {
      // lane_value / max(system_total) — shows system pressure
      const maxSysTotal = Math.max(...[...channelTotals.values()], 1);
      Z = ZrawAbs.map(row => row.map(v => Math.min(1, v / maxSysTotal)));
    } else if (stressScale === 'contrast') {
      let peak = 0;
      for (const row of Zshare) for (const v of row) if (v > peak) peak = v;
      const s = peak > 0 ? 1 / peak : 1;
      Z = Zshare.map(row => row.map(v => Math.min(1, v * s)));
    } else {
      Z = Zshare;
    }

    return { Z, Zraw: Zshare, laneLabels, laneIds, timeLabels, systemPeak, laneAvgShare, laneBreakdowns };
  }
}
