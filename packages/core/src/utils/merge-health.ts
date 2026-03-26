/**
 * Merge health derivation — builds a health tree from live merge data.
 *
 * Used by the MergeHealthSunburst visualization but lives in core so
 * it can be tested independently of React/D3.
 */

import type { MergeInfo, MutationInfo, BackgroundPoolMetrics, MergeThroughputEstimate } from '../types/merge.js';
import { pickThroughputEstimate } from './merge-eta.js';

// ── Types ──────────────────────────────────────────────────────────

export type Health = 'green' | 'yellow' | 'red';

export interface HealthNode {
  name: string;
  health: Health;
  metric: string;
  size?: number;
  children?: HealthNode[];
}

/** Map of "database.table" → throughput estimates from part_log */
export type ThroughputMap = Map<string, MergeThroughputEstimate[]>;

// ── Formatters (minimal, no frontend dependency) ───────────────────

function fmtBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function fmtRate(bytesPerSec: number): string {
  if (bytesPerSec === 0) return '0 B/s';
  return `${fmtBytes(bytesPerSec)}/s`;
}

// ── Stuck merge detection ──────────────────────────────────────────

/** Thresholds for elapsed time (in seconds) */
const ELAPSED_WARN_SEC = 10 * 60;   // 10 min
const ELAPSED_DANGER_SEC = 30 * 60; // 30 min

/**
 * A merge is considered "potentially stuck" when it has been running for a long
 * time with very little progress.  This is a heuristic: ClickHouse reports
 * `progress` as bytes_read/total_bytes which can be misleading for vertical
 * merges (reads are front-loaded) or when a merge is in its finalization phase.
 */
export function isMergeStuck(m: MergeInfo): boolean {
  if (m.elapsed < ELAPSED_WARN_SEC) return false;
  // progress >= 99.95% but still in system.merges — stuck in finalization
  if (m.progress >= 0.9995) return m.elapsed > ELAPSED_DANGER_SEC;
  if (m.progress <= 0.001) return true; // no progress at all after 10min
  const estimatedTotal = m.elapsed / m.progress;
  const remaining = estimatedTotal - m.elapsed;
  return remaining > ELAPSED_DANGER_SEC;
}

// ── Worst health reducer ───────────────────────────────────────────

export function worstHealth(nodes: Pick<HealthNode, 'health'>[]): Health {
  if (nodes.some(n => n.health === 'red')) return 'red';
  if (nodes.some(n => n.health === 'yellow')) return 'yellow';
  return 'green';
}

// ── Per-merge throughput health ────────────────────────────────────

export function mergeThroughputHealth(
  m: MergeInfo,
  estimates: ThroughputMap,
): { health: Health; metric: string } {
  const key = `${m.database}.${m.table}`;
  const tableEstimates = estimates.get(key);
  const est = tableEstimates
    ? pickThroughputEstimate(tableEstimates, m.merge_algorithm, m.total_size_bytes_compressed)
    : null;

  const liveRate = m.elapsed > 0 ? (m.total_size_bytes_compressed * m.progress) / m.elapsed : 0;
  const pct = (m.progress * 100).toFixed(0);

  if (!est || est.merge_count < 3) {
    let health: Health = 'green';
    let reason = '';
    if (m.progress < 0.1 && m.elapsed > 60) { health = 'red'; reason = ' — <10% after 60s, no baseline data'; }
    else if (m.progress < 0.3 && m.elapsed > 30) { health = 'yellow'; reason = ' — slow start, no baseline data'; }
    return {
      health,
      metric: `${pct}% in ${m.elapsed.toFixed(0)}s — ${fmtBytes(m.total_size_bytes_compressed)}${reason}`,
    };
  }

  const expected = est.median_bytes_per_sec;
  const ratio = expected > 0 ? liveRate / expected : 1;
  const rateStr = liveRate > 0 ? fmtRate(liveRate) : 'starting';
  const pctOfExpected = (ratio * 100).toFixed(0);

  let health: Health = 'green';
  let reason = '';
  if (ratio < 0.25 && m.elapsed > 30) {
    health = 'red';
    reason = ` — ${pctOfExpected}% of expected rate (${fmtRate(expected)})`;
  } else if (ratio < 0.5 && m.elapsed > 10) {
    health = 'yellow';
    reason = ` — ${pctOfExpected}% of expected rate (${fmtRate(expected)})`;
  }

  return {
    health,
    metric: `${pct}% in ${m.elapsed.toFixed(0)}s — ${rateStr}${reason}`,
  };
}

// ── Full health tree derivation ────────────────────────────────────

export function deriveHealth(
  activeMerges: MergeInfo[],
  mutations: MutationInfo[],
  poolMetrics: BackgroundPoolMetrics | null,
  throughputEstimates: ThroughputMap,
): HealthNode {
  const mergesByTable = new Map<string, MergeInfo[]>();
  for (const m of activeMerges) {
    const key = `${m.database}.${m.table}`;
    const arr = mergesByTable.get(key) || [];
    arr.push(m);
    mergesByTable.set(key, arr);
  }

  // Part Count Pressure
  const partCountChildren: HealthNode[] = [];
  for (const [table, merges] of mergesByTable) {
    const totalParts = merges.reduce((s, m) => s + m.num_parts, 0);
    const stuckCount = merges.filter(isMergeStuck).length;
    const health: Health = stuckCount > 0 ? 'red' : totalParts > 20 ? 'yellow' : 'green';
    const tableReason = stuckCount > 0 ? ` — ${stuckCount} stuck merge(s)` : totalParts > 20 ? ' — high part count, merges may be falling behind' : '';
    partCountChildren.push({
      name: table, health,
      metric: `${merges.length} active merges, ${totalParts} source parts${tableReason}`,
      children: merges.map(m => {
        const stuck = isMergeStuck(m);
        const mergeHealth: Health = stuck ? 'red' : m.num_parts > 20 ? 'yellow' : 'green';
        const reason = stuck ? ' — merge appears stuck (no progress)' : m.num_parts > 20 ? ' — merging many parts' : '';
        return {
          name: m.result_part_name,
          health: mergeHealth,
          metric: `${(m.progress * 100).toFixed(0)}% — ${m.num_parts} parts → ${m.result_part_name}${reason}`,
          size: 1,
        };
      }),
    });
  }
  if (partCountChildren.length === 0) {
    partCountChildren.push({ name: 'no tables', health: 'green', metric: 'No active merges', size: 1 });
  }

  // Merge Throughput — compared against historical expectations from part_log
  const throughputChildren: HealthNode[] = [];
  for (const [table, merges] of mergesByTable) {
    const mergeHealths = merges.map(m => mergeThroughputHealth(m, throughputEstimates));
    const tableHealth = worstHealth(mergeHealths);
    throughputChildren.push({
      name: table, health: tableHealth,
      metric: `${merges.length} active merges`,
      children: merges.map((m, i) => ({
        name: m.result_part_name,
        health: mergeHealths[i].health,
        metric: mergeHealths[i].metric,
        size: 1,
      })),
    });
  }
  if (throughputChildren.length === 0) {
    throughputChildren.push({ name: 'idle', health: 'green', metric: 'No active merges', size: 1 });
  }

  // Mutations
  const mutationsByTable = new Map<string, MutationInfo[]>();
  for (const m of mutations) {
    const key = `${m.database}.${m.table}`;
    const arr = mutationsByTable.get(key) || [];
    arr.push(m);
    mutationsByTable.set(key, arr);
  }
  const mutChildren: HealthNode[] = [];
  for (const [table, muts] of mutationsByTable) {
    const failedCount = muts.filter(m => m.latest_fail_reason).length;
    const health: Health = failedCount > 0 ? 'red' : muts.length > 5 ? 'yellow' : 'green';
    const tableReason = failedCount > 0 ? ` — ${failedCount} failed` : muts.length > 5 ? ' — mutation queue building up' : '';
    mutChildren.push({
      name: table, health,
      metric: `${muts.length} pending mutations${tableReason}`,
      children: muts.map(m => ({
        name: m.mutation_id,
        health: m.latest_fail_reason ? 'red' : 'green',
        metric: m.latest_fail_reason
          ? `FAILED: ${m.latest_fail_reason.slice(0, 80)}`
          : m.command.slice(0, 60) + (m.command.length > 60 ? '...' : ''),
        size: 1,
      })),
    });
  }
  if (mutChildren.length === 0) {
    mutChildren.push({ name: 'none', health: 'green', metric: 'No pending mutations', size: 1 });
  }

  // Pool Saturation
  const poolChildren: HealthNode[] = [];
  if (poolMetrics) {
    for (const p of [
      { name: 'Merge/Mutation', active: poolMetrics.merge_pool_active, total: poolMetrics.merge_pool_size },
      { name: 'Move', active: poolMetrics.move_pool_active, total: poolMetrics.move_pool_size },
      { name: 'Fetch', active: poolMetrics.fetch_pool_active, total: poolMetrics.fetch_pool_size },
      { name: 'Schedule', active: poolMetrics.schedule_pool_active, total: poolMetrics.schedule_pool_size },
      { name: 'Common', active: poolMetrics.common_pool_active, total: poolMetrics.common_pool_size },
      { name: 'Distributed', active: poolMetrics.distributed_pool_active, total: poolMetrics.distributed_pool_size },
    ].filter(p => p.total > 0)) {
      const util = p.total > 0 ? p.active / p.total : 0;
      const poolHealth: Health = util > 0.8 ? 'red' : util > 0.5 ? 'yellow' : 'green';
      const reason = util > 0.8 ? ' — near saturation, new merges may queue'
        : util > 0.5 ? ' — over 50% utilized' : '';
      poolChildren.push({
        name: p.name, health: poolHealth,
        metric: `${p.active}/${p.total} threads (${(util * 100).toFixed(0)}%)${reason}`,
        size: 1,
      });
    }
  }
  if (poolChildren.length === 0) {
    poolChildren.push({ name: 'no data', health: 'green', metric: 'Pool metrics unavailable', size: 1 });
  }

  // Resources
  const totalBytes = activeMerges.reduce((s, m) => s + m.total_size_bytes_compressed, 0);
  const totalMemory = activeMerges.reduce((s, m) => s + m.memory_usage, 0);
  const bytesHealth: Health = totalBytes > 10e9 ? 'yellow' : 'green';
  const memHealth: Health = totalMemory > 4e9 ? 'red' : totalMemory > 1e9 ? 'yellow' : 'green';
  const diskChildren: HealthNode[] = [
    { name: 'Merge bytes', health: bytesHealth, metric: `${fmtBytes(totalBytes)} being merged${totalBytes > 10e9 ? ' — over 10 GB in-flight' : ''}`, size: 1 },
    { name: 'Memory', health: memHealth, metric: `${fmtBytes(totalMemory)} merge memory${totalMemory > 4e9 ? ' — over 4 GB, risk of OOM' : totalMemory > 1e9 ? ' — over 1 GB memory pressure' : ''}`, size: 1 },
  ];
  if (poolMetrics) {
    const cleanupHealth: Health = poolMetrics.outdated_parts > 500 ? 'red' : poolMetrics.outdated_parts > 100 ? 'yellow' : 'green';
    diskChildren.push({
      name: 'Pending cleanup',
      health: cleanupHealth,
      metric: `${poolMetrics.outdated_parts} outdated parts (${fmtBytes(poolMetrics.outdated_parts_bytes)})${poolMetrics.outdated_parts > 500 ? ' — cleanup falling behind' : poolMetrics.outdated_parts > 100 ? ' — parts accumulating' : ''}`,
      size: 1,
    });
  }

  const categories: HealthNode[] = [
    { name: 'Part Count', health: worstHealth(partCountChildren), metric: `Active parts across ${mergesByTable.size} tables`, children: partCountChildren },
    { name: 'Throughput', health: worstHealth(throughputChildren), metric: 'Merge speed vs historical baseline', children: throughputChildren },
    { name: 'Mutations', health: worstHealth(mutChildren), metric: `${mutations.length} pending mutations`, children: mutChildren },
    { name: 'Pool Usage', health: worstHealth(poolChildren), metric: 'Background thread pool saturation', children: poolChildren },
    { name: 'Resources', health: worstHealth(diskChildren), metric: 'Memory and disk pressure', children: diskChildren },
  ];

  return { name: 'Merge Health', health: worstHealth(categories), metric: 'Overall merge subsystem health', children: categories };
}
