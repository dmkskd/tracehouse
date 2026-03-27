/**
 * Query health derivation — builds a health tree from live query data.
 *
 * Used by the QueryHealthSunburst visualization but lives in core so
 * it can be tested independently of React/D3.
 */

import type { QueryMetrics, QueryHistoryItem } from '../types/query.js';
import type { QueryConcurrency } from '../types/overview.js';
import type { Health, HealthNode } from './merge-health.js';
import { worstHealth } from './merge-health.js';

// ── Formatters ───────────────────────────────────────────────────

function fmtBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function fmtDuration(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

// ── Thresholds ───────────────────────────────────────────────────

const LONG_RUNNING_WARN_SEC = 60;
const LONG_RUNNING_DANGER_SEC = 300;
const MEMORY_WARN_BYTES = 2e9;    // 2 GB
const MEMORY_DANGER_BYTES = 8e9;  // 8 GB
const STUCK_ELAPSED_SEC = 300;    // 5 min
const STUCK_PROGRESS_THRESHOLD = 0.01;

// ── Stuck query detection ────────────────────────────────────────

export function isQueryStuck(q: QueryMetrics): boolean {
  if (q.elapsed_seconds < STUCK_ELAPSED_SEC) return false;
  // No progress info or negligible progress after 5 min
  if (q.progress <= STUCK_PROGRESS_THRESHOLD) return true;
  // If progress is available, estimate total time
  const estimatedTotal = q.elapsed_seconds / q.progress;
  const remaining = estimatedTotal - q.elapsed_seconds;
  return remaining > LONG_RUNNING_DANGER_SEC;
}

// ── Full health tree derivation ──────────────────────────────────

export function deriveQueryHealth(
  runningQueries: QueryMetrics[],
  recentHistory: QueryHistoryItem[],
  concurrency: QueryConcurrency | null,
): HealthNode {

  // ── 1. Running Queries ──────────────────────────────────────────
  const runningChildren: HealthNode[] = [];

  // Group by query_kind
  const byKind = new Map<string, QueryMetrics[]>();
  for (const q of runningQueries) {
    const kind = q.query_kind || 'Other';
    const arr = byKind.get(kind) || [];
    arr.push(q);
    byKind.set(kind, arr);
  }

  for (const [kind, queries] of byKind) {
    const stuckCount = queries.filter(isQueryStuck).length;
    const longRunners = queries.filter(q => q.elapsed_seconds > LONG_RUNNING_WARN_SEC).length;
    const memHogs = queries.filter(q => q.memory_usage > MEMORY_WARN_BYTES).length;

    const kindHealth: Health = stuckCount > 0 ? 'red'
      : (longRunners > 0 || memHogs > 0) ? 'yellow'
      : 'green';

    const parts: string[] = [`${queries.length} running`];
    if (stuckCount > 0) parts.push(`${stuckCount} stuck`);
    if (longRunners > 0) parts.push(`${longRunners} long-running`);
    if (memHogs > 0) parts.push(`${memHogs} high memory`);

    runningChildren.push({
      name: kind,
      health: kindHealth,
      metric: parts.join(', '),
      children: queries.map(q => {
        const stuck = isQueryStuck(q);
        const longRun = q.elapsed_seconds > LONG_RUNNING_DANGER_SEC;
        const highMem = q.memory_usage > MEMORY_DANGER_BYTES;
        const qHealth: Health = (stuck || longRun || highMem) ? 'red'
          : (q.elapsed_seconds > LONG_RUNNING_WARN_SEC || q.memory_usage > MEMORY_WARN_BYTES) ? 'yellow'
          : 'green';

        const reason = stuck ? ' — appears stuck'
          : longRun ? ' — long-running'
          : highMem ? ` — ${fmtBytes(q.memory_usage)} memory`
          : '';

        return {
          name: q.query_id,
          health: qHealth,
          metric: `${q.elapsed_seconds.toFixed(0)}s elapsed, ${fmtBytes(q.memory_usage)} mem${reason}`,
          size: 1,
        };
      }),
    });
  }
  if (runningChildren.length === 0) {
    runningChildren.push({ name: 'idle', health: 'green', metric: 'No running queries', size: 1 });
  }

  // ── 2. Error Rate ───────────────────────────────────────────────
  const errorChildren: HealthNode[] = [];
  const recentByKind = new Map<string, { total: number; failed: number; errors: QueryHistoryItem[] }>();

  for (const q of recentHistory) {
    const kind = q.query_kind || 'Other';
    const entry = recentByKind.get(kind) || { total: 0, failed: 0, errors: [] };
    entry.total++;
    if (q.exception) {
      entry.failed++;
      entry.errors.push(q);
    }
    recentByKind.set(kind, entry);
  }

  for (const [kind, stats] of recentByKind) {
    const rate = stats.total > 0 ? stats.failed / stats.total : 0;
    const kindHealth: Health = rate > 0.05 ? 'red' : rate > 0.01 ? 'yellow' : 'green';
    const pct = (rate * 100).toFixed(1);

    errorChildren.push({
      name: kind,
      health: kindHealth,
      metric: `${stats.failed}/${stats.total} failed (${pct}%)`,
      children: stats.errors.length > 0 ? stats.errors.slice(0, 10).map(q => ({
        name: q.query_id,
        health: 'red' as Health,
        metric: q.exception ? q.exception.slice(0, 80) : 'Unknown error',
        size: 1,
      })) : [{ name: 'none', health: 'green' as Health, metric: 'No errors', size: 1 }],
    });
  }
  if (errorChildren.length === 0) {
    errorChildren.push({ name: 'none', health: 'green', metric: 'No recent queries', size: 1 });
  }

  // ── 3. Queue Pressure ──────────────────────────────────────────
  const queueChildren: HealthNode[] = [];
  if (concurrency) {
    // Concurrency slots
    const slotUtil = concurrency.maxConcurrent > 0
      ? concurrency.running / concurrency.maxConcurrent
      : 0;
    const slotHealth: Health = slotUtil > 0.8 ? 'red' : slotUtil > 0.5 ? 'yellow' : 'green';
    queueChildren.push({
      name: 'Concurrency',
      health: slotHealth,
      metric: `${concurrency.running}/${concurrency.maxConcurrent} slots (${(slotUtil * 100).toFixed(0)}%)${slotUtil > 0.8 ? ' — near capacity' : ''}`,
      size: 1,
    });

    // Queued queries
    const queuedHealth: Health = concurrency.queued > 5 ? 'red' : concurrency.queued > 0 ? 'yellow' : 'green';
    queueChildren.push({
      name: 'Queued',
      health: queuedHealth,
      metric: `${concurrency.queued} queries waiting${concurrency.queued > 5 ? ' — queue building up' : ''}`,
      size: 1,
    });

    // Rejected queries
    const rejectedHealth: Health = concurrency.rejectedRecent > 10 ? 'red' : concurrency.rejectedRecent > 0 ? 'yellow' : 'green';
    queueChildren.push({
      name: 'Rejected (1h)',
      health: rejectedHealth,
      metric: `${concurrency.rejectedRecent} rejected${concurrency.rejectedRecent > 0 ? ' — TOO_MANY_SIMULTANEOUS_QUERIES' : ''}`,
      size: 1,
    });
  } else {
    queueChildren.push({ name: 'no data', health: 'green', metric: 'Concurrency data unavailable', size: 1 });
  }

  // ── 4. Latency ─────────────────────────────────────────────────
  const latencyChildren: HealthNode[] = [];
  const latencyByKind = new Map<string, number[]>();

  for (const q of recentHistory) {
    if (!q.query_duration_ms && q.query_duration_ms !== 0) continue;
    const kind = q.query_kind || 'Other';
    const arr = latencyByKind.get(kind) || [];
    arr.push(q.query_duration_ms);
    latencyByKind.set(kind, arr);
  }

  for (const [kind, durations] of latencyByKind) {
    if (durations.length === 0) continue;
    durations.sort((a, b) => a - b);
    const p50 = durations[Math.floor(durations.length * 0.5)];
    const p95 = durations[Math.floor(durations.length * 0.95)];
    const max = durations[durations.length - 1];

    // For SELECTs, flag high latency; for INSERTs, be more lenient
    const isSelect = kind.toLowerCase() === 'select';
    const p95Threshold = isSelect ? 10_000 : 30_000;   // 10s / 30s
    const p95Danger = isSelect ? 30_000 : 120_000;      // 30s / 2m

    const kindHealth: Health = p95 > p95Danger ? 'red' : p95 > p95Threshold ? 'yellow' : 'green';

    latencyChildren.push({
      name: kind,
      health: kindHealth,
      metric: `p50 ${fmtDuration(p50)}, p95 ${fmtDuration(p95)}, max ${fmtDuration(max)} (${durations.length} queries)`,
      size: 1,
    });
  }
  if (latencyChildren.length === 0) {
    latencyChildren.push({ name: 'no data', health: 'green', metric: 'No recent query data', size: 1 });
  }

  // ── 5. Index Efficiency ────────────────────────────────────────
  const effChildren: HealthNode[] = [];
  const effByTable = new Map<string, { scores: number[]; fullScans: number; total: number }>();

  for (const q of recentHistory) {
    if (q.query_kind?.toLowerCase() !== 'select') continue;
    if (q.efficiency_score === null || q.efficiency_score === undefined) continue;
    const tables = q.tables || ['unknown'];
    for (const table of tables) {
      const entry = effByTable.get(table) || { scores: [], fullScans: 0, total: 0 };
      entry.scores.push(q.efficiency_score);
      entry.total++;
      if (q.efficiency_score < 10) entry.fullScans++;
      effByTable.set(table, entry);
    }
  }

  for (const [table, stats] of effByTable) {
    const avg = stats.scores.reduce((s, v) => s + v, 0) / stats.scores.length;
    const tableHealth: Health = avg < 50 ? 'red' : avg < 90 ? 'yellow' : 'green';
    const reason = stats.fullScans > 0 ? ` — ${stats.fullScans} full scan(s)` : '';

    effChildren.push({
      name: table.length > 30 ? '...' + table.slice(-27) : table,
      health: tableHealth,
      metric: `${avg.toFixed(0)}% avg pruning (${stats.total} queries)${reason}`,
      size: 1,
    });
  }
  if (effChildren.length === 0) {
    effChildren.push({ name: 'no data', health: 'green', metric: 'No efficiency data available', size: 1 });
  }

  // ── Assemble tree ──────────────────────────────────────────────

  const categories: HealthNode[] = [
    { name: 'Running', health: worstHealth(runningChildren), metric: `${runningQueries.length} active queries`, children: runningChildren },
    { name: 'Errors', health: worstHealth(errorChildren), metric: `Recent error rate`, children: errorChildren },
    { name: 'Queue', health: worstHealth(queueChildren), metric: 'Concurrency and queue pressure', children: queueChildren },
    { name: 'Latency', health: worstHealth(latencyChildren), metric: 'Query duration percentiles', children: latencyChildren },
    { name: 'Efficiency', health: worstHealth(effChildren), metric: 'Index pruning effectiveness', children: effChildren },
  ];

  return { name: 'Query Health', health: worstHealth(categories), metric: 'Overall query subsystem health', children: categories };
}
