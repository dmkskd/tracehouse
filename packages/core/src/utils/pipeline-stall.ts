/**
 * Pipeline stall summary — naming what a query was blocked on.
 *
 * The time breakdown (see time-breakdown.ts) can say a query's threads were
 * parked but not on what: ClickHouse meters CPU, run-queue, disk and socket
 * syscalls, and nothing else. Coordinators blocked on async remote reads and
 * pipeline threads starved on ports are both structurally untimed in query_log.
 *
 * system.processors_profile_log does name them, per pipeline stage. This module
 * reduces those rows to a single sentence for a summary surface — "which stage,
 * blocked on which side" — with the full table living in the Pipeline tab.
 *
 * Deliberately qualitative. Per-processor waits overlap across concurrently
 * blocked processors and were measured at 3x-100x a query's thread-time
 * residual, so they can rank stages but must never be presented as a share of a
 * duration.
 */

export type PipelineStallKind = 'input' | 'output';

export interface PipelineStallRow {
  name: string;
  input_wait_us: number;
  output_wait_us: number;
  active_us: number;
}

export interface PipelineStall {
  /** Processor name, e.g. "ExpressionTransform". */
  processor: string;
  /** 'input' = starved for upstream data; 'output' = back-pressured. */
  kind: PipelineStallKind;
  waitUs: number;
  activeUs: number;
  /** Human-readable summary, e.g. "ExpressionTransform blocked on input". */
  summary: string;
}

const num = (value: unknown): number => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : 0;
};

/** Normalize a raw ClickHouse row, whose numerics arrive as strings over HTTP. */
export function mapPipelineStallRow(row: Record<string, unknown>): PipelineStallRow {
  return {
    name: String(row.name ?? ''),
    input_wait_us: num(row.input_wait_us),
    output_wait_us: num(row.output_wait_us),
    active_us: num(row.active_us),
  };
}

/**
 * Pick the stage that best explains where a query was blocked.
 *
 * Ranks by the larger of the two wait sides. Stages that spent more time working
 * than waiting are skipped — they are doing their job, not stalling — otherwise
 * a genuinely busy aggregation outranks the stage actually holding things up.
 */
export function summarizePipelineStall(rows: PipelineStallRow[]): PipelineStall | undefined {
  let best: PipelineStall | undefined;

  for (const row of rows) {
    if (!row.name) continue;

    const kind: PipelineStallKind = row.output_wait_us > row.input_wait_us ? 'output' : 'input';
    const waitUs = Math.max(row.input_wait_us, row.output_wait_us);
    if (waitUs <= 0 || waitUs <= row.active_us) continue;

    if (!best || waitUs > best.waitUs) {
      best = {
        processor: row.name,
        kind,
        waitUs,
        activeUs: row.active_us,
        summary: `${row.name} blocked on ${kind}`,
      };
    }
  }

  return best;
}

/** One-line explanation of what the stall kind means. */
export function pipelineStallHint(kind: PipelineStallKind): string {
  return kind === 'input'
    ? 'starved — waiting for upstream data (reads, or a remote shard)'
    : 'back-pressured — downstream could not consume fast enough';
}
