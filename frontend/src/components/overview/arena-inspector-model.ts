/**
 * What the ArenaInspector shows for a block, and what each value is called.
 *
 * An arena entry is in one of three phases, and its numbers mean different
 * things in each:
 * - running: live poll values. CPU and read/write rates are averages since the
 *   start (CPU time / elapsed, bytes / elapsed); memory is current. Running
 *   merges have no CPU data (system.merges does not expose it), so CPU is left
 *   out; a merge's `cpu` is 0 unless part_log recorded it.
 * - final: values from query_log / part_log: duration, run averages, peak memory.
 *   Progress is meaningless once finished, so it is never shown.
 * - last-sample: seen live, finished, final record not loaded (yet, or ever for
 *   operations shorter than the backfill minimum). Values are the last poll's.
 */

import { formatBytes, formatElapsed } from '../../utils/formatters';
import { fmtMicro, fmtNum, fmtRate } from './arena-hud-format';
import type { BlockEntry } from './arena-types';

export type InspectorPhase = 'running' | 'final' | 'last-sample';

export interface InspectorValue {
  label: string;
  value: string;
}

export interface InspectorModel {
  phase: InspectorPhase;
  /** Running only, when the operation reports progress. */
  progress: { pct: number; caption: string } | null;
  /** Final only: duration, average CPU, peak memory. */
  hero: InspectorValue[] | null;
  resources: InspectorValue[];
  context: InspectorValue[];
  /** Explains values that are not what they appear to be. */
  note: string | null;
}

export function inspectorPhase(entry: BlockEntry): InspectorPhase {
  if (entry.endTime === null) return 'running';
  return entry.final ? 'final' : 'last-sample';
}

export function buildInspectorModel(entry: BlockEntry): InspectorModel {
  const phase = inspectorPhase(entry);
  const pe = entry.profileEvents;
  const readRate = entry.isMerge ? (entry.readBytesPerSec || 0) : entry.ioReadRate;
  const writeRate = entry.writeBytesPerSec || 0;
  const progressPct = entry.isMerge ? entry.progress * 100 : entry.progress;

  const resources: InspectorValue[] = [];
  const push = (cond: boolean, label: string, value: () => string) => {
    if (cond) resources.push({ label, value: value() });
  };

  let hero: InspectorValue[] | null = null;
  if (phase === 'final') {
    hero = [{ label: 'duration', value: formatElapsed(entry.elapsed) }];
    if (entry.cpu > 0) hero.push({ label: 'avg CPU cores', value: entry.cpu.toFixed(2) });
    if (entry.mem > 0) hero.push({ label: 'peak memory', value: formatBytes(entry.mem) });
  } else {
    // A merge's CPU before its part_log record is the arena's placeholder.
    push(entry.cpu > 0 && !entry.isMerge, 'Avg CPU', () => `${entry.cpu.toFixed(2)} cores`);
    push(entry.mem > 0, 'Memory', () => formatBytes(entry.mem));
  }

  push(!entry.isMerge && entry.rowsRead > 0, 'Rows read', () => fmtNum(entry.rowsRead));
  push(readRate > 0, 'Avg read', () => fmtRate(readRate));
  push(entry.isMerge && writeRate > 0, 'Avg write', () => fmtRate(writeRate));
  push(entry.isMerge && entry.numParts !== undefined, 'Parts',
    () => [String(entry.numParts), entry.mergeType].filter(Boolean).join(' · '));
  push(!!pe && (pe.userTimeMicroseconds > 0 || pe.systemTimeMicroseconds > 0), 'CPU time',
    () => `${fmtMicro(pe!.userTimeMicroseconds)} user · ${fmtMicro(pe!.systemTimeMicroseconds)} sys`);
  push(!!pe && (pe.osReadBytes > 0 || pe.osWriteBytes > 0), 'Disk',
    () => [pe!.osReadBytes > 0 && `${formatBytes(pe!.osReadBytes)} read`, pe!.osWriteBytes > 0 && `${formatBytes(pe!.osWriteBytes)} written`]
      .filter(Boolean).join(' · '));
  push(!!pe && pe.selectedParts > 0, 'Selected',
    () => `${fmtNum(pe!.selectedParts)} parts · ${fmtNum(pe!.selectedMarks)} marks`);

  const context: InspectorValue[] = [];
  if (entry.user) context.push({ label: 'User', value: entry.user });
  if (entry.hostname) context.push({ label: 'Host', value: entry.hostname });

  return {
    phase,
    progress: phase === 'running' && progressPct > 0
      ? { pct: Math.min(100, progressPct), caption: entry.isMerge ? 'merged' : 'of estimated rows read' }
      : null,
    hero,
    resources,
    context,
    note: phase === 'last-sample' ? 'Finished between polls. Values are from the last live sample.' : null,
  };
}
