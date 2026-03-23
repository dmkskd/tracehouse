/**
 * MergeDetailModal — Full detail modal for merge/mutation operations.
 *
 * Reusable across pages:
 *   - TimeTravelPage: opens from timeline chart, passes MergeSeries/MutationSeries
 *   - MergeTracker: opens from MergeHistoryDetailPanel, passes MergeHistoryRecord
 *
 * Shows Details (with TTLMove info), Logs, and Profile tabs.
 */

import React, { useState, useEffect, useMemo } from 'react';
import type { MergeHistoryRecord, MergeInfo, MergeThroughputEstimate } from '@tracehouse/core';
import type { MergeSeries, MutationSeries } from '@tracehouse/core';
import type { VerticalMergeProgress } from '@tracehouse/core';
import { classifyActiveMerge, parseVerticalMergeProgress, isMergedPart, computeMergeEta, pickThroughputEstimate } from '@tracehouse/core';
import { useNavigate } from '../../hooks/useAppLocation';
import { ModalWrapper } from '../shared/ModalWrapper';
import { formatBytes } from '../../stores/databaseStore';
import { formatDuration } from '../../utils/formatters';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { encodeSql } from '../../hooks/useUrlState';
import { TraceLogViewer } from '../tracing/TraceLogViewer';
import type { TraceLogFilter } from '../../stores/traceStore';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { MergeXRay } from './MergeXRay';
import { useProfileEventDescriptionsStore } from '../../stores/profileEventDescriptionsStore';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

/** Open from TimeTravelPage — pass a MergeSeries or MutationSeries, record is fetched */
export interface MergeDetailModalProps {
  merge: MergeSeries | MutationSeries | null;
  onClose: () => void;
  /** When true, uses "Mutation" titles instead of "Merge" */
  isMutation?: boolean;
}


/** Open from MergeTracker — pass a MergeHistoryRecord directly */
export interface MergeDetailModalFromRecordProps {
  record: MergeHistoryRecord | null;
  onClose: () => void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type MergeDetailTab = 'details' | 'logs' | 'profile' | 'xray';

/** Build a partial MergeHistoryRecord from an active MergeInfo so the detail modal can render. */
function mergeInfoToPartialRecord(merge: MergeInfo): MergeHistoryRecord {
  return {
    event_time: new Date().toISOString(),
    event_type: 'MergeParts',
    database: merge.database,
    table: merge.table,
    part_name: merge.result_part_name,
    partition_id: '',
    rows: merge.rows_written,
    size_in_bytes: merge.total_size_bytes_compressed,
    duration_ms: merge.elapsed * 1000,
    merge_reason: classifyActiveMerge(merge.merge_type, merge.is_mutation, merge.result_part_name),
    source_part_names: merge.source_part_names,
    bytes_uncompressed: merge.bytes_read_uncompressed,
    read_bytes: merge.bytes_read_uncompressed,
    read_rows: merge.rows_read,
    peak_memory_usage: merge.memory_usage,
    size_diff: 0,
    size_diff_pct: 0,
    rows_diff: 0,
    hostname: merge.hostname,
    merge_algorithm: merge.merge_algorithm,
  };
}

/** Build a partial MergeHistoryRecord from a MergeSeries/MutationSeries (running, no part_log yet). */
function seriesToPartialRecord(series: MergeSeries | MutationSeries, isMutation: boolean): MergeHistoryRecord {
  const dotIdx = series.table.indexOf('.');
  const db = dotIdx > 0 ? series.table.slice(0, dotIdx) : 'default';
  const tbl = dotIdx > 0 ? series.table.slice(dotIdx + 1) : series.table;
  return {
    event_time: series.start_time,
    event_type: 'MergeParts',
    database: db,
    table: tbl,
    part_name: series.part_name,
    partition_id: '',
    rows: 0,
    size_in_bytes: 0,
    duration_ms: series.duration_ms,
    merge_reason: isMutation ? 'Mutation' : ('merge_reason' in series ? (series as MergeSeries).merge_reason || '' : ''),
    source_part_names: [],
    bytes_uncompressed: 0,
    read_bytes: 0,
    read_rows: 0,
    peak_memory_usage: series.peak_memory,
    size_diff: 0,
    size_diff_pct: 0,
    rows_diff: 0,
    hostname: series.hostname,
  };
}

const MERGE_PROFILE_EVENTS = new Set([
  'Merge', 'MergeSourceParts', 'MergedRows', 'MergedColumns', 'GatheredColumns',
  'MergedUncompressedBytes', 'MergeTotalMilliseconds', 'MergeExecuteMilliseconds',
  'MergeHorizontalStageTotalMilliseconds', 'MergeVerticalStageTotalMilliseconds',
  'MergeProjectionStageTotalMilliseconds',
]);

// ---------------------------------------------------------------------------
// VerticalMergeProgressSection — Gantt timeline for vertical merge columns
// ---------------------------------------------------------------------------

const fmtDuration = (sec: number) => sec < 1 ? `${(sec * 1000).toFixed(0)}ms` : `${sec.toFixed(2)}s`;
const fmtThroughput = (bytes: number, sec: number) => {
  if (sec <= 0) return '-';
  const bps = bytes / sec;
  if (bps >= 1024 * 1024 * 1024) return `${(bps / (1024 * 1024 * 1024)).toFixed(2)} GiB/s`;
  if (bps >= 1024 * 1024) return `${(bps / (1024 * 1024)).toFixed(1)} MiB/s`;
  return `${(bps / 1024).toFixed(0)} KiB/s`;
};

const ROW_HEIGHT = 18;
const LABEL_WIDTH = 130;
const DURATION_WIDTH = 50;

const VerticalMergeProgressSection: React.FC<{
  progress: VerticalMergeProgress | null | undefined;
  columnsWritten?: number;
  allColumns?: string[];
  isActive?: boolean;
}> = ({ progress, columnsWritten, allColumns, isActive }) => {
  const totalColumnCount = allColumns?.length ?? 0;
  const showActiveCounter = isActive && totalColumnCount > 0;
  const written = columnsWritten ?? 0;
  const segments = progress?.segments ?? [];
  const totalMs = progress?.total_ms ?? 0;

  // For active merges, compute remaining columns (not yet gathered)
  const remainingColumns = useMemo(() => {
    if (!isActive || !allColumns || allColumns.length === 0) return [];
    const gatheredSet = new Set(segments.filter(s => s.kind === 'gathered').map(s => s.name));
    // Also exclude PK columns (they're in the horizontal stage, not gathered individually)
    return allColumns.filter(c => !gatheredSet.has(c));
  }, [isActive, allColumns, segments]);

  // Generate time axis ticks
  const ticks = useMemo(() => {
    if (totalMs <= 0) return [];
    const count = Math.min(6, Math.max(2, Math.ceil(totalMs / 1000)));
    const step = totalMs / count;
    return Array.from({ length: count + 1 }, (_, i) => i * step);
  }, [totalMs]);

  return (
    <div style={{ marginBottom: 16, borderRadius: 8, border: '1px solid rgba(56,139,253,0.2)', background: 'rgba(56,139,253,0.04)', padding: 12 }}>
      <div style={{ fontSize: 10, marginBottom: 8, color: '#388bfd', fontWeight: 600, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>
          Vertical Merge — Column Timeline
          {showActiveCounter && (
            <span style={{ fontWeight: 400, marginLeft: 8, color: 'var(--text-muted)' }}>
              {segments.filter(s => s.kind === 'gathered').length} / {totalColumnCount} columns
            </span>
          )}
        </span>
        {totalMs > 0 && (
          <span style={{ fontWeight: 400, color: 'var(--text-muted)', fontSize: 9 }}>
            {fmtDuration(totalMs / 1000)} wall time
          </span>
        )}
      </div>

      {/* Active merge progress bar */}
      {showActiveCounter && (
        <div style={{ marginBottom: segments.length > 0 ? 10 : 0 }}>
          <div style={{ height: 6, borderRadius: 3, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
            <div style={{
              height: '100%', borderRadius: 3, background: '#388bfd',
              width: `${Math.min(100, (written / totalColumnCount) * 100).toFixed(1)}%`,
              transition: 'width 0.3s',
            }} />
          </div>
        </div>
      )}

      {/* Gantt chart */}
      {segments.length > 0 && totalMs > 0 && (
        <div>
          {/* Time axis header */}
          <div style={{ display: 'flex', marginLeft: LABEL_WIDTH, marginRight: DURATION_WIDTH, marginBottom: 2 }}>
            <div style={{ flex: 1, position: 'relative', height: 14 }}>
              {ticks.map((t) => (
                <div key={t} style={{
                  position: 'absolute',
                  left: `${(t / totalMs) * 100}%`,
                  fontSize: 8,
                  color: 'var(--text-muted)',
                  fontFamily: 'monospace',
                  transform: 'translateX(-50%)',
                }}>
                  {fmtDuration(t / 1000)}
                </div>
              ))}
            </div>
          </div>

          {/* Segment rows */}
          <div style={{ position: 'relative' }}>
            {/* Vertical grid lines */}
            <div style={{ position: 'absolute', left: LABEL_WIDTH, right: DURATION_WIDTH, top: 0, bottom: 0, pointerEvents: 'none' }}>
              {ticks.map((t) => (
                <div key={t} style={{
                  position: 'absolute',
                  left: `${(t / totalMs) * 100}%`,
                  top: 0, bottom: 0,
                  width: 1,
                  background: 'var(--border-primary)',
                  opacity: 0.4,
                }} />
              ))}
            </div>

            {segments.map((seg, i) => {
              const leftPct = (seg.start_ms / totalMs) * 100;
              const widthPct = Math.max(0.5, ((seg.end_ms - seg.start_ms) / totalMs) * 100);
              const isHorizontal = seg.kind === 'horizontal';
              const color = isHorizontal ? '#f0883e' : '#388bfd';
              const tooltip = `${seg.name}: ${fmtDuration(seg.duration_sec)} · ${seg.rows.toLocaleString()} rows · ${fmtThroughput(seg.bytes, seg.duration_sec)}`;

              return (
                <div
                  key={`${seg.name}-${i}`}
                  title={tooltip}
                  style={{ display: 'flex', alignItems: 'center', height: ROW_HEIGHT }}
                >
                  <div style={{
                    width: LABEL_WIDTH, flexShrink: 0,
                    fontSize: 10, fontFamily: 'monospace',
                    color, fontWeight: isHorizontal ? 600 : 400,
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    paddingRight: 8,
                  }}>
                    {seg.name}
                  </div>
                  <div style={{ flex: 1, position: 'relative', height: 12 }}>
                    <div style={{
                      position: 'absolute',
                      left: `${leftPct}%`,
                      width: `${widthPct}%`,
                      top: 1, bottom: 1,
                      borderRadius: 3,
                      background: color,
                      opacity: 0.85,
                    }} />
                  </div>
                  <div style={{
                    width: DURATION_WIDTH, flexShrink: 0, textAlign: 'right',
                    fontSize: 9, fontFamily: 'monospace', color: 'var(--text-muted)',
                    paddingLeft: 6,
                  }}>
                    {fmtDuration(seg.duration_sec)}
                  </div>
                </div>
              );
            })}

            {/* Remaining columns (active merge only) */}
            {remainingColumns.map((col) => (
              <div key={col} style={{ display: 'flex', alignItems: 'center', height: ROW_HEIGHT }}>
                <div style={{
                  width: LABEL_WIDTH, flexShrink: 0,
                  fontSize: 10, fontFamily: 'monospace',
                  color: 'var(--text-muted)', opacity: 0.5,
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                  paddingRight: 8,
                }}>
                  {col}
                </div>
                <div style={{ flex: 1, position: 'relative', height: 12 }}>
                  <div style={{
                    position: 'absolute', left: 0, right: 0,
                    top: 4, bottom: 4,
                    borderRadius: 2,
                    background: 'var(--border-primary)',
                    opacity: 0.2,
                  }} />
                </div>
                <div style={{ width: DURATION_WIDTH, flexShrink: 0 }} />
              </div>
            ))}
          </div>

        </div>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// DetailsTab — shows merge record details + TTLMove storage info
// ---------------------------------------------------------------------------

const DetailsTab: React.FC<{
  record: MergeHistoryRecord;
  volumeInfo: { volumeName: string; policyName: string } | null;
  isActive?: boolean;
  verticalProgress?: VerticalMergeProgress | null;
  columnsWritten?: number;
  allColumns?: string[];
  onSourcePartClick?: (partName: string) => void;
}> = ({ record, volumeInfo, isActive, verticalProgress, columnsWritten, allColumns, onSourcePartClick }) => {
  const isTTLMove = record.merge_reason === 'TTLMove';
  const isMutation = record.merge_reason === 'Mutation' || record.event_type === 'MutatePart';
  const reasonColor = isMutation ? '#a855f7' : '#f0883e';

  return (
    <>
      {!!record.error && (
        <div style={{ marginBottom: 16, padding: 12, borderRadius: 8, background: 'rgba(229,83,75,0.08)', border: '1px solid rgba(229,83,75,0.25)' }}>
          <div style={{ fontSize: 10, color: '#e5534b', fontWeight: 600, marginBottom: 4 }}>Failed (Error {record.error})</div>
          <code style={{ fontSize: 10, color: '#e5534b', wordBreak: 'break-all', display: 'block', whiteSpace: 'pre-wrap' }}>{record.exception || 'Unknown error'}</code>
        </div>
      )}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Table</div>
        <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>{record.database}.{record.table}</div>
      </div>
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Part Name</div>
        <code style={{ fontSize: 11, padding: '4px 8px', borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-secondary)', display: 'block', wordBreak: 'break-all' }}>{record.part_name}</code>
      </div>
      {(record.merge_reason || record.merge_algorithm || record.part_name.startsWith('patch-')) && (
        <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          {record.merge_reason && (
            <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: `${reasonColor}26`, color: reasonColor, border: `1px solid ${reasonColor}4d` }}>{record.merge_reason}</span>
          )}
          {record.merge_algorithm && record.merge_algorithm !== 'Undecided' && (
            <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}>{record.merge_algorithm}</span>
          )}
          {record.part_name.startsWith('patch-') && (
            <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: 'rgba(63,185,80,0.15)', color: '#3fb950', border: '1px solid rgba(63,185,80,0.3)' }}>Lightweight (Patch)</span>
          )}
        </div>
      )}
      {isTTLMove && (
        <div style={{ marginBottom: 16, borderRadius: 8, border: '1px solid rgba(249,115,22,0.2)', background: 'rgba(249,115,22,0.05)', padding: 12 }}>
          <div style={{ fontSize: 10, marginBottom: 8, color: '#f97316', fontWeight: 600 }}>Storage Move</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
            <div style={{ flex: 1, textAlign: 'center' }}>
              <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 2 }}>Source Disk</div>
              <code style={{ fontSize: 11, padding: '2px 6px', borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }}>default</code>
            </div>
            <div style={{ color: '#f97316', fontSize: 14, fontWeight: 600 }}>→</div>
            <div style={{ flex: 1, textAlign: 'center' }}>
              <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 2 }}>Dest Disk</div>
              <code style={{ fontSize: 11, padding: '2px 6px', borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }}>{record.disk_name || 'unknown'}</code>
            </div>
          </div>
          {volumeInfo && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <div style={{ flex: 1, textAlign: 'center' }}>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 2 }}>Volume</div>
                <code style={{ fontSize: 10, color: 'var(--text-muted)' }}>{volumeInfo.volumeName}</code>
              </div>
              <div style={{ width: 14 }} />
              <div style={{ flex: 1, textAlign: 'center' }}>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 2 }}>Policy</div>
                <code style={{ fontSize: 10, color: 'var(--text-muted)' }}>{volumeInfo.policyName}</code>
              </div>
            </div>
          )}
          {record.path_on_disk && (
            <div style={{ marginTop: 8 }}>
              <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 2 }}>Path</div>
              <code style={{ fontSize: 9, color: 'var(--text-muted)', wordBreak: 'break-all', display: 'block' }}>{record.path_on_disk}</code>
            </div>
          )}
        </div>
      )}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
        {(() => {
          const throughput = record.duration_ms > 0 ? record.size_in_bytes / (record.duration_ms / 1000) : 0;
          const stats: { label: string; value: string; highlight?: string }[] = [
            { label: isActive ? 'Elapsed' : 'Duration', value: `${(record.duration_ms / 1000).toFixed(2)}s` },
            { label: isActive ? 'Rows Written' : 'Rows (output)', value: record.rows.toLocaleString() },
            { label: isActive ? 'Size (compressed)' : 'Final Size', value: formatBytes(record.size_in_bytes) },
            { label: isActive ? 'Memory (current)' : 'Peak Memory', value: formatBytes(record.peak_memory_usage) },
            { label: 'Throughput', value: throughput > 0 ? `${formatBytes(throughput)}/s` : '-' },
          ];
          if (record.read_rows > 0) stats.push({ label: 'Read Rows (input)', value: record.read_rows.toLocaleString() });
          if ((record.rows_diff ?? 0) !== 0) stats.push({ label: 'Rows Diff', value: record.rows_diff.toLocaleString(), highlight: record.rows_diff < 0 ? '#e5534b' : '#3fb950' });
          return stats;
        })().map(({ label, value, highlight }) => (
          <div key={label} style={{ borderRadius: 8, padding: 12, background: 'var(--bg-tertiary)' }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{label}</div>
            <div style={{ fontWeight: 600, color: highlight || 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>{value}</div>
          </div>
        ))}
      </div>
      {!isTTLMove && record.source_part_names && record.source_part_names.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 6, color: 'var(--text-muted)' }}>Source Parts ({record.source_part_names.length})</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {record.source_part_names.map((part, i) => {
              const canDrill = onSourcePartClick && isMergedPart(part);
              return (
                <code
                  key={i}
                  onClick={canDrill ? () => onSourcePartClick(part) : undefined}
                  style={{
                    fontSize: 10, fontFamily: 'monospace', color: 'var(--text-secondary)',
                    padding: '2px 6px', borderRadius: 4, background: 'var(--bg-tertiary)',
                    ...(canDrill ? { cursor: 'pointer', transition: 'background 0.15s' } : {}),
                  }}
                  onMouseEnter={canDrill ? (e) => { (e.target as HTMLElement).style.background = 'var(--bg-hover)'; } : undefined}
                  onMouseLeave={canDrill ? (e) => { (e.target as HTMLElement).style.background = 'var(--bg-tertiary)'; } : undefined}
                  title={canDrill ? `View merge details for ${part}` : undefined}
                >{part}</code>
              );
            })}
          </div>
        </div>
      )}
      {/* Vertical merge column progress */}
      {record.merge_algorithm === 'Vertical' && (verticalProgress || (isActive && allColumns && allColumns.length > 0)) && (
        <VerticalMergeProgressSection
          progress={verticalProgress}
          columnsWritten={columnsWritten}
          allColumns={allColumns}
          isActive={isActive}
        />
      )}
      {record.query_id && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Query ID</div>
          <code style={{ fontSize: 10, padding: '4px 8px', borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-muted)', display: 'block', wordBreak: 'break-all' }}>{record.query_id}</code>
        </div>
      )}
      {!isActive && (
        <div>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Event Time</div>
          <div style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)' }}>{new Date(record.event_time).toLocaleString()}</div>
        </div>
      )}
    </>
  );
};

// ---------------------------------------------------------------------------
// ProfileTab — shows ProfileEvents from part_log
// ---------------------------------------------------------------------------

const ProfileTab: React.FC<{
  profileEvents?: Record<string, number>;
  record: MergeHistoryRecord;
}> = ({ profileEvents, record }) => {
  const [search, setSearch] = useState('');
  const descriptions = useProfileEventDescriptionsStore((s) => s.descriptions);

  if (!profileEvents || Object.keys(profileEvents).length === 0) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 120, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 11 }}>No ProfileEvents available for this event</div>
        </div>
      </div>
    );
  }

  const entries = Object.entries(profileEvents)
    .filter(([k]) => !search || k.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => a[0].localeCompare(b[0]));

  const fmtVal = (key: string, val: number): string => {
    if (key.includes('Bytes') || key.includes('bytes')) return formatBytes(val);
    if (key.includes('Milliseconds') || key.includes('milliseconds')) return `${(val / 1000).toFixed(2)}s`;
    if (key.includes('Microseconds') || key.includes('microseconds')) return `${(val / 1_000_000).toFixed(3)}s`;
    return val.toLocaleString();
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 8, marginBottom: 12 }}>
        {[
          { label: 'Duration', value: `${(record.duration_ms / 1000).toFixed(2)}s` },
          { label: 'Peak Memory', value: formatBytes(record.peak_memory_usage) },
          { label: 'Read Rows', value: record.read_rows.toLocaleString() },
        ].map(s => (
          <div key={s.label} style={{ borderRadius: 6, padding: 8, background: 'var(--bg-tertiary)' }}>
            <div style={{ fontSize: 9, color: 'var(--text-muted)' }}>{s.label}</div>
            <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 12 }}>{s.value}</div>
          </div>
        ))}
      </div>
      <input type="text" placeholder="Filter events..." value={search} onChange={e => setSearch(e.target.value)}
        style={{ width: '100%', padding: '4px 8px', fontSize: 10, marginBottom: 8, background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)', borderRadius: 4, color: 'var(--text-primary)', outline: 'none', boxSizing: 'border-box' }} />
      <div style={{ flex: 1, overflow: 'auto', background: 'var(--bg-tertiary)', borderRadius: 6 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 10 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-primary)' }}>
              <th style={{ padding: '4px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, position: 'sticky', top: 0, background: 'var(--bg-tertiary)' }}>Event</th>
              <th style={{ padding: '4px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, position: 'sticky', top: 0, background: 'var(--bg-tertiary)' }}>Value</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([key, val]) => (
              <tr key={key} style={{ borderBottom: '1px solid var(--border-primary)' }}>
                <td style={{ padding: '3px 8px', fontFamily: 'monospace', color: MERGE_PROFILE_EVENTS.has(key) ? '#f0883e' : 'var(--text-secondary)', fontWeight: MERGE_PROFILE_EVENTS.has(key) ? 600 : 400, cursor: descriptions[key] ? 'help' : undefined }} title={descriptions[key] || undefined}>{key}</td>
                <td style={{ padding: '3px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)' }}>{fmtVal(key, val)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={{ fontSize: 9, color: 'var(--text-muted)', marginTop: 4, textAlign: 'right' }}>{entries.length} events</div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// MergeDetailInner — shared inner component with tabs
// ---------------------------------------------------------------------------

const MergeDetailInner: React.FC<{
  record: MergeHistoryRecord;
  onClose: () => void;
  /** Label for the header, e.g. "Merge Details" or "Mutation Details" */
  title?: string;
  /** True when showing an active (in-progress) merge with synthetic record */
  isActive?: boolean;
  /** Original MergeInfo for active merges — used for progress display */
  activeMerge?: MergeInfo;
}> = ({ record: rootRecord, onClose, title: rootTitle = 'Merge Details', isActive: rootIsActive, activeMerge: rootActiveMerge }) => {
  const services = useClickHouseServices();

  // Drill-down navigation stack for source parts
  const [drillStack, setDrillStack] = useState<{ record: MergeHistoryRecord; title: string }[]>([]);
  const [isDrilling, setIsDrilling] = useState(false);
  const currentDrill = drillStack.length > 0 ? drillStack[drillStack.length - 1] : null;
  const record = currentDrill?.record ?? rootRecord;
  const title = currentDrill?.title ?? rootTitle;
  const isActive = currentDrill ? false : rootIsActive;
  const activeMerge = currentDrill ? undefined : rootActiveMerge;

  const handleSourcePartClick = (partName: string) => {
    if (!services) return;
    setIsDrilling(true);
    services.mergeTracker.getMergeHistoryByPartName(record.database, record.table, partName)
      .then(r => {
        if (r) {
          setDrillStack(prev => [...prev, { record: r, title: `Source Part — ${partName}` }]);
        }
      })
      .finally(() => setIsDrilling(false));
  };

  const handleDrillBack = () => {
    setDrillStack(prev => prev.slice(0, -1));
  };

  // Reset drill stack when root record changes
  const rootKey = `${rootRecord.part_name}:${rootRecord.database}.${rootRecord.table}`;
  const [prevRootKey, setPrevRootKey] = useState(rootKey);
  if (rootKey !== prevRootKey) {
    setPrevRootKey(rootKey);
    setDrillStack([]);
  }

  const [activeTab, setActiveTab] = useState<MergeDetailTab>('details');
  const [volumeInfo, setVolumeInfo] = useState<{ volumeName: string; policyName: string } | null>(null);
  const [textLogs, setTextLogs] = useState<import('@tracehouse/core').MergeTextLog[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [logFilter, setLogFilter] = useState<TraceLogFilter>({});
  const experimentalEnabled = useUserPreferenceStore(s => s.experimentalEnabled);
  const { available: hasMergesHistory } = useCapabilityCheck(['tracehouse_merges_history']);

  // Fetch storage policy volume info for TTLMove
  useEffect(() => {
    setVolumeInfo(null);
    if (record.merge_reason !== 'TTLMove' || !services) return;
    let cancelled = false;
    services.mergeTracker.getStoragePolicyVolumes().then(volumes => {
      if (cancelled) return;
      const diskName = record.disk_name || 'default';
      for (const v of volumes) {
        if (v.disks.includes(diskName)) {
          setVolumeInfo({ volumeName: v.volumeName, policyName: v.policyName });
          return;
        }
      }
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [record.disk_name, record.merge_reason, services]);

  const isVerticalMerge = record.merge_algorithm === 'Vertical' || activeMerge?.merge_algorithm === 'Vertical';

  // Fetch text_log when switching to logs tab, or eagerly for vertical merges (column progress).
  // Only clear logs on tab/record identity changes, not on polling updates (avoids flicker).
  const logRecordKey = `${record.part_name}:${record.database}.${record.table}`;
  const [prevLogKey, setPrevLogKey] = useState(logRecordKey);
  if (logRecordKey !== prevLogKey) {
    setPrevLogKey(logRecordKey);
    setTextLogs([]);
    setLogsError(null);
    setLogFilter({});
    setActiveTab('details');
  }

  useEffect(() => {
    const needLogs = activeTab === 'logs' || (isVerticalMerge && activeTab === 'details');
    if (!services || !needLogs) return;
    let cancelled = false;
    setIsLoadingLogs(true);
    services.mergeTracker.getMergeEventTextLogs({
      query_id: record.query_id,
      event_time: record.event_time,
      duration_ms: record.duration_ms,
      database: record.database,
      table: record.table,
      part_name: record.part_name,
      hostname: record.hostname,
    }).then(logs => {
      if (!cancelled) setTextLogs(logs);
    }).catch(e => {
      if (!cancelled) setLogsError(e instanceof Error ? e.message : 'Failed to fetch logs');
    }).finally(() => {
      if (!cancelled) setIsLoadingLogs(false);
    });
    return () => { cancelled = true; };
  }, [record.event_time, record.part_name, activeTab, services, isVerticalMerge]); // eslint-disable-line react-hooks/exhaustive-deps

  // Vertical merge: parse column progress from text_log
  const verticalProgress = useMemo(
    () => (isVerticalMerge && textLogs.length > 0 ? parseVerticalMergeProgress(textLogs) : null),
    [isVerticalMerge, textLogs],
  );

  // Vertical merge: fetch table column names for active merge (remaining columns)
  const [allColumns, setAllColumns] = useState<string[]>([]);
  useEffect(() => {
    if (!isVerticalMerge || !isActive || !services) return;
    let cancelled = false;
    services.mergeTracker.getTableColumns(record.database, record.table).then(cols => {
      if (!cancelled) setAllColumns(cols);
    });
    return () => { cancelled = true; };
  }, [isVerticalMerge, isActive, record.database, record.table, services]);

  // Auto-refresh text_logs for active vertical merges (every 5s)
  useEffect(() => {
    if (!isActive || !isVerticalMerge || !services || activeTab !== 'details') return;
    const timer = setInterval(() => {
      services.mergeTracker.getMergeEventTextLogs({
        query_id: record.query_id,
        event_time: record.event_time,
        duration_ms: record.duration_ms,
        database: record.database,
        table: record.table,
        part_name: record.part_name,
        hostname: record.hostname,
      }).then(setTextLogs).catch(() => {});
    }, 5000);
    return () => clearInterval(timer);
  }, [isActive, isVerticalMerge, services, activeTab, record.query_id, record.event_time, record.duration_ms, record.database, record.table, record.part_name, record.hostname]);

  // Fetch historical throughput for ETA estimation (active merges only)
  const [throughputEstimate, setThroughputEstimate] = useState<MergeThroughputEstimate | null>(null);
  useEffect(() => {
    if (!isActive || !services || !activeMerge) return;
    let cancelled = false;
    services.mergeTracker.getMergeThroughputEstimate(record.database, record.table).then(estimates => {
      if (cancelled) return;
      setThroughputEstimate(pickThroughputEstimate(estimates, activeMerge.merge_algorithm, activeMerge.total_size_bytes_compressed));
    }).catch(err => {
      console.error('[MergeDetailModal] ETA fetch failed:', err);
    });
    return () => { cancelled = true; };
  }, [isActive, services, record.database, record.table, activeMerge?.merge_algorithm]); // eslint-disable-line react-hooks/exhaustive-deps

  const etaInfo = useMemo(() => {
    if (!isActive || !activeMerge) return null;
    return computeMergeEta(activeMerge.total_size_bytes_compressed, activeMerge.progress, activeMerge.elapsed, throughputEstimate);
  }, [isActive, activeMerge, throughputEstimate]);

  const navigate = useNavigate();

  const hasProfileEvents = !!record.profile_events && Object.keys(record.profile_events).length > 0;
  const isTTLMoveRecord = record.merge_reason === 'TTLMove';
  const isMutationRecord = record.merge_reason === 'Mutation' || record.event_type === 'MutatePart';
  const accent = isMutationRecord ? '#a855f7' : '#f0883e';

  // Reset tab if experimental is disabled while viewing X-Ray
  useEffect(() => {
    if (!experimentalEnabled && activeTab === 'xray') setActiveTab('details');
  }, [experimentalEnabled, activeTab]);

  const tabs: { id: MergeDetailTab; label: string; disabled?: boolean; title?: string; experimental?: boolean }[] = [
    { id: 'details', label: 'Details' },
    { id: 'logs', label: 'Logs', disabled: isTTLMoveRecord, title: isTTLMoveRecord ? 'TTL moves do not produce dedicated log entries' : undefined },
    { id: 'profile', label: 'Profile Events', disabled: !hasProfileEvents, title: !hasProfileEvents ? (isActive ? 'ProfileEvents are available after merge completes' : 'No ProfileEvents in part_log for this event') : undefined },
  ];
  if (experimentalEnabled) {
    tabs.push({
      id: 'xray', label: 'X-Ray', disabled: !hasMergesHistory, experimental: true,
      title: !hasMergesHistory ? 'Requires tracehouse.merges_history — run infra/scripts/setup_sampling.sh' : undefined,
    });
  }

  return (
    <>
      {/* Header */}
      <div style={{ padding: '16px 20px 12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {drillStack.length > 0 && (
              <button onClick={handleDrillBack} style={{
                background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)', borderRadius: 4,
                color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 11, padding: '2px 8px',
                display: 'flex', alignItems: 'center', gap: 4,
              }}>← Back</button>
            )}
            <h3 style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 15, margin: 0 }}>{title}</h3>
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2, fontFamily: 'monospace' }}>
            {record.database}.{record.table} → {record.part_name}
          </div>
          {record.hostname && (
            <div style={{ fontSize: 10, color: 'var(--text-secondary)', marginTop: 2, fontFamily: 'monospace' }}>
              Server: {record.hostname}
            </div>
          )}
        </div>
        <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 18, padding: '4px 8px' }}>✕</button>
      </div>
      {isActive && activeMerge && (
        <div style={{ padding: '8px 20px', background: `${accent}14`, borderBottom: `1px solid ${accent}33`, display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ height: 4, borderRadius: 2, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
              <div style={{ height: '100%', borderRadius: 2, background: accent, width: `${(activeMerge.progress * 100).toFixed(1)}%`, transition: 'width 0.3s' }} />
            </div>
          </div>
          <span style={{ fontSize: 11, fontWeight: 600, color: accent, fontFamily: 'monospace', whiteSpace: 'nowrap' }}>{(activeMerge.progress * 100).toFixed(1)}%</span>
          {etaInfo ? (
            <span
              title={`Blended throughput: ${formatBytes(etaInfo.medianThroughput)}/s · based on ${etaInfo.basedOnCount} past merges`}
              style={{ fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}
            >
              ETA <span style={{ fontWeight: 500, color: 'var(--text-secondary)' }}>~{formatDuration(etaInfo.remainingSec)}</span>
              {' · '}based on {etaInfo.basedOnCount} {etaInfo.sizeMatched ? 'similarly sized ' : ''}merges
            </span>
          ) : (
            <span style={{ fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>In progress</span>
          )}
          {etaInfo && (
            <button
              onClick={() => {
                const db = record.database.replace(/'/g, "\\'");
                const tbl = record.table.replace(/'/g, "\\'");
                const sql = [
                  `-- Merge performance analysis for ${db}.${tbl}`,
                  `-- Throughput stats by size bucket and algorithm`,
                  `SELECT`,
                  `    merge_algorithm,`,
                  `    multiIf(`,
                  `        size_in_bytes < 10 * 1024 * 1024, '< 10 MB',`,
                  `        size_in_bytes < 100 * 1024 * 1024, '10–100 MB',`,
                  `        size_in_bytes < 1024 * 1024 * 1024, '100 MB–1 GB',`,
                  `        size_in_bytes < 5 * 1024 * 1024 * 1024, '1–5 GB',`,
                  `        size_in_bytes < 10 * 1024 * 1024 * 1024, '5–10 GB',`,
                  `        size_in_bytes < 20 * 1024 * 1024 * 1024, '10–20 GB',`,
                  `        size_in_bytes < 50 * 1024 * 1024 * 1024, '20–50 GB',`,
                  `        '>= 50 GB'`,
                  `    ) AS size_bucket,`,
                  `    count() AS merges,`,
                  `    formatReadableSize(avg(size_in_bytes)) AS avg_size,`,
                  `    formatReadableTimeDelta(avg(duration_ms) / 1000) AS avg_duration,`,
                  `    formatReadableTimeDelta(quantile(0.5)(duration_ms) / 1000) AS p50_duration,`,
                  `    formatReadableTimeDelta(quantile(0.95)(duration_ms) / 1000) AS p95_duration,`,
                  `    formatReadableTimeDelta(max(duration_ms) / 1000) AS max_duration,`,
                  `    formatReadableSize(avg(size_in_bytes / (duration_ms / 1000))) AS avg_throughput,`,
                  `    formatReadableSize(quantile(0.5)(size_in_bytes / (duration_ms / 1000))) AS p50_throughput,`,
                  `    formatReadableSize(quantile(0.95)(size_in_bytes / (duration_ms / 1000))) AS p95_throughput,`,
                  `    formatReadableSize(avg(peak_memory_usage)) AS avg_peak_memory,`,
                  `    round(avg(length(merged_from)), 1) AS avg_source_parts`,
                  `FROM {{cluster_aware:system.part_log}}`,
                  `WHERE database = '${db}'`,
                  `    AND table = '${tbl}'`,
                  `    AND event_type = 'MergeParts'`,
                  `    AND duration_ms > 100`,
                  `    AND size_in_bytes > 0`,
                  `GROUP BY merge_algorithm, size_bucket`,
                  `ORDER BY merge_algorithm, avg(size_in_bytes)`,
                ].join('\n');
                const params = new URLSearchParams({ tab: 'misc', sql: encodeSql(sql), from: 'merges' });
                navigate(`/analytics?${params.toString()}`);
              }}
              title="Open full merge throughput analysis in Analytics"
              style={{
                background: 'none', border: 'none', cursor: 'pointer',
                fontSize: 9, color: accent, opacity: 0.7, padding: 0,
              }}
            >
              Analyze →
            </button>
          )}
        </div>
      )}

      {/* Tabs */}
      <div style={{ display: 'flex', borderBottom: '1px solid var(--border-primary)', padding: '0 20px' }}>
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => !tab.disabled && setActiveTab(tab.id)}
            title={tab.title}
            style={{
              padding: '10px 14px', fontSize: 12, position: 'relative',
              fontWeight: activeTab === tab.id ? 600 : 400,
              color: tab.disabled ? 'var(--text-muted)' : activeTab === tab.id ? accent : 'var(--text-secondary)',
              background: 'none', border: 'none',
              borderBottom: activeTab === tab.id ? `2px solid ${accent}` : '2px solid transparent',
              cursor: tab.disabled ? 'not-allowed' : 'pointer',
              opacity: tab.disabled ? 0.5 : 1,
            }}
          >
            {tab.label}
            {tab.experimental && (
              <span style={{
                position: 'absolute', top: -4, right: -2,
                fontSize: 7, fontWeight: 700, color: accent,
                background: 'var(--bg-tertiary)', border: `1px solid ${accent}4d`,
                borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                textTransform: 'uppercase', letterSpacing: '0.3px',
              }}>exp</span>
            )}
          </button>
        ))}
      </div>

      {/* Content */}
      <div style={{ flex: 1, overflow: 'auto', padding: 20 }}>
        {isDrilling && (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)', fontSize: 11 }}>Loading source part…</div>
        )}
        {activeTab === 'details' && <DetailsTab record={record} volumeInfo={volumeInfo} isActive={isActive} verticalProgress={verticalProgress} columnsWritten={activeMerge?.columns_written} allColumns={allColumns} onSourcePartClick={handleSourcePartClick} />}
        {activeTab === 'logs' && (
          <div style={{ height: '100%', margin: -20 }}>
            {record.exception && record.exception.length > 0 && (
              <div style={{ margin: '12px 12px 0', padding: 10, borderRadius: 6, background: 'rgba(229,83,75,0.1)', border: '1px solid rgba(229,83,75,0.3)' }}>
                <div style={{ fontSize: 10, color: '#e5534b', fontWeight: 600, marginBottom: 4 }}>Exception</div>
                <code style={{ fontSize: 10, color: '#e5534b', wordBreak: 'break-all', display: 'block', whiteSpace: 'pre-wrap' }}>{record.exception}</code>
              </div>
            )}
            <TraceLogViewer
              logs={textLogs}
              isLoading={isLoadingLogs}
              error={logsError}
              filter={logFilter}
              onFilterChange={(f) => setLogFilter(prev => ({ ...prev, ...f }))}
            />
          </div>
        )}
        {activeTab === 'profile' && <ProfileTab profileEvents={record.profile_events} record={record} />}
        {activeTab === 'xray' && (
          <MergeXRay
            database={record.database}
            table={record.table}
            resultPartName={record.part_name}
            eventTime={record.event_time}
            durationMs={record.duration_ms}
            queryId={record.query_id}
            hostname={record.hostname}
          />
        )}
      </div>
    </>
  );
};

// ---------------------------------------------------------------------------
// MergeDetailModalFromRecord — opens from MergeTracker with a record directly
// ---------------------------------------------------------------------------

export const MergeDetailModalFromRecord: React.FC<MergeDetailModalFromRecordProps> = ({ record, onClose }) => {
  if (!record) return null;
  return (
    <ModalWrapper isOpen={true} onClose={onClose}>
      <MergeDetailInner record={record} onClose={onClose} title="Merge Details" />
    </ModalWrapper>
  );
};

// ---------------------------------------------------------------------------
// MergeDetailModal — opens from TimeTravelPage with a MergeSeries
// Fetches MergeHistoryRecord by part name, then delegates to MergeDetailInner
// ---------------------------------------------------------------------------

export const MergeDetailModal: React.FC<MergeDetailModalProps> = ({ merge, onClose, isMutation }) => {
  const services = useClickHouseServices();
  const [record, setRecord] = useState<MergeHistoryRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const kind = isMutation ? 'mutation' : 'merge';

  const syntheticRecord = useMemo(
    () => (merge?.is_running ? seriesToPartialRecord(merge, !!isMutation) : null),
    [merge?.part_name, merge?.table, merge?.is_running, isMutation],  // eslint-disable-line react-hooks/exhaustive-deps
  );

  useEffect(() => {
    setRecord(null);
    setError(null);
    if (!merge || !services) return;
    let cancelled = false;
    setLoading(true);
    // merge.table is "database.table" format
    const dotIdx = merge.table.indexOf('.');
    const db = dotIdx > 0 ? merge.table.slice(0, dotIdx) : 'default';
    const tbl = dotIdx > 0 ? merge.table.slice(dotIdx + 1) : merge.table;
    services.mergeTracker.getMergeHistoryByPartName(db, tbl, merge.part_name)
      .then(r => { if (!cancelled) setRecord(r); })
      .catch(e => { if (!cancelled) setError(e instanceof Error ? e.message : `Failed to fetch ${kind} details`); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [merge?.part_name, merge?.table, services, kind]);

  if (!merge) return null;

  const titleBase = isMutation ? 'Mutation' : 'Merge';

  return (
    <ModalWrapper isOpen={true} onClose={onClose}>
      {loading && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Loading {kind} details…</div>
            <div style={{ fontSize: 11 }}>{merge.table} → {merge.part_name}</div>
          </div>
        </div>
      )}
      {error && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#e5534b' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Failed to load {kind} details</div>
            <div style={{ fontSize: 11 }}>{error}</div>
          </div>
        </div>
      )}
      {!loading && !error && !record && !syntheticRecord && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>No part_log entry found</div>
            <div style={{ fontSize: 11 }}>{merge.table} → {merge.part_name}</div>
          </div>
        </div>
      )}
      {!loading && !error && !record && syntheticRecord && (
        <MergeDetailInner record={syntheticRecord} onClose={onClose} title={`Active ${titleBase} — Details`} isActive />
      )}
      {record && <MergeDetailInner record={record} onClose={onClose} title={`${titleBase} Details`} />}
    </ModalWrapper>
  );
};

export const MutationDetailModal: React.FC<{ mutation: MutationSeries | null; onClose: () => void }> = ({ mutation, onClose }) => (
  <MergeDetailModal merge={mutation} onClose={onClose} isMutation />
);

// ---------------------------------------------------------------------------
// ActiveMergeDetailModal — Open from MergeTracker active merge panel
// Takes a MergeInfo (active merge) and fetches the part_log record by part name.
// ---------------------------------------------------------------------------

export interface ActiveMergeDetailModalProps {
  merge: MergeInfo | null;
  onClose: () => void;
}

export const ActiveMergeDetailModal: React.FC<ActiveMergeDetailModalProps> = ({ merge, onClose }) => {
  const services = useClickHouseServices();
  // Live merge state — polls for updates while the merge is active
  const [liveMerge, setLiveMerge] = useState<MergeInfo | null>(merge);
  // Once the merge completes (disappears from system.merges), fetch part_log record
  const [completedRecord, setCompletedRecord] = useState<MergeHistoryRecord | null>(null);

  // Reset when the prop merge identity changes
  useEffect(() => {
    setLiveMerge(merge);
    setCompletedRecord(null);
  }, [merge?.database, merge?.table, merge?.result_part_name]); // eslint-disable-line react-hooks/exhaustive-deps

  // Poll active merges every 5s to refresh stats
  useEffect(() => {
    if (!liveMerge || !services || completedRecord) return;
    const poll = () => {
      services.mergeTracker.getActiveMerges(liveMerge.database, liveMerge.table).then(merges => {
        const found = merges.find(m => m.result_part_name === liveMerge.result_part_name);
        if (found) {
          setLiveMerge(found);
        } else {
          // Merge completed — fetch part_log record
          services.mergeTracker.getMergeHistoryByPartName(
            liveMerge.database, liveMerge.table, liveMerge.result_part_name,
          ).then(r => {
            if (r) setCompletedRecord(r);
          });
        }
      }).catch(() => {});
    };
    const timer = setInterval(poll, 5000);
    return () => clearInterval(timer);
  }, [liveMerge?.database, liveMerge?.table, liveMerge?.result_part_name, services, completedRecord]); // eslint-disable-line react-hooks/exhaustive-deps

  // Stable event_time — captured once when the modal opens, so text_log effects don't re-trigger
  const [stableEventTime] = useState(() => new Date().toISOString());
  const syntheticRecord = useMemo(() => {
    if (!liveMerge) return null;
    const rec = mergeInfoToPartialRecord(liveMerge);
    rec.event_time = stableEventTime;
    return rec;
  }, [liveMerge, stableEventTime]);

  if (!merge) return null;

  // Once completed, show the part_log record (has ProfileEvents, etc.)
  if (completedRecord) {
    return (
      <ModalWrapper isOpen={true} onClose={onClose}>
        <MergeDetailInner record={completedRecord} onClose={onClose} title="Merge Details (completed)" />
      </ModalWrapper>
    );
  }

  if (!syntheticRecord || !liveMerge) return null;

  return (
    <ModalWrapper isOpen={true} onClose={onClose}>
      <MergeDetailInner record={syntheticRecord} onClose={onClose} title="Active Merge — Full Details" isActive activeMerge={liveMerge} />
    </ModalWrapper>
  );
};
