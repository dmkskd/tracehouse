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
import type { MergeHistoryRecord, MergeInfo } from '@tracehouse/core';
import type { MergeSeries, MutationSeries } from '@tracehouse/core';
import { ModalWrapper } from '../shared/ModalWrapper';
import { formatBytes } from '../../stores/databaseStore';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { TraceLogViewer } from '../tracing/TraceLogViewer';
import type { TraceLogFilter } from '../../stores/traceStore';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { MergeXRay } from './MergeXRay';

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

/** Open from TimeTravelPage — pass a MergeSeries, record is fetched */
export interface MergeDetailModalProps {
  merge: MergeSeries | null;
  onClose: () => void;
}

/** Open from TimeTravelPage — pass a MutationSeries, record is fetched */
export interface MutationDetailModalProps {
  mutation: MutationSeries | null;
  onClose: () => void;
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
    merge_reason: merge.is_mutation ? 'Mutation' : '',
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
// DetailsTab — shows merge record details + TTLMove storage info
// ---------------------------------------------------------------------------

const DetailsTab: React.FC<{
  record: MergeHistoryRecord;
  volumeInfo: { volumeName: string; policyName: string } | null;
  isActive?: boolean;
}> = ({ record, volumeInfo, isActive }) => {
  const isTTLMove = record.merge_reason === 'TTLMove';

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
            <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: 'rgba(240,136,62,0.15)', color: '#f0883e', border: '1px solid rgba(240,136,62,0.3)' }}>{record.merge_reason}</span>
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
          <div style={{ fontSize: 10, marginBottom: 8, color: 'var(--text-muted)' }}>Source Parts ({record.source_part_names.length})</div>
          <div style={{ maxHeight: 120, overflow: 'auto', background: 'var(--bg-tertiary)', borderRadius: 6, padding: 8 }}>
            {record.source_part_names.map((part, i) => (
              <div key={i} style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-secondary)', padding: '2px 0' }}>{part}</div>
            ))}
          </div>
        </div>
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
                <td style={{ padding: '3px 8px', fontFamily: 'monospace', color: MERGE_PROFILE_EVENTS.has(key) ? '#f0883e' : 'var(--text-secondary)', fontWeight: MERGE_PROFILE_EVENTS.has(key) ? 600 : 400 }}>{key}</td>
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
}> = ({ record, onClose, title = 'Merge Details', isActive, activeMerge }) => {
  const services = useClickHouseServices();
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

  // Lazy-load text_log when switching to logs tab
  useEffect(() => {
    setTextLogs([]);
    setLogsError(null);
    setLogFilter({});
    if (!services || activeTab !== 'logs') return;
    let cancelled = false;
    setIsLoadingLogs(true);
    services.mergeTracker.getMergeEventTextLogs({
      query_id: record.query_id,
      event_time: record.event_time,
      duration_ms: record.duration_ms,
      database: record.database,
      table: record.table,
      part_name: record.part_name,
    }).then(logs => {
      if (!cancelled) setTextLogs(logs);
    }).catch(e => {
      if (!cancelled) setLogsError(e instanceof Error ? e.message : 'Failed to fetch logs');
    }).finally(() => {
      if (!cancelled) setIsLoadingLogs(false);
    });
    return () => { cancelled = true; };
  }, [record.event_time, record.part_name, activeTab, services]);

  const hasProfileEvents = !!record.profile_events && Object.keys(record.profile_events).length > 0;
  const isTTLMoveRecord = record.merge_reason === 'TTLMove';

  // Reset tab if experimental is disabled while viewing X-Ray
  useEffect(() => {
    if (!experimentalEnabled && activeTab === 'xray') setActiveTab('details');
  }, [experimentalEnabled, activeTab]);

  const tabs: { id: MergeDetailTab; label: string; disabled?: boolean; title?: string; experimental?: boolean }[] = [
    { id: 'details', label: 'Details' },
    { id: 'logs', label: 'Logs', disabled: isTTLMoveRecord, title: isTTLMoveRecord ? 'TTL moves do not produce dedicated log entries' : undefined },
    { id: 'profile', label: 'Profile', disabled: !hasProfileEvents, title: !hasProfileEvents ? (isActive ? 'ProfileEvents are available after merge completes' : 'No ProfileEvents in part_log for this event') : undefined },
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
          <h3 style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 15, margin: 0 }}>{title}</h3>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2, fontFamily: 'monospace' }}>
            {record.database}.{record.table} → {record.part_name}
          </div>
        </div>
        <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 18, padding: '4px 8px' }}>✕</button>
      </div>
      {isActive && activeMerge && (
        <div style={{ padding: '8px 20px', background: 'rgba(240,136,62,0.08)', borderBottom: '1px solid rgba(240,136,62,0.2)', display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ height: 4, borderRadius: 2, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
              <div style={{ height: '100%', borderRadius: 2, background: '#f0883e', width: `${(activeMerge.progress * 100).toFixed(1)}%`, transition: 'width 0.3s' }} />
            </div>
          </div>
          <span style={{ fontSize: 11, fontWeight: 600, color: '#f0883e', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>{(activeMerge.progress * 100).toFixed(1)}%</span>
          <span style={{ fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>In progress — some data may be partial</span>
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
              color: tab.disabled ? 'var(--text-muted)' : activeTab === tab.id ? '#f0883e' : 'var(--text-secondary)',
              background: 'none', border: 'none',
              borderBottom: activeTab === tab.id ? '2px solid #f0883e' : '2px solid transparent',
              cursor: tab.disabled ? 'not-allowed' : 'pointer',
              opacity: tab.disabled ? 0.5 : 1,
            }}
          >
            {tab.label}
            {tab.experimental && (
              <span style={{
                position: 'absolute', top: -4, right: -2,
                fontSize: 7, fontWeight: 700, color: '#f0883e',
                background: 'var(--bg-tertiary)', border: '1px solid rgba(240,136,62,0.3)',
                borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                textTransform: 'uppercase', letterSpacing: '0.3px',
              }}>exp</span>
            )}
          </button>
        ))}
      </div>

      {/* Content */}
      <div style={{ flex: 1, overflow: 'auto', padding: 20 }}>
        {activeTab === 'details' && <DetailsTab record={record} volumeInfo={volumeInfo} isActive={isActive} />}
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

export const MergeDetailModal: React.FC<MergeDetailModalProps> = ({ merge, onClose }) => {
  const services = useClickHouseServices();
  const [record, setRecord] = useState<MergeHistoryRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const syntheticRecord = useMemo(
    () => (merge?.is_running ? seriesToPartialRecord(merge, false) : null),
    [merge?.part_name, merge?.table, merge?.is_running],  // eslint-disable-line react-hooks/exhaustive-deps
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
      .catch(e => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to fetch merge details'); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [merge?.part_name, merge?.table, services]);

  if (!merge) return null;

  return (
    <ModalWrapper isOpen={true} onClose={onClose}>
      {loading && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Loading merge details…</div>
            <div style={{ fontSize: 11 }}>{merge.table} → {merge.part_name}</div>
          </div>
        </div>
      )}
      {error && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#e5534b' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Failed to load merge details</div>
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
        <MergeDetailInner record={syntheticRecord} onClose={onClose} title="Active Merge — Details" isActive />
      )}
      {record && <MergeDetailInner record={record} onClose={onClose} title="Merge Details" />}
    </ModalWrapper>
  );
};

// ---------------------------------------------------------------------------
// MutationDetailModal — opens from TimeTravelPage with a MutationSeries
// Same as MergeDetailModal but with "Mutation Details" title
// ---------------------------------------------------------------------------

export const MutationDetailModal: React.FC<MutationDetailModalProps> = ({ mutation, onClose }) => {
  const services = useClickHouseServices();
  const [record, setRecord] = useState<MergeHistoryRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const syntheticRecord = useMemo(
    () => (mutation?.is_running ? seriesToPartialRecord(mutation, true) : null),
    [mutation?.part_name, mutation?.table, mutation?.is_running],  // eslint-disable-line react-hooks/exhaustive-deps
  );

  useEffect(() => {
    setRecord(null);
    setError(null);
    if (!mutation || !services) return;
    let cancelled = false;
    setLoading(true);
    const dotIdx = mutation.table.indexOf('.');
    const db = dotIdx > 0 ? mutation.table.slice(0, dotIdx) : 'default';
    const tbl = dotIdx > 0 ? mutation.table.slice(dotIdx + 1) : mutation.table;
    services.mergeTracker.getMergeHistoryByPartName(db, tbl, mutation.part_name)
      .then(r => { if (!cancelled) setRecord(r); })
      .catch(e => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to fetch mutation details'); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [mutation?.part_name, mutation?.table, services]);

  if (!mutation) return null;

  return (
    <ModalWrapper isOpen={true} onClose={onClose}>
      {loading && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Loading mutation details…</div>
            <div style={{ fontSize: 11 }}>{mutation.table} → {mutation.part_name}</div>
          </div>
        </div>
      )}
      {error && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#e5534b' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Failed to load mutation details</div>
            <div style={{ fontSize: 11 }}>{error}</div>
          </div>
        </div>
      )}
      {!loading && !error && !record && !syntheticRecord && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>No part_log entry found</div>
            <div style={{ fontSize: 11 }}>{mutation.table} → {mutation.part_name}</div>
          </div>
        </div>
      )}
      {!loading && !error && !record && syntheticRecord && (
        <MergeDetailInner record={syntheticRecord} onClose={onClose} title="Active Mutation — Details" isActive />
      )}
      {record && <MergeDetailInner record={record} onClose={onClose} title="Mutation Details" />}
    </ModalWrapper>
  );
};

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
  const [record, setRecord] = useState<MergeHistoryRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Memoize synthetic record so it doesn't recreate on every render (avoids cascading re-fetches in X-Ray)
  const syntheticRecord = useMemo(
    () => merge ? mergeInfoToPartialRecord(merge) : null,
    [merge?.database, merge?.table, merge?.result_part_name],  // eslint-disable-line react-hooks/exhaustive-deps
  );

  useEffect(() => {
    setRecord(null);
    setError(null);
    if (!merge || !services) return;
    let cancelled = false;
    setLoading(true);
    services.mergeTracker.getMergeHistoryByPartName(merge.database, merge.table, merge.result_part_name)
      .then(r => { if (!cancelled) setRecord(r); })
      .catch(e => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to fetch merge details'); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [merge?.result_part_name, merge?.database, merge?.table, services]);

  if (!merge) return null;

  return (
    <ModalWrapper isOpen={true} onClose={onClose}>
      {loading && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Loading merge details…</div>
            <div style={{ fontSize: 11 }}>{merge.database}.{merge.table} → {merge.result_part_name}</div>
          </div>
        </div>
      )}
      {error && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#e5534b' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: 13, marginBottom: 4 }}>Failed to load merge details</div>
            <div style={{ fontSize: 11 }}>{error}</div>
          </div>
        </div>
      )}
      {!loading && !error && !record && syntheticRecord && (
        <MergeDetailInner record={syntheticRecord} onClose={onClose} title="Active Merge — Full Details" isActive activeMerge={merge!} />
      )}
      {record && <MergeDetailInner record={record} onClose={onClose} title="Active Merge — Full Details" />}
    </ModalWrapper>
  );
};
