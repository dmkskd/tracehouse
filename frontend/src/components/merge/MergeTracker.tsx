/**
 * MergeTracker - Dark theme merge tracking view
 */

import React, { useEffect, useCallback, useState, useRef, useMemo } from 'react';
import { useConnectionStore } from '../../stores/connectionStore';
import {
  useMergeStore,
  mergeApi,
  formatBytes,
  formatBytesPerSec,
} from '../../stores/mergeStore';
import type { MergeInfo, MutationInfo, MutationDependencyInfo } from '../../stores/mergeStore';
import type { MergeThroughputEstimate, ThroughputMap } from '@tracehouse/core';
import { formatDuration } from '../../utils/formatters';
import { useDatabaseStore, databaseApi } from '../../stores/databaseStore';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../../stores/refreshSettingsStore';
import { MergeActivityTable } from './MergeActivityTable';
import { MergeFilterBar } from './MergeFilterBar';
import { buildErrorCodeSuggestions } from '../common/errorCodeFilterModel';
import type { MergeQuickFilter, MergeTab } from './MergeFilterBar';
import {
  createMergeActivityState,
  buildMergeActivityRecords,
  filterMergeActivity,
  isMergeStuck,
  limitMergeActivityRecords,
  mergeActivityHosts,
  mergeActivityStatuses,
  reconcileMergeActivity,
} from './merge-activity-model';
import { MutationDependencyDiagram } from '../tracing/MutationDependencyDiagram';
import { MergeDependencyDiagram } from './MergeDependencyDiagram';
import type { MutationHistoryRecord, MergeHistoryRecord } from '../../stores/mergeStore';
import { CopyTableButton } from '../common/CopyTableButton';
import { BackLink } from '../common/BackLink';
import { DocsLink } from '../common/DocsLink';
import {
  MetricStrip,
  MetricStripDivider,
  MetricStripItem,
} from '../common/MetricStrip';
import { MergeDetailModalFromRecord, ActiveMergeDetailModal } from './MergeDetailModal';
import {
  buildPartToMergeMap,
  getMergeForMutation,
  groupMutationsByMerge,
  computeMutationDependency,
} from '../../helpers/mutationDependencyHelpers';
import { PermissionGate } from '../shared/PermissionGate';
import { extractErrorMessage } from '../../utils/errorFormatters';
import { PreviewToggleButton } from '../common/PreviewToggleButton';
import {
  loadPreviewPreference,
  MERGE_ACTIVITY_PREVIEW_STORAGE_KEY,
  savePreviewPreference,
} from '../../utils/previewPreference';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { classifyActiveMerge, getMergeCategoryInfo, classifyMutationCommand, MUTATION_SUBTYPES, computeMergeEta, pickThroughputEstimate, ALL_MERGE_CATEGORIES, isCategoryClientSideOnly } from '@tracehouse/core';
import type { MergeCategory } from '@tracehouse/core';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { MergeHealthSunburst } from './MergeHealthSunburst';
import { useUrlState } from '../../hooks/useUrlState';
import type { UrlSchema } from '../../hooks/useUrlState';

// URL schema for shareable merge tracker links
const mergeUrlSchema = {
  tab:       { type: 'string',  default: 'merges' },
  database:  { type: 'string[]' },
  table:     { type: 'string[]' },
  category:  { type: 'string[]' },
  timeRange: { type: 'string',  default: '1 HOUR' },
  minDurMs:  { type: 'number' },
  minSizeB:  { type: 'number' },
  limit:     { type: 'number',  default: 100 },
  excludeSys: { type: 'boolean', default: true },
  sortField: { type: 'string',  default: 'event_time' },
  sortDir:   { type: 'string',  default: 'desc' },
  host:      { type: 'string[]' },
  status:    { type: 'string[]' },
  errorCode: { type: 'string[]' },
  quick:     { type: 'string' },
  mergeType: { type: 'string' },
  part:      { type: 'string' },
  // Merge detail deep-link: db, table, part_name to reopen modal
  md_db:     { type: 'string' },
  md_tbl:    { type: 'string' },
  md_part:   { type: 'string' },
} as const satisfies UrlSchema;

interface PoolUsage {
  label: string;
  shortLabel: string;
  active: number;
  total: number;
  color: string;
}

const PoolUsageSummary: React.FC<{
  pools: PoolUsage[];
  totalActive: number;
  total: number;
  isLoading: boolean;
}> = ({ pools, totalActive, total, isLoading }) => (
  <div
    aria-label={isLoading
      ? 'Background pool usage loading'
      : `Background pools: ${pools.map(pool => `${pool.label} ${pool.active} of ${pool.total}`).join(', ')}`}
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      flexWrap: 'nowrap',
      gap: 10,
      flexShrink: 0,
    }}
  >
    <span style={{ display: 'inline-flex', alignItems: 'baseline', gap: 6, whiteSpace: 'nowrap' }}>
      <span style={{
        color: 'var(--text-primary)',
        fontFamily: 'var(--font-mono, monospace)',
        fontSize: 13,
        fontWeight: 600,
      }}>
        {isLoading ? '— / —' : `${totalActive} / ${total}`}
      </span>
      <span style={{
        color: 'var(--text-muted)',
        fontSize: 10,
        letterSpacing: '0.04em',
        textTransform: 'uppercase',
      }}>
        pools
      </span>
    </span>

    <span style={{ display: 'inline-flex', alignItems: 'center', flexWrap: 'nowrap', gap: 6 }}>
      {(isLoading ? [] : pools).map(pool => {
        const utilization = pool.total > 0 ? (pool.active / pool.total) * 100 : 0;
        const pressureColor = utilization >= 90
          ? '#ef4444'
          : utilization >= 70
            ? '#f59e0b'
            : pool.color;

        return (
          <span
            key={pool.label}
            title={`${pool.label}: ${pool.active} / ${pool.total} threads (${utilization.toFixed(1)}%)`}
            style={{ display: 'inline-flex', flexDirection: 'column', gap: 3, width: 36 }}
          >
            <span style={{
              display: 'flex',
              alignItems: 'baseline',
              justifyContent: 'center',
              whiteSpace: 'nowrap',
            }}>
              <span style={{
                color: 'var(--text-muted)',
                fontSize: 8,
                fontWeight: 600,
                letterSpacing: '0.03em',
                textTransform: 'uppercase',
                marginRight: 3,
              }}>
                {pool.shortLabel}
              </span>
              <span style={{
                color: pressureColor,
                fontFamily: 'var(--font-mono, monospace)',
                fontSize: 8,
                fontWeight: 600,
              }}>
                {pool.active}/{pool.total}
              </span>
            </span>
            <span style={{
              height: 3,
              overflow: 'hidden',
              borderRadius: 2,
              background: 'var(--bg-tertiary)',
            }}>
              <span style={{
                display: 'block',
                width: `${Math.min(utilization, 100)}%`,
                minWidth: pool.active > 0 ? 1 : 0,
                height: '100%',
                borderRadius: 2,
                background: pressureColor,
              }} />
            </span>
          </span>
        );
      })}
      {isLoading && (
        <span style={{ color: 'var(--text-muted)', fontSize: 10 }}>
          Loading pool breakdown…
        </span>
      )}
    </span>
  </div>
);

// Mutations Panel - Shows running and queued mutations in table format
// Color palette for mutation indicators (matching Time Travel style)
const MUTATION_COLORS = ['#f778ba', '#ff9ff3', '#f368e0', '#e056fd', '#be2edd', '#8854d0'];

const MutationsPanel: React.FC<{
  mutations: MutationInfo[];
  activeMerges: MergeInfo[];
  isLoading: boolean;
  selectedMutation: MutationInfo | null;
  onSelectMutation: (mutation: MutationInfo) => void;
  onPreviewMutation?: (mutation: MutationInfo) => void;
}> = ({ mutations, activeMerges, isLoading, selectedMutation, onSelectMutation, onPreviewMutation }) => {
  const [diagramMutation, setDiagramMutation] = useState<MutationInfo | null>(null);
  const [diagramMerge, setDiagramMerge] = useState<{ merge: MergeInfo; mutations: MutationInfo[] } | null>(null);

  // Compute dependency info for the diagram mutation on the fly
  const diagramDependency = React.useMemo<MutationDependencyInfo | null>(() => {
    if (!diagramMutation) return null;
    return computeMutationDependency(diagramMutation, activeMerges, mutations);
  }, [diagramMutation, activeMerges, mutations]);

  if (mutations.length === 0 && isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 16, height: 16, border: '2px solid var(--border-primary)', borderTopColor: 'var(--accent-primary)', borderRadius: '50%', animation: 'spin 1s linear infinite', marginBottom: 4 }} />
          <p style={{ fontSize: 11 }}>Loading...</p>
        </div>
      </div>
    );
  }

  if (mutations.length === 0) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 13, marginBottom: 2, fontWeight: 300 }}>OK</div>
          <div style={{ fontSize: 10 }}>No pending mutations</div>
        </div>
      </div>
    );
  }

  // Build a lookup: part name -> active merge (for linking mutations to merges)
  const partToMerge = buildPartToMergeMap(activeMerges);

  // Group mutations by their blocking merge to build the summary banner
  const { mergeGroups } = groupMutationsByMerge(mutations, partToMerge);

  const formatElapsed = (createTime: string): string => {
    const seconds = Math.floor((Date.now() - new Date(createTime).getTime()) / 1000);
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    const hours = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${mins}m`;
  };

  // Count mutation-type merges (is_mutation=true) — these are the merges applying mutations to parts
  const mutationMerges = activeMerges.filter(m => m.is_mutation);
  const linkedMergeKeys = new Set(Array.from(mergeGroups.keys()));
  const unlinkedMerges = mutationMerges.filter(m => !linkedMergeKeys.has(m.result_part_name));

  // Parse data_version from result part name
  const getDataVersion = (resultPart: string) => {
    const segments = resultPart.split('_');
    return segments.length >= 5 ? segments[segments.length - 1] : null;
  };

  // Sort merge groups: most mutations first
  const sortedGroups = Array.from(mergeGroups.values()).sort((a, b) => b.count - a.count);

  // Compute overall stats
  const totalMutationMerges = mutationMerges.length;
  const killedStateUnavailable = mutations.some(
    mutation => mutation.is_killed_supported === false,
  );

  return (
    <div style={{ overflow: 'auto' }}>
      <style>{`
        .mutation-row:hover { background: var(--bg-hover) !important; }
      `}</style>

      {killedStateUnavailable && (
        <div style={{
          marginBottom: 10, padding: '7px 10px', borderRadius: 4,
          border: '1px solid rgba(245,158,11,0.35)',
          background: 'rgba(245,158,11,0.08)', color: '#d97706', fontSize: 10,
        }}>
          Compatibility: this ClickHouse version does not expose mutation
          killed-state. Active state is shown, but killed mutations cannot be
          identified separately.
        </div>
      )}

      {/* Consolidated merge activity summary */}
      {totalMutationMerges > 0 && (
        <div style={{
          background: 'var(--bg-tertiary)', borderRadius: 6, padding: 12, marginBottom: 12,
          border: '1px solid var(--border-primary)',
        }}>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 8 }}>
            Merge Activity
          </div>

          {/* Per-part merge rows */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {sortedGroups.map(({ merge: grpMerge, count }) => {
              const pct = (grpMerge.progress * 100).toFixed(0);
              const targetVersion = getDataVersion(grpMerge.result_part_name);
              // Shorten part name: just show the partition + block range
              const shortPart = grpMerge.source_part_names[0] || grpMerge.result_part_name;
              // Find the actual mutations linked to this merge
              const linkedMuts = mutations.filter(m => {
                const allParts = [...m.parts_in_progress_names, ...m.parts_to_do_names];
                return allParts.some(p => grpMerge.source_part_names.includes(p));
              });
              return (
                <div key={grpMerge.result_part_name}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 3 }}>
                    <span style={{ fontSize: 9, fontFamily: 'monospace', color: 'var(--text-muted)', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }} title={shortPart}>
                      {shortPart}
                    </span>
                    <button
                      onClick={() => setDiagramMerge({ merge: grpMerge, mutations: linkedMuts })}
                      title="View merge dependency map"
                      style={{
                        background: 'rgba(59,130,246,0.12)', border: '1px solid rgba(59,130,246,0.25)',
                        borderRadius: 3, padding: '1px 5px', fontSize: 8, color: '#3b82f6',
                        cursor: 'pointer', fontWeight: 500, whiteSpace: 'nowrap',
                      }}
                    >
                      ◈ {count} mut{count !== 1 ? 's' : ''}
                    </button>
                    <span style={{ fontSize: 10, fontFamily: 'monospace', fontWeight: 600, color: '#a855f7', minWidth: 32, textAlign: 'right' }}>
                      {pct}%
                    </span>
                  </div>
                  <div style={{ height: 3, borderRadius: 2, background: 'rgba(168,85,247,0.15)' }}>
                    <div style={{
                      height: '100%', borderRadius: 2, background: '#a855f7',
                      width: `${grpMerge.progress * 100}%`, transition: 'width 0.3s ease',
                    }} />
                  </div>
                  {targetVersion && (
                    <div style={{ fontSize: 8, color: 'var(--text-muted)', marginTop: 2 }}>
                      Completes {count} mutation{count !== 1 ? 's' : ''}, applies to parts ≤ {targetVersion} ({grpMerge.elapsed.toFixed(0)}s)
                    </div>
                  )}
                </div>
              );
            })}

            {/* Unlinked merges — mutation merges not tied to any pending mutation */}
            {unlinkedMerges.length > 0 && (
              <div style={{ marginTop: 4, paddingTop: 6, borderTop: '1px solid var(--border-primary)' }}>
                {unlinkedMerges.map(merge => {
                  const pct = (merge.progress * 100).toFixed(0);
                  const shortPart = merge.source_part_names[0] || merge.result_part_name;
                  return (
                    <div key={merge.result_part_name} style={{ marginBottom: 4 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 3 }}>
                        <span style={{ fontSize: 9, fontFamily: 'monospace', color: 'var(--text-muted)', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }} title={shortPart}>
                          {shortPart}
                        </span>
                        <span style={{ fontSize: 9, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
                          finishing
                        </span>
                        <span style={{ fontSize: 10, fontFamily: 'monospace', fontWeight: 500, color: 'var(--text-muted)', minWidth: 32, textAlign: 'right' }}>
                          {pct}%
                        </span>
                      </div>
                      <div style={{ height: 2, borderRadius: 2, background: 'rgba(255,255,255,0.06)' }}>
                        <div style={{
                          height: '100%', borderRadius: 2, background: 'var(--text-muted)',
                          width: `${merge.progress * 100}%`, transition: 'width 0.3s ease', opacity: 0.5,
                        }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Footer summary */}
          <div style={{ marginTop: 8, paddingTop: 6, borderTop: '1px solid var(--border-primary)', fontSize: 9, color: 'var(--text-muted)', display: 'flex', justifyContent: 'space-between' }}>
            <span>{totalMutationMerges} mutation merge{totalMutationMerges !== 1 ? 's' : ''} active</span>
            <span>{mutations.length} pending mutation{mutations.length !== 1 ? 's' : ''}</span>
          </div>
        </div>
      )}

      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-primary)' }}>
            <th style={{ padding: '6px 12px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10, width: 18 }}>
              <CopyTableButton
                headers={['Table', 'Mutation ID', 'Command', 'Merge Progress', 'Parts', 'Age']}
                rows={mutations.map(m => {
                  const lm = getMergeForMutation(m, partToMerge);
                  return [
                    `${m.database}.${m.table}`, m.mutation_id, m.command,
                    lm ? `${(lm.progress * 100).toFixed(0)}%` : 'waiting',
                    `${m.parts_done}/${m.total_parts}`, formatElapsed(m.create_time),
                  ];
                })}
                size={12}
              />
            </th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Table</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Mutation ID</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Command</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Merge</th>
            <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Parts</th>
            <th style={{ padding: '6px 12px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Age</th>
            <th style={{ padding: '6px 8px', textAlign: 'center', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10, width: 28 }}></th>
          </tr>
        </thead>
        <tbody>
          {mutations.map((mutation, idx) => {
            const isSelected = selectedMutation?.mutation_id === mutation.mutation_id &&
                               selectedMutation?.database === mutation.database &&
                               selectedMutation?.table === mutation.table;
            const linkedMerge = getMergeForMutation(mutation, partToMerge);
            const totalParts = mutation.total_parts;
            const partsDone = mutation.parts_done;
            return (
              <tr 
                key={`${mutation.mutation_id}-${idx}`}
                className="mutation-row"
                onMouseEnter={() => onPreviewMutation?.(mutation)}
                onClick={() => onSelectMutation(mutation)}
                style={{ 
                  borderBottom: '1px solid var(--border-primary)',
                  background: isSelected ? 'rgba(168,85,247,0.2)' : 'transparent',
                  cursor: 'pointer',
                  transition: 'background 0.15s ease',
                }}
              >
                <td style={{ padding: '5px 4px 5px 12px', width: 18 }}>
                  <div style={{ width: 8, height: 8, borderRadius: 2, background: MUTATION_COLORS[idx % MUTATION_COLORS.length] }} />
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-secondary)', fontSize: 10 }}>
                  {mutation.database}.{mutation.table}
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 10 }}>
                  {mutation.mutation_id}
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={mutation.command}>
                  {(() => {
                    const subtype = classifyMutationCommand(mutation.command);
                    const info = MUTATION_SUBTYPES[subtype];
                    return (
                      <span style={{ padding: '1px 4px', fontSize: 8, borderRadius: 3, background: `${info.color}20`, color: info.color, border: `1px solid ${info.color}40`, marginRight: 4, fontWeight: 500, whiteSpace: 'nowrap' }} title={info.description}>
                        {info.shortLabel}
                      </span>
                    );
                  })()}
                  {mutation.command.length > 50 ? mutation.command.slice(0, 50) + '...' : mutation.command}
                </td>
                <td style={{ padding: '5px 8px' }}>
                  {linkedMerge ? (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, minWidth: 80 }}>
                      <div style={{ flex: 1, height: 3, borderRadius: 2, background: 'rgba(168,85,247,0.15)', minWidth: 40 }}>
                        <div style={{
                          height: '100%', borderRadius: 2, background: '#a855f7',
                          width: `${linkedMerge.progress * 100}%`, transition: 'width 0.3s ease',
                        }} />
                      </div>
                      <span style={{ fontSize: 9, fontFamily: 'monospace', color: '#a855f7', whiteSpace: 'nowrap' }}>
                        {(linkedMerge.progress * 100).toFixed(0)}%
                      </span>
                    </div>
                  ) : (
                    <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>waiting</span>
                  )}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', fontSize: 10 }}>
                  {totalParts > 0 && partsDone > 0 ? (
                    <span>
                      <span style={{ color: '#3fb950' }}>{partsDone}</span>
                      <span style={{ color: 'var(--text-muted)' }}>/{totalParts}</span>
                    </span>
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>{mutation.parts_to_do}</span>
                  )}
                </td>
                <td style={{ padding: '5px 12px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 10 }}>
                  {formatElapsed(mutation.create_time)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'center', width: 28 }}>
                  {(mutation.parts_to_do_names.length > 0 || mutation.parts_in_progress_names.length > 0) && (
                    <button
                      onClick={(e) => { e.stopPropagation(); setDiagramMutation(mutation); }}
                      title="View dependency map"
                      style={{
                        background: 'none', border: 'none', cursor: 'pointer', padding: 2,
                        color: 'rgba(168,85,247,0.5)', fontSize: 13, lineHeight: 1,
                        transition: 'color 0.15s, transform 0.15s',
                      }}
                      onMouseEnter={e => { e.currentTarget.style.color = '#a855f7'; e.currentTarget.style.transform = 'scale(1.2)'; }}
                      onMouseLeave={e => { e.currentTarget.style.color = 'rgba(168,85,247,0.5)'; e.currentTarget.style.transform = 'scale(1)'; }}
                    >
                      ◈
                    </button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      {/* Dependency diagram modal triggered from row icon */}
      {diagramMutation && diagramDependency && (
        <MutationDependencyDiagram
          dependency={diagramDependency}
          mutation={diagramMutation}
          onClose={() => setDiagramMutation(null)}
        />
      )}

      {/* Merge dependency diagram modal triggered from merge summary */}
      {diagramMerge && (
        <MergeDependencyDiagram
          merge={diagramMerge.merge}
          affectedMutations={diagramMerge.mutations}
          onClose={() => setDiagramMerge(null)}
        />
      )}
    </div>
  );
};

// Mutation History Panel - Shows completed mutations in table format (Time Travel style)
const MutationHistoryPanel: React.FC<{
  history: MutationHistoryRecord[];
  isLoading: boolean;
  selectedRecord: MutationHistoryRecord | null;
  onSelectRecord: (record: MutationHistoryRecord) => void;
  onPreviewRecord?: (record: MutationHistoryRecord) => void;
}> = ({ history, isLoading, selectedRecord, onSelectRecord, onPreviewRecord }) => {
  if (history.length === 0 && isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 16, height: 16, border: '2px solid var(--border-primary)', borderTopColor: 'var(--accent-primary)', borderRadius: '50%', animation: 'spin 1s linear infinite', marginBottom: 4 }} />
          <p style={{ fontSize: 11 }}>Loading...</p>
        </div>
      </div>
    );
  }

  if (history.length === 0) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 13, marginBottom: 2, fontWeight: 300 }}>No History</div>
          <div style={{ fontSize: 10 }}>No completed mutations found</div>
        </div>
      </div>
    );
  }

  const getStatusColor = (record: MutationHistoryRecord) => {
    if (record.is_killed) return '#f97316';
    if (record.latest_fail_reason) return '#f85149';
    return '#3fb950';
  };

  const getStatusLabel = (record: MutationHistoryRecord) => {
    if (record.is_killed) return 'Killed';
    if (record.latest_fail_reason) return 'Failed';
    return 'Done';
  };
  const killedStateUnavailable = history.some(
    record => record.is_killed_supported === false,
  );

  return (
    <div style={{ overflow: 'auto' }}>
      <style>{`
        .mutation-history-row:hover { background: var(--bg-hover) !important; }
      `}</style>
      {killedStateUnavailable && (
        <div style={{
          margin: '0 0 10px', padding: '7px 10px', borderRadius: 4,
          border: '1px solid rgba(245,158,11,0.35)',
          background: 'rgba(245,158,11,0.08)', color: '#d97706', fontSize: 10,
        }}>
          Compatibility: this ClickHouse version does not expose mutation
          killed-state. Completed mutations are shown, but killed mutations
          cannot be identified separately.
        </div>
      )}
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-primary)' }}>
            <th style={{ padding: '6px 12px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10, width: 18 }}>
              <CopyTableButton
                headers={['Table', 'Mutation ID', 'Command', 'Status', 'Created']}
                rows={history.map(r => [
                  `${r.database}.${r.table}`, r.mutation_id, r.command,
                  getStatusLabel(r), new Date(r.create_time).toLocaleString(),
                ])}
                size={12}
              />
            </th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Table</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Mutation ID</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Command</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Status</th>
            <th style={{ padding: '6px 12px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Created</th>
          </tr>
        </thead>
        <tbody>
          {history.map((record, idx) => {
            const statusColor = getStatusColor(record);
            const isSelected = selectedRecord?.mutation_id === record.mutation_id && 
                               selectedRecord?.database === record.database &&
                               selectedRecord?.table === record.table;
            return (
              <tr 
                key={`${record.mutation_id}-${idx}`}
                className="mutation-history-row"
                onMouseEnter={() => onPreviewRecord?.(record)}
                onClick={() => onSelectRecord(record)}
                style={{ 
                  borderBottom: '1px solid var(--border-primary)',
                  background: isSelected ? 'rgba(247,120,186,0.2)' : 'transparent',
                  cursor: 'pointer',
                  transition: 'background 0.15s ease',
                }}
              >
                <td style={{ padding: '5px 4px 5px 12px', width: 18 }}>
                  <div style={{ width: 8, height: 8, borderRadius: 2, background: statusColor }} />
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
                  {record.database}.{record.table}
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {record.mutation_id}
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={record.command}>
                  {record.command.length > 50 ? record.command.slice(0, 50) + '...' : record.command}
                </td>
                <td style={{ padding: '5px 8px' }}>
                  <span style={{ 
                    padding: '1px 6px', 
                    fontSize: 9, 
                    borderRadius: 3,
                    background: `${statusColor}20`,
                    color: statusColor,
                    border: `1px solid ${statusColor}33`,
                    textTransform: 'uppercase',
                  }}>
                    {getStatusLabel(record)}
                  </span>
                </td>
                <td style={{ padding: '5px 12px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 10 }}>
                  {new Date(record.create_time).toLocaleString()}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

// Merge Detail Panel
const MergeDetailPanel: React.FC<{
  merge: MergeInfo | null;
  onClose: () => void;
  onOpenFullDetails: (merge: MergeInfo) => void;
}> = ({ merge, onClose, onOpenFullDetails }) => {
  const services = useClickHouseServices();

  // Fetch historical throughput for ETA estimation
  const [throughputEstimate, setThroughputEstimate] = useState<MergeThroughputEstimate | null>(null);
  useEffect(() => {
    setThroughputEstimate(null);
    if (!merge || !services) return;
    let cancelled = false;
    services.mergeTracker.getMergeThroughputEstimate(merge.database, merge.table).then(estimates => {
      if (cancelled) return;
      setThroughputEstimate(pickThroughputEstimate(estimates, merge.merge_algorithm, merge.total_size_bytes_compressed));
    }).catch(err => {
      console.error('[MergeDetailPanel] ETA fetch failed:', err);
    });
    return () => { cancelled = true; };
  }, [merge?.database, merge?.table, merge?.merge_algorithm, services]); // eslint-disable-line react-hooks/exhaustive-deps

  const etaInfo = useMemo(() => {
    if (!merge) return null;
    return computeMergeEta(merge.total_size_bytes_compressed, merge.progress, merge.elapsed, throughputEstimate);
  }, [merge, throughputEstimate]);

  if (!merge) {
    return (
      <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 24, marginBottom: 8, fontWeight: 300 }}>--</div>
          <p style={{ fontSize: 12 }}>Select a merge to view details</p>
        </div>
      </div>
    );
  }

  const percentage = (merge.progress * 100).toFixed(1);
  const category = classifyActiveMerge(merge.merge_type, merge.is_mutation, merge.result_part_name);
  const categoryInfo = getMergeCategoryInfo(category);
  const getTypeColor = () => categoryInfo.color;

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div style={{ padding: '12px 16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)' }}>
        <h3 style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 14, margin: 0 }}>
          Active Merge
        </h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button
            onClick={() => onOpenFullDetails(merge)}
            style={{ padding: '4px 10px', fontSize: 10, borderRadius: 4, background: `${getTypeColor()}20`, color: getTypeColor(), border: `1px solid ${getTypeColor()}33`, cursor: 'pointer' }}
          >
            Open full details
          </button>
          <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14 }}>✕</button>
        </div>
      </div>

      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Table</div>
          <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>
            {merge.database}.{merge.table}
          </div>
        </div>

        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Result Part</div>
          <code style={{ fontSize: 11, padding: '4px 8px', borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-secondary)', display: 'block', wordBreak: 'break-all' }}>
            {merge.result_part_name}
          </code>
        </div>

        <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <span style={{
            padding: '2px 8px', fontSize: 10, borderRadius: 4,
            background: `${getTypeColor()}20`, color: getTypeColor(),
            border: `1px solid ${getTypeColor()}33`,
          }}>
            {categoryInfo.label}
          </span>
          {merge.merge_algorithm && (
            <span style={{
              padding: '2px 8px', fontSize: 10, borderRadius: 4,
              background: 'var(--bg-tertiary)', color: 'var(--text-muted)',
            }}>
              {merge.merge_algorithm}
            </span>
          )}
          {merge.is_replica_merge && (
            <span style={{
              padding: '2px 8px', fontSize: 10, borderRadius: 4,
              background: 'rgba(136,136,136,0.15)', color: '#888',
              border: '1px solid rgba(136,136,136,0.25)',
            }}>
              Replica Merge
            </span>
          )}
        </div>

        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 8, color: 'var(--text-muted)' }}>Progress</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <div style={{ flex: 1, height: 6, borderRadius: 3, background: 'var(--bg-tertiary)' }}>
              <div style={{
                width: `${percentage}%`, height: '100%', borderRadius: 3,
                background: getTypeColor(), transition: 'width 0.3s ease',
              }} />
            </div>
            <span style={{ fontSize: 12, fontFamily: 'monospace', color: getTypeColor(), fontWeight: 600 }}>
              {percentage}%
            </span>
          </div>
          {etaInfo && (
            <div
              title={`Blended throughput: ${formatBytes(etaInfo.medianThroughput)}/s · based on ${etaInfo.basedOnCount} past merges`}
              style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 8 }}
            >
              ETA <span style={{ color: getTypeColor(), fontWeight: 600, fontFamily: 'monospace' }}>~{formatDuration(etaInfo.remainingSec)}</span>
              {' · '}based on {etaInfo.basedOnCount} {etaInfo.sizeMatched ? 'similarly sized ' : ''}merges
            </div>
          )}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
          {(() => {
            const bytesProcessed = merge.total_size_bytes_compressed * merge.progress;
            const throughput = merge.elapsed > 0 ? bytesProcessed / merge.elapsed : 0;
            return [
              { label: 'Elapsed', value: `${merge.elapsed.toFixed(2)}s` },
              { label: 'Parts', value: `${merge.num_parts} → 1` },
              { label: 'Size', value: formatBytes(merge.total_size_bytes_compressed) },
              { label: 'Memory', value: formatBytes(merge.memory_usage || 0) },
              { label: 'Rows Read', value: merge.rows_read.toLocaleString() },
              { label: 'Rows Written', value: merge.rows_written.toLocaleString() },
              { label: 'Throughput', value: throughput > 0 ? `${formatBytes(throughput)}/s` : '-' },
            ];
          })().map(({ label, value }) => (
            <div
              key={label}
              style={{ borderRadius: 8, padding: 12, background: 'var(--bg-tertiary)' }}
            >
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{label}</div>
              <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>{value}</div>
            </div>
          ))}
        </div>

        {merge.source_part_names && merge.source_part_names.length > 0 && (
          <div>
            <div style={{ fontSize: 10, marginBottom: 6, color: 'var(--text-muted)' }}>
              Source Parts ({merge.source_part_names.length})
            </div>
            <div style={{ maxHeight: 120, overflow: 'auto', background: 'var(--bg-tertiary)', borderRadius: 6, padding: 8, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
              {merge.source_part_names.map((part, i) => (
                <code key={i} style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-secondary)', padding: '1px 4px', background: 'var(--bg-secondary)', borderRadius: 3 }}>
                  {part}
                </code>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

// Merge History Detail Panel — details-only summary with "Open full details" button

const MergeHistoryDetailPanel: React.FC<{
  record: MergeHistoryRecord | null;
  onClose: () => void;
  onOpenFullDetails: (record: MergeHistoryRecord) => void;
}> = ({ record: liteRecord, onClose, onOpenFullDetails }) => {
  const services = useClickHouseServices();
  const [fullRecord, setFullRecord] = useState<MergeHistoryRecord | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [volumeInfo, setVolumeInfo] = useState<{ volumeName: string; policyName: string } | null>(null);

  // Fetch full record (with merge_algorithm, disk_name, query_id, etc.) on selection
  useEffect(() => {
    setFullRecord(null);
    setDetailError(null);
    if (!liteRecord || !services) return;
    let cancelled = false;
    services.mergeTracker.getMergeHistoryByPartName(liteRecord.database, liteRecord.table, liteRecord.part_name)
      .then(r => {
        if (cancelled) return;
        if (!r) {
          console.warn('[MergeHistoryDetailPanel] No detail record found for', liteRecord.database, liteRecord.table, liteRecord.part_name);
          setDetailError('Detail record not found in part_log');
        }
        setFullRecord(r);
      })
      .catch((err) => {
        if (cancelled) return;
        console.error('[MergeHistoryDetailPanel] Failed to fetch detail record:', err);
        setDetailError(err instanceof Error ? err.message : 'Failed to fetch detail record');
      });
    return () => { cancelled = true; };
  }, [liteRecord?.database, liteRecord?.table, liteRecord?.part_name, services]); // eslint-disable-line react-hooks/exhaustive-deps

  const record = fullRecord ?? liteRecord;

  // Fetch storage policy volume info for TTLMove
  useEffect(() => {
    setVolumeInfo(null);
    if (record?.merge_reason !== 'TTLMove' || !services) return;
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
  }, [record?.disk_name, record?.merge_reason, services]);

  if (!record) {
    return (
      <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 24, marginBottom: 8, fontWeight: 300 }}>--</div>
          <p style={{ fontSize: 12 }}>Select a merge to view details</p>
        </div>
      </div>
    );
  }

  const isTTLMove = record.merge_reason === 'TTLMove';
  const isMutationRecord = record.merge_reason === 'Mutation' || record.event_type === 'MutatePart';
  const accentColor = isMutationRecord ? '#a855f7' : '#f0883e';

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div style={{ padding: '12px 16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)' }}>
        <h3 style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 14, margin: 0 }}>Merge Details</h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <button
            onClick={() => onOpenFullDetails(record)}
            style={{ padding: '4px 10px', fontSize: 10, borderRadius: 4, background: `${accentColor}20`, color: accentColor, border: `1px solid ${accentColor}33`, cursor: 'pointer' }}
          >
            Open full details
          </button>
          <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14 }}>✕</button>
        </div>
      </div>
      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
        {detailError && (
          <div style={{ marginBottom: 12, padding: '8px 12px', borderRadius: 6, background: 'rgba(229,83,75,0.1)', border: '1px solid rgba(229,83,75,0.25)', color: '#e5534b', fontSize: 11 }}>
            {detailError} — showing partial data
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
        {(record.merge_reason || record.merge_algorithm || record.is_replica_merge) && (
          <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            {record.merge_reason && (
              <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: `${accentColor}26`, color: accentColor, border: `1px solid ${accentColor}4d` }}>{record.merge_reason}</span>
            )}
            {record.merge_algorithm && record.merge_algorithm !== 'Undecided' && (
              <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}>{record.merge_algorithm}</span>
            )}
            {record.is_replica_merge && (
              <span style={{ padding: '2px 8px', fontSize: 10, borderRadius: 4, background: 'rgba(136,136,136,0.15)', color: '#888', border: '1px solid rgba(136,136,136,0.25)' }}>
                {record.event_type === 'DownloadPart' ? 'Replica Fetch' : 'Replica Merge'}
              </span>
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
              { label: 'Duration', value: `${(record.duration_ms / 1000).toFixed(2)}s` },
              { label: 'Rows (output)', value: record.rows.toLocaleString() },
              { label: 'Final Size', value: formatBytes(record.size_in_bytes) },
              { label: 'Peak Memory', value: formatBytes(record.peak_memory_usage) },
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
        <div>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Event Time</div>
          <div style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)' }}>{new Date(record.event_time).toLocaleString()}</div>
        </div>
      </div>
    </div>
  );
};

// Mutation History Detail Panel
const MutationHistoryDetailPanel: React.FC<{
  record: MutationHistoryRecord | null;
  onClose: () => void;
}> = ({ record, onClose }) => {
  if (!record) {
    return (
      <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 24, marginBottom: 8, fontWeight: 300 }}>--</div>
          <p style={{ fontSize: 12 }}>Select a mutation to view details</p>
        </div>
      </div>
    );
  }

  const getStatusColor = () => {
    if (record.is_killed) return '#f97316';
    if (record.latest_fail_reason) return '#f85149';
    return '#3fb950';
  };

  const getStatusLabel = () => {
    if (record.is_killed) return 'Killed';
    if (record.latest_fail_reason) return 'Failed';
    return 'Completed';
  };

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div 
        style={{ padding: 16, display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)' }}
      >
        <h3 style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 14 }}>
          Mutation Details
        </h3>
        <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14 }}>✕</button>
      </div>

      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Table</div>
          <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>
            {record.database}.{record.table}
          </div>
        </div>

        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Mutation ID</div>
          <code style={{ fontSize: 11, padding: '4px 8px', borderRadius: 4, background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }}>
            {record.mutation_id}
          </code>
        </div>

        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Status</div>
          <span style={{ 
            padding: '4px 10px', fontSize: 11, borderRadius: 4,
            background: `${getStatusColor()}20`, color: getStatusColor(),
            border: `1px solid ${getStatusColor()}33`,
            fontWeight: 500,
          }}>
            {getStatusLabel()}
          </span>
        </div>

        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 10, marginBottom: 8, color: 'var(--text-muted)' }}>Command</div>
          <div style={{ 
            background: 'var(--bg-tertiary)', borderRadius: 6, padding: 12,
            maxHeight: 200, overflow: 'auto',
          }}>
            <code style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
              {record.command}
            </code>
          </div>
        </div>

        {record.latest_fail_reason && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 10, marginBottom: 8, color: 'var(--text-muted)' }}>Failure Reason</div>
            <div style={{ 
              background: 'rgba(248,81,73,0.1)', borderRadius: 6, padding: 12,
              border: '1px solid rgba(248,81,73,0.2)',
            }}>
              <code style={{ fontSize: 11, fontFamily: 'monospace', color: '#f85149', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {record.latest_fail_reason}
              </code>
            </div>
          </div>
        )}

        {record.latest_failed_part && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Failed Part</div>
            <code style={{ fontSize: 11, padding: '4px 8px', borderRadius: 4, background: 'var(--bg-tertiary)', color: '#f85149' }}>
              {record.latest_failed_part}
            </code>
          </div>
        )}

        <div>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Created</div>
          <div style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
            {new Date(record.create_time).toLocaleString()}
          </div>
        </div>
      </div>
    </div>
  );
};

// Mutation Dependency Section — compact, narrative-driven
const MutationDependencySection: React.FC<{
  dependency: MutationDependencyInfo;
  mutation: MutationInfo;
}> = ({ dependency, mutation }) => {
  const [showParts, setShowParts] = useState(false);
  const [showCoDeps, setShowCoDeps] = useState(false);
  const [showDiagram, setShowDiagram] = useState(false);

  const { part_statuses, co_dependent_mutations } = dependency;
  const mutatingParts = part_statuses.filter(p => p.status === 'mutating');
  const mutatingWithMerge = mutatingParts.filter(p => p.merge_progress !== undefined);
  const idleParts = part_statuses.filter(p => p.status === 'idle');

  // Extract the merge info for the active mutation merge (if any)
  const activeMerge = mutatingWithMerge.length > 0 ? mutatingWithMerge[0] : null;

  // Parse data_version from result part name
  const getDataVersion = (resultPart?: string) => {
    if (!resultPart) return null;
    const segments = resultPart.split('_');
    return segments.length >= 5 ? segments[segments.length - 1] : null;
  };

  // Parse mutation number from mutation_id like "mutation_2725.txt"
  const getMutationNumber = (mutId: string) => {
    const match = mutId.match(/mutation_(\d+)/);
    return match ? parseInt(match[1], 10) : null;
  };

  const targetVersion = activeMerge ? getDataVersion(activeMerge.merge_result_part) : null;
  const myMutationNum = getMutationNumber(mutation.mutation_id);

  // How many co-deps will complete with this merge?
  const willCompleteCount = targetVersion && myMutationNum && myMutationNum <= parseInt(targetVersion, 10)
    ? co_dependent_mutations.filter(cd => {
        const n = getMutationNumber(cd.mutation_id);
        return n !== null && n <= parseInt(targetVersion!, 10);
      }).length
    : 0;

  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>
          Merge Activity
        </div>
        <button
          onClick={() => setShowDiagram(true)}
          style={{
            background: 'rgba(168,85,247,0.12)', border: '1px solid rgba(168,85,247,0.25)',
            borderRadius: 4, padding: '2px 8px', fontSize: 9, color: '#a855f7',
            cursor: 'pointer', fontWeight: 500,
          }}
          title="Open dependency diagram"
        >
          ◈ Dependency Map
        </button>
      </div>

      {/* Dependency diagram modal */}
      {showDiagram && (
        <MutationDependencyDiagram
          dependency={dependency}
          mutation={mutation}
          onClose={() => setShowDiagram(false)}
        />
      )}

      {/* Active merge progress card */}
      {activeMerge && activeMerge.merge_progress !== undefined ? (
        <div style={{
          background: 'rgba(168,85,247,0.08)', borderRadius: 6, padding: 12, marginBottom: 10,
          border: '1px solid rgba(168,85,247,0.2)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <span style={{ fontSize: 10, color: '#a855f7', fontWeight: 500 }}>
              Mutation merge active
            </span>
            <span style={{ fontSize: 12, fontFamily: 'monospace', color: '#a855f7', fontWeight: 600 }}>
              {(activeMerge.merge_progress * 100).toFixed(1)}%
            </span>
          </div>
          {/* Progress bar */}
          <div style={{ height: 4, borderRadius: 2, background: 'rgba(168,85,247,0.15)', marginBottom: 8 }}>
            <div style={{
              height: '100%', borderRadius: 2, background: '#a855f7',
              width: `${(activeMerge.merge_progress * 100)}%`, transition: 'width 0.3s ease',
            }} />
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, color: 'var(--text-muted)' }}>
            {activeMerge.merge_elapsed !== undefined && (
              <span>Elapsed: {activeMerge.merge_elapsed.toFixed(0)}s</span>
            )}
            {targetVersion && (
              <span title={`Result part: ${activeMerge.merge_result_part}`}>
                Applies to parts ≤ {targetVersion}
              </span>
            )}
          </div>
          {/* What completes when this merge finishes */}
          {targetVersion && myMutationNum && myMutationNum <= parseInt(targetVersion, 10) && (
            <div style={{ marginTop: 8, fontSize: 9, color: '#3fb950', lineHeight: 1.4 }}>
              This mutation will complete when merge finishes
              {willCompleteCount > 0 && (
                <span style={{ color: 'var(--text-muted)' }}>
                  {' '}(along with {willCompleteCount} other mutation{willCompleteCount !== 1 ? 's' : ''})
                </span>
              )}
            </div>
          )}
          {targetVersion && myMutationNum && myMutationNum > parseInt(targetVersion, 10) && (
            <div style={{ marginTop: 8, fontSize: 9, color: 'var(--text-muted)', lineHeight: 1.4 }}>
              Merge covers parts ≤ {targetVersion}, this mutation ({myMutationNum}) needs another pass
            </div>
          )}
        </div>
      ) : mutatingParts.length > 0 ? (
        <div style={{
          background: 'rgba(168,85,247,0.08)', borderRadius: 6, padding: 10, marginBottom: 10,
          border: '1px solid rgba(168,85,247,0.15)', fontSize: 10, color: '#a855f7',
        }}>
          {mutatingParts.length} part{mutatingParts.length !== 1 ? 's' : ''} in progress (merge starting...)
        </div>
      ) : (
        <div style={{
          background: 'var(--bg-tertiary)', borderRadius: 6, padding: 10, marginBottom: 10,
          fontSize: 10, color: 'var(--text-muted)',
        }}>
          Waiting — no active merge on these parts
        </div>
      )}

      {/* Parts summary — collapsible */}
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', padding: '4px 0' }}
        onClick={() => setShowParts(!showParts)}
      >
        <span style={{ fontSize: 8, color: 'var(--text-muted)', transition: 'transform 0.15s', transform: showParts ? 'rotate(90deg)' : 'rotate(0deg)' }}>▶</span>
        <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
          {part_statuses.length} part{part_statuses.length !== 1 ? 's' : ''}
          {mutatingParts.length > 0 && <span style={{ color: '#a855f7' }}> ({mutatingParts.length} active)</span>}
          {idleParts.length > 0 && <span> ({idleParts.length} waiting)</span>}
        </span>
      </div>
      {showParts && (
        <div style={{ background: 'var(--bg-tertiary)', borderRadius: 6, padding: 8, marginTop: 4, maxHeight: 160, overflow: 'auto' }}>
          {part_statuses.map((part, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '2px 0' }}>
              <span style={{
                width: 6, height: 6, borderRadius: '50%', flexShrink: 0,
                background: part.status === 'mutating' ? '#a855f7' : part.status === 'merging' ? '#f0883e' : 'var(--text-muted)',
              }} />
              <span style={{
                flex: 1, fontSize: 9, fontFamily: 'monospace',
                color: part.status === 'mutating' ? '#a855f7' : part.status === 'merging' ? '#f0883e' : 'var(--text-muted)',
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              }} title={part.part_name}>
                {part.part_name}
              </span>
              {part.merge_progress !== undefined && (
                <span style={{ fontSize: 8, fontFamily: 'monospace', color: part.status === 'mutating' ? '#a855f7' : '#f0883e' }}>
                  {(part.merge_progress * 100).toFixed(0)}%
                </span>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Co-dependent mutations — collapsible */}
      {co_dependent_mutations.length > 0 && (
        <>
          <div
            style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', padding: '4px 0', marginTop: 4 }}
            onClick={() => setShowCoDeps(!showCoDeps)}
          >
            <span style={{ fontSize: 8, color: 'var(--text-muted)', transition: 'transform 0.15s', transform: showCoDeps ? 'rotate(90deg)' : 'rotate(0deg)' }}>▶</span>
            <span style={{ fontSize: 10, color: '#f778ba' }}>
              {co_dependent_mutations.length} co-dependent mutation{co_dependent_mutations.length !== 1 ? 's' : ''}
            </span>
            <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>
              (same parts)
            </span>
          </div>
          {showCoDeps && (
            <div style={{ background: 'var(--bg-tertiary)', borderRadius: 6, padding: 8, marginTop: 4, maxHeight: 140, overflow: 'auto' }}>
              {co_dependent_mutations.map((dep, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '2px 0', fontSize: 9 }}>
                  <code style={{ color: '#f778ba', fontFamily: 'monospace' }}>{dep.mutation_id}</code>
                  <span style={{ color: 'var(--text-muted)' }}>{dep.shared_parts_count} shared</span>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
};

// Active Mutation Detail Panel
const ActiveMutationDetailPanel: React.FC<{
  mutation: MutationInfo | null;
  activeMerges: MergeInfo[];
  allMutations: MutationInfo[];
  onClose: () => void;
}> = ({ mutation, activeMerges, allMutations, onClose }) => {
  const [showDiagram, setShowDiagram] = useState(false);
  // Compute dependency info
  const dependencyInfo = React.useMemo<MutationDependencyInfo | null>(() => {
    if (!mutation) return null;
    const allParts = [...mutation.parts_to_do_names, ...mutation.parts_in_progress_names];
    if (allParts.length === 0) return null;

    // Build lookup: source part name -> active merge (mutation merges included)
    // A mutation merge has is_mutation=true and its result_part_name encodes the
    // data_version (last segment) which equals the highest mutation number it applies.
    const tableMerges = activeMerges.filter(
      m => m.database === mutation.database && m.table === mutation.table,
    );
    const partToMerge = new Map<string, MergeInfo>();
    for (const merge of tableMerges) {
      for (const src of merge.source_part_names) {
        partToMerge.set(src, merge);
      }
    }

    const inProgressSet = new Set(mutation.parts_in_progress_names);
    const partStatuses = allParts.map(partName => {
      const merge = partToMerge.get(partName);
      if (inProgressSet.has(partName) && merge) {
        // Part is in progress AND there's an active merge processing it
        return {
          part_name: partName,
          status: 'mutating' as const,
          merge_result_part: merge.result_part_name,
          merge_progress: merge.progress,
          merge_elapsed: merge.elapsed,
        };
      }
      if (inProgressSet.has(partName)) {
        // Part is in progress but no active merge visible (between scheduling and start)
        return { part_name: partName, status: 'mutating' as const };
      }
      if (merge && !merge.is_mutation) {
        // Part is being merged by a regular merge (will subsume pending mutations)
        return {
          part_name: partName,
          status: 'merging' as const,
          merge_result_part: merge.result_part_name,
          merge_progress: merge.progress,
          merge_elapsed: merge.elapsed,
        };
      }
      return { part_name: partName, status: 'idle' as const };
    });

    const myParts = new Set(allParts);
    const coDeps = allMutations
      .filter(other => other.mutation_id !== mutation.mutation_id && other.database === mutation.database && other.table === mutation.table)
      .map(other => {
        const otherParts = [...other.parts_to_do_names, ...other.parts_in_progress_names];
        const shared = otherParts.filter(p => myParts.has(p));
        return shared.length > 0 ? { mutation_id: other.mutation_id, command: other.command, shared_parts_count: shared.length, shared_parts: shared } : null;
      })
      .filter((x): x is NonNullable<typeof x> => x !== null)
      .sort((a, b) => b.shared_parts_count - a.shared_parts_count);

    const mergingParts = partStatuses.filter(p => p.status === 'merging');
    const mutatingWithMerge = partStatuses.filter(p => p.status === 'mutating' && p.merge_result_part);
    const allCoveredParts = [...mergingParts, ...mutatingWithMerge];
    const uniqueMerges = new Set(allCoveredParts.map(p => p.merge_result_part).filter(Boolean));

    return {
      mutation_id: mutation.mutation_id,
      database: mutation.database,
      table: mutation.table,
      part_statuses: partStatuses,
      co_dependent_mutations: coDeps,
      parts_covered_by_merges: allCoveredParts.length,
      active_merges_covering: uniqueMerges.size,
    };
  }, [mutation, activeMerges, allMutations]);

  if (!mutation) {
    return (
      <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 24, marginBottom: 8, fontWeight: 300 }}>--</div>
          <p style={{ fontSize: 12 }}>Select a mutation to view details</p>
        </div>
      </div>
    );
  }

  const getStatusColor = () => {
    if (mutation.is_killed) return '#f97316';
    if (mutation.latest_fail_reason) return '#f85149';
    if (mutation.status === 'running') return '#a855f7';
    return 'var(--text-muted)';
  };

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div
        style={{ padding: 16, display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)' }}
      >
        <h3 style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 14 }}>
          Active Mutation
        </h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {dependencyInfo && dependencyInfo.part_statuses.length > 0 && (
            <button
              onClick={() => setShowDiagram(true)}
              style={{
                background: 'rgba(168,85,247,0.12)', border: '1px solid rgba(168,85,247,0.25)',
                borderRadius: 4, padding: '3px 10px', fontSize: 10, color: '#a855f7',
                cursor: 'pointer', fontWeight: 500,
              }}
              title="Open dependency diagram"
            >
              ◈ Map
            </button>
          )}
          <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14 }}>✕</button>
        </div>
      </div>

      {/* Dependency diagram modal */}
      {showDiagram && dependencyInfo && mutation && (
        <MutationDependencyDiagram
          dependency={dependencyInfo}
          mutation={mutation}
          onClose={() => setShowDiagram(false)}
        />
      )}

      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
        {/* Header: table + mutation ID + status in compact layout */}
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>
            {mutation.database}.{mutation.table}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 6 }}>
            <code style={{ fontSize: 10, padding: '2px 6px', borderRadius: 3, background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}>
              {mutation.mutation_id}
            </code>
            <span style={{
              padding: '2px 8px', fontSize: 9, borderRadius: 3,
              background: `${getStatusColor()}20`, color: getStatusColor(),
              border: `1px solid ${getStatusColor()}33`,
              fontWeight: 500, textTransform: 'uppercase',
            }}>
              {mutation.status}
            </span>
          </div>
        </div>

        {/* Parts counters — compact row */}
        <div style={{
          display: 'flex', gap: 2, marginBottom: 16, background: 'var(--bg-tertiary)', borderRadius: 6, padding: 10,
        }}>
          {[
            { label: 'Done', value: mutation.parts_done, color: '#3fb950' },
            { label: 'Active', value: mutation.parts_in_progress, color: '#a855f7' },
            { label: 'Waiting', value: mutation.parts_to_do, color: 'var(--text-muted)' },
          ].map(({ label, value, color }) => (
            <div key={label} style={{ flex: 1, textAlign: 'center' }}>
              <div style={{ fontWeight: 600, color, fontFamily: 'monospace', fontSize: 14 }}>{value}</div>
              <div style={{ fontSize: 9, color: 'var(--text-muted)' }}>{label}</div>
            </div>
          ))}
        </div>

        {/* Dependency Analysis — the main event */}
        {dependencyInfo && dependencyInfo.part_statuses.length > 0 && (
          <MutationDependencySection dependency={dependencyInfo} mutation={mutation} />
        )}

        {/* Command — collapsible */}
        <details style={{ marginBottom: 16 }}>
          <summary style={{ fontSize: 10, color: 'var(--text-muted)', cursor: 'pointer', padding: '4px 0' }}>
            Command
          </summary>
          <div style={{
            background: 'var(--bg-tertiary)', borderRadius: 6, padding: 10, marginTop: 4,
            maxHeight: 120, overflow: 'auto',
          }}>
            <code style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
              {mutation.command}
            </code>
          </div>
        </details>

        {mutation.latest_fail_reason && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 10, marginBottom: 8, color: 'var(--text-muted)' }}>Failure Reason</div>
            <div style={{
              background: 'rgba(248,81,73,0.1)', borderRadius: 6, padding: 12,
              border: '1px solid rgba(248,81,73,0.2)',
            }}>
              <code style={{ fontSize: 11, fontFamily: 'monospace', color: '#f85149', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {mutation.latest_fail_reason}
              </code>
            </div>
          </div>
        )}

        {mutation.latest_failed_part && (
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Failed Part</div>
            <code style={{ fontSize: 11, padding: '4px 8px', borderRadius: 4, background: 'var(--bg-tertiary)', color: '#f85149' }}>
              {mutation.latest_failed_part}
            </code>
          </div>
        )}

        <div>
          <div style={{ fontSize: 10, marginBottom: 4, color: 'var(--text-muted)' }}>Created</div>
          <div style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
            {new Date(mutation.create_time).toLocaleString()}
          </div>
        </div>
      </div>
    </div>
  );
};

// No Connection
const NoConnection: React.FC<{ onConnect: () => void }> = ({ onConnect }) => (
  <div className="flex flex-col items-center justify-center py-16">
    <div 
      className="w-16 h-16 rounded-full flex items-center justify-center text-xl font-bold mb-4"
      style={{ background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}
    >
      M
    </div>
    <h3 className="text-lg font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>
      No Connection
    </h3>
    <p className="text-sm mb-4" style={{ color: 'var(--text-secondary)' }}>
      Connect to track merge operations
    </p>
    <button className="btn btn-primary" onClick={onConnect}>
      Add Connection
    </button>
  </div>
);

export const MergeTrackerView: React.FC = () => {
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const {
    activeMerges, mergeHistory, mutations, mutationHistory, poolMetrics, selectedMerge, historyFilter, historySort,
    statistics, isLoadingMerges, isLoadingHistory, isLoadingMutations, isLoadingMutationHistory, isLoadingPoolMetrics,
    error,
    setActiveMerges, setMergeHistory, setMutations, setMutationHistory, setPoolMetrics, selectMerge, setHistoryFilter,
    setHistorySort, setIsLoadingMerges, setIsLoadingHistory, setIsLoadingMutations, setIsLoadingMutationHistory,
    setIsLoadingPoolMetrics, setError, clearError, clearAll,
  } = useMergeStore();
  const { databases, setDatabases } = useDatabaseStore();

  // URL-synced state for shareable links
  const { state: urlState, update: updateUrl } = useUrlState(mergeUrlSchema);
  const requestedTab = urlState.tab || 'merges';
  const activeTab: MergeTab = requestedTab === 'health'
    ? 'health'
    : requestedTab === 'mutations' || requestedTab === 'mutationHistory'
      ? 'mutations'
      : 'merges';

  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [showActivityPreview, setShowActivityPreview] = useState(
    () => loadPreviewPreference(MERGE_ACTIVITY_PREVIEW_STORAGE_KEY),
  );
  const activityStateRef = useRef(createMergeActivityState());
  const mergeActivity = useMemo(
    () => reconcileMergeActivity(activityStateRef.current, activeMerges, mergeHistory),
    [activeMerges, mergeHistory],
  );
  const activityMerges = useMemo(() => mergeActivity.live.map(item => item.merge), [mergeActivity.live]);

  useEffect(() => {
    activityStateRef.current = createMergeActivityState();
  }, [activeProfileId]);

  // Selected items for detail panels
  const [selectedMergeHistoryRaw, setSelectedMergeHistoryRaw] = useState<MergeHistoryRecord | null>(null);
  const [selectedMutationHistory, setSelectedMutationHistory] = useState<MutationHistoryRecord | null>(null);
  const [selectedActiveMutation, setSelectedActiveMutation] = useState<MutationInfo | null>(null);
  const [previewMergeHistory, setPreviewMergeHistory] = useState<MergeHistoryRecord | null>(null);
  const [previewMutationHistory, setPreviewMutationHistory] = useState<MutationHistoryRecord | null>(null);
  const [previewActiveMutation, setPreviewActiveMutation] = useState<MutationInfo | null>(null);
  const [mergeDetailRecord, setMergeDetailRecordRaw] = useState<MergeHistoryRecord | null>(null);
  const [activeMergeDetail, setActiveMergeDetailRaw] = useState<MergeInfo | null>(null);

  // Sync detail deep-link (md_db/md_tbl/md_part) to URL for all selection types
  const syncDetailToUrl = useCallback((db?: string, tbl?: string, part?: string) => {
    if (db && tbl && part) {
      updateUrl({ md_db: db, md_tbl: tbl, md_part: part } as any);
    } else {
      updateUrl({ md_db: undefined, md_tbl: undefined, md_part: undefined } as any);
    }
  }, [updateUrl]);

  const setSelectedMergeHistory = useCallback((record: MergeHistoryRecord | null) => {
    setSelectedMergeHistoryRaw(record);
    if (record) {
      syncDetailToUrl(record.database, record.table, record.part_name);
    } else {
      syncDetailToUrl();
    }
  }, [syncDetailToUrl]);
  const selectedMergeHistory = selectedMergeHistoryRaw;
  const previewedMergeHistory = previewMergeHistory ?? selectedMergeHistory;
  const previewedMutationHistory = previewMutationHistory ?? selectedMutationHistory;
  const previewedActiveMutation = previewActiveMutation ?? selectedActiveMutation;

  useEffect(() => {
    if (!selectedActiveMutation) return;
    const stillRunning = mutations.some(mutation =>
      mutation.database === selectedActiveMutation.database &&
      mutation.table === selectedActiveMutation.table &&
      mutation.mutation_id === selectedActiveMutation.mutation_id
    );
    if (stillRunning) return;
    const completed = mutationHistory.find(record =>
      record.database === selectedActiveMutation.database &&
      record.table === selectedActiveMutation.table &&
      record.mutation_id === selectedActiveMutation.mutation_id
    );
    if (!completed) return;
    setSelectedMutationHistory(completed);
    setPreviewMutationHistory(completed);
    setSelectedActiveMutation(null);
    setPreviewActiveMutation(null);
  }, [mutationHistory, mutations, selectedActiveMutation]);

  const setMergeDetailRecord = useCallback((record: MergeHistoryRecord | null) => {
    setMergeDetailRecordRaw(record);
    if (record) {
      syncDetailToUrl(record.database, record.table, record.part_name);
    } else {
      syncDetailToUrl();
    }
  }, [syncDetailToUrl]);
  const setActiveMergeDetail = useCallback((merge: MergeInfo | null) => {
    setActiveMergeDetailRaw(merge);
    if (merge) {
      syncDetailToUrl(merge.database, merge.table, merge.result_part_name);
    } else {
      syncDetailToUrl();
    }
  }, [syncDetailToUrl]);

  const openMergeHistoryDetails = useCallback((record: MergeHistoryRecord) => {
    selectMerge(null);
    setSelectedMergeHistoryRaw(record);
    setPreviewMergeHistory(record);
    setMergeDetailRecord(record);
  }, [selectMerge, setMergeDetailRecord]);

  const openActiveMergeDetails = useCallback((merge: MergeInfo) => {
    setSelectedMergeHistoryRaw(null);
    setPreviewMergeHistory(null);
    selectMerge(merge);
    setActiveMergeDetail(merge);
  }, [selectMerge, setActiveMergeDetail]);

  // Keep selectedMerge in sync with refreshed activeMerges data
  const liveSelectedMerge = useMemo(() => {
    if (!selectedMerge) return null;
    return activityMerges.find(
      m => m.database === selectedMerge.database &&
        m.table === selectedMerge.table &&
        m.result_part_name === selectedMerge.result_part_name &&
        (m.hostname || '') === (selectedMerge.hostname || ''),
    ) ?? null;
  }, [activityMerges, selectedMerge]);

  // Keep the same detail selection open when a live merge becomes a part_log row.
  useEffect(() => {
    if (!selectedMerge || liveSelectedMerge) return;
    const completed = mergeHistory.find(record =>
      record.database === selectedMerge.database &&
      record.table === selectedMerge.table &&
      record.part_name === selectedMerge.result_part_name &&
      (record.hostname || '') === (selectedMerge.hostname || '')
    );
    if (!completed) return;
    setSelectedMergeHistoryRaw(completed);
    setPreviewMergeHistory(completed);
    selectMerge(null);
  }, [liveSelectedMerge, mergeHistory, selectMerge, selectedMerge]);

  // Client-side filters are URL-driven for shareable links
  const selectedMergeType = urlState.mergeType;
  const selectedMergeReason = historyFilter.category;
  const selectedHost = urlState.host;
  const setSelectedHost = useCallback((v: string[] | undefined) => updateUrl({ host: v }), [updateUrl]);
  const selectedStatus = historyFilter.status;
  const setSelectedStatus = useCallback(
    (v: string[] | undefined) => {
      const includesError = v?.some(status => status.toLowerCase() === 'error') ?? false;
      setHistoryFilter({
        status: v,
        ...(!includesError ? { errorCode: undefined } : {}),
      });
      updateUrl({
        status: v,
        quick: undefined,
        ...(!includesError ? { errorCode: undefined } : {}),
      });
    },
    [setHistoryFilter, updateUrl],
  );
  const selectedQuickFilter = (
    urlState.quick === 'running'
    || urlState.quick === 'recent'
    || urlState.quick === 'failed'
    || urlState.quick === 'slow'
  ) ? urlState.quick as MergeQuickFilter : undefined;
  const setSelectedQuickFilter = useCallback((
    v: MergeQuickFilter | undefined,
    constraints: { status?: string[]; minDurationMs?: number },
  ) => {
    const includesError = constraints.status?.some(
      status => status.toLowerCase() === 'error',
    ) ?? false;
    setHistoryFilter({
      minDurationMs: constraints.minDurationMs,
      status: constraints.status,
      ...(!includesError ? { errorCode: undefined } : {}),
    });
    updateUrl({
      quick: v,
      status: constraints.status,
      minDurMs: constraints.minDurationMs,
      ...(!includesError ? { errorCode: undefined } : {}),
    });
  }, [setHistoryFilter, updateUrl]);
  const selectedPartName = urlState.part;
  const setSelectedPartName = useCallback((v: string | undefined) => updateUrl({ part: v }), [updateUrl]);
  const { hideReplicaMerges, setHideReplicaMerges, experimentalEnabled } = useUserPreferenceStore();
  useEffect(() => {
    if (!experimentalEnabled && activeTab === 'health') {
      updateUrl({ tab: 'merges' });
    }
  }, [activeTab, experimentalEnabled, updateUrl]);

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;
  const { available: hasMerges, probing: isCapProbing } = useCapabilityCheck(['system_merges']);

  // Get services from ClickHouseProvider
  const services = useClickHouseServices();

  const fetchActiveMerges = useCallback(async (isInitialLoad = false) => {
    if (!services || !isConnected || !hasMerges) return;
    // Only show loading state on initial load to prevent flickering during polling
    if (isInitialLoad) {
      setIsLoadingMerges(true);
      clearError();
    }
    try {
      const merges = await mergeApi.fetchActiveMerges(
        services.mergeTracker,
        historyFilter.limit,
      );
      setActiveMerges(merges);
    } catch (err) {
      setError(extractErrorMessage(err, 'Failed to fetch merges'));
    } finally {
      if (isInitialLoad) {
        setIsLoadingMerges(false);
      }
    }
  }, [services, isConnected, hasMerges, historyFilter.limit, setActiveMerges, setIsLoadingMerges, setError, clearError]);

  const fetchMergeHistory = useCallback(async (isInitialLoad = false) => {
    if (!services || !isConnected || !hasMerges) return;
    if (isInitialLoad) {
      setIsLoadingHistory(true);
      clearError();
    }
    try {
      const history = await mergeApi.fetchMergeHistory(services.mergeTracker, historyFilter);
      setMergeHistory(history);
    } catch (err) {
      setError(extractErrorMessage(err, 'Failed to fetch history'));
    } finally {
      if (isInitialLoad) {
        setIsLoadingHistory(false);
      }
    }
  }, [services, isConnected, hasMerges, historyFilter, setMergeHistory, setIsLoadingHistory, setError, clearError]);

  const fetchMutations = useCallback(async (isInitialLoad = false) => {
    if (!services || !isConnected || !hasMerges) return;
    if (isInitialLoad) {
      setIsLoadingMutations(true);
    }
    try {
      const data = await mergeApi.fetchMutations(services.mergeTracker);
      setMutations(data);
    } catch (err) {
      console.error('Failed to fetch mutations:', err);
    } finally {
      if (isInitialLoad) {
        setIsLoadingMutations(false);
      }
    }
  }, [services, isConnected, hasMerges, setMutations, setIsLoadingMutations]);

  const fetchMutationHistory = useCallback(async (isInitialLoad = false) => {
    if (!services || !isConnected || !hasMerges) return;
    if (isInitialLoad) {
      setIsLoadingMutationHistory(true);
    }
    try {
      const data = await mergeApi.fetchMutationHistory(services.mergeTracker, historyFilter);
      setMutationHistory(data);
    } catch (err) {
      console.error('Failed to fetch mutation history:', err);
    } finally {
      if (isInitialLoad) {
        setIsLoadingMutationHistory(false);
      }
    }
  }, [services, isConnected, hasMerges, historyFilter, setMutationHistory, setIsLoadingMutationHistory]);

  // Hydrate store from URL params on mount (URL is source of truth for shared links)
  const urlHydrated = useRef(false);
  useEffect(() => {
    if (urlHydrated.current) return;
    urlHydrated.current = true;
    const patch: Partial<typeof historyFilter> = {};
    if (urlState.database) patch.database = urlState.database;
    if (urlState.table) patch.table = urlState.table;
    if (urlState.category) patch.category = urlState.category;
    if (urlState.status) patch.status = urlState.status;
    if (urlState.errorCode) {
      patch.errorCode = urlState.errorCode
        .map(value => Number(value))
        .filter(value => Number.isInteger(value) && value > 0);
    }
    if (urlState.timeRange && urlState.timeRange !== '1 HOUR') patch.timeRange = urlState.timeRange;
    if (urlState.minDurMs) patch.minDurationMs = urlState.minDurMs;
    if (urlState.minSizeB) patch.minSizeBytes = urlState.minSizeB;
    if (urlState.limit && urlState.limit !== 100) patch.limit = urlState.limit;
    if (urlState.excludeSys !== undefined && !urlState.excludeSys) patch.excludeSystemDatabases = false;
    if (Object.keys(patch).length > 0) setHistoryFilter(patch);
    if (urlState.sortField || urlState.sortDir) {
      setHistorySort({
        field: (urlState.sortField || 'event_time') as typeof historySort.field,
        direction: (urlState.sortDir || 'desc') as typeof historySort.direction,
      });
    }
  }, []);

  // Rehydrate merge detail panel from URL (md_db/md_tbl/md_part)
  const mdHydrated = useRef(false);
  useEffect(() => {
    if (mdHydrated.current || !services || !urlState.md_db || !urlState.md_tbl || !urlState.md_part) return;
    mdHydrated.current = true;
    services.mergeTracker.getMergeHistoryByPartName(urlState.md_db, urlState.md_tbl, urlState.md_part)
      .then(record => {
        if (record) setSelectedMergeHistoryRaw(record);
        else console.warn(`[MergeTracker] No merge found for ${urlState.md_db}.${urlState.md_tbl}.${urlState.md_part}`);
      })
      .catch(err => console.error('[MergeTracker] Failed to fetch merge detail:', err));
  }, [services, urlState.md_db, urlState.md_tbl, urlState.md_part]);

  // Refresh history data when switching to history/mutationHistory tabs
  const setActiveTab = useCallback((tab: MergeTab) => {
    setPreviewMergeHistory(null);
    setPreviewMutationHistory(null);
    setPreviewActiveMutation(null);
    updateUrl({ tab }, { push: true });
    if (tab === 'merges') fetchMergeHistory(false);
    if (tab === 'mutations') fetchMutationHistory(false);
  }, [fetchMergeHistory, fetchMutationHistory, updateUrl]);

  const fetchPoolMetrics = useCallback(async (isInitialLoad = false) => {
    if (!services || !isConnected || !hasMerges) return;
    if (isInitialLoad) {
      setIsLoadingPoolMetrics(true);
    }
    try {
      const data = await mergeApi.fetchPoolMetrics(services.mergeTracker);
      setPoolMetrics(data);
    } catch (err) {
      console.error('Failed to fetch pool metrics:', err);
    } finally {
      if (isInitialLoad) {
        setIsLoadingPoolMetrics(false);
      }
    }
  }, [services, isConnected, hasMerges, setPoolMetrics, setIsLoadingPoolMetrics]);

  // Throughput estimates for the health sunburst — derived from active merge tables
  const [throughputEstimates, setThroughputEstimates] = useState<ThroughputMap>(new Map());
  const activeMergeTables = useMemo(() => {
    const tables = new Set<string>();
    for (const m of activeMerges) tables.add(`${m.database}\t${m.table}`);
    return tables;
  }, [activeMerges]);

  useEffect(() => {
    if (!services || activeMergeTables.size === 0) {
      setThroughputEstimates(new Map());
      return;
    }
    let cancelled = false;
    const map: ThroughputMap = new Map();
    const fetches = [...activeMergeTables].map(key => {
      const [db, tbl] = key.split('\t');
      return services.mergeTracker.getMergeThroughputEstimate(db, tbl)
        .then(estimates => { if (!cancelled) map.set(`${db}.${tbl}`, estimates); })
        .catch(() => {}); // silently skip — deriveHealth falls back to elapsed-based thresholds
    });
    Promise.all(fetches).then(() => { if (!cancelled) setThroughputEstimates(map); });
    return () => { cancelled = true; };
  }, [services, activeMergeTables]);

  const fetchTablesForDatabases = useCallback(async (selectedDatabases: string[]) => {
    if (!services || !isConnected) return;
    try {
      const tableGroups = await Promise.all(selectedDatabases.map(database =>
        databaseApi.fetchTables(services.databaseExplorer, database)
      ));
      setAvailableTables(Array.from(new Set(
        tableGroups.flatMap(tables => tables.map(table => table.name))
      )).sort());
    } catch {
      setAvailableTables([]);
    }
  }, [services, isConnected]);

  useEffect(() => {
    const selectedDatabases = historyFilter.database ?? [];
    if (selectedDatabases.length > 0) {
      fetchTablesForDatabases(selectedDatabases);
    } else {
      setAvailableTables([]);
    }
  }, [fetchTablesForDatabases, historyFilter.database]);

  const handleFilterChange = useCallback((filter: Partial<typeof historyFilter>) => {
    setHistoryFilter(filter);
    // Mirror server-side filter changes to URL
    const urlPatch: Record<string, unknown> = {};
    if ('database' in filter) urlPatch.database = filter.database?.length ? filter.database : undefined;
    if ('table' in filter) urlPatch.table = filter.table?.length ? filter.table : undefined;
    if ('category' in filter) urlPatch.category = filter.category?.length ? filter.category : undefined;
    if ('errorCode' in filter) urlPatch.errorCode = filter.errorCode?.map(String);
    if ('timeRange' in filter) urlPatch.timeRange = filter.timeRange || undefined;
    if ('minDurationMs' in filter) {
      urlPatch.minDurMs = filter.minDurationMs || undefined;
      urlPatch.quick = undefined;
    }
    if ('minSizeBytes' in filter) urlPatch.minSizeB = filter.minSizeBytes || undefined;
    if ('limit' in filter) urlPatch.limit = filter.limit;
    if ('excludeSystemDatabases' in filter) urlPatch.excludeSys = filter.excludeSystemDatabases;
    if (Object.keys(urlPatch).length > 0) updateUrl(urlPatch as any);
  }, [setHistoryFilter, updateUrl]);

  const handleSortChange = useCallback((sort: typeof historySort) => {
    setHistorySort(sort);
    updateUrl({ sortField: sort.field, sortDir: sort.direction });
  }, [setHistorySort, updateUrl]);

  useEffect(() => {
    if (!services || !isConnected) {
      clearAll();
      if (pollingRef.current) clearInterval(pollingRef.current);
      return;
    }
    // Wait for capability probe; don't fire queries if system.merges is inaccessible
    if (isCapProbing || !hasMerges) return;
    // Initial load - show loading states
    fetchActiveMerges(true);
    fetchMergeHistory(true);
    fetchMutations(true);
    fetchMutationHistory(true);
    fetchPoolMetrics(true);
    // Ensure databases are loaded for the filter bar
    if (databases.length === 0) {
      databaseApi.fetchDatabases(services.databaseExplorer)
        .then(dbs => setDatabases(dbs))
        .catch(() => {});
    }
    // Polling - don't show loading states to prevent flickering
    if (refreshRateSeconds > 0) {
      const intervalMs = clampToAllowed(refreshRateSeconds, refreshConfig) * 1000;
      pollingRef.current = setInterval(() => {
        fetchActiveMerges(false);
        fetchMergeHistory(false);
        fetchMutations(false);
        fetchMutationHistory(false);
        fetchPoolMetrics(false);
      }, intervalMs);
    }
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, [services, isConnected, hasMerges, isCapProbing, fetchActiveMerges, fetchMergeHistory, fetchMutations, fetchMutationHistory, fetchPoolMetrics, clearAll, refreshRateSeconds, refreshConfig, manualRefreshTick]);

  useEffect(() => {
    if (services && isConnected && hasMerges) fetchMergeHistory(true);
  }, [historyFilter, services, isConnected, hasMerges, fetchMergeHistory]);

  // Fetch mutation history when filter changes
  useEffect(() => {
    if (services && isConnected && hasMerges) fetchMutationHistory(true);
  }, [historyFilter, services, isConnected, hasMerges, fetchMutationHistory]);

  // Count pending mutations
  const pendingMutations = mutations.filter(m => !m.is_done).length;

  // --- Client-side filtering for all tabs ---
  // Helper: filter by database/table from historyFilter
  const dbFilter = historyFilter.database;
  const tblFilter = historyFilter.table;

  const filteredMergeActivity = useMemo(
    () => filterMergeActivity(mergeActivity, {
      hideReplicaMerges,
      excludeSystemDatabases: historyFilter.excludeSystemDatabases,
      database: dbFilter,
      table: tblFilter,
      liveCategory: selectedMergeType,
      category: selectedMergeReason,
      minDurationMs: historyFilter.minDurationMs,
      minSizeBytes: historyFilter.minSizeBytes,
      status: selectedStatus,
      errorCode: historyFilter.errorCode,
      hostname: selectedHost,
      partName: selectedPartName,
    }),
    [
      mergeActivity,
      hideReplicaMerges,
      historyFilter.excludeSystemDatabases,
      historyFilter.minDurationMs,
      historyFilter.minSizeBytes,
      dbFilter,
      tblFilter,
      selectedMergeType,
      selectedMergeReason,
      selectedStatus,
      historyFilter.errorCode,
      selectedHost,
      selectedPartName,
    ],
  );
  const mergeActivityRecords = useMemo(
    () => limitMergeActivityRecords(
      buildMergeActivityRecords(
        filteredMergeActivity.live,
        filteredMergeActivity.recent,
      ),
      historyFilter.limit,
    ),
    [filteredMergeActivity, historyFilter.limit],
  );

  // Filtered mutations (client-side: database, table)
  const filteredMutations = React.useMemo(() => {
    let result = mutations;
    if (dbFilter?.length) result = result.filter(m => dbFilter.includes(m.database));
    if (tblFilter?.length) result = result.filter(m => tblFilter.includes(m.table));
    return result;
  }, [mutations, dbFilter, tblFilter]);

  // Filtered mutation history (server-side handles db/table/limit, no extra client filter needed)
  const filteredMutationHistory = mutationHistory;

  // Available merge reasons from merge history
  // Use static list so the dropdown is always fully populated (server-side filtering
  // would otherwise strip categories not in the current result set)
  const availableMergeReasons = ALL_MERGE_CATEGORIES as unknown as string[];

  // Available hostnames from active merges + merge history
  const availableHosts = React.useMemo(
    () => mergeActivityHosts(mergeActivity),
    [mergeActivity],
  );

  // These lifecycle states are always available. Terminal outcomes are
  // filtered server-side, so suggestions must not depend on the limited page.
  const availableStatuses = mergeActivityStatuses();
  const errorCodeSuggestions = useMemo(
    () => buildErrorCodeSuggestions(
      mergeHistory,
      record => record.error,
      record => record.exception,
    ),
    [mergeHistory],
  );

  const isHealthTab = activeTab === 'health';
  const poolTotals = useMemo(() => {
    if (!poolMetrics) return { active: 0, total: 0 };
    const pools = [
      [poolMetrics.merge_pool_active, poolMetrics.merge_pool_size],
      [poolMetrics.move_pool_active, poolMetrics.move_pool_size],
      [poolMetrics.fetch_pool_active, poolMetrics.fetch_pool_size],
      [poolMetrics.schedule_pool_active, poolMetrics.schedule_pool_size],
      [poolMetrics.common_pool_active, poolMetrics.common_pool_size],
      [poolMetrics.distributed_pool_active, poolMetrics.distributed_pool_size],
    ];
    return pools.reduce(
      (acc, [active, total]) => ({ active: acc.active + active, total: acc.total + total }),
      { active: 0, total: 0 },
    );
  }, [poolMetrics]);
  const poolUsages: PoolUsage[] = poolMetrics
    ? [
        { label: 'Merge', shortLabel: 'M', active: poolMetrics.merge_pool_active, total: poolMetrics.merge_pool_size, color: '#8b5cf6' },
        { label: 'Move', shortLabel: 'MV', active: poolMetrics.move_pool_active, total: poolMetrics.move_pool_size, color: '#3b82f6' },
        { label: 'Fetch', shortLabel: 'F', active: poolMetrics.fetch_pool_active, total: poolMetrics.fetch_pool_size, color: '#10b981' },
        { label: 'Schedule', shortLabel: 'S', active: poolMetrics.schedule_pool_active, total: poolMetrics.schedule_pool_size, color: '#f59e0b' },
        { label: 'Common', shortLabel: 'C', active: poolMetrics.common_pool_active, total: poolMetrics.common_pool_size, color: '#8b5cf6' },
        { label: 'Distributed', shortLabel: 'D', active: poolMetrics.distributed_pool_active, total: poolMetrics.distributed_pool_size, color: '#ec4899' },
      ]
    : [];

  if (!activeProfileId || !isConnected) {
    return (
      <div style={{ padding: '24px', background: 'var(--bg-primary)', minHeight: '100%' }}>
        <h1 className="text-xl font-semibold mb-6" style={{ color: 'var(--text-primary)' }}>
          Merge Tracker
        </h1>
        <div className="card">
          <NoConnection onConnect={() => setConnectionFormOpen(true)} />
        </div>
      </div>
    );
  }

  // Capability gate — show centered message when system.merges is inaccessible
  if (!isCapProbing && !hasMerges) {
    return (
      <div className="page-layout">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Merge Tracker</h1>
        </div>
        <PermissionGate
          error="Insufficient privileges to access system.merges. Ask your administrator to grant SELECT on this table."
          title="Merge Tracker"
          variant="page"
        />
      </div>
    );
  }

  return (
    <div style={{
      height: '100%',
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
      background: 'var(--bg-primary)',
    }}>
      <div
        style={{
          flexShrink: 0,
          padding: '16px 24px 0',
          background: 'var(--bg-secondary)',
          borderBottom: '1px solid var(--border-primary)',
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between" style={{ marginBottom: 12 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <h1 style={{ margin: 0, color: 'var(--text-primary)', fontSize: 18, fontWeight: 600 }}>
                Merge Tracker
              </h1>
              <DocsLink path="/features/merge-tracker" />
              <BackLink />
            </div>
          </div>
        </div>

        {/* Compact merge vitals */}
        {statistics && (
          <MetricStrip
            ariaLabel="Merge tracker summary"
            style={{
              flexWrap: 'nowrap',
              gap: '8px 12px',
              overflowX: 'auto',
              marginBottom: 12,
            }}
          >
            <PoolUsageSummary
              pools={poolUsages}
              totalActive={poolTotals.active}
              total={poolTotals.total}
              isLoading={isLoadingPoolMetrics}
            />
            <MetricStripDivider />
            <MetricStripItem
              label="active"
              value={statistics.activeMergeCount}
              indicatorColor={statistics.activeMergeCount > 0 ? '#3b82f6' : '#3fb950'}
            />
            <MetricStripItem
              label="stuck"
              value={activeMerges.filter(isMergeStuck).length}
              color={activeMerges.some(isMergeStuck) ? '#f85149' : undefined}
              indicatorColor={activeMerges.some(isMergeStuck) ? '#f85149' : '#3fb950'}
            />
            <MetricStripItem
              label="tables"
              value={statistics.tablesWithMerges.length}
            />
            <MetricStripItem
              label="pending mutations"
              value={pendingMutations}
              color={pendingMutations > 0 ? '#d29922' : undefined}
              indicatorColor={pendingMutations > 0 ? '#d29922' : '#3fb950'}
            />
            <MetricStripDivider />
            <MetricStripItem
              label="throughput"
              value={formatBytesPerSec(
                activeMerges.reduce((sum, merge) => {
                  const bytesProcessed = merge.total_size_bytes_compressed * merge.progress;
                  return sum + (merge.elapsed > 0 ? bytesProcessed / merge.elapsed : 0);
                }, 0)
              )}
            />
            <MetricStripItem
              label="total size"
              value={formatBytes(statistics.totalBytesBeingMerged)}
            />
            <MetricStripItem
              label="avg progress"
              value={`${(statistics.averageProgress * 100).toFixed(1)}%`}
            />
            <MetricStripItem
              label="active parts"
              value={
                isLoadingPoolMetrics
                  ? '—'
                  : poolMetrics?.active_parts && poolMetrics.active_parts > 1000
                    ? `${(poolMetrics.active_parts / 1000).toFixed(1)}K`
                    : poolMetrics?.active_parts ?? 0
              }
            />
            <MetricStripItem
              label="pending cleanup"
              value={isLoadingPoolMetrics ? '—' : poolMetrics?.outdated_parts ?? 0}
              color={(poolMetrics?.outdated_parts ?? 0) > 100 ? '#f59e0b' : undefined}
              indicatorColor={(poolMetrics?.outdated_parts ?? 0) > 100 ? '#f59e0b' : undefined}
            />
          </MetricStrip>
        )}

        <div className="page-tabs">
          <button
            className={`page-tab ${activeTab === 'merges' ? 'active' : ''}`}
            onClick={() => setActiveTab('merges')}
          >
            Merges
            {mergeActivityRecords.length > 0 && (
              <span className="page-tab-count">{mergeActivityRecords.length}</span>
            )}
          </button>
          <button
            className={`page-tab ${activeTab === 'mutations' ? 'active' : ''}`}
            onClick={() => setActiveTab('mutations')}
          >
            Mutations
            {pendingMutations > 0 && (
              <span className="page-tab-count">{pendingMutations}</span>
            )}
          </button>
          {experimentalEnabled && (
            <button
              className={`page-tab ${activeTab === 'health' ? 'active' : ''}`}
              onClick={() => setActiveTab('health')}
            >
              Health Map
              <span className="page-tab-exp">exp</span>
            </button>
          )}
        </div>
      </div>

      {/* Error */}
      {error && (
        <div style={{ margin: '12px 24px 0' }}>
          <PermissionGate error={error} title="Merge Tracker" variant="banner" onDismiss={clearError} />
        </div>
      )}

      {/* Main Content */}
      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflow: 'auto',
          background: 'var(--bg-primary)',
          padding: '0 24px 24px',
        }}
      >
      <div className="flex flex-col min-w-0" style={{ minHeight: isHealthTab ? '520px' : '400px' }}>
          {/* Filter Bar */}
          {!isHealthTab && (
            <div style={{ paddingTop: 12 }}>
              <MergeFilterBar
                tab={activeTab}
                filter={historyFilter}
                onFilterChange={handleFilterChange}
                availableDatabases={databases.map(d => d.name)}
                availableTables={availableTables}
                mergeReasons={availableMergeReasons}
                selectedMergeReason={selectedMergeReason}
                onMergeReasonChange={(reason) => handleFilterChange({ category: reason })}
                availableHosts={availableHosts}
                selectedHost={selectedHost}
                onHostChange={setSelectedHost}
                availableStatuses={availableStatuses}
                errorCodeSuggestions={errorCodeSuggestions}
                selectedStatus={selectedStatus}
                onStatusChange={setSelectedStatus}
                quickFilter={selectedQuickFilter}
                onQuickFilterChange={setSelectedQuickFilter}
                selectedPartName={selectedPartName}
                onPartNameChange={setSelectedPartName}
                excludeSystemDatabases={historyFilter.excludeSystemDatabases}
                onExcludeSystemChange={(v) => handleFilterChange({ excludeSystemDatabases: v })}
                hideReplicaMerges={activeTab === 'merges' ? hideReplicaMerges : undefined}
                onHideReplicaMergesChange={activeTab === 'merges' ? setHideReplicaMerges : undefined}
                onRefresh={activeTab === 'merges'
                  ? () => { fetchActiveMerges(false); fetchMergeHistory(true); }
                  : () => { fetchMutations(false); fetchMutationHistory(true); }}
                isLoading={activeTab === 'merges'
                  ? isLoadingMerges || isLoadingHistory
                  : isLoadingMutations || isLoadingMutationHistory}
              />
            </div>
          )}

          {!isHealthTab && (
            <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: 8 }}>
              <PreviewToggleButton
                label={activeTab === 'mutations' ? 'Mutation Preview' : 'Merge Preview'}
                visible={showActivityPreview}
                onToggle={() => setShowActivityPreview(visible => {
                  const next = !visible;
                  savePreviewPreference(MERGE_ACTIVITY_PREVIEW_STORAGE_KEY, next);
                  return next;
                })}
              />
            </div>
          )}

          {/* Content and optional preview share a row so their top edges align. */}
          <div className="flex gap-4" style={{ paddingTop: 12, alignItems: 'flex-start' }}>
          <div className="flex-1 overflow-auto min-w-0">
            {activeTab === 'health' ? (
              <MergeHealthSunburst
                activeMerges={activeMerges}
                mutations={mutations}
                poolMetrics={poolMetrics}
                throughputEstimates={throughputEstimates}
                onLeafClick={(name, category) => {
                  if (category === 'Mutations') {
                    const mut = mutations.find(m => m.mutation_id === name);
                    if (mut) { setSelectedActiveMutation(mut); return; }
                    return 'not-found';
                  }
                  const merge = activeMerges.find(m => m.result_part_name === name);
                  if (merge) { setActiveMergeDetail(merge); return; }
                  return 'not-found';
                }}
              />
            ) : activeTab === 'merges' ? (
              <>
                {selectedMergeReason?.some(reason => isCategoryClientSideOnly(reason as MergeCategory)) && (
                  <div style={{ padding: '6px 12px', fontSize: 11, color: 'var(--text-warning, #d4a72c)', background: 'var(--bg-warning, rgba(212,167,44,0.08))', borderRadius: 4, margin: '0 0 6px' }}>
                    This category is filtered client-side after the LIMIT — results may be incomplete. Increase the limit to see more rows.
                  </div>
                )}
                <div style={{ border: '1px solid var(--border-primary)', borderRadius: 8, overflow: 'hidden' }}>
                  <MergeActivityTable
                    activity={mergeActivityRecords}
                    sort={historySort}
                    onSortChange={handleSortChange}
                    isLoading={isLoadingMerges || isLoadingHistory}
                    selectedLiveMerge={liveSelectedMerge}
                    selectedHistoryRecord={previewedMergeHistory}
                    onSelectLive={openActiveMergeDetails}
                    onPreviewLive={(merge) => {
                      setSelectedMergeHistoryRaw(null);
                      setPreviewMergeHistory(null);
                      selectMerge(merge);
                    }}
                    onSelectHistory={openMergeHistoryDetails}
                    onPreviewHistory={(record) => {
                      selectMerge(null);
                      setPreviewMergeHistory(record);
                    }}
                  />
                </div>
              </>
            ) : (
              <>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '2px 2px 6px' }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>Running</span>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{filteredMutations.length}</span>
                </div>
                <div style={{ border: '1px solid var(--border-primary)', borderRadius: 8, overflow: 'hidden' }}>
                  <MutationsPanel
                    mutations={filteredMutations}
                    activeMerges={activeMerges}
                    isLoading={isLoadingMutations}
                    selectedMutation={previewedActiveMutation}
                    onSelectMutation={(mutation) => {
                      setSelectedMutationHistory(null);
                      setPreviewMutationHistory(null);
                      setSelectedActiveMutation(mutation);
                    }}
                    onPreviewMutation={(mutation) => {
                      setPreviewMutationHistory(null);
                      setPreviewActiveMutation(mutation);
                    }}
                  />
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '18px 2px 6px' }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)' }}>Recent</span>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{filteredMutationHistory.length}</span>
                </div>
                <div style={{ border: '1px solid var(--border-primary)', borderRadius: 8, overflow: 'hidden' }}>
                  <MutationHistoryPanel
                    history={filteredMutationHistory}
                    isLoading={isLoadingMutationHistory}
                    selectedRecord={previewedMutationHistory}
                    onSelectRecord={(record) => {
                      setSelectedActiveMutation(null);
                      setPreviewActiveMutation(null);
                      setSelectedMutationHistory(record);
                    }}
                    onPreviewRecord={(record) => {
                      setPreviewActiveMutation(null);
                      setPreviewMutationHistory(record);
                    }}
                  />
                </div>
              </>
            )}
          </div>

        {/* Right Panel - Detail */}
        {!isHealthTab && showActivityPreview && (
          <div
            className="w-80 flex-shrink-0 card overflow-hidden"
            style={{ position: 'sticky', top: 12, zIndex: 2 }}
          >
            {activeTab === 'merges' ? (
              liveSelectedMerge ? (
                <MergeDetailPanel merge={liveSelectedMerge} onClose={() => selectMerge(null)} onOpenFullDetails={setActiveMergeDetail} />
              ) : (
                <MergeHistoryDetailPanel
                  record={previewedMergeHistory}
                  onClose={() => {
                    setSelectedMergeHistory(null);
                    setPreviewMergeHistory(null);
                  }}
                  onOpenFullDetails={setMergeDetailRecord}
                />
              )
            ) : previewedActiveMutation ? (
              <ActiveMutationDetailPanel
                mutation={previewedActiveMutation}
                activeMerges={activeMerges}
                allMutations={mutations}
                onClose={() => {
                  setSelectedActiveMutation(null);
                  setPreviewActiveMutation(null);
                }}
              />
            ) : (
              <MutationHistoryDetailPanel
                record={previewedMutationHistory}
                onClose={() => {
                  setSelectedMutationHistory(null);
                  setPreviewMutationHistory(null);
                }}
              />
            )}
          </div>
        )}
        </div>
      </div>
      </div>

      {/* Full merge detail modal */}
      <MergeDetailModalFromRecord record={mergeDetailRecord} onClose={() => setMergeDetailRecord(null)} />
      <ActiveMergeDetailModal merge={activeMergeDetail} onClose={() => setActiveMergeDetail(null)} />
    </div>
  );
};

export default MergeTrackerView;
