/**
 * TimelineQueryModal — Rich query detail modal used by TimeTravelPage,
 * QueryMonitor, and OrderingKeyTable.
 *
 * Extracted from QueryDetailModal.tsx which was growing too large.
 * Contains: Overview, Details (Performance/Objects/Functions/Settings),
 * Anatomy, History, Logs, Spans, Flamegraph, Pipeline, and Threads tabs.
 */

import React, { useEffect, useCallback, useState } from 'react';
import type { QuerySeries, TraceLog, OpenTelemetrySpan, FlamegraphNode } from '@tracehouse/core';
import type { QueryDetail as QueryDetailType, SimilarQuery, SubQueryInfo, QueryThreadBreakdown } from '@tracehouse/core';
import type { FlamegraphType } from '@tracehouse/core';
import { extractLiterals, diffLiterals, formatLiteral } from '@tracehouse/core';
import type { LiteralDiff } from '@tracehouse/core';
import { ThreadBreakdownSection } from './QueryDetail';
import { QueryScanEfficiency } from './QueryScanEfficiency';
import { ColumnCostAnalysis } from './ColumnCostAnalysis';
import { TraceLogViewer } from '../tracing/TraceLogViewer';
import { Flamegraph } from '../tracing/Flamegraph';
import { PipelineProfileTab } from '../tracing/PipelineProfileTab';
import { formatBytes } from '../../stores/databaseStore';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { useProfileEventDescriptionsStore } from '../../stores/profileEventDescriptionsStore';
import { ModalWrapper, MetricItem } from '../shared/ModalWrapper';
import { QueryComparisonPanel } from './QueryComparisonPanel';
import type { ComparableQuery } from './QueryComparisonPanel';
import { SqlHighlight } from '../common/SqlHighlight';
import { PROFILE_EVENT_CATEGORIES } from './profileEventCategories';
import { QueryXRay3D } from './QueryXRay3D';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';

export interface TimelineQueryModalProps {
  /** The query from timeline data (null to hide modal) */
  query: QuerySeries | null;
  /** Called when modal should close */
  onClose: () => void;
}

type QueryModalTab = 'overview' | 'details' | 'analytics' | 'history' | 'logs' | 'spans' | 'flamegraph' | 'pipeline' | 'threads' | 'xray';
type DetailsSubTab = 'performance' | 'objects' | 'functions' | 'settings';
type AnalyticsSubTab = 'scan_efficiency' | 'column_cost';

/**
 * OpenTelemetry Spans Viewer component - Work in Progress
 */
const SpansViewer: React.FC<{
  spans: OpenTelemetrySpan[];
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
}> = () => {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      height: '100%',
      minHeight: 300,
      padding: 40,
    }}>
      <div style={{ textAlign: 'center', maxWidth: 400 }}>
        <div style={{
          fontSize: 16,
          fontWeight: 500,
          color: 'var(--text-secondary)',
          marginBottom: 8,
        }}>
          Not Yet Available
        </div>
        <div style={{
          fontSize: 13,
          color: 'var(--text-muted)',
          lineHeight: 1.6,
        }}>
          OpenTelemetry span visualization is not yet implemented.
        </div>
      </div>
    </div>
  );
};


/**
 * Performance Tab - ProfileEvents breakdown
 */
const PerformanceTab: React.FC<{
  profileEvents: Record<string, number> | undefined;
  isLoading: boolean;
}> = ({ profileEvents, isLoading }) => {
  const [filter, setFilter] = useState('');
  const [hideZero, setHideZero] = useState(true);
  const descriptions = useProfileEventDescriptionsStore((s) => s.descriptions);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: 24, height: 24,
            borderWidth: 2,
            borderStyle: 'solid',
            borderColor: 'var(--border-primary)',
            borderTopColor: 'var(--text-tertiary)',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite',
            margin: '0 auto 8px',
          }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading performance data...</span>
        </div>
      </div>
    );
  }

  if (!profileEvents || Object.keys(profileEvents).length === 0) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <div style={{ fontSize: 14, color: 'var(--text-tertiary)' }}>No ProfileEvents data available</div>
      </div>
    );
  }

  const lowerFilter = filter.toLowerCase();

  // Group events by category
  const categorizedEvents: Record<string, { event: string; value: number }[]> = {};
  const uncategorized: { event: string; value: number }[] = [];
  let totalCount = 0;
  let shownCount = 0;

  Object.entries(profileEvents).forEach(([event, value]) => {
    if (hideZero && value === 0) return;
    totalCount++;
    if (lowerFilter && !event.toLowerCase().includes(lowerFilter)) return;
    shownCount++;
    let found = false;
    for (const [cat, config] of Object.entries(PROFILE_EVENT_CATEGORIES)) {
      if (config.events.some(e => event.includes(e) || e.includes(event))) {
        if (!categorizedEvents[cat]) categorizedEvents[cat] = [];
        categorizedEvents[cat].push({ event, value });
        found = true;
        break;
      }
    }
    if (!found) uncategorized.push({ event, value });
  });

  // Sort categorized events to match the order defined in PROFILE_EVENT_CATEGORIES
  for (const [cat, config] of Object.entries(PROFILE_EVENT_CATEGORIES)) {
    if (categorizedEvents[cat]) {
      const order = config.events;
      categorizedEvents[cat].sort((a, b) => {
        const ai = order.findIndex(e => a.event === e || a.event.includes(e) || e.includes(a.event));
        const bi = order.findIndex(e => b.event === e || b.event.includes(e) || e.includes(b.event));
        const aIdx = ai === -1 ? 999 : ai;
        const bIdx = bi === -1 ? 999 : bi;
        if (aIdx !== bIdx) return aIdx - bIdx;
        return a.event.localeCompare(b.event);
      });
    }
  }

  const fmtValue = (event: string, value: number) => {
    if (event.includes('Bytes')) return formatBytes(value);
    if (event.includes('Microseconds')) return value >= 1000000 ? `${(value / 1000000).toFixed(2)}s` : value >= 1000 ? `${(value / 1000).toFixed(1)}ms` : `${value}µs`;
    if (event.includes('Nanoseconds')) return value >= 1000000000 ? `${(value / 1000000000).toFixed(2)}s` : value >= 1000000 ? `${(value / 1000000).toFixed(1)}ms` : value >= 1000 ? `${(value / 1000).toFixed(1)}µs` : `${value}ns`;
    return value.toLocaleString();
  };

  return (
    <div style={{ padding: 20, overflow: 'auto', height: '100%' }}>
      {/* Filter bar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
        <input
          type="text"
          placeholder="Filter events..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            flex: 1,
            maxWidth: 300,
            padding: '6px 10px',
            fontSize: 12,
            fontFamily: 'monospace',
            borderRadius: 6,
            border: '1px solid var(--border-primary)',
            background: 'var(--bg-code)',
            color: 'var(--text-primary)',
            outline: 'none',
          }}
        />
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: 'var(--text-muted)', cursor: 'pointer' }}>
          <input
            type="checkbox"
            checked={hideZero}
            onChange={(e) => setHideZero(e.target.checked)}
            style={{ accentColor: '#58a6ff' }}
          />
          Hide zero
        </label>
        <span style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 'auto' }}>
          {shownCount}{filter ? ` / ${totalCount}` : ''} events
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}>
        {Object.entries(PROFILE_EVENT_CATEGORIES).map(([cat, config]) => {
          const events = categorizedEvents[cat] || [];
          if (events.length === 0) return null;
          return (
            <div key={cat} style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: config.color, textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 8, height: 8, borderRadius: '50%', background: config.color }} />
                {config.label}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {events.sort((a, b) => b.value - a.value).map(({ event, value }) => (
                  <div key={event} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 11, color: 'var(--text-tertiary)', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }} title={descriptions[event] || event}>{event}</span>
                    <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace' }}>{fmtValue(event, value)}</span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
      {uncategorized.length > 0 && (
        <div style={{ marginTop: 16, background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12 }}>
            Other Events ({uncategorized.length})
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 8 }}>
            {uncategorized.sort((a, b) => b.value - a.value).map(({ event, value }) => (
              <div key={event} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }} title={descriptions[event] || event}>{event}</span>
                <span style={{ fontSize: 11, color: 'var(--text-tertiary)', fontFamily: 'monospace' }}>{fmtValue(event, value)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * Objects Tab - databases, tables, columns, partitions, views
 */
const ObjectsTab: React.FC<{
  queryDetail: QueryDetailType | null;
  isLoading: boolean;
}> = ({ queryDetail, isLoading }) => {
  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 24, height: 24, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading...</span>
        </div>
      </div>
    );
  }

  if (!queryDetail) {
    return <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>No data available</div>;
  }

  const sections = [
    { label: 'Databases', items: queryDetail.databases, color: 'var(--color-info)' },
    { label: 'Tables', items: queryDetail.tables, color: 'var(--color-success)' },
    { label: 'Columns', items: queryDetail.columns, color: 'var(--color-warning)' },
    { label: 'Partitions', items: queryDetail.partitions, color: 'var(--color-memory)' },
    { label: 'Views', items: queryDetail.views, color: '#f0883e' },
    { label: 'Projections', items: queryDetail.projections ? [queryDetail.projections] : [], color: '#f778ba' },
  ];

  return (
    <div style={{ padding: 20, overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}>
        {sections.map(({ label, items, color }) => (
          <div key={label} style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 600, color, textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: color }} />
              {label} ({items?.length || 0})
            </div>
            {items && items.length > 0 ? (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {items.slice(0, 20).map((item, i) => (
                  <span key={i} style={{ fontSize: 11, fontFamily: 'monospace', padding: '4px 8px', background: 'var(--bg-code)', borderRadius: 4, color: 'var(--text-secondary)' }}>{item}</span>
                ))}
                {items.length > 20 && <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>+{items.length - 20} more</span>}
              </div>
            ) : (
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>None</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

/**
 * Functions Tab - used functions, aggregates, table functions, etc.
 */
const FunctionsTab: React.FC<{
  queryDetail: QueryDetailType | null;
  isLoading: boolean;
}> = ({ queryDetail, isLoading }) => {
  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 24, height: 24, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading...</span>
        </div>
      </div>
    );
  }

  if (!queryDetail) {
    return <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>No data available</div>;
  }

  const sections = [
    { label: 'Functions', items: queryDetail.used_functions, color: '#58a6ff' },
    { label: 'Aggregate Functions', items: queryDetail.used_aggregate_functions, color: 'var(--color-success)' },
    { label: 'Aggregate Combinators', items: queryDetail.used_aggregate_function_combinators, color: 'var(--color-info)' },
    { label: 'Table Functions', items: queryDetail.used_table_functions, color: 'var(--color-warning)' },
    { label: 'Storages', items: queryDetail.used_storages, color: 'var(--color-memory)' },
    { label: 'Formats', items: queryDetail.used_formats, color: '#f0883e' },
    { label: 'Dictionaries', items: queryDetail.used_dictionaries, color: '#f778ba' },
  ];

  return (
    <div style={{ padding: 20, overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}>
        {sections.map(({ label, items, color }) => (
          <div key={label} style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 600, color, textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: color }} />
              {label} ({items?.length || 0})
            </div>
            {items && items.length > 0 ? (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {items.map((item, i) => (
                  <span key={i} style={{ fontSize: 11, fontFamily: 'monospace', padding: '4px 8px', background: 'var(--bg-code)', borderRadius: 4, color: 'var(--text-secondary)' }}>{item}</span>
                ))}
              </div>
            ) : (
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>None</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

/**
 * Settings Tab - query settings and cache usage
 */
const SettingsTab: React.FC<{
  queryDetail: QueryDetailType | null;
  isLoading: boolean;
  onFetchDefaults: (settingNames: string[]) => Promise<Record<string, { default: string; description: string }>>;
}> = ({ queryDetail, isLoading, onFetchDefaults }) => {
  const [filter, setFilter] = useState('');
  const [defaults, setDefaults] = useState<Record<string, { default: string; description: string }>>({});
  const [isLoadingDefaults, setIsLoadingDefaults] = useState(false);

  // Fetch defaults when queryDetail changes
  useEffect(() => {
    if (queryDetail?.Settings && Object.keys(queryDetail.Settings).length > 0 && Object.keys(defaults).length === 0 && !isLoadingDefaults) {
      setIsLoadingDefaults(true);
      onFetchDefaults(Object.keys(queryDetail.Settings))
        .then(setDefaults)
        .catch(() => {/* ignore errors */ })
        .finally(() => setIsLoadingDefaults(false));
    }
  }, [queryDetail?.Settings, defaults, isLoadingDefaults, onFetchDefaults]);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 24, height: 24, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading...</span>
        </div>
      </div>
    );
  }

  if (!queryDetail) {
    return <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>No data available</div>;
  }

  const settings = queryDetail.Settings || {};
  const filteredSettings = Object.entries(settings).filter(([key]) =>
    filter === '' || key.toLowerCase().includes(filter.toLowerCase())
  );

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header with cache info */}
      <div style={{ padding: 16, borderBottom: '1px solid var(--border-secondary)', background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Query Cache</span>
            <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>{queryDetail.query_cache_usage || 'None'}</div>
          </div>
          <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Overridden Settings</span>
            <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>{Object.keys(settings).length}</div>
          </div>
        </div>
        <input
          type="text"
          placeholder="Filter settings..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{ width: '100%', padding: '8px 12px', fontSize: 12, borderRadius: 6, border: '1px solid var(--border-primary)', background: 'var(--bg-code)', color: 'var(--text-primary)' }}
        />
      </div>
      {/* Settings table */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {filteredSettings.length === 0 ? (
          <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>
            {Object.keys(settings).length === 0 ? 'No settings were overridden for this query' : 'No settings match filter'}
          </div>
        ) : (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: 'var(--bg-card)', borderBottom: '1px solid var(--border-secondary)' }}>
                <th style={{ padding: '10px 16px', textAlign: 'left', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Setting</th>
                <th style={{ padding: '10px 16px', textAlign: 'right', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', width: 150 }}>Value</th>
                <th style={{ padding: '10px 16px', textAlign: 'right', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', width: 150 }}>Default</th>
              </tr>
            </thead>
            <tbody>
              {filteredSettings.map(([key, value], i) => {
                const defaultInfo = defaults[key];
                const isChanged = defaultInfo && defaultInfo.default !== value;
                return (
                  <tr key={key} style={{ borderBottom: '1px solid var(--border-secondary)', background: i % 2 === 0 ? 'transparent' : 'var(--bg-card)' }} title={defaultInfo?.description}>
                    <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)' }}>{key}</td>
                    <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontSize: 11, color: isChanged ? 'var(--color-warning)' : '#58a6ff', textAlign: 'right', fontWeight: 500 }} title={value}>{value}</td>
                    <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)', textAlign: 'right' }}>
                      {isLoadingDefaults ? '...' : (defaultInfo?.default ?? '—')}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

/**
 * Similar Queries Tab - queries with same normalized_query_hash
 * Shows timeline chart and table to spot performance patterns over time
 */
type ChartMetric = 'duration' | 'cpu' | 'memory' | 'rows' | 'status';

const METRIC_COLORS: Record<ChartMetric, string> = {
  duration: '#58a6ff',
  cpu: '#f59e0b',
  memory: '#10b981',
  rows: '#3b82f6',
  status: '#f85149',
};

// Calculate percentile value from sorted array
const percentile = (sortedArr: number[], p: number): number => {
  if (sortedArr.length === 0) return 0;
  if (sortedArr.length === 1) return sortedArr[0];
  const index = (p / 100) * (sortedArr.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sortedArr[lower];
  return sortedArr[lower] + (sortedArr[upper] - sortedArr[lower]) * (index - lower);
};

const QUERY_KIND_COLORS: Record<string, string> = {
  SELECT: '#3b82f6', INSERT: '#f59e0b', ALTER: '#ef4444',
  CREATE: '#22c55e', DROP: '#f43f5e', SYSTEM: '#8b5cf6', OPTIMIZE: '#06b6d4',
};

const QueryKindDot: React.FC<{ kind: string }> = ({ kind }) => {
  const k = (kind || '').toUpperCase();
  const color = QUERY_KIND_COLORS[k] || '#94a3b8';
  return (
    <span
      title={kind || 'Unknown'}
      style={{
        display: 'inline-block',
        width: 8, height: 8, borderRadius: '50%',
        background: color,
        boxShadow: `0 0 4px ${color}60`,
        flexShrink: 0,
      }}
    />
  );
};

const HistoryTab: React.FC<{
  similarQueries: SimilarQuery[];
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
  cpuTimeline?: { t: string; cpu_pct: number }[];
  memTimeline?: { t: string; mem_pct: number }[];
  isLoadingCpu?: boolean;
  limit: number;
  onLimitChange: (limit: number) => void;
  onSelectQuery?: (query: SimilarQuery) => void;
  hashMode: 'normalized' | 'exact';
  onHashModeChange: (mode: 'normalized' | 'exact') => void;
  currentQueryId?: string;
}> = ({ similarQueries, isLoading, error, onRefresh, cpuTimeline, memTimeline, limit, onLimitChange, onSelectQuery, hashMode, onHashModeChange, currentQueryId }) => {
  const [selectedMetrics, setSelectedMetrics] = useState<Set<ChartMetric>>(new Set(['duration']));
  const [hoveredPoint, setHoveredPoint] = useState<number | null>(null);
  const [hoveredEvent, setHoveredEvent] = useState<number | null>(null);
  const [eventTooltipPos, setEventTooltipPos] = useState<{ x: number; y: number } | null>(null);
  const [scaleMode, setScaleMode] = useState<'minmax' | 'percentile' | 'p5p95'>('minmax');
  const [showCpuOverlay, setShowCpuOverlay] = useState(false);
  const [showMemOverlay, setShowMemOverlay] = useState(false);
  const [showSettingsEvents, setShowSettingsEvents] = useState(true);
  const [selectedForCompare, setSelectedForCompare] = useState<Set<number>>(new Set());
  const [compareMode, setCompareMode] = useState(false);
  const [settingsPopover, setSettingsPopover] = useState<{ idx: number; x: number; y: number } | null>(null);

  // Sort state
  type SortKey = 'query_id' | 'time' | 'duration' | 'cpu' | 'memory' | 'rows' | 'server' | null;
  const [sortKey, setSortKey] = useState<SortKey>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir(key === 'time' || key === 'query_id' ? 'asc' : 'desc');
    }
  };

  // Zoom state — time range filter
  const [zoomRange, setZoomRange] = useState<{ start: number; end: number } | null>(null);
  const [brushStart, setBrushStart] = useState<number | null>(null); // SVG x coordinate
  const [brushEnd, setBrushEnd] = useState<number | null>(null);
  const svgRef = React.useRef<SVGSVGElement>(null);

  // Filter queries by zoom range
  const visibleQueries = React.useMemo(() => {
    if (!zoomRange) return similarQueries;
    return similarQueries.filter(q => {
      const t = new Date(q.query_start_time).getTime();
      return t >= zoomRange.start && t <= zoomRange.end;
    });
  }, [similarQueries, zoomRange]);

  // Map from visibleQueries index back to similarQueries index (for hover sync with table)
  const visibleToOriginalIdx = React.useMemo(() => {
    if (!zoomRange) return similarQueries.map((_, i) => i);
    const map: number[] = [];
    similarQueries.forEach((q, i) => {
      const t = new Date(q.query_start_time).getTime();
      if (t >= zoomRange.start && t <= zoomRange.end) map.push(i);
    });
    return map;
  }, [similarQueries, zoomRange]);

  // Sort the display queries
  const sortedQueries = React.useMemo(() => {
    const base = zoomRange ? visibleQueries : similarQueries;
    if (!sortKey) return base;
    const sorted = [...base].sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case 'query_id': cmp = a.query_id.localeCompare(b.query_id); break;
        case 'time': cmp = new Date(a.query_start_time).getTime() - new Date(b.query_start_time).getTime(); break;
        case 'duration': cmp = Number(a.query_duration_ms) - Number(b.query_duration_ms); break;
        case 'cpu': cmp = (Number(a.cpu_time_us) || 0) - (Number(b.cpu_time_us) || 0); break;
        case 'memory': cmp = Number(a.memory_usage) - Number(b.memory_usage); break;
        case 'rows': cmp = (Number(a.result_rows) || 0) - (Number(b.result_rows) || 0); break;
        case 'server': cmp = (a.client_hostname || '').localeCompare(b.client_hostname || ''); break;
      }
      return sortDir === 'asc' ? cmp : -cmp;
    });
    return sorted;
  }, [similarQueries, visibleQueries, zoomRange, sortKey, sortDir]);

  // Compute settings change events — compare each query's Settings to the previous one
  const settingsEvents = React.useMemo(() => {
    const events: { idx: number; changes: { key: string; from: string; to: string }[] }[] = [];
    for (let i = 1; i < similarQueries.length; i++) {
      const prev = similarQueries[i - 1].Settings || {};
      const curr = similarQueries[i].Settings || {};
      const allKeys = new Set([...Object.keys(prev), ...Object.keys(curr)]);
      const changes: { key: string; from: string; to: string }[] = [];
      for (const key of allKeys) {
        const prevVal = prev[key] ?? '(default)';
        const currVal = curr[key] ?? '(default)';
        if (prevVal !== currVal) {
          changes.push({ key, from: prevVal, to: currVal });
        }
      }
      if (changes.length > 0) {
        events.push({ idx: i, changes });
      }
    }
    return events;
  }, [similarQueries]);

  // Build a lookup: query index → settings changes from previous query
  const settingsChangesByIdx = React.useMemo(() => {
    const map = new Map<number, { key: string; from: string; to: string }[]>();
    for (const evt of settingsEvents) {
      map.set(evt.idx, evt.changes);
    }
    return map;
  }, [settingsEvents]);

  // Compute literal diffs — compare each query's SQL literals against the first execution
  // Since all queries share the same normalized_query_hash, the structure is identical
  // but literal values (strings, numbers, dates, IN-lists) may differ
  const literalDiffsByIdx = React.useMemo(() => {
    const map = new Map<number, LiteralDiff[]>();
    if (similarQueries.length < 2) return map;
    const refSql = similarQueries[0].query;
    if (!refSql) return map;
    // refLiterals intentionally unused — extractLiterals called for side-effect validation
    void extractLiterals(refSql);
    for (let i = 1; i < similarQueries.length; i++) {
      const sql = similarQueries[i].query;
      if (!sql) continue;
      const diffs = diffLiterals(refSql, sql);
      if (diffs.length > 0) map.set(i, diffs);
    }
    return map;
  }, [similarQueries]);

  // Check if any queries have literal differences (to decide whether to show the column)
  const hasAnyLiteralDiffs = literalDiffsByIdx.size > 0;

  const toggleMetric = (metric: ChartMetric, e: React.MouseEvent) => {
    const newSet = new Set(selectedMetrics);
    if (e.metaKey || e.ctrlKey) {
      // Multi-select: toggle the clicked metric
      if (newSet.has(metric)) {
        if (newSet.size > 1) newSet.delete(metric); // Keep at least one
      } else {
        newSet.add(metric);
      }
    } else {
      // Single select: replace with just this metric
      newSet.clear();
      newSet.add(metric);
    }
    setSelectedMetrics(newSet);
  };

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 24, height: 24, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading similar queries...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: 20 }}>
        <div style={{ padding: 16, borderRadius: 8, background: 'rgba(var(--color-error-rgb), 0.1)', border: '1px solid rgba(var(--color-error-rgb), 0.2)' }}>
          <div style={{ fontWeight: 500, color: 'var(--color-error)', marginBottom: 4 }}>Error loading similar queries</div>
          <div style={{ fontSize: 12, color: 'var(--text-tertiary)' }}>{error}</div>
        </div>
      </div>
    );
  }

  if (similarQueries.length === 0) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <div style={{ fontSize: 14, color: 'var(--text-tertiary)', marginBottom: 12 }}>No similar queries found</div>
        <button onClick={onRefresh} style={{ padding: '6px 16px', fontSize: 12, borderRadius: 6, border: '1px solid var(--border-primary)', background: 'var(--bg-card)', color: 'var(--text-tertiary)', cursor: 'pointer' }}>Retry</button>
      </div>
    );
  }

  const fmtMs = (ms: number) => ms < 1000 ? `${Number(ms.toFixed(2))}ms` : ms < 60000 ? `${(ms / 1000).toFixed(2)}s` : `${(ms / 60000).toFixed(2)}m`;
  const fmtUs = (us: number) => us < 1000 ? `${us}µs` : us < 1000000 ? `${(us / 1000).toFixed(1)}ms` : `${(us / 1000000).toFixed(2)}s`;
  const fmtTimeShort = (ts: string) => {
    const d = new Date(ts);
    return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
  };

  // Calculate stats - coerce to numbers in case they come as strings
  // Stats use visibleQueries so they update when zoomed
  const durations = visibleQueries.map(q => Number(q.query_duration_ms));
  const avgDuration = durations.reduce((sum, d) => sum + d, 0) / durations.length;

  // Standard deviation for duration
  const durationVariance = durations.reduce((sum, d) => sum + Math.pow(d - avgDuration, 2), 0) / durations.length;
  const stdDevDuration = Math.sqrt(durationVariance);

  // Stats for other metrics (for deviation indicators — use full dataset for z-scores)
  const cpuTimes = similarQueries.map(q => Number(q.cpu_time_us) || 0);
  const avgCpu = cpuTimes.reduce((sum, c) => sum + c, 0) / cpuTimes.length;
  const stdDevCpu = Math.sqrt(cpuTimes.reduce((sum, c) => sum + Math.pow(c - avgCpu, 2), 0) / cpuTimes.length);

  const memories = similarQueries.map(q => Number(q.memory_usage));
  const avgMemory = memories.reduce((sum, m) => sum + m, 0) / memories.length;
  const stdDevMemory = Math.sqrt(memories.reduce((sum, m) => sum + Math.pow(m - avgMemory, 2), 0) / memories.length);

  // Duration deviation uses full dataset for table z-scores
  const allDurations = similarQueries.map(q => Number(q.query_duration_ms));
  const allAvgDuration = allDurations.reduce((sum, d) => sum + d, 0) / allDurations.length;
  const allStdDevDuration = Math.sqrt(allDurations.reduce((sum, d) => sum + Math.pow(d - allAvgDuration, 2), 0) / allDurations.length);

  const errorCount = visibleQueries.filter(q => q.exception_code !== 0).length;

  // Helper to get deviation color and indicator
  // Returns how many std devs from mean (clamped to -2 to +2 for display)
  const getDeviation = (value: number, avg: number, stdDev: number): { zScore: number; color: string; indicator: string } => {
    if (stdDev === 0) return { zScore: 0, color: 'var(--text-muted)', indicator: '' };
    const zScore = (value - avg) / stdDev;
    const clampedZ = Math.max(-2, Math.min(2, zScore));

    // Color based on how far from mean (for duration/cpu/memory, higher = worse)
    let color = 'var(--text-muted)';
    let indicator = '';
    if (zScore > 1.5) { color = 'var(--color-error)'; indicator = '▲▲'; }
    else if (zScore > 0.75) { color = 'var(--color-warning)'; indicator = '▲'; }
    else if (zScore < -1.5) { color = 'var(--color-success)'; indicator = '▼▼'; }
    else if (zScore < -0.75) { color = '#3fb950'; indicator = '▼'; }

    return { zScore: clampedZ, color, indicator };
  };

  // Get value for chart based on metric
  const getValue = (q: SimilarQuery, metric: ChartMetric): number => {
    switch (metric) {
      case 'duration': return Number(q.query_duration_ms) || 0;
      case 'cpu': return Number(q.cpu_time_us) || 0;
      case 'memory': return Number(q.memory_usage) || 0;
      case 'rows': return Number(q.result_rows) || 0;
      case 'status': return q.exception_code !== 0 ? 1 : 0;
    }
  };

  const formatValue = (v: number, metric: ChartMetric): string => {
    switch (metric) {
      case 'duration': return fmtMs(v);
      case 'cpu': return fmtUs(v);
      case 'memory': return formatBytes(v);
      case 'rows': return v.toLocaleString();
      case 'status': return v === 1 ? 'Error' : 'OK';
    }
  };

  // Get scale range for a metric based on scale mode
  const getScaleRange = (metric: ChartMetric): { min: number; max: number } => {
    const values = visibleQueries.map(q => getValue(q, metric));
    if (values.length === 0) return { min: 0, max: 1 };

    if (scaleMode === 'p5p95' && values.length >= 5 && metric !== 'status') {
      const sorted = [...values].sort((a, b) => a - b);
      const p5 = percentile(sorted, 5);
      const p95 = percentile(sorted, 95);
      if (p5 === p95) {
        return { min: Math.min(...values), max: Math.max(...values, 1) };
      }
      return { min: p5, max: p95 };
    }

    return { min: Math.min(...values), max: Math.max(...values, 1) };
  };

  // Compute percentile reference values for a metric (used in 'percentile' scale mode)
  const getPercentileLines = (metric: ChartMetric): { label: string; p: number; value: number }[] => {
    const values = visibleQueries.map(q => getValue(q, metric));
    if (values.length < 4 || metric === 'status') return [];
    const sorted = [...values].sort((a, b) => a - b);
    return [
      { label: 'p50', p: 50, value: percentile(sorted, 50) },
      { label: 'p95', p: 95, value: percentile(sorted, 95) },
      { label: 'p99', p: 99, value: percentile(sorted, 99) },
    ];
  };

  // Chart dimensions
  const chartHeight = 150;
  const chartPadding = { top: 20, right: 55, bottom: 30, left: 60 };

  const metricButtons: { key: ChartMetric; label: string }[] = [
    { key: 'duration', label: 'Duration' },
    { key: 'cpu', label: 'CPU' },
    { key: 'memory', label: 'Memory' },
    { key: 'rows', label: 'Rows' },
    { key: 'status', label: 'Status' },
  ];

  const selectedMetricsArray = Array.from(selectedMetrics);

  // Current query visibility checks
  const currentQueryInData = currentQueryId != null && similarQueries.some(q => q.query_id === currentQueryId);
  const currentQueryInView = currentQueryId != null && visibleQueries.some(q => q.query_id === currentQueryId);

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', position: 'relative' }}>
      {/* Banner: current query not visible */}
      {currentQueryId != null && !currentQueryInData && similarQueries.length > 0 && (
        <div style={{
          padding: '8px 16px',
          background: 'rgba(245, 158, 11, 0.1)',
          borderBottom: '1px solid rgba(245, 158, 11, 0.2)',
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          fontSize: 12,
          color: '#f59e0b',
        }}>
          <span style={{ fontSize: 14 }}>&#9888;</span>
          <span>The query you're viewing is not among the latest {limit} executions. Increase the "Show" limit to find it.</span>
        </div>
      )}
      {currentQueryId != null && currentQueryInData && !currentQueryInView && zoomRange && (
        <div style={{
          padding: '8px 16px',
          background: 'rgba(88, 166, 255, 0.08)',
          borderBottom: '1px solid rgba(88, 166, 255, 0.15)',
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          fontSize: 12,
          color: '#58a6ff',
        }}>
          <span style={{ fontSize: 14 }}>&#8505;</span>
          <span>The query you're viewing is outside the zoomed range.</span>
          <button
            onClick={() => setZoomRange(null)}
            style={{
              padding: '2px 8px', fontSize: 11, borderRadius: 4,
              border: '1px solid rgba(88, 166, 255, 0.3)', background: 'transparent',
              color: '#58a6ff', cursor: 'pointer',
            }}
          >Reset zoom</button>
        </div>
      )}
      {/* Stats header */}
      <div style={{ padding: 16, borderBottom: '1px solid var(--border-secondary)', background: 'var(--bg-card)', display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
          <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Executions</span>
          <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>
            {visibleQueries.length}
            {!zoomRange && visibleQueries.length >= limit && (
              <span
                style={{ fontSize: 9, color: 'var(--text-muted)', fontWeight: 400, marginLeft: 4 }}
              >(latest {limit})</span>
            )}
            {zoomRange && (
              <span style={{ fontSize: 9, color: 'var(--text-muted)', fontWeight: 400, marginLeft: 4 }}>
                of {similarQueries.length}
              </span>
            )}
          </div>
        </div>
        <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
          <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Avg Duration</span>
          <div style={{ fontSize: 13, fontFamily: 'monospace', color: '#58a6ff', marginTop: 4 }}>{fmtMs(avgDuration)}</div>
        </div>
        <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }} title="Standard deviation - lower means more consistent">
          <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Std Dev</span>
          <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>±{fmtMs(stdDevDuration)}</div>
        </div>

        <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }} title="p50 / p95 / p99 percentiles">
          <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>p50 / p95 / p99</span>
          <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>
            {fmtMs(percentile([...durations].sort((a, b) => a - b), 50))}
            <span style={{ color: 'var(--text-muted)' }}> / </span>
            <span style={{ color: '#f59e0b' }}>{fmtMs(percentile([...durations].sort((a, b) => a - b), 95))}</span>
            <span style={{ color: 'var(--text-muted)' }}> / </span>
            <span style={{ color: 'var(--color-error)' }}>{fmtMs(percentile([...durations].sort((a, b) => a - b), 99))}</span>
          </div>
        </div>
        <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
          <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Errors</span>
          <div style={{ fontSize: 13, fontFamily: 'monospace', color: errorCount > 0 ? 'var(--color-error)' : 'var(--color-success)', marginTop: 4 }}>{errorCount}</div>
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>Match:</span>
            <div className="tabs" style={{ padding: 2 }}>
              {(['normalized', 'exact'] as const).map(m => (
                <button
                  key={m}
                  className={`tab${hashMode === m ? ' active' : ''}`}
                  onClick={() => onHashModeChange(m)}
                  title={m === 'normalized'
                    ? 'Groups queries with same structure but different literal values'
                    : 'Only byte-identical SQL statements'}
                  style={{ border: 'none', padding: '4px 12px', fontSize: 11 }}
                >
                  {m === 'normalized' ? 'Normalized' : 'Exact'}
                </button>
              ))}
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>Show:</span>
            <select
              value={limit}
              onChange={(e) => onLimitChange(Number(e.target.value))}
              style={{
                padding: '4px 8px',
                fontSize: 11,
                fontFamily: 'monospace',
                borderRadius: 4,
                border: '1px solid var(--border-primary)',
                background: 'var(--bg-code)',
                color: 'var(--text-primary)',
                cursor: 'pointer',
              }}
            >
              {[25, 50, 100, 250, 500].map(n => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Timeline Chart */}
      <div style={{ padding: 16, borderBottom: '1px solid var(--border-secondary)', position: 'relative' }}>
        {/* Metric selector */}
        <div style={{ display: 'flex', gap: 8, marginBottom: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          {metricButtons.map(({ key, label }) => {
            const isSelected = selectedMetrics.has(key);
            const color = METRIC_COLORS[key];
            return (
              <button
                key={key}
                onClick={(e) => toggleMetric(key, e)}
                style={{
                  padding: '4px 12px',
                  fontSize: 11,
                  borderRadius: 4,
                  border: isSelected ? `1px solid ${color}` : '1px solid var(--border-primary)',
                  background: isSelected ? `${color}20` : 'transparent',
                  color: isSelected ? color : 'var(--text-muted)',
                  cursor: 'pointer',
                  fontWeight: isSelected ? 500 : 400,
                  transition: 'all 0.15s ease',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                }}
              >
                {isSelected && <span style={{ width: 8, height: 8, borderRadius: '50%', background: color }} />}
                {label}
              </button>
            );
          })}
          <span style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 8 }}>
            {navigator.platform.includes('Mac') ? '⌘' : 'Ctrl'}+click to compare
          </span>
          {cpuTimeline && cpuTimeline.length >= 2 && (
            <button
              onClick={() => setShowCpuOverlay(!showCpuOverlay)}
              style={{
                fontSize: 10,
                marginLeft: 8,
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                opacity: showCpuOverlay ? 0.9 : 0.4,
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                padding: '2px 4px',
                borderRadius: 4,
                color: '#ef4444',
                transition: 'opacity 0.15s ease',
              }}
              title={showCpuOverlay ? 'Hide server CPU overlay' : 'Show server CPU overlay'}
            >
              <svg width="20" height="6"><line x1="0" y1="3" x2="20" y2="3" stroke="#ef4444" strokeWidth="1.5" strokeDasharray="4,3" strokeOpacity={showCpuOverlay ? 1 : 0.4} /></svg>
              Server CPU
            </button>
          )}
          {memTimeline && memTimeline.length >= 2 && (
            <button
              onClick={() => setShowMemOverlay(!showMemOverlay)}
              style={{
                fontSize: 10,
                marginLeft: 4,
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                opacity: showMemOverlay ? 0.9 : 0.4,
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                padding: '2px 4px',
                borderRadius: 4,
                color: '#8b5cf6',
                transition: 'opacity 0.15s ease',
              }}
              title={showMemOverlay ? 'Hide server memory overlay' : 'Show server memory overlay'}
            >
              <svg width="20" height="6"><line x1="0" y1="3" x2="20" y2="3" stroke="#8b5cf6" strokeWidth="1.5" strokeDasharray="4,3" strokeOpacity={showMemOverlay ? 1 : 0.4} /></svg>
              Server Mem
            </button>
          )}
          {settingsEvents.length > 0 && (
            <button
              onClick={() => setShowSettingsEvents(!showSettingsEvents)}
              style={{
                fontSize: 10,
                marginLeft: 4,
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                opacity: showSettingsEvents ? 0.9 : 0.4,
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                padding: '2px 4px',
                borderRadius: 4,
                color: '#f59e0b',
                transition: 'opacity 0.15s ease',
              }}
              title={showSettingsEvents ? `Hide settings change events (${settingsEvents.length})` : `Show settings change events (${settingsEvents.length})`}
            >
              <svg width="14" height="10"><circle cx="7" cy="5" r="3.5" fill="none" stroke="#f59e0b" strokeWidth="1.5" strokeOpacity={showSettingsEvents ? 1 : 0.4} /><circle cx="7" cy="5" r="1.2" fill="#f59e0b" fillOpacity={showSettingsEvents ? 1 : 0.4} /></svg>
              Settings ({settingsEvents.length})
            </button>
          )}
          <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
            {(['percentile', 'p5p95'] as const).map(mode => {
              const labels = { percentile: 'p50 p95 p99', p5p95: 'P5–P95' };
              const titles = { percentile: 'Show p50/p95/p99 reference lines', p5p95: 'Zoom to P5–P95 range, reduces outlier impact' };
              const isActive = scaleMode === mode;
              return (
                <button
                  key={mode}
                  onClick={() => setScaleMode(isActive ? 'minmax' : mode)}
                  style={{
                    padding: '3px 8px',
                    fontSize: 10,
                    borderRadius: 4,
                    border: '1px solid var(--border-primary)',
                    background: isActive ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
                    color: isActive ? '#58a6ff' : 'var(--text-muted)',
                    cursor: 'pointer',
                    transition: 'all 0.15s',
                  }}
                  title={titles[mode]}
                >
                  {labels[mode]}
                </button>
              );
            })}
          </div>
        </div>

        {/* Zoom indicator */}
        {zoomRange && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
              Zoomed: {fmtTimeShort(new Date(zoomRange.start).toISOString())} – {fmtTimeShort(new Date(zoomRange.end).toISOString())}
              <span style={{ marginLeft: 4 }}>({visibleQueries.length} queries)</span>
            </span>
            <button
              onClick={() => setZoomRange(null)}
              style={{
                padding: '2px 8px',
                fontSize: 10,
                borderRadius: 4,
                border: '1px solid var(--border-primary)',
                background: 'transparent',
                color: 'var(--text-muted)',
                cursor: 'pointer',
              }}
            >
              Reset
            </button>
          </div>
        )}

        {/* SVG Chart */}
        <svg
          ref={svgRef}
          width="100%" height={chartHeight + chartPadding.top + chartPadding.bottom}
          style={{ overflow: 'visible', cursor: brushStart !== null ? 'col-resize' : 'crosshair' }}
          viewBox="0 0 800 200"
          preserveAspectRatio="xMidYMid meet"
          onMouseDown={(e) => {
            if (!svgRef.current) return;
            const pt = svgRef.current.createSVGPoint();
            pt.x = e.clientX; pt.y = e.clientY;
            const svgX = Math.max(chartPadding.left, Math.min(800 - chartPadding.right, pt.matrixTransform(svgRef.current.getScreenCTM()!.inverse()).x));
            setBrushStart(svgX);
            setBrushEnd(svgX);
          }}
          onMouseMove={(e) => {
            if (brushStart === null || !svgRef.current) return;
            const pt = svgRef.current.createSVGPoint();
            pt.x = e.clientX; pt.y = e.clientY;
            const svgX = Math.max(chartPadding.left, Math.min(800 - chartPadding.right, pt.matrixTransform(svgRef.current.getScreenCTM()!.inverse()).x));
            setBrushEnd(svgX);
          }}
          onMouseUp={() => {
            if (brushStart !== null && brushEnd !== null && Math.abs(brushEnd - brushStart) > 5) {
              // Convert SVG x coordinates to timestamps
              const chartWidth = 800 - chartPadding.left - chartPadding.right;
              const queryTimes = visibleQueries.map(q => new Date(q.query_start_time).getTime());
              const tMin = queryTimes[0] ?? 0;
              const tMax = queryTimes[queryTimes.length - 1] ?? 0;
              const tRange = tMax - tMin || 1;
              const x1 = Math.min(brushStart, brushEnd);
              const x2 = Math.max(brushStart, brushEnd);
              const t1 = tMin + ((x1 - chartPadding.left) / chartWidth) * tRange;
              const t2 = tMin + ((x2 - chartPadding.left) / chartWidth) * tRange;
              setZoomRange({ start: t1, end: t2 });
            }
            setBrushStart(null);
            setBrushEnd(null);
          }}
          onMouseLeave={() => {
            setBrushStart(null);
            setBrushEnd(null);
          }}
        >
          {/* Brush selection overlay */}
          {brushStart !== null && brushEnd !== null && Math.abs(brushEnd - brushStart) > 2 && (
            <rect
              x={Math.min(brushStart, brushEnd)}
              y={chartPadding.top}
              width={Math.abs(brushEnd - brushStart)}
              height={chartHeight}
              fill="#58a6ff"
              fillOpacity={0.1}
              stroke="#58a6ff"
              strokeWidth={0.5}
              strokeOpacity={0.4}
            />
          )}
          {/* Grid lines - softer */}
          <line x1={chartPadding.left} y1={chartPadding.top} x2={800 - chartPadding.right} y2={chartPadding.top} stroke="var(--border-secondary)" strokeOpacity={0.5} />
          {scaleMode !== 'percentile' && (
            <line x1={chartPadding.left} y1={chartPadding.top + chartHeight / 2} x2={800 - chartPadding.right} y2={chartPadding.top + chartHeight / 2} stroke="var(--border-secondary)" strokeOpacity={0.3} strokeDasharray="4,4" />
          )}
          <line x1={chartPadding.left} y1={chartPadding.top + chartHeight} x2={800 - chartPadding.right} y2={chartPadding.top + chartHeight} stroke="var(--border-secondary)" strokeOpacity={0.5} />

          {/* Percentile reference lines — only in 'percentile' mode with a single metric */}
          {scaleMode === 'percentile' && selectedMetricsArray.length === 1 && (() => {
            const metric = selectedMetricsArray[0];
            const pLines = getPercentileLines(metric);
            if (pLines.length === 0) return null;
            const { min: minVal, max: maxVal } = getScaleRange(metric);
            const pColors: Record<string, string> = { p50: '#3fb950', p95: '#f97316', p99: '#ef4444' };
            return pLines.map(({ label, value }) => {
              if (maxVal === minVal) return null;
              const norm = (value - minVal) / (maxVal - minVal);
              const y = chartPadding.top + chartHeight - (Math.max(0, Math.min(1, norm)) * chartHeight);
              const color = pColors[label] || 'var(--text-muted)';
              return (
                <g key={label}>
                  <line
                    x1={chartPadding.left}
                    y1={y}
                    x2={800 - chartPadding.right}
                    y2={y}
                    stroke={color}
                    strokeWidth={0.75}
                    strokeDasharray="6,4"
                    strokeOpacity={0.5}
                  />
                  <text
                    x={800 - chartPadding.right + 4}
                    y={y}
                    fontSize={8}
                    fill={color}
                    fillOpacity={0.8}
                    dominantBaseline="middle"
                    fontFamily="ui-monospace, monospace"
                  >
                    {label}
                  </text>
                  <text
                    x={chartPadding.left - 8}
                    y={y}
                    fontSize={8}
                    fill={color}
                    fillOpacity={0.8}
                    textAnchor="end"
                    dominantBaseline="middle"
                    fontFamily="ui-monospace, monospace"
                  >
                    {formatValue(value, metric)}
                  </text>
                </g>
              );
            });
          })()}

          {/* Y-axis labels: show "0%" and "100%" for normalized view when multiple metrics */}
          <text x={chartPadding.left - 8} y={chartPadding.top} fontSize={9} fill="var(--text-muted)" textAnchor="end" dominantBaseline="middle">
            {selectedMetricsArray.length > 1 ? '100%' : (scaleMode === 'percentile' ? '' : formatValue(getScaleRange(selectedMetricsArray[0]).max, selectedMetricsArray[0]))}
          </text>
          <text x={chartPadding.left - 8} y={chartPadding.top + chartHeight} fontSize={9} fill="var(--text-muted)" textAnchor="end" dominantBaseline="middle">
            {selectedMetricsArray.length > 1 ? '0%' : (scaleMode === 'percentile' ? '' : formatValue(getScaleRange(selectedMetricsArray[0]).min, selectedMetricsArray[0]))}
          </text>

          {/* Render a line for each selected metric */}
          {selectedMetricsArray.map(metric => {
            const color = METRIC_COLORS[metric];
            const { min: minVal, max: maxVal } = getScaleRange(metric);
            const chartWidth = 800 - chartPadding.left - chartPadding.right;

            // Use time-based X positioning for proper spacing
            const queryTimes = visibleQueries.map(q => new Date(q.query_start_time).getTime());
            const tMin = queryTimes[0];
            const tMax = queryTimes[queryTimes.length - 1];
            const tRange = tMax - tMin || 1;

            const getX = (idx: number) => {
              if (visibleQueries.length === 1) return chartPadding.left + chartWidth / 2;
              return chartPadding.left + ((queryTimes[idx] - tMin) / tRange) * chartWidth;
            };

            // Normalize and clamp value to 0-1 range
            const normalize = (value: number) => {
              if (maxVal === minVal) return 0.5;
              const norm = (value - minVal) / (maxVal - minVal);
              return Math.max(0, Math.min(1, norm));
            };

            // Build all points
            const allPoints = visibleQueries.map((q, i) => ({
              x: getX(i),
              y: chartPadding.top + chartHeight - (normalize(getValue(q, metric)) * chartHeight),
              idx: i,
            }));

            // Simple polyline path — no spline interpolation
            const linePath = allPoints.length >= 2
              ? 'M' + allPoints.map(p => `${p.x},${p.y}`).join(' L')
              : '';

            // Only show dot circles when density allows
            const showDots = visibleQueries.length <= 80;

            return (
              <g key={metric}>
                {/* Line — skip for status since colors vary per point */}
                {metric !== 'status' && allPoints.length >= 2 && (
                  <path
                    d={linePath}
                    fill="none"
                    stroke={color}
                    strokeWidth={1.5}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                )}
                {/* Dots when sparse enough */}
                {showDots && visibleQueries.map((q, i) => {
                  const origIdx = visibleToOriginalIdx[i];
                  const isHovered = hoveredPoint === origIdx;
                  const isCurrent = currentQueryId != null && q.query_id === currentQueryId;
                  const value = getValue(q, metric);
                  const normalized = normalize(value);
                  const isOutlier = normalized <= 0 || normalized >= 1;
                  const pointColor = metric === 'status'
                    ? (q.exception_code !== 0 ? '#f85149' : '#3fb950')
                    : color;
                  return (
                    <g key={`${metric}-point-${i}`}>
                      {isCurrent && (
                        <circle
                          cx={allPoints[i].x}
                          cy={allPoints[i].y}
                          r={8}
                          fill="none"
                          stroke={pointColor}
                          strokeWidth={1.5}
                          strokeOpacity={0.5}
                        />
                      )}
                      <circle
                        cx={allPoints[i].x}
                        cy={allPoints[i].y}
                        r={isHovered ? 5 : isCurrent ? 4.5 : (isOutlier && scaleMode === 'p5p95' ? 4 : 3)}
                        fill={pointColor}
                        stroke={isHovered || isCurrent ? 'var(--bg-primary)' : 'none'}
                        strokeWidth={2}
                        style={{ cursor: 'pointer' }}
                        onMouseEnter={() => setHoveredPoint(origIdx)}
                        onMouseLeave={() => setHoveredPoint(null)}
                      />
                    </g>
                  );
                })}
                {/* Hovered point highlight when dots are hidden */}
                {!showDots && hoveredPoint !== null && (() => {
                  // Find the visible index for the hovered original index
                  const visIdx = visibleToOriginalIdx.indexOf(hoveredPoint);
                  if (visIdx === -1) return null;
                  const value = getValue(visibleQueries[visIdx], metric);
                  const normalized = normalize(value);
                  const pointColor = metric === 'status'
                    ? (visibleQueries[visIdx].exception_code !== 0 ? '#f85149' : '#3fb950')
                    : color;
                  return (
                    <circle
                      cx={getX(visIdx)}
                      cy={chartPadding.top + chartHeight - (normalized * chartHeight)}
                      r={5}
                      fill={pointColor}
                      stroke="var(--bg-primary)"
                      strokeWidth={2}
                    />
                  );
                })()}
                {/* Current query marker — always visible even when dots are hidden */}
                {!showDots && currentQueryId != null && (() => {
                  const visIdx = visibleQueries.findIndex(q => q.query_id === currentQueryId);
                  if (visIdx === -1) return null;
                  const value = getValue(visibleQueries[visIdx], metric);
                  const normalized = normalize(value);
                  const pointColor = metric === 'status'
                    ? (visibleQueries[visIdx].exception_code !== 0 ? '#f85149' : '#3fb950')
                    : color;
                  const cx = getX(visIdx);
                  const cy = chartPadding.top + chartHeight - (Math.max(0, Math.min(1, normalized)) * chartHeight);
                  return (
                    <g>
                      <circle cx={cx} cy={cy} r={8} fill="none" stroke={pointColor} strokeWidth={1.5} strokeOpacity={0.5} />
                      <circle cx={cx} cy={cy} r={4.5} fill={pointColor} stroke="var(--bg-primary)" strokeWidth={2} />
                    </g>
                  );
                })()}
              </g>
            );
          })}

          {/* Invisible hover zones when many points */}
          {visibleQueries.length > 80 && (() => {
            const chartWidth = 800 - chartPadding.left - chartPadding.right;
            const queryTimes = visibleQueries.map(q => new Date(q.query_start_time).getTime());
            const tMin = queryTimes[0];
            const tMax = queryTimes[queryTimes.length - 1];
            const tRange = tMax - tMin || 1;
            return visibleQueries.map((_, i) => {
              const cx = chartPadding.left + ((queryTimes[i] - tMin) / tRange) * chartWidth;
              const origIdx = visibleToOriginalIdx[i];
              return (
                <rect
                  key={`hover-${i}`}
                  x={cx - 3}
                  y={chartPadding.top}
                  width={6}
                  height={chartHeight}
                  fill="transparent"
                  style={{ cursor: 'pointer' }}
                  onMouseEnter={() => setHoveredPoint(origIdx)}
                  onMouseLeave={() => setHoveredPoint(null)}
                />
              );
            });
          })()}

          {/* CPU Load overlay - dotted line */}
          {showCpuOverlay && cpuTimeline && cpuTimeline.length >= 2 && visibleQueries.length >= 2 && (() => {
            const queryTimes = visibleQueries.map(q => new Date(q.query_start_time).getTime());
            const chartWidth = 800 - chartPadding.left - chartPadding.right;
            const qTimeMin = queryTimes[0];
            const qTimeMax = queryTimes[queryTimes.length - 1];
            const qTimeRange = qTimeMax - qTimeMin || 1;

            // CPU data is already aggregated to ~100 buckets server-side
            const cpuPoints = cpuTimeline
              .map(p => {
                const t = new Date(p.t).getTime();
                const x = chartPadding.left + ((t - qTimeMin) / qTimeRange) * chartWidth;
                const y = chartPadding.top + chartHeight - (Math.min(p.cpu_pct, 100) / 100) * chartHeight;
                return { x: Math.max(chartPadding.left, Math.min(800 - chartPadding.right, x)), y, pct: p.cpu_pct };
              });

            if (cpuPoints.length < 2) return null;

            const cpuPath = 'M' + cpuPoints.map(p => `${p.x},${p.y}`).join(' L');

            return (
              <g>
                <path
                  d={cpuPath}
                  fill="none"
                  stroke="#ef4444"
                  strokeWidth={1.5}
                  strokeDasharray="4,3"
                  strokeOpacity={0.5}
                  strokeLinecap="round"
                />
                <text
                  x={800 - chartPadding.right + 4}
                  y={cpuPoints[cpuPoints.length - 1]?.y ?? chartPadding.top}
                  fontSize={8}
                  fill="#ef4444"
                  fillOpacity={0.7}
                  dominantBaseline="middle"
                >
                  CPU {cpuPoints[cpuPoints.length - 1]?.pct.toFixed(0)}%
                </text>
              </g>
            );
          })()}

          {/* Memory Load overlay - dotted line */}
          {showMemOverlay && memTimeline && memTimeline.length >= 2 && visibleQueries.length >= 2 && (() => {
            const queryTimes = visibleQueries.map(q => new Date(q.query_start_time).getTime());
            const chartWidth = 800 - chartPadding.left - chartPadding.right;
            const qTimeMin = queryTimes[0];
            const qTimeMax = queryTimes[queryTimes.length - 1];
            const qTimeRange = qTimeMax - qTimeMin || 1;

            const memPoints = memTimeline
              .map(p => {
                const t = new Date(p.t).getTime();
                const x = chartPadding.left + ((t - qTimeMin) / qTimeRange) * chartWidth;
                const y = chartPadding.top + chartHeight - (Math.min(p.mem_pct, 100) / 100) * chartHeight;
                return { x: Math.max(chartPadding.left, Math.min(800 - chartPadding.right, x)), y, pct: p.mem_pct };
              });

            if (memPoints.length < 2) return null;

            const memPath = 'M' + memPoints.map(p => `${p.x},${p.y}`).join(' L');

            return (
              <g>
                <path
                  d={memPath}
                  fill="none"
                  stroke="#8b5cf6"
                  strokeWidth={1.5}
                  strokeDasharray="4,3"
                  strokeOpacity={0.5}
                  strokeLinecap="round"
                />
                <text
                  x={800 - chartPadding.right + 4}
                  y={memPoints[memPoints.length - 1]?.y ?? chartPadding.top}
                  fontSize={8}
                  fill="#8b5cf6"
                  fillOpacity={0.7}
                  dominantBaseline="middle"
                >
                  Mem {memPoints[memPoints.length - 1]?.pct.toFixed(0)}%
                </text>
              </g>
            );
          })()}

          {/* Settings change event markers */}
          {showSettingsEvents && settingsEvents.length > 0 && (() => {
            const chartWidth = 800 - chartPadding.left - chartPadding.right;
            const queryTimes = visibleQueries.map(q => new Date(q.query_start_time).getTime());
            const tMin = queryTimes[0];
            const tMax = queryTimes[queryTimes.length - 1];
            const tRange = tMax - tMin || 1;
            const getX = (origIdx: number) => {
              const t = new Date(similarQueries[origIdx].query_start_time).getTime();
              if (visibleQueries.length === 1) return chartPadding.left + chartWidth / 2;
              return chartPadding.left + ((t - tMin) / tRange) * chartWidth;
            };
            // Only show events whose original index is within the visible set
            const visibleOrigSet = new Set(visibleToOriginalIdx);
            return settingsEvents.filter(evt => visibleOrigSet.has(evt.idx)).map((evt, evtIdx) => {
              const x = getX(evt.idx);
              const isHovered = hoveredEvent === evtIdx;
              return (
                <g key={`settings-event-${evtIdx}`}>
                  {/* Subtle vertical line */}
                  <line
                    x1={x} y1={chartPadding.top + 2} x2={x} y2={chartPadding.top + chartHeight}
                    stroke="#f59e0b"
                    strokeWidth={0.75}
                    strokeDasharray="2,4"
                    strokeOpacity={isHovered ? 0.6 : 0.25}
                  />
                  {/* Small circle marker at top */}
                  <circle
                    cx={x} cy={chartPadding.top - 4}
                    r={isHovered ? 5 : 4}
                    fill={isHovered ? '#f59e0b' : 'var(--bg-code)'}
                    stroke="#f59e0b"
                    strokeWidth={1.5}
                    strokeOpacity={isHovered ? 1 : 0.6}
                    style={{ cursor: 'pointer', transition: 'all 0.1s ease' }}
                  />
                  {/* Inner dot */}
                  <circle
                    cx={x} cy={chartPadding.top - 4}
                    r={1.5}
                    fill="#f59e0b"
                    fillOpacity={isHovered ? 1 : 0.5}
                    style={{ pointerEvents: 'none' }}
                  />
                  {/* Invisible hover zone — tracks mouse position for HTML tooltip */}
                  <rect
                    x={x - 10} y={chartPadding.top - 12} width={20} height={chartHeight + 14}
                    fill="transparent"
                    style={{ cursor: 'pointer' }}
                    onMouseEnter={(e) => {
                      setHoveredEvent(evtIdx);
                      const svgEl = (e.target as SVGElement).closest('svg');
                      if (svgEl) {
                        const parentRect = svgEl.parentElement?.getBoundingClientRect();
                        if (parentRect) {
                          setEventTooltipPos({
                            x: e.clientX - parentRect.left,
                            y: e.clientY - parentRect.top,
                          });
                        }
                      }
                    }}
                    onMouseMove={(e) => {
                      const svgEl = (e.target as SVGElement).closest('svg');
                      if (svgEl) {
                        const parentRect = svgEl.parentElement?.getBoundingClientRect();
                        if (parentRect) {
                          setEventTooltipPos({
                            x: e.clientX - parentRect.left,
                            y: e.clientY - parentRect.top,
                          });
                        }
                      }
                    }}
                    onMouseLeave={() => {
                      setHoveredEvent(null);
                      setEventTooltipPos(null);
                    }}
                  />
                </g>
              );
            });
          })()}

          {/* Tooltip for hovered point */}
          {hoveredPoint !== null && (
            <g style={{ pointerEvents: 'none' }}>
              {(() => {
                const q = similarQueries[hoveredPoint];
                if (!q) return null;
                const chartWidth = 800 - chartPadding.left - chartPadding.right;
                // Time-based X positioning matching the chart (use visible range)
                const queryTimes = visibleQueries.map(sq => new Date(sq.query_start_time).getTime());
                const tMin = queryTimes[0];
                const tMax = queryTimes[queryTimes.length - 1];
                const tRange = tMax - tMin || 1;
                const qTime = new Date(q.query_start_time).getTime();
                const xNum = visibleQueries.length === 1
                  ? chartPadding.left + chartWidth / 2
                  : chartPadding.left + ((qTime - tMin) / tRange) * chartWidth;
                // Find nearest CPU value for this query's timestamp
                let cpuAtPoint: number | null = null;
                if (showCpuOverlay && cpuTimeline && cpuTimeline.length > 0) {
                  const qTime = new Date(q.query_start_time).getTime();
                  let bestDist = Infinity;
                  for (const cp of cpuTimeline) {
                    const d = Math.abs(new Date(cp.t).getTime() - qTime);
                    if (d < bestDist) { bestDist = d; cpuAtPoint = cp.cpu_pct; }
                  }
                }
                // Find nearest memory value for this query's timestamp
                let memAtPoint: number | null = null;
                if (showMemOverlay && memTimeline && memTimeline.length > 0) {
                  const qTime = new Date(q.query_start_time).getTime();
                  let bestDist = Infinity;
                  for (const mp of memTimeline) {
                    const d = Math.abs(new Date(mp.t).getTime() - qTime);
                    if (d < bestDist) { bestDist = d; memAtPoint = mp.mem_pct; }
                  }
                }
                const hasCpu = cpuAtPoint !== null;
                const hasMem = memAtPoint !== null;
                const tooltipHeight = 20 + selectedMetricsArray.length * 14 + (hasCpu ? 14 : 0) + (hasMem ? 14 : 0);
                // Clamp tooltip position to stay within chart bounds
                const tooltipX = Math.max(70, Math.min(xNum, 800 - 70));
                return (
                  <>
                    <rect
                      x={tooltipX - 70}
                      y={chartPadding.top - 5}
                      width={140}
                      height={tooltipHeight}
                      rx={4}
                      fill="var(--bg-primary)"
                      stroke="var(--border-primary)"
                    />
                    <text x={tooltipX} y={chartPadding.top + 8} fontSize={9} fill="var(--text-muted)" textAnchor="middle">
                      {fmtTimeShort(q.query_start_time)}
                    </text>
                    {selectedMetricsArray.map((metric, idx) => (
                      <text
                        key={metric}
                        x={tooltipX}
                        y={chartPadding.top + 22 + idx * 14}
                        fontSize={10}
                        fill={METRIC_COLORS[metric]}
                        textAnchor="middle"
                      >
                        {metric}: {formatValue(getValue(q, metric), metric)}
                      </text>
                    ))}
                    {hasCpu && (
                      <text
                        x={tooltipX}
                        y={chartPadding.top + 22 + selectedMetricsArray.length * 14}
                        fontSize={10}
                        fill="#ef4444"
                        textAnchor="middle"
                      >
                        server cpu: {cpuAtPoint!.toFixed(1)}%
                      </text>
                    )}
                    {hasMem && (
                      <text
                        x={tooltipX}
                        y={chartPadding.top + 22 + selectedMetricsArray.length * 14 + (hasCpu ? 14 : 0)}
                        fontSize={10}
                        fill="#8b5cf6"
                        textAnchor="middle"
                      >
                        server mem: {memAtPoint!.toFixed(1)}%
                      </text>
                    )}
                  </>
                );
              })()}
            </g>
          )}

          {/* X-axis time labels */}
          {visibleQueries.length > 0 && (
            <>
              <text x={chartPadding.left} y={chartPadding.top + chartHeight + 16} fontSize={9} fill="var(--text-muted)" textAnchor="start">
                {fmtTimeShort(visibleQueries[0].query_start_time)}
              </text>
              <text x={800 - chartPadding.right} y={chartPadding.top + chartHeight + 16} fontSize={9} fill="var(--text-muted)" textAnchor="end">
                {fmtTimeShort(visibleQueries[visibleQueries.length - 1].query_start_time)}
              </text>
            </>
          )}
        </svg>

        {/* HTML tooltip for settings events — positioned near mouse */}
        {hoveredEvent !== null && eventTooltipPos && settingsEvents[hoveredEvent] && (
          <div
            style={{
              position: 'absolute',
              left: eventTooltipPos.x,
              top: eventTooltipPos.y,
              transform: 'translate(-50%, -100%) translateY(-12px)',
              zIndex: 30,
              background: 'var(--bg-primary)',
              border: '1px solid rgba(245, 158, 11, 0.3)',
              borderRadius: 6,
              padding: '6px 10px',
              pointerEvents: 'none',
              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.25)',
              minWidth: 160,
              maxWidth: 260,
            }}
          >
            <div style={{ fontSize: 9, fontWeight: 600, color: '#f59e0b', marginBottom: 4, letterSpacing: '0.3px' }}>
              Settings changed
            </div>
            {settingsEvents[hoveredEvent].changes.map((c, ci) => (
              <div key={ci} style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-tertiary)', lineHeight: '16px', whiteSpace: 'nowrap' }}>
                <span style={{ color: 'var(--text-muted)' }}>{c.key}:</span>{' '}
                <span style={{ color: 'var(--color-error)', textDecoration: 'line-through', opacity: 0.6 }}>{c.from}</span>{' '}
                <span style={{ color: 'var(--text-muted)' }}>→</span>{' '}
                <span style={{ color: 'var(--color-success)' }}>{c.to}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Compare mode bar — fixed above scroll area */}
      <div style={{
        padding: '8px 16px',
        borderBottom: '1px solid var(--border-secondary)',
        background: 'var(--bg-card)',
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        flexShrink: 0,
      }}>
        <button
          onClick={() => {
            if (compareMode) {
              setCompareMode(false);
              setSelectedForCompare(new Set());
            } else {
              setCompareMode(true);
            }
          }}
          style={{
            padding: '5px 14px',
            fontSize: 11,
            borderRadius: 5,
            border: compareMode ? '1px solid #58a6ff' : '1px solid var(--border-primary)',
            background: compareMode ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
            color: compareMode ? '#58a6ff' : 'var(--text-muted)',
            cursor: 'pointer',
            fontWeight: compareMode ? 600 : 400,
            transition: 'all 0.15s',
          }}
        >
          {compareMode ? 'Cancel Compare' : '⇄ Compare Queries'}
        </button>
        {compareMode && (
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            {selectedForCompare.size === 0
              ? 'Select 2 or more queries to compare'
              : `${selectedForCompare.size} selected`}
          </span>
        )}
      </div>
      {/* Fixed header row */}
      <table style={{ width: '100%', borderCollapse: 'collapse', flexShrink: 0, tableLayout: 'fixed' }}>
        <colgroup>
          <col style={{ width: '3%' }} />   {/* Type dot */}
          <col style={{ width: '7%' }} />   {/* Query ID */}
          <col style={{ width: '8%' }} />   {/* Time */}
          <col style={{ width: '9%' }} />   {/* Duration */}
          <col style={{ width: '9%' }} />   {/* CPU */}
          <col style={{ width: '9%' }} />   {/* Memory */}
          <col style={{ width: '6%' }} />   {/* Rows */}
          <col style={{ width: '10%' }} />  {/* Server */}
          <col style={{ width: '3%' }} />   {/* Status icon */}
          <col style={{ width: '9%' }} />   {/* Settings */}
          <col style={{ width: '13%' }} />  {/* Changes */}
          {hasAnyLiteralDiffs && <col style={{ width: '14%' }} />}  {/* Params */}
        </colgroup>
        <thead>
          <tr>
            {([
              { label: '', key: null },
              { label: 'Query ID', key: 'query_id' as SortKey },
              { label: 'Time', key: 'time' as SortKey },
              { label: 'Duration', key: 'duration' as SortKey },
              { label: 'CPU', key: 'cpu' as SortKey },
              { label: 'Memory', key: 'memory' as SortKey },
              { label: 'Rows', key: 'rows' as SortKey },
              { label: 'Server', key: 'server' as SortKey },
              { label: '', key: null },
              { label: 'Settings', key: null },
              { label: 'Changes', key: null },
            ]).map((col, ci) => (
              <th
                key={ci}
                onClick={col.key ? () => handleSort(col.key) : undefined}
                style={{
                  background: 'var(--bg-card)',
                  padding: '8px 8px',
                  fontSize: 10,
                  fontWeight: 600,
                  color: sortKey === col.key ? 'var(--text-primary)' : 'var(--text-muted)',
                  textTransform: 'uppercase',
                  letterSpacing: '0.5px',
                  borderBottom: '1px solid var(--border-secondary)',
                  textAlign: 'left',
                  whiteSpace: 'nowrap',
                  cursor: col.key ? 'pointer' : 'default',
                  userSelect: 'none',
                  transition: 'color 0.15s ease',
                }}
              >
                {col.label}
                {sortKey === col.key && (
                  <span style={{ marginLeft: 3, fontSize: 9 }}>{sortDir === 'asc' ? '▲' : '▼'}</span>
                )}
              </th>
            ))}
            {hasAnyLiteralDiffs && (
              <th style={{ background: 'var(--bg-card)', padding: '8px 8px', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', borderBottom: '1px solid var(--border-secondary)', textAlign: 'left', whiteSpace: 'nowrap' }}>
                Params
              </th>
            )}
          </tr>
        </thead>
      </table>
      {/* Scrollable rows */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
          <colgroup>
            <col style={{ width: '3%' }} />
            <col style={{ width: '7%' }} />
            <col style={{ width: '8%' }} />
            <col style={{ width: '9%' }} />
            <col style={{ width: '9%' }} />
            <col style={{ width: '9%' }} />
            <col style={{ width: '6%' }} />
            <col style={{ width: '10%' }} />
            <col style={{ width: '3%' }} />
            <col style={{ width: '9%' }} />
            <col style={{ width: '13%' }} />
            {hasAnyLiteralDiffs && <col style={{ width: '14%' }} />}
          </colgroup>
          <tbody>
            {sortedQueries.map((q, i) => {
              const origIdx = zoomRange ? visibleToOriginalIdx[i] : i;
              const durationVal = Number(q.query_duration_ms);
              const cpuVal = Number(q.cpu_time_us) || 0;
              const memVal = Number(q.memory_usage);

              const durationDev = getDeviation(durationVal, allAvgDuration, allStdDevDuration);
              const cpuDev = getDeviation(cpuVal, avgCpu, stdDevCpu);
              const memDev = getDeviation(memVal, avgMemory, stdDevMemory);

              // Collect non-default settings for this query
              const settings = q.Settings || {};
              const settingKeys = Object.keys(settings);
              const settingsCount = settingKeys.length;

              const isCurrentQuery = currentQueryId != null && q.query_id === currentQueryId;
              return (
                <tr
                  key={q.query_id}
                  style={{
                    fontSize: 11,
                    background: hoveredPoint === origIdx ? 'var(--bg-hover)' : (selectedForCompare.has(origIdx) ? 'rgba(88, 166, 255, 0.08)' : isCurrentQuery ? 'rgba(245, 158, 11, 0.06)' : (i % 2 === 0 ? 'transparent' : 'var(--bg-card)')),
                    transition: 'background 0.1s ease',
                    cursor: compareMode ? 'pointer' : 'default',
                    borderLeft: isCurrentQuery ? '2px solid #f59e0b' : '2px solid transparent',
                  }}
                  onMouseEnter={() => setHoveredPoint(origIdx)}
                  onMouseLeave={() => setHoveredPoint(null)}
                  onClick={() => {
                    if (compareMode) {
                      setSelectedForCompare(prev => {
                        const next = new Set(prev);
                        if (next.has(origIdx)) next.delete(origIdx); else next.add(origIdx);
                        return next;
                      });
                    }
                  }}
                >
                  {/* Type dot */}
                  <td style={{ padding: '8px 4px 8px 8px', borderBottom: '1px solid var(--border-secondary)', textAlign: 'center' }}>
                    <QueryKindDot kind={q.query_kind || ''} />
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>
                    <span style={{ fontFamily: 'monospace', fontSize: 10, color: onSelectQuery && !compareMode ? '#58a6ff' : 'var(--text-tertiary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'flex', alignItems: 'center', gap: 4, cursor: onSelectQuery && !compareMode ? 'pointer' : 'default', textDecoration: onSelectQuery && !compareMode ? 'underline' : 'none', textDecorationColor: 'rgba(88,166,255,0.3)', textUnderlineOffset: '2px' }} title={`${q.query_id}${onSelectQuery && !compareMode ? '\nClick to view details' : ''}`}
                      onClick={(e) => {
                        if (compareMode) return; // let row handler deal with it
                        if (onSelectQuery) {
                          e.stopPropagation();
                          onSelectQuery(q);
                        }
                      }}
                    >
                      {compareMode && (
                        <span style={{ width: 14, height: 14, borderRadius: 3, border: selectedForCompare.has(origIdx) ? '2px solid #58a6ff' : '1px solid var(--border-primary)', background: selectedForCompare.has(origIdx) ? '#58a6ff' : 'transparent', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, fontSize: 9, color: '#fff' }}>
                          {selectedForCompare.has(origIdx) ? '✓' : ''}
                        </span>
                      )}
                      {q.query_id.substring(0, 8)}
                      {isCurrentQuery && (
                        <span style={{
                          fontSize: 8, padding: '1px 4px', borderRadius: 3,
                          background: 'rgba(245, 158, 11, 0.15)', color: '#f59e0b',
                          fontFamily: 'system-ui, sans-serif', fontWeight: 500, flexShrink: 0,
                        }}>current</span>
                      )}
                    </span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', whiteSpace: 'nowrap' }}>
                    <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{fmtTimeShort(q.query_start_time)}</span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', whiteSpace: 'nowrap' }}>
                    <span style={{ fontFamily: 'monospace', display: 'flex', alignItems: 'center', gap: 4 }} title={`${durationDev.zScore > 0 ? '+' : ''}${(durationDev.zScore).toFixed(1)}σ from avg`}>
                      <span style={{ color: '#58a6ff' }}>{fmtMs(durationVal)}</span>
                      {durationDev.indicator && <span style={{ fontSize: 8, color: durationDev.color }}>{durationDev.indicator}</span>}
                    </span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', whiteSpace: 'nowrap' }}>
                    <span style={{ fontFamily: 'monospace', display: 'flex', alignItems: 'center', gap: 4 }} title={`${cpuDev.zScore > 0 ? '+' : ''}${(cpuDev.zScore).toFixed(1)}σ from avg`}>
                      <span style={{ color: 'var(--text-tertiary)' }}>{fmtUs(cpuVal)}</span>
                      {cpuDev.indicator && <span style={{ fontSize: 8, color: cpuDev.color }}>{cpuDev.indicator}</span>}
                    </span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', whiteSpace: 'nowrap' }}>
                    <span style={{ fontFamily: 'monospace', display: 'flex', alignItems: 'center', gap: 4 }} title={`${memDev.zScore > 0 ? '+' : ''}${(memDev.zScore).toFixed(1)}σ from avg`}>
                      <span style={{ color: 'var(--text-tertiary)' }}>{formatBytes(memVal)}</span>
                      {memDev.indicator && <span style={{ fontSize: 8, color: memDev.color }}>{memDev.indicator}</span>}
                    </span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', whiteSpace: 'nowrap' }}>
                    <span style={{ fontFamily: 'monospace', color: 'var(--text-tertiary)' }}>{(Number(q.result_rows) || 0).toLocaleString()}</span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', whiteSpace: 'nowrap' }}>
                    <span style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', display: 'block' }} title={q.client_hostname || '—'}>{q.client_hostname || '—'}</span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>
                    <span style={{ fontSize: 10, color: q.exception_code !== 0 ? 'var(--color-error)' : 'var(--color-success)' }}>{q.exception_code !== 0 ? '✗' : '✓'}</span>
                  </td>
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', overflow: 'visible' }}>
                    {settingsCount > 0 ? (
                      <span
                        style={{
                          fontSize: 10,
                          color: '#f59e0b',
                          cursor: 'pointer',
                          padding: '2px 6px',
                          borderRadius: 4,
                          background: 'rgba(245, 158, 11, 0.1)',
                          border: '1px solid rgba(245, 158, 11, 0.2)',
                          display: 'inline-block',
                        }}
                        onMouseEnter={(e) => {
                          const rect = e.currentTarget.getBoundingClientRect();
                          setSettingsPopover({ idx: i, x: rect.right, y: rect.top });
                        }}
                        onMouseLeave={(e) => {
                          // Only close if mouse isn't moving to the popover
                          const related = e.relatedTarget as HTMLElement | null;
                          if (related?.closest?.('.settings-popover-panel')) return;
                          setSettingsPopover(null);
                        }}
                      >
                        {settingsCount} setting{settingsCount > 1 ? 's' : ''}
                      </span>
                    ) : (
                      <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>—</span>
                    )}
                  </td>
                  {/* Changes column — settings diff from previous query */}
                  <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', overflow: 'visible' }}>
                    {(() => {
                      const changes = settingsChangesByIdx.get(i);
                      if (!changes || changes.length === 0) {
                        return i === 0
                          ? <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>—</span>
                          : <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>—</span>;
                      }
                      return (
                        <span
                          style={{
                            fontSize: 10,
                            color: '#f59e0b',
                            padding: '2px 6px',
                            borderRadius: 4,
                            background: 'rgba(245, 158, 11, 0.08)',
                            border: '1px solid rgba(245, 158, 11, 0.15)',
                            display: 'inline-block',
                            cursor: 'default',
                          }}
                          title={changes.map(c => `${c.key}: ${c.from} → ${c.to}`).join('\n')}
                        >
                          {changes.map((c, ci) => (
                            <span key={ci} style={{ display: 'block', whiteSpace: 'nowrap', lineHeight: '15px' }}>
                              <span style={{ color: 'var(--text-muted)' }}>{c.key}:</span>{' '}
                              <span style={{ color: 'var(--color-error)', textDecoration: 'line-through', opacity: 0.6, fontSize: 9 }}>{c.from}</span>{' '}
                              <span style={{ color: 'var(--text-muted)', fontSize: 8 }}>→</span>{' '}
                              <span style={{ color: 'var(--color-success)', fontSize: 9 }}>{c.to}</span>
                            </span>
                          ))}
                        </span>
                      );
                    })()}
                  </td>
                  {/* Params column — literal value diffs vs first execution */}
                  {hasAnyLiteralDiffs && (
                    <td style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)', overflow: 'visible', maxWidth: 220 }}>
                      {(() => {
                        if (i === 0) {
                          return <span style={{ fontSize: 10, color: 'var(--text-muted)', fontStyle: 'italic' }}>ref</span>;
                        }
                        const diffs = literalDiffsByIdx.get(i);
                        if (!diffs || diffs.length === 0) {
                          return <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>same</span>;
                        }
                        // Show up to 3 diffs inline, rest as count
                        const shown = diffs.slice(0, 3);
                        const remaining = diffs.length - shown.length;
                        return (
                          <span
                            style={{
                              fontSize: 10,
                              padding: '2px 6px',
                              borderRadius: 4,
                              background: 'rgba(88, 166, 255, 0.06)',
                              border: '1px solid rgba(88, 166, 255, 0.15)',
                              display: 'inline-block',
                              cursor: 'default',
                            }}
                            title={diffs.map(d => `${d.context ? d.context + ': ' : '#' + d.index + ': '}${formatLiteral(d.reference, 40)} → ${formatLiteral(d.current, 40)}`).join('\n')}
                          >
                            {shown.map((d, di) => (
                              <span key={di} style={{ display: 'block', whiteSpace: 'nowrap', lineHeight: '15px', fontFamily: 'monospace' }}>
                                {d.context && <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>{d.context}: </span>}
                                <span style={{ color: 'var(--text-muted)', opacity: 0.6, textDecoration: 'line-through', fontSize: 9 }}>{formatLiteral(d.reference, 12)}</span>
                                <span style={{ color: 'var(--text-muted)', fontSize: 8 }}> → </span>
                                <span style={{ color: '#58a6ff', fontSize: 9 }}>{formatLiteral(d.current, 12)}</span>
                              </span>
                            ))}
                            {remaining > 0 && (
                              <span style={{ display: 'block', fontSize: 9, color: 'var(--text-muted)', lineHeight: '15px' }}>
                                +{remaining} more
                              </span>
                            )}
                          </span>
                        );
                      })()}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Comparison panel */}
      {compareMode && selectedForCompare.size >= 2 && (() => {
        const selected = Array.from(selectedForCompare).sort((a, b) => a - b).map(i => similarQueries[i]);
        const comparableQueries: ComparableQuery[] = selected.map(q => ({
          query_id: q.query_id,
          query_start_time: q.query_start_time,
          query_duration_ms: q.query_duration_ms,
          read_rows: q.read_rows,
          read_bytes: q.read_bytes,
          result_rows: q.result_rows,
          memory_usage: q.memory_usage,
          cpu_time_us: q.cpu_time_us,
          exception_code: q.exception_code,
          exception: q.exception,
          Settings: q.Settings,
          hostname: q.client_hostname,
        }));
        return (
          <QueryComparisonPanel
            queries={comparableQueries}
            mode="overlay"
            onClose={() => {
              setCompareMode(false);
              setSelectedForCompare(new Set());
            }}
          />
        );
      })()}

      {/* Settings popover - positioned via state to avoid clipping */}
      {settingsPopover !== null && (() => {
        const q = similarQueries[settingsPopover.idx];
        const s = q?.Settings;
        if (!s || typeof s !== 'object') return null;
        const keys = Object.keys(s);
        if (keys.length === 0) return null;
        return (
          <div
            className="settings-popover-panel"
            onMouseLeave={() => setSettingsPopover(null)}
            style={{
              position: 'fixed',
              right: document.documentElement.clientWidth - settingsPopover.x + 4,
              top: settingsPopover.y,
              transform: 'translateY(-50%)',
              zIndex: 9999,
              background: 'var(--bg-primary)',
              border: '1px solid var(--border-accent)',
              borderRadius: 8,
              padding: '8px 0',
              minWidth: 280,
              maxWidth: 400,
              boxShadow: '0 8px 24px rgba(0,0,0,0.3)',
            }}
          >
            <div style={{ padding: '4px 12px 8px', fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', borderBottom: '1px solid var(--border-secondary)' }}>Query Settings</div>
            {keys.map(k => (
              <div key={k} style={{ padding: '6px 12px', display: 'flex', justifyContent: 'space-between', gap: 12, fontSize: 11 }}>
                <span style={{ color: 'var(--text-tertiary)', fontFamily: 'monospace' }}>{k}</span>
                <span style={{ color: '#f59e0b', fontFamily: 'monospace', fontWeight: 500 }}>{s[k]}</span>
              </div>
            ))}
          </div>
        );
      })()}
    </div>
  );
};

/**
 * Timeline Query Modal with tabs - matching Part Inspector style
 */
export const QueryDetailModal: React.FC<TimelineQueryModalProps> = ({
  query,
  onClose,
}) => {
  const services = useClickHouseServices();
  const { available: hasTraceLog } = useCapabilityCheck(['trace_log']);
  const { available: hasTextLog } = useCapabilityCheck(['text_log']);
  const { available: hasOpenTelemetry } = useCapabilityCheck(['opentelemetry_span_log']);
  const { available: hasQueryLog } = useCapabilityCheck(['query_log']);
  const { available: hasQueryThreadLog } = useCapabilityCheck(['query_thread_log']);
  const { available: hasProcessesHistory } = useCapabilityCheck(['tracehouse_processes_history']);
  const { experimentalEnabled } = useUserPreferenceStore();

  const [activeTab, setActiveTab] = useState<QueryModalTab>('overview');
  const [detailsSubTab, setDetailsSubTab] = useState<DetailsSubTab>('performance');
  const [analyticsSubTab, setAnalyticsSubTab] = useState<AnalyticsSubTab>('scan_efficiency');
  const [logs, setLogs] = useState<TraceLog[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [logFilter, setLogFilter] = useState<{ logLevels?: string[] }>({});

  // OpenTelemetry spans state
  const [spans, setSpans] = useState<OpenTelemetrySpan[]>([]);
  const [isLoadingSpans, setIsLoadingSpans] = useState(false);
  const [spansError, setSpansError] = useState<string | null>(null);

  // Flamegraph state
  const [flamegraphData, setFlamegraphData] = useState<FlamegraphNode | null>(null);
  const [isLoadingFlamegraph, setIsLoadingFlamegraph] = useState(false);
  const [flamegraphError, setFlamegraphError] = useState<string | null>(null);
  const [flamegraphType, setFlamegraphType] = useState<FlamegraphType>('CPU');

  // Query detail state (for new tabs)
  const [queryDetail, setQueryDetail] = useState<QueryDetailType | null>(null);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  // Similar queries state
  const [similarQueries, setSimilarQueries] = useState<SimilarQuery[]>([]);
  const [isLoadingSimilar, setIsLoadingSimilar] = useState(false);
  const [similarError, setSimilarError] = useState<string | null>(null);
  const [hasFetchedSimilar, setHasFetchedSimilar] = useState(false);
  const [historyLimit, setHistoryLimit] = useState(50);
  const [historyHashMode, setHistoryHashMode] = useState<'normalized' | 'exact'>('normalized');

  // CPU timeline state for history chart overlay
  const [cpuTimeline, setCpuTimeline] = useState<{ t: string; cpu_pct: number }[]>([]);
  const [isLoadingCpu, setIsLoadingCpu] = useState(false);

  // Sub-queries state (for distributed coordinator queries)
  const [subQueries, setSubQueries] = useState<SubQueryInfo[]>([]);
  const [isLoadingSubQueries, setIsLoadingSubQueries] = useState(false);

  // Memory timeline state for history chart overlay
  const [memTimeline, setMemTimeline] = useState<{ t: string; mem_pct: number }[]>([]);
  const [isLoadingMem, setIsLoadingMem] = useState(false);

  // Thread breakdown state
  const [threads, setThreads] = useState<QueryThreadBreakdown[]>([]);
  const [isLoadingThreads, setIsLoadingThreads] = useState(false);
  const [threadsError, setThreadsError] = useState<string | null>(null);
  const [threadsFetched, setThreadsFetched] = useState(false);

  // Internal query override — when user clicks a query ID in the History tab,
  // we build a QuerySeries from the SimilarQuery and switch to it
  const [queryOverride, setQueryOverride] = useState<QuerySeries | null>(null);
  const activeQuery = queryOverride ?? query;

  // Reset tab if experimental disabled while on xray
  useEffect(() => {
    if (!experimentalEnabled && activeTab === 'xray') setActiveTab('overview');
  }, [experimentalEnabled]);

  // Reset state when query changes
  useEffect(() => {
    setQueryOverride(null);
    setActiveTab('overview');
    setDetailsSubTab('performance');
    setLogs([]);
    setLogsError(null);
    setLogFilter({});
    setSpans([]);
    setSpansError(null);
    setFlamegraphData(null);
    setFlamegraphError(null);
    setFlamegraphType('CPU');
    setQueryDetail(null);
    setDetailError(null);
    setSimilarQueries([]);
    setSimilarError(null);
    setHasFetchedSimilar(false);
    setCpuTimeline([]);
    setIsLoadingCpu(false);
    setMemTimeline([]);
    setIsLoadingMem(false);
    setThreads([]);
    setThreadsError(null);
    setThreadsFetched(false);
    setSubQueries([]);
    setIsLoadingSubQueries(false);
  }, [query?.query_id]);

  // Reset tab-specific state when activeQuery changes (e.g. clicking a history row)
  useEffect(() => {
    if (!queryOverride) return; // Only reset when navigating via history
    setActiveTab('overview');
    setDetailsSubTab('performance');
    setLogs([]);
    setLogsError(null);
    setLogFilter({});
    setSpans([]);
    setSpansError(null);
    setFlamegraphData(null);
    setFlamegraphError(null);
    setFlamegraphType('CPU');
    setQueryDetail(null);
    setDetailError(null);
    // Keep similar queries — they share the same normalized hash
    setThreads([]);
    setThreadsError(null);
    setThreadsFetched(false);
    setSubQueries([]);
    setIsLoadingSubQueries(false);
  }, [queryOverride?.query_id]);

  // Fetch logs when Logs tab is selected
  const fetchLogs = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoadingLogs(true);
    setLogsError(null);
    try {
      const result = await services.traceService.getQueryLogs(activeQuery.query_id, undefined, activeQuery.start_time);
      setLogs(result);
    } catch (e) {
      setLogsError(e instanceof Error ? e.message : 'Failed to fetch logs');
    } finally {
      setIsLoadingLogs(false);
    }
  }, [services, activeQuery]);

  // Fetch spans when Spans tab is selected
  const fetchSpans = useCallback(async () => {
    if (!services || !activeQuery) {
      return;
    }
    setIsLoadingSpans(true);
    setSpansError(null);

    // Set up a timeout that will force completion after 3 seconds
    let completed = false;
    const timeoutId = setTimeout(() => {
      if (!completed) {
        completed = true;
        setSpans([]);
        setIsLoadingSpans(false);
      }
    }, 3000);

    try {
      const result = await services.traceService.getOpenTelemetrySpans(activeQuery.query_id);
      if (!completed) {
        completed = true;
        clearTimeout(timeoutId);
        setSpans(result);
        setIsLoadingSpans(false);
      }
    } catch (e) {
      if (!completed) {
        completed = true;
        clearTimeout(timeoutId);
        setSpansError(e instanceof Error ? e.message : 'Failed to fetch spans');
        setIsLoadingSpans(false);
      }
    }
  }, [services, activeQuery]);

  // Fetch flamegraph data when Flamegraph tab is selected
  const fetchFlamegraph = useCallback(async (type: FlamegraphType = flamegraphType) => {
    if (!services || !activeQuery) return;
    setIsLoadingFlamegraph(true);
    setFlamegraphError(null);
    try {
      const result = await services.traceService.getFlamegraphData(activeQuery.query_id, type, activeQuery.start_time);
      setFlamegraphData(result);
    } catch (e) {
      setFlamegraphError(e instanceof Error ? e.message : 'Failed to fetch flamegraph data');
    } finally {
      setIsLoadingFlamegraph(false);
    }
  }, [services, activeQuery, flamegraphType]);

  // Fetch query detail when Performance/Objects/Functions/Settings tabs are selected
  const fetchQueryDetail = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoadingDetail(true);
    setDetailError(null);
    try {
      const result = await services.queryAnalyzer.getQueryDetail(activeQuery.query_id, activeQuery.start_time);
      setQueryDetail(result);
    } catch (e) {
      setDetailError(e instanceof Error ? e.message : 'Failed to fetch query detail');
    } finally {
      setIsLoadingDetail(false);
    }
  }, [services, activeQuery]);

  // Fetch similar queries when Similar tab is selected
  const fetchSimilarQueries = useCallback(async (limit?: number, hashMode?: 'normalized' | 'exact') => {
    if (!services || !activeQuery || !queryDetail) {
      return;
    }
    const effectiveLimit = limit ?? historyLimit;
    const effectiveMode = hashMode ?? historyHashMode;
    const hash = effectiveMode === 'exact'
      ? queryDetail.query_hash
      : queryDetail.normalized_query_hash;
    if (!hash) return;
    setIsLoadingSimilar(true);
    setHasFetchedSimilar(true);
    setSimilarError(null);
    // Reset overlays when re-fetching
    setCpuTimeline([]);
    setMemTimeline([]);
    try {
      const result = await services.queryAnalyzer.getSimilarQueries(String(hash), effectiveLimit, effectiveMode);
      setSimilarQueries(result);
    } catch (e) {
      setSimilarError(e instanceof Error ? e.message : 'Failed to fetch similar queries');
    } finally {
      setIsLoadingSimilar(false);
    }
  }, [services, activeQuery, queryDetail, historyLimit, historyHashMode]);

  // Fetch settings defaults
  const fetchSettingsDefaults = useCallback(async (settingNames: string[]): Promise<Record<string, { default: string; description: string }>> => {
    if (!services || settingNames.length === 0) return {};
    try {
      const result = await services.queryAnalyzer.getSettingsDefaults(settingNames);
      const map: Record<string, { default: string; description: string }> = {};
      for (const s of result) {
        map[s.name] = { default: s.default, description: s.description };
      }
      return map;
    } catch {
      return {};
    }
  }, [services]);

  // Fetch thread breakdown
  const fetchThreads = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoadingThreads(true);
    setThreadsError(null);
    try {
      const result = await services.queryAnalyzer.getQueryThreadBreakdown(activeQuery.query_id, activeQuery.start_time);
      setThreads(result);
    } catch (e) {
      setThreadsError(e instanceof Error ? e.message : 'Failed to fetch thread breakdown');
    } finally {
      setIsLoadingThreads(false);
      setThreadsFetched(true);
    }
  }, [services, activeQuery]);

  useEffect(() => {
    if ((activeTab === 'logs' || activeTab === 'xray') && activeQuery && logs.length === 0 && !isLoadingLogs && !logsError) {
      fetchLogs();
    }
  }, [activeTab, activeQuery, logs.length, isLoadingLogs, logsError, fetchLogs]);

  useEffect(() => {
    if (activeTab === 'spans' && activeQuery && spans.length === 0 && !isLoadingSpans && !spansError) {
      fetchSpans();
    }
  }, [activeTab, activeQuery, spans.length, isLoadingSpans, spansError, fetchSpans]);

  // Fetch flamegraph when Flamegraph tab is selected
  useEffect(() => {
    if (activeTab === 'flamegraph' && query && !flamegraphData && !isLoadingFlamegraph && !flamegraphError) {
      fetchFlamegraph();
    }
  }, [activeTab, activeQuery, flamegraphData, isLoadingFlamegraph, flamegraphError, fetchFlamegraph]);

  // Fetch threads when Threads tab is selected
  useEffect(() => {
    if (activeTab === 'threads' && activeQuery && threads.length === 0 && !isLoadingThreads && !threadsError && !threadsFetched) {
      fetchThreads();
    }
  }, [activeTab, activeQuery, threads.length, isLoadingThreads, threadsError, threadsFetched, fetchThreads]);

  // Handle flamegraph type change
  const handleFlamegraphTypeChange = useCallback((newType: FlamegraphType) => {
    setFlamegraphType(newType);
    setFlamegraphData(null);
    setFlamegraphError(null);
    fetchFlamegraph(newType);
  }, [fetchFlamegraph]);

  // Always fetch query detail on mount — needed for title (query_kind), Pipeline tab gating, and multiple tabs
  useEffect(() => {
    if (activeQuery && !queryDetail && !isLoadingDetail && !detailError) {
      fetchQueryDetail();
    }
  }, [activeQuery, queryDetail, isLoadingDetail, detailError, fetchQueryDetail]);

  // Fetch sub-queries when we detect this is a coordinator query
  useEffect(() => {
    if (queryDetail && queryDetail.is_initial_query === 1 && services && subQueries.length === 0 && !isLoadingSubQueries) {
      setIsLoadingSubQueries(true);
      services.queryAnalyzer.getSubQueries(activeQuery!.query_id, activeQuery!.start_time)
        .then(setSubQueries)
        .catch(() => { /* best-effort */ })
        .finally(() => setIsLoadingSubQueries(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryDetail, services]);

  // Navigate to a query by ID (parent or child)
  const navigateToQuery = useCallback(async (queryId: string) => {
    if (!services) return;
    try {
      const detail = await services.queryAnalyzer.getQueryDetail(queryId);
      if (!detail) return;
      const durationMs = Number(detail.query_duration_ms) || 0;
      const startMs = new Date(detail.query_start_time).getTime();
      setQueryOverride({
        query_id: detail.query_id,
        label: detail.query || '',
        user: detail.user,
        peak_memory: Number(detail.memory_usage) || 0,
        duration_ms: durationMs,
        cpu_us: (detail.ProfileEvents?.['UserTimeMicroseconds'] || 0) + (detail.ProfileEvents?.['SystemTimeMicroseconds'] || 0),
        net_send: detail.ProfileEvents?.['NetworkSendBytes'] || 0,
        net_recv: detail.ProfileEvents?.['NetworkReceiveBytes'] || 0,
        disk_read: Number(detail.read_bytes) || 0,
        disk_write: detail.ProfileEvents?.['OSWriteBytes'] || 0,
        start_time: detail.query_start_time,
        end_time: new Date(startMs + durationMs).toISOString(),
        exception_code: detail.exception_code,
        exception: detail.exception,
        points: [],
      });
    } catch { /* ignore navigation errors */ }
  }, [services]);

  // Fetch similar queries when History tab is selected and we have the hash
  useEffect(() => {
    if (activeTab === 'history' && queryDetail?.normalized_query_hash && !hasFetchedSimilar && !isLoadingSimilar) {
      fetchSimilarQueries();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, queryDetail?.normalized_query_hash, hasFetchedSimilar, isLoadingSimilar]);

  // Fetch CPU timeline once similar queries are loaded (for chart overlay)
  useEffect(() => {
    if (similarQueries.length >= 2 && cpuTimeline.length === 0 && !isLoadingCpu && services) {
      const startTime = similarQueries[0].query_start_time;
      const endTime = similarQueries[similarQueries.length - 1].query_start_time;
      setIsLoadingCpu(true);
      services.queryAnalyzer.getServerCpuForRange(startTime, endTime)
        .then(data => setCpuTimeline(data))
        .catch(() => { /* CPU overlay is best-effort */ })
        .finally(() => setIsLoadingCpu(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [similarQueries, services]);

  // Fetch memory timeline once similar queries are loaded (for chart overlay)
  useEffect(() => {
    if (similarQueries.length >= 2 && memTimeline.length === 0 && !isLoadingMem && services) {
      const startTime = similarQueries[0].query_start_time;
      const endTime = similarQueries[similarQueries.length - 1].query_start_time;
      setIsLoadingMem(true);
      services.queryAnalyzer.getServerMemoryForRange(startTime, endTime)
        .then(data => setMemTimeline(data))
        .catch(() => { /* Memory overlay is best-effort */ })
        .finally(() => setIsLoadingMem(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [similarQueries, services]);

  if (!query) return null;

  // After the null guard, activeQuery is guaranteed non-null
  // (queryOverride is always non-null when set, and query is non-null here)
  const q = activeQuery!;

  const fmtMs = (ms: number) => ms < 1000 ? `${ms}ms` : ms < 60000 ? `${(ms / 1000).toFixed(2)}s` : `${(ms / 60000).toFixed(2)}m`;
  const fmtUs = (us: number) => us < 1000 ? `${us}µs` : us < 1000000 ? `${(us / 1000).toFixed(1)}ms` : `${(us / 1000000).toFixed(2)}s`;
  const fmtTime = (ts: string) => new Date(ts).toLocaleString();

  const tabs: { key: QueryModalTab; label: string; unavailable?: boolean; reason?: string }[] = [
    { key: 'overview', label: 'Overview' },
    { key: 'details', label: 'Details' },
    { key: 'analytics', label: 'Analytics' },
    { key: 'logs', label: 'Logs', unavailable: !hasTextLog, reason: 'system.text_log' },
    { key: 'history', label: 'History', unavailable: !hasQueryLog, reason: 'system.query_log' },
  ];

  // Show Pipeline tab only for SELECT queries (use query_kind from query_log, not fragile regex)
  const isSelectQuery = queryDetail?.query_kind?.toUpperCase() === 'SELECT';
  if (isSelectQuery) {
    tabs.push({ key: 'pipeline', label: 'Pipeline' });
  }

  if (experimentalEnabled) {
    tabs.push(
      { key: 'xray', label: 'X-Ray', unavailable: !hasProcessesHistory, reason: 'tracehouse.processes_history' },
    );
  }
  tabs.push(
    { key: 'threads', label: 'Threads', unavailable: !hasQueryThreadLog, reason: 'system.query_thread_log' },
    { key: 'flamegraph', label: 'Flamegraph', unavailable: !hasTraceLog, reason: 'system.trace_log' },
    { key: 'spans', label: 'Spans', unavailable: !hasOpenTelemetry, reason: 'system.opentelemetry_span_log' },
  );

  const detailsSubTabs: { key: DetailsSubTab; label: string }[] = [
    { key: 'performance', label: 'Profile Events' },
    { key: 'objects', label: 'Objects' },
    { key: 'functions', label: 'Functions' },
    { key: 'settings', label: 'Settings' },
  ];

  const analyticsSubTabs: { key: AnalyticsSubTab; label: string }[] = [
    { key: 'scan_efficiency', label: 'Scan Efficiency' },
    { key: 'column_cost', label: 'Column Cost' },
  ];

  return (
    <ModalWrapper isOpen={!!query} onClose={onClose} maxWidth={1400}>
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        {/* Header */}
        <div style={{
          padding: '16px 32px 12px 32px',
          borderBottom: '1px solid var(--border-accent)',
          background: 'var(--bg-card)',
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <h2 style={{
              fontSize: 18,
              fontWeight: 500,
              color: 'var(--text-primary)',
              letterSpacing: '0.5px',
              fontFamily: 'monospace',
              margin: 0,
            }}>
              {queryDetail?.query_kind || 'Query'} Details
            </h2>
            <button
              onClick={onClose}
              style={{
                padding: 6,
                background: 'transparent',
                border: 'none',
                color: 'var(--text-muted)',
                cursor: 'pointer',
                borderRadius: 8,
                transition: 'all 0.15s ease',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'var(--bg-card-hover)';
                e.currentTarget.style.color = 'var(--text-primary)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
                e.currentTarget.style.color = 'var(--text-muted)';
              }}
            >
              <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          {/* Tabs */}
          <div style={{ display: 'flex', gap: 2 }}>
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => !tab.unavailable && setActiveTab(tab.key)}
                title={tab.unavailable ? `Requires ${tab.reason} (not available)` : undefined}
                style={{
                  fontFamily: 'monospace',
                  padding: '8px 16px',
                  fontSize: 10,
                  textTransform: 'uppercase',
                  letterSpacing: '1.5px',
                  borderRadius: '6px 6px 0 0',
                  borderTop: activeTab === tab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                  borderLeft: activeTab === tab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                  borderRight: activeTab === tab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                  borderBottom: 'none',
                  background: activeTab === tab.key ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
                  color: tab.unavailable ? 'var(--text-muted)' : activeTab === tab.key ? 'var(--text-primary)' : 'var(--text-muted)',
                  cursor: tab.unavailable ? 'not-allowed' : 'pointer',
                  opacity: tab.unavailable ? 0.4 : 1,
                  transition: 'all 0.2s ease',
                  position: 'relative',
                  marginBottom: -1,
                }}
                onMouseEnter={(e) => {
                  if (activeTab !== tab.key && !tab.unavailable) {
                    e.currentTarget.style.color = 'var(--text-tertiary)';
                    e.currentTarget.style.background = 'var(--bg-card)';
                  }
                }}
                onMouseLeave={(e) => {
                  if (activeTab !== tab.key && !tab.unavailable) {
                    e.currentTarget.style.color = 'var(--text-muted)';
                    e.currentTarget.style.background = 'transparent';
                  }
                }}
              >
                {tab.label}
                {tab.key === 'xray' && (
                  <span style={{
                    position: 'absolute', top: -4, right: -2,
                    fontSize: 7, fontWeight: 700, color: '#f0883e',
                    background: 'var(--bg-tertiary)', border: '1px solid rgba(240,136,62,0.3)',
                    borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                    textTransform: 'uppercase', letterSpacing: '0.3px',
                  }}>exp</span>
                )}
                {activeTab === tab.key && (
                  <div style={{
                    position: 'absolute',
                    bottom: 0,
                    left: 0,
                    right: 0,
                    height: 2,
                    background: '#58a6ff',
                    borderRadius: '2px 2px 0 0',
                  }} />
                )}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div style={{ flex: 1, overflow: activeTab === 'history' ? 'hidden' : 'auto', padding: ['logs', 'spans', 'details', 'pipeline', 'history', 'analytics', 'xray'].includes(activeTab) ? 0 : 24 }}>
          {activeTab === 'overview' && (
            <>
              {/* Query ID + Status row */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 16, marginBottom: 16 }}>
                <div>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                    Query ID
                  </div>
                  <div style={{
                    fontFamily: 'monospace',
                    fontSize: 13,
                    color: 'var(--text-secondary)',
                    background: 'var(--bg-tertiary)',
                    border: '1px solid var(--border-primary)',
                    padding: '10px 14px',
                    borderRadius: 6,
                    wordBreak: 'break-all',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: 12,
                  }}>
                    <span>{q.query_id}</span>
                    <button
                      onClick={() => {
                        navigator.clipboard.writeText(q.query_id);
                      }}
                      title="Copy Query ID"
                      style={{
                        background: 'transparent',
                        border: 'none',
                        cursor: 'pointer',
                        padding: 4,
                        borderRadius: 4,
                        color: 'var(--text-muted)',
                        display: 'flex',
                        alignItems: 'center',
                        flexShrink: 0,
                      }}
                      onMouseOver={(e) => e.currentTarget.style.color = 'var(--text-primary)'}
                      onMouseOut={(e) => e.currentTarget.style.color = 'var(--text-muted)'}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                      </svg>
                    </button>
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                    Status
                  </div>
                  {(() => {
                    const status = (q as QuerySeries & { status?: string }).status;
                    const exCode = (q as QuerySeries & { exception_code?: number }).exception_code;
                    const exMsg = (q as QuerySeries & { exception?: string }).exception;
                    const isRunningFlag = (q as QuerySeries & { is_running?: boolean }).is_running;

                    const isFailed = status === 'ExceptionWhileProcessing' || status === 'ExceptionBeforeStart' || (exCode !== undefined && exCode !== 0) || (exMsg !== undefined && exMsg !== null && exMsg !== '');
                    const isRunning = isRunningFlag === true;

                    let displayStatus = 'Success';
                    let statusColor = 'var(--color-success)';
                    let statusBg = 'rgba(var(--color-success-rgb), 0.1)';

                    if (isRunning) {
                      displayStatus = 'Running';
                      statusColor = 'var(--color-warning)';
                      statusBg = 'rgba(var(--color-warning-rgb), 0.1)';
                    } else if (isFailed) {
                      displayStatus = 'Failed';
                      statusColor = 'var(--color-error)';
                      statusBg = 'rgba(var(--color-error-rgb), 0.1)';
                    }

                    return (
                      <div style={{
                        fontFamily: 'monospace',
                        fontSize: 13,
                        color: statusColor,
                        background: statusBg,
                        border: `1px solid ${statusColor}33`,
                        padding: '10px 14px',
                        borderRadius: 6,
                        display: 'flex',
                        alignItems: 'center',
                        gap: 8,
                      }}>
                        <span style={{
                          width: 8,
                          height: 8,
                          borderRadius: '50%',
                          background: statusColor,
                          animation: isRunning ? 'pulse 1.5s ease-in-out infinite' : 'none',
                        }} />
                        {displayStatus}
                        {exCode ? ` (${exCode})` : ''}
                      </div>
                    );
                  })()}
                </div>
              </div>

              {/* Exception message if query failed */}
              {(() => {
                const status = (q as QuerySeries & { status?: string }).status;
                const exCode = (q as QuerySeries & { exception_code?: number }).exception_code;
                const exceptionMsg = (q as QuerySeries & { exception?: string }).exception;
                const isFailed = status === 'ExceptionWhileProcessing' || status === 'ExceptionBeforeStart' || (exCode !== undefined && exCode !== 0) || (exceptionMsg !== undefined && exceptionMsg !== null && exceptionMsg !== '');
                if (!isFailed || !exceptionMsg) return null;
                return (
                  <div style={{ marginBottom: 20 }}>
                    <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                      Error
                    </div>
                    <div style={{
                      padding: '12px 14px',
                      borderRadius: 6,
                      background: 'rgba(var(--color-error-rgb), 0.08)',
                      border: '1px solid rgba(var(--color-error-rgb), 0.2)',
                    }}>
                      <pre style={{
                        margin: 0,
                        fontFamily: 'monospace',
                        fontSize: 12,
                        color: 'var(--color-error)',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-word',
                      }}>
                        {exceptionMsg}
                      </pre>
                    </div>
                  </div>
                );
              })()}

              {/* SQL */}
              <div style={{ marginBottom: 20 }}>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                  SQL
                </div>
                <div style={{
                  maxHeight: 200,
                  overflow: 'auto',
                  borderRadius: 8,
                  background: 'var(--bg-code)',
                  border: '1px solid var(--border-primary)',
                }}>
                  <SqlHighlight style={{
                    padding: 14,
                    fontSize: 12,
                    lineHeight: 1.5,
                    color: 'var(--text-secondary)',
                  }}>
                    {queryDetail?.query || queryDetail?.formatted_query || q.label || '-- no query text available'}
                  </SqlHighlight>
                </div>
              </div>

              {/* Metrics Grid - all 3 columns */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 12 }}>
                <MetricItem label="User" value={q.user} />
                <MetricItem label="Duration" value={fmtMs(q.duration_ms)} />
                <MetricItem label="Peak Memory" value={formatBytes(q.peak_memory)} />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 12 }}>
                <MetricItem label="CPU Time" value={fmtUs(q.cpu_us)} />
                <MetricItem label="Network I/O" value={formatBytes(q.net_send + q.net_recv)} />
                <MetricItem label="Disk I/O" value={formatBytes(q.disk_read + q.disk_write)} />
              </div>

              {/* Time Range */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
                <MetricItem label="Started" value={fmtTime(q.start_time)} />
                <MetricItem label="Ended" value={fmtTime(q.end_time)} />
              </div>

              {/* Distributed query / server info */}
              {queryDetail && (
                <div style={{ marginTop: 16 }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 8 }}>
                    Origin
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
                    {queryDetail.client_hostname && (
                      <MetricItem label="Client Host" value={queryDetail.client_hostname} />
                    )}
                    <MetricItem label="Role" value={
                      <span style={{ color: queryDetail.is_initial_query === 1 ? 'var(--color-info, #58a6ff)' : 'var(--color-warning, #d29922)' }}>
                        {queryDetail.is_initial_query === 1 ? 'Coordinator' : 'Shard sub-query'}
                      </span>
                    } />
                    {queryDetail.is_initial_query === 0 && queryDetail.initial_query_id && (
                      <MetricItem label="Parent Query" value={
                        <button
                          onClick={() => navigateToQuery(queryDetail.initial_query_id)}
                          title={`Go to parent: ${queryDetail.initial_query_id}`}
                          style={{
                            fontFamily: 'monospace', fontSize: 11, color: '#58a6ff',
                            background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                            textDecoration: 'underline', textDecorationStyle: 'dotted',
                          }}
                        >
                          {queryDetail.initial_query_id.slice(0, 16)}… ↗
                        </button>
                      } />
                    )}
                    {queryDetail.is_initial_query === 0 && queryDetail.initial_address && (
                      <MetricItem label="Coordinator Address" value={queryDetail.initial_address} />
                    )}
                    {queryDetail.current_database && (
                      <MetricItem label="Database" value={queryDetail.current_database} />
                    )}
                  </div>
                </div>
              )}

              {/* Sub-queries for coordinator queries */}
              {queryDetail && queryDetail.is_initial_query === 1 && subQueries.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 8 }}>
                    Shard Sub-queries ({subQueries.length})
                  </div>
                  <div style={{
                    border: '1px solid var(--border-secondary)',
                    borderRadius: 6,
                    overflow: 'hidden',
                    maxHeight: 240,
                    overflowY: 'auto',
                  }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
                      <thead>
                        <tr style={{ background: 'var(--bg-tertiary)' }}>
                          <th style={{ padding: '6px 10px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Query ID</th>
                          <th style={{ padding: '6px 10px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Shard</th>
                          <th style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Duration</th>
                          <th style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Memory</th>
                          <th style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Rows Read</th>
                          <th style={{ padding: '6px 10px', textAlign: 'center', color: 'var(--text-muted)', fontWeight: 500, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.5px' }}>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {subQueries.map((sq) => (
                          <tr
                            key={sq.query_id}
                            style={{ borderTop: '1px solid var(--border-primary)' }}
                          >
                            <td style={{ padding: '6px 10px' }}>
                              <button
                                onClick={() => navigateToQuery(sq.query_id)}
                                style={{
                                  fontFamily: 'monospace', fontSize: 11, color: '#58a6ff',
                                  background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                                  textDecoration: 'underline', textDecorationStyle: 'dotted',
                                }}
                                title={sq.query_id}
                              >
                                {sq.query_id.slice(0, 12)}…
                              </button>
                            </td>
                            <td style={{ padding: '6px 10px', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)' }}>{sq.hostname}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-secondary)' }}>{fmtMs(sq.query_duration_ms)}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-secondary)' }}>{formatBytes(sq.memory_usage)}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-secondary)' }}>{sq.read_rows.toLocaleString()}</td>
                            <td style={{ padding: '6px 10px', textAlign: 'center' }}>
                              {sq.exception_code ? (
                                <span style={{ color: 'var(--color-error)', fontSize: 10 }}>✗ {sq.exception_code}</span>
                              ) : (
                                <span style={{ color: 'var(--color-success)', fontSize: 10 }}>✓</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
              {queryDetail && queryDetail.is_initial_query === 1 && isLoadingSubQueries && (
                <div style={{ marginTop: 16, fontSize: 11, color: 'var(--text-muted)' }}>Loading sub-queries…</div>
              )}

              {/* Index Selectivity - shows how well the primary key pruned data */}
              {queryDetail?.ProfileEvents && (() => {
                const pe = queryDetail.ProfileEvents;
                const selectedParts = pe['SelectedParts'] || 0;
                const selectedPartsTotal = pe['SelectedPartsTotal'] || 0;
                const selectedMarks = pe['SelectedMarks'] || 0;
                const selectedMarksTotal = pe['SelectedMarksTotal'] || 0;

                const partsSelectivity = selectedPartsTotal > 0 ? (selectedParts / selectedPartsTotal) * 100 : null;
                const marksSelectivity = selectedMarksTotal > 0 ? (selectedMarks / selectedMarksTotal) * 100 : null;

                // Color: lower is better (more pruning)
                const getSelectivityColor = (pct: number) =>
                  pct <= 10 ? 'var(--color-success)' : pct <= 50 ? 'var(--color-warning)' : 'var(--color-error)';

                if (partsSelectivity === null && marksSelectivity === null) return null;

                return (
                  <div style={{ marginTop: 16 }}>
                    <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 8 }}>
                      Index Selectivity
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 12 }}>
                      {partsSelectivity !== null && (
                        <div style={{
                          background: 'var(--bg-card)',
                          border: '1px solid var(--border-secondary)',
                          borderRadius: 6,
                          padding: '8px 12px',
                        }}>
                          <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 4 }}>Parts Scanned</div>
                          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                            <div style={{ fontSize: 14, fontWeight: 500, color: getSelectivityColor(partsSelectivity), fontFamily: 'monospace' }}>
                              {partsSelectivity.toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                              {selectedParts.toLocaleString()} / {selectedPartsTotal.toLocaleString()}
                            </div>
                          </div>
                        </div>
                      )}
                      {marksSelectivity !== null && (
                        <div style={{
                          background: 'var(--bg-card)',
                          border: '1px solid var(--border-secondary)',
                          borderRadius: 6,
                          padding: '8px 12px',
                        }}>
                          <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 4 }}>Marks Scanned</div>
                          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                            <div style={{ fontSize: 14, fontWeight: 500, color: getSelectivityColor(marksSelectivity), fontFamily: 'monospace' }}>
                              {marksSelectivity.toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                              {selectedMarks.toLocaleString()} / {selectedMarksTotal.toLocaleString()}
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })()}
            </>
          )}

          {activeTab === 'logs' && (
            <div style={{ height: '100%' }}>
              <TraceLogViewer
                logs={logs}
                isLoading={isLoadingLogs}
                error={logsError}
                filter={logFilter}
                onFilterChange={(newFilter) => setLogFilter(prev => ({ ...prev, ...newFilter }))}
                onRefresh={fetchLogs}
                queryId={activeQuery?.query_id}
                queryStartTime={activeQuery?.start_time}
                queryEndTime={activeQuery?.end_time}
              />
            </div>
          )}

          {activeTab === 'spans' && (
            <div style={{ height: '100%' }}>
              <SpansViewer
                spans={spans}
                isLoading={isLoadingSpans}
                error={spansError}
                onRefresh={fetchSpans}
              />
            </div>
          )}

          {activeTab === 'flamegraph' && (
            <div style={{ height: '100%' }}>
              <Flamegraph
                data={flamegraphData}
                isLoading={isLoadingFlamegraph}
                error={flamegraphError}
                onRefresh={fetchFlamegraph}
                profileType={flamegraphType}
                onTypeChange={handleFlamegraphTypeChange}
              />
            </div>
          )}

          {activeTab === 'details' && (
            <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              {/* Sub-tabs */}
              <div style={{
                padding: '12px 16px',
                borderBottom: '1px solid var(--border-secondary)',
                background: 'var(--bg-card)',
                display: 'flex',
                gap: 4,
              }}>
                {detailsSubTabs.map((subTab) => (
                  <button
                    key={subTab.key}
                    onClick={() => setDetailsSubTab(subTab.key)}
                    style={{
                      padding: '6px 14px',
                      fontSize: 11,
                      borderRadius: 6,
                      border: detailsSubTab === subTab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                      background: detailsSubTab === subTab.key ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
                      color: detailsSubTab === subTab.key ? 'var(--text-primary)' : 'var(--text-muted)',
                      cursor: 'pointer',
                      transition: 'all 0.15s ease',
                      fontFamily: 'monospace',
                    }}
                    onMouseEnter={(e) => {
                      if (detailsSubTab !== subTab.key) {
                        e.currentTarget.style.color = 'var(--text-tertiary)';
                        e.currentTarget.style.background = 'var(--bg-code)';
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (detailsSubTab !== subTab.key) {
                        e.currentTarget.style.color = 'var(--text-muted)';
                        e.currentTarget.style.background = 'transparent';
                      }
                    }}
                  >
                    {subTab.label}
                  </button>
                ))}
              </div>
              {/* Sub-tab content */}
              <div style={{ flex: 1, overflow: 'auto' }}>
                {detailsSubTab === 'performance' && (
                  <PerformanceTab
                    profileEvents={queryDetail?.ProfileEvents}
                    isLoading={isLoadingDetail}
                  />
                )}
                {detailsSubTab === 'objects' && (
                  <ObjectsTab
                    queryDetail={queryDetail}
                    isLoading={isLoadingDetail}
                  />
                )}
                {detailsSubTab === 'functions' && (
                  <FunctionsTab
                    queryDetail={queryDetail}
                    isLoading={isLoadingDetail}
                  />
                )}
                {detailsSubTab === 'settings' && (
                  <SettingsTab
                    queryDetail={queryDetail}
                    isLoading={isLoadingDetail}
                    onFetchDefaults={fetchSettingsDefaults}
                  />
                )}
              </div>
            </div>
          )}

          {activeTab === 'analytics' && (
            <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
              {/* Analytics sub-tabs */}
              <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--border-secondary)', padding: '0 22px', flexShrink: 0 }}>
                {analyticsSubTabs.map((st) => (
                  <button
                    key={st.key}
                    onClick={() => setAnalyticsSubTab(st.key)}
                    style={{
                      fontFamily: 'monospace',
                      padding: '10px 16px',
                      fontSize: 11,
                      letterSpacing: '0.5px',
                      border: 'none',
                      borderBottom: analyticsSubTab === st.key ? '2px solid #58a6ff' : '2px solid transparent',
                      background: 'transparent',
                      color: analyticsSubTab === st.key ? 'var(--text-primary)' : 'var(--text-muted)',
                      cursor: 'pointer',
                      transition: 'all 0.15s ease',
                    }}
                    onMouseEnter={(e) => {
                      if (analyticsSubTab !== st.key) e.currentTarget.style.color = 'var(--text-tertiary)';
                    }}
                    onMouseLeave={(e) => {
                      if (analyticsSubTab !== st.key) e.currentTarget.style.color = 'var(--text-muted)';
                    }}
                  >
                    {st.label}
                  </button>
                ))}
              </div>
              {/* Analytics sub-tab content */}
              <div style={{ flex: 1, overflow: 'auto' }}>
                {analyticsSubTab === 'scan_efficiency' && (
                  <QueryScanEfficiency
                    queryDetail={queryDetail}
                    isLoading={isLoadingDetail}
                  />
                )}
                {analyticsSubTab === 'column_cost' && (
                  <ColumnCostAnalysis
                    queryDetail={queryDetail}
                    isLoading={isLoadingDetail}
                  />
                )}
              </div>
            </div>
          )}

          {activeTab === 'history' && (
            (() => {
              const isRunning = (q as QuerySeries & { is_running?: boolean }).is_running === true;
              if (isRunning) {
                return (
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 14, color: 'var(--text-tertiary)', marginBottom: 8 }}>Query is still running</div>
                      <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>History will be available after the query completes</div>
                    </div>
                  </div>
                );
              }
              return (
                <HistoryTab
                  similarQueries={similarQueries}
                  isLoading={isLoadingSimilar || (isLoadingDetail && !queryDetail)}
                  error={similarError}
                  onRefresh={fetchSimilarQueries}
                  cpuTimeline={cpuTimeline}
                  memTimeline={memTimeline}
                  isLoadingCpu={isLoadingCpu}
                  limit={historyLimit}
                  onLimitChange={(newLimit) => {
                    setHistoryLimit(newLimit);
                    setHasFetchedSimilar(false);
                    setSimilarQueries([]);
                    setCpuTimeline([]);
                    setMemTimeline([]);
                    fetchSimilarQueries(newLimit);
                  }}
                  hashMode={historyHashMode}
                  currentQueryId={query?.query_id}
                  onHashModeChange={(mode) => {
                    setHistoryHashMode(mode);
                    setHasFetchedSimilar(false);
                    setSimilarQueries([]);
                    setCpuTimeline([]);
                    setMemTimeline([]);
                    fetchSimilarQueries(undefined, mode);
                  }}
                  onSelectQuery={(sq) => {
                    // Build a QuerySeries from the SimilarQuery to switch the modal view
                    const durationMs = Number(sq.query_duration_ms) || 0;
                    const startMs = new Date(sq.query_start_time).getTime();
                    setQueryOverride({
                      query_id: sq.query_id,
                      label: q.label ?? '',
                      user: sq.user,
                      peak_memory: Number(sq.memory_usage) || 0,
                      duration_ms: durationMs,
                      cpu_us: Number(sq.cpu_time_us) || 0,
                      net_send: 0,
                      net_recv: 0,
                      disk_read: 0,
                      disk_write: 0,
                      start_time: sq.query_start_time,
                      end_time: new Date(startMs + durationMs).toISOString(),
                      exception_code: sq.exception_code,
                      exception: sq.exception,
                      points: [],
                    });
                  }}
                />
              );
            })()
          )}

          {activeTab === 'pipeline' && q && (
            <PipelineProfileTab
              querySQL={q.label}
              queryId={q.query_id}
              database={queryDetail?.current_database}
              eventDate={activeQuery?.start_time}
            />
          )}

          {activeTab === 'threads' && (
            <div style={{ height: '100%' }}>
              <ThreadBreakdownSection
                threads={threads}
                isLoading={isLoadingThreads || !threadsFetched}
                error={threadsError}
                onRefresh={fetchThreads}
              />
            </div>
          )}

          {activeTab === 'xray' && q && (
            <div style={{ height: '100%' }}>
              <QueryXRay3D
                queryId={q.query_id}
                logs={logs}
                queryStartTime={q.start_time}
              />
            </div>
          )}
        </div>
      </div>
    </ModalWrapper>
  );
};
