/**
 * DashboardViewer — renders a dashboard as a grid of panels, each executing
 * its referenced preset query and displaying a mini chart + data table.
 *
 * Also provides a dashboard list/selector, create/edit/delete/import/export UI.
 */

import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { type Query } from './types';
import { getAllQueries } from './customQueries';
import { resolveTimeRange, resolveDrillParams } from './templateResolution';
import { formatClickHouseError } from '../../utils/errorFormatters';
import { type ChartType, parseChartDirective, resolveQueryRef } from './metaLanguage';
import { LinkQueryModal } from './LinkQueryModal';
import { TimeRangePicker } from './TimeRangePicker';
import {
  formatCell,
  buildChartData, buildGroupedChartData, isGroupedChartType, sortRows,
  ChartRenderer, isTimeSeriesChartType,
  type ChartDataPoint, type GroupedChartData, type DrillDownEvent, type CorrelationEntry,
} from './charts';
import { Chart3DCanvas } from './charts3d';
import { ResultsTable } from './ResultsTable';
import {
  type Dashboard,
  type DashboardPanel,
  type DashboardGroup,
  DASHBOARD_GROUPS,
  resolvePanel,
  loadDashboards,
  upsertDashboard,
  deleteDashboard,
  exportDashboardJson,
  importDashboardJson,
} from './dashboards';

// ─── Panel component — executes one preset query and shows result ───

interface PanelResult {
  columns: string[];
  rows: Record<string, unknown>[];
}

type PanelView = 'chart' | 'table';

/** Data reported by a panel for cross-panel correlation. */
export interface PanelTimeSeriesInfo {
  name: string;
  color: string;
  unit?: string;
  /** Map from label (timestamp string) → numeric value */
  dataByLabel: Map<string, number>;
}

const DashboardPanelCard: React.FC<{
  panel: DashboardPanel;
  timeRangeOverride: string | null;
  dashboardId: string;
  isFullscreen: boolean;
  onToggleFullscreen: () => void;
  isHidden: boolean;
  hoveredTimestamp?: string | null;
  onTimestampHover?: (label: string | null) => void;
  onTimeSeriesData?: (info: PanelTimeSeriesInfo | null) => void;
  correlationValues?: CorrelationEntry[];
  isHoveredPanel?: boolean;
}> = ({ panel, timeRangeOverride, dashboardId, isFullscreen, onToggleFullscreen, isHidden, hoveredTimestamp, onTimestampHover, onTimeSeriesData, correlationValues, isHoveredPanel }) => {
  const services = useClickHouseServices();
  const [, setSearchParams] = useSearchParams();
  const originalPreset = resolvePanel(panel);
  const [drillPreset, setDrillPreset] = useState<Query | null>(null);
  const [drillParams, setDrillParams] = useState<Record<string, string>>({});
  const preset = drillPreset ?? originalPreset;
  const [result, setResult] = useState<PanelResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [view, setView] = useState<PanelView>('chart');
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [hovered, setHovered] = useState(false);
  const fullscreen = isFullscreen;

  // Escape exits fullscreen, 'f' toggles fullscreen when hovered
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (e.key === 'Escape' && fullscreen) onToggleFullscreen();
      if (e.key === 'f' && hovered && !fullscreen) onToggleFullscreen();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [fullscreen, hovered, onToggleFullscreen]);

  // Hide body scrollbar when fullscreen
  useEffect(() => {
    if (fullscreen) {
      document.body.style.overflow = 'hidden';
      return () => { document.body.style.overflow = ''; };
    }
  }, [fullscreen]);

  const openInEditor = useCallback(() => {
    if (!preset) return;
    const all = getAllQueries();
    const idx = all.findIndex(q => q.name === preset.name);
    const params: Record<string, string> = { tab: 'misc', fromDashboard: dashboardId };
    if (idx >= 0) params.preset = String(idx);
    setSearchParams(new URLSearchParams(params), { replace: false });
  }, [preset, setSearchParams, dashboardId]);

  const run = useCallback(async (overridePreset?: Query, overrideParams?: Record<string, string>) => {
    const p = overridePreset ?? preset;
    if (!services || !p) return;
    setLoading(true);
    setError(null);
    try {
      let sql = resolveTimeRange(p.sql, p.directives.meta?.interval, timeRangeOverride);
      sql = resolveDrillParams(sql, overrideParams ?? drillParams);
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(sql);
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
      setResult({ columns, rows });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [services, preset, timeRangeOverride, drillParams]);

  useEffect(() => {
    if (services && preset) {
      run();
    }
  }, [run, services, preset]);

  // Re-run when time range changes (explicit trigger for prop changes)
  const prevTimeRange = React.useRef(timeRangeOverride);
  useEffect(() => {
    if (prevTimeRange.current !== timeRangeOverride && services && preset) {
      prevTimeRange.current = timeRangeOverride;
      run();
    }
  }, [timeRangeOverride, run, services, preset]);

  // Build chart data from result
  const chartDirective = useMemo(() => preset ? parseChartDirective(preset.sql) : null, [preset]);
  const chartData = useMemo((): ChartDataPoint[] => {
    if (!result || result.rows.length === 0) return [];
    const isTimeSeries = chartDirective?.type && ['line', 'area', 'grouped_line'].includes(chartDirective.type);
    return buildChartData(result.rows, result.columns, chartDirective?.groupByColumn, chartDirective?.valueColumn, isTimeSeries ? undefined : 50, chartDirective?.descriptionColumn);
  }, [result, chartDirective]);

  const groupedChartData = useMemo((): GroupedChartData[] => {
    if (!result || !chartDirective?.groupByColumn || !chartDirective?.valueColumn) return [];
    const isTimeSeries = chartDirective?.type && ['line', 'area', 'grouped_line'].includes(chartDirective.type);
    return buildGroupedChartData(result.rows, chartDirective.groupByColumn, chartDirective.valueColumn, chartDirective.seriesColumn, chartDirective.valueColumns, isTimeSeries ? undefined : 30);
  }, [result, chartDirective]);

  const hasChartDirective = !!(chartDirective?.type || preset?.directives.chart?.type);
  const chartType: ChartType = chartDirective?.type || preset?.directives.chart?.type || 'bar';
  const chartStyle = chartDirective?.visualization || preset?.directives.chart?.style || '2d';
  const chartUnit = chartDirective?.unit;
  const chartColor = chartDirective?.color;
  const isGroupedChart = isGroupedChartType(chartType);
  const isTimeSeries = isTimeSeriesChartType(chartType);
  const hasChart = hasChartDirective && (isGroupedChart ? groupedChartData.length > 0 : chartData.length > 0);

  // Report time-series data upward for correlation
  useEffect(() => {
    if (!onTimeSeriesData) return;
    if (!isTimeSeries || !hasChart || !preset) { onTimeSeriesData(null); return; }
    const dataByLabel = new Map<string, number>();
    if (isGroupedChart && groupedChartData.length > 0) {
      // Sum all series values per timestamp (e.g. TCP + MySQL + HTTP + Interserver)
      for (const d of groupedChartData) {
        const sum = d.groups.reduce((acc, g) => acc + g.value, 0);
        dataByLabel.set(d.label, sum);
      }
    } else {
      for (const d of chartData) dataByLabel.set(d.label, d.value);
    }
    onTimeSeriesData({ name: preset.name, color: chartColor || '#6366f1', unit: chartUnit, dataByLabel });
  }, [onTimeSeriesData, isTimeSeries, isGroupedChart, hasChart, chartData, groupedChartData, preset, chartColor, chartUnit]);

  // Drill-down support
  const isDrillable = !!(preset?.directives.drill?.on && preset?.directives.drill?.into) &&
    (!result || result.columns.includes(preset.directives.drill.on));
  const isDrilled = drillPreset !== null;

  const handleDrillDown = useCallback((event: DrillDownEvent) => {
    if (!preset?.directives.drill?.on || !preset?.directives.drill?.into) return;
    const all = getAllQueries();
    const target = resolveQueryRef(preset.directives.drill.into, preset.group, all);
    if (!target) return;
    const newParams = { ...drillParams, [preset.directives.drill.on]: event.label };
    setDrillPreset(target);
    setDrillParams(newParams);
    setResult(null);
    setTimeout(() => run(target, newParams), 0);
  }, [preset, drillParams, run]);

  const handleDrillReset = useCallback(() => {
    setDrillPreset(null);
    setDrillParams({});
    setResult(null);
    setTimeout(() => run(originalPreset ?? undefined, {}), 0);
  }, [originalPreset, run]);

  // ArrowLeft key undoes drill-down
  const drillResetRef = useRef(handleDrillReset);
  drillResetRef.current = handleDrillReset;
  useEffect(() => {
    if (!isDrilled) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (e.key === 'ArrowLeft') { e.preventDefault(); drillResetRef.current(); }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isDrilled]);

  // @link support
  const [linkModal, setLinkModal] = useState<{ targetQuery: Query; params: Record<string, string> } | null>(null);
  const isLinkable = !!(preset?.directives.link?.on && preset?.directives.link?.into) &&
    (!result || result.columns.includes(preset.directives.link.on));

  const handleLinkClick = useCallback((column: string, value: string) => {
    if (!preset?.directives.link?.into) return;
    const all = getAllQueries();
    const target = resolveQueryRef(preset.directives.link.into, preset.group, all);
    if (!target) return;
    setLinkModal({ targetQuery: target, params: { [column]: value } });
  }, [preset]);

  if (!preset) {
    return (
      <div style={{ ...panelStyle, display: isHidden ? 'none' : 'flex' }}>
        <div style={{ padding: 16, color: 'var(--text-muted)', fontSize: 12 }}>
          Unknown query: <code>{panel.queryName}</code>
        </div>
      </div>
    );
  }

  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={fullscreen ? {
        position: 'fixed', top: 48, left: 0, right: 0, bottom: 0, zIndex: 1999, background: 'var(--bg-primary)',
        display: 'flex', flexDirection: 'column', overflow: 'hidden',
      } : { ...panelStyle, display: isHidden ? 'none' : 'flex' }}>
      <div style={{ padding: fullscreen ? '16px 20px 8px' : '10px 14px 6px', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: fullscreen ? 18 : 12, fontWeight: 600, color: 'var(--text-primary)', lineHeight: 1.3, display: 'flex', alignItems: 'center', gap: 6 }}>
            {isDrilled && (
              <button onClick={handleDrillReset}
                style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--accent-primary, #6366f1)', fontSize: 11, padding: 0 }}
                title="Back to original query">
                ←
              </button>
            )}
            {preset.name}
            {isDrilled && (
              <span style={{ fontSize: 9, color: 'var(--text-muted)', fontWeight: 400 }}>
                ({Object.entries(drillParams).map(([k, v]) => `${k}=${v}`).join(', ')})
              </span>
            )}
          </div>
          {preset.description && (
            <div style={{ fontSize: fullscreen ? 13 : 10, color: 'var(--text-muted)', marginTop: 2, lineHeight: 1.3 }}>{preset.description}</div>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 2, flexShrink: 0 }}>
          {result && hasChart && (
            <div style={{ display: 'flex', borderRadius: 4, overflow: 'hidden', border: '1px solid var(--border-primary)' }}>
              <button onClick={() => setView('chart')}
                style={{ ...viewToggleStyle, background: view === 'chart' ? 'var(--bg-card-hover, rgba(88,166,255,0.1))' : 'transparent', color: view === 'chart' ? 'var(--text-primary)' : 'var(--text-muted)' }}
                title="Chart view">◩</button>
              <button onClick={() => setView('table')}
                style={{ ...viewToggleStyle, background: view === 'table' ? 'var(--bg-card-hover, rgba(88,166,255,0.1))' : 'transparent', color: view === 'table' ? 'var(--text-primary)' : 'var(--text-muted)', borderLeft: '1px solid var(--border-primary)' }}
                title="Table view">☰</button>
            </div>
          )}
          <button onClick={openInEditor}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', fontSize: 11, padding: '2px 5px', opacity: 0.7 }}
            title="Open in query editor">
            ↗
          </button>
          <button onClick={onToggleFullscreen}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', fontSize: 12, padding: '2px 5px', opacity: 0.7 }}
            title={fullscreen ? 'Exit fullscreen (Esc)' : 'Fullscreen (f)'}>
            {fullscreen ? '⊗' : '⊞'}
          </button>
          <button onClick={() => run()} disabled={loading}
            style={{ background: 'none', border: 'none', cursor: loading ? 'not-allowed' : 'pointer', color: 'var(--text-muted)', fontSize: 12, padding: '2px 6px', opacity: loading ? 0.4 : 0.7 }}
            title="Refresh">
            ↻
          </button>
        </div>
      </div>
      <div style={{ padding: fullscreen ? '0 8px 8px' : '0 14px 10px', flex: 1, minHeight: 0, overflowX: 'hidden', overflowY: 'visible', display: 'flex', flexDirection: 'column' }}>
        {loading && <div style={{ color: 'var(--text-muted)', fontSize: 11, padding: '20px 0', textAlign: 'center' }}>Loading…</div>}
        {error && (() => {
          const fmt = formatClickHouseError(error);
          return (
            <div style={{ color: fmt.isPermissionError ? '#d29922' : '#f85149', fontSize: 11, padding: '8px 0', cursor: 'help' }} title={error}>
              ⚠ {fmt.message}
            </div>
          );
        })()}
        {!loading && !error && result && (
          <>
            {/* Chart view */}
            {view === 'chart' && hasChart && (
              <div style={{ marginBottom: fullscreen ? 0 : 6, flex: 1, minHeight: fullscreen ? 0 : (chartStyle === '3d' ? 250 : 180), position: 'relative' }}>
                {chartStyle === '3d' ? (
                  <Chart3DCanvas key={fullscreen ? 'fs' : 'normal'} data={chartData} type={chartType}
                    orientation={chartDirective?.orientation}
                    groupedData={isGroupedChart ? groupedChartData : undefined}
                    isFullscreen={fullscreen}
                    onDrillDown={isDrillable ? handleDrillDown : undefined} />
                ) : (
                  <ChartRenderer chartType={chartType} data={chartData} groupedData={groupedChartData}
                    orientation={chartDirective?.orientation} fullHeight unit={chartUnit} color={chartColor}
                    onDrillDown={isDrillable ? handleDrillDown : undefined}
                    drillIntoQuery={isDrillable ? preset?.directives.drill?.into : undefined}
                    valueColumns={chartDirective?.valueColumns}
                    hoveredTimestamp={hoveredTimestamp} onTimestampHover={onTimestampHover} correlationValues={correlationValues} currentPanelName={preset?.name} isHoveredPanel={isHoveredPanel} />
                )}
              </div>
            )}

            {/* Table view */}
            {(view === 'table' || !hasChart) && (() => {
              const handleSort = (col: string) => {
                if (sortCol === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
                else { setSortCol(col); setSortDir('desc'); }
              };
              const sortedRows = sortCol ? sortRows(result.rows, sortCol, sortDir) : result.rows;
              return (
                <div style={{ flex: 1, overflow: 'auto', borderRadius: 4, border: '1px solid var(--border-secondary)' }}>
                  <ResultsTable
                    columns={result.columns}
                    rows={sortedRows}
                    sortColumn={sortCol}
                    sortDirection={sortDir}
                    onSort={handleSort}
                    linkOnColumn={isLinkable ? preset?.directives.link?.on : undefined}
                    ragRules={preset?.directives.rag}
                    onLinkClick={isLinkable ? handleLinkClick : undefined}
                    drillOnColumn={isDrillable ? preset?.directives.drill?.on : undefined}
                    onDrillClick={isDrillable ? ((_col: string, value: string) => handleDrillDown({ label: value, value: 0 })) : undefined}
                    drillIntoQuery={isDrillable ? preset?.directives.drill?.into : undefined}
                    compact
                  />
                </div>
              );
            })()}

            <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4, flexShrink: 0 }}>
              {result.rows.length} row{result.rows.length !== 1 ? 's' : ''}
            </div>
          </>
        )}
      </div>
      {linkModal && (
        <LinkQueryModal
          targetQuery={linkModal.targetQuery}
          params={linkModal.params}
          parentDrillParams={drillParams}
          onClose={() => setLinkModal(null)}
        />
      )}
    </div>
  );
}

const viewToggleStyle: React.CSSProperties = {
  background: 'none', border: 'none', cursor: 'pointer',
  fontSize: 11, padding: '2px 6px', lineHeight: 1,
};;

const panelStyle: React.CSSProperties = {
  background: 'var(--bg-card, var(--bg-secondary))',
  border: '1px solid var(--border-primary)',
  borderRadius: 8,
  display: 'flex',
  flexDirection: 'column',
  overflowX: 'hidden',
  overflowY: 'visible',
  height: '100%',
};

// ─── Dashboard editor modal ───

interface EditorProps {
  initial?: Dashboard;
  onSave: (d: Omit<Dashboard, 'builtin'>) => void;
  onCancel: () => void;
}

const DashboardEditor: React.FC<EditorProps> = ({ initial, onSave, onCancel }) => {
  const [title, setTitle] = useState(initial?.title ?? '');
  const [description, setDescription] = useState(initial?.description ?? '');
  const [columns, setColumns] = useState<1 | 2 | 3 | 4>(initial?.columns ?? 2);
  const [panels, setPanels] = useState<DashboardPanel[]>(initial?.panels ?? []);
  const [addQuery, setAddQuery] = useState('');

  const availableQueries = getAllQueries()
    .filter(q => !panels.some(p => p.queryName === `${q.group}#${q.name}`))
    .sort((a, b) => a.group.localeCompare(b.group) || a.name.localeCompare(b.name));

  const handleAdd = () => {
    if (!addQuery) return;
    setPanels(prev => [...prev, { queryName: addQuery }]);  // addQuery is already namespaced
    setAddQuery('');
  };

  const handleRemove = (idx: number) => {
    setPanels(prev => prev.filter((_, i) => i !== idx));
  };

  const handleMoveUp = (idx: number) => {
    if (idx === 0) return;
    setPanels(prev => {
      const next = [...prev];
      [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
      return next;
    });
  };

  const handleMoveDown = (idx: number) => {
    setPanels(prev => {
      if (idx >= prev.length - 1) return prev;
      const next = [...prev];
      [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
      return next;
    });
  };

  const canSave = title.trim().length > 0 && panels.length > 0;

  return (
    <div style={{ padding: 24, maxWidth: 600 }}>
      <h3 style={{ margin: '0 0 20px', fontSize: 16, fontWeight: 600, color: 'var(--text-primary)' }}>
        {initial ? 'Edit Dashboard' : 'New Dashboard'}
      </h3>

      <label style={labelStyle}>Title</label>
      <input value={title} onChange={e => setTitle(e.target.value)} placeholder="My Dashboard"
        style={inputStyle} />

      <label style={labelStyle}>Description (optional)</label>
      <input value={description} onChange={e => setDescription(e.target.value)} placeholder="What this dashboard monitors"
        style={inputStyle} />

      <label style={labelStyle}>Grid Columns</label>
      <div className="tabs" style={{ display: 'inline-flex', marginBottom: 16 }}>
        {([1, 2, 3, 4] as const).map(n => (
          <button key={n} onClick={() => setColumns(n)}
            className={`tab${columns === n ? ' active' : ''}`}
            style={{ border: 'none', padding: '6px 16px', fontSize: 13 }}>
            {n}
          </button>
        ))}
      </div>

      <label style={labelStyle}>Panels ({panels.length})</label>
      <div style={{
        marginBottom: 16, borderRadius: 8, border: '1px solid var(--border-primary)',
        background: 'var(--bg-secondary)', overflow: 'hidden',
      }}>
        {panels.length === 0 && (
          <div style={{ padding: '12px 14px', fontSize: 12, color: 'var(--text-muted)', fontStyle: 'italic' }}>
            No panels added yet
          </div>
        )}
        {panels.map((p, i) => {
          const resolved = resolvePanel(p);
          return (
            <div key={i} style={{
              display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px',
              borderBottom: i < panels.length - 1 ? '1px solid var(--border-secondary)' : 'none'
            }}>
              <span style={{ flex: 1, fontSize: 12, color: resolved ? 'var(--text-secondary)' : '#f85149' }}>
                {resolved?.name ?? p.queryName}{!resolved && ' (not found)'}
              </span>
              <button onClick={() => handleMoveUp(i)} disabled={i === 0} style={iconBtnStyle} title="Move up">↑</button>
              <button onClick={() => handleMoveDown(i)} disabled={i === panels.length - 1} style={iconBtnStyle} title="Move down">↓</button>
              <button onClick={() => handleRemove(i)} style={{ ...iconBtnStyle, color: '#f85149' }} title="Remove">×</button>
            </div>
          );
        })}
      </div>

      <div style={{ display: 'flex', gap: 8, marginBottom: 24 }}>
        <select value={addQuery} onChange={e => setAddQuery(e.target.value)}
          style={{ ...inputStyle, flex: 1, marginBottom: 0 }}>
          <option value="">Add a preset query…</option>
          {availableQueries.map(q => (
            <option key={q.name} value={`${q.group}#${q.name}`}>{q.group} — {q.name}</option>
          ))}
        </select>
        <button onClick={handleAdd} disabled={!addQuery}
          style={{ ...primaryBtnStyle, opacity: addQuery ? 1 : 0.4, cursor: addQuery ? 'pointer' : 'not-allowed' }}>
          Add
        </button>
      </div>

      <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', borderTop: '1px solid var(--border-secondary)', paddingTop: 16 }}>
        <button onClick={onCancel} style={secondaryBtnStyle}>Cancel</button>
        <button onClick={() => onSave({ id: initial?.id ?? '', title: title.trim(), description: description.trim() || undefined, columns, panels })}
          disabled={!canSave}
          style={{ ...primaryBtnStyle, opacity: canSave ? 1 : 0.4, cursor: canSave ? 'pointer' : 'not-allowed' }}>
          {initial ? 'Save Changes' : 'Create Dashboard'}
        </button>
      </div>
    </div>
  );
};

const labelStyle: React.CSSProperties = { display: 'block', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.3px' };
const inputStyle: React.CSSProperties = { width: '100%', padding: '8px 12px', fontSize: 13, borderRadius: 6, border: '1px solid var(--border-input, var(--border-primary))', background: 'var(--bg-input, var(--bg-tertiary))', color: 'var(--text-primary)', marginBottom: 16, boxSizing: 'border-box', fontFamily: 'inherit', transition: 'border-color 0.15s ease' };
const iconBtnStyle: React.CSSProperties = { background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', fontSize: 14, padding: '2px 4px', transition: 'color 0.15s ease' };
const primaryBtnStyle: React.CSSProperties = { padding: '7px 18px', fontSize: 12, borderRadius: 7, border: '1px solid var(--border-primary)', background: 'var(--bg-primary)', color: 'var(--text-primary)', fontWeight: 600, cursor: 'pointer', transition: 'all 0.15s ease', boxShadow: '0 1px 3px rgba(0,0,0,0.08)' };
const secondaryBtnStyle: React.CSSProperties = { padding: '7px 18px', fontSize: 12, borderRadius: 7, border: '1px solid var(--border-primary)', background: 'var(--bg-secondary)', color: 'var(--text-secondary)', cursor: 'pointer', transition: 'all 0.15s ease' };

// ─── Import/Export modal ───

const ImportModal: React.FC<{ onImport: (json: string) => void; onClose: () => void }> = ({ onImport, onClose }) => {
  const [json, setJson] = useState('');
  const [err, setErr] = useState<string | null>(null);

  const handleImport = () => {
    try {
      importDashboardJson(json); // validate
      onImport(json);
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Invalid JSON');
    }
  };

  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'rgba(0,0,0,0.5)' }}
      onClick={onClose}>
      <div style={{ background: 'var(--bg-secondary)', borderRadius: 8, border: '1px solid var(--border-primary)', padding: 20, width: 500, maxHeight: '80vh', overflow: 'auto' }}
        onClick={e => e.stopPropagation()}>
        <h3 style={{ margin: '0 0 12px', fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>Import Dashboard JSON</h3>
        <textarea value={json} onChange={e => { setJson(e.target.value); setErr(null); }}
          placeholder='Paste dashboard JSON here…'
          style={{ width: '100%', height: 200, padding: 10, fontSize: 12, fontFamily: "'Share Tech Mono',monospace", borderRadius: 4, border: '1px solid var(--border-primary)', background: 'var(--bg-card)', color: 'var(--text-primary)', resize: 'vertical', boxSizing: 'border-box' }} />
        {err && <div style={{ color: '#f85149', fontSize: 11, marginTop: 6 }}>⚠ {err}</div>}
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 12 }}>
          <button onClick={onClose} style={secondaryBtnStyle}>Cancel</button>
          <button onClick={handleImport} disabled={!json.trim()} style={{ ...primaryBtnStyle, opacity: json.trim() ? 1 : 0.4 }}>Import</button>
        </div>
      </div>
    </div>
  );
};

// ─── Mini panel — lightweight version for previews (no drill-down, no toolbar) ───

const MiniPanelCard: React.FC<{ panel: DashboardPanel; timeRangeOverride: string | null }> = ({ panel, timeRangeOverride }) => {
  const services = useClickHouseServices();
  const preset = resolvePanel(panel);
  const [result, setResult] = useState<PanelResult | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!services || !preset) return;
    let cancelled = false;
    setLoading(true);
    const sql = resolveTimeRange(preset.sql, preset.directives.meta?.interval, timeRangeOverride);
    services.adapter.executeQuery<Record<string, unknown>>(sql)
      .then(rows => {
        if (cancelled) return;
        const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
        setResult({ columns, rows });
      })
      .catch(() => { if (!cancelled) setResult(null); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [services, preset, timeRangeOverride]);

  const chartDirective = useMemo(() => preset ? parseChartDirective(preset.sql) : null, [preset]);
  const chartData = useMemo((): ChartDataPoint[] => {
    if (!result || result.rows.length === 0) return [];
    const isTimeSeries = chartDirective?.type && ['line', 'area', 'grouped_line'].includes(chartDirective.type);
    return buildChartData(result.rows, result.columns, chartDirective?.groupByColumn, chartDirective?.valueColumn, isTimeSeries ? undefined : 20, chartDirective?.descriptionColumn);
  }, [result, chartDirective]);

  const groupedChartData2 = useMemo((): GroupedChartData[] => {
    if (!result || !chartDirective?.groupByColumn || !chartDirective?.valueColumn) return [];
    const isTimeSeries = chartDirective?.type && ['line', 'area', 'grouped_line'].includes(chartDirective.type);
    return buildGroupedChartData(result.rows, chartDirective.groupByColumn, chartDirective.valueColumn, chartDirective.seriesColumn, chartDirective.valueColumns, isTimeSeries ? undefined : 20);
  }, [result, chartDirective]);

  const chartType: ChartType = chartDirective?.type || preset?.directives.chart?.type || 'bar';
  const chartStyle = chartDirective?.visualization || preset?.directives.chart?.style || '2d';
  const miniChartColor = chartDirective?.color;
  const isGroupedChart2 = isGroupedChartType(chartType);
  const hasChart = isGroupedChart2 ? groupedChartData2.length > 0 : chartData.length > 0;

  if (!preset) return null;

  return (
    <div style={{
      background: 'var(--bg-card, var(--bg-secondary))',
      border: '1px solid var(--border-secondary)',
      borderRadius: 6,
      overflow: 'hidden',
      display: 'flex',
      flexDirection: 'column',
      height: 140,
    }}>
      <div style={{ padding: '5px 8px 2px', fontSize: 10, fontWeight: 600, color: 'var(--text-secondary)', flexShrink: 0, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
        {preset.name}
      </div>
      <div style={{ flex: 1, minHeight: 0, padding: '0 4px 4px', overflow: 'hidden' }}>
        {loading && <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 10 }}>…</div>}
        {!loading && hasChart && (
          <>
            {chartStyle === '3d' ? (
              <Chart3DCanvas data={chartData} type={chartType} orientation={chartDirective?.orientation} groupedData={isGroupedChart2 ? groupedChartData2 : undefined} />
            ) : (
              <ChartRenderer chartType={chartType} data={chartData} groupedData={groupedChartData2}
                orientation={chartDirective?.orientation} fullHeight color={miniChartColor}
                valueColumns={chartDirective?.valueColumns} />
            )}
          </>
        )}
        {!loading && !hasChart && result && (
          <div style={{ fontSize: 9, color: 'var(--text-muted)', padding: '4px 0', lineHeight: 1.6 }}>
            {result.rows.slice(0, 5).map((r, i) => (
              <div key={i} style={{ display: 'flex', gap: 4, overflow: 'hidden' }}>
                {result.columns.map(c => (
                  <span key={c} style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
                    {formatCell(r[c])}
                  </span>
                ))}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

// ─── Hover preview popup ───

const DashboardPreviewPopup: React.FC<{
  dashboard: Dashboard;
  mouseX: number;
  mouseY: number;
}> = ({ dashboard, mouseX, mouseY }) => {
  const POPUP_W = 720;
  const POPUP_H = 530;
  const CURSOR_GAP = 16; // gap between cursor and popup edge
  const SCREEN_PAD = 10; // min distance from viewport edges

  // Prefer right of cursor; flip to left if it would overflow
  let left = mouseX + CURSOR_GAP;
  if (left + POPUP_W > window.innerWidth - SCREEN_PAD) {
    left = mouseX - POPUP_W - CURSOR_GAP;
  }
  // Prefer below cursor; flip above if it would overflow
  let top = mouseY + CURSOR_GAP;
  if (top + POPUP_H > window.innerHeight - SCREEN_PAD) {
    top = mouseY - POPUP_H - CURSOR_GAP;
  }
  // Hard-clamp to viewport
  left = Math.max(SCREEN_PAD, left);
  top = Math.max(SCREEN_PAD, top);

  const cols = Math.min(dashboard.columns, 2); // at most 2 cols in preview
  const PANEL_H = 160;
  const MAX_PANELS = cols * 3;

  return (
    <div style={{
      position: 'fixed',
      left,
      top,
      width: POPUP_W,
      height: POPUP_H,
      zIndex: 9000,
      background: 'var(--bg-card, var(--bg-secondary))',
      backdropFilter: 'blur(18px) saturate(1.4)',
      WebkitBackdropFilter: 'blur(18px) saturate(1.4)',
      border: '1px solid rgba(var(--accent-primary-rgb, 99,102,241), 0.55)',
      borderRadius: 12,
      boxShadow: '0 20px 60px rgba(0,0,0,0.55), inset 0 1px 0 rgba(255,255,255,0.06)',
      opacity: 0.88,
      overflow: 'hidden',
      display: 'flex',
      flexDirection: 'column',
      pointerEvents: 'none',
      animation: 'popupReveal 0.5s cubic-bezier(0.22,1,0.36,1) both',
    }}>
      <style>{`
        @keyframes popupReveal {
          from { opacity: 0; transform: scale(0.96) translateY(8px); filter: blur(6px); }
          to   { opacity: 1; transform: scale(1)    translateY(0);   filter: blur(0);   }
        }
        @keyframes panelColourIn {
          from { opacity: 0; transform: translateY(6px); filter: grayscale(1) brightness(0.6); }
          to   { opacity: 1; transform: translateY(0);   filter: grayscale(0) brightness(1);   }
        }
        .preview-panel {
          animation: panelColourIn 0.45s cubic-bezier(0.22,1,0.36,1) both;
        }
      `}</style>
      {/* Header */}
      <div style={{
        padding: '10px 16px 8px',
        flexShrink: 0,
        borderBottom: '1px solid var(--border-secondary)',
        display: 'flex',
        alignItems: 'baseline',
        gap: 10,
      }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-primary)' }}>{dashboard.title}</span>
        {dashboard.description && (
          <span style={{ fontSize: 11, color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {dashboard.description}
          </span>
        )}
      </div>
      {/* Mini grid */}
      <div style={{
        flex: 1,
        padding: 12,
        overflow: 'hidden',
        display: 'grid',
        gridTemplateColumns: `repeat(${cols}, 1fr)`,
        gridAutoRows: `${PANEL_H}px`,
        gap: 10,
        alignContent: 'start',
      }}>
        {dashboard.panels.slice(0, MAX_PANELS).map((panel, i) => (
          <div
            key={`${panel.queryName}-${i}`}
            className="preview-panel"
            style={{ animationDelay: `${i * 0.12}s` }}
          >
            <MiniPanelCard panel={panel} timeRangeOverride="1 DAY" />
          </div>
        ))}
      </div>
      {/* Footer hint */}
      <div style={{ flexShrink: 0, padding: '5px 16px 7px', fontSize: 10, color: 'var(--text-muted)', borderTop: '1px solid var(--border-secondary)' }}>
        Click to open · showing {Math.min(dashboard.panels.length, MAX_PANELS)} of {dashboard.panels.length} panels
      </div>
    </div>
  );
};

// ─── Dashboard list (3-column) with hover preview ───

const DashboardListView: React.FC<{
  dashboards: Dashboard[];
  onOpen: (id: string) => void;
  onImport: () => void;
  onNew: () => void;
}> = ({ dashboards, onOpen, onImport, onNew }) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [mousePos, setMousePos] = useState<{ x: number; y: number } | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleMouseEnter = (id: string, e: React.MouseEvent<HTMLDivElement>) => {
    (e.currentTarget as HTMLDivElement).style.borderColor = 'var(--accent-primary)';
    // start 1-second delay before showing preview
    if (hoverTimer.current) clearTimeout(hoverTimer.current);
    const x = e.clientX; const y = e.clientY;
    hoverTimer.current = setTimeout(() => {
      setHoveredId(id);
      setMousePos({ x, y });
    }, 1000);
  };
  const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    setMousePos({ x: e.clientX, y: e.clientY });
  };
  const handleMouseLeave = (e: React.MouseEvent<HTMLDivElement>) => {
    if (hoverTimer.current) { clearTimeout(hoverTimer.current); hoverTimer.current = null; }
    setHoveredId(null);
    setMousePos(null);
    (e.currentTarget as HTMLDivElement).style.borderColor = 'var(--border-primary)';
  };

  const hoveredDashboard = hoveredId ? dashboards.find(d => d.id === hoveredId) : null;

  // Group dashboards by their group field
  const grouped = useMemo(() => {
    const groups: { group: DashboardGroup; color: string; items: Dashboard[] }[] =
      DASHBOARD_GROUPS.map(g => ({ group: g.name, color: g.color, items: [] }));
    for (const d of dashboards) {
      const g = groups.find(g => g.group === (d.group ?? 'Custom'));
      if (g) g.items.push(d);
      else groups[groups.length - 1].items.push(d); // fallback to Custom
    }
    return groups.filter(g => g.items.length > 0);
  }, [dashboards]);

  return (
    <div ref={containerRef} style={{ padding: 24, overflow: 'auto', height: '100%', position: 'relative' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 20 }}>
        <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>Dashboards</span>
        <div style={{ display: 'flex', gap: 6 }}>
          <button onClick={onImport} style={secondaryBtnStyle}>Import JSON</button>
          <button onClick={onNew} style={secondaryBtnStyle}>+ New Dashboard</button>
        </div>
      </div>

      {grouped.map(({ group, color, items }) => (
        <div key={group} style={{ marginBottom: 24 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
            <div style={{ width: 3, height: 16, borderRadius: 2, background: color }} />
            <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>{group}</span>
            <span style={{ fontSize: 10, color: 'var(--text-tertiary)' }}>({items.length})</span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
            {items.map(d => (
              <div
                key={d.id}
                onClick={() => onOpen(d.id)}
                onMouseEnter={e => handleMouseEnter(d.id, e)}
                onMouseMove={handleMouseMove}
                onMouseLeave={handleMouseLeave}
                style={{ ...panelStyle, cursor: 'pointer', padding: 16, transition: 'border-color 0.15s' }}
              >
                <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}>{d.title}</div>
                {d.description && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8, lineHeight: 1.4 }}>{d.description}</div>}
                <div style={{ fontSize: 10, color: 'var(--text-tertiary)' }}>
                  {d.panels.length} panel{d.panels.length !== 1 ? 's' : ''} · {d.columns} col{d.columns !== 1 ? 's' : ''}
                  {d.builtin && <span style={{ marginLeft: 8, color }}>built-in</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}

      {dashboards.length === 0 && (
        <div style={{ textAlign: 'center', padding: '40px 0', color: 'var(--text-muted)', fontSize: 13 }}>
          No dashboards yet. Create one or import a JSON definition.
        </div>
      )}

      {/* Hover preview — uses fixed positioning, follows cursor */}
      {hoveredDashboard && mousePos && (
        <DashboardPreviewPopup
          dashboard={hoveredDashboard}
          mouseX={mousePos.x}
          mouseY={mousePos.y}
        />
      )}
    </div>
  );
};

// ─── Correlation summary strip ───

// ─── Main DashboardViewer ───

type ViewState = { mode: 'list' } | { mode: 'view'; dashboardId: string } | { mode: 'edit'; dashboard?: Dashboard } | { mode: 'import' };

export const DashboardViewer: React.FC<{ initialDashboardId?: string }> = ({ initialDashboardId }) => {
  const [dashboards, setDashboards] = useState<Dashboard[]>(() => loadDashboards());
  const [fullscreenPanelIndex, setFullscreenPanelIndex] = useState<number | null>(null);
  const [view, setView] = useState<ViewState>(() => {
    if (initialDashboardId) {
      const dbs = loadDashboards();
      if (dbs.some(d => d.id === initialDashboardId)) {
        return { mode: 'view', dashboardId: initialDashboardId };
      }
    }
    return { mode: 'list' };
  });

  // When navigating back from query editor with a dashboard ID, open that dashboard
  useEffect(() => {
    if (initialDashboardId && dashboards.some(d => d.id === initialDashboardId)) {
      setView({ mode: 'view', dashboardId: initialDashboardId });
    }
  }, [initialDashboardId, dashboards]);
  const [timeRangeOverride, setTimeRangeOverride] = useState<string | null>('1 HOUR');

  // ─── Cross-panel correlation ───
  const [correlationEnabled, setCorrelationEnabled] = useState(false);
  const [hoveredTimestamp, setHoveredTimestamp] = useState<string | null>(null);
  const [hoveredPanelIndex, setHoveredPanelIndex] = useState<number | null>(null);
  // Keep last non-null timestamp so the strip doesn't flicker away on mouse leave
  const lastTimestampRef = useRef<string | null>(null);
  if (hoveredTimestamp) lastTimestampRef.current = hoveredTimestamp;
  const displayTimestamp = hoveredTimestamp ?? lastTimestampRef.current;
  const panelDataRef = useRef<Map<number, PanelTimeSeriesInfo>>(new Map());

  const handlePanelData = useCallback((panelIndex: number) => (info: PanelTimeSeriesInfo | null) => {
    if (info) panelDataRef.current.set(panelIndex, info);
    else panelDataRef.current.delete(panelIndex);
  }, []);

  // Collect correlation values sorted by panel order (matching dashboard layout)
  const correlationValues = useMemo(() => {
    if (!correlationEnabled || !displayTimestamp) return [];
    const entries: [number, CorrelationEntry][] = [];
    for (const [idx, info] of panelDataRef.current) {
      const val = info.dataByLabel.get(displayTimestamp) ?? null;
      entries.push([idx, { name: info.name, color: info.color, value: val, unit: info.unit }]);
    }
    entries.sort((a, b) => a[0] - b[0]);
    return entries.map(([, e]) => e);
  }, [correlationEnabled, displayTimestamp]);

  // Clear correlation state when switching dashboards
  const activeDashboardId = view.mode === 'view' ? view.dashboardId : null;
  useEffect(() => {
    panelDataRef.current.clear();
    setHoveredTimestamp(null);
    lastTimestampRef.current = null;
  }, [activeDashboardId]);

  const activeDashboard = view.mode === 'view'
    ? dashboards.find(d => d.id === view.dashboardId)
    : undefined;

  const handleSave = useCallback((d: Omit<Dashboard, 'builtin'>) => {
    const next = upsertDashboard(dashboards, d);
    setDashboards(next);
    const saved = next.find(x => x.title === d.title) ?? next[next.length - 1];
    setView({ mode: 'view', dashboardId: saved.id });
  }, [dashboards]);

  const handleDelete = useCallback((id: string) => {
    if (!confirm('Delete this dashboard?')) return;
    const next = deleteDashboard(dashboards, id);
    setDashboards(next);
    setView({ mode: 'list' });
  }, [dashboards]);

  const handleExport = useCallback((d: Dashboard) => {
    const json = exportDashboardJson(d);
    navigator.clipboard.writeText(json).catch(() => { });
    alert('Dashboard JSON copied to clipboard');
  }, []);

  const handleImport = useCallback((json: string) => {
    try {
      const imported = importDashboardJson(json);
      const next = upsertDashboard(dashboards, imported);
      setDashboards(next);
      const added = next[next.length - 1];
      setView({ mode: 'view', dashboardId: added.id });
    } catch { /* ImportModal already validates */ }
  }, [dashboards]);

  const handleClone = useCallback((d: Dashboard) => {
    const clone: Omit<Dashboard, 'builtin'> = {
      id: '',
      title: d.title + ' (copy)',
      description: d.description,
      columns: d.columns,
      panels: [...d.panels],
    };
    const next = upsertDashboard(dashboards, clone);
    setDashboards(next);
    const added = next[next.length - 1];
    setView({ mode: 'edit', dashboard: next.find(x => x.id === added.id) });
  }, [dashboards]);

  // ─── List view ───
  if (view.mode === 'list') {
    return (
      <DashboardListView
        dashboards={dashboards}
        onOpen={id => setView({ mode: 'view', dashboardId: id })}
        onImport={() => setView({ mode: 'import' })}
        onNew={() => setView({ mode: 'edit' })}
      />
    );
  }

  // ─── Import modal ───
  if (view.mode === 'import') {
    return (
      <>
        <ImportModal onImport={handleImport} onClose={() => setView({ mode: 'list' })} />
        <div style={{ padding: 24, opacity: 0.3 }}>
          <span style={{ color: 'var(--text-muted)' }}>Importing…</span>
        </div>
      </>
    );
  }

  // ─── Editor ───
  if (view.mode === 'edit') {
    return (
      <div style={{ overflow: 'auto', height: '100%' }}>
        <DashboardEditor
          initial={view.dashboard}
          onSave={handleSave}
          onCancel={() => view.dashboard ? setView({ mode: 'view', dashboardId: view.dashboard.id }) : setView({ mode: 'list' })}
        />
      </div>
    );
  }

  // ─── Dashboard grid view ───
  if (!activeDashboard) {
    return (
      <div style={{ padding: 24, color: 'var(--text-muted)', fontSize: 13 }}>
        Dashboard not found. <button onClick={() => setView({ mode: 'list' })} style={{ ...secondaryBtnStyle, fontSize: 11 }}>Back to list</button>
      </div>
    );
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* Dashboard header */}
      <div style={{ flexShrink: 0, padding: '12px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)', background: 'var(--bg-secondary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <button onClick={() => setView({ mode: 'list' })} style={{ ...iconBtnStyle, fontSize: 16 }} title="Back to list">←</button>
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>{activeDashboard.title}</div>
            {activeDashboard.description && <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{activeDashboard.description}</div>}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <TimeRangePicker value={timeRangeOverride} onChange={setTimeRangeOverride} />
          <button
            onClick={() => { setCorrelationEnabled(e => !e); setHoveredTimestamp(null); }}
            className={`tab${correlationEnabled ? ' active' : ''}`}
            style={{ border: '1px solid var(--border-primary)', padding: '6px 16px', fontSize: 12, cursor: 'pointer' }}
            title="Cross-panel time correlation — hover one chart to see values across all panels"
          >
            Correlate
          </button>
          <button onClick={() => handleExport(activeDashboard)} style={secondaryBtnStyle} title="Copy JSON to clipboard">Export</button>
          <button onClick={() => handleClone(activeDashboard)} style={secondaryBtnStyle}>Clone</button>
          <button onClick={() => setView({ mode: 'edit', dashboard: activeDashboard })} style={secondaryBtnStyle}>Edit</button>
          {!activeDashboard.builtin && (
            <button onClick={() => handleDelete(activeDashboard.id)} style={{ ...secondaryBtnStyle, color: '#f85149', borderColor: 'rgba(248,81,73,0.3)' }}>Delete</button>
          )}
        </div>
      </div>

      {/* Grid of panels */}
      <div style={{ flex: 1, overflow: 'auto', padding: 24 }}>
        <div style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${activeDashboard.columns}, 1fr)`,
          gridAutoRows: '420px',
          gap: 16,
          position: 'relative', zIndex: 0,
        }}>
          {activeDashboard.panels.map((panel, i) => (
            <DashboardPanelCard
              key={`${panel.queryName}-${i}`}
              panel={panel}
              timeRangeOverride={timeRangeOverride}
              dashboardId={activeDashboard.id}
              isFullscreen={fullscreenPanelIndex === i}
              onToggleFullscreen={() => setFullscreenPanelIndex(prev => prev === i ? null : i)}
              isHidden={fullscreenPanelIndex !== null && fullscreenPanelIndex !== i}
              hoveredTimestamp={correlationEnabled ? hoveredTimestamp : undefined}
              onTimestampHover={correlationEnabled ? (ts: string | null) => { setHoveredTimestamp(ts); if (ts !== null) setHoveredPanelIndex(i); } : undefined}
              onTimeSeriesData={correlationEnabled ? handlePanelData(i) : undefined}
              correlationValues={correlationEnabled ? correlationValues : undefined}
              isHoveredPanel={correlationEnabled ? hoveredPanelIndex === i : undefined}
            />
          ))}
        </div>
      </div>
    </div>
  );
};

export default DashboardViewer;
