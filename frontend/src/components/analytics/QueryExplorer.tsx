/**
 * QueryExplorer — run preset (or custom) monitoring queries with inline charts.
 * Mirrors the k8s-compass AnalyticsView pattern:
 *   - Metadata-driven presets (-- @meta / -- @chart in SQL)
 *   - "Show All" queries grid with categories
 *   - 2D SVG charts + 3D Three.js charts
 *   - Chart controls (type, group_by, value, series, 2D/3D style)
 *   - SQL syntax highlighting overlay
 *   - Resizable editor pane
 */

import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { PRESET_QUERIES } from './presetQueries';
import { type Query } from './types';
import { formatClickHouseError } from '../../utils/errorFormatters';
import {
  addCustomQuery, deleteCustomQuery, loadCustomQueries, isQueryNameTaken,
  buildCustomQuerySql, getAllQueries as getAllQueriesFromPresets,
} from './customQueries';
import { resolveTimeRange, resolveDrillParams, isDrillTarget } from './templateResolution';

const MAX_SIDEBAR_QUERIES = 12;
import {
  QUERY_GROUPS, CHART_TYPE_LABELS,
  type QueryGroup, type ChartType, type ChartStyle,
  parseCellStyles, parseRagRules, parseChartDirective, parseDirectives, resolveQueryRef,
} from './metaLanguage';
import { LinkQueryModal } from './LinkQueryModal';
import { PartInspector } from '../database/PartInspector';
import { databaseApi } from '../../stores/databaseStore';
import type { PartDetailInfo } from '@tracehouse/core';
import {
  isNumericValue, formatCell,
  buildChartData, buildGroupedChartData, isGroupedChartType, sortRows,
  ChartRenderer,
  type ChartDataPoint, type ChartConfig, type GroupedChartData, type DrillDownEvent,
} from './charts';
import { Chart3DCanvas } from './charts3d';
import { ResultsTable } from './ResultsTable';
import { highlightSQL } from '../../utils/sqlHighlighter';
import DOMPurify from 'dompurify';
import { TimeRangePicker } from './TimeRangePicker';
import { SqlEditor } from './editor/SqlEditor';
import { useClusterStore } from '../../stores/clusterStore';
import { ClusterService, type ChFunction } from '@tracehouse/core';
import { toGrafanaPanel, type GrafanaExportInput } from '@tracehouse/core/services/grafana-export';

/* ═══════════════════════════════════════════════════════════════════════════
   SQL Syntax Highlighting — imported from utils/sqlHighlighter
   ═══════════════════════════════════════════════════════════════════════════ */

/* Chart helpers, types, and 2D SVG charts are imported from ./charts */


/* 3D charts imported from ./charts3d */

/* ═══════════════════════════════════════════════════════════════════════════
   Constants
   ═══════════════════════════════════════════════════════════════════════════ */

const NEW_QUERY_TEMPLATE_BODY = `SELECT
    database,
    table,
    count() AS count
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY count DESC
LIMIT 10`;

/* ═══════════════════════════════════════════════════════════════════════════
   Types
   ═══════════════════════════════════════════════════════════════════════════ */

interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTime: number;
}

type ViewMode = 'table' | 'chart' | 'queries';

interface DrillStackEntry {
  queryName: string;
  params: Record<string, string>;
}

import type { AnalyticsUrlState } from '../../hooks/useUrlState';

/* ═══════════════════════════════════════════════════════════════════════════
   Main Component
   ═══════════════════════════════════════════════════════════════════════════ */

export interface GrafanaExportPayload {
  panel: ReturnType<typeof toGrafanaPanel>;
  title: string;
}

interface QueryExplorerProps {
  urlState?: AnalyticsUrlState;
  onUrlStateChange?: (partial: Partial<AnalyticsUrlState>) => void;
  /** When provided, shows an "Export to Grafana" button that calls this with the panel JSON. */
  onExportToGrafana?: (payload: GrafanaExportPayload) => void;
}

export const QueryExplorer: React.FC<QueryExplorerProps> = ({ urlState, onUrlStateChange, onExportToGrafana }) => {
  const services = useClickHouseServices();
  const { clusterName } = useClusterStore();
  const [customQueries, setCustomQueries] = useState<Query[]>(() => loadCustomQueries());
  const allQueries = useMemo(() => [...PRESET_QUERIES, ...customQueries], [customQueries]);
  const allQueryEntries = useMemo(() => allQueries.map(q => ({
    name: q.name,
    group: q.group,
    isLinkTarget: isDrillTarget(q.sql),
  })), [allQueries]);

  // Initialize from URL state if available
  const initialPreset = urlState?.preset !== undefined ? allQueries[urlState.preset] : undefined;
  const initialSql = urlState?.sql ?? initialPreset?.sql ?? allQueries[0]?.sql ?? '';
  const initialView: ViewMode = (urlState?.view as ViewMode) ?? 'table';

  const [sql, setSql] = useState(initialSql);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>(initialView);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [editorHeight, setEditorHeight] = useState(240);
  const [querySearch, setQuerySearch] = useState('');
  const [copied, setCopied] = useState(false);
  const [grafanaCopied, setGrafanaCopied] = useState(false);
  const [sortConfig, setSortConfig] = useState<{ column: string; direction: 'asc' | 'desc' } | null>(null);
  const [chartConfig, setChartConfig] = useState<ChartConfig>({
    type: (urlState?.chart as ChartType) ?? 'bar',
    groupByColumn: urlState?.group_by ?? '',
    valueColumn: urlState?.value ?? '',
    seriesColumn: urlState?.series,
    orientation: undefined,
    visualization: (urlState?.style as ChartStyle) ?? '2d',
  });

  const isDragging = useRef(false);
  const hasAutoExecuted = useRef(false);
  const [isFullscreen, setIsFullscreen] = useState(urlState?.fullscreen ?? false);
  const [timeRangeOverride, setTimeRangeOverride] = useState<string | null>('1 HOUR');
  const [showResolved, setShowResolved] = useState(false);

  /* ── dynamic ClickHouse function list for editor autocomplete ── */
  const [chFunctions, setChFunctions] = useState<ChFunction[] | undefined>();
  useEffect(() => {
    if (!services) return;
    services.databaseExplorer.getFunctions()
      .then(setChFunctions)
      .catch((err) => console.warn('[QueryExplorer] Failed to load ClickHouse functions for autocomplete:', err.message));
  }, [services]);

  /* ── drill-down state ── */
  const [drillStack, setDrillStack] = useState<DrillStackEntry[]>([]);
  const [rootQueryName, setRootQueryName] = useState<string>('');

  /* ── resolved SQL (template variables replaced) ── */
  const resolvedSql = useMemo(() => {
    const activePreset = allQueries.find(p => p.sql.trim() === sql.trim());
    let resolved = resolveTimeRange(sql, activePreset?.directives.meta?.interval, timeRangeOverride);
    const params = drillStack.length > 0 ? drillStack[drillStack.length - 1].params : {};
    resolved = resolveDrillParams(resolved, params);
    resolved = ClusterService.resolveTableRefs(resolved, clusterName);
    return resolved;
  }, [sql, allQueries, timeRangeOverride, drillStack, clusterName]);

  /** Sync current explorer state to URL */
  const syncUrl = useCallback((sqlText: string, view: ViewMode, chart: ChartConfig, fs?: boolean) => {
    if (!onUrlStateChange) return;
    const presetIdx = allQueries.findIndex(p => p.sql.trim() === sqlText.trim());
    onUrlStateChange({
      preset: presetIdx >= 0 ? presetIdx : undefined,
      sql: presetIdx < 0 ? sqlText : undefined,
      view: view !== 'table' ? view : undefined,
      chart: view === 'chart' ? chart.type : undefined,
      group_by: view === 'chart' ? chart.groupByColumn : undefined,
      value: view === 'chart' ? chart.valueColumn : undefined,
      series: view === 'chart' ? chart.seriesColumn : undefined,
      style: view === 'chart' && chart.visualization !== '2d' ? chart.visualization : undefined,
      fullscreen: fs || undefined,
    });
  }, [onUrlStateChange]);

  /* ── run query ── */
  const runQuery = useCallback(async (queryStr?: string, drillParams?: Record<string, string>) => {
    if (!services) return;
    const q = (queryStr ?? sql).trim();
    if (!q) return;
    setIsRunning(true);
    setError(null);
    setResult(null);
    setSortConfig(null);
    const t0 = performance.now();
    const directive = parseChartDirective(q);
    try {
      // Resolve {{time_range}} using the active preset's default interval
      const activePreset = allQueries.find(p => p.sql.trim() === q.trim());
      let resolvedSql = resolveTimeRange(q, activePreset?.directives.meta?.interval, timeRangeOverride);
      // Resolve {{drill:col | fallback}} — uses drill params if provided, else current stack, else empty (standalone)
      const params = drillParams ?? (drillStack.length > 0 ? drillStack[drillStack.length - 1].params : {});
      resolvedSql = resolveDrillParams(resolvedSql, params);
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(resolvedSql);
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
      setResult({ columns, rows, rowCount: rows.length, executionTime: performance.now() - t0 });

      if (directive) {
        const numCol = columns.find(c => rows.some(r => isNumericValue(r[c])));
        const lblCol = columns.find(c => c !== numCol);
        const newChart: ChartConfig = {
          type: directive.type ?? 'bar',
          groupByColumn: directive.groupByColumn ?? lblCol ?? columns[0],
          valueColumn: directive.valueColumn ?? numCol ?? columns[1] ?? columns[0],
          valueColumns: directive.valueColumns,
          seriesColumn: directive.seriesColumn,
          orientation: directive.orientation,
          visualization: directive.visualization ?? '2d',
          title: directive.title,
          description: directive.description,
          unit: directive.unit,
          descriptionColumn: directive.descriptionColumn,
          color: directive.color,
        };
        setChartConfig(newChart);
        setViewMode('chart');
        syncUrl(q, 'chart', newChart);
      } else {
        setViewMode('table');
        if (columns.length >= 2) {
          const numCol = columns.find(c => rows.some(r => isNumericValue(r[c])));
          const lblCol = columns.find(c => c !== numCol);
          const newChart = { ...chartConfig, groupByColumn: lblCol ?? columns[0], valueColumn: numCol ?? columns[1], seriesColumn: undefined, title: undefined, description: undefined };
          setChartConfig(newChart);
          syncUrl(q, 'table', newChart);
        } else {
          syncUrl(q, 'table', chartConfig);
        }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsRunning(false);
    }
  }, [services, sql, chartConfig, syncUrl, timeRangeOverride, allQueries, drillStack]);

  const selectPreset = useCallback((p: Query) => {
    setSql(p.sql);
    setActiveQueryName(p.name);
    setRootQueryName(p.name);
    setDrillStack([]);
    setResult(null);
    setError(null);
    setSortConfig(null);
    setViewMode('table');
    setChartConfig({ type: 'bar', groupByColumn: '', valueColumn: '', seriesColumn: undefined, orientation: undefined, visualization: '2d' });
    // auto-run
    setTimeout(() => runQuery(p.sql), 0);
  }, [runQuery]);

  // Track which query is currently loaded
  const [activeQueryName, setActiveQueryName] = useState<string | null>(
    initialPreset?.name ?? allQueries[0]?.name ?? null
  );
  const activeIsBuiltin = activeQueryName ? PRESET_QUERIES.some(q => q.name === activeQueryName) : false;
  const activeIsCustom = activeQueryName ? customQueries.some(q => q.name === activeQueryName) : false;

  /* ── drill-down handlers ── */
  // Match saved query first; fall back to parsing directives from the live editor SQL
  const currentQuery = useMemo(() => {
    const saved = allQueries.find(q => q.sql.trim() === sql.trim());
    if (saved) return saved;
    // Parse directives from the live SQL so edits (e.g. adding @drill) work without saving
    const directives = parseDirectives(sql);
    if (!directives?.meta) return undefined;
    return { name: directives.meta.title, group: directives.meta.group, directives, sql } as const;
  }, [allQueries, sql]);
  const isDrillable = !!(currentQuery?.directives.drill?.on && currentQuery?.directives.drill?.into) &&
    (!result || result.columns.includes(currentQuery.directives.drill.on));

  const handleDrillDown = useCallback((event: DrillDownEvent) => {
    if (!currentQuery?.directives.drill?.on || !currentQuery?.directives.drill?.into) return;
    const targetQuery = resolveQueryRef(currentQuery.directives.drill!.into, currentQuery.group, allQueries);
    if (!targetQuery) {
      setError(`Drill target query not found: "${currentQuery.directives.drill!.into}"`);
      return;
    }
    // Carry forward existing drill params and add the new one
    const prevParams = drillStack.length > 0 ? drillStack[drillStack.length - 1].params : {};
    const newParams = { ...prevParams, [currentQuery.directives.drill!.on]: event.label };

    // Track root if this is the first drill
    if (drillStack.length === 0 && activeQueryName) {
      setRootQueryName(activeQueryName);
    }

    const newStack = [...drillStack, { queryName: targetQuery.name, params: newParams }];
    setDrillStack(newStack);
    setSql(targetQuery.sql);
    setActiveQueryName(targetQuery.name);
    setResult(null);
    setError(null);
    setSortConfig(null);
    setTimeout(() => runQuery(targetQuery.sql, newParams), 0);
  }, [currentQuery, allQueries, drillStack, activeQueryName, runQuery]);

  const handleBreadcrumbClick = useCallback((index: number) => {
    if (index < 0) {
      // Go back to root
      const rootQuery = allQueries.find(q => q.name === rootQueryName);
      if (!rootQuery) return;
      setDrillStack([]);
      setSql(rootQuery.sql);
      setActiveQueryName(rootQuery.name);
      setResult(null);
      setError(null);
      setTimeout(() => runQuery(rootQuery.sql, {}), 0);
      return;
    }
    const entry = drillStack[index];
    const targetQuery = allQueries.find(q => q.name === entry.queryName);
    if (!targetQuery) return;
    setDrillStack(prev => prev.slice(0, index + 1));
    setSql(targetQuery.sql);
    setActiveQueryName(targetQuery.name);
    setResult(null);
    setError(null);
    setTimeout(() => runQuery(targetQuery.sql, entry.params), 0);
  }, [drillStack, allQueries, rootQueryName, runQuery]);

  /* ── @link modal state ── */
  const [linkModal, setLinkModal] = useState<{ targetQuery: Query; params: Record<string, string> } | null>(null);

  const isLinkable = !!(currentQuery?.directives.link?.on && currentQuery?.directives.link?.into) &&
    (!result || result.columns.includes(currentQuery.directives.link.on));

  const handleLinkClick = useCallback((column: string, value: string) => {
    if (!currentQuery?.directives.link?.into) return;
    const allQs = getAllQueriesFromPresets();
    const target = allQs.find(q => q.name === currentQuery.directives.link!.into);
    if (!target) {
      setError(`Link target query not found: "${currentQuery.directives.link!.into}"`);
      return;
    }
    setLinkModal({ targetQuery: target, params: { [column]: value } });
  }, [currentQuery]);

  /* ── @part_link state ── */
  const [partLinkTarget, setPartLinkTarget] = useState<{ partName: string; database: string; table: string } | null>(null);
  const [partLinkDetail, setPartLinkDetail] = useState<PartDetailInfo | null>(null);
  const [partLinkLoading, setPartLinkLoading] = useState(false);
  const isPartLinkable = !!(currentQuery?.directives.partLink?.on) &&
    (!result || result.columns.includes(currentQuery.directives.partLink.on));

  const resolvePartLink = useCallback((partName: string, row?: Record<string, unknown>) => {
    const pl = currentQuery?.directives.partLink;
    if (!pl || !partName) return;
    const currentDrillParams = drillStack.length > 0 ? drillStack[drillStack.length - 1].params : {};
    const db = String(row?.[pl.database] ?? currentDrillParams[pl.database] ?? '');
    const tbl = String(row?.[pl.table] ?? currentDrillParams[pl.table] ?? '');
    if (db && tbl) {
      setPartLinkTarget({ partName, database: db, table: tbl });
    }
  }, [currentQuery, drillStack]);

  const handlePartLinkClick = useCallback((_column: string, value: string, row: Record<string, unknown>) => {
    resolvePartLink(value, row);
  }, [resolvePartLink]);

  const handlePartLinkChartClick = useCallback((event: DrillDownEvent) => {
    const pl = currentQuery?.directives.partLink;
    if (!pl || !result) return;
    const row = result.rows.find(r => String(r[pl.on]) === event.label);
    resolvePartLink(event.label, row ?? undefined);
  }, [currentQuery, result, resolvePartLink]);

  useEffect(() => {
    if (!partLinkTarget || !services) {
      setPartLinkDetail(null);
      return;
    }
    let cancelled = false;
    setPartLinkLoading(true);
    databaseApi.fetchPartDetail(services.databaseExplorer, partLinkTarget.database, partLinkTarget.table, partLinkTarget.partName)
      .then(detail => { if (!cancelled) setPartLinkDetail(detail); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setPartLinkLoading(false); });
    return () => { cancelled = true; };
  }, [partLinkTarget, services]);

  // Modal state for new/clone query
  const [queryModal, setQueryModal] = useState<{ mode: 'new' | 'clone'; defaultName: string; defaultDesc: string; defaultGroup: string; bodySql: string } | null>(null);

  /** Open modal to clone a builtin query */
  const handleCloneQuery = useCallback(() => {
    const source = allQueries.find(q => q.name === activeQueryName);
    // Strip only @meta (rebuilt with new name/group) and Source comment; preserve @chart/@rag/@drill/@link
    const body = sql.replace(/^--\s*@meta:.*\n?/gim, '').replace(/^--\s*Source:.*\n?/gim, '').trimStart();
    setQueryModal({
      mode: 'clone',
      defaultName: source ? `${source.name} (copy)` : '',
      defaultDesc: source?.description ?? '',
      defaultGroup: 'Custom',
      bodySql: body,
    });
  }, [activeQueryName, allQueries, sql]);

  /** Open modal to create a new custom query */
  const handleNewCustomQuery = useCallback(() => {
    setQueryModal({ mode: 'new', defaultName: '', defaultDesc: '', defaultGroup: 'Custom', bodySql: NEW_QUERY_TEMPLATE_BODY });
  }, []);

  /** Commit the modal — save and load the new query */
  const handleModalSave = useCallback((name: string, description: string, group: string) => {
    if (!queryModal) return;
    const fullSql = buildCustomQuerySql(name, description, queryModal.bodySql, group);
    const updated = addCustomQuery(fullSql);
    setCustomQueries(updated);
    setActiveQueryName(name);
    setQueryModal(null);
    // Load the newly created query into the editor
    setSql(fullSql);
    setResult(null);
    setError(null);
    setViewMode('table');
  }, [queryModal]);

  /** Save edits to an existing custom query */
  const handleSaveCustomQuery = useCallback(() => {
    if (!activeQueryName || !activeIsCustom) return;
    const updated = addCustomQuery(sql);
    setCustomQueries(updated);
  }, [sql, activeQueryName, activeIsCustom]);

  const handleDeleteCustomQuery = useCallback((name: string) => {
    if (!confirm(`Delete custom query "${name}"?`)) return;
    const updated = deleteCustomQuery(name);
    setCustomQueries(updated);
    if (activeQueryName === name) setActiveQueryName(null);
  }, [activeQueryName]);

  // Auto-execute first query on mount (unless noAutoExecute is set, e.g. when opened from query detail modal)
  useEffect(() => {
    if (!hasAutoExecuted.current && services) {
      hasAutoExecuted.current = true;
      if (urlState?.noAutoExecute) {
        // Clear the flag from the URL so it doesn't persist on subsequent navigations
        onUrlStateChange?.({ noAutoExecute: undefined });
      } else {
        runQuery();
      }
    }
  }, [runQuery, services, urlState?.noAutoExecute]);

  // Re-run query when time range changes (after initial mount)
  const prevTimeRange = useRef(timeRangeOverride);
  useEffect(() => {
    if (!hasAutoExecuted.current) return;
    if (prevTimeRange.current !== timeRangeOverride) {
      prevTimeRange.current = timeRangeOverride;
      runQuery();
    }
  }, [timeRangeOverride, runQuery]);

  // Sync chart control changes to URL (debounced by React batching)
  const prevChartRef = useRef(chartConfig);
  useEffect(() => {
    if (!hasAutoExecuted.current) return; // Don't sync during init
    if (viewMode === 'chart' && prevChartRef.current !== chartConfig) {
      prevChartRef.current = chartConfig;
      syncUrl(sql, viewMode, chartConfig, isFullscreen);
    }
  }, [chartConfig, viewMode, sql, syncUrl, isFullscreen]);

  // Keyboard shortcuts: Escape exits fullscreen, 'f' toggles fullscreen in chart view
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      // Don't trigger when typing in inputs/textareas
      const el = e.target as HTMLElement;
      const tag = el?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
      if (el?.closest?.('.cm-editor')) return;

      if (e.key === 'Escape' && isFullscreen) {
        setIsFullscreen(false);
        syncUrl(sql, viewMode, chartConfig, false);
      }
      if (e.key === 'f' && viewMode === 'chart' && result) {
        setIsFullscreen(prev => {
          const next = !prev;
          syncUrl(sql, viewMode, chartConfig, next);
          return next;
        });
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [isFullscreen, sql, viewMode, chartConfig, syncUrl, result]);

  /* ── resizer ── */
  const handleResizerDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDragging.current = true;
    const startY = e.clientY, startH = editorHeight;
    const onMove = (ev: MouseEvent) => { if (isDragging.current) setEditorHeight(Math.max(80, Math.min(500, startH + ev.clientY - startY))); };
    const onUp = () => { isDragging.current = false; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, [editorHeight]);

  /* ── Cell styles: parse directly from live SQL so editor changes take effect ── */
  const activeCellStyles = useMemo(() => {
    const styles = [...parseCellStyles(sql), ...parseRagRules(sql)];
    return styles.length > 0 ? styles : undefined;
  }, [sql]);

  /* ── chart data ── */
  const chartData = useMemo((): ChartDataPoint[] => {
    if (!result || !chartConfig.groupByColumn || !chartConfig.valueColumn) return [];
    const isTimeSeries = chartConfig.type && ['line', 'area', 'grouped_line'].includes(chartConfig.type);
    return buildChartData(result.rows, result.columns, chartConfig.groupByColumn, chartConfig.valueColumn, isTimeSeries ? undefined : 50, chartConfig.descriptionColumn);
  }, [result, chartConfig.groupByColumn, chartConfig.valueColumn, chartConfig.type, chartConfig.descriptionColumn]);

  /* ── grouped chart data (for grouped_bar, stacked_bar, grouped_line) ── */
  const groupedChartData = useMemo((): GroupedChartData[] => {
    if (!result || !chartConfig.groupByColumn || !chartConfig.valueColumn) return [];
    const isTimeSeries = chartConfig.type && ['line', 'area', 'grouped_line'].includes(chartConfig.type);
    return buildGroupedChartData(result.rows, chartConfig.groupByColumn, chartConfig.valueColumn, chartConfig.seriesColumn, chartConfig.valueColumns, isTimeSeries ? undefined : 30);
  }, [result, chartConfig.groupByColumn, chartConfig.valueColumn, chartConfig.valueColumns, chartConfig.seriesColumn]);

  const isGroupedChart = isGroupedChartType(chartConfig.type);

  const canShowChart = useMemo(() => {
    if (!result || result.rows.length === 0) return false;
    return result.columns.some(c => result.rows.some(r => isNumericValue(r[c])));
  }, [result]);

  /* ── sorted rows ── */
  const sortedRows = useMemo(() => {
    if (!result || !sortConfig) return result?.rows ?? [];
    return sortRows(result.rows, sortConfig.column, sortConfig.direction);
  }, [result, sortConfig]);

  const handleSort = useCallback((col: string) => {
    setSortConfig(prev => {
      if (prev?.column === col) return prev.direction === 'asc' ? { column: col, direction: 'desc' } : null;
      return { column: col, direction: 'asc' };
    });
  }, []);

  const copyResults = useCallback(async () => {
    if (!result) return;
    const tsv = [result.columns.join('\t'), ...result.rows.map(r => result.columns.map(c => formatCell(r[c], c)).join('\t'))].join('\n');
    await navigator.clipboard.writeText(tsv);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [result]);

  const isGrafana = typeof window !== 'undefined' && 'grafanaBootData' in window;

  const exportToGrafana = useCallback(async () => {
    const ragRules = parseRagRules(sql);
    const title = activeQueryName ?? 'Tracehouse Query';
    const input: GrafanaExportInput = {
      sql: resolvedSql,
      title,
      chart: viewMode === 'chart' ? {
        type: chartConfig.type,
        groupByColumn: chartConfig.groupByColumn,
        valueColumn: chartConfig.valueColumn,
        valueColumns: chartConfig.valueColumns,
        seriesColumn: chartConfig.seriesColumn,
        orientation: chartConfig.orientation,
        unit: chartConfig.unit,
      } : undefined,
      rag: ragRules.length > 0 ? ragRules : undefined,
    };
    const panel = toGrafanaPanel(input);

    if (onExportToGrafana) {
      onExportToGrafana({ panel, title });
    } else {
      // Inside Grafana: create a new dashboard via the API
      try {
        const resp = await fetch('/api/dashboards/db', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            dashboard: {
              title: `Tracehouse: ${title}`,
              tags: ['tracehouse', 'clickhouse'],
              timezone: 'browser',
              panels: [panel],
              schemaVersion: 39,
            },
            overwrite: false,
          }),
        });
        if (!resp.ok) {
          const body = await resp.json().catch(() => ({}));
          throw new Error(body.message || `HTTP ${resp.status}`);
        }
        const { url } = await resp.json();
        // Open the new dashboard in a new tab
        window.open(url, '_blank');
      } catch (e) {
        console.error('[Grafana export]', e);
        // Fallback: copy panel JSON to clipboard
        await navigator.clipboard.writeText(JSON.stringify(panel, null, 2));
      }
    }
    setGrafanaCopied(true);
    setTimeout(() => setGrafanaCopied(false), 2500);
  }, [sql, resolvedSql, activeQueryName, viewMode, chartConfig, onExportToGrafana]);

  const toggleFullscreen = useCallback(() => {
    setIsFullscreen(prev => {
      const next = !prev;
      syncUrl(sql, viewMode, chartConfig, next);
      return next;
    });
  }, [sql, viewMode, chartConfig, syncUrl]);

  /* ═══════════════════════════════════════════════════════════════════════
     Render
     ═══════════════════════════════════════════════════════════════════════ */
  return (
    <div style={{ display: 'flex', height: '100%', overflow: 'hidden' }}>
      {/* ── Sidebar ── */}
      <div style={{
        width: sidebarCollapsed ? 40 : 260, borderRight: '1px solid var(--border-primary)',
        display: 'flex', flexDirection: 'column', transition: 'width 0.2s ease', flexShrink: 0, overflow: 'hidden',
      }}>
        {sidebarCollapsed ? (
          <button onClick={() => setSidebarCollapsed(false)} title="Expand sidebar"
            style={{ margin: '12px auto', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-tertiary)', fontSize: 16 }}>›</button>
        ) : (
          <>
            <div style={{ padding: '12px 12px 8px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Preset Queries</span>
              <div style={{ display: 'flex', gap: 4 }}>
                {allQueries.length > MAX_SIDEBAR_QUERIES && (
                  <button onClick={() => { setResult(null); setViewMode('queries'); }}
                    style={{ padding: '2px 8px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', background: 'rgba(99,102,241,0.1)', borderRadius: 4, border: 'none', cursor: 'pointer' }}
                    title="Show all queries">
                    All ({allQueries.length})
                  </button>
                )}
                <button onClick={() => setSidebarCollapsed(true)} title="Collapse sidebar"
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-tertiary)', fontSize: 14, padding: '0 4px' }}>‹</button>
              </div>
            </div>
            <div style={{ flex: 1, overflowY: 'auto', padding: '0 8px 8px' }}>
              {allQueries.slice(0, MAX_SIDEBAR_QUERIES).map((p, i) => {
                const isCustom = customQueries.some(q => q.name === p.name);
                return (
                <button key={i} onClick={() => selectPreset(p)}
                  style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', gap: 2, width: '100%', textAlign: 'left', padding: '8px 10px', borderRadius: 6, border: 'none', background: sql.trim() === p.sql.trim() ? 'rgba(88,166,255,0.1)' : 'transparent', cursor: 'pointer', marginBottom: 2, position: 'relative' }}>
                  <span style={{ fontSize: 12, fontWeight: 500, color: sql.trim() === p.sql.trim() ? 'var(--text-primary)' : 'var(--text-secondary)', display: 'flex', alignItems: 'center', gap: 6 }}>
                    {p.name}
                    {isCustom && <span style={{ fontSize: 8, fontWeight: 600, color: '#79c0ff', background: 'rgba(121,192,255,0.12)', padding: '1px 5px', borderRadius: 3, letterSpacing: '0.3px', flexShrink: 0 }}>CUSTOM</span>}
                  </span>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.3 }}>{p.description}</span>
                  {isCustom && (
                    <span onClick={(e) => { e.stopPropagation(); handleDeleteCustomQuery(p.name); }}
                      style={{ position: 'absolute', top: 6, right: 6, fontSize: 13, color: 'var(--text-muted)', cursor: 'pointer', padding: '0 3px', borderRadius: 3, lineHeight: 1 }}
                      title="Delete custom query">×</span>
                  )}
                </button>
                );
              })}
            </div>
          </>
        )}
      </div>

      {/* ── Main ── */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, overflow: 'hidden' }}>
        {/* Editor */}
        <div style={{ height: editorHeight, flexShrink: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 16px', background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-tertiary)' }}>SQL Query</span>
              <button
                onClick={() => setShowResolved(v => !v)}
                title={showResolved ? 'Show template query (with variables)' : 'Show resolved query (variables replaced)'}
                style={{
                  padding: '2px 8px', fontSize: 10, cursor: 'pointer',
                  background: showResolved ? 'rgba(209,154,102,0.15)' : 'transparent',
                  border: `1px solid ${showResolved ? 'rgba(209,154,102,0.4)' : 'var(--border-primary)'}`,
                  borderRadius: 4,
                  color: showResolved ? '#d19a66' : 'var(--text-muted)',
                  fontFamily: "'Share Tech Mono',monospace",
                  fontWeight: 500,
                  transition: 'all 0.15s ease',
                }}
              >
                {showResolved ? '{{ }}' : '{ → }'}
              </button>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <TimeRangePicker value={timeRangeOverride} onChange={setTimeRangeOverride} />
              <span style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: "'Share Tech Mono',monospace", padding: '2px 6px', background: 'rgba(255,255,255,0.05)', borderRadius: 4 }}>⌘ Enter</span>
              {activeIsBuiltin && (
                <button onClick={handleCloneQuery}
                  style={{ padding: '4px 12px', fontSize: 12, cursor: 'pointer', background: 'var(--bg-card)', border: '1px solid var(--border-primary)', borderRadius: 4, color: 'var(--text-secondary)', fontWeight: 500 }}
                  title="Clone this builtin query as a custom query">
                  Clone
                </button>
              )}
              {activeIsCustom && (
                <button onClick={handleSaveCustomQuery}
                  style={{ padding: '4px 12px', fontSize: 12, cursor: 'pointer', background: 'var(--bg-card)', border: '1px solid var(--border-primary)', borderRadius: 4, color: 'var(--text-secondary)', fontWeight: 500 }}
                  title="Save changes to this custom query">
                  Save
                </button>
              )}
              <button onClick={handleNewCustomQuery}
                style={{ padding: '4px 12px', fontSize: 12, cursor: 'pointer', background: 'var(--bg-card)', border: '1px solid var(--border-primary)', borderRadius: 4, color: 'var(--text-secondary)', fontWeight: 500 }}
                title="Create a new custom query from a template">
                + New
              </button>
              <button className="btn btn-primary" onClick={() => runQuery()} disabled={isRunning || !services}
                style={{ padding: '4px 12px', fontSize: 12, cursor: isRunning ? 'not-allowed' : 'pointer', opacity: isRunning ? 0.6 : 1 }}>
                {isRunning ? 'Running…' : '▶ Run Query'}
              </button>
            </div>
          </div>
          <div style={{ position: 'relative', flex: 1, background: 'var(--bg-code, #0d1117)', overflow: 'hidden' }}>
              {showResolved ? (
                /* ── Resolved query view (read-only, copyable) ── */
                <pre
                  style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, margin: 0, padding: 16, fontFamily: "'Share Tech Mono','Fira Code',monospace", fontSize: 12, lineHeight: 1.6, letterSpacing: 'normal', tabSize: 4, color: 'var(--text-secondary)', background: 'transparent', border: 'none', whiteSpace: 'pre-wrap', wordWrap: 'break-word', wordBreak: 'break-all', overflowY: 'auto', overflowX: 'hidden', boxSizing: 'border-box', cursor: 'text', userSelect: 'text' }}>
                  <code style={{ fontFamily: 'inherit', fontSize: 'inherit', lineHeight: 'inherit', letterSpacing: 'inherit', tabSize: 'inherit' }} dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(highlightSQL(resolvedSql) + '\n') }} />
                </pre>
              ) : (
                <SqlEditor value={sql} onChange={setSql} onRun={runQuery} functions={chFunctions} columns={result?.columns} queries={allQueryEntries} />
              )}
          </div>
        </div>

        {/* Resizer */}
        <div onMouseDown={handleResizerDown}
          style={{ height: 6, cursor: 'ns-resize', position: 'relative', flexShrink: 0, background: 'transparent' }}>
          <div style={{ position: 'absolute', left: '50%', top: '50%', transform: 'translate(-50%,-50%)', width: 40, height: 4, background: 'var(--border-primary)', borderRadius: 2, opacity: 0.5 }} />
        </div>

        {/* Results */}
        <div style={{ flex: 1, position: 'relative', minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          {error && (() => {
            const fmt = formatClickHouseError(error);
            return (
              <div style={{
                padding: '10px 16px', fontSize: 12, flexShrink: 0,
                borderBottom: '1px solid',
                ...(fmt.isPermissionError
                  ? { background: 'rgba(210,153,34,0.08)', color: '#d29922', borderBottomColor: 'rgba(210,153,34,0.2)' }
                  : { background: 'rgba(248,81,73,0.08)', color: '#f85149', borderBottomColor: 'rgba(248,81,73,0.2)' }),
              }}
              title={error}>
                {fmt.message}
              </div>
            );
          })()}

          {/* Queries Grid View */}
          {viewMode === 'queries' && <QueriesGrid search={querySearch} onSearchChange={setQuerySearch} onSelect={p => { selectPreset(p); setQuerySearch(''); }} customQueries={customQueries} onDeleteCustom={handleDeleteCustomQuery} />}

          {/* Results header */}
          {result && viewMode !== 'queries' && (
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 16px', background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)', flexShrink: 0 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{result.rowCount} rows</span>
                <span style={{ fontSize: 11, color: 'var(--accent-green)', fontFamily: "'Share Tech Mono',monospace" }}>{result.executionTime.toFixed(1)}ms</span>
                <button onClick={copyResults} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', fontSize: 14, padding: '2px 6px', borderRadius: 4 }} title="Copy as TSV">
                  {copied ? '✓' : '⧉'}
                </button>
                {(onExportToGrafana || isGrafana) && (
                  <button onClick={exportToGrafana} style={{ background: 'none', border: 'none', cursor: 'pointer', color: grafanaCopied ? 'var(--accent-green)' : 'var(--text-muted)', fontSize: 10, fontWeight: 500, padding: '2px 8px', borderRadius: 4, transition: 'color 0.15s ease' }} title="Create as Grafana dashboard">
                    {grafanaCopied ? '✓ Created in Grafana' : 'Grafana ⤴'}
                  </button>
                )}
              </div>
              <div className="tabs" style={{ padding: 2 }}>
                {(['table', 'chart'] as const).map(m => (
                  <button key={m} className={`tab${viewMode === m ? ' active' : ''}`}
                    onClick={() => setViewMode(m)}
                    disabled={m === 'chart' && !canShowChart}
                    style={{ border: 'none', padding: '4px 12px', fontSize: 11, opacity: m === 'chart' && !canShowChart ? 0.35 : undefined }}>
                    {m === 'table' ? 'Table' : 'Chart'}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Drill-down breadcrumbs — shared across table and chart views */}
          {result && drillStack.length > 0 && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 4, padding: '5px 16px',
              background: 'rgba(99,102,241,0.06)', borderBottom: '1px solid var(--border-primary)',
              fontSize: 11, flexShrink: 0, flexWrap: 'wrap' }}>
              <span style={{ color: 'var(--text-tertiary)', fontSize: 10, marginRight: 4 }}>DRILL</span>
              <button onClick={() => handleBreadcrumbClick(-1)}
                style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '2px 4px',
                  color: 'var(--accent-primary, #6366f1)', fontSize: 11, fontWeight: 500 }}>
                {rootQueryName}
              </button>
              {drillStack.map((entry, i) => (
                <React.Fragment key={i}>
                  <span style={{ color: 'var(--text-tertiary)' }}>/</span>
                  <span style={{ color: 'var(--text-muted)', fontSize: 10 }}>
                    {Object.values(entry.params).slice(-1)[0]}
                  </span>
                  <span style={{ color: 'var(--text-tertiary)' }}>/</span>
                  {i < drillStack.length - 1 ? (
                    <button onClick={() => handleBreadcrumbClick(i)}
                      style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '2px 4px',
                        color: 'var(--accent-primary, #6366f1)', fontSize: 11 }}>
                      {entry.queryName}
                    </button>
                  ) : (
                    <span style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 11 }}>{entry.queryName}</span>
                  )}
                </React.Fragment>
              ))}
            </div>
          )}

          {/* Table view */}
          {result && viewMode === 'table' && (
            <div style={{ flex: 1, overflow: 'auto', position: 'relative' }}>
              <ResultsTable
                columns={result.columns}
                rows={sortedRows}
                sortColumn={sortConfig?.column ?? null}
                sortDirection={sortConfig?.direction ?? 'asc'}
                onSort={handleSort}
                linkOnColumn={isLinkable ? currentQuery?.directives.link?.on : undefined}
                cellStyles={activeCellStyles}
                onLinkClick={isLinkable ? handleLinkClick : undefined}
                drillOnColumn={isDrillable ? currentQuery?.directives.drill?.on : undefined}
                onDrillClick={isDrillable ? ((_col, value) => handleDrillDown({ label: value, value: 0 })) : undefined}
                drillIntoQuery={isDrillable ? currentQuery?.directives.drill?.into : undefined}
                partLinkOnColumn={isPartLinkable ? currentQuery?.directives.partLink?.on : undefined}
                onPartLinkClick={isPartLinkable ? handlePartLinkClick : undefined}
              />
            </div>
          )}

          {/* Chart view */}
          {result && viewMode === 'chart' && (
            <div style={{
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
              ...(isFullscreen ? {
                position: 'fixed',
                top: 0,
                left: 0,
                right: 0,
                bottom: 0,
                zIndex: 9999,
                background: 'var(--bg-primary, #030712)',
              } : {}),
            }}>
              {/* Chart controls — hidden in fullscreen */}
              {!isFullscreen && (
              <div style={{ display: 'flex', gap: 16, padding: '6px 16px', background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)', flexShrink: 0, flexWrap: 'wrap', alignItems: 'center' }}>
                <ChartControl label="Type" value={chartConfig.type} options={[['bar','Bar'],['line','Line'],['pie','Pie'],['area','Area'],['grouped_bar','Grouped Bar'],['stacked_bar','Stacked Bar'],['grouped_line','Grouped Line']]} onChange={v => setChartConfig(p => ({ ...p, type: v as ChartType }))} />
                <ChartControl label="Group By" value={chartConfig.groupByColumn} options={result.columns.map(c => [c, c])} onChange={v => setChartConfig(p => ({ ...p, groupByColumn: v }))} />
                <ChartControl label="Value" value={chartConfig.valueColumn} options={result.columns.map(c => [c, c])} onChange={v => setChartConfig(p => ({ ...p, valueColumn: v }))} />
                {/* Series column selector for grouped/stacked charts */}
                {isGroupedChart && (
                  <ChartControl label="Series" value={chartConfig.seriesColumn ?? ''} options={[['','(none)'],...result.columns.map(c => [c, c] as [string,string])]} onChange={v => setChartConfig(p => ({ ...p, seriesColumn: v || undefined }))} />
                )}
                {/* 2D / 3D toggle */}
                <div className="tabs" style={{ padding: 2, marginLeft: 'auto' }}>
                  {(['2d', '3d'] as ChartStyle[]).map(s => (
                    <button key={s} className={`tab${chartConfig.visualization === s ? ' active' : ''}`} onClick={() => setChartConfig(p => ({ ...p, visualization: s }))}
                      style={{ border: 'none', padding: '4px 12px', fontSize: 11 }}>
                      {s.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>
              )}

              {/* Title/description for 2D */}
              {chartConfig.visualization === '2d' && (chartConfig.title || chartConfig.description) && (
                <div style={{ padding: '12px 24px 0' }}>
                  {chartConfig.title && <div style={{ color: 'var(--text-secondary)', fontSize: 15, fontWeight: 600, marginBottom: 4 }}>{chartConfig.title}</div>}
                  {chartConfig.description && <div style={{ color: 'var(--text-muted)', fontSize: 11, lineHeight: 1.4 }}>{chartConfig.description}</div>}
                </div>
              )}

              {/* Chart area */}
              <div style={{ flex: 1, overflow: 'auto', padding: chartConfig.visualization === '3d' ? 0 : 24, minHeight: 0 }}>
                {chartConfig.visualization === '2d' ? (
                  <div style={{ position: 'relative', width: '100%', height: '100%' }}>
                    <ChartRenderer chartType={chartConfig.type} data={chartData} groupedData={groupedChartData}
                      orientation={chartConfig.orientation} unit={chartConfig.unit} color={chartConfig.color}
                      onDrillDown={isDrillable ? handleDrillDown : isPartLinkable ? handlePartLinkChartClick : undefined}
                      drillIntoQuery={isDrillable ? currentQuery?.directives.drill?.into : isPartLinkable ? 'Part Inspector' : undefined}
                      valueColumns={chartConfig.valueColumns} />
                    <button
                      onClick={toggleFullscreen}
                      title={isFullscreen ? 'Exit fullscreen (Esc)' : 'Fullscreen (f)'}
                      style={{
                        position: 'absolute', top: 8, right: 8,
                        background: 'rgba(0,0,0,0.5)', border: '1px solid var(--border-primary)',
                        borderRadius: 4, color: 'var(--text-secondary)', cursor: 'pointer',
                        padding: '4px 8px', fontSize: 14, lineHeight: 1, zIndex: 10,
                      }}
                    >{isFullscreen ? '⊗' : '⊞'}</button>
                  </div>
                ) : (
                  <Chart3DCanvas
                    data={chartData}
                    groupedData={isGroupedChart ? groupedChartData : undefined}
                    type={chartConfig.type}
                    orientation={chartConfig.orientation}
                    title={chartConfig.title}
                    description={chartConfig.description}
                    isFullscreen={isFullscreen}
                    onToggleFullscreen={toggleFullscreen}
                    onDrillDown={isDrillable ? handleDrillDown : isPartLinkable ? handlePartLinkChartClick : undefined}
                    unit={chartConfig.unit}
                  />
                )}
              </div>
            </div>
          )}

          {/* Placeholder */}
          {!result && !error && !isRunning && viewMode !== 'queries' && (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 8, color: 'var(--text-muted)' }}>
              <span style={{ fontSize: 13 }}>Select a preset query or write your own SQL</span>
              <span style={{ fontSize: 11 }}>Press <code style={{ background: 'rgba(255,255,255,0.05)', padding: '2px 6px', borderRadius: 4 }}>⌘ Enter</code> to run</span>
            </div>
          )}

          {isRunning && (
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)', fontSize: 13 }}>
              Running query…
            </div>
          )}
        </div>
      </div>

      {/* New/Clone query modal */}
      {queryModal && (
        <QueryNameModal
          mode={queryModal.mode}
          defaultName={queryModal.defaultName}
          defaultDesc={queryModal.defaultDesc}
          defaultGroup={queryModal.defaultGroup}
          onSave={handleModalSave}
          onCancel={() => setQueryModal(null)}
        />
      )}
      {linkModal && (
        <LinkQueryModal
          targetQuery={linkModal.targetQuery}
          params={linkModal.params}
          parentDrillParams={drillStack.length > 0 ? drillStack[drillStack.length - 1].params : undefined}
          onClose={() => setLinkModal(null)}
        />
      )}
      {partLinkTarget && (
        <PartInspector
          partDetail={partLinkDetail}
          isLoading={partLinkLoading}
          onClose={() => { setPartLinkTarget(null); setPartLinkDetail(null); }}
          breadcrumbPath={[partLinkTarget.database, partLinkTarget.table]}
          database={partLinkTarget.database}
          table={partLinkTarget.table}
          zIndex={100000}
        />
      )}
    </div>
  );
};

/* ═══════════════════════════════════════════════════════════════════════════
   Sub-components
   ═══════════════════════════════════════════════════════════════════════════ */

const ChartControl: React.FC<{ label: string; value: string; options: [string, string][]; onChange: (v: string) => void }> = ({ label, value, options, onChange }) => (
  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
    <label style={{ fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.03em' }}>{label}</label>
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ fontFamily: "'Share Tech Mono',monospace", fontSize: 11, color: 'var(--text-secondary)', background: 'var(--bg-code, #0d1117)', border: '1px solid var(--border-primary)', borderRadius: 4, padding: '3px 6px', minWidth: 100 }}>
      {options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
    </select>
  </div>
);

/** Modal for naming a new or cloned query */
const QueryNameModal: React.FC<{
  mode: 'new' | 'clone';
  defaultName: string;
  defaultDesc: string;
  defaultGroup: string;
  onSave: (name: string, description: string, group: string) => void;
  onCancel: () => void;
}> = ({ mode, defaultName, defaultDesc, defaultGroup, onSave, onCancel }) => {
  const [name, setName] = useState(defaultName);
  const [desc, setDesc] = useState(defaultDesc);
  const [group, setGroup] = useState(defaultGroup);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => { inputRef.current?.focus(); inputRef.current?.select(); }, []);

  const trimmedName = name.trim();
  const trimmedGroup = group.trim();
  const isDuplicate = trimmedName.length > 0 && isQueryNameTaken(trimmedName);
  const isBuiltinGroup = trimmedGroup.length > 0 && QUERY_GROUPS[trimmedGroup as QueryGroup]?.builtin === true;
  const canSave = trimmedName.length > 0 && trimmedGroup.length > 0 && !isDuplicate && !isBuiltinGroup;

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && canSave) onSave(trimmedName, desc.trim(), trimmedGroup);
    if (e.key === 'Escape') onCancel();
  };

  const modalInputStyle: React.CSSProperties = { width: '100%', padding: '8px 12px', fontSize: 13, color: 'var(--text-primary)', background: 'var(--bg-input, var(--bg-tertiary))', border: '1px solid var(--border-input, var(--border-primary))', borderRadius: 6, outline: 'none', boxSizing: 'border-box', fontFamily: 'inherit', transition: 'border-color 0.15s ease' };
  const modalLabelStyle: React.CSSProperties = { display: 'block', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.3px' };

  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 10000, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'rgba(0,0,0,0.5)', backdropFilter: 'blur(4px)' }}
      onClick={onCancel}>
      <div style={{ background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)', borderRadius: 10, padding: 24, minWidth: 360, maxWidth: 440, boxShadow: '0 12px 40px rgba(0,0,0,0.3)' }}
        onClick={e => e.stopPropagation()}>
        <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 16 }}>
          {mode === 'clone' ? 'Clone Query' : 'New Query'}
        </div>
        <label style={modalLabelStyle}>Title</label>
        <input ref={inputRef} value={name} onChange={e => setName(e.target.value)} onKeyDown={handleKeyDown}
          placeholder="My query"
          style={{ ...modalInputStyle, marginBottom: isDuplicate ? 4 : 16, borderColor: isDuplicate ? '#f85149' : undefined }} />
        {isDuplicate && (
          <div style={{ fontSize: 11, color: '#f85149', marginBottom: 16 }}>
            A query with this name already exists
          </div>
        )}
        <label style={modalLabelStyle}>Group</label>
        <input value={group} onChange={e => setGroup(e.target.value)} onKeyDown={handleKeyDown}
          placeholder="Custom"
          style={{ ...modalInputStyle, marginBottom: isBuiltinGroup ? 4 : 16, borderColor: isBuiltinGroup ? '#f85149' : undefined }} />
        {isBuiltinGroup && (
          <div style={{ fontSize: 11, color: '#f85149', marginBottom: 16 }}>
            Cannot add queries to built-in group "{trimmedGroup}"
          </div>
        )}
        <label style={modalLabelStyle}>Description</label>
        <input value={desc} onChange={e => setDesc(e.target.value)} onKeyDown={handleKeyDown}
          placeholder="Optional description"
          style={{ ...modalInputStyle, marginBottom: 20 }} />
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, borderTop: '1px solid var(--border-secondary)', paddingTop: 16 }}>
          <button onClick={onCancel}
            style={{ padding: '7px 18px', fontSize: 12, borderRadius: 7, border: '1px solid var(--border-primary)', background: 'var(--bg-secondary)', color: 'var(--text-secondary)', cursor: 'pointer', transition: 'all 0.15s ease' }}>
            Cancel
          </button>
          <button onClick={() => canSave && onSave(trimmedName, desc.trim(), trimmedGroup)} disabled={!canSave}
            style={{ padding: '7px 18px', fontSize: 12, borderRadius: 7, border: '1px solid var(--border-primary)', background: 'var(--bg-primary)', color: 'var(--text-primary)', fontWeight: 600, cursor: canSave ? 'pointer' : 'not-allowed', opacity: canSave ? 1 : 0.4, transition: 'all 0.15s ease', boxShadow: '0 1px 3px rgba(0,0,0,0.08)' }}>
            {mode === 'clone' ? 'Clone' : 'Create'}
          </button>
        </div>
      </div>
    </div>
  );
};

/** All-queries grid view with search and categories */
const QueriesGrid: React.FC<{ search: string; onSearchChange: (s: string) => void; onSelect: (p: Query) => void; customQueries: Query[]; onDeleteCustom: (name: string) => void }> = ({ search, onSearchChange, onSelect, customQueries, onDeleteCustom }) => {
  const searchLower = search.toLowerCase();
  const allQueries = useMemo(() => [...PRESET_QUERIES, ...customQueries], [customQueries]);
  const customNames = useMemo(() => new Set(customQueries.map(q => q.name)), [customQueries]);
  return (
    <div style={{ flex: 1, overflow: 'auto', padding: 24 }}>
      {/* Search */}
      <div style={{ position: 'relative', marginBottom: 24 }}>
        <input type="text" value={search} onChange={e => onSearchChange(e.target.value)} autoFocus
          placeholder="Search queries…"
          style={{ width: '100%', maxWidth: 400, padding: '10px 14px', fontFamily: 'inherit', fontSize: 13, color: 'var(--text-primary)', background: 'var(--bg-input)', border: '1px solid var(--border-primary)', borderRadius: 6, outline: 'none' }} />
        {search && (
          <button onClick={() => onSearchChange('')}
            style={{ position: 'absolute', right: 'calc(100% - 400px + 10px)', top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', fontSize: 16 }}>×</button>
        )}
      </div>

      {/* Groups */}
      {(Object.keys(QUERY_GROUPS) as QueryGroup[]).map(groupKey => {
        const groupQueries = allQueries.filter(q =>
          q.group === groupKey &&
          (!search || q.name.toLowerCase().includes(searchLower) || q.description.toLowerCase().includes(searchLower) || q.sql.toLowerCase().includes(searchLower))
        );
        if (groupQueries.length === 0) return null;
        const g = QUERY_GROUPS[groupKey];
        return (
          <div key={groupKey} style={{ marginBottom: 32 }}>
            <h3 style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 13, fontWeight: 600, color: 'var(--text-secondary)', marginBottom: 16, paddingBottom: 8, borderBottom: `1px solid ${g.color}40` }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: g.color, flexShrink: 0 }} />
              {groupKey}
            </h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 12 }}>
              {groupQueries.map((p, i) => (
                <button key={i} onClick={() => onSelect(p)}
                  style={{ display: 'flex', flexDirection: 'column', gap: 6, padding: '14px 16px', background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 6, textAlign: 'left', cursor: 'pointer', position: 'relative', transition: 'all .15s' }}>
                  {p.directives.chart?.type && (
                    <span style={{ position: 'absolute', top: 8, right: customNames.has(p.name) ? 28 : 8, display: 'flex', alignItems: 'center', gap: 4, fontSize: 9, fontWeight: 500, color: 'var(--text-muted)', background: 'rgba(99,102,241,0.15)', padding: '2px 6px', borderRadius: 4 }}>
                      {CHART_TYPE_LABELS[p.directives.chart.type] ?? p.directives.chart.type}
                      {p.directives.chart.style && <span style={{ fontSize: 8, fontWeight: 600, color: 'var(--accent-primary)', background: 'rgba(99,102,241,0.25)', padding: '1px 4px', borderRadius: 2 }}>{p.directives.chart.style.toUpperCase()}</span>}
                    </span>
                  )}
                  {customNames.has(p.name) && (
                    <span
                      onClick={(e) => { e.stopPropagation(); onDeleteCustom(p.name); }}
                      style={{ position: 'absolute', top: 6, right: 6, fontSize: 14, color: 'var(--text-muted)', cursor: 'pointer', padding: '0 4px', borderRadius: 4, lineHeight: 1 }}
                      title="Delete custom query">×</span>
                  )}
                  <span style={{ fontSize: 13, fontWeight: 500, color: 'var(--text-primary)', paddingRight: p.directives.chart?.type ? 60 : 0, display: 'flex', alignItems: 'center', gap: 6 }}>
                    {p.name}
                    {customNames.has(p.name) && <span style={{ fontSize: 8, fontWeight: 600, color: '#79c0ff', background: 'rgba(121,192,255,0.12)', padding: '1px 5px', borderRadius: 3, letterSpacing: '0.3px', flexShrink: 0 }}>CUSTOM</span>}
                  </span>
                  <span style={{ fontSize: 11, color: 'var(--text-muted)', lineHeight: 1.4 }}>{p.description}</span>
                  {p.directives.source && (
                    <span style={{ fontSize: 9, color: 'var(--text-muted)', opacity: 0.7, marginTop: 2 }} title={p.directives.source}>
                      Source: {p.directives.source.includes('github.com/ClickHouse') ? 'ClickHouse OSS (Apache 2.0)' : p.directives.source.includes('clickhouse.com/blog') ? 'ClickHouse Blog' : p.directives.source.replace(/^https?:\/\//, '').split('/').slice(0, 2).join('/')}
                    </span>
                  )}
                </button>
              ))}
            </div>
          </div>
        );
      })}

      {search && allQueries.filter(q => q.name.toLowerCase().includes(searchLower) || q.description.toLowerCase().includes(searchLower) || q.sql.toLowerCase().includes(searchLower)).length === 0 && (
        <div style={{ textAlign: 'center', padding: '48px 24px', color: 'var(--text-muted)', fontSize: 14 }}>
          No queries match "{search}"
        </div>
      )}
    </div>
  );
};

export default QueryExplorer;
