import { useCallback, useEffect, useMemo, useState, type Dispatch, type SetStateAction } from 'react';
import { buildConfig } from '../../buildConfig';
import { useConnectionStore } from '../../stores/connectionStore';
import { ClusterService } from '@tracehouse/core';
import {
  analyzeGrafanaExport,
  toGrafanaDashboard,
  toGrafanaPanel,
  type GrafanaExportInput,
} from '@tracehouse/core/services/grafana-export';
import { resolveDrillParams } from './templateResolution';
import { isGroupedChartType, type ChartConfig } from './charts';
import type { CellStyleRule } from './metaLanguage';
import type { Query } from './types';
import type {
  GrafanaDashboardOption,
  GrafanaExportOptions,
  GrafanaPanelOption,
  GrafanaPanelSummary,
} from './GrafanaExportDialog';

const GRAFANA_SIMPLE_CHART_MAX_ROWS = 50;
const GRAFANA_GROUPED_CHART_MAX_ROWS = 30;

type ViewMode = 'table' | 'chart' | 'queries';
type GrafanaExportStatus = 'idle' | 'created' | 'copied';

interface QueryResultForExport {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
}

interface UseGrafanaExportArgs {
  sql: string;
  clusterName: string | null;
  drillParams: Record<string, string>;
  activeQueryName?: string;
  currentQuery?: Query;
  viewMode: ViewMode;
  chartConfig: ChartConfig;
  result: QueryResultForExport | null;
  cellStyles?: CellStyleRule[];
  onExportToGrafana?: (payload: { panel: ReturnType<typeof toGrafanaPanel>; title: string }) => void;
}

export interface GrafanaExportController {
  isGrafana: boolean;
  status: GrafanaExportStatus;
  isDialogOpen: boolean;
  options: GrafanaExportOptions | null;
  dashboards: GrafanaDashboardOption[];
  panels: GrafanaPanelOption[];
  error: string | null;
  isLoadingTargets: boolean;
  panelSummary: GrafanaPanelSummary | null;
  jsonPreview: string;
  showJsonPreview: boolean;
  setOptions: Dispatch<SetStateAction<GrafanaExportOptions | null>>;
  setShowJsonPreview: Dispatch<SetStateAction<boolean>>;
  openDialog: () => void;
  closeDialog: () => void;
  exportToGrafana: (options?: GrafanaExportOptions) => Promise<void>;
}

function grafanaDashboardUid(title: string): string {
  const slug = title
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 28);
  return `tracehouse-${slug || 'query'}`;
}

function grafanaPanelTypeLabel(panelType: string): string {
  switch (panelType) {
    case 'timeseries': return 'Time series';
    case 'barchart': return 'Bar chart';
    case 'piechart': return 'Pie chart';
    case 'table': return 'Table';
    default: return panelType;
  }
}

function isTimeSeriesChart(type: ChartConfig['type']): boolean {
  return ['line', 'area', 'grouped_line'].includes(type);
}

function defaultMaxRows(result: QueryResultForExport | null, chartConfig: ChartConfig): number {
  if (isTimeSeriesChart(chartConfig.type)) return 0;
  const limit = isGroupedChartType(chartConfig.type) ? GRAFANA_GROUPED_CHART_MAX_ROWS : GRAFANA_SIMPLE_CHART_MAX_ROWS;
  return Math.min(result?.rowCount ?? GRAFANA_SIMPLE_CHART_MAX_ROWS, limit);
}

function defaultPanelHeight(viewMode: ViewMode, chartConfig: ChartConfig, maxRows: number): number {
  if (viewMode === 'table') return 9;
  if (chartConfig.type.includes('bar') && maxRows > 0) {
    return Math.min(36, Math.max(10, Math.ceil(6 + maxRows * 0.55)));
  }
  return 10;
}

export function useGrafanaExport({
  sql,
  clusterName,
  drillParams,
  activeQueryName,
  currentQuery,
  viewMode,
  chartConfig,
  result,
  cellStyles,
  onExportToGrafana,
}: UseGrafanaExportArgs): GrafanaExportController {
  const activeProfileId = useConnectionStore(s => s.activeProfileId);
  const isGrafana = buildConfig.grafanaPlugin || (typeof window !== 'undefined' && 'grafanaBootData' in window);
  const [status, setStatus] = useState<GrafanaExportStatus>('idle');
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [options, setOptions] = useState<GrafanaExportOptions | null>(null);
  const [dashboards, setDashboards] = useState<GrafanaDashboardOption[]>([]);
  const [panels, setPanels] = useState<GrafanaPanelOption[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoadingTargets, setIsLoadingTargets] = useState(false);
  const [showJsonPreview, setShowJsonPreview] = useState(false);

  const exportSql = useMemo(() => {
    return ClusterService.resolveTableRefs(resolveDrillParams(sql, drillParams), clusterName);
  }, [sql, drillParams, clusterName]);

  const defaultOptions = useCallback((): GrafanaExportOptions => {
    const title = activeQueryName ?? 'Tracehouse Query';
    const maxRows = defaultMaxRows(result, chartConfig);
    return {
      dashboardMode: dashboards.length > 0 ? 'existing' : 'new',
      dashboardUid: dashboards[0]?.uid ?? '',
      dashboardTitle: `Tracehouse: ${title}`,
      panelMode: 'new',
      panelTitle: title,
      width: 18,
      height: defaultPanelHeight(viewMode, chartConfig, maxRows),
      maxRows,
    };
  }, [activeQueryName, chartConfig, dashboards, result, viewMode]);

  useEffect(() => {
    if (!isDialogOpen || !isGrafana) return;
    let cancelled = false;
    setIsLoadingTargets(true);
    fetch('/api/search?type=dash-db')
      .then(resp => resp.ok ? resp.json() : Promise.reject(new Error(`HTTP ${resp.status}`)))
      .then((items: Array<{ uid?: string; title?: string; folderTitle?: string }>) => {
        if (cancelled) return;
        const nextDashboards = items
          .filter(item => item.uid && item.title)
          .map(item => ({ uid: item.uid!, title: item.title!, folderTitle: item.folderTitle }));
        setDashboards(nextDashboards);
        setOptions(prev => {
          if (!prev) return prev;
          const untouchedDefaultNew = prev.dashboardMode === 'new' && prev.dashboardTitle === `Tracehouse: ${prev.panelTitle}`;
          if (((prev.dashboardMode === 'existing' && !prev.dashboardUid) || untouchedDefaultNew) && nextDashboards[0]) {
            return { ...prev, dashboardMode: 'existing', dashboardUid: nextDashboards[0].uid };
          }
          return prev;
        });
      })
      .catch(err => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (!cancelled) setIsLoadingTargets(false);
      });
    return () => { cancelled = true; };
  }, [isDialogOpen, isGrafana]);

  useEffect(() => {
    if (!isDialogOpen || !isGrafana || options?.dashboardMode !== 'existing' || !options.dashboardUid) {
      setPanels([]);
      return;
    }
    let cancelled = false;
    fetch(`/api/dashboards/uid/${encodeURIComponent(options.dashboardUid)}`)
      .then(resp => resp.ok ? resp.json() : Promise.reject(new Error(`HTTP ${resp.status}`)))
      .then((body: { dashboard?: { panels?: Array<{ id?: number; title?: string }> } }) => {
        if (cancelled) return;
        setPanels((body.dashboard?.panels ?? [])
          .filter(panel => typeof panel.id === 'number')
          .map(panel => ({ id: panel.id!, title: panel.title ?? `Panel ${panel.id}` })));
      })
      .catch(() => {
        if (!cancelled) setPanels([]);
      });
    return () => { cancelled = true; };
  }, [isDialogOpen, isGrafana, options?.dashboardMode, options?.dashboardUid]);

  const buildInput = useCallback((nextOptions?: GrafanaExportOptions): GrafanaExportInput => {
    const ragRules = (cellStyles ?? []).filter(rule => rule.type === 'rag');
    const interactions: GrafanaExportInput['interactions'] = [];
    if (currentQuery?.directives.link) {
      interactions.push({ type: 'link', on: currentQuery.directives.link.on, into: currentQuery.directives.link.into });
    }
    if (currentQuery?.directives.drill) {
      interactions.push({ type: 'drill', on: currentQuery.directives.drill.on, into: currentQuery.directives.drill.into });
    }
    if (currentQuery?.directives.partLink) {
      interactions.push({ type: 'part_link', on: currentQuery.directives.partLink.on });
    }

    const title = nextOptions?.panelTitle.trim() || activeQueryName || 'Tracehouse Query';
    const dashboardTitle = nextOptions?.dashboardTitle.trim() || `Tracehouse: ${title}`;
    const dashboardUid = nextOptions?.dashboardMode === 'existing' && nextOptions.dashboardUid
      ? nextOptions.dashboardUid
      : grafanaDashboardUid(dashboardTitle);
    const chartMaxRows = nextOptions
      ? (nextOptions.maxRows > 0 ? nextOptions.maxRows : undefined)
      : (isTimeSeriesChart(chartConfig.type) ? undefined : defaultMaxRows(result, chartConfig));
    const exportValueColumns = chartConfig.valueColumns?.length && isTimeSeriesChart(chartConfig.type)
      ? chartConfig.valueColumns
      : undefined;
    const exportSeriesValues = chartConfig.seriesColumn && ['grouped_bar', 'stacked_bar'].includes(chartConfig.type)
      ? Array.from(new Set((result?.rows ?? []).map(row => row[chartConfig.seriesColumn!]).filter(value => value != null).map(String)))
      : undefined;

    return {
      sql: exportSql,
      title,
      chart: viewMode === 'chart' ? {
        type: chartConfig.type,
        labelColumn: chartConfig.labelColumn,
        groupByColumn: chartConfig.groupByColumn,
        valueColumn: chartConfig.valueColumn,
        valueColumns: exportValueColumns,
        seriesColumn: chartConfig.seriesColumn,
        seriesValues: exportSeriesValues,
        orientation: chartConfig.orientation,
        unit: chartConfig.unit,
        color: chartConfig.color,
        profile: chartConfig.profile,
        axes: chartConfig.axes,
        ranges: chartConfig.ranges,
        transforms: chartConfig.transforms,
        valuesColumn: chartConfig.valuesColumn,
        labelsColumn: chartConfig.labelsColumn,
        colorByColumn: chartConfig.colorByColumn,
        maxRows: chartMaxRows,
      } : undefined,
      rag: ragRules.length > 0 ? ragRules : undefined,
      cellStyles: cellStyles && cellStyles.length > 0 ? cellStyles : undefined,
      resultColumns: result?.columns,
      resultRows: result?.rows,
      interactions: interactions.length > 0 ? interactions : undefined,
      datasourceUid: isGrafana ? activeProfileId ?? undefined : undefined,
      dashboardUid,
      panel: nextOptions ? { width: nextOptions.width, height: nextOptions.height } : undefined,
    };
  }, [activeProfileId, activeQueryName, cellStyles, chartConfig, currentQuery, exportSql, isGrafana, result, viewMode]);

  const exportToGrafana = useCallback(async (nextOptions?: GrafanaExportOptions) => {
    const input = buildInput(nextOptions);
    const title = input.title;
    const dashboardTitle = nextOptions?.dashboardTitle.trim() || `Tracehouse: ${title}`;
    const dashboardUid = input.dashboardUid ?? grafanaDashboardUid(dashboardTitle);
    const panel = toGrafanaPanel(input);
    const dashboard = toGrafanaDashboard(input);

    if (onExportToGrafana) {
      onExportToGrafana({ panel, title });
      setStatus('copied');
    } else {
      try {
        let dashboardPayload: Record<string, unknown>;
        let folderUid: string | undefined;
        let openUrl: string | undefined;

        if (nextOptions?.dashboardMode === 'existing' && nextOptions.dashboardUid) {
          const dashResp = await fetch(`/api/dashboards/uid/${encodeURIComponent(nextOptions.dashboardUid)}`);
          if (!dashResp.ok) {
            const body = await dashResp.json().catch(() => ({}));
            throw new Error(body.message || `HTTP ${dashResp.status}`);
          }
          const existing = await dashResp.json();
          const existingDashboard = existing.dashboard as { panels?: Array<Record<string, any>>; [key: string]: unknown };
          const existingPanels = Array.isArray(existingDashboard.panels) ? existingDashboard.panels : [];
          const replacePanel = nextOptions.panelMode === 'replace' && nextOptions.panelId != null
            ? existingPanels.find(p => p.id === nextOptions.panelId)
            : undefined;
          const nextId = replacePanel?.id ?? Math.max(0, ...existingPanels.map(p => Number(p.id) || 0)) + 1;
          const nextY = replacePanel?.gridPos?.y ?? existingPanels.reduce((max, p) => Math.max(max, Number(p.gridPos?.y ?? 0) + Number(p.gridPos?.h ?? 0)), 0);
          const nextPanel = {
            ...panel,
            id: nextId,
            gridPos: {
              ...panel.gridPos,
              x: Number(replacePanel?.gridPos?.x ?? 0),
              y: Number(nextY),
              w: nextOptions.width,
              h: nextOptions.height,
            },
          };
          existingDashboard.panels = replacePanel
            ? existingPanels.map(p => p.id === nextOptions.panelId ? nextPanel : p)
            : [...existingPanels, nextPanel];
          dashboardPayload = existingDashboard;
          folderUid = existing.meta?.folderUid;
          openUrl = existing.meta?.url;
        } else {
          dashboardPayload = {
            uid: dashboardUid,
            title: dashboardTitle,
            tags: ['tracehouse', 'clickhouse'],
            timezone: 'browser',
            panels: [panel],
            schemaVersion: 39,
          };
        }

        const resp = await fetch('/api/dashboards/db', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            dashboard: dashboardPayload,
            ...(folderUid ? { folderUid } : {}),
            overwrite: true,
          }),
        });
        if (!resp.ok) {
          const body = await resp.json().catch(() => ({}));
          throw new Error(body.message || `HTTP ${resp.status}`);
        }
        const { url } = await resp.json();
        window.open(url || openUrl || `/d/${dashboardUid}`, '_blank', 'noopener,noreferrer');
        setStatus('created');
        setIsDialogOpen(false);
      } catch (e) {
        console.error('[Grafana export]', e);
        setError(e instanceof Error ? e.message : String(e));
        await navigator.clipboard.writeText(JSON.stringify(dashboard, null, 2));
        setStatus('copied');
      }
    }
    setTimeout(() => setStatus('idle'), 2500);
  }, [buildInput, onExportToGrafana]);

  const openDialog = useCallback(() => {
    if (!isGrafana || onExportToGrafana) {
      void exportToGrafana();
      return;
    }
    setError(null);
    setOptions(defaultOptions());
    setShowJsonPreview(false);
    setIsDialogOpen(true);
  }, [defaultOptions, exportToGrafana, isGrafana, onExportToGrafana]);

  const closeDialog = useCallback(() => setIsDialogOpen(false), []);

  useEffect(() => {
    if (!isDialogOpen) return;
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation();
        setIsDialogOpen(false);
      }
    };
    window.addEventListener('keydown', handleEscape, true);
    return () => window.removeEventListener('keydown', handleEscape, true);
  }, [isDialogOpen]);

  const jsonPreview = useMemo(() => {
    if (!options) return '';
    return JSON.stringify(toGrafanaPanel(buildInput(options)), null, 2);
  }, [buildInput, options]);

  const panelSummary = useMemo<GrafanaPanelSummary | null>(() => {
    if (!options) return null;
    const input = buildInput(options);
    const panel = toGrafanaPanel(input);
    const analysis = analyzeGrafanaExport(input);
    const timeRange = input.sql.includes('{{time_range}}')
      ? 'Grafana time picker'
      : 'Fixed SQL time range';
    return {
      type: grafanaPanelTypeLabel(panel.type),
      visual: viewMode === 'chart' ? `${chartConfig.type}${chartConfig.color ? ` · ${chartConfig.color}` : ''}` : 'table',
      data: panel.type === 'timeseries' ? timeRange : `Top ${options.maxRows || 'all'} rows`,
      layout: `${panel.gridPos.w} cols x ${panel.gridPos.h} rows`,
      capabilities: analysis.capabilities,
    };
  }, [buildInput, chartConfig.color, chartConfig.type, options, viewMode]);

  return {
    isGrafana,
    status,
    isDialogOpen,
    options,
    dashboards,
    panels,
    error,
    isLoadingTargets,
    panelSummary,
    jsonPreview,
    showJsonPreview,
    setOptions,
    setShowJsonPreview,
    openDialog,
    closeDialog,
    exportToGrafana,
  };
}
