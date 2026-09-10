export type TimeTravelMetric = 'memory' | 'cpu' | 'network' | 'disk';
export type TimeTravelView = '2d' | '3d' | '3d-surface';
export type TimeTravelSortField = 'metric' | 'duration' | 'started';
export type TimeTravelSortDir = 'asc' | 'desc';
export type TimeTravelNavigatorShape = 'trend' | 'peaks' | 'change';
export type TimeTravelActivityCategory = 'query' | 'merge' | 'mutation';

export interface TimeTravelCoordinates {
  queryHashOnly: boolean;
  windowSec: number;
  isLive: boolean;
  autoRefresh: boolean;
  customStartTime: string | null;
  customEndTime: string | null;
  viewportEndTime: string | null;
  pinnedMs: number | null;
  selectedEventId: string | null;
  zoomRange: [number, number] | null;
  metricMode: TimeTravelMetric;
  viewMode: TimeTravelView;
  hiddenCategories: Set<TimeTravelActivityCategory>;
  activityLimit: number;
  selectedHosts: string[];
  perServerView: boolean;
  selectedTimeRange: string;
  sortField: TimeTravelSortField;
  sortDir: TimeTravelSortDir;
  includeRunning: boolean;
  eventsVisible: boolean;
  navigatorShape: TimeTravelNavigatorShape;
  hiddenEventSeverities: Set<string>;
  hiddenEventCategories: Set<string>;
  hiddenEventKinds: Set<string>;
}

const METRICS = new Set<TimeTravelMetric>(['memory', 'cpu', 'network', 'disk']);
const VIEWS = new Set<TimeTravelView>(['2d', '3d', '3d-surface']);
const SORT_FIELDS = new Set<TimeTravelSortField>(['metric', 'duration', 'started']);
const SORT_DIRS = new Set<TimeTravelSortDir>(['asc', 'desc']);
const NAVIGATOR_SHAPES = new Set<TimeTravelNavigatorShape>(['trend', 'peaks', 'change']);
const ACTIVITY_CATEGORIES = new Set<TimeTravelActivityCategory>(['query', 'merge', 'mutation']);
const TIME_RANGES = new Set(['1h', '3h', '6h', '12h', '1d', 'Custom']);

function enumParam<T extends string>(params: URLSearchParams, key: string, values: Set<T>, fallback: T): T {
  const value = params.get(key) as T | null;
  return value && values.has(value) ? value : fallback;
}

function finiteNumber(params: URLSearchParams, key: string, fallback: number): number {
  const raw = params.get(key);
  if (raw == null || raw === '') return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

function nullableFiniteNumber(params: URLSearchParams, key: string): number | null {
  const raw = params.get(key);
  if (raw == null || raw === '') return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function stringSet(params: URLSearchParams, key: string): Set<string> {
  return new Set(params.getAll(key).filter(Boolean));
}

export function readTimeTravelCoordinates(
  params: URLSearchParams,
  preferences: { eventsVisible: boolean; navigatorShape: TimeTravelNavigatorShape },
): TimeTravelCoordinates {
  const eventTime = params.get('event_time');
  const eventMs = eventTime ? Date.parse(eventTime) : Number.NaN;
  const hasEvent = Number.isFinite(eventMs);
  const zoomStart = nullableFiniteNumber(params, 'tt_zoom_start');
  const zoomEnd = nullableFiniteNumber(params, 'tt_zoom_end');
  const range = params.get('tt_range');

  return {
    queryHashOnly: params.get('tt_hash_only') === '1',
    windowSec: finiteNumber(params, 'tt_window', 150),
    isLive: params.has('tt_live') ? params.get('tt_live') === '1' : !hasEvent,
    autoRefresh: params.get('tt_auto') === '1',
    customStartTime: params.get('tt_start'),
    customEndTime: params.get('tt_end') ?? (hasEvent ? toLocalDatetimeString(eventMs + 150_000) : null),
    viewportEndTime: params.get('tt_viewport'),
    pinnedMs: nullableFiniteNumber(params, 'tt_pin') ?? (hasEvent ? eventMs : null),
    selectedEventId: params.get('event_id'),
    zoomRange: zoomStart != null && zoomEnd != null && zoomEnd > zoomStart ? [zoomStart, zoomEnd] : null,
    metricMode: enumParam(params, 'tt_metric', METRICS, 'cpu'),
    viewMode: enumParam(params, 'tt_view', VIEWS, '2d'),
    hiddenCategories: new Set(
      params.getAll('tt_hide').filter((value): value is TimeTravelActivityCategory => ACTIVITY_CATEGORIES.has(value as TimeTravelActivityCategory)),
    ),
    activityLimit: finiteNumber(params, 'tt_limit', 100),
    selectedHosts: params.getAll('tt_host').filter(Boolean),
    perServerView: params.get('tt_split') === '1',
    selectedTimeRange: range && TIME_RANGES.has(range) ? range : '1h',
    sortField: enumParam(params, 'tt_sort', SORT_FIELDS, 'metric'),
    sortDir: enumParam(params, 'tt_dir', SORT_DIRS, 'desc'),
    includeRunning: params.get('tt_running') !== '0',
    eventsVisible: params.has('tt_events') ? params.get('tt_events') === '1' : preferences.eventsVisible,
    navigatorShape: enumParam(params, 'tt_nav', NAVIGATOR_SHAPES, preferences.navigatorShape),
    hiddenEventSeverities: stringSet(params, 'tt_event_severity'),
    hiddenEventCategories: stringSet(params, 'tt_event_category'),
    hiddenEventKinds: stringSet(params, 'tt_event_kind'),
  };
}

function toLocalDatetimeString(ms: number): string {
  const date = new Date(ms);
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function replaceMany(params: URLSearchParams, key: string, values: Iterable<string>): void {
  params.delete(key);
  for (const value of values) params.append(key, value);
}

function setOptional(params: URLSearchParams, key: string, value: string | null, fallback?: string): void {
  if (value == null || value === '' || value === fallback) params.delete(key);
  else params.set(key, value);
}

export function writeTimeTravelCoordinates(params: URLSearchParams, coordinates: TimeTravelCoordinates): URLSearchParams {
  const next = new URLSearchParams(params);
  next.set('tt_hash_only', coordinates.queryHashOnly ? '1' : '0');
  next.set('tt_window', String(coordinates.windowSec));
  next.set('tt_live', coordinates.isLive ? '1' : '0');
  next.set('tt_auto', coordinates.autoRefresh ? '1' : '0');
  setOptional(next, 'tt_start', coordinates.customStartTime);
  setOptional(next, 'tt_end', coordinates.customEndTime);
  setOptional(next, 'tt_viewport', coordinates.viewportEndTime);
  setOptional(next, 'tt_pin', coordinates.pinnedMs == null ? null : String(coordinates.pinnedMs));
  setOptional(next, 'event_id', coordinates.selectedEventId);
  setOptional(next, 'tt_zoom_start', coordinates.zoomRange ? String(coordinates.zoomRange[0]) : null);
  setOptional(next, 'tt_zoom_end', coordinates.zoomRange ? String(coordinates.zoomRange[1]) : null);
  next.set('tt_metric', coordinates.metricMode);
  next.set('tt_view', coordinates.viewMode);
  replaceMany(next, 'tt_hide', [...coordinates.hiddenCategories].sort());
  next.set('tt_limit', String(coordinates.activityLimit));
  replaceMany(next, 'tt_host', coordinates.selectedHosts);
  next.set('tt_split', coordinates.perServerView ? '1' : '0');
  next.set('tt_range', coordinates.selectedTimeRange);
  next.set('tt_sort', coordinates.sortField);
  next.set('tt_dir', coordinates.sortDir);
  next.set('tt_running', coordinates.includeRunning ? '1' : '0');
  next.set('tt_events', coordinates.eventsVisible ? '1' : '0');
  next.set('tt_nav', coordinates.navigatorShape);
  replaceMany(next, 'tt_event_severity', [...coordinates.hiddenEventSeverities].sort());
  replaceMany(next, 'tt_event_category', [...coordinates.hiddenEventCategories].sort());
  replaceMany(next, 'tt_event_kind', [...coordinates.hiddenEventKinds].sort());
  return next;
}
