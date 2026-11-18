import type { MemoryTimeline, TimelineService, TimeseriesPoint, ResourceAttributionService } from '@tracehouse/core';
import { parseOperationTimestamp } from '@tracehouse/core';
import type { ResourceDataLoader } from './resource-exploration-model';

const MAX_FULL_WINDOW_MS = 10 * 60_000;
const SAMPLE_CHUNK_MS = 2 * 60_000;

/** Stable chunks prevent a network request on every mouse movement. */
export function resourceSampleWindow(source: MemoryTimeline, timeMs: number) {
  const start = parseOperationTimestamp(source.window_start);
  const end = parseOperationTimestamp(source.window_end);
  if (end - start <= MAX_FULL_WINDOW_MS) return { start, end };
  const chunkStart = start + Math.floor(Math.max(0, Math.min(end - 1, timeMs) - start) / SAMPLE_CHUNK_MS) * SAMPLE_CHUNK_MS;
  return { start: chunkStart, end: Math.min(end, chunkStart + SAMPLE_CHUNK_MS) };
}

type TimelineOptions = Parameters<TimelineService['getTimeline']>[0];
type XRaySelection = Parameters<TimelineService['getZoomData']>[4];
export interface ResourceLoaderOptions {
  attributionService?: Pick<ResourceAttributionService, 'getSnapshots' | 'getRange' | 'getDetails'>;
  timelineService: Pick<TimelineService, 'getTimeline' | 'getZoomData'> & Partial<Pick<TimelineService, 'getNavigatorMetric'>>;
  hostnameFilter: TimelineOptions['hostname'];
  includeRunning: boolean;
  activityLimit: number;
  queryHashFilter: string | null;
  queryHashOnly: boolean;
  xraySelection: XRaySelection;
}

/** Adapter to existing core APIs; core has no dependency on the experiment. */
export function createResourceDataLoader(options: ResourceLoaderOptions): ResourceDataLoader {
  const contextCache = new Map<string, Promise<TimeseriesPoint[]>>();
  const legacy: ResourceDataLoader = async (source, mode, timeMs, selectedHost, selection) => {
    const { timelineService, hostnameFilter, includeRunning, activityLimit, queryHashFilter, queryHashOnly, xraySelection } = options;
    const startMs = selection?.kind === 'range' ? selection.startMs : parseOperationTimestamp(source.window_start);
    const endMs = selection?.kind === 'range' ? selection.endMs : parseOperationTimestamp(source.window_end);
    const hostname = selectedHost ?? hostnameFilter;
    const timelineRequest = timelineService.getTimeline({
      timestamp: new Date((startMs + endMs) / 2), windowSeconds: (endMs - startMs) / 2000,
      includeRunning, hostname, activityLimit, activeMetric: mode,
      normalizedQueryHash: queryHashFilter ?? undefined,
    });
    let contextRequest: Promise<{ contextSeries?: TimeseriesPoint[]; warning?: string }> = Promise.resolve({});
    if (selection?.kind === 'range' && timelineService.getNavigatorMetric) {
      const contextStart = Math.min(parseOperationTimestamp(source.window_start), startMs);
      const contextEnd = Math.max(parseOperationTimestamp(source.window_end), endMs);
      const contextKey = JSON.stringify([mode, hostname, contextStart, contextEnd]);
      let request = contextCache.get(contextKey);
      if (!request) {
        request = timelineService.getNavigatorMetric({ startTime: new Date(contextStart), endTime: new Date(contextEnd),
          metric: mode, hostname, bucketSeconds: Math.max(1, Math.ceil((contextEnd - contextStart) / 1000 / 1000)) })
          .then(result => result.points.map(point => ({ t: point.t, v: point.average_v })))
          .catch(error => { contextCache.delete(contextKey); throw error; });
        if (contextCache.size >= 8) contextCache.delete(contextCache.keys().next().value!);
        contextCache.set(contextKey, request);
      }
      contextRequest = request.then(contextSeries => ({ contextSeries }))
        .catch(error => ({ warning: `Timeline context could not be loaded: ${error instanceof Error ? error.message : String(error)}` }));
    }
    const [loaded, context] = await Promise.all([timelineRequest, contextRequest]);
    const extras = context.contextSeries ? { contextSeries: context.contextSeries } : {};
    const filter = (data: MemoryTimeline) => queryHashOnly && queryHashFilter
      ? { ...data, queries: data.queries.filter(q => q.matched_hash), merges: [], mutations: [] } : data;
    const window = selection?.kind === 'range' ? { start: startMs, end: endMs } : resourceSampleWindow(source, timeMs);
    try {
      const sampled = await timelineService.getZoomData(loaded, window.start, window.end, hostname, xraySelection);
      return { data: filter(sampled), ...extras, ...(context.warning ? { samplingWarning: context.warning } : {}) };
    } catch (error) {
      return { data: filter(loaded), ...extras, samplingWarning: `Operation samples could not be loaded: ${error instanceof Error ? error.message : String(error)}${context.warning ? ` · ${context.warning}` : ''}` };
    }
  };
  if (!options.attributionService) return legacy;
  const service = options.attributionService;
  const cache = new Map<string, ReturnType<ResourceDataLoader>>();
  const timelineCache = new Map<string, Promise<MemoryTimeline>>();
  const loader: ResourceDataLoader = (source, mode, timeMs, selectedHost, selection) => {
    const hostname = selectedHost ?? options.hostnameFilter;
    const range = selection?.kind === 'range';
    const chunk = range ? { start: selection.startMs, end: selection.endMs } : resourceSampleWindow(source, timeMs);
    const startMs = range ? Math.floor(chunk.start) : Math.floor(chunk.start / 1000) * 1000;
    const endMs = range ? Math.ceil(chunk.end) : Math.floor(chunk.end / 1000) * 1000 + 1000;
    const key = JSON.stringify([mode, hostname, range, startMs, endMs]);
    const cached = cache.get(key);
    if (cached) return cached;
    const request = { mode, hostname, includeRunning: options.includeRunning,
      normalizedQueryHash: options.queryHashFilter, queryHashOnly: options.queryHashOnly,
      sampleSelection: options.xraySelection, startMs, endMs };
    const result = (async () => {
      // Timeline rows provide server measurements and capacity only. They never supply operation totals.
      const contextStart = range ? startMs : parseOperationTimestamp(source.window_start);
      const contextEnd = range ? endMs : parseOperationTimestamp(source.window_end);
      const contextKey = JSON.stringify([mode,hostname,contextStart,contextEnd]);
      let timelineRequest = timelineCache.get(contextKey);
      if (!timelineRequest) {
        timelineRequest = options.timelineService.getTimeline({ timestamp: new Date((contextStart+contextEnd)/2),
          windowSeconds: (contextEnd-contextStart)/2000, hostname, includeRunning: false, activityLimit: 1, activeMetric: mode })
          .catch(error => { timelineCache.delete(contextKey); throw error; });
        if (timelineCache.size >= 8) timelineCache.delete(timelineCache.keys().next().value!);
        timelineCache.set(contextKey,timelineRequest);
      }
      const [data, aggregation] = await Promise.all([
        timelineRequest,
        range ? service.getRange(request) : service.getSnapshots(request),
      ]);
      let contextSeries: TimeseriesPoint[] | undefined;
      if (range && options.timelineService.getNavigatorMetric) {
        const result = await options.timelineService.getNavigatorMetric({
          startTime: new Date(Math.min(parseOperationTimestamp(source.window_start), startMs)),
          endTime: new Date(Math.max(parseOperationTimestamp(source.window_end), endMs)),
          metric: mode, hostname, bucketSeconds: Math.max(1, Math.ceil((endMs-startMs)/1000000)),
        });
        contextSeries = result.points.map(point => ({ t: point.t, v: point.average_v }));
      }
      return { data, contextSeries, samplingWarning: aggregation.warnings.join(' ') || undefined,
        ...('snapshots' in aggregation ? { snapshots: aggregation } : { summary: aggregation }) };
    })().catch(error => { cache.delete(key); throw error; });
    if (cache.size >= 8) cache.delete(cache.keys().next().value!);
    cache.set(key, result);
    return result;
  };
  loader.loadDetails = request => service.getDetails({ ...request,
    hostname: request.hostname ?? options.hostnameFilter, includeRunning: options.includeRunning,
    normalizedQueryHash: options.queryHashFilter, queryHashOnly: options.queryHashOnly, sampleSelection: options.xraySelection });
  return loader;
}
