import { useEffect, useState } from 'react';
import type { MemoryTimeline, AttributionSelection, TimeseriesPoint } from '@tracehouse/core';
import type { MetricMode } from '../timeline-constants';
import type { ResourceDataLoader } from './resource-exploration-model';
import { resourceSampleWindow } from './resource-data-loader';

/** Request identity includes resource, host, and sample window; late responses cannot replace the current view. */
export function useResourceData(original: MemoryTimeline, mode: MetricMode, timeMs: number, scope: string | readonly string[] | undefined, loader?: ResourceDataLoader, selection?: AttributionSelection, initial?: { samplingWarning?: string }) {
  const isRange = selection?.kind === 'range';
  const { start, end } = isRange ? { start: selection.startMs, end: selection.endMs } : resourceSampleWindow(original, timeMs);
  const scopeKey = JSON.stringify([mode, scope ?? null]);
  const key = JSON.stringify([mode, scope ?? null, isRange ? 'range' : 'instant', start, end]);
  const [initialRequest, setInitialRequest] = useState(() => initial && loader ? { key, scopeKey, loader, data: original, samplingWarning: initial.samplingWarning, contextSeries: undefined as TimeseriesPoint[] | undefined } : null);
  const [revision, setRevision] = useState(0);
  const [loaded, setLoaded] = useState<{ key: string; scopeKey: string; loader: ResourceDataLoader; data: MemoryTimeline; samplingWarning?: string; contextSeries?: TimeseriesPoint[] } | null>(null);
  const [error, setError] = useState<{ key: string; loader: ResourceDataLoader; message: string } | null>(null);
  useEffect(() => {
    if (!loader) return;
    if (initialRequest?.key === key && initialRequest.loader === loader) return;
    let cancelled = false;
    // Point scrubbing uses stable chunks. Range dragging waits for a short pause
    // so a pointer movement does not issue a new hour-long request per frame.
    const run = () => loader(original, mode, (start + end) / 2, scope, isRange ? { kind: 'range', startMs: start, endMs: end } : undefined).then(result => {
      if (!cancelled) setLoaded({ key, scopeKey, loader, ...result });
    }).catch(reason => {
      if (!cancelled) setError({ key, loader, message: reason instanceof Error ? reason.message : String(reason) });
    });
    const timer = isRange ? setTimeout(run, 200) : undefined;
    if (!isRange) void run();
    return () => { cancelled = true; if (timer !== undefined) clearTimeout(timer); };
  }, [loader, original, mode, scope, start, end, key, scopeKey, revision, isRange, initialRequest]);
  const current = loaded?.key === key && loaded.loader === loader ? loaded
    : initialRequest?.key === key && initialRequest.loader === loader ? initialRequest : null;
  const loadError = current ? null : error?.key === key && error.loader === loader ? error.message : null;
  return {
    retry: () => { setInitialRequest(null); setError(null); setLoaded(null); setRevision(value => value + 1); },
    source: current?.data ?? original,
    loading: !!loader && !current && !loadError,
    loadError,
    samplingWarning: current?.samplingWarning,
    contextSeries: current?.contextSeries ?? (loaded?.loader === loader && loaded?.scopeKey === scopeKey ? loaded.contextSeries : undefined),
  };
}
