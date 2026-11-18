import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { MemoryTimeline } from '@tracehouse/core';
import { createResourceDataLoader, resourceSampleWindow } from '../resource-data-loader';
import { useResourceData } from '../useResourceData';

const start = Date.parse('2026-11-17T16:00:00Z');
function timeline(span = 3600_000): MemoryTimeline {
  return { window_start: new Date(start).toISOString(), window_end: new Date(start + span).toISOString(), target: 'all',
    server_cpu: [], server_memory: [], server_network_send: [], server_network_recv: [], server_disk_read: [], server_disk_write: [],
    host_count: 2, cpu_cores: 8, server_total_ram: 1024, queries: [], merges: [], mutations: [], query_count: 0, merge_count: 0, merge_peak_total: 0, mutation_count: 0 };
}

describe('resource data loading boundary', () => {
  it('uses stable bounded sample windows, including the last partial chunk', () => {
    const data = timeline(3700_000);
    expect(resourceSampleWindow(data, start + 1000)).toEqual(resourceSampleWindow(data, start + 119000));
    expect(resourceSampleWindow(data, start + 120000)).toEqual({ start: start + 120000, end: start + 240000 });
    expect(resourceSampleWindow(data, start + 3700_000)).toEqual({ start: start + 3600_000, end: start + 3700_000 });
    expect(resourceSampleWindow(timeline(600_000), start + 1000)).toEqual({ start, end: start + 600_000 });
  });
  it('loads metric-ranked operations and samples using the selected host', async () => {
    const data = timeline();
    const service = { getTimeline: vi.fn(async () => data), getZoomData: vi.fn(async () => data) };
    const load = createResourceDataLoader({ timelineService: service, hostnameFilter: ['ch-01', 'ch-02'], includeRunning: false,
      activityLimit: 100, queryHashFilter: null, queryHashOnly: false, xraySelection: undefined });
    await load(data, 'memory', start + 130000, 'ch-02');
    expect(service.getTimeline).toHaveBeenCalledWith(expect.objectContaining({ hostname: 'ch-02', activeMetric: 'memory' }));
    expect(service.getZoomData).toHaveBeenCalledWith(data, start + 120000, start + 240000, 'ch-02', undefined);
    // All servers must override the page filter rather than falling back to it.
    await load(data, 'cpu', start + 130000, []);
    expect(service.getTimeline).toHaveBeenLastCalledWith(expect.objectContaining({ hostname: [], activeMetric: 'cpu' }));
    expect(service.getZoomData).toHaveBeenLastCalledWith(data, start + 120000, start + 240000, [], undefined);
  });
  it('loads the complete selected hour, even when the originating timeline was only one minute', async () => {
    const original = timeline(60_000), loaded = timeline();
    const service = { getTimeline: vi.fn(async () => loaded), getZoomData: vi.fn(async () => loaded) };
    const load = createResourceDataLoader({ timelineService: service, hostnameFilter: null, includeRunning: false, activityLimit: 100,
      queryHashFilter: null, queryHashOnly: false, xraySelection: undefined });
    const selection = { kind: 'range' as const, startMs: start - 1800_000, endMs: start + 1800_000 };
    await load(original, 'cpu', start, undefined, selection);
    expect(service.getTimeline).toHaveBeenCalledWith(expect.objectContaining({ timestamp: new Date(start), windowSeconds: 1800 }));
    expect(service.getZoomData).toHaveBeenCalledWith(loaded, selection.startMs, selection.endMs, null, undefined);
  });
  it('keeps full navigator context outside the selected range and caches it while the selection moves', async () => {
    const original = timeline();
    const contextPoints = [{ t: new Date(start).toISOString(), average_v: 1e6, peak_v: 1e6 }, { t: new Date(start + 300000).toISOString(), average_v: 2e6, peak_v: 2e6 }];
    const service = { getTimeline: vi.fn(async () => timeline(60000)), getZoomData: vi.fn(async () => timeline(60000)),
      getNavigatorMetric: vi.fn(async () => ({ window_start: original.window_start, window_end: original.window_end, bucket_seconds: 4, points: contextPoints })) };
    const load = createResourceDataLoader({ timelineService: service, hostnameFilter: ['ch-01', 'ch-02'], includeRunning: false, activityLimit: 100,
      queryHashFilter: null, queryHashOnly: false, xraySelection: undefined });
    const result = await load(original, 'cpu', start + 150000, undefined, { kind: 'range', startMs: start + 120000, endMs: start + 180000 });
    expect(service.getNavigatorMetric).toHaveBeenCalledWith(expect.objectContaining({ startTime: new Date(start), endTime: new Date(start + 3600000), hostname: ['ch-01', 'ch-02'] }));
    expect(result.contextSeries).toEqual(contextPoints.map(point => ({ t: point.t, v: point.average_v })));
    await load(original, 'cpu', start + 210000, undefined, { kind: 'range', startMs: start + 180000, endMs: start + 240000 });
    expect(service.getNavigatorMetric).toHaveBeenCalledTimes(1);
  });
  it('discloses sampling errors and retains the successfully loaded server data', async () => {
    const data = timeline();
    const load = createResourceDataLoader({ timelineService: { getTimeline: vi.fn(async () => data), getZoomData: vi.fn(async () => { throw new Error('sampler unavailable'); }) },
      hostnameFilter: null, includeRunning: false, activityLimit: 100, queryHashFilter: null, queryHashOnly: false, xraySelection: undefined });
    await expect(load(data, 'cpu', start)).resolves.toEqual({ data, samplingWarning: 'Operation samples could not be loaded: sampler unavailable' });
  });
  it('reloads only when crossing a sample chunk and ignores stale host responses', async () => {
    const data = timeline();
    let finishFirst!: (value: { data: MemoryTimeline }) => void;
    const loader = vi.fn().mockImplementationOnce(() => new Promise(resolve => { finishFirst = resolve; }))
      .mockImplementation(async () => ({ data: { ...data, host_count: 1 } }));
    const { result, rerender } = renderHook(({ time, host }) => useResourceData(data, 'cpu', time, host, loader), { initialProps: { time: start + 1000, host: 'ch-01' } });
    rerender({ time: start + 10000, host: 'ch-01' });
    expect(loader).toHaveBeenCalledTimes(1);
    rerender({ time: start + 10000, host: 'ch-02' });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.source.host_count).toBe(1);
    await act(async () => { finishFirst({ data }); });
    expect(result.current.source.host_count).toBe(1);
    rerender({ time: start + 121000, host: 'ch-02' });
    await waitFor(() => expect(loader).toHaveBeenCalledTimes(3));
    expect(loader.mock.calls[2][2]).toBe(start + 180000);
  });
  it('uses the pin snapshot as a seed and restores it after exploring another resource', async () => {
    const original = timeline(60_000), other = { ...original, host_count: 1 };
    const loader = vi.fn(async () => ({ data: other }));
    const { result, rerender } = renderHook(({ mode }: { mode: 'cpu' | 'memory' }) => useResourceData(original, mode, start + 30000, undefined, loader, undefined, {}), { initialProps: { mode: 'cpu' } });
    expect(result.current.source).toBe(original); expect(result.current.loading).toBe(false);
    expect(loader).not.toHaveBeenCalled();
    rerender({ mode: 'memory' });
    await waitFor(() => expect(result.current.source).toBe(other));
    expect(loader).toHaveBeenCalledTimes(1);
    rerender({ mode: 'cpu' });
    expect(result.current.source).toBe(original); expect(result.current.loading).toBe(false);
    expect(loader).toHaveBeenCalledTimes(1);
  });
  it('retries a failed request without showing another metric as successfully loaded', async () => {
    const data = timeline();
    const loader = vi.fn().mockRejectedValueOnce(new Error('connection failed')).mockResolvedValue({ data });
    const { result } = renderHook(() => useResourceData(data, 'memory', start + 1000, undefined, loader));
    await waitFor(() => expect(result.current.loadError).toBe('connection failed'));
    act(() => result.current.retry());
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(loader).toHaveBeenCalledTimes(2);
    expect(result.current.loadError).toBeNull();
  });
});

describe('server aggregate loading and local scrubbing', () => {
  function setup() {
    const data = timeline();
    const summary = { mode: 'cpu' as const, selection: { kind: 'range' as const, startMs: start, endMs: start+3600000 }, groups: [], warnings: [] };
    const aggregation = {
      getSnapshots: vi.fn(async (request: { startMs: number; endMs: number }) => ({ ...request, mode: 'cpu' as const, stepMs: 1000 as const,
        snapshots: new Map([[request.startMs, { ...summary, selection: { kind: 'instant' as const, atMs: request.startMs } }]]), warnings: [] })),
      getRange: vi.fn(async () => summary), getDetails: vi.fn(async () => ({ operations: [], warnings: [] })),
    };
    const service = { getTimeline: vi.fn(async () => data), getZoomData: vi.fn(async () => data) };
    const loader = createResourceDataLoader({ attributionService: aggregation, timelineService: service, hostnameFilter: ['ch-01'],
      includeRunning: false, activityLimit: 100, queryHashFilter: null, queryHashOnly: false, xraySelection: undefined });
    return { data, aggregation, service, loader, summary };
  }
  it('scrubs 100 cursor positions locally and fetches only once per new chunk', async () => {
    const { data, loader, aggregation, service } = setup();
    const { result, rerender } = renderHook(({ time }) => useResourceData(data, 'cpu', time, undefined, loader), { initialProps: { time: start } });
    await waitFor(() => expect(result.current.loading).toBe(false));
    for (let i=1;i<=100;i++) rerender({ time: start+i*1000 });
    expect(aggregation.getSnapshots).toHaveBeenCalledTimes(1);
    expect(service.getZoomData).not.toHaveBeenCalled();
    expect(aggregation.getDetails).not.toHaveBeenCalled();
    expect(result.current.summary?.selection).toEqual({ kind: 'instant', atMs: start+100000 });
    rerender({ time: start+121000 });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(aggregation.getSnapshots).toHaveBeenCalledTimes(2);
    rerender({ time: start+1000 });
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(aggregation.getSnapshots).toHaveBeenCalledTimes(2);
    expect(service.getTimeline).toHaveBeenCalledTimes(1);
  });
  it('gets range totals from the aggregation service and loads details separately', async () => {
    const { data, loader, aggregation, summary, service } = setup();
    const result = await loader(data,'cpu',start,[],summary.selection);
    expect(result.summary).toBe(summary);
    expect(aggregation.getRange).toHaveBeenCalledWith(expect.objectContaining({ startMs: start, endMs: start+3600000, hostname: [] }));
    expect(service.getZoomData).not.toHaveBeenCalled();
    expect(aggregation.getDetails).not.toHaveBeenCalled();
    await loader.loadDetails!({ mode: 'cpu', selection: summary.selection, groups: [], limit: 5, hostname: [] });
    expect(aggregation.getDetails).toHaveBeenCalledWith(expect.objectContaining({ limit: 5, hostname: [] }));
    expect(result.summary).toBe(summary);
  });
  it('does not fall back to limited timeline rows after an aggregation error and permits retry', async () => {
    const { data, loader, aggregation } = setup();
    aggregation.getSnapshots.mockRejectedValueOnce(new Error('aggregate failed'));
    await expect(loader(data,'cpu',start)).rejects.toThrow('aggregate failed');
    await expect(loader(data,'cpu',start)).resolves.toHaveProperty('snapshots');
    expect(aggregation.getSnapshots).toHaveBeenCalledTimes(2);
  });
});
