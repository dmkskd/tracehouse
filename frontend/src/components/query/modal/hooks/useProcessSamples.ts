import { useQueryXRayPreference } from '../../query-xray-preference';
/**
 * useProcessSamples — fetches per-sample query resource series with server-side
 * delta computation (no ProfileEvents maps shipped to browser).
 *
 * The table is chosen by selectQueryXRaySource(), not by this hook: either
 * tracehouse.processes_history (the sampler) or system.query_metric_log. Both
 * builders emit the same columns, so everything below the fetch is identical.
 * The selection is returned as `meta` so the UI can badge the source and hide
 * the fields the chosen source could not populate (`meta.missing`).
 *
 * Exposes both cumulative and per-interval delta values so consumers can choose
 * without re-summing.
 *
 * Supports multi-host queries: samples are fetched with hostname, then grouped
 * per host and aggregated into an "All" view that sums metrics across hosts.
 */

import { useState, useCallback, useMemo } from 'react';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';
import {
  mapHostProcessSampleRow,
  selectQueryXRaySource,
  buildXRaySamplesSQL,
  tagQuery,
  sourceTag,
  TAB_QUERIES,
  type ProcessSample,
  type HostProcessSample,
  type XRayQueryState,
  type QueryXRaySourceSelection,
} from '@tracehouse/core';
import { useMonitoringCapabilitiesStore } from '../../../../stores/monitoringCapabilitiesStore';

export type { ProcessSample, HostProcessSample };

export interface ProcessSamplesResult {
  /** Which table these samples came from, and what it could not provide. */
  meta: QueryXRaySourceSelection;
  /** Aggregated samples (summed across all hosts, aligned by time bucket) */
  samples: ProcessSample[];
  /** Per-host sample arrays, keyed by hostname */
  hostSamples: Map<string, ProcessSample[]>;
  /** Sorted list of hostnames that contributed samples */
  hosts: string[];
  isLoading: boolean;
  error: string | null;
  fetch: () => Promise<void>;
}

/**
 * Aggregate host samples into a single "All" time series.
 * Buckets samples to 0.5s intervals and sums metrics across hosts.
 */
function aggregateHostSamples(
  hostSamples: Map<string, ProcessSample[]>,
): ProcessSample[] {
  // Collect all samples with their rounded time bucket
  const buckets = new Map<number, ProcessSample[]>();
  for (const samples of hostSamples.values()) {
    for (const s of samples) {
      const bucket = Math.round(s.t * 2) / 2; // 0.5s buckets
      let arr = buckets.get(bucket);
      if (!arr) {
        arr = [];
        buckets.set(bucket, arr);
      }
      arr.push(s);
    }
  }

  // Sum metrics per bucket
  const sortedTimes = Array.from(buckets.keys()).sort((a, b) => a - b);
  return sortedTimes.map(t => {
    const group = buckets.get(t)!;
    const agg: ProcessSample = {
      t,
      elapsed: Math.max(...group.map(s => s.elapsed)),
      thread_count: group.reduce((sum, s) => sum + s.thread_count, 0),
      memory_mb: group.reduce((sum, s) => sum + s.memory_mb, 0),
      peak_memory_mb: group.reduce((sum, s) => sum + s.peak_memory_mb, 0),
      read_rows: group.reduce((sum, s) => sum + s.read_rows, 0),
      written_rows: group.reduce((sum, s) => sum + s.written_rows, 0),
      read_bytes: group.reduce((sum, s) => sum + s.read_bytes, 0),
      cpu_us: group.reduce((sum, s) => sum + s.cpu_us, 0),
      io_wait_us: group.reduce((sum, s) => sum + s.io_wait_us, 0),
      cpu_wait_us: group.reduce((sum, s) => sum + s.cpu_wait_us, 0),
      net_recv_wait_us: group.reduce((sum, s) => sum + s.net_recv_wait_us, 0),
      net_send_wait_us: group.reduce((sum, s) => sum + s.net_send_wait_us, 0),
      net_send_bytes: group.reduce((sum, s) => sum + s.net_send_bytes, 0),
      net_recv_bytes: group.reduce((sum, s) => sum + s.net_recv_bytes, 0),
      d_cpu_cores: group.reduce((sum, s) => sum + s.d_cpu_cores, 0),
      d_io_wait_s: group.reduce((sum, s) => sum + s.d_io_wait_s, 0),
      d_cpu_wait_s: group.reduce((sum, s) => sum + s.d_cpu_wait_s, 0),
      d_net_recv_wait_s: group.reduce((sum, s) => sum + s.d_net_recv_wait_s, 0),
      d_net_send_wait_s: group.reduce((sum, s) => sum + s.d_net_send_wait_s, 0),
      d_read_mb: group.reduce((sum, s) => sum + s.d_read_mb, 0),
      d_read_rows: group.reduce((sum, s) => sum + s.d_read_rows, 0),
      d_written_rows: group.reduce((sum, s) => sum + s.d_written_rows, 0),
      d_net_send_kb: group.reduce((sum, s) => sum + s.d_net_send_kb, 0),
      d_net_recv_kb: group.reduce((sum, s) => sum + s.d_net_recv_kb, 0),
      rate_clamped: group.some(s => s.rate_clamped),
      rate_unclamped: group.some(s => s.rate_unclamped),
    };
    return agg;
  });
}

export function useProcessSamples(
  queryId: string | undefined,
  queryState: XRayQueryState = 'unknown',
  /**
   * Query start time, when known. Only used by the query_metric_log source, to
   * bound its system.query_log identity scan to the right day; without it an
   * older query falls outside the default lookback and returns nothing.
   */
  startedAt?: string,
): ProcessSamplesResult {
  const services = useClickHouseServices();
  const flags = useMonitoringCapabilitiesStore(s => s.flags);
  const probeStatus = useMonitoringCapabilitiesStore(s => s.probeStatus);
  const { preference } = useQueryXRayPreference();

  // Until the capability probe has answered, every flag reads false, which would
  // make the X-Ray refuse to query at all — a regression against the old
  // behaviour of simply asking processes_history and reporting what came back.
  // Embedders that never probe (the Grafana plugin) would be stuck there. So an
  // unprobed connection is assumed to have the sampler, and the real answer
  // takes over once it arrives.
  const probed = probeStatus === 'done';
  const meta = useMemo(() => selectQueryXRaySource({
    availability: {
      processesHistory: probed ? flags.hasProcessesHistory : true,
      queryMetricLog: probed ? flags.hasQueryMetricLogXRay : false,
    },
    preference,
    queryState,
  }), [probed, flags.hasProcessesHistory, flags.hasQueryMetricLogXRay, preference, queryState]);

  const [samples, setSamples] = useState<ProcessSample[]>([]);
  const [hostSamples, setHostSamples] = useState<Map<string, ProcessSample[]>>(new Map());
  const [hosts, setHosts] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async () => {
    if (!services || !queryId) return;
    if (!meta.source) {
      setError(meta.note ?? 'No X-Ray source available on this connection');
      return;
    }
    setIsLoading(true);
    setError(null);
    try {
      const sql = buildXRaySamplesSQL({
        availability: {
          processesHistory: probed ? flags.hasProcessesHistory : true,
          queryMetricLog: probed ? flags.hasQueryMetricLogXRay : false,
        },
        preference,
        queryState: queryState,
      }, [queryId], { startedAt, perHost: true });
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(sql, sourceTag(TAB_QUERIES, meta.source === 'query_metric_log'
          ? 'queryMetricLogSamples'
          : 'processSamples')),
      );
      const tagged = rows.map(mapHostProcessSampleRow);

      // Group by hostname
      const perHost = new Map<string, ProcessSample[]>();
      for (const s of tagged) {
        const { hostname, ...sample } = s;
        const key = hostname;
        let arr = perHost.get(key);
        if (!arr) {
          arr = [];
          perHost.set(key, arr);
        }
        // Strip hostname for the ProcessSample stored per-host
        arr.push(sample);
      }

      const sortedHosts = Array.from(perHost.keys()).sort();
      setHostSamples(perHost);
      setHosts(sortedHosts);

      // Aggregate: if single host, use directly; if multi-host, sum across hosts
      if (perHost.size <= 1) {
        const single = perHost.values().next().value;
        setSamples(single || []);
      } else {
        setSamples(aggregateHostSamples(perHost));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch process samples');
    } finally {
      setIsLoading(false);
    }
  }, [services, queryId, meta, startedAt, probed, flags.hasProcessesHistory, flags.hasQueryMetricLogXRay, preference, queryState]);

  return { meta, samples, hostSamples, hosts, isLoading, error, fetch };
}
