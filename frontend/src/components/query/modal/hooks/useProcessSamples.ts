/**
 * useProcessSamples — fetches per-second process samples from tracehouse.processes_history
 * with server-side delta computation (no ProfileEvents maps shipped to browser).
 *
 * Exposes both cumulative and per-interval delta values so consumers can choose
 * without re-summing.
 *
 * Supports multi-host queries: samples are fetched with hostname, then grouped
 * per host and aggregated into an "All" view that sums metrics across hosts.
 */

import { useState, useCallback } from 'react';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';
import {
  buildHostProcessSamplesSQL,
  mapHostProcessSampleRow,
  type ProcessSample,
  type HostProcessSample,
} from '@tracehouse/core';

export type { ProcessSample, HostProcessSample };

export interface ProcessSamplesResult {
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
      net_send_bytes: group.reduce((sum, s) => sum + s.net_send_bytes, 0),
      net_recv_bytes: group.reduce((sum, s) => sum + s.net_recv_bytes, 0),
      d_cpu_cores: group.reduce((sum, s) => sum + s.d_cpu_cores, 0),
      d_io_wait_s: group.reduce((sum, s) => sum + s.d_io_wait_s, 0),
      d_read_mb: group.reduce((sum, s) => sum + s.d_read_mb, 0),
      d_read_rows: group.reduce((sum, s) => sum + s.d_read_rows, 0),
      d_written_rows: group.reduce((sum, s) => sum + s.d_written_rows, 0),
      d_net_send_kb: group.reduce((sum, s) => sum + s.d_net_send_kb, 0),
      d_net_recv_kb: group.reduce((sum, s) => sum + s.d_net_recv_kb, 0),
    };
    return agg;
  });
}

export function useProcessSamples(queryId: string | undefined): ProcessSamplesResult {
  const services = useClickHouseServices();
  const [samples, setSamples] = useState<ProcessSample[]>([]);
  const [hostSamples, setHostSamples] = useState<Map<string, ProcessSample[]>>(new Map());
  const [hosts, setHosts] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async () => {
    if (!services || !queryId) return;
    setIsLoading(true);
    setError(null);
    try {
      const sql = buildHostProcessSamplesSQL(queryId);
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(sql);
      const tagged = rows.map(mapHostProcessSampleRow);

      // Group by hostname
      const perHost = new Map<string, ProcessSample[]>();
      for (const s of tagged) {
        const key = s.hostname;
        let arr = perHost.get(key);
        if (!arr) {
          arr = [];
          perHost.set(key, arr);
        }
        // Strip hostname for the ProcessSample stored per-host
        const { hostname: _, ...sample } = s;
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
  }, [services, queryId]);

  return { samples, hostSamples, hosts, isLoading, error, fetch };
}
