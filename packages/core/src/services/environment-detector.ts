/**
 * EnvironmentDetector — centralized detection of the runtime environment.
 *
 * Detects whether ClickHouse is running inside a container (Docker, Kubernetes),
 * resolves the effective CPU core count (cgroup-limited vs host), and exposes
 * a single EnvironmentInfo object that the rest of the app can consume.
 *
 * This replaces the duplicated cgroup detection logic that was scattered across
 * MetricsCollector, TimelineService, and EngineInternalsService.
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import { tagQuery } from '../queries/builder.js';
import { sourceTag } from '../queries/source-tags.js';

const TAG = 'env';

/** Immutable snapshot of the detected environment. */
export interface EnvironmentInfo {
  /** True when a cgroup CPU limit was detected (container / k8s pod). */
  isContainerized: boolean;
  /** True when the hostname pattern suggests a Kubernetes pod. */
  isKubernetes: boolean;
  /** Effective CPU cores available to the ClickHouse process (cgroup-limited). */
  effectiveCores: number;
  /** Total logical cores on the host node. */
  hostCores: number;
  /** True when effectiveCores < hostCores. */
  isCgroupLimited: boolean;
  /** Total RAM visible to the OS (bytes). May be host RAM in containers. */
  hostMemoryBytes: number;
  /** Cgroup memory limit (bytes), or 0 if no limit detected. */
  cgroupMemoryLimitBytes: number;
  /** Effective memory: cgroup limit if set, otherwise host memory. */
  effectiveMemoryBytes: number;
  /** Raw hostname as reported by ClickHouse. */
  hostname: string;
}

/** Default / unknown environment — safe fallback. */
const UNKNOWN_ENV: EnvironmentInfo = {
  isContainerized: false,
  isKubernetes: false,
  effectiveCores: 0,
  hostCores: 0,
  isCgroupLimited: false,
  hostMemoryBytes: 0,
  cgroupMemoryLimitBytes: 0,
  effectiveMemoryBytes: 0,
  hostname: '',
};

/**
 * Kubernetes pod hostname patterns:
 * - StatefulSet: <name>-<ordinal>  e.g. clickhouse-0, chi-default-0-0
 * - Deployment:  <name>-<replicaset>-<hash>  e.g. clickhouse-7b4f9d6c5-x2k9p
 * - DaemonSet:   <name>-<hash>  e.g. clickhouse-x2k9p
 *
 * Common signals: contains hyphens + trailing numeric/hash segments,
 * or matches known k8s operator naming conventions.
 */
const K8S_HOSTNAME_RE = /^[a-z][\w.-]*-\d+$/i;                    // StatefulSet
const K8S_DEPLOY_RE = /^[a-z][\w.-]*-[a-z0-9]{5,10}-[a-z0-9]{5}$/i; // Deployment/RS
const K8S_CHI_RE = /^chi-/i;                                       // Altinity operator

function looksLikeK8sHostname(hostname: string): boolean {
  return K8S_HOSTNAME_RE.test(hostname)
    || K8S_DEPLOY_RE.test(hostname)
    || K8S_CHI_RE.test(hostname);
}

export class EnvironmentDetector {
  private cache: EnvironmentInfo | null = null;

  constructor(private adapter: IClickHouseAdapter) {}

  /** Clear cached result (e.g. on reconnect). */
  invalidate(): void {
    this.cache = null;
  }

  /**
   * Detect the environment. Result is cached until invalidate() is called.
   * Safe to call repeatedly — subsequent calls return the cached snapshot.
   */
  async detect(): Promise<EnvironmentInfo> {
    if (this.cache) return this.cache;

    try {
      const info = await this.detectInternal();
      this.cache = info;
      return info;
    } catch (error) {
      console.error('[EnvironmentDetector] detection failed:', error);
      return { ...UNKNOWN_ENV };
    }
  }

  /** Return cached info synchronously, or null if not yet detected. */
  getCached(): EnvironmentInfo | null {
    return this.cache;
  }

  private async detectInternal(): Promise<EnvironmentInfo> {
    // Fetch all the signals we need in parallel
    const [asyncMetrics, hostname, cgroupCpu, maxThreads, cgroupMem] = await Promise.all([
      this.fetchAsyncMetrics(),
      this.fetchHostname(),
      this.fetchCGroupMaxCPU(),
      this.fetchMaxThreads(),
      this.fetchCGroupMemoryLimit(),
    ]);

    const hostCores = asyncMetrics.get('NumberOfCPUCores')
      || asyncMetrics.get('NumberOfPhysicalCores')
      || 0;

    const hostMemoryBytes = asyncMetrics.get('OSMemoryTotal') || 0;

    // Determine effective cores
    let effectiveCores = hostCores;
    let isCgroupLimited = false;

    // 1. CGroupMaxCPU (ClickHouse >= 23.8) — most reliable
    if (cgroupCpu > 0 && cgroupCpu < hostCores) {
      effectiveCores = Math.round(cgroupCpu);
      isCgroupLimited = true;
    }
    // 2. max_threads fallback — ClickHouse sets this to detected CPU count at startup
    else if (maxThreads > 0 && maxThreads < hostCores && maxThreads <= 256) {
      effectiveCores = maxThreads;
      isCgroupLimited = true;
    }

    // Determine effective memory
    const cgroupMemoryLimitBytes = cgroupMem;
    const effectiveMemoryBytes = (cgroupMemoryLimitBytes > 0 && cgroupMemoryLimitBytes < hostMemoryBytes)
      ? cgroupMemoryLimitBytes
      : hostMemoryBytes;

    // Container detection heuristics
    const isKubernetes = looksLikeK8sHostname(hostname);
    const isContainerized = isCgroupLimited || isKubernetes;

    return {
      isContainerized,
      isKubernetes,
      effectiveCores,
      hostCores,
      isCgroupLimited,
      hostMemoryBytes,
      cgroupMemoryLimitBytes,
      effectiveMemoryBytes,
      hostname,
    };
  }

  private async fetchAsyncMetrics(): Promise<Map<string, number>> {
    try {
      const rows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(
          `SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('NumberOfCPUCores', 'NumberOfPhysicalCores', 'OSMemoryTotal')`,
          sourceTag(TAG, 'asyncMetrics')
        )
      );
      return new Map(rows.map(r => [String(r.metric), Number(r.value)]));
    } catch {
      return new Map();
    }
  }

  private async fetchHostname(): Promise<string> {
    try {
      const rows = await this.adapter.executeQuery<{ h: string }>(
        tagQuery(`SELECT hostName() AS h`, sourceTag(TAG, 'hostname'))
      );
      return rows.length > 0 ? String(rows[0].h) : '';
    } catch {
      return '';
    }
  }

  private async fetchCGroupMaxCPU(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ value: number }>(
        tagQuery(
          `SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMaxCPU' LIMIT 1`,
          sourceTag(TAG, 'cgroupCpu')
        )
      );
      return rows.length > 0 ? Number(rows[0].value) : 0;
    } catch {
      return 0;
    }
  }

  private async fetchMaxThreads(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ value: string }>(
        tagQuery(
          `SELECT value FROM system.settings WHERE name = 'max_threads' LIMIT 1`,
          sourceTag(TAG, 'maxThreads')
        )
      );
      return rows.length > 0 ? parseInt(String(rows[0].value), 10) : 0;
    } catch {
      return 0;
    }
  }

  private async fetchCGroupMemoryLimit(): Promise<number> {
    try {
      // CGroupMemoryLimit (CH 23.8–25.x) was renamed to CGroupMemoryTotal (CH 26+)
      const rows = await this.adapter.executeQuery<{ value: number }>(
        tagQuery(
          `SELECT value FROM system.asynchronous_metrics WHERE metric IN ('CGroupMemoryTotal', 'CGroupMemoryLimit') LIMIT 1`,
          sourceTag(TAG, 'cgroupMem')
        )
      );
      if (rows.length > 0) {
        const val = Number(rows[0].value);
        // CGroupMemoryLimit/Total returns a very large number when no limit is set
        if (val > 0 && val < 1e18) return val;
      }
    } catch (err) {
      console.warn('[EnvironmentDetector] Memory limit metric not available:', err);
    }
    return 0;
  }
}
