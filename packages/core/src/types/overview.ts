/**
 * Overview Types
 * Types for the Overview monitoring page that provides real-time resource attribution
 */

export interface ResourceAttribution {
  cpu: {
    totalPct: number;
    cores: number;
    breakdown: {
      queries: number;
      merges: number;
      mutations: number;
      other: number;
    };
  };
  memory: {
    totalRSS: number;
    totalRAM: number;
    tracked: number;
    breakdown: {
      queries: number;
      merges: number;
      markCache: number;
      uncompressedCache: number;
      primaryKeys: number;
      dictionaries: number;
      other: number;
    };
  };
  io: {
    readBytesPerSec: number;
    writeBytesPerSec: number;
    breakdown: {
      queryRead: number;
      queryWrite: number;
      mergeRead: number;
      mergeWrite: number;
      replicationRead: number;
      replicationWrite: number;
    };
  };
}

export interface RunningQueryInfo {
  queryId: string;
  user: string;
  elapsed: number;
  cpuCores: number;
  memoryUsage: number;
  ioReadRate: number;
  rowsRead: number;
  bytesRead: number;
  progress: number;
  queryKind: string;
  query: string;
  hostname?: string;
  profileEvents?: {
    userTimeMicroseconds: number;
    systemTimeMicroseconds: number;
    osReadBytes: number;
    osWriteBytes: number;
    selectedParts: number;
    selectedMarks: number;
    markCacheHits: number;
    markCacheMisses: number;
  };
}


export interface ActiveMergeInfo {
  database: string;
  table: string;
  partName: string;
  elapsed: number;
  progress: number;
  memoryUsage: number;
  readBytesPerSec: number;
  writeBytesPerSec: number;
  rowsRead: number;
  numParts: number;
  isMutation: boolean;
  cpuEstimate: number;
  mergeType: string;
  hostname?: string;
}

export interface ReplicationSummary {
  totalTables: number;
  healthyTables: number;
  readonlyReplicas: number;
  maxDelay: number;
  queueSize: number;
  fetchesActive: number;
}

export interface AlertInfo {
  severity: 'warn' | 'crit';
  message: string;
  source: 'parts' | 'replica' | 'disk' | 'mutation';
  details?: {
    database?: string;
    table?: string;
    partition?: string;
    value?: number;
  };
}

export interface QpsPoint {
  time: string;
  qps: number;
}

export interface QueryConcurrency {
  running: number;
  queued: number;
  maxConcurrent: number;
  /** Number of queries rejected with TOO_MANY_SIMULTANEOUS_QUERIES in the last hour */
  rejectedRecent: number;
  /** Recent queries/second sparkline data */
  qpsHistory: QpsPoint[];
}

export interface OverviewData {
  serverInfo: {
    hostname: string;
    version: string;
    uptime: string;
    cores: number;
    totalRAM: number;
    /** Container/cgroup environment info, if detected. */
    environment?: {
      isContainerized: boolean;
      isKubernetes: boolean;
      effectiveCores: number;
      hostCores: number;
      isCgroupLimited: boolean;
    };
    /** Hostnames of all cluster nodes (from clusterAllReplicas). Empty on single-node. */
    clusterHosts?: string[];
  };
  resourceAttribution: ResourceAttribution;
  runningQueries: RunningQueryInfo[];
  activeMerges: ActiveMergeInfo[];
  replication: ReplicationSummary;
  queryConcurrency: QueryConcurrency;
  alerts: AlertInfo[];
  deepDiveWidgets?: DeepDiveWidgets;
  lastPollTime: Date;
}

/**
 * Configuration for alert thresholds in OverviewService
 */
export interface AlertThresholds {
  /** Max parts per partition before warning (default: 150) */
  partsPerPartition: number;
  /** Min free disk ratio before warning (default: 0.15 = 15%) */
  diskFreeRatio: number;
  /** Max pending mutations before warning (default: 50) */
  pendingMutations: number;
}

// ── Deep-dive widget types ─────────────────────────────────────────────

export interface TopTableInfo {
  database: string;
  table: string;
  totalBytes: number;
  totalRows: number;
  partCount: number;
}

export interface EngineHealthInfo {
  jemallocAllocated: number;
  jemallocResident: number;
  memoryResident: number;
  memoryTotal: number;
  pools: {
    name: string;
    active: number;
    size: number;
  }[];
}

export interface SlowQueriesSummary {
  count: number;
  maxDurationMs: number;
  avgDurationMs: number;
}

export interface WorstOrderingKey {
  database: string;
  table: string;
  selectedMarks: number;
  selectedParts: number;
  totalReadRows: number;
  queryCount: number;
}

export interface CpuSpikesInfo {
  spikeCount: number;
  maxCpu: number;
}

export interface DeepDiveWidgets {
  topTables: TopTableInfo[];
  engineHealth: EngineHealthInfo | null;
  slowQueries: SlowQueriesSummary | null;
  worstOrderingKey: WorstOrderingKey | null;
  cpuSpikes: CpuSpikesInfo | null;
}

export const DEFAULT_ALERT_THRESHOLDS: AlertThresholds = {
  partsPerPartition: 150,
  diskFreeRatio: 0.15,
  pendingMutations: 50,
};
