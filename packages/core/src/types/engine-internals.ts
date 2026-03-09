/**
 * Engine Internals Types
 * Types for the Engine Internals page that provides deep-dive engine diagnostics
 */

export interface MemorySubsystem {
  id: string;
  label: string;
  bytes: number;
  color: string;
  detail: string;
  icon: string;
  sub?: {
    files?: number;
    cells?: number;
    hitRate?: number;
    missesPerSec?: number;
    configured?: number;
    tables?: number;
    count?: number;
    names?: string[];
  };
}

export interface MemoryXRay {
  totalRSS: number;
  totalRAM: number;
  jemalloc: {
    allocated: number;
    resident: number;
    mapped: number;
    retained: number;
    metadata: number;
  };
  subsystems: MemorySubsystem[];
  osPageCache: number;
  free: number;
  fragmentationPct: number;
}

export interface CPUCoreInfo {
  core: number;
  pct: number;
  state: 'user' | 'system' | 'iowait' | 'idle';
  owner: string | null;
  // Breakdown percentages for stacked visualization
  breakdown?: {
    user: number;
    system: number;
    iowait: number;
    idle: number;
  };
}

export interface ThreadPoolInfo {
  name: string;
  active: number;
  max: number;
  color: string;
  metric: string;
  isSaturated: boolean;
}


export interface PKIndexEntry {
  database: string;
  table: string;
  pkMemory: number;
  pkAllocated: number;
  parts: number;
  rows: number;
  granules: number;
}

export interface DictionaryInfo {
  name: string;
  type: string;
  bytesAllocated: number;
  elementCount: number;
  loadFactor: number;
  source: string;
  loadingStatus: string;
  lastSuccessfulUpdate: string | null;
}

export interface QueryMemoryBreakdown {
  component: string;
  bytes: number;
  detail: string;
  color: string;
}

export interface QueryPipelineStage {
  stage: string;
  threads: number;
  status: 'active' | 'waiting' | 'pending';
  detail: string;
}

export interface QueryInternals {
  queryId: string;
  kind: string;
  user: string;
  elapsed: number;
  query: string;
  totalMemory: number;
  memoryBreakdown: QueryMemoryBreakdown[];
  pipeline: QueryPipelineStage[];
  profileEvents: {
    userTimeMicroseconds: number;
    systemTimeMicroseconds: number;
    realTimeMicroseconds: number;
    osIOWaitMicroseconds: number;
    readCompressedBytes: number;
    selectedParts: number;
    selectedMarks: number;
    totalMarks: number;
    markCacheHits: number;
    markCacheMisses: number;
  };
  threads: number;
  maxThreads: number;
}

export interface EngineInternalsData {
  serverInfo: {
    hostname: string;
    version: string;
    cores: number;
    totalRAM: number;
  };
  memoryXRay: MemoryXRay;
  cpuCores: CPUCoreInfo[];
  /** When running in a container (Kubernetes), the effective core count may differ from the host */
  cpuCoresMeta?: {
    /** Effective cores available to the ClickHouse process (cgroup-limited) */
    effectiveCores: number;
    /** Total logical cores on the host node */
    hostCores: number;
    /** True when a cgroup CPU limit was detected */
    isCgroupLimited: boolean;
  };
  threadPools: ThreadPoolInfo[];
  pkIndex: PKIndexEntry[];
  dictionaries: DictionaryInfo[];
  queryInternals: QueryInternals[];
  lastPollTime: Date;
}


/** CPU sampling data from trace_log, aggregated by thread pool */
export interface CPUSamplingByThread {
  threadName: string;
  cpuSamples: number;
  querySamples: number;
  backgroundSamples: number;
  /** Mapped pool category for grouping/coloring */
  pool: 'queries' | 'merges' | 'mutations' | 'merge_mutate' | 'replication' | 'io' | 'schedule' | 'handler' | 'other';
}

/** Top CPU-consuming function from trace_log */
export interface TopCPUFunction {
  threadName: string;
  functionName: string;
  samples: number;
}

/** Full CPU sampling attribution data */
export interface CPUSamplingData {
  /** Per-thread-name sample counts */
  byThread: CPUSamplingByThread[];
  /** Top functions burning CPU */
  topFunctions: TopCPUFunction[];
  /** Total CPU samples in the window */
  totalSamples: number;
  /** Window size in seconds */
  windowSeconds: number;
}


// =============================================================================
// Per-Core Timeline Types
// =============================================================================

/** A single time slot on a specific CPU core */
export interface CoreTimelineSlot {
  /** Physical CPU core number */
  core: number;
  /** Slot timestamp (ISO string, 100ms buckets) */
  time: string;
  /** Slot timestamp as epoch ms (for zoom math) */
  timeMs: number;
  /** Dominant thread name in this slot */
  threadName: string;
  /** Whether this was query work (has query_id) or background */
  isQuery: boolean;
  /** Query ID if this was query work */
  queryId: string;
  /** Thread pool category */
  pool: CPUSamplingByThread['pool'];
  /** Number of samples in this slot (density indicator) */
  samples: number;
  /** Dominant trace type: 'CPU' = on-CPU execution, 'Real' = wall-clock (may include IO wait) */
  traceType: 'CPU' | 'Real' | 'Mixed';
  /** CPU-type sample count in this slot (for filtering/visual cues) */
  cpuSamples: number;
  /** Real-type sample count in this slot */
  realSamples: number;
}

/** Full per-core timeline data */
export interface CoreTimelineData {
  /** Slots grouped by core, sorted by time */
  slots: CoreTimelineSlot[];
  /** Number of distinct cores observed */
  coreCount: number;
  /** Time window in seconds */
  windowSeconds: number;
  /** Earliest slot timestamp */
  minTime: string;
  /** Latest slot timestamp */
  maxTime: string;
  /** Total samples across all cores */
  totalSamples: number;
  /** True when cpu_id was unavailable and cores are synthetic (thread_id % N) */
  syntheticCores?: boolean;
}
