/**
 * Color constants for Overview and Engine Internals pages
 * Follows the existing Tailwind CSS color scheme
 */

export const OVERVIEW_COLORS = {
  // Activity types
  queries: '#3b82f6',      // blue-500
  merges: '#f59e0b',       // amber-500
  mutations: '#ef4444',    // red-500
  replication: '#8b5cf6',  // purple-500
  mvs: '#06b6d4',          // cyan-500
  other: '#94a3b8',        // slate-400
  
  // Memory subsystems
  markCache: '#22d3ee',    // cyan-400
  uncompCache: '#14b8a6',  // teal-500
  primaryKey: '#a78bfa',   // violet-400
  dictionaries: '#fb923c', // orange-400
  hashTables: '#f472b6',   // pink-400
  mergeBuffers: '#fbbf24', // amber-400
  queryMem: '#60a5fa',     // blue-400
  jemalloc: '#818cf8',     // indigo-400
  osPageCache: '#475569',  // slate-600
  free: '#1e293b',         // slate-800
  
  // CPU states
  cpuUser: '#3b82f6',      // blue-500
  cpuSystem: '#ef4444',    // red-500
  cpuIOWait: '#f59e0b',    // amber-500
  cpuIdle: '#1e293b',      // slate-800
  
  // Health
  ok: '#22c55e',           // green-500
  warn: '#f59e0b',         // amber-500
  crit: '#ef4444',         // red-500
  
  // Background
  bg: '#0f172a',           // slate-900
  bgCard: '#1e293b',       // slate-800
  bgDeep: '#0b1120',
  border: '#334155',       // slate-700
  
  // Text
  text: '#e2e8f0',         // slate-200
  textMuted: '#94a3b8',    // slate-400
  textDim: '#64748b',      // slate-500
} as const;

// Type for color keys
export type OverviewColorKey = keyof typeof OVERVIEW_COLORS;

// Helper to get color by key
export function getColor(key: OverviewColorKey): string {
  return OVERVIEW_COLORS[key];
}

// Resource type colors
export const RESOURCE_COLORS = {
  cpu: {
    queries: OVERVIEW_COLORS.queries,
    merges: OVERVIEW_COLORS.merges,
    mutations: OVERVIEW_COLORS.mutations,
    other: OVERVIEW_COLORS.other,
  },
  memory: {
    queries: OVERVIEW_COLORS.queryMem,
    merges: OVERVIEW_COLORS.mergeBuffers,
    markCache: OVERVIEW_COLORS.markCache,
    uncompressedCache: OVERVIEW_COLORS.uncompCache,
    primaryKeys: OVERVIEW_COLORS.primaryKey,
    dictionaries: OVERVIEW_COLORS.dictionaries,
    other: OVERVIEW_COLORS.other,
  },
  io: {
    queryRead: OVERVIEW_COLORS.queries,
    queryWrite: '#60a5fa',  // blue-400
    mergeRead: OVERVIEW_COLORS.merges,
    mergeWrite: '#fbbf24',  // amber-400
    replicationRead: OVERVIEW_COLORS.replication,
    replicationWrite: '#a78bfa', // violet-400
  },
} as const;

// Thread pool colors
export const THREAD_POOL_COLORS = {
  queryExecution: OVERVIEW_COLORS.queries,
  mergesAndMutations: OVERVIEW_COLORS.merges,
  replicationFetches: OVERVIEW_COLORS.replication,
  schedulePool: '#94a3b8',  // slate-400
  ioThreadPool: '#22c55e',  // green-500
  globalThreadPool: '#64748b', // slate-500
} as const;

export default OVERVIEW_COLORS;
