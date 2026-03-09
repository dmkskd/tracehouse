import { create } from 'zustand';

// Connection types
export interface ConnectionProfile {
  id: string;
  name: string;
  host: string;
  port: number;
  user: string;
  database: string;
  isConnected: boolean;
}

// Metrics types
export interface ServerMetrics {
  timestamp: Date;
  cpuUsage: number;
  memoryUsed: number;
  memoryTotal: number;
  diskReadBytes: number;
  diskWriteBytes: number;
}

// Query types
export interface RunningQuery {
  queryId: string;
  user: string;
  query: string;
  elapsedSeconds: number;
  memoryUsage: number;
  readRows: number;
  progress: number;
}

export interface QueryHistoryItem {
  queryId: string;
  type: string;
  queryStartTime: Date;
  queryDurationMs: number;
  readRows: number;
  readBytes: number;
  resultRows: number;
  resultBytes: number;
  memoryUsage: number;
  query: string;
  exception?: string;
  user: string;
}

// Database types
export interface DatabaseInfo {
  name: string;
  engine: string;
  tableCount: number;
}

export interface TableInfo {
  database: string;
  name: string;
  engine: string;
  totalRows: number;
  totalBytes: number;
  partitionKey?: string;
  sortingKey?: string;
}

export interface PartInfo {
  partitionId: string;
  name: string;
  rows: number;
  bytesOnDisk: number;
  modificationTime: Date;
  level: number;
  primaryKeyBytesInMemory: number;
}

// Merge types
export interface MergeInfo {
  database: string;
  table: string;
  elapsed: number;
  progress: number;
  numParts: number;
  sourcePartNames: string[];
  resultPartName: string;
  totalSizeBytesCompressed: number;
  rowsRead: number;
  rowsWritten: number;
}

// Trace types
export interface TraceLog {
  eventTime: Date;
  queryId: string;
  level: string;
  message: string;
  source: string;
}

export interface ExplainResult {
  explainType: 'AST' | 'SYNTAX' | 'PLAN' | 'PIPELINE' | 'QUERY_TREE';
  output: string;
  parsedTree?: Record<string, unknown>;
}

// Store interface
interface MonitorStore {
  // Connection state
  connections: ConnectionProfile[];
  activeConnectionId: string | null;

  // Metrics state
  serverMetrics: ServerMetrics | null;
  metricsHistory: ServerMetrics[];

  // Query state
  runningQueries: RunningQuery[];
  queryHistory: QueryHistoryItem[];

  // Database state
  databases: DatabaseInfo[];
  selectedDatabase: string | null;
  tables: TableInfo[];
  selectedTable: string | null;
  tableParts: PartInfo[];

  // Merge state
  activeMerges: MergeInfo[];

  // Trace state
  selectedQueryId: string | null;
  traceLogs: TraceLog[];
  explainResult: ExplainResult | null;

  // UI state
  isPolling: boolean;
  pollInterval: number;
  error: string | null;

  // Actions
  setActiveConnection: (id: string | null) => void;
  setServerMetrics: (metrics: ServerMetrics | null) => void;
  addMetricsHistory: (metrics: ServerMetrics) => void;
  setRunningQueries: (queries: RunningQuery[]) => void;
  setQueryHistory: (history: QueryHistoryItem[]) => void;
  setDatabases: (databases: DatabaseInfo[]) => void;
  selectDatabase: (name: string | null) => void;
  setTables: (tables: TableInfo[]) => void;
  selectTable: (name: string | null) => void;
  setTableParts: (parts: PartInfo[]) => void;
  setActiveMerges: (merges: MergeInfo[]) => void;
  setSelectedQueryId: (queryId: string | null) => void;
  setTraceLogs: (logs: TraceLog[]) => void;
  setExplainResult: (result: ExplainResult | null) => void;
  setIsPolling: (isPolling: boolean) => void;
  setPollInterval: (interval: number) => void;
  setError: (error: string | null) => void;
  clearError: () => void;
}

export const useMonitorStore = create<MonitorStore>((set) => ({
  // Initial connection state
  connections: [],
  activeConnectionId: null,

  // Initial metrics state
  serverMetrics: null,
  metricsHistory: [],

  // Initial query state
  runningQueries: [],
  queryHistory: [],

  // Initial database state
  databases: [],
  selectedDatabase: null,
  tables: [],
  selectedTable: null,
  tableParts: [],

  // Initial merge state
  activeMerges: [],

  // Initial trace state
  selectedQueryId: null,
  traceLogs: [],
  explainResult: null,

  // Initial UI state
  isPolling: false,
  pollInterval: 5000, // 5 seconds default
  error: null,

  // Actions
  setActiveConnection: (id) => set({ activeConnectionId: id }),
  setServerMetrics: (metrics) => set({ serverMetrics: metrics }),
  addMetricsHistory: (metrics) =>
    set((state) => ({
      metricsHistory: [...state.metricsHistory.slice(-99), metrics], // Keep last 100
    })),
  setRunningQueries: (queries) => set({ runningQueries: queries }),
  setQueryHistory: (history) => set({ queryHistory: history }),
  setDatabases: (databases) => set({ databases }),
  selectDatabase: (name) => set({ selectedDatabase: name, selectedTable: null, tableParts: [] }),
  setTables: (tables) => set({ tables }),
  selectTable: (name) => set({ selectedTable: name }),
  setTableParts: (parts) => set({ tableParts: parts }),
  setActiveMerges: (merges) => set({ activeMerges: merges }),
  setSelectedQueryId: (queryId) => set({ selectedQueryId: queryId }),
  setTraceLogs: (logs) => set({ traceLogs: logs }),
  setExplainResult: (result) => set({ explainResult: result }),
  setIsPolling: (isPolling) => set({ isPolling }),
  setPollInterval: (interval) => set({ pollInterval: interval }),
  setError: (error) => set({ error }),
  clearError: () => set({ error: null }),
}));

export default useMonitorStore;
