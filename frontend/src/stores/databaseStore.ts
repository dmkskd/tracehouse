/**
 * Database Store - Zustand store for managing database explorer state
 * 
 * This store handles database structure exploration including databases,
 * tables, schemas, and parts information.
 * 
 */

import { create } from 'zustand';
import type {
  DatabaseInfo,
  TableInfo,
  ColumnSchema,
  PartInfo,
  PartDetailInfo,
  PartColumnInfo,
  PartLineage,
  LineageNode,
  MergeEvent,
  DatabaseExplorer,
} from '@tracehouse/core';

// Re-export core types for consumers that import from this store
export type {
  DatabaseInfo,
  TableInfo,
  ColumnSchema,
  PartInfo,
  PartDetailInfo,
  PartColumnInfo,
  PartLineage,
  LineageNode,
  MergeEvent,
};

// Sort options for parts table
export type PartSortField = 
  | 'name' 
  | 'partition_id' 
  | 'rows' 
  | 'bytes_on_disk' 
  | 'modification_time' 
  | 'level';

export type SortDirection = 'asc' | 'desc';

export interface PartSort {
  field: PartSortField;
  direction: SortDirection;
}

// Tree node state for expanded databases
export interface TreeState {
  expandedDatabases: Set<string>;
}

// Part data sample response (kept locally — not part of core domain)
export interface PartDataResponse {
  columns: string[];
  rows: (string | number | boolean | null)[][];
  total_rows_in_part: number;
  returned_rows: number;
}

// Legacy lineage types re-exported for backward compatibility
export type MergeEventInfo = MergeEvent;
export type PartLineageInfo = PartLineage;

// Database store state
interface DatabaseState {
  databases: DatabaseInfo[];
  
  tables: TableInfo[];
  
  // Selected database and table
  selectedDatabase: string | null;
  selectedTable: TableInfo | null;
  
  tableSchema: ColumnSchema[];
  
  tableParts: PartInfo[];
  
  // Parts sorting
  partSort: PartSort;
  
  // Tree state for expanded databases
  treeState: TreeState;
  
  // Loading states
  isLoadingDatabases: boolean;
  isLoadingTables: boolean;
  isLoadingSchema: boolean;
  isLoadingParts: boolean;
  
  // Error state
  error: string | null;
  
  // Actions
  setDatabases: (databases: DatabaseInfo[]) => void;
  setTables: (tables: TableInfo[]) => void;
  setSelectedDatabase: (database: string | null) => void;
  setSelectedTable: (table: TableInfo | null) => void;
  setTableSchema: (schema: ColumnSchema[]) => void;
  setTableParts: (parts: PartInfo[]) => void;
  setPartSort: (sort: PartSort) => void;
  toggleDatabaseExpanded: (database: string) => void;
  setIsLoadingDatabases: (loading: boolean) => void;
  setIsLoadingTables: (loading: boolean) => void;
  setIsLoadingSchema: (loading: boolean) => void;
  setIsLoadingParts: (loading: boolean) => void;
  setError: (error: string | null) => void;
  clearError: () => void;
  clearSelection: () => void;
  clearAll: () => void;
}

export const useDatabaseStore = create<DatabaseState>((set) => ({
  // Initial state
  databases: [],
  tables: [],
  selectedDatabase: null,
  selectedTable: null,
  tableSchema: [],
  tableParts: [],
  
  // Default sort: by modification time descending
  partSort: {
    field: 'modification_time',
    direction: 'desc',
  },
  
  // Tree state
  treeState: {
    expandedDatabases: new Set<string>(),
  },
  
  isLoadingDatabases: false,
  isLoadingTables: false,
  isLoadingSchema: false,
  isLoadingParts: false,
  error: null,

  // Actions
  setDatabases: (databases) => set({ databases }),
  
  setTables: (tables) => set({ tables }),
  
  setSelectedDatabase: (database) => set({ 
    selectedDatabase: database,
    // Clear table selection when database changes
    selectedTable: null,
    tableSchema: [],
    tableParts: [],
  }),
  
  setSelectedTable: (table) => set({ 
    selectedTable: table,
    // Clear schema and parts when table changes
    tableSchema: [],
    tableParts: [],
  }),
  
  setTableSchema: (schema) => set({ tableSchema: schema }),
  
  setTableParts: (parts) => set({ tableParts: parts }),
  
  setPartSort: (sort) => set({ partSort: sort }),
  
  toggleDatabaseExpanded: (database) => set((state) => {
    const newExpanded = new Set(state.treeState.expandedDatabases);
    if (newExpanded.has(database)) {
      newExpanded.delete(database);
    } else {
      newExpanded.add(database);
    }
    return {
      treeState: {
        ...state.treeState,
        expandedDatabases: newExpanded,
      },
    };
  }),
  
  setIsLoadingDatabases: (loading) => set({ isLoadingDatabases: loading }),
  
  setIsLoadingTables: (loading) => set({ isLoadingTables: loading }),
  
  setIsLoadingSchema: (loading) => set({ isLoadingSchema: loading }),
  
  setIsLoadingParts: (loading) => set({ isLoadingParts: loading }),
  
  setError: (error) => set({ error }),
  
  clearError: () => set({ error: null }),
  
  clearSelection: () => set({
    selectedDatabase: null,
    selectedTable: null,
    tables: [],
    tableSchema: [],
    tableParts: [],
  }),
  
  clearAll: () => set({
    databases: [],
    tables: [],
    selectedDatabase: null,
    selectedTable: null,
    tableSchema: [],
    tableParts: [],
    treeState: {
      expandedDatabases: new Set<string>(),
    },
    error: null,
  }),
}));

/**
 * API functions for database exploration.
 * 
 * Each function accepts a DatabaseExplorer service instance (from ClickHouseProvider)
 * instead of a connectionId string.
 */
export const databaseApi = {
  /**
   * Fetch all databases
   */
  async fetchDatabases(service: DatabaseExplorer): Promise<DatabaseInfo[]> {
    return service.listDatabases();
  },

  /**
   * Fetch tables for a database
   */
  async fetchTables(service: DatabaseExplorer, database: string): Promise<TableInfo[]> {
    return service.listTables(database);
  },

  /**
   * Fetch table schema
   */
  async fetchTableSchema(
    service: DatabaseExplorer,
    database: string, 
    table: string
  ): Promise<ColumnSchema[]> {
    return service.getTableSchema(database, table);
  },

  /**
   * Fetch table parts
   */
  async fetchTableParts(
    service: DatabaseExplorer,
    database: string, 
    table: string
  ): Promise<PartInfo[]> {
    return service.getTableParts(database, table);
  },

  /**
   * Fetch detailed part information including column sizes
   */
  async fetchPartDetail(
    service: DatabaseExplorer,
    database: string,
    table: string,
    partName: string
  ): Promise<PartDetailInfo | null> {
    return service.getPartDetail(database, table, partName);
  },

  /**
   * Fetch merge lineage for a specific part
   * Shows the complete merge history tree
   */
  async fetchPartLineage(
    service: DatabaseExplorer,
    database: string,
    table: string,
    partName: string
  ): Promise<PartLineage> {
    return service.getPartLineage(database, table, partName);
  },

  /**
   * Fetch sample data from a specific part
   */
  async fetchPartData(
    service: DatabaseExplorer,
    database: string,
    table: string,
    partName: string,
    limit = 100
  ) {
    return service.getPartData(database, table, partName, limit);
  },

  /**
   * Fetch min/max values for columns in a specific part
   */
  async fetchPartColumnMinMax(
    service: DatabaseExplorer,
    database: string,
    table: string,
    partName: string,
    columns: Array<{ column_name: string; type: string }>
  ) {
    return service.getPartColumnMinMax(database, table, partName, columns);
  },
};

// Re-export shared formatters for backward compatibility
export { formatBytes, formatNumber } from '../utils/formatters';

/**
 * Sort parts by the specified field and direction
 */
export function sortParts(parts: PartInfo[], sort: PartSort): PartInfo[] {
  return [...parts].sort((a, b) => {
    let aVal: number | string;
    let bVal: number | string;
    
    switch (sort.field) {
      case 'name':
        aVal = a.name;
        bVal = b.name;
        break;
      case 'partition_id':
        aVal = a.partition_id;
        bVal = b.partition_id;
        break;
      case 'rows':
        aVal = a.rows;
        bVal = b.rows;
        break;
      case 'bytes_on_disk':
        aVal = a.bytes_on_disk;
        bVal = b.bytes_on_disk;
        break;
      case 'modification_time':
        aVal = new Date(a.modification_time).getTime();
        bVal = new Date(b.modification_time).getTime();
        break;
      case 'level':
        aVal = a.level;
        bVal = b.level;
        break;
      default:
        return 0;
    }
    
    if (sort.direction === 'asc') {
      return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    } else {
      return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
    }
  });
}

export default useDatabaseStore;
