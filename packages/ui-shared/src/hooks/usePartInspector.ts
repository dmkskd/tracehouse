/**
 * Headless hook for part inspector functionality.
 * Manages tab state, fetches part detail and column data,
 * and provides column sorting.
 */
import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { IClickHouseAdapter, PartDetailInfo, PartColumnInfo } from '@tracehouse/core';
import { DatabaseExplorer } from '@tracehouse/core';

export type InspectorTab = 'overview' | 'columns' | 'lineage';
export type SortField = 'column_name' | 'type' | 'compressed_bytes' | 'uncompressed_bytes' | 'compression_ratio';
export type SortDir = 'asc' | 'desc';

export interface UsePartInspectorResult {
  activeTab: InspectorTab;
  setActiveTab: (tab: InspectorTab) => void;
  partDetail: PartDetailInfo | null;
  isLoading: boolean;
  error: string | null;
  columnSort: { field: SortField; dir: SortDir };
  setColumnSort: (field: SortField) => void;
  sortedColumns: PartColumnInfo[];
}

export function usePartInspector(
  adapter: IClickHouseAdapter,
  database: string,
  table: string,
  partName: string | null
): UsePartInspectorResult {
  const [activeTab, setActiveTab] = useState<InspectorTab>('overview');
  const [partDetail, setPartDetail] = useState<PartDetailInfo | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [columnSort, setColumnSortState] = useState<{ field: SortField; dir: SortDir }>({
    field: 'compressed_bytes',
    dir: 'desc',
  });

  const cancelledRef = useRef(false);

  const setColumnSort = useCallback((field: SortField) => {
    setColumnSortState(prev => ({
      field,
      dir: prev.field === field && prev.dir === 'desc' ? 'asc' : 'desc',
    }));
  }, []);

  useEffect(() => {
    if (!partName || !database || !table) {
      setPartDetail(null);
      setError(null);
      return;
    }

    cancelledRef.current = false;
    setIsLoading(true);
    setError(null);

    const explorer = new DatabaseExplorer(adapter);
    explorer
      .getPartDetail(database, table, partName)
      .then(detail => {
        if (!cancelledRef.current) {
          setPartDetail(detail);
          setIsLoading(false);
        }
      })
      .catch(err => {
        if (!cancelledRef.current) {
          setError(err instanceof Error ? err.message : String(err));
          setIsLoading(false);
        }
      });

    return () => {
      cancelledRef.current = true;
    };
  }, [adapter, database, table, partName]);

  const sortedColumns = useMemo(() => {
    if (!partDetail?.columns) return [];

    const cols = [...partDetail.columns];
    const { field, dir } = columnSort;

    cols.sort((a, b) => {
      let cmp: number;
      if (field === 'column_name' || field === 'type') {
        cmp = (a[field] as string).localeCompare(b[field] as string);
      } else {
        cmp = (a[field] as number) - (b[field] as number);
      }
      return dir === 'asc' ? cmp : -cmp;
    });

    return cols;
  }, [partDetail?.columns, columnSort]);

  return {
    activeTab,
    setActiveTab,
    partDetail,
    isLoading,
    error,
    columnSort,
    setColumnSort,
    sortedColumns,
  };
}
