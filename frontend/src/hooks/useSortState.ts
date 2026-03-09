import { useState, useMemo, useCallback } from 'react';

export type SortDirection = 'asc' | 'desc';

export interface SortState<K extends string> {
  key: K;
  direction: SortDirection;
}

export function useSortState<K extends string>(defaultKey: K, defaultDirection: SortDirection = 'desc') {
  const [sort, setSort] = useState<SortState<K>>({ key: defaultKey, direction: defaultDirection });

  const toggleSort = useCallback((key: K) => {
    setSort(prev =>
      prev.key === key
        ? { key, direction: prev.direction === 'desc' ? 'asc' : 'desc' }
        : { key, direction: 'desc' }
    );
  }, []);

  return { sort, toggleSort };
}

export function useSortedData<T, K extends string>(
  data: T[],
  sort: SortState<K>,
  getValue: (item: T, key: K) => number | string,
): T[] {
  return useMemo(() => {
    const sorted = [...data].sort((a, b) => {
      const aVal = getValue(a, sort.key);
      const bVal = getValue(b, sort.key);
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sort.direction === 'desc' ? bVal - aVal : aVal - bVal;
      }
      const cmp = String(aVal).localeCompare(String(bVal));
      return sort.direction === 'desc' ? -cmp : cmp;
    });
    return sorted;
  }, [data, sort, getValue]);
}
