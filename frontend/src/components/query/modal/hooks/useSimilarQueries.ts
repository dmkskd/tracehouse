import { useCallback, useEffect, useState } from 'react';
import type { QuerySeries, QueryDetail as QueryDetailType, SimilarQuery } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export type HistoryHashMode = 'normalized' | 'exact';

export function useSimilarQueries(
  query: QuerySeries | null,
  activeQuery: QuerySeries | null,
  queryDetail: QueryDetailType | null,
  isActive: boolean,
) {
  const services = useClickHouseServices();
  const [similarQueries, setSimilarQueries] = useState<SimilarQuery[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasFetched, setHasFetched] = useState(false);
  const [limit, setLimit] = useState(50);
  const [hashMode, setHashMode] = useState<HistoryHashMode>('normalized');

  const fetchSimilarQueries = useCallback(
    async (overrideLimit?: number, overrideMode?: HistoryHashMode) => {
      if (!services || !activeQuery || !queryDetail) return;
      const effectiveLimit = overrideLimit ?? limit;
      const effectiveMode = overrideMode ?? hashMode;
      const hash = effectiveMode === 'exact' ? queryDetail.query_hash : queryDetail.normalized_query_hash;
      if (!hash) return;
      setIsLoading(true);
      setHasFetched(true);
      setError(null);
      try {
        const result = await services.queryAnalyzer.getSimilarQueries(String(hash), effectiveLimit, effectiveMode);
        setSimilarQueries(result);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to fetch similar queries');
      } finally {
        setIsLoading(false);
      }
    },
    [services, activeQuery, queryDetail, limit, hashMode]
  );

  // Cross-execution: reset only when the *root* query changes, not on navigation within history
  useEffect(() => {
    setSimilarQueries([]);
    setError(null);
    setHasFetched(false);
  }, [query?.query_id]);

  // Auto-fetch when History tab is active and we have a hash
  useEffect(() => {
    if (isActive && queryDetail?.normalized_query_hash && !hasFetched && !isLoading) {
      fetchSimilarQueries();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isActive, queryDetail?.normalized_query_hash, hasFetched, isLoading]);

  const changeLimit = useCallback(
    (newLimit: number) => {
      setLimit(newLimit);
      setHasFetched(false);
      setSimilarQueries([]);
      fetchSimilarQueries(newLimit);
    },
    [fetchSimilarQueries]
  );

  const changeHashMode = useCallback(
    (mode: HistoryHashMode) => {
      setHashMode(mode);
      setHasFetched(false);
      setSimilarQueries([]);
      fetchSimilarQueries(undefined, mode);
    },
    [fetchSimilarQueries]
  );

  return {
    similarQueries,
    isLoading,
    error,
    limit,
    hashMode,
    changeLimit,
    changeHashMode,
    refresh: fetchSimilarQueries,
  };
}
