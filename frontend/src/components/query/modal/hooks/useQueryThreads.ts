import { useCallback, useEffect, useState } from 'react';
import type { QuerySeries, QueryThreadBreakdown } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export function useQueryThreads(activeQuery: QuerySeries | null, isActive: boolean) {
  const services = useClickHouseServices();
  const [threads, setThreads] = useState<QueryThreadBreakdown[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fetched, setFetched] = useState(false);

  const fetchThreads = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoading(true);
    setError(null);
    try {
      const result = await services.queryAnalyzer.getQueryThreadBreakdown(activeQuery.query_id, activeQuery.start_time);
      setThreads(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch thread breakdown');
    } finally {
      setIsLoading(false);
      setFetched(true);
    }
  }, [services, activeQuery]);

  useEffect(() => {
    setThreads([]);
    setError(null);
    setFetched(false);
  }, [activeQuery?.query_id]);

  useEffect(() => {
    if (isActive && activeQuery && threads.length === 0 && !isLoading && !error && !fetched) {
      fetchThreads();
    }
  }, [isActive, activeQuery, threads.length, isLoading, error, fetched, fetchThreads]);

  return { threads, isLoading, error, fetched, refresh: fetchThreads };
}
