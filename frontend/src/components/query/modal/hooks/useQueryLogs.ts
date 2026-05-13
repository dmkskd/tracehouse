import { useCallback, useEffect, useState } from 'react';
import type { QuerySeries, TraceLog } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export function useQueryLogs(activeQuery: QuerySeries | null, isActive: boolean) {
  const services = useClickHouseServices();
  const [logs, setLogs] = useState<TraceLog[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<{ logLevels?: string[] }>({});

  const fetchLogs = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoading(true);
    setError(null);
    try {
      const result = await services.traceService.getQueryLogs(activeQuery.query_id, undefined, activeQuery.start_time);
      setLogs(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch logs');
    } finally {
      setIsLoading(false);
    }
  }, [services, activeQuery]);

  useEffect(() => {
    setLogs([]);
    setError(null);
    setFilter({});
  }, [activeQuery?.query_id]);

  useEffect(() => {
    if (isActive && activeQuery && logs.length === 0 && !isLoading && !error) {
      fetchLogs();
    }
  }, [isActive, activeQuery, logs.length, isLoading, error, fetchLogs]);

  return { logs, isLoading, error, filter, setFilter, refresh: fetchLogs };
}
