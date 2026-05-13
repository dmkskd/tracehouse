import { useCallback, useEffect, useState } from 'react';
import type { QuerySeries, FlamegraphType } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export function useQueryFlamegraph(activeQuery: QuerySeries | null, isActive: boolean) {
  const services = useClickHouseServices();
  const [folded, setFolded] = useState('');
  const [unavailable, setUnavailable] = useState<string | undefined>();
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [type, setType] = useState<FlamegraphType>('CPU');
  const [fetched, setFetched] = useState(false);

  const fetchFlamegraph = useCallback(async (t: FlamegraphType = type) => {
    if (!services || !activeQuery) return;
    setIsLoading(true);
    setError(null);
    setUnavailable(undefined);
    try {
      const result = await services.traceService.getFlamegraphFolded(activeQuery.query_id, t, activeQuery.start_time);
      setFolded(result.folded);
      setUnavailable(result.unavailableReason);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch flamegraph data');
    } finally {
      setIsLoading(false);
      setFetched(true);
    }
  }, [services, activeQuery, type]);

  useEffect(() => {
    setFolded('');
    setUnavailable(undefined);
    setError(null);
    setType('CPU');
    setFetched(false);
  }, [activeQuery?.query_id]);

  useEffect(() => {
    if (isActive && activeQuery && !folded && !isLoading && !error && !unavailable && !fetched) {
      fetchFlamegraph();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- fetchFlamegraph depends on type which would cause re-fires
  }, [isActive, activeQuery, fetched]);

  const handleTypeChange = useCallback((newType: FlamegraphType) => {
    setType(newType);
    setFolded('');
    setUnavailable(undefined);
    setError(null);
    fetchFlamegraph(newType);
  }, [fetchFlamegraph]);

  return { folded, unavailable, isLoading, error, type, onTypeChange: handleTypeChange, refresh: fetchFlamegraph };
}
