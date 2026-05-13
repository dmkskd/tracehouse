import { useCallback, useEffect, useState } from 'react';
import type { QuerySeries, QueryDetail as QueryDetailType } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export function useQueryDetail(activeQuery: QuerySeries | null) {
  const services = useClickHouseServices();
  const [queryDetail, setQueryDetail] = useState<QueryDetailType | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchQueryDetail = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoading(true);
    setError(null);
    try {
      const result = await services.queryAnalyzer.getQueryDetail(activeQuery.query_id, activeQuery.start_time);
      setQueryDetail(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch query detail');
    } finally {
      setIsLoading(false);
    }
  }, [services, activeQuery]);

  const fetchSettingsDefaults = useCallback(
    async (settingNames: string[]): Promise<Record<string, { default: string; description: string }>> => {
      if (!services || settingNames.length === 0) return {};
      try {
        const result = await services.queryAnalyzer.getSettingsDefaults(settingNames);
        const map: Record<string, { default: string; description: string }> = {};
        for (const s of result) {
          map[s.name] = { default: s.default, description: s.description };
        }
        return map;
      } catch {
        return {};
      }
    },
    [services]
  );

  useEffect(() => {
    setQueryDetail(null);
    setError(null);
  }, [activeQuery?.query_id]);

  // Always fetch query detail when active query is present — needed for title, gating, multiple tabs
  useEffect(() => {
    if (activeQuery && !queryDetail && !isLoading && !error) {
      fetchQueryDetail();
    }
  }, [activeQuery, queryDetail, isLoading, error, fetchQueryDetail]);

  return { queryDetail, isLoading, error, fetchSettingsDefaults, refresh: fetchQueryDetail };
}
