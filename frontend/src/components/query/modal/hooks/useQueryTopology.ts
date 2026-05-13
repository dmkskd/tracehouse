import { useEffect, useState } from 'react';
import type { QuerySeries, QueryDetail as QueryDetailType, SubQueryInfo } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';
import type { TopologyCoordinator } from '../shared/DistributedQueryTopology';

export function useQueryTopology(activeQuery: QuerySeries | null, queryDetail: QueryDetailType | null) {
  const services = useClickHouseServices();
  const [subQueries, setSubQueries] = useState<SubQueryInfo[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [coordinator, setCoordinator] = useState<TopologyCoordinator | null>(null);

  useEffect(() => {
    setSubQueries([]);
    setIsLoading(false);
    setCoordinator(null);
  }, [activeQuery?.query_id]);

  useEffect(() => {
    if (!queryDetail || !services || !activeQuery || subQueries.length > 0 || isLoading) return;

    if (queryDetail.is_initial_query === 1) {
      // Viewing the coordinator — fetch its sub-queries
      setIsLoading(true);
      setCoordinator({
        query_id: queryDetail.query_id,
        hostname: queryDetail.hostname,
        query_duration_ms: queryDetail.query_duration_ms,
        query_start_time_microseconds: queryDetail.query_start_time_microseconds,
        memory_usage: queryDetail.memory_usage,
        read_rows: queryDetail.read_rows,
        exception: queryDetail.exception,
      });
      services.queryAnalyzer.getSubQueries(activeQuery.query_id, activeQuery.start_time)
        .then(setSubQueries)
        .catch((err) => console.error('Failed to fetch sub-queries:', err))
        .finally(() => setIsLoading(false));
    } else if (queryDetail.is_initial_query === 0 && queryDetail.initial_query_id) {
      // Viewing a sub-query — fetch coordinator detail + all sibling sub-queries
      setIsLoading(true);
      Promise.all([
        services.queryAnalyzer.getQueryDetail(queryDetail.initial_query_id),
        services.queryAnalyzer.getSubQueries(queryDetail.initial_query_id, activeQuery.start_time),
      ])
        .then(([coordDetail, siblings]) => {
          if (coordDetail) {
            setCoordinator({
              query_id: coordDetail.query_id,
              hostname: coordDetail.hostname,
              query_duration_ms: coordDetail.query_duration_ms,
              query_start_time_microseconds: coordDetail.query_start_time_microseconds,
              memory_usage: coordDetail.memory_usage,
              read_rows: coordDetail.read_rows,
              exception: coordDetail.exception,
            });
          }
          setSubQueries(siblings);
        })
        .catch((err) => console.error('Failed to fetch topology:', err))
        .finally(() => setIsLoading(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryDetail, services]);

  return { subQueries, isLoading, coordinator };
}
