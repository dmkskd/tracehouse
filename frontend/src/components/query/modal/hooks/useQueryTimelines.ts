import { useEffect, useState } from 'react';
import type { QuerySeries, SimilarQuery } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export function useQueryTimelines(query: QuerySeries | null, similarQueries: SimilarQuery[]) {
  const services = useClickHouseServices();
  const [cpuTimeline, setCpuTimeline] = useState<{ t: string; cpu_pct: number }[]>([]);
  const [isLoadingCpu, setIsLoadingCpu] = useState(false);
  const [memTimeline, setMemTimeline] = useState<{ t: string; mem_pct: number }[]>([]);
  const [isLoadingMem, setIsLoadingMem] = useState(false);

  useEffect(() => {
    setCpuTimeline([]);
    setMemTimeline([]);
    setIsLoadingCpu(false);
    setIsLoadingMem(false);
  }, [query?.query_id]);

  useEffect(() => {
    if (similarQueries.length >= 2 && cpuTimeline.length === 0 && !isLoadingCpu && services) {
      const startTime = similarQueries[0].query_start_time;
      const endTime = similarQueries[similarQueries.length - 1].query_start_time;
      setIsLoadingCpu(true);
      services.queryAnalyzer.getServerCpuForRange(startTime, endTime)
        .then(setCpuTimeline)
        .catch((err) => console.error('Failed to fetch CPU timeline:', err))
        .finally(() => setIsLoadingCpu(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [similarQueries, services]);

  useEffect(() => {
    if (similarQueries.length >= 2 && memTimeline.length === 0 && !isLoadingMem && services) {
      const startTime = similarQueries[0].query_start_time;
      const endTime = similarQueries[similarQueries.length - 1].query_start_time;
      setIsLoadingMem(true);
      services.queryAnalyzer.getServerMemoryForRange(startTime, endTime)
        .then(setMemTimeline)
        .catch((err) => console.error('Failed to fetch memory timeline:', err))
        .finally(() => setIsLoadingMem(false));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [similarQueries, services]);

  return { cpuTimeline, isLoadingCpu, memTimeline, isLoadingMem };
}
