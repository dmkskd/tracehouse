import { useCallback, useEffect, useState } from 'react';
import type { QuerySeries, OpenTelemetrySpan } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';

export function useQuerySpans(activeQuery: QuerySeries | null, isActive: boolean) {
  const services = useClickHouseServices();
  const [spans, setSpans] = useState<OpenTelemetrySpan[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchSpans = useCallback(async () => {
    if (!services || !activeQuery) return;
    setIsLoading(true);
    setError(null);

    let completed = false;
    const timeoutId = setTimeout(() => {
      if (!completed) {
        completed = true;
        setSpans([]);
        setIsLoading(false);
      }
    }, 3000);

    try {
      const result = await services.traceService.getOpenTelemetrySpans(activeQuery.query_id);
      if (!completed) {
        completed = true;
        clearTimeout(timeoutId);
        setSpans(result);
        setIsLoading(false);
      }
    } catch (e) {
      if (!completed) {
        completed = true;
        clearTimeout(timeoutId);
        setError(e instanceof Error ? e.message : 'Failed to fetch spans');
        setIsLoading(false);
      }
    }
  }, [services, activeQuery]);

  useEffect(() => {
    setSpans([]);
    setError(null);
  }, [activeQuery?.query_id]);

  useEffect(() => {
    if (isActive && activeQuery && spans.length === 0 && !isLoading && !error) {
      fetchSpans();
    }
  }, [isActive, activeQuery, spans.length, isLoading, error, fetchSpans]);

  return { spans, isLoading, error, refresh: fetchSpans };
}
