/**
 * useProcessSamples — fetches per-second process samples from tracehouse.processes_history
 * with server-side delta computation (no ProfileEvents maps shipped to browser).
 *
 * Exposes both cumulative and per-interval delta values so consumers can choose
 * without re-summing.
 */

import { useState, useCallback } from 'react';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import {
  PROCESS_SAMPLES_SQL,
  mapProcessSampleRow,
  type ProcessSample,
} from '@tracehouse/core';

export type { ProcessSample };

export interface ProcessSamplesResult {
  samples: ProcessSample[];
  isLoading: boolean;
  error: string | null;
  fetch: () => Promise<void>;
}

export function useProcessSamples(queryId: string | undefined): ProcessSamplesResult {
  const services = useClickHouseServices();
  const [samples, setSamples] = useState<ProcessSample[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async () => {
    if (!services || !queryId) return;
    setIsLoading(true);
    setError(null);
    try {
      const sql = PROCESS_SAMPLES_SQL.replace(/\{qid:String\}/g, `'${queryId.replace(/'/g, "''")}'`);
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(sql);
      setSamples(rows.map(mapProcessSampleRow));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch process samples');
    } finally {
      setIsLoading(false);
    }
  }, [services, queryId]);

  return { samples, isLoading, error, fetch };
}
