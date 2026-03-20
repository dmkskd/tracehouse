/**
 * useMergeSamples — fetches per-second merge samples from tracehouse.merges_history
 * with server-side delta computation.
 *
 * Similar to useProcessSamples but for merge operations.
 */

import { useState, useCallback } from 'react';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import {
  buildMergeSamplesSQL,
  mapMergeSampleRow,
  type MergeSample,
} from '@tracehouse/core';

export type { MergeSample };

export interface MergeSamplesResult {
  samples: MergeSample[];
  isLoading: boolean;
  error: string | null;
  fetch: () => Promise<void>;
}

export function useMergeSamples(opts: {
  database: string | undefined;
  table: string | undefined;
  resultPartName: string | undefined;
  hostname?: string;
}): MergeSamplesResult {
  const services = useClickHouseServices();
  const [samples, setSamples] = useState<MergeSample[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetch = useCallback(async () => {
    if (!services || !opts.database || !opts.table) return;
    setIsLoading(true);
    setError(null);
    try {
      const sql = buildMergeSamplesSQL({
        database: opts.database,
        table: opts.table,
        resultPartName: opts.resultPartName,
        hostname: opts.hostname,
      });
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(sql);
      setSamples(rows.map(mapMergeSampleRow));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch merge samples');
    } finally {
      setIsLoading(false);
    }
  }, [services, opts.database, opts.table, opts.resultPartName, opts.hostname]);

  return { samples, isLoading, error, fetch };
}
