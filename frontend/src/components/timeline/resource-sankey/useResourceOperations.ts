import { useEffect, useState } from 'react';
import type { AttributionSelection } from '@tracehouse/core';
import type { MetricMode } from '../timeline-constants';
import { attributionConsumers, type ResourceConsumer, type ResourceDataLoader, type ResourceFlowGroup } from './resource-exploration-model';

/** Details are fetched only for an expanded branch; aggregate totals remain authoritative. */
export function useResourceOperations(loader: ResourceDataLoader | undefined, group: ResourceFlowGroup | undefined,
  mode: MetricMode, selection: AttributionSelection, hostname: string | readonly string[] | undefined, limit: number) {
  const [revision, setRevision] = useState(0);
  const [result, setResult] = useState<{ key: string; rows: ResourceConsumer[]; error?: string }>();
  const key = JSON.stringify([mode, selection, hostname, group?.id, limit, revision]);
  useEffect(() => {
    if (!group?.aggregates || !loader?.loadDetails) return;
    const aggregates = group.aggregates;
    let cancelled = false;
    loader.loadDetails({ mode, selection, hostname, groups: aggregates, limit: Number.isFinite(limit) ? limit : 10000 })
      .then(details => {
        if (cancelled) return;
        const rows = attributionConsumers({ operations: details.operations, mode }).consumers;
        // A live execution may have changed since the aggregate request. Never rescale its cost silently.
        if (rows.reduce((sum,row) => sum+row.value,0) > group.value * 1.000001) throw new Error('Operation values changed since this snapshot. Reload the resource view.');
        if (!Number.isFinite(limit) && rows.length < aggregates.reduce((sum,row) => sum+row.operationCount-row.zeroCount-row.unavailableCount,0))
          throw new Error('This branch exceeds the 10,000-operation detail limit. Select a smaller time range.');
        setResult({ key, rows, error: details.warnings.join(' ') || undefined });
      }).catch(error => { if (!cancelled) setResult({ key, rows: [], error: error instanceof Error ? error.message : String(error) }); });
    return () => { cancelled = true; };
    // The serialized key captures request values without retriggering for render-created objects.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, loader]);
  const active = !!group?.aggregates && !!loader?.loadDetails;
  return { rows: active && result?.key === key ? result.rows : [], loading: active && result?.key !== key,
    error: active && result?.key === key ? result.error : undefined, retry: () => setRevision(value => value+1) };
}
