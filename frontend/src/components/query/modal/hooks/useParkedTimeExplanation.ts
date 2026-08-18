/**
 * Explains a query's parked thread time, as deeply as the deployment allows.
 *
 * Layer 1 (the time breakdown itself) can only say threads were parked. This
 * hook fetches the two layers that say on what:
 *
 *   Layer 2  processors_profile_log  which pipeline stage stalled, and on which
 *                                    side. No special privilege.
 *   Layer 3  trace_log Real traces   the call the thread was actually blocked
 *                                    in. Needs allow_introspection_functions,
 *                                    commonly denied on hosted offerings.
 *
 * They are not two resolutions of one answer — layer 2 locates the choke point
 * inside the plan, layer 3 names the cause outside it ("back-pressured" vs
 * "because the client is not consuming"). Both are fetched when available and
 * neither is required.
 *
 * Fetched lazily: this is only needed once a user looks at the breakdown, and
 * both queries scan log tables.
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  summarizePipelineStall,
  summarizeBlockedStacks,
  type QuerySeries,
  type PipelineStall,
  type BlockedCategorySummary,
  type BlockedStackRow,
} from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';
import { useCapabilityCheck } from '../../../shared/RequiresCapability';

export interface ParkedTimeExplanation {
  /** Layer 2 — undefined when unavailable or nothing stalled. */
  stall: PipelineStall | undefined;
  /** Layer 3 — empty when unavailable. */
  blocked: BlockedCategorySummary[];
  /** True while either layer is in flight. */
  isLoading: boolean;
  /** Which layers this deployment can offer, for honest empty states. */
  layers: { pipeline: boolean; stacks: boolean };
  /** True when the server refused the introspection functions layer 3 needs. */
  introspectionDenied: boolean;
}

export function useParkedTimeExplanation(
  activeQuery: QuerySeries | null,
  enabled: boolean,
): ParkedTimeExplanation {
  const services = useClickHouseServices();
  const { available: hasProcessors } = useCapabilityCheck(['processors_profile_log']);
  // Both, not just trace_log: the layer needs symbolization, and
  // introspection_functions is the probe Engine Internals already gates its CPU
  // sampling on. Checking only the table produced a panel that advertised the
  // layer and then reported it denied.
  const { available: hasTraceLog } = useCapabilityCheck(['trace_log', 'introspection_functions']);

  const [stallRows, setStallRows] = useState<Awaited<ReturnType<NonNullable<typeof services>['queryAnalyzer']['getPipelineStall']>>>([]);
  const [blockedRows, setBlockedRows] = useState<BlockedStackRow[]>([]);
  const [introspectionDenied, setIntrospectionDenied] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const queryId = activeQuery?.query_id;
  const startTime = activeQuery?.start_time;

  const fetch = useCallback(async () => {
    if (!services || !queryId) return;
    setIsLoading(true);
    try {
      // Both layers are optional and independent, so a denied privilege on one
      // must not suppress the other. The service methods already swallow
      // errors into empty results.
      const [stall, blocked] = await Promise.all([
        hasProcessors ? services.queryAnalyzer.getPipelineStall(queryId, startTime) : Promise.resolve([]),
        hasTraceLog
          ? services.queryAnalyzer.getBlockedStacks(queryId, startTime)
          : Promise.resolve({ rows: [], denied: false }),
      ]);
      setStallRows(stall);
      setBlockedRows(blocked.rows);
      setIntrospectionDenied(blocked.denied);
    } finally {
      setIsLoading(false);
    }
  }, [services, queryId, startTime, hasProcessors, hasTraceLog]);

  useEffect(() => {
    setStallRows([]);
    setBlockedRows([]);
    setIntrospectionDenied(false);
  }, [queryId]);

  useEffect(() => {
    if (enabled && queryId && (hasProcessors || hasTraceLog)) fetch();
  }, [enabled, queryId, hasProcessors, hasTraceLog, fetch]);

  const stall = useMemo(() => summarizePipelineStall(stallRows), [stallRows]);
  const blocked = useMemo(() => summarizeBlockedStacks(blockedRows), [blockedRows]);

  return {
    stall,
    blocked,
    isLoading,
    layers: { pipeline: hasProcessors, stacks: hasTraceLog },
    introspectionDenied,
  };
}
