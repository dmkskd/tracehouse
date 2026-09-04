import { useCallback, useEffect, useRef, useState } from 'react';
import type { QueryAnalyzer, SubQueryInfo } from '@tracehouse/core';
import type { QueryHistoryItem } from '../../../stores/queryStore';
import { hoverTopologyRootId, hoverTopologyRootIds } from '../../../utils/queryHoverTopology';

interface UseQueryHoverTopologyArgs {
  enabled: boolean;
  queryAnalyzer?: QueryAnalyzer;
  history: QueryHistoryItem[];
  coordinatorIds?: Set<string>;
  startTime?: string;
}

interface UseQueryHoverTopologyResult {
  isLoading: boolean;
  error: Error | null;
  getChildQueriesForQuery: (query: QueryHistoryItem | null | undefined) => SubQueryInfo[] | undefined;
}

/**
 * Delay before re-reading the children of a query that just showed up.
 *
 * Each node flushes system.query_log on its own schedule, so the children of one
 * distributed query become visible in stages — measured locally as 0, then 3,
 * then 4 of 4 over about ten seconds. A fetch landing inside that window sees a
 * partial fan-out and reports, say, "1 child query · 1 node" for a query that
 * ran on two nodes. The effect below only re-runs when the set of visible rows
 * changes, so without this second pass the partial answer would stay cached for
 * as long as the row is on screen, disagreeing with the query detail modal,
 * which fetches later and sees everything.
 */
const SETTLE_REFETCH_MS = 12_000;

const isParallelTopologyCandidate = (
  query: Pick<QueryHistoryItem, 'query_id' | 'is_initial_query'>,
  coordinatorIds?: Set<string>,
): boolean => query.is_initial_query === 0 || Boolean(coordinatorIds?.has(query.query_id));

export const useQueryHoverTopology = ({
  enabled,
  queryAnalyzer,
  history,
  coordinatorIds,
  startTime,
}: UseQueryHoverTopologyArgs): UseQueryHoverTopologyResult => {
  const [childQueriesByRoot, setChildQueriesByRoot] = useState<Map<string, SubQueryInfo[]>>(new Map());
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const requestSeq = useRef(0);

  const candidates = history.filter(query => isParallelTopologyCandidate(query, coordinatorIds));
  const rootIdsKey = JSON.stringify(hoverTopologyRootIds(candidates));

  /** Roots already fetched at least once, so a re-fetch targets only new ones. */
  const seenRootIds = useRef(new Set<string>());

  useEffect(() => {
    requestSeq.current += 1;
    const seq = requestSeq.current;
    const rootIds = JSON.parse(rootIdsKey) as string[];
    let settleTimer: ReturnType<typeof setTimeout> | undefined;

    if (!enabled || !queryAnalyzer || rootIds.length === 0) {
      setChildQueriesByRoot(new Map());
      setIsLoading(false);
      setError(null);
      return;
    }

    // Roots seen for the first time are the ones that may still be flushing:
    // a query already fetched on an earlier pass has had time to settle. This
    // deliberately avoids comparing timestamps to the clock, because
    // query_start_time is normalised upstream and can carry a zone shift.
    const freshRootIds = rootIds.filter(id => !seenRootIds.current.has(id));
    // Track only what is on screen. Keeping every id ever seen would grow without
    // bound in a long-lived tab, and a root that scrolled away and came back has
    // earned a fresh read anyway.
    seenRootIds.current = new Set(rootIds);

    // One follow-up pass over just those, merged over the first answer.
    const scheduleSettleRefetch = () => {
      const ids = freshRootIds;
      if (ids.length === 0) return;
      settleTimer = setTimeout(() => {
        queryAnalyzer
          .getSubQueriesForInitialQueries(ids, startTime)
          .then((late) => {
            if (seq !== requestSeq.current) return;
            setChildQueriesByRoot((current) => {
              const merged = new Map(current);
              for (const [rootId, rows] of late) merged.set(rootId, rows);
              return merged;
            });
          })
          .catch((err: unknown) => {
            // The first answer stands; a failed refresh must not blank the card.
            console.error('[useQueryHoverTopology] Failed to refresh settling child rows', err);
          });
      }, SETTLE_REFETCH_MS);
    };

    setIsLoading(true);
    setChildQueriesByRoot(new Map());
    setError(null);
    queryAnalyzer.getSubQueriesForInitialQueries(rootIds, startTime)
      .then((result) => {
        if (seq !== requestSeq.current) return;
        console.debug('[useQueryHoverTopology] child query rows loaded', {
          rootIds,
          counts: Object.fromEntries([...result.entries()].map(([rootId, rows]) => [rootId, rows.length])),
        });
        setChildQueriesByRoot(result);
        scheduleSettleRefetch();
      })
      .catch((err: unknown) => {
        if (seq !== requestSeq.current) return;
        const error = err instanceof Error ? err : new Error(String(err));
        console.error('[useQueryHoverTopology] Failed to fetch child query rows', error);
        setError(error);
        setChildQueriesByRoot(new Map());
      })
      .finally(() => {
        if (seq !== requestSeq.current) return;
        setIsLoading(false);
      });

    return () => {
      if (settleTimer) clearTimeout(settleTimer);
    };
  }, [enabled, queryAnalyzer, rootIdsKey, startTime]);

  const getChildQueriesForQuery = useCallback((query: QueryHistoryItem | null | undefined): SubQueryInfo[] | undefined => {
    if (!query) return undefined;
    return childQueriesByRoot.get(hoverTopologyRootId(query));
  }, [childQueriesByRoot]);

  return { isLoading, error, getChildQueriesForQuery };
};
