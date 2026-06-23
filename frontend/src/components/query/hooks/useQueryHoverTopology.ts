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

  const rootIdsKey = JSON.stringify(
    hoverTopologyRootIds(history.filter(query => isParallelTopologyCandidate(query, coordinatorIds))),
  );

  useEffect(() => {
    requestSeq.current += 1;
    const seq = requestSeq.current;
    const rootIds = JSON.parse(rootIdsKey) as string[];

    if (!enabled || !queryAnalyzer || rootIds.length === 0) {
      setChildQueriesByRoot(new Map());
      setIsLoading(false);
      setError(null);
      return;
    }

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
  }, [enabled, queryAnalyzer, rootIdsKey, startTime]);

  const getChildQueriesForQuery = useCallback((query: QueryHistoryItem | null | undefined): SubQueryInfo[] | undefined => {
    if (!query) return undefined;
    return childQueriesByRoot.get(hoverTopologyRootId(query));
  }, [childQueriesByRoot]);

  return { isLoading, error, getChildQueriesForQuery };
};
