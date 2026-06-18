import type { QueryHistoryItem } from '../stores/queryStore';

export const hoverTopologyRootId = (query: Pick<QueryHistoryItem, 'query_id' | 'is_initial_query' | 'initial_query_id'>): string =>
  query.is_initial_query === 0 && query.initial_query_id ? query.initial_query_id : query.query_id;

export const hoverTopologyRootIds = (
  queries: Pick<QueryHistoryItem, 'query_id' | 'is_initial_query' | 'initial_query_id'>[],
): string[] => [...new Set(queries.map(hoverTopologyRootId).filter(Boolean))];
