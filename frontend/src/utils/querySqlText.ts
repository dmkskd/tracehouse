import type { QueryDetail, QuerySeries } from '@tracehouse/core';

export type SqlDisplayMode = 'formatted' | 'raw';

export function querySqlText(
  q: Pick<QuerySeries, 'label'>,
  queryDetail: Pick<QueryDetail, 'formatted_query' | 'query'> | null,
  mode: SqlDisplayMode,
  fallback = '-- no query text available',
): string {
  if (mode === 'formatted') {
    return queryDetail?.formatted_query || queryDetail?.query || q.label || fallback;
  }
  return queryDetail?.query || queryDetail?.formatted_query || q.label || fallback;
}

export function querySqlLineCount(sql: string): number {
  return sql.split(/\r?\n/).filter(line => line.trim()).length;
}
