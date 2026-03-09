/**
 * ClickStack Store - Persists ClickStack integration settings.
 *
 * ClickStack (HyperDX) is embedded in ClickHouse 26.2+ at /clickstack/.
 * Users must configure a data source in ClickStack pointing to system.text_log
 * before deep links will work.
 *
 * ClickStack stores sources in the browser's localStorage (on the ClickHouse
 * origin) under the key `hdx-local-source` as a JSON array. The source ID
 * (e.g. "l1933286829") appears in the URL as `source=l1933286829`.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface ClickStackState {
  /** Whether the ClickStack integration is enabled */
  enabled: boolean;
  /**
   * The ClickStack data source ID (e.g. "l1933286829") from the URL.
   * Found in ClickStack's localStorage key `hdx-last-selected-source-id`
   * or in the `source=` URL parameter.
   */
  sourceId: string;
  setEnabled: (enabled: boolean) => void;
  setSourceId: (sourceId: string) => void;
}

export const useClickStackStore = create<ClickStackState>()(
  persist(
    (set) => ({
      enabled: false,
      sourceId: '',
      setEnabled: (enabled) => set({ enabled }),
      setSourceId: (sourceId) => set({ sourceId }),
    }),
    { name: 'tracehouse-clickstack' }
  )
);

/**
 * Build a ClickStack deep link URL for a query's trace logs.
 *
 * @param clickHouseHost - The ClickHouse HTTP base URL (e.g. "http://localhost:8123")
 * @param queryId - The query_id to filter on
 * @param sourceId - The ClickStack data source ID (e.g. "l1933286829")
 * @param startTime - Query start time (ISO string or Date)
 * @param endTime - Query end time (ISO string or Date)
 */
export function buildClickStackUrl(
  clickHouseHost: string,
  queryId: string,
  sourceId: string,
  startTime: string | Date,
  endTime: string | Date,
): string {
  // Normalize base URL — strip trailing slash
  const base = clickHouseHost.replace(/\/+$/, '');

  // Build time window: 1 minute before start, 1 minute after end
  const from = new Date(startTime).getTime() - 60_000;
  const to = new Date(endTime).getTime() + 60_000;

  const params = new URLSearchParams({
    select: '*',
    where: `query_id = '${queryId}'`,
    whereLanguage: 'sql',
    source: sourceId,
    filters: '[]',
    orderBy: 'event_time_microseconds ASC',
    from: String(from),
    to: String(to),
    isLive: 'false',
  });

  return `${base}/clickstack/search?${params.toString()}`;
}

/**
 * Build a ClickStack URL without a source — opens the search page
 * and lets ClickStack use whatever source is currently selected.
 * Useful as a fallback when the user hasn't configured a source ID.
 */
export function buildClickStackUrlNoSource(
  clickHouseHost: string,
  queryId: string,
  startTime: string | Date,
  endTime: string | Date,
): string {
  const base = clickHouseHost.replace(/\/+$/, '');
  const from = new Date(startTime).getTime() - 60_000;
  const to = new Date(endTime).getTime() + 60_000;

  const params = new URLSearchParams({
    select: '*',
    where: `query_id = '${queryId}'`,
    whereLanguage: 'sql',
    filters: '[]',
    orderBy: 'event_time_microseconds ASC',
    from: String(from),
    to: String(to),
    isLive: 'false',
  });

  return `${base}/clickstack/search?${params.toString()}`;
}
