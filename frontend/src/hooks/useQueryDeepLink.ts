/**
 * useQueryDeepLink — syncs query detail modal state to URL (qd_id param).
 *
 * Each page that renders <QueryDetailModal> calls this hook. It:
 *   - Reads qd_id from URL on mount (raw hash read to dodge React Router timing race)
 *   - If qd_id is present and no query is selected, fetches the query detail
 *   - When a query is selected by the user, writes qd_id to URL
 *   - On close, clears qd_id from URL
 *
 * Preserves all other search params so it coexists safely with
 * useAnalyticsUrlState, useUrlState, etc.
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import type { QuerySeries } from '@tracehouse/core';
import { useUrlState } from './useUrlState';
import { defineShareSchema } from '../share/shareSchema';

/** State listed here must survive copying a URL with Query Details open. */
export const QUERY_DETAILS_SHARE_SCHEMA = defineShareSchema({
  qd_id: { type: 'string' },
  qd_tab: { type: 'string' },
});

function detailToSeries(detail: any): QuerySeries {
  const durationMs = Number(detail.query_duration_ms) || 0;
  const startMs = new Date(detail.query_start_time).getTime();
  return {
    query_id: detail.query_id,
    label: detail.query || '',
    user: detail.user,
    peak_memory: Number(detail.memory_usage) || 0,
    duration_ms: durationMs,
    cpu_us: (detail.ProfileEvents?.['UserTimeMicroseconds'] || 0) + (detail.ProfileEvents?.['SystemTimeMicroseconds'] || 0),
    net_send: detail.ProfileEvents?.['NetworkSendBytes'] || 0,
    net_recv: detail.ProfileEvents?.['NetworkReceiveBytes'] || 0,
    disk_read: Number(detail.read_bytes) || 0,
    disk_write: detail.ProfileEvents?.['OSWriteBytes'] || 0,
    start_time: detail.query_start_time,
    end_time: new Date(startMs + durationMs).toISOString(),
    exception_code: detail.exception_code,
    exception: detail.exception,
    points: [],
  };
}

/**
 * @param query  The query selected by user interaction (clicking a row), or null.
 * @param onClose  Callback to clear the parent's selection state.
 * @returns { query, onClose } — pass these to <QueryDetailModal>.
 */
export function useQueryDeepLink(
  query: QuerySeries | null,
  onClose: () => void,
): { query: QuerySeries | null; onClose: () => void } {
  const services = useClickHouseServices();
  const { state: sharedState, update: updateSharedState } = useUrlState(QUERY_DETAILS_SHARE_SCHEMA);
  const [deepLinkedQuery, setDeepLinkedQuery] = useState<QuerySeries | null>(null);

  const [pendingQdId, setPendingQdId] = useState<string | null>(sharedState.qd_id ?? null);
  const fetchedRef = useRef('');

  // The URL adapter emits standalone hash changes and Grafana location changes.
  useEffect(() => {
    const qdId = sharedState.qd_id;
    if (qdId && qdId !== fetchedRef.current) setPendingQdId(qdId);
    if (!qdId) {
      setPendingQdId(null);
      setDeepLinkedQuery(null);
    }
  }, [sharedState.qd_id]);

  // When the parent selects a query, write qd_id to URL
  useEffect(() => {
    if (!query) return;
    setDeepLinkedQuery(null);
    setPendingQdId(null);
    fetchedRef.current = query.query_id;
    updateSharedState({ qd_id: query.query_id });
  }, [query?.query_id]);

  // When we have a pending qd_id and services are ready, fetch
  useEffect(() => {
    if (query || !pendingQdId || !services) return;
    if (fetchedRef.current === pendingQdId) return;
    const qdId = pendingQdId;
    fetchedRef.current = qdId;
    setPendingQdId(null);

    services.queryAnalyzer.getQueryDetail(qdId).then(detail => {
      if (!detail) {
        console.warn(`[useQueryDeepLink] No query found for qd_id=${qdId}`);
        return;
      }
      setDeepLinkedQuery(detailToSeries(detail));
    }).catch(err => {
      console.error('[useQueryDeepLink] Failed to fetch query detail:', { qdId, err });
    });
  }, [query, pendingQdId, services]);

  // Close: clear qd_id from URL (preserving other params)
  const handleClose = useCallback(() => {
    setDeepLinkedQuery(null);
    setPendingQdId(null);
    fetchedRef.current = '';
    updateSharedState({ qd_id: undefined, qd_tab: undefined });
    onClose();
  }, [onClose, updateSharedState]);

  return { query: query ?? deepLinkedQuery, onClose: handleClose };
}
