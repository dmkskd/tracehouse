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
import { useSearchParams } from 'react-router-dom';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import type { QuerySeries } from '@tracehouse/core';

/** Read a param from the hash-based URL (/#/path?key=val) or standard search */
function getUrlParam(key: string): string | null {
  // HashRouter: params are in the hash fragment
  const hash = window.location.hash;
  const qIdx = hash.indexOf('?');
  if (qIdx !== -1) {
    const val = new URLSearchParams(hash.slice(qIdx + 1)).get(key);
    if (val) return val;
  }
  // Grafana / standard: params are in window.location.search
  return new URLSearchParams(window.location.search).get(key);
}

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
  const [, setSearchParams] = useSearchParams();
  const [deepLinkedQuery, setDeepLinkedQuery] = useState<QuerySeries | null>(null);

  // Read qd_id once on mount from the raw URL — avoids React Router timing race.
  const mountQdId = useRef(getUrlParam('qd_id'));
  const [pendingQdId, setPendingQdId] = useState<string | null>(mountQdId.current);
  const fetchedRef = useRef('');

  // Listen for hash changes (e.g. programmatic navigations)
  useEffect(() => {
    const onHashChange = () => {
      const qdId = getUrlParam('qd_id');
      if (qdId && qdId !== fetchedRef.current) {
        setPendingQdId(qdId);
      }
    };
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  }, []);

  // When the parent selects a query, write qd_id to URL
  useEffect(() => {
    if (!query) return;
    setDeepLinkedQuery(null);
    setPendingQdId(null);
    fetchedRef.current = query.query_id;
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      next.set('qd_id', query.query_id);
      return next;
    }, { replace: true });
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
      console.error(`[useQueryDeepLink] Failed to fetch qd_id=${qdId}:`, err);
    });
  }, [query, pendingQdId, services]);

  // Close: clear qd_id from URL (preserving other params)
  const handleClose = useCallback(() => {
    setDeepLinkedQuery(null);
    setPendingQdId(null);
    fetchedRef.current = '';
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      next.delete('qd_id');
      return next;
    }, { replace: true });
    onClose();
  }, [onClose, setSearchParams]);

  return { query: query ?? deepLinkedQuery, onClose: handleClose };
}
