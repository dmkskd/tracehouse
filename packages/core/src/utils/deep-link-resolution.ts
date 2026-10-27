/**
 * Poll loop behind query / merge deep links.
 *
 * Tries the finished-record lookup first. On a miss it asks `diagnose` why,
 * reports that status, and retries while the operation is running or its log
 * row is pending flush. Pending flush is capped at `pendingFlushWindowMs` so a
 * QueryStart row that never gets a finish row does not poll forever.
 */

import type { LinkStatus } from '../services/link-resolver.js';

export interface ResolveDeepLinkOptions<T> {
  fetch: () => Promise<T | null>;
  diagnose: () => Promise<LinkStatus>;
  onStatus: (status: LinkStatus | null) => void;
  signal: AbortSignal;
  pollIntervalMs: number;
  pendingFlushWindowMs: number;
}

/**
 * Queries page link for one query. `endedAt` (epoch ms) tells the target page
 * the query finished recently, so a missing query_log row reads as pending flush.
 */
export function buildQueryLinkUrl(queryId: string, endedAt?: number | null): string {
  const params = new URLSearchParams({ qd_id: queryId });
  if (endedAt != null) params.set('qd_end', String(endedAt));
  return `/queries?${params.toString()}`;
}

/**
 * Merges page link for the merge / mutation producing `partName` on
 * `hostname`. Replicas merge the same part name independently, so the host
 * keeps the link on the replica it came from.
 */
export function buildMergeLinkUrl(database: string, table: string, partName: string, hostname?: string, startedAt?: number): string {
  const params = new URLSearchParams({ md_db: database, md_tbl: table, md_part: partName });
  if (hostname) params.set('md_host', hostname);
  if (startedAt != null) params.set('md_start', String(startedAt));
  return `/merges?${params.toString()}`;
}

export type DeepLinkKind = 'query' | 'merge';

/** 33s, 4m 12s, 2h 53m */
function shortDuration(sec: number): string {
  const s = Math.max(0, Math.round(sec));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

/**
 * User-facing text for a deep link that has not resolved to a record.
 * The id is already in the URL, so only the not-found case repeats it.
 */
export function describeLinkStatus(kind: DeepLinkKind, subject: string, status: LinkStatus): string {
  const noun = kind === 'query' ? 'Query' : 'Merge';
  const log = kind === 'query' ? 'system.query_log' : 'system.part_log';
  switch (status.state) {
    case 'running': {
      const progress = status.progress !== undefined ? `${Math.round(status.progress * 100)}% · ` : '';
      return `${noun} still running on ${status.hostname} · ${progress}${shortDuration(status.elapsedSec)}. Details open when it finishes.`;
    }
    case 'pending_flush':
      return `${noun} finished. Waiting for ${log} to be written.`;
    case 'not_found':
      return `No record of ${kind} ${subject} in ${log}. It may be outside the log's retention, on another cluster, or the log may be disabled.`;
  }
}

function wait(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise(resolve => {
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
    function done() {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    }
  });
}

/**
 * Resolves to the record, or null when it is not found or the signal aborts.
 * Calls `onStatus(null)` once the record is found.
 */
export async function resolveDeepLink<T>(opts: ResolveDeepLinkOptions<T>): Promise<T | null> {
  let pendingSince: number | null = null;
  let sawRunning = false;
  while (!opts.signal.aborted) {
    const record = await opts.fetch();
    if (opts.signal.aborted) return null;
    if (record) {
      opts.onStatus(null);
      return record;
    }

    let status = await opts.diagnose();
    if (opts.signal.aborted) return null;
    // Seen running earlier and gone now: it just finished, the log row is on its way.
    if (status.state === 'running') sawRunning = true;
    else if (status.state === 'not_found' && sawRunning) status = { state: 'pending_flush' };
    if (status.state === 'pending_flush') {
      pendingSince ??= Date.now();
      if (Date.now() - pendingSince >= opts.pendingFlushWindowMs) status = { state: 'not_found' };
    } else {
      pendingSince = null;
    }
    opts.onStatus(status);
    if (status.state === 'not_found') return null;

    await wait(opts.pollIntervalMs, opts.signal);
  }
  return null;
}
