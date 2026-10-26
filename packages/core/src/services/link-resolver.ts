/**
 * LinkResolver — explains why a deep link found no finished record.
 *
 * Query and merge deep links resolve against query_log / part_log, which only
 * hold finished operations and are written on a flush interval. When that
 * lookup misses, this service checks whether the operation is still running
 * or finished recently, so the UI can say so instead of "not found".
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import { buildQuery, eventDateBound, tagQuery } from '../queries/builder.js';
import { TAB_MERGES, TAB_QUERIES, sourceTag } from '../queries/source-tags.js';
import {
  LINK_QUERY_RUNNING,
  LINK_QUERY_STARTED,
  LINK_MERGE_RUNNING,
  LINK_MERGE_RESULT_PART_RECENT,
} from '../queries/link-resolution-queries.js';

export interface LinkResolutionConfig {
  /** Check system.processes / system.merges for a still-running operation. */
  checkRunning: boolean;
  /** Check for an operation that finished but whose log row is not flushed yet. */
  checkPendingFlush: boolean;
  /** How long after an operation ends its log row may still be missing. */
  pendingFlushWindowMs: number;
  /** Delay between re-checks while an operation is running or pending flush. */
  pollIntervalMs: number;
}

/** Code-level switches; not exposed to users. */
export const LINK_RESOLUTION_CONFIG: LinkResolutionConfig = {
  checkRunning: true,
  checkPendingFlush: true,
  // Stock system log flush interval is 7.5s; leave room for slow flushes.
  pendingFlushWindowMs: 20_000,
  pollIntervalMs: 3_000,
};

export type LinkStatus =
  | { state: 'running'; hostname: string; elapsedSec: number; progress?: number }
  | { state: 'pending_flush'; hostname?: string }
  | { state: 'not_found' };

export interface MergeLinkRef {
  database: string;
  table: string;
  partName: string;
  /** The replica the link is about; replicas merge the same part name independently. */
  hostname?: string;
}

export class LinkResolver {
  constructor(
    private adapter: IClickHouseAdapter,
    private config: LinkResolutionConfig = LINK_RESOLUTION_CONFIG,
  ) {}

  get pollIntervalMs(): number {
    return this.config.pollIntervalMs;
  }

  get pendingFlushWindowMs(): number {
    return this.config.pendingFlushWindowMs;
  }

  /**
   * Called after QUERY_DETAIL found no finished row for `queryId`.
   * `endedAt` is when the caller saw the query finish (e.g. the Overview arena),
   * in epoch ms of the same clock as Date.now().
   */
  async diagnoseQuery(queryId: string, endedAt?: number): Promise<LinkStatus> {
    if (this.config.checkRunning) {
      const rows = await this.adapter.executeQuery<{ hostname: string; elapsed: number }>(
        tagQuery(buildQuery(LINK_QUERY_RUNNING, { query_id: queryId }), sourceTag(TAB_QUERIES, 'linkQueryRunning')),
      );
      if (rows.length > 0) {
        return { state: 'running', hostname: String(rows[0].hostname), elapsedSec: Number(rows[0].elapsed) };
      }
    }
    if (this.config.checkPendingFlush) {
      if (endedAt !== undefined && Date.now() - endedAt < this.config.pendingFlushWindowMs) {
        return { state: 'pending_flush' };
      }
      const sql = buildQuery(
        LINK_QUERY_STARTED.replace('{event_date_bound}', eventDateBound()),
        { query_id: queryId },
      );
      const rows = await this.adapter.executeQuery<{ hostname: string }>(
        tagQuery(sql, sourceTag(TAB_QUERIES, 'linkQueryStarted')),
      );
      if (rows.length > 0) return { state: 'pending_flush', hostname: String(rows[0].hostname) };
    }
    return { state: 'not_found' };
  }

  /** Called after part_log had no record for the merge producing `ref.partName`. */
  async diagnoseMerge(ref: MergeLinkRef): Promise<LinkStatus> {
    const params = { database: ref.database, table: ref.table, part_name: ref.partName };
    const onHost = <T extends { hostname: string }>(rows: T[]): T | undefined =>
      ref.hostname ? rows.find(r => String(r.hostname) === ref.hostname) : rows[0];
    if (this.config.checkRunning) {
      const rows = await this.adapter.executeQuery<{ hostname: string; elapsed: number; progress: number }>(
        tagQuery(buildQuery(LINK_MERGE_RUNNING, params), sourceTag(TAB_MERGES, 'linkMergeRunning')),
      );
      const row = onHost(rows);
      if (row) {
        return {
          state: 'running',
          hostname: String(row.hostname),
          elapsedSec: Number(row.elapsed),
          progress: Number(row.progress),
        };
      }
    }
    if (this.config.checkPendingFlush) {
      const sql = buildQuery(LINK_MERGE_RESULT_PART_RECENT, {
        ...params,
        window_s: Math.ceil(this.config.pendingFlushWindowMs / 1000),
      });
      const rows = await this.adapter.executeQuery<{ hostname: string }>(
        tagQuery(sql, sourceTag(TAB_MERGES, 'linkMergeResultPart')),
      );
      const row = onHost(rows);
      if (row) return { state: 'pending_flush', hostname: String(row.hostname) };
    }
    return { state: 'not_found' };
  }
}
