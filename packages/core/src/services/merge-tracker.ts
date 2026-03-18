import type { IClickHouseAdapter } from '../adapters/types.js';
import type { MergeInfo, MergeHistoryRecord, MutationInfo, MutationHistoryRecord, BackgroundPoolMetrics, MutationDependencyInfo, MutationPartStatus, CoDependentMutation, StoragePolicyVolume, MergeTextLog } from '../types/merge.js';
import {
  GET_ACTIVE_MERGES,
  GET_MERGE_HISTORY,
  GET_ALL_MERGE_HISTORY,
  GET_DATABASE_MERGE_HISTORY,
  GET_MUTATIONS,
  GET_MUTATION_HISTORY,
  GET_DATABASE_MUTATION_HISTORY,
  GET_TABLE_MUTATION_HISTORY,
  GET_BACKGROUND_POOL_METRICS,
  GET_OUTDATED_PARTS_SIZE,
  GET_STORAGE_POLICY_VOLUMES,
  GET_MERGE_TEXT_LOGS_BY_QUERY_ID,
  GET_TABLE_UUID,
  GET_MERGE_HISTORY_BY_PART_NAME,
} from '../queries/merge-queries.js';
import { buildQuery, tagQuery, eventDateBound } from '../queries/builder.js';
import { TAB_MERGES, sourceTag } from '../queries/source-tags.js';
import { mapMergeInfo, mapMergeHistoryRecord, mapMutationInfo, mapMutationHistoryRecord, mapBackgroundPoolMetrics, mapMergeTextLog } from '../mappers/merge-mappers.js';

export class MergeTrackerError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'MergeTrackerError';
  }
}

export interface MergeHistoryOptions {
  database?: string;
  table?: string;
  minDurationMs?: number;
  minSizeBytes?: number;
  limit?: number;
}

/**
 * Inject optional duration_ms / size_in_bytes threshold filters into a part_log query.
 * Inserts before the ORDER BY clause so it applies as a WHERE condition.
 */
function injectThresholdFilters(sql: string, opts: MergeHistoryOptions): string {
  const clauses: string[] = [];
  if (opts.minDurationMs != null && opts.minDurationMs > 0) {
    clauses.push(`duration_ms >= ${Math.round(opts.minDurationMs)}`);
  }
  if (opts.minSizeBytes != null && opts.minSizeBytes > 0) {
    clauses.push(`size_in_bytes >= ${Math.round(opts.minSizeBytes)}`);
  }
  if (clauses.length === 0) return sql;
  const extra = clauses.map(c => `    AND ${c}`).join('\n');
  return sql.replace(/(\s+ORDER BY)/, `\n${extra}$1`);
}

export class MergeTracker {
  private uuidCache = new Map<string, string>();

  constructor(private adapter: IClickHouseAdapter) {}

  async getActiveMerges(database?: string, table?: string): Promise<MergeInfo[]> {
    try {
      const rows = await this.adapter.executeQuery(tagQuery(GET_ACTIVE_MERGES, sourceTag(TAB_MERGES, 'activeMerges')));
      let merges = rows.map(mapMergeInfo);
      
      // Filter by database/table if specified
      if (database) {
        merges = merges.filter(m => m.database === database);
      }
      if (table) {
        merges = merges.filter(m => m.table === table);
      }
      
      return merges;
    } catch (error) {
      throw new MergeTrackerError('Failed to get active merges', error as Error);
    }
  }

  async getMergeHistory(options: MergeHistoryOptions = {}): Promise<MergeHistoryRecord[]> {
    const limit = options.limit ?? 100;
    try {
      let sql: string;
      if (options.database && options.table) {
        sql = buildQuery(GET_MERGE_HISTORY, { database: options.database, table: options.table, limit });
      } else if (options.database) {
        sql = buildQuery(GET_DATABASE_MERGE_HISTORY, { database: options.database, limit });
      } else {
        sql = buildQuery(GET_ALL_MERGE_HISTORY, { limit });
      }
      sql = injectThresholdFilters(sql, options);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_MERGES, 'mergeHistory')));
      return rows.map(mapMergeHistoryRecord);
    } catch (error) {
      throw new MergeTrackerError('Failed to get merge history', error as Error);
    }
  }

  async getMutations(): Promise<MutationInfo[]> {
    const sql = buildQuery(GET_MUTATIONS, {});
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_MERGES, 'mutations')));
      return rows.map(mapMutationInfo);
    } catch (error) {
      throw new MergeTrackerError('Failed to get mutations', error as Error);
    }
  }

  async getMutationHistory(options: MergeHistoryOptions = {}): Promise<MutationHistoryRecord[]> {
    const limit = options.limit ?? 100;
    try {
      let sql: string;
      if (options.database && options.table) {
        sql = buildQuery(GET_TABLE_MUTATION_HISTORY, { database: options.database, table: options.table, limit });
      } else if (options.database) {
        sql = buildQuery(GET_DATABASE_MUTATION_HISTORY, { database: options.database, limit });
      } else {
        sql = buildQuery(GET_MUTATION_HISTORY, { limit });
      }
      // Note: do NOT apply injectThresholdFilters here — system.mutations lacks duration_ms/size_in_bytes columns
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_MERGES, 'mutationHistory')));
      return rows.map(mapMutationHistoryRecord);
    } catch (error) {
      throw new MergeTrackerError('Failed to get mutation history', error as Error);
    }
  }

  async getBackgroundPoolMetrics(): Promise<BackgroundPoolMetrics> {
    try {
      // Fetch pool metrics and outdated parts size in parallel
      const [poolRows, sizeRows] = await Promise.all([
        this.adapter.executeQuery(tagQuery(GET_BACKGROUND_POOL_METRICS, sourceTag(TAB_MERGES, 'poolMetrics'))),
        this.adapter.executeQuery(tagQuery(GET_OUTDATED_PARTS_SIZE, sourceTag(TAB_MERGES, 'outdatedParts'))),
      ]);
      
      const poolRow = poolRows.length > 0 ? poolRows[0] : {};
      const sizeRow = sizeRows.length > 0 ? sizeRows[0] : undefined;
      
      return mapBackgroundPoolMetrics(poolRow, sizeRow);
    } catch (error) {
      throw new MergeTrackerError('Failed to get background pool metrics', error as Error);
    }
  }

  /**
   * Get storage policy volume mapping: disk → { volume, policy }.
   * Used to resolve logical volume/policy names for TTL move events.
   */
  async getStoragePolicyVolumes(): Promise<StoragePolicyVolume[]> {
    try {
      const rows = await this.adapter.executeQuery<{
        policy_name: string;
        volume_name: string;
        disks: string[];
      }>(tagQuery(GET_STORAGE_POLICY_VOLUMES, sourceTag(TAB_MERGES, 'storagePolicies')));
      return rows.map(row => ({
        policyName: String(row.policy_name),
        volumeName: String(row.volume_name),
        disks: Array.isArray(row.disks) ? row.disks.map(String) : [],
      }));
    } catch (error) {
      console.error('[MergeTracker] getStoragePolicyVolumes error:', error);
      return [];
    }
  }

  /**
   * Look up a single MergeHistoryRecord by database + table + part_name.
   * Returns null if no matching part_log entry is found.
   */
  async getMergeHistoryByPartName(database: string, table: string, partName: string): Promise<MergeHistoryRecord | null> {
    try {
      const sql = buildQuery(GET_MERGE_HISTORY_BY_PART_NAME, { database, table, part_name: partName });
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_MERGES, 'mergeHistoryByPart')));
      if (rows.length === 0) return null;
      return mapMergeHistoryRecord(rows[0]);
    } catch (error) {
      console.error('[MergeTracker] getMergeHistoryByPartName error:', error);
      return null;
    }
  }

  /**
   * Fetch text_log messages correlated to a merge/mutation event.
   *
   * Strategy:
   * 1. If query_id is available (user-triggered mutations, newer CH), use exact match.
   * 2. Otherwise, correlate by time window: look for log messages from the table's
   *    logger within the merge's time window (event_time - duration → event_time).
   *    Also matches messages mentioning the result part name.
   *
   * This is fuzzy for case 2 — we may pick up unrelated messages or miss some —
   * but something is better than nothing for debugging merge behavior.
   */
  /**
     * Fetch text_log messages correlated to a merge/mutation event.
     *
     * Strategy (in priority order):
     * 1. If query_id is available from part_log, use exact match.
     * 2. Construct the merge query_id as `{table_uuid}::{part_name}` — ClickHouse
     *    uses this format in system.text_log for background merges even though
     *    part_log.query_id is empty. Try exact match with this constructed ID.
     * 3. Fall back to time-window correlation with logger name matching.
     */
    /**
       * Fetch text_log messages for a merge/mutation event.
       *
       * ClickHouse logs background merges to system.text_log with query_id
       * formatted as `{table_uuid}::{result_part_name}`. We look up the table
       * UUID and construct this ID for an exact match.
       */
      async getMergeEventTextLogs(record: {
        query_id?: string;
        event_time: string;
        duration_ms: number;
        database: string;
        table: string;
        part_name: string;
      }): Promise<MergeTextLog[]> {
        try {
          const cacheKey = `${record.database}.${record.table}`;
          let uuid = this.uuidCache.get(cacheKey);
          if (!uuid) {
            const uuidSql = buildQuery(GET_TABLE_UUID, { database: record.database, table: record.table });
            const uuidRows = await this.adapter.executeQuery<{ uuid: string }>(tagQuery(uuidSql, sourceTag(TAB_MERGES, 'tableUuid')));
            uuid = uuidRows.length > 0 ? String(uuidRows[0].uuid) : '';
            if (uuid && uuid !== '00000000-0000-0000-0000-000000000000') {
              this.uuidCache.set(cacheKey, uuid);
            }
          }
          if (!uuid || uuid === '00000000-0000-0000-0000-000000000000') return [];

          const queryId = `${uuid}::${record.part_name}`;
          const dateBound = eventDateBound(record.event_time);
          const sql = buildQuery(GET_MERGE_TEXT_LOGS_BY_QUERY_ID.replace('{event_date_bound}', dateBound), { query_id: queryId });
          const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_MERGES, 'mergeTextLogs')));
          return rows.map(mapMergeTextLog);
        } catch (error) {
          console.error('[MergeTracker] getMergeEventTextLogs error:', error);
          return [];
        }
      }

  /**
   * Compute mutation dependency info by cross-referencing mutations with active merges.
   * This is a pure computation — no extra queries needed.
   */
  analyzeMutationDependencies(
    mutation: MutationInfo,
    allMutations: MutationInfo[],
    activeMerges: MergeInfo[],
  ): MutationDependencyInfo {
    const allParts = [
      ...mutation.parts_to_do_names,
      ...mutation.parts_in_progress_names,
    ];

    // Build a lookup: part name → active merge (includes mutation merges)
    const tableMerges = activeMerges.filter(
      m => m.database === mutation.database && m.table === mutation.table,
    );
    const partToMerge = new Map<string, MergeInfo>();
    for (const merge of tableMerges) {
      for (const src of merge.source_part_names) {
        partToMerge.set(src, merge);
      }
    }

    // Classify each part
    const inProgressSet = new Set(mutation.parts_in_progress_names);
    const partStatuses: MutationPartStatus[] = allParts.map(partName => {
      const merge = partToMerge.get(partName);
      if (inProgressSet.has(partName) && merge) {
        // Part is in progress AND there's an active merge processing it
        return {
          part_name: partName,
          status: 'mutating' as const,
          merge_result_part: merge.result_part_name,
          merge_progress: merge.progress,
          merge_elapsed: merge.elapsed,
        };
      }
      if (inProgressSet.has(partName)) {
        // Part is in progress but no active merge visible yet
        return { part_name: partName, status: 'mutating' as const };
      }
      if (merge && !merge.is_mutation) {
        // Part is in a regular merge (will subsume pending mutations)
        return {
          part_name: partName,
          status: 'merging' as const,
          merge_result_part: merge.result_part_name,
          merge_progress: merge.progress,
          merge_elapsed: merge.elapsed,
        };
      }
      return { part_name: partName, status: 'idle' as const };
    });

    // Find co-dependent mutations (other mutations on same table sharing parts)
    const myParts = new Set(allParts);
    const coDeps: CoDependentMutation[] = [];
    for (const other of allMutations) {
      if (other.mutation_id === mutation.mutation_id) continue;
      if (other.database !== mutation.database || other.table !== mutation.table) continue;
      const otherParts = [...other.parts_to_do_names, ...other.parts_in_progress_names];
      const shared = otherParts.filter(p => myParts.has(p));
      if (shared.length > 0) {
        coDeps.push({
          mutation_id: other.mutation_id,
          command: other.command,
          shared_parts_count: shared.length,
          shared_parts: shared,
        });
      }
    }

    // Count parts covered by any active merge (mutating with merge + regular merging)
    const mergingParts = partStatuses.filter(p => p.status === 'merging');
    const mutatingWithMerge = partStatuses.filter(p => p.status === 'mutating' && p.merge_result_part);
    const allCoveredParts = [...mergingParts, ...mutatingWithMerge];
    const uniqueMerges = new Set(allCoveredParts.map(p => p.merge_result_part).filter(Boolean));

    return {
      mutation_id: mutation.mutation_id,
      database: mutation.database,
      table: mutation.table,
      part_statuses: partStatuses,
      co_dependent_mutations: coDeps.sort((a, b) => b.shared_parts_count - a.shared_parts_count),
      parts_covered_by_merges: allCoveredParts.length,
      active_merges_covering: uniqueMerges.size,
    };
  }
}
