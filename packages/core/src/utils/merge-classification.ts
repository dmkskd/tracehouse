/**
 * Merge type classification — single source of truth for all merge/move/mutation
 * categories in ClickHouse.
 *
 * ClickHouse uses different naming between system.merges (merge_type) and
 * system.part_log (merge_reason / event_type). This module normalizes both
 * into a unified set of categories.
 *
 * Categories:
 *   Regular        — background size-based merge (SimpleMergeSelector)
 *   TTLDelete      — merge triggered to remove expired rows (TTLMergeSelector)
 *   TTLRecompress  — merge triggered to apply new codec on aged data
 *   TTLMove        — part relocated to a different volume/disk (NOT a merge)
 *   Mutation       — ALTER TABLE UPDATE/DELETE applied to parts
 *   LightweightDelete — regular merge that cleaned up rows masked by DELETE FROM
 *                       (only inferred on MergeTree; skipped for Replacing/Collapsing/
 *                       Aggregating engines that naturally lose rows during merges)
 *
 * Patch-part detection (lightweight UPDATE, beta in CH 26.1):
 *   UPDATE table SET ... WHERE ... creates patch-<hash>-* parts. ClickHouse sets
 *   is_mutation=false for these merges, but we detect them via the patch- prefix
 *   on result_part_name and classify as Mutation.
 *
 *   Observability gap (as of CH 26.1): patch part creation and consolidation
 *   (patch+patch→patch) are NOT logged in system.part_log. Only RemovePart
 *   events appear after patches are consumed. This means:
 *     - Active merges (system.merges): detected via patch- prefix on result_part_name
 *     - Merge history (system.part_log): invisible — no MergeParts/MutatePart events
 *   The mutation version is embedded in the part name (e.g. patch-<hash>-..._1934
 *   where 1934 is the mutation version from system.mutations), which could be used
 *   for correlation if CH adds part_log coverage in the future.
 */

/** Canonical merge category used throughout the app. */
export type MergeCategory =
  | 'Regular'
  | 'TTLDelete'
  | 'TTLRecompress'
  | 'TTLMove'
  | 'Mutation'
  | 'LightweightDelete'
  | 'LightweightUpdate';

export interface MergeCategoryInfo {
  label: string;
  color: string;
  description: string;
  icon: string;
}

/** Visual metadata for each merge category. */
export const MERGE_CATEGORIES: Record<MergeCategory, MergeCategoryInfo> = {
  Regular:            { label: 'Regular',            color: '#f0883e', icon: '🔄', description: 'Background size-based merge' },
  TTLDelete:          { label: 'TTL Delete',         color: '#e5534b', icon: '🗑', description: 'Merge to remove expired rows' },
  TTLRecompress:      { label: 'TTL Recompress',     color: '#986ee2', icon: '🗜', description: 'Merge to apply new codec on aged data' },
  TTLMove:            { label: 'TTL Move',           color: '#58a6ff', icon: '📦', description: 'Part relocated to different volume/disk' },
  Mutation:           { label: 'Mutation',            color: '#f778ba', icon: '✏', description: 'ALTER TABLE UPDATE/DELETE' },
  LightweightDelete:  { label: 'Lightweight Delete', color: '#d4a72c', icon: '🧹', description: 'Regular merge that cleaned up rows from DELETE FROM' },
  LightweightUpdate:  { label: 'Lightweight Update', color: '#3fb950', icon: '🩹', description: 'UPDATE SET — creates patch parts' },
};

/**
 * Map from system.merges.merge_type → canonical category.
 * Values: Regular, TTLDelete, TTLRecompress
 */
const MERGE_TYPE_MAP: Record<string, MergeCategory> = {
  'Regular':        'Regular',
  'TTLDelete':      'TTLDelete',
  'TTLRecompress':  'TTLRecompress',
};

/**
 * Map from system.part_log.merge_reason → canonical category.
 * ClickHouse uses different naming here (e.g. TTLDropMerge vs TTLDelete).
 */
const MERGE_REASON_MAP: Record<string, MergeCategory> = {
  'RegularMerge':         'Regular',
  'TTLDeleteMerge':       'TTLDelete',
  'TTLDropMerge':         'TTLDelete',
  'TTLRecompressMerge':   'TTLRecompress',
  'TTLMerge':             'TTLDelete',     // older CH versions use generic TTLMerge
  // NotAMerge is NOT mapped here — it means "no merge happened" (e.g. failed
  // fetch, mutation). Actual mutations are caught by event_type === 'MutatePart'
  // in classifyMergeHistory(). Mapping NotAMerge → Mutation would misclassify
  // failed MergeParts operations (e.g. NO_REPLICA_HAS_PART replication errors).
};

/**
 * Classify an active merge (from system.merges).
 * Uses merge_type + is_mutation fields.
 */
export function classifyActiveMerge(mergeType: string, isMutation: boolean, resultPartName?: string): MergeCategory {
  if (resultPartName && isPatchPart(resultPartName)) return 'LightweightUpdate';
  if (isMutation) return 'Mutation';
  return MERGE_TYPE_MAP[mergeType] ?? 'Regular';
}

/**
 * Classify a historical merge/move event (from system.part_log).
 * Uses event_type + merge_reason fields.
 */
export function classifyMergeHistory(eventType: string, mergeReason: string, partName?: string): MergeCategory {
  if (eventType === 'MovePart') return 'TTLMove';
  if (partName && isPatchPart(partName)) return 'LightweightUpdate';
  if (eventType === 'MutatePart') return 'Mutation';
  if (!mergeReason) return 'Regular';
  return MERGE_REASON_MAP[mergeReason] ?? 'Regular';
}

/**
 * Get display info for a merge category.
 */
export function getMergeCategoryInfo(category: MergeCategory): MergeCategoryInfo {
  return MERGE_CATEGORIES[category];
}

/**
 * Table engines that naturally remove rows during regular merges.
 * These must NOT be classified as LightweightDelete when rows_diff < 0
 * because row loss is expected behavior (dedup, collapsing, aggregation).
 */
const DEDUP_ENGINES = new Set([
  'ReplacingMergeTree',
  'CollapsingMergeTree',
  'VersionedCollapsingMergeTree',
  'AggregatingMergeTree',
]);

/** Check if a table engine naturally deduplicates/collapses rows during merges. */
export function isDeduplicatingEngine(engine: string): boolean {
  // Strip Replicated/Shared prefix: ReplicatedReplacingMergeTree → ReplacingMergeTree
  const base = engine.replace(/^(Replicated|Shared)/, '');
  return DEDUP_ENGINES.has(base);
}

/**
 * Refine a merge category using row-level data.
 *
 * A Regular merge that loses rows (rows_diff < 0) is likely cleaning up
 * rows masked by a lightweight DELETE FROM statement. However, engines like
 * ReplacingMergeTree and CollapsingMergeTree also lose rows during normal
 * merges (dedup/collapse), so we skip the heuristic for those engines.
 *
 * @param tableEngine - optional engine name from system.tables; when provided,
 *   deduplicating engines are excluded from the lightweight delete heuristic.
 */
export function refineCategoryWithRowDiff(category: MergeCategory, rowsDiff: number, tableEngine?: string): MergeCategory {
  if (category === 'Regular' && rowsDiff < 0) {
    if (tableEngine && isDeduplicatingEngine(tableEngine)) return category;
    return 'LightweightDelete';
  }
  return category;
}

/** All known merge_reason values for filter dropdowns. */
export const ALL_MERGE_CATEGORIES: MergeCategory[] = [
  'Regular', 'TTLDelete', 'TTLRecompress', 'TTLMove', 'Mutation', 'LightweightDelete', 'LightweightUpdate',
];

// ── Mutation subtype classification ──────────────────────────────────

/**
 * Mutation subtypes — distinguishes lightweight from heavy mutations.
 *
 * ClickHouse doesn't expose this directly in system.mutations, but we can
 * infer it from the command text and result part names:
 *
 *   HeavyDelete   — ALTER TABLE DELETE WHERE ... (rewrites full part, drops rows)
 *   HeavyUpdate   — ALTER TABLE UPDATE SET ... (rewrites full part, preserves rows)
 *   LightweightDelete — DELETE FROM ... (internally: UPDATE _row_exists = 0)
 *   LightweightUpdate — UPDATE ... SET ... WHERE ... (creates patch-* parts, beta)
 */
export type MutationSubtype = 'HeavyDelete' | 'HeavyUpdate' | 'LightweightDelete' | 'LightweightUpdate';

export interface MutationSubtypeInfo {
  label: string;
  shortLabel: string;
  color: string;
  description: string;
}

export const MUTATION_SUBTYPES: Record<MutationSubtype, MutationSubtypeInfo> = {
  HeavyDelete:        { label: 'Heavy Delete',        shortLabel: 'Heavy',       color: '#e5534b', description: 'ALTER TABLE DELETE — rewrites full parts' },
  HeavyUpdate:        { label: 'Heavy Update',        shortLabel: 'Heavy',       color: '#f0883e', description: 'ALTER TABLE UPDATE — rewrites full parts' },
  LightweightDelete:  { label: 'Lightweight Delete',  shortLabel: 'Lightweight', color: '#d4a72c', description: 'DELETE FROM — masks rows via _row_exists column' },
  LightweightUpdate:  { label: 'Lightweight Update',  shortLabel: 'Lightweight', color: '#3fb950', description: 'UPDATE SET — creates patch parts (beta)' },
};

/**
 * Classify a mutation command into a subtype.
 *
 * Detection heuristics:
 * - _row_exists = 0 in command → LightweightDelete (DELETE FROM internally)
 * - DELETE WHERE (no _row_exists) → HeavyDelete (ALTER TABLE DELETE)
 * - UPDATE ... SET ... → HeavyUpdate (ALTER TABLE UPDATE)
 */
export function classifyMutationCommand(command: string): MutationSubtype {
  // ClickHouse converts DELETE FROM → UPDATE _row_exists = 0 WHERE ...
  if (command.includes('_row_exists') && /=\s*0/.test(command)) {
    return 'LightweightDelete';
  }
  // ALTER TABLE DELETE produces "DELETE WHERE ..."
  if (/^DELETE\s/i.test(command.trim())) {
    return 'HeavyDelete';
  }
  // Everything else is an UPDATE (ALTER TABLE UPDATE)
  return 'HeavyUpdate';
}

/**
 * Detect if a part name indicates a lightweight update (patch part).
 * Patch parts have names like: patch-<hash>-<partition>_<min>_<max>_<level>_<version>
 */
export function isPatchPart(partName: string): boolean {
  return partName.startsWith('patch-');
}

/**
 * Mark replica merges in a list of active merges.
 *
 * On replicated tables each replica independently merges the same source parts
 * into the same result part name. The cluster-wide system.merges query returns
 * one entry per replica. This function groups merges by (database, table,
 * result_part_name) and marks all but the most-progressed entry as replica merges.
 */
export function markReplicaMerges<T extends { database: string; table: string; result_part_name: string; progress: number; hostname?: string; is_replica_merge?: boolean }>(merges: T[]): T[] {
  const groups = new Map<string, T[]>();
  for (const m of merges) {
    const key = `${m.database}\0${m.table}\0${m.result_part_name}`;
    const group = groups.get(key);
    if (group) group.push(m);
    else groups.set(key, [m]);
  }
  for (const group of groups.values()) {
    if (group.length <= 1) continue;
    // Sort by progress descending — the most advanced one is the "primary"
    group.sort((a, b) => b.progress - a.progress);
    for (let i = 1; i < group.length; i++) {
      group[i].is_replica_merge = true;
    }
  }
  return merges;
}

/**
 * Mark replica merges in merge history records.
 *
 * Groups by (database, table, part_name); if the same part was merged on
 * multiple hosts, the earliest event is primary and the rest are replicas.
 */
export function markReplicaMergeHistory<T extends { database: string; table: string; part_name: string; event_type: string; event_time: string; hostname?: string; is_replica_merge?: boolean }>(records: T[]): T[] {
  // DownloadPart events are always replica fetches
  for (const r of records) {
    if (r.event_type === 'DownloadPart') {
      r.is_replica_merge = true;
    }
  }
  // For non-fetch events, group by part_name and mark duplicates
  const groups = new Map<string, T[]>();
  for (const r of records) {
    if (r.event_type === 'DownloadPart') continue; // already tagged
    const key = `${r.database}\0${r.table}\0${r.part_name}`;
    const group = groups.get(key);
    if (group) group.push(r);
    else groups.set(key, [r]);
  }
  for (const group of groups.values()) {
    if (group.length <= 1) continue;
    // Sort by event_time ascending — earliest is the "primary" merge
    group.sort((a, b) => a.event_time.localeCompare(b.event_time));
    for (let i = 1; i < group.length; i++) {
      group[i].is_replica_merge = true;
    }
  }
  return records;
}
