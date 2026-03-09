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
 *
 * Not yet classified:
 *   Lightweight UPDATE (UPDATE table SET ... WHERE ...) — beta feature in CH 26.1.
 *   Creates patch-<hash>-* parts that are materialized during a subsequent regular
 *   merge (rows_diff = 0). Currently shows as Regular. Unstable: consecutive
 *   updates before patch materialization fail with internal errors.
 */

/** Canonical merge category used throughout the app. */
export type MergeCategory =
  | 'Regular'
  | 'TTLDelete'
  | 'TTLRecompress'
  | 'TTLMove'
  | 'Mutation'
  | 'LightweightDelete';

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
export function classifyActiveMerge(mergeType: string, isMutation: boolean): MergeCategory {
  if (isMutation) return 'Mutation';
  return MERGE_TYPE_MAP[mergeType] ?? 'Regular';
}

/**
 * Classify a historical merge/move event (from system.part_log).
 * Uses event_type + merge_reason fields.
 */
export function classifyMergeHistory(eventType: string, mergeReason: string): MergeCategory {
  if (eventType === 'MovePart') return 'TTLMove';
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
 * Refine a merge category using row-level data.
 *
 * A Regular merge that loses rows (rows_diff < 0) is almost certainly
 * cleaning up rows masked by a lightweight DELETE FROM statement.
 * ClickHouse doesn't distinguish this in merge_reason, so we infer it.
 */
export function refineCategoryWithRowDiff(category: MergeCategory, rowsDiff: number): MergeCategory {
  if (category === 'Regular' && rowsDiff < 0) return 'LightweightDelete';
  return category;
}

/** All known merge_reason values for filter dropdowns. */
export const ALL_MERGE_CATEGORIES: MergeCategory[] = [
  'Regular', 'TTLDelete', 'TTLRecompress', 'TTLMove', 'Mutation', 'LightweightDelete',
];
