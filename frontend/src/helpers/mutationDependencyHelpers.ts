/**
 * Mutation dependency helpers.
 *
 * Pure functions extracted from MutationsPanel (MergeTracker.tsx) so the
 * component stays focused on rendering and the dependency logic is
 * independently testable.
 */

import type {
  MergeInfo,
  MutationInfo,
  MutationDependencyInfo,
} from '../stores/mergeStore';

/**
 * Build a lookup map from part name → active merge that references it.
 */
export function buildPartToMergeMap(
  activeMerges: MergeInfo[],
): Map<string, MergeInfo> {
  const map = new Map<string, MergeInfo>();
  for (const merge of activeMerges) {
    for (const src of merge.source_part_names) {
      map.set(src, merge);
    }
  }
  return map;
}

/**
 * Find the active merge (if any) that is processing a mutation's parts.
 */
export function getMergeForMutation(
  mutation: MutationInfo,
  partToMerge: Map<string, MergeInfo>,
): MergeInfo | null {
  for (const part of mutation.parts_in_progress_names) {
    const merge = partToMerge.get(part);
    if (merge) return merge;
  }
  for (const part of mutation.parts_to_do_names) {
    const merge = partToMerge.get(part);
    if (merge) return merge;
  }
  return null;
}

/**
 * Group mutations by the merge that blocks them.
 *
 * Returns a map of result_part_name → { merge, count } plus the number of
 * mutations not linked to any merge.
 */
export function groupMutationsByMerge(
  mutations: MutationInfo[],
  partToMerge: Map<string, MergeInfo>,
): { mergeGroups: Map<string, { merge: MergeInfo; count: number }>; unlinkedCount: number } {
  const mergeGroups = new Map<string, { merge: MergeInfo; count: number }>();
  let unlinkedCount = 0;

  for (const mut of mutations) {
    const merge = getMergeForMutation(mut, partToMerge);
    if (merge) {
      const key = merge.result_part_name;
      const existing = mergeGroups.get(key);
      if (existing) {
        existing.count++;
      } else {
        mergeGroups.set(key, { merge, count: 1 });
      }
    } else {
      unlinkedCount++;
    }
  }

  return { mergeGroups, unlinkedCount };
}

/**
 * Compute full dependency info for a single mutation against the current
 * set of active merges and sibling mutations.
 */
export function computeMutationDependency(
  mutation: MutationInfo,
  activeMerges: MergeInfo[],
  allMutations: MutationInfo[],
): MutationDependencyInfo | null {
  const allParts = [...mutation.parts_to_do_names, ...mutation.parts_in_progress_names];
  if (allParts.length === 0) return null;

  const tableMerges = activeMerges.filter(
    m => m.database === mutation.database && m.table === mutation.table,
  );

  const ptm = new Map<string, MergeInfo>();
  for (const merge of tableMerges) {
    for (const src of merge.source_part_names) ptm.set(src, merge);
  }

  const inProgressSet = new Set(mutation.parts_in_progress_names);

  const partStatuses = allParts.map(partName => {
    const merge = ptm.get(partName);
    if (inProgressSet.has(partName) && merge) {
      return {
        part_name: partName,
        status: 'mutating' as const,
        merge_result_part: merge.result_part_name,
        merge_progress: merge.progress,
        merge_elapsed: merge.elapsed,
      };
    }
    if (inProgressSet.has(partName)) {
      return { part_name: partName, status: 'mutating' as const };
    }
    if (merge && !merge.is_mutation) {
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

  const myParts = new Set(allParts);
  const coDeps = allMutations
    .filter(
      o =>
        o.mutation_id !== mutation.mutation_id &&
        o.database === mutation.database &&
        o.table === mutation.table,
    )
    .map(o => {
      const shared = [...o.parts_to_do_names, ...o.parts_in_progress_names].filter(p =>
        myParts.has(p),
      );
      return shared.length > 0
        ? {
            mutation_id: o.mutation_id,
            command: o.command,
            shared_parts_count: shared.length,
            shared_parts: shared,
          }
        : null;
    })
    .filter((x): x is NonNullable<typeof x> => x !== null);

  const covered = partStatuses.filter(p => p.merge_result_part);
  const uniqueMerges = new Set(covered.map(p => p.merge_result_part).filter(Boolean));

  return {
    mutation_id: mutation.mutation_id,
    database: mutation.database,
    table: mutation.table,
    part_statuses: partStatuses,
    co_dependent_mutations: coDeps,
    parts_covered_by_merges: covered.length,
    active_merges_covering: uniqueMerges.size,
  };
}
