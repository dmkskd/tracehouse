import type { IClickHouseAdapter } from '../adapters/types.js';
import type { PartLineage, LineageNode, MergeEvent } from '../types/lineage.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_DATABASES, sourceTag } from '../queries/source-tags.js';
import { GET_ACTIVE_PART_SIZES, GET_MERGE_EVENTS_BATCH, GET_L0_PART_SIZES } from '../queries/lineage-queries.js';
import { normalizeTimestamp } from '../mappers/timestamp.js';
import { toInt, toStr } from '../mappers/helpers.js';
import { parseMergedFrom } from '../mappers/lineage-mappers.js';
import { getLevelFromName } from '../utils/part-name-parser.js';
import { classifyMergeHistory } from '../utils/merge-classification.js';

const MAX_PARTS = 2000;

export class LineageServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'LineageServiceError';
  }
}

interface PartSize {
  rows: number;
  size: number;
  level: number;
}

export class LineageService {
  constructor(private adapter: IClickHouseAdapter) {}

  /**
   * Build a merge lineage tree for a specific part.
   *
   * The algorithm:
   * 1. Fetch active part sizes from `system.parts`
   * 2. Iteratively fetch merge events from `system.part_log` level by level
   * 3. Fetch L0 sizes from `part_log` for merged-away parts
   * 4. Build the tree recursively with cycle detection (visited set per path)
   * 5. Calculate summary statistics
   */
  async buildLineageTree(
    database: string,
    table: string,
    partName: string,
  ): Promise<PartLineage> {
    // Step 1: Fetch active part sizes
    const sizesSql = tagQuery(buildQuery(GET_ACTIVE_PART_SIZES, { database, table }), sourceTag(TAB_DATABASES, 'lineagePartSizes'));
    const sizeRows = await this.adapter.executeQuery<Record<string, unknown>>(sizesSql);

    const sizeMap = new Map<string, PartSize>();
    for (const r of sizeRows) {
      sizeMap.set(toStr(r.name), {
        rows: toInt(r.rows),
        size: toInt(r.bytes_on_disk),
        level: toInt(r.level),
      });
    }

    // Step 2: Iteratively fetch merge events level by level
    const allMergeEvents = new Map<string, MergeEvent>();
    let currentParts = [partName];
    let totalSeen = 0;

    while (currentParts.length > 0 && totalSeen < MAX_PARTS) {
      const toFetch = currentParts
        .filter(p => !allMergeEvents.has(p) && getLevelFromName(p) > 0)
        .slice(0, 500);

      if (toFetch.length === 0) break;

      const inList = toFetch.map(p => `'${p}'`).join(',');
      const mergeSql = tagQuery(
        buildQuery(GET_MERGE_EVENTS_BATCH, { database, table }).replace('{partNames}', inList),
        sourceTag(TAB_DATABASES, 'lineageMergeEvents'),
      );

      const mergeRows = await this.adapter.executeQuery<Record<string, unknown>>(mergeSql);

      for (const r of mergeRows) {
        const pname = toStr(r.part_name);
        if (allMergeEvents.has(pname)) continue;

        const mergedFrom = parseMergedFrom(r.merged_from);
        const eventType = toStr(r.event_type) as 'MergeParts' | 'MutatePart';
        allMergeEvents.set(pname, {
          part_name: pname,
          merged_from: mergedFrom,
          event_time: normalizeTimestamp(r.event_time),
          duration_ms: toInt(r.duration_ms),
          rows: toInt(r.rows),
          size_in_bytes: toInt(r.size_in_bytes),
          bytes_uncompressed: toInt(r.bytes_uncompressed),
          read_bytes: toInt(r.read_bytes),
          peak_memory_usage: toInt(r.peak_memory_usage),
          merge_reason: classifyMergeHistory(eventType, toStr(r.merge_reason), pname),
          merge_algorithm: toStr(r.merge_algorithm) || 'Horizontal',
          level: getLevelFromName(pname),
          event_type: eventType,
        });
        totalSeen++;
      }

      // Collect children for next level
      const next: string[] = [];
      for (const p of currentParts) {
        const ev = allMergeEvents.get(p);
        if (ev) next.push(...ev.merged_from);
      }
      currentParts = next;
    }

    // Step 3: Fetch L0 sizes from part_log for merged-away parts
    const allSources = new Set<string>();
    for (const ev of allMergeEvents.values()) {
      for (const s of ev.merged_from) allSources.add(s);
    }
    const l0Parts = [...allSources].filter(
      p => getLevelFromName(p) === 0 && !sizeMap.has(p),
    );
    const l0Sizes = new Map<string, { rows: number; size: number }>();

    if (l0Parts.length > 0) {
      // Fetch in chunks of 500 to avoid huge IN clauses
      for (let i = 0; i < l0Parts.length; i += 500) {
        const chunk = l0Parts.slice(i, i + 500);
        const l0InList = chunk.map(p => `'${p}'`).join(',');
        const l0Sql = tagQuery(
          buildQuery(GET_L0_PART_SIZES, { database, table }).replace('{partNames}', l0InList),
          sourceTag(TAB_DATABASES, 'lineageL0Sizes'),
        );

        const l0Rows = await this.adapter.executeQuery<Record<string, unknown>>(l0Sql);
        for (const r of l0Rows) {
          const pname = toStr(r.part_name);
          if (!l0Sizes.has(pname)) {
            l0Sizes.set(pname, {
              rows: toInt(r.rows),
              size: toInt(r.size_in_bytes),
            });
          }
        }
      }
    }

    // Step 4: Build tree recursively with cycle detection and mutation chain collapsing
    //
    // Mutation chains can be very long: each ALTER TABLE mutation applied to a part
    // creates a new part with the same partition/minBlock/maxBlock/level but a new
    // mutation version suffix.  E.g.:
    //   202602_634_1583_4_2789 → 202602_634_1583_4_2759 → 202602_634_1583_4_2736 → ...
    // Each has event_type='MutatePart' and exactly 1 source part.
    // Without collapsing, this produces a tree thousands of nodes deep.
    //
    // We detect consecutive mutation chains and skip to the end of the chain,
    // keeping only the first (newest) and last (oldest / the real merge) entries.

    /** Check if a merge event is a single-source mutation */
    function isSingleMutation(ev: MergeEvent | undefined): boolean {
      return !!ev && ev.event_type === 'MutatePart' && ev.merged_from.length === 1;
    }

    /**
     * Follow a chain of single-source mutations and return the final part name
     * (the first part that is NOT a single-source mutation, or the end of the
     * chain if we run out of merge events).  Also returns the count of skipped
     * mutations so the UI can indicate the collapse.
     */
    function collapseMutationChain(startSource: string): { endPart: string; skipped: number } {
      let current = startSource;
      let skipped = 0;
      const seen = new Set<string>();
      while (true) {
        if (seen.has(current)) break; // safety: avoid cycles within the chain
        seen.add(current);
        const ev = allMergeEvents.get(current);
        if (!isSingleMutation(ev)) break;
        skipped++;
        current = ev!.merged_from[0];
      }
      return { endPart: current, skipped };
    }

    function buildNode(pname: string, visited: Set<string>): LineageNode {
      // Cycle detection: if this part is already on the current root-to-leaf path, stop
      if (visited.has(pname)) {
        return {
          part_name: pname,
          level: getLevelFromName(pname),
          rows: 0,
          size_in_bytes: 0,
          children: [],
        };
      }
      visited.add(pname);

      const sz = sizeMap.get(pname);
      const l0 = l0Sizes.get(pname);
      let rows = sz?.rows ?? l0?.rows ?? 0;
      let size = sz?.size ?? l0?.size ?? 0;
      const level = sz?.level || getLevelFromName(pname);

      const ev = allMergeEvents.get(pname);
      if (ev) {
        rows = ev.rows || rows;
        size = ev.size_in_bytes || size;

        // Collapse mutation chains: if this is a single-source mutation whose
        // source is also a single-source mutation, skip the intermediate nodes.
        const children = ev.merged_from.map(sourcePart => {
          const sourceEv = allMergeEvents.get(sourcePart);
          if (isSingleMutation(ev) && isSingleMutation(sourceEv)) {
            // The source is another single-source mutation — collapse the chain
            const { endPart, skipped } = collapseMutationChain(sourcePart);
            // Build the node at the end of the chain (the real merge or leaf)
            const endNode = buildNode(endPart, new Set(visited));
            // Annotate the merge event so the UI can show "N mutations skipped"
            if (skipped > 0 && endNode.merge_event) {
              endNode.merge_event = {
                ...endNode.merge_event,
                merge_reason: `${endNode.merge_event.merge_reason} (${skipped} mutation${skipped !== 1 ? 's' : ''} collapsed)`,
              };
            }
            return endNode;
          }
          return buildNode(sourcePart, new Set(visited));
        });

        return {
          part_name: pname,
          level,
          rows,
          size_in_bytes: size,
          event_time: ev.event_time,
          merge_event: ev,
          children,
        };
      }

      // Leaf node (L0 or part without merge event)
      return { part_name: pname, level, rows, size_in_bytes: size, children: [] };
    }

    const root = buildNode(partName, new Set());

    // Step 5: Calculate summary statistics
    function countStats(node: LineageNode): {
      merges: number;
      originals: number;
      timeMs: number;
    } {
      if (node.children.length === 0) {
        return { merges: 0, originals: 1, timeMs: 0 };
      }
      let merges = 1;
      let originals = 0;
      let timeMs = node.merge_event?.duration_ms ?? 0;
      for (const c of node.children) {
        const s = countStats(c);
        merges += s.merges;
        originals += s.originals;
        timeMs += s.timeMs;
      }
      return { merges, originals, timeMs };
    }

    /**
     * Calculate the original L0 size by traversing the tree.
     *
     * Strategy:
     * - For L0 leaf nodes: use size_in_bytes (compressed on-disk size)
     * - For all merges (L1+): recursively sum children's sizes
     *
     * IMPORTANT: Do NOT use read_bytes - it represents uncompressed data read
     * during the merge, not the compressed on-disk sizes of source parts.
     * Using read_bytes would give inflated original size values.
     */
    function calcOriginalSize(node: LineageNode): number {
      // Leaf node (L0 part) - return its compressed size
      if (node.children.length === 0) {
        return node.size_in_bytes;
      }

      // For all merges (L1+), recursively sum children's compressed sizes
      return node.children.reduce((s, c) => s + calcOriginalSize(c), 0);
    }

    const stats = countStats(root);
    return {
      root,
      total_merges: stats.merges,
      total_original_parts: stats.originals,
      total_time_ms: stats.timeMs,
      original_total_size: calcOriginalSize(root),
      final_size: root.size_in_bytes,
    };
  }
}
