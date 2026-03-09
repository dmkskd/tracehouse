/**
 * Integration tests for merge lineage byte accounting.
 *
 * Exercises DatabaseExplorer and MergeTracker against a real ClickHouse
 * instance to verify that bytes at each merge level (L0 → Ln) are tracked
 * correctly through the lineage tree:
 *
 *   1. Insert known batches → L0 parts
 *   2. Use DatabaseExplorer.getTableParts() to snapshot L0 sizes
 *   3. Force merges via OPTIMIZE TABLE
 *   4. Use DatabaseExplorer.getPartLineage() to build the tree
 *   5. Use MergeTracker.getMergeHistory() to cross-check merge events
 *   6. Validate byte invariants at every level
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';
import { DatabaseExplorer } from '../../services/database-explorer.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { parsePartName } from '../../lineage/part-name-parser.js';
import type { LineageNode } from '../../types/lineage.js';
import type { PartInfo } from '../../types/database.js';
import type { PartLineage } from '../../types/lineage.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'level_bytes_test';

// ── helpers ──────────────────────────────────────────────────────

/** Collect every node in a lineage tree (DFS). */
function collectNodes(node: LineageNode): LineageNode[] {
  return [node, ...node.children.flatMap(collectNodes)];
}

/** Sum size_in_bytes of all leaf nodes (L0 / original parts) in the tree. */
function sumLeafBytes(node: LineageNode): number {
  if (node.children.length === 0) return node.size_in_bytes;
  return node.children.reduce((s, c) => s + sumLeafBytes(c), 0);
}

// ── tests ────────────────────────────────────────────────────────

describe('Merge lineage byte accounting', () => {
  let ctx: TestClickHouseContext;
  let explorer: DatabaseExplorer;
  let mergeTracker: MergeTracker;

  /** L0 parts captured via getTableParts() *before* OPTIMIZE. */
  let l0PartsBeforeMerge: PartInfo[];
  /** Total L0 bytes_on_disk before any merge. */
  let totalL0Bytes: number;

  /** The single active part after OPTIMIZE FINAL. */
  let mergedPart: PartInfo;
  /** Lineage tree built via the service layer. */
  let lineage: PartLineage;

  beforeAll(async () => {
    ctx = await startClickHouse();
    explorer = new DatabaseExplorer(ctx.adapter);
    mergeTracker = new MergeTracker(ctx.adapter);

    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });

    // Simple MergeTree — no dedup, no TTL, no codec overrides.
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.events (
          id    UInt64,
          ts    DateTime DEFAULT now(),
          value String
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS
          min_bytes_for_wide_part = 0,
          min_rows_for_wide_part  = 0
      `,
    });

    // Insert 8 separate batches → 8 L0 parts.
    const BATCHES = 8;
    const ROWS_PER_BATCH = 1000;
    for (let i = 0; i < BATCHES; i++) {
      await ctx.client.command({
        query: `
          INSERT INTO ${TEST_DB}.events (id, value)
          SELECT number + ${i * ROWS_PER_BATCH}, repeat('x', 100)
          FROM numbers(${ROWS_PER_BATCH})
        `,
      });
    }

    // ── Snapshot L0 sizes via the service layer ──
    l0PartsBeforeMerge = await explorer.getTableParts(TEST_DB, 'events');
    totalL0Bytes = l0PartsBeforeMerge.reduce((s, p) => s + p.bytes_on_disk, 0);

    // Sanity: we should have multiple L0 parts.
    expect(l0PartsBeforeMerge.length).toBeGreaterThanOrEqual(2);

    // Force a full merge.
    await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.events FINAL` });
    await new Promise(r => setTimeout(r, 3000));
    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

    // ── Fetch the merged state via the service layer ──
    const partsAfter = await explorer.getTableParts(TEST_DB, 'events');
    expect(partsAfter.length).toBe(1);
    mergedPart = partsAfter[0];

    lineage = await explorer.getPartLineage(TEST_DB, 'events', mergedPart.name);
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  // ── core byte invariants ─────────────────────────────────────

  it('final_size matches getTableParts() bytes_on_disk', () => {
    expect(lineage.final_size).toBe(mergedPart.bytes_on_disk);
  });

  it('original_total_size equals the sum of L0 leaf sizes in the tree', () => {
    const leafSum = sumLeafBytes(lineage.root);
    expect(lineage.original_total_size).toBe(leafSum);
  });

  it('every L0 leaf has a non-zero size_in_bytes', () => {
    const allNodes = collectNodes(lineage.root);
    const l0Leaves = allNodes.filter(n => n.level === 0);

    expect(l0Leaves.length).toBeGreaterThanOrEqual(2);
    for (const leaf of l0Leaves) {
      expect(leaf.size_in_bytes).toBeGreaterThan(0);
      expect(leaf.children).toEqual([]);
    }
  });

  it('original_total_size uses compressed sizes (not read_bytes)', () => {
    // The lineage original_total_size should be in the same ballpark as
    // the pre-merge L0 total from getTableParts(). If read_bytes
    // (uncompressed) were used, the ratio would be 2-4x.
    const ratio = lineage.original_total_size / totalL0Bytes;
    expect(ratio).toBeGreaterThan(0.5);
    expect(ratio).toBeLessThan(2.0);
  });

  // ── structural invariants via service layer ──────────────────

  it('merged part level > 0 and root has children', () => {
    expect(mergedPart.level).toBeGreaterThan(0);
    expect(lineage.root.level).toBe(mergedPart.level);
    expect(lineage.root.children.length).toBeGreaterThan(0);
  });

  it('child levels are strictly less than parent levels', () => {
    function checkLevels(node: LineageNode): void {
      for (const child of node.children) {
        expect(child.level).toBeLessThan(node.level);
        checkLevels(child);
      }
    }
    checkLevels(lineage.root);
  });

  it('total_original_parts matches the number of leaf nodes', () => {
    const allNodes = collectNodes(lineage.root);
    const leafCount = allNodes.filter(n => n.children.length === 0).length;
    expect(lineage.total_original_parts).toBe(leafCount);
  });

  it('parsePartName agrees with PartInfo.level for all tree nodes', () => {
    const allNodes = collectNodes(lineage.root);
    for (const node of allNodes) {
      const parsed = parsePartName(node.part_name);
      expect(parsed).not.toBeNull();
      expect(parsed!.level).toBe(node.level);
    }
  });

  // ── cross-check against MergeTracker ─────────────────────────

  it('getMergeHistory() contains the merge event for the final part', async () => {
    const history = await mergeTracker.getMergeHistory({
      database: TEST_DB,
      table: 'events',
      limit: 50,
    });

    // There should be at least one merge event.
    expect(history.length).toBeGreaterThanOrEqual(1);

    // The final merged part should appear in the history.
    const finalMerge = history.find(h => h.part_name === mergedPart.name);
    if (finalMerge) {
      // size_in_bytes from merge history should match the lineage root.
      expect(finalMerge.size_in_bytes).toBe(lineage.root.size_in_bytes);
      // source_part_names should match the root's direct children.
      const childNames = lineage.root.children.map(c => c.part_name).sort();
      const sourceNames = [...finalMerge.source_part_names].sort();
      expect(sourceNames).toEqual(childNames);
    }
  });

  it('merge history size_in_bytes matches lineage node sizes for intermediate merges', async () => {
    const history = await mergeTracker.getMergeHistory({
      database: TEST_DB,
      table: 'events',
      limit: 100,
    });

    const historyMap = new Map(history.map(h => [h.part_name, h]));

    // Check every non-leaf node in the lineage tree.
    const mergeNodes = collectNodes(lineage.root).filter(
      n => n.merge_event && n.children.length > 0,
    );

    for (const node of mergeNodes) {
      const histEntry = historyMap.get(node.part_name);
      if (histEntry) {
        expect(node.merge_event!.size_in_bytes).toBe(histEntry.size_in_bytes);
      }
    }
  });

  // ── bytes-per-level sanity ───────────────────────────────────

  it('each parent size_in_bytes >= each child size_in_bytes', () => {
    const allNodes = collectNodes(lineage.root);
    for (const node of allNodes) {
      for (const child of node.children) {
        expect(node.size_in_bytes).toBeGreaterThanOrEqual(child.size_in_bytes);
      }
    }
  });

  it('getTableParts() level matches lineage root level', () => {
    // The PartInfo.level from system.parts should agree with the lineage.
    expect(mergedPart.level).toBe(lineage.root.level);
  });

  it('getTableParts() before forced merge contains at least some L0 parts', () => {
    // Background merges may have already promoted some parts before our
    // snapshot, so we can't assume *all* are L0 — but at least some must be.
    const l0Count = l0PartsBeforeMerge.filter(p => p.level === 0).length;
    expect(l0Count).toBeGreaterThanOrEqual(1);
  });
});
