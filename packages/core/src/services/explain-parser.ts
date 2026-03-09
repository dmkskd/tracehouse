/**
 * Parser for ClickHouse EXPLAIN json = 1, indexes = 1 output.
 *
 * Walks the nested plan tree and extracts index usage information:
 * which keys were used, the condition, parts/granules selected.
 */

import type { ExplainIndexesResult, ExplainIndexEntry } from '../types/analytics.js';

/**
 * Parse the JSON output of EXPLAIN json = 1, indexes = 1.
 *
 * The output is a JSON array with a nested plan tree. We walk the tree
 * looking for nodes that have an "Indexes" array (ReadFromMergeTree nodes).
 */
export function parseExplainIndexesJson(jsonText: string): ExplainIndexesResult {
  const empty: ExplainIndexesResult = { indexes: [], primaryKey: null, skipIndexes: [], success: false };

  if (!jsonText.trim()) return { ...empty, error: 'Empty EXPLAIN output' };

  try {
    const parsed = JSON.parse(jsonText);
    const planArray = Array.isArray(parsed) ? parsed : [parsed];
    const allIndexes: ExplainIndexEntry[] = [];

    function walkPlan(node: Record<string, unknown>): void {
      if (!node || typeof node !== 'object') return;
      const plan = (node.Plan ?? node) as Record<string, unknown>;

      if (Array.isArray(plan.Indexes)) {
        for (const idx of plan.Indexes) {
          allIndexes.push(parseIndexEntry(idx as Record<string, unknown>));
        }
      }

      if (Array.isArray(plan.Plans)) {
        for (const child of plan.Plans) {
          walkPlan(child as Record<string, unknown>);
        }
      }
    }

    for (const root of planArray) {
      walkPlan(root as Record<string, unknown>);
    }

    return {
      indexes: allIndexes,
      primaryKey: allIndexes.find(i => i.type === 'PrimaryKey') ?? null,
      skipIndexes: allIndexes.filter(i => i.type === 'Skip'),
      success: true,
    };
  } catch {
    return { ...empty, error: 'Failed to parse EXPLAIN JSON output' };
  }
}

/**
 * Parse a single index entry from the EXPLAIN JSON Indexes array.
 *
 * Example:
 * { "Type": "PrimaryKey", "Keys": ["UserID"], "Condition": "...", "Parts": "1/1", "Granules": "1/1083" }
 */
export function parseIndexEntry(raw: Record<string, unknown>): ExplainIndexEntry {
  return {
    type: String(raw.Type ?? ''),
    keys: Array.isArray(raw.Keys) ? raw.Keys.map(String) : [],
    condition: String(raw.Condition ?? ''),
    name: raw.Name != null ? String(raw.Name) : undefined,
    description: raw.Description != null ? String(raw.Description) : undefined,
    parts: parseRatio(raw.Parts),
    granules: parseRatio(raw.Granules),
  };
}

/** Parse "2/5" into { selected: 2, total: 5 } */
export function parseRatio(val: unknown): { selected: number; total: number } | null {
  if (val == null) return null;
  const str = String(val);
  const match = str.match(/^(\d+)\s*\/\s*(\d+)$/);
  if (!match) return null;
  return { selected: Number(match[1]), total: Number(match[2]) };
}
