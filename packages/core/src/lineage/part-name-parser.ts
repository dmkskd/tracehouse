/**
 * Part name parsing utilities for ClickHouse parts.
 * 
 * ClickHouse part names follow these patterns:
 * - Regular: `partition_minBlock_maxBlock_level` (e.g., `202602_1_100_3`)
 * - Mutated: `partition_minBlock_maxBlock_level_mutationVersion` (e.g., `202602_1_100_3_19118`)
 * 
 * The partition can contain underscores (e.g., `all`, `202602`, `2026_02`).
 */

export interface ParsedPartName {
  /** Original part name */
  name: string;
  /** Partition ID (may contain underscores) */
  partition: string;
  /** Minimum block number */
  minBlock: number;
  /** Maximum block number */
  maxBlock: number;
  /** Merge level (0 = L0/original part) */
  level: number;
  /** Mutation version (undefined if not mutated) */
  mutationVersion?: number;
  /** Whether this part was created by a mutation */
  isMutated: boolean;
}

/**
 * Parse a ClickHouse part name into its components.
 * 
 * @param name - The part name (e.g., `202602_1_100_3` or `202602_1_100_3_19118`)
 * @returns Parsed components or null if invalid
 */
export function parsePartName(name: string): ParsedPartName | null {
  const segments = name.split('_');
  if (segments.length < 4) return null;
  
  // Check if this is a mutated part (5+ segments, last 4 are numeric)
  if (segments.length >= 5) {
    const last4 = segments.slice(-4).map(s => parseInt(s, 10));
    if (last4.every(n => !isNaN(n))) {
      // Mutated part: partition_minBlock_maxBlock_level_mutationVersion
      const [minBlock, maxBlock, level, mutationVersion] = last4;
      const partition = segments.slice(0, -4).join('_');
      return {
        name,
        partition,
        minBlock,
        maxBlock,
        level,
        mutationVersion,
        isMutated: true,
      };
    }
  }
  
  // Regular part: partition_minBlock_maxBlock_level
  const last3 = segments.slice(-3).map(s => parseInt(s, 10));
  if (last3.every(n => !isNaN(n))) {
    const [minBlock, maxBlock, level] = last3;
    const partition = segments.slice(0, -3).join('_');
    return {
      name,
      partition,
      minBlock,
      maxBlock,
      level,
      isMutated: false,
    };
  }
  
  return null;
}

/**
 * Extract the merge level from a ClickHouse part name.
 * 
 * @param name - The part name
 * @returns The merge level (0 for L0 parts, higher for merged parts)
 */
export function getLevelFromName(name: string): number {
  const parsed = parsePartName(name);
  return parsed?.level ?? 0;
}

/**
 * Check if a part name represents a mutated part.
 * 
 * @param name - The part name
 * @returns true if the part was created by a mutation
 */
export function isMutatedPart(name: string): boolean {
  const parsed = parsePartName(name);
  return parsed?.isMutated ?? false;
}
