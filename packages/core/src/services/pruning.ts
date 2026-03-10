/**
 * Pruning effectiveness calculation for ClickHouse queries.
 *
 * ClickHouse prunes data in two stages:
 *   1. **Partition pruning** — eliminates entire parts whose partition key doesn't match the WHERE clause.
 *      `SelectedParts` / `SelectedPartsTotal` from ProfileEvents.
 *   2. **Primary key pruning** — within surviving parts, eliminates granules (marks) whose primary key
 *      range doesn't overlap the WHERE clause.
 *      `SelectedMarks` / `SelectedMarksTotal` from ProfileEvents.
 *
 * Crucially, `SelectedMarksTotal` only counts marks in the **surviving** parts — not all parts.
 * So marks-only pruning underestimates effectiveness when partition pruning already eliminated most parts.
 *
 * The combined formula multiplies survival rates:
 *   `combinedPruned = 1 - (parts_survival × marks_survival)`
 *
 * This correctly weights parts more heavily: pruning a part with N marks eliminates all N marks at once.
 *
 * Example (from a real query):
 *   - 75 parts total, 1 survives partition pruning → partsSurvival = 1/75
 *   - Surviving part has 1 mark, 1 selected → marksSurvival = 1/1
 *   - Combined: 1 - (1/75)(1/1) = 98.7% pruned → Excellent
 *   - Marks-only would say 0% pruned → Poor (wrong!)
 */

export interface PruningInput {
  /** Parts selected after partition pruning (ProfileEvents.SelectedParts) */
  selectedParts: number;
  /** Total parts considered before pruning (ProfileEvents.SelectedPartsTotal) */
  totalParts: number;
  /** Marks selected after primary key pruning (ProfileEvents.SelectedMarks) */
  selectedMarks: number;
  /** Total marks in surviving parts (ProfileEvents.SelectedMarksTotal) */
  totalMarks: number;
}

export type PruningSeverity = 'excellent' | 'good' | 'fair' | 'poor' | 'none';

export interface PruningResult {
  /** Combined pruning percentage (0-100). Accounts for both partition and primary key stages. */
  combinedPrunedPct: number;
  /** Parts-only pruning percentage (0-100). How much data partition pruning eliminated. */
  partsPrunedPct: number;
  /** Marks-only pruning percentage (0-100). How much data primary key pruning eliminated within surviving parts. */
  marksPrunedPct: number;
  /** Fraction of parts that survived partition pruning (0-1). */
  partsSurvival: number;
  /** Fraction of marks that survived primary key pruning within surviving parts (0-1). */
  marksSurvival: number;
  /** Whether any pruning data was available at all. */
  hasData: boolean;
  /** Severity rating based on combined pruning. */
  severity: PruningSeverity;
  /** Human-readable label: Excellent, Good, Fair, Poor, or N/A. */
  label: string;
}

/**
 * Severity thresholds for combined pruning percentage.
 * These are intentionally simple and conservative:
 *   >=90% → Excellent (the index is doing its job well)
 *   >=70% → Good      (reasonable, but there may be room to improve)
 *   >=50% → Fair      (WHERE likely doesn't fully align with keys)
 *   <50%  → Poor      (near full scan territory)
 */
const THRESHOLDS: Array<{ min: number; severity: PruningSeverity; label: string }> = [
  { min: 90, severity: 'excellent', label: 'Excellent' },
  { min: 70, severity: 'good',      label: 'Good' },
  { min: 50, severity: 'fair',      label: 'Fair' },
  { min: 0,  severity: 'poor',      label: 'Poor' },
];

/**
 * Calculate combined pruning effectiveness from parts and marks profile events.
 *
 * @param input - The four ProfileEvent counters. All values should be >= 0.
 * @returns Pruning analysis result with breakdown and severity.
 */
export function calculatePruning(input: PruningInput): PruningResult {
  const { selectedParts, totalParts, selectedMarks, totalMarks } = input;

  const hasPartsData = totalParts > 0;
  const hasMarksData = totalMarks > 0;
  const hasData = hasPartsData || hasMarksData;

  const partsSurvival = hasPartsData ? selectedParts / totalParts : 1;
  const marksSurvival = hasMarksData ? selectedMarks / totalMarks : 1;

  const partsPrunedPct = hasPartsData ? (1 - partsSurvival) * 100 : 0;
  const marksPrunedPct = hasMarksData ? (1 - marksSurvival) * 100 : 0;
  const combinedPrunedPct = (1 - partsSurvival * marksSurvival) * 100;

  let severity: PruningSeverity = 'none';
  let label = 'N/A';

  if (hasData) {
    const match = THRESHOLDS.find(t => combinedPrunedPct >= t.min) ?? THRESHOLDS[THRESHOLDS.length - 1];
    severity = match.severity;
    label = match.label;
  }

  return {
    combinedPrunedPct,
    partsPrunedPct,
    marksPrunedPct,
    partsSurvival,
    marksSurvival,
    hasData,
    severity,
    label,
  };
}

/**
 * Build a human-readable detail string showing where pruning came from.
 *
 * Omits the marks breakdown when totalMarks <= 1 (nothing meaningful to prune at mark level).
 * Omits the parts breakdown when totalParts is 0 (no partition data).
 */
export function formatPruningDetail(input: PruningInput, result: PruningResult): string {
  if (!result.hasData) return 'No pruning data available for this query.';

  const parts: string[] = [];

  if (input.totalParts > 0) {
    parts.push(`${result.partsPrunedPct.toFixed(1)}% by partition key (${input.selectedParts}/${input.totalParts} parts)`);
  }
  if (input.totalMarks > 1) {
    parts.push(`${result.marksPrunedPct.toFixed(1)}% by primary key (${input.selectedMarks}/${input.totalMarks} marks)`);
  }

  const breakdown = parts.join(', ');
  return breakdown
    ? `${result.combinedPrunedPct.toFixed(1)}% overall — ${breakdown}.`
    : `${result.combinedPrunedPct.toFixed(1)}% overall pruning.`;
}
