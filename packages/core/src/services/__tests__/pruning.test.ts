import { describe, it, expect } from 'vitest';
import { calculatePruning, formatPruningDetail, type PruningInput } from '../pruning.js';

// ─── calculatePruning — combined formula ─────────────────────────────────────
//
// The core insight: ClickHouse prunes in two stages and `SelectedMarksTotal`
// is scoped to surviving parts. So we must multiply survival rates:
//
//   combinedPruned = 1 - (selectedParts/totalParts) × (selectedMarks/totalMarks)
//
// This correctly weights parts more than marks: pruning 1 part eliminates
// all its marks at once, while pruning 1 mark only eliminates 1 granule.

describe('calculatePruning', { tags: ['storage'] }, () => {

  // ── Excellent (≥90%) ─────────────────────────────────────────────────────

  describe('Excellent — ≥90% combined pruning', () => {
    it('great partition pruning, no mark pruning (the 1/75 parts bug)', () => {
      // This was the original bug: 1/75 parts selected, 1/1 marks.
      // Marks-only would say 0% pruned → Poor.
      // Combined: 1 - (1/75)(1/1) = 98.7% → Excellent.
      const r = calculatePruning({ selectedParts: 1, totalParts: 75, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(98.67, 1);
      expect(r.partsPrunedPct).toBeCloseTo(98.67, 1);
      expect(r.marksPrunedPct).toBe(0);
      expect(r.severity).toBe('excellent');
      expect(r.label).toBe('Excellent');
    });

    it('great mark pruning, no partition pruning', () => {
      // All 10 parts survive, but PK eliminates 95% of marks.
      const r = calculatePruning({ selectedParts: 10, totalParts: 10, selectedMarks: 5, totalMarks: 100 });
      expect(r.combinedPrunedPct).toBeCloseTo(95, 1);
      expect(r.partsPrunedPct).toBe(0);
      expect(r.marksPrunedPct).toBe(95);
      expect(r.severity).toBe('excellent');
    });

    it('both stages contribute to reach ≥90%', () => {
      // Parts: 2/10 survive (80% pruned), Marks: 5/10 survive (50% pruned)
      // Combined: 1 - (2/10)(5/10) = 1 - 0.1 = 90%
      const r = calculatePruning({ selectedParts: 2, totalParts: 10, selectedMarks: 5, totalMarks: 10 });
      expect(r.combinedPrunedPct).toBeCloseTo(90, 1);
      expect(r.severity).toBe('excellent');
    });

    it('single part out of thousands → near-perfect pruning', () => {
      const r = calculatePruning({ selectedParts: 1, totalParts: 1000, selectedMarks: 50, totalMarks: 50 });
      expect(r.combinedPrunedPct).toBeCloseTo(99.9, 1);
      expect(r.severity).toBe('excellent');
    });

    it('extreme mark pruning compensates for no partition pruning', () => {
      const r = calculatePruning({ selectedParts: 100, totalParts: 100, selectedMarks: 1, totalMarks: 500 });
      expect(r.combinedPrunedPct).toBeCloseTo(99.8, 1);
      expect(r.severity).toBe('excellent');
    });
  });

  // ── Good (≥70%, <90%) ───────────────────────────────────────────────────

  describe('Good — ≥70%, <90% combined pruning', () => {
    it('moderate partition + moderate mark pruning', () => {
      // Parts: 5/10 (50%), Marks: 10/20 (50%)
      // Combined: 1 - 0.5 * 0.5 = 75%
      const r = calculatePruning({ selectedParts: 5, totalParts: 10, selectedMarks: 10, totalMarks: 20 });
      expect(r.combinedPrunedPct).toBeCloseTo(75, 1);
      expect(r.severity).toBe('good');
    });

    it('partition pruning alone at 80%', () => {
      // 2/10 parts, all marks in surviving parts selected
      const r = calculatePruning({ selectedParts: 2, totalParts: 10, selectedMarks: 20, totalMarks: 20 });
      expect(r.combinedPrunedPct).toBeCloseTo(80, 1);
      expect(r.severity).toBe('good');
    });

    it('mark pruning alone at 75%', () => {
      const r = calculatePruning({ selectedParts: 5, totalParts: 5, selectedMarks: 25, totalMarks: 100 });
      expect(r.combinedPrunedPct).toBeCloseTo(75, 1);
      expect(r.severity).toBe('good');
    });
  });

  // ── Fair (≥50%, <70%) ───────────────────────────────────────────────────

  describe('Fair — ≥50%, <70% combined pruning', () => {
    it('50% partition + 80% marks survival = 60% combined', () => {
      // Parts: 5/10 survive (50% pruned), Marks: 8/10 survive (20% pruned)
      // Combined: 1 - 0.5 * 0.8 = 60%
      const r = calculatePruning({ selectedParts: 5, totalParts: 10, selectedMarks: 8, totalMarks: 10 });
      expect(r.combinedPrunedPct).toBeCloseTo(60, 1);
      expect(r.severity).toBe('fair');
    });

    it('mark pruning alone at 55%', () => {
      const r = calculatePruning({ selectedParts: 10, totalParts: 10, selectedMarks: 45, totalMarks: 100 });
      expect(r.combinedPrunedPct).toBeCloseTo(55, 1);
      expect(r.severity).toBe('fair');
    });
  });

  // ── Poor (<50%) ─────────────────────────────────────────────────────────

  describe('Poor — <50% combined pruning', () => {
    it('near full scan', () => {
      // Parts: 9/10, Marks: 95/100
      // Combined: 1 - 0.9 * 0.95 = 14.5%
      const r = calculatePruning({ selectedParts: 9, totalParts: 10, selectedMarks: 95, totalMarks: 100 });
      expect(r.combinedPrunedPct).toBeCloseTo(14.5, 1);
      expect(r.severity).toBe('poor');
    });

    it('complete full scan — 0% pruned', () => {
      const r = calculatePruning({ selectedParts: 10, totalParts: 10, selectedMarks: 100, totalMarks: 100 });
      expect(r.combinedPrunedPct).toBe(0);
      expect(r.partsPrunedPct).toBe(0);
      expect(r.marksPrunedPct).toBe(0);
      expect(r.severity).toBe('poor');
    });

    it('single part table, all marks read', () => {
      const r = calculatePruning({ selectedParts: 1, totalParts: 1, selectedMarks: 50, totalMarks: 50 });
      expect(r.combinedPrunedPct).toBe(0);
      expect(r.severity).toBe('poor');
    });

    it('slight mark pruning on all parts', () => {
      // 10% mark pruning, no partition pruning
      const r = calculatePruning({ selectedParts: 5, totalParts: 5, selectedMarks: 90, totalMarks: 100 });
      expect(r.combinedPrunedPct).toBeCloseTo(10, 1);
      expect(r.severity).toBe('poor');
    });
  });

  // ── N/A — no data ───────────────────────────────────────────────────────

  describe('N/A — no pruning data', () => {
    it('all zeros', () => {
      const r = calculatePruning({ selectedParts: 0, totalParts: 0, selectedMarks: 0, totalMarks: 0 });
      expect(r.hasData).toBe(false);
      expect(r.severity).toBe('none');
      expect(r.label).toBe('N/A');
      expect(r.combinedPrunedPct).toBe(0);
    });
  });

  // ── Partial data (only parts or only marks) ─────────────────────────────

  describe('partial data — only one stage available', () => {
    it('only parts data, no marks → uses parts-only', () => {
      const r = calculatePruning({ selectedParts: 1, totalParts: 100, selectedMarks: 0, totalMarks: 0 });
      expect(r.hasData).toBe(true);
      expect(r.combinedPrunedPct).toBeCloseTo(99, 1);
      expect(r.marksSurvival).toBe(1); // defaults to 1 (no data = assume all survive)
      expect(r.severity).toBe('excellent');
    });

    it('only marks data, no parts → uses marks-only', () => {
      const r = calculatePruning({ selectedParts: 0, totalParts: 0, selectedMarks: 10, totalMarks: 200 });
      expect(r.hasData).toBe(true);
      expect(r.combinedPrunedPct).toBeCloseTo(95, 1);
      expect(r.partsSurvival).toBe(1); // defaults to 1 (no data = assume all survive)
      expect(r.severity).toBe('excellent');
    });
  });

  // ── Boundary conditions ─────────────────────────────────────────────────

  describe('boundary conditions', () => {
    it('exactly 90% → Excellent (inclusive)', () => {
      // 1/10 parts, 1/1 marks → 90%
      const r = calculatePruning({ selectedParts: 1, totalParts: 10, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(90, 1);
      expect(r.severity).toBe('excellent');
    });

    it('just below 90% → Good', () => {
      // Need combined ~89.x%
      // 11/100 parts, all marks → 89%
      const r = calculatePruning({ selectedParts: 11, totalParts: 100, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(89, 0);
      expect(r.severity).toBe('good');
    });

    it('exactly 70% → Good (inclusive)', () => {
      // 3/10 parts, all marks → 70%
      const r = calculatePruning({ selectedParts: 3, totalParts: 10, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(70, 1);
      expect(r.severity).toBe('good');
    });

    it('just below 70% → Fair', () => {
      const r = calculatePruning({ selectedParts: 31, totalParts: 100, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(69, 0);
      expect(r.severity).toBe('fair');
    });

    it('exactly 50% → Fair (inclusive)', () => {
      const r = calculatePruning({ selectedParts: 5, totalParts: 10, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(50, 1);
      expect(r.severity).toBe('fair');
    });

    it('just below 50% → Poor', () => {
      const r = calculatePruning({ selectedParts: 51, totalParts: 100, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeCloseTo(49, 0);
      expect(r.severity).toBe('poor');
    });

    it('selectedParts > totalParts (shouldn\'t happen but be safe)', () => {
      // Defensive: if ClickHouse emits weird data, survival > 1 → negative pruning
      const r = calculatePruning({ selectedParts: 20, totalParts: 10, selectedMarks: 1, totalMarks: 1 });
      expect(r.combinedPrunedPct).toBeLessThan(0);
      expect(r.severity).toBe('poor');
    });
  });

  // ── Mathematical properties ─────────────────────────────────────────────

  describe('mathematical properties of the formula', () => {
    it('multiplicative: pruning a part with N marks eliminates all N marks', () => {
      // 10 parts with ~10 marks each = 100 total marks
      // Prune 9 parts → 1 part with 10 marks → read 10/100 = 10% of data
      // This is the same as combined: 1 - (1/10)(10/10) = 90%
      const r = calculatePruning({ selectedParts: 1, totalParts: 10, selectedMarks: 10, totalMarks: 10 });
      expect(r.combinedPrunedPct).toBeCloseTo(90, 1);
    });

    it('commutative in effect: same result regardless of which stage prunes', () => {
      // Scenario A: partition prunes 90%, marks prune 0% → 90% total
      const a = calculatePruning({ selectedParts: 1, totalParts: 10, selectedMarks: 5, totalMarks: 5 });
      // Scenario B: partition prunes 0%, marks prune 90% → 90% total
      const b = calculatePruning({ selectedParts: 10, totalParts: 10, selectedMarks: 5, totalMarks: 50 });
      expect(a.combinedPrunedPct).toBeCloseTo(b.combinedPrunedPct, 1);
    });

    it('two-stage pruning always ≥ either stage alone', () => {
      const input: PruningInput = { selectedParts: 3, totalParts: 10, selectedMarks: 20, totalMarks: 50 };
      const r = calculatePruning(input);
      expect(r.combinedPrunedPct).toBeGreaterThanOrEqual(r.partsPrunedPct);
      expect(r.combinedPrunedPct).toBeGreaterThanOrEqual(r.marksPrunedPct);
    });

    it('100% partition pruning (0 parts survive) → 100% combined', () => {
      // This edge case shouldn't happen (no query reads 0 parts), but mathematically:
      const r = calculatePruning({ selectedParts: 0, totalParts: 10, selectedMarks: 0, totalMarks: 0 });
      expect(r.combinedPrunedPct).toBeCloseTo(100, 1);
    });
  });

  // ── Real-world scenarios ────────────────────────────────────────────────

  describe('real-world scenarios', () => {
    it('date-partitioned table, query filters on date → great partition pruning', () => {
      // Table with daily partitions for 1 year (365 parts), query hits 1 day
      // The selected part has 200 marks, all read (no PK pruning within the day)
      const r = calculatePruning({ selectedParts: 1, totalParts: 365, selectedMarks: 200, totalMarks: 200 });
      expect(r.combinedPrunedPct).toBeCloseTo(99.7, 1);
      expect(r.severity).toBe('excellent');
    });

    it('date-partitioned table, query hits a week → good partition pruning', () => {
      const r = calculatePruning({ selectedParts: 7, totalParts: 365, selectedMarks: 1400, totalMarks: 1400 });
      expect(r.combinedPrunedPct).toBeCloseTo(98.1, 1);
      expect(r.severity).toBe('excellent');
    });

    it('no partition key, but good ordering key → mark pruning only', () => {
      // Single partition (1 part), but PK prunes well
      const r = calculatePruning({ selectedParts: 1, totalParts: 1, selectedMarks: 50, totalMarks: 10000 });
      expect(r.combinedPrunedPct).toBeCloseTo(99.5, 1);
      expect(r.severity).toBe('excellent');
    });

    it('query with no WHERE clause → full scan', () => {
      const r = calculatePruning({ selectedParts: 100, totalParts: 100, selectedMarks: 50000, totalMarks: 50000 });
      expect(r.combinedPrunedPct).toBe(0);
      expect(r.severity).toBe('poor');
    });

    it('WHERE on non-key column → near full scan with generic exclusion', () => {
      // ClickHouse generic exclusion might prune a few marks
      const r = calculatePruning({ selectedParts: 100, totalParts: 100, selectedMarks: 48000, totalMarks: 50000 });
      expect(r.combinedPrunedPct).toBeCloseTo(4, 0);
      expect(r.severity).toBe('poor');
    });

    it('WHERE filters on partition key AND ordering key → both stages help', () => {
      // Partition: date prunes 30/365 days
      // PK: user_id narrows within those days
      const r = calculatePruning({ selectedParts: 30, totalParts: 365, selectedMarks: 500, totalMarks: 6000 });
      expect(r.combinedPrunedPct).toBeCloseTo(99.3, 1);
      expect(r.severity).toBe('excellent');
    });

    it('small table with few parts — low absolute numbers but good ratio', () => {
      const r = calculatePruning({ selectedParts: 1, totalParts: 3, selectedMarks: 2, totalMarks: 10 });
      // 1 - (1/3)(2/10) = 1 - 0.0667 = 93.3%
      expect(r.combinedPrunedPct).toBeCloseTo(93.3, 1);
      expect(r.severity).toBe('excellent');
    });
  });
});

// ─── formatPruningDetail ─────────────────────────────────────────────────────

describe('formatPruningDetail', { tags: ['storage'] }, () => {
  it('shows both stages when both have meaningful data', () => {
    const input: PruningInput = { selectedParts: 5, totalParts: 10, selectedMarks: 10, totalMarks: 20 };
    const result = calculatePruning(input);
    const detail = formatPruningDetail(input, result);
    expect(detail).toContain('75.0% overall');
    expect(detail).toContain('50.0% by partition key (5/10 parts)');
    expect(detail).toContain('50.0% by primary key (10/20 marks)');
  });

  it('omits marks breakdown when totalMarks is 1 (single mark = nothing to prune)', () => {
    const input: PruningInput = { selectedParts: 1, totalParts: 75, selectedMarks: 1, totalMarks: 1 };
    const result = calculatePruning(input);
    const detail = formatPruningDetail(input, result);
    expect(detail).toContain('98.7% overall');
    expect(detail).toContain('partition key (1/75 parts)');
    expect(detail).not.toContain('primary key');
  });

  it('omits parts breakdown when totalParts is 0', () => {
    const input: PruningInput = { selectedParts: 0, totalParts: 0, selectedMarks: 5, totalMarks: 100 };
    const result = calculatePruning(input);
    const detail = formatPruningDetail(input, result);
    expect(detail).toContain('95.0% overall');
    expect(detail).not.toContain('partition key');
    expect(detail).toContain('primary key (5/100 marks)');
  });

  it('returns N/A message when no data', () => {
    const input: PruningInput = { selectedParts: 0, totalParts: 0, selectedMarks: 0, totalMarks: 0 };
    const result = calculatePruning(input);
    const detail = formatPruningDetail(input, result);
    expect(detail).toBe('No pruning data available for this query.');
  });

  it('shows partition-only when marks data exists but is trivial (1 total mark)', () => {
    const input: PruningInput = { selectedParts: 2, totalParts: 20, selectedMarks: 1, totalMarks: 1 };
    const result = calculatePruning(input);
    const detail = formatPruningDetail(input, result);
    expect(detail).toContain('partition key (2/20 parts)');
    expect(detail).not.toContain('primary key');
  });

  it('shows 0.0% partition pruning when all parts survive', () => {
    const input: PruningInput = { selectedParts: 10, totalParts: 10, selectedMarks: 5, totalMarks: 100 };
    const result = calculatePruning(input);
    const detail = formatPruningDetail(input, result);
    expect(detail).toContain('0.0% by partition key (10/10 parts)');
    expect(detail).toContain('95.0% by primary key (5/100 marks)');
  });
});
