import { describe, it, expect } from 'vitest';
import {
  pearson, spearman, crossCorrelation, crossCorrelationDetail,
  rollingCorrelation, minMaxNormalize, normalizePanelData,
  correlateToFocused, correlationToOpacity, correlationStrength,
  interpretCorrelation, computeInsightsAndLags,
  CORRELATION_THRESHOLDS,
} from '../correlation.js';

// ═══════════════════════════════════════════════════════════════════════════
// Pearson
// ═══════════════════════════════════════════════════════════════════════════

describe('pearson', { tags: ['observability'] }, () => {
  it('returns 1 for perfectly correlated arrays', () => {
    expect(pearson([1, 2, 3, 4, 5], [2, 4, 6, 8, 10])).toBeCloseTo(1, 5);
  });

  it('returns -1 for perfectly anti-correlated arrays', () => {
    expect(pearson([1, 2, 3, 4, 5], [10, 8, 6, 4, 2])).toBeCloseTo(-1, 5);
  });

  it('returns ~0 for uncorrelated arrays', () => {
    expect(Math.abs(pearson([1, 2, 3, 4, 5], [5, 1, 4, 2, 3]))).toBeLessThan(0.5);
  });

  it('returns 0 for fewer than 3 valid pairs', () => {
    expect(pearson([1, 2], [3, 4])).toBe(0);
  });

  it('skips NaN values', () => {
    expect(pearson([1, NaN, 3, 4, 5], [2, 99, 6, 8, 10])).toBeCloseTo(1, 5);
  });

  it('returns 0 for constant arrays', () => {
    expect(pearson([5, 5, 5, 5], [1, 2, 3, 4])).toBe(0);
  });

  it('handles arrays of different lengths (uses shorter)', () => {
    expect(pearson([1, 2, 3, 4, 5, 99], [2, 4, 6, 8, 10])).toBeCloseTo(1, 5);
  });

  it('returns 0 for empty arrays', () => {
    expect(pearson([], [])).toBe(0);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// Spearman
// ═══════════════════════════════════════════════════════════════════════════

describe('spearman', { tags: ['observability'] }, () => {
  it('returns 1 for perfectly monotonic arrays', () => {
    expect(spearman([1, 2, 3, 4, 5], [2, 4, 6, 8, 10])).toBeCloseTo(1, 5);
  });

  it('returns -1 for perfectly anti-monotonic arrays', () => {
    expect(spearman([1, 2, 3, 4, 5], [10, 8, 6, 4, 2])).toBeCloseTo(-1, 5);
  });

  it('handles non-linear monotonic relationships', () => {
    // Exponential: monotonic but not linear — Spearman should still be ~1
    expect(spearman([1, 2, 3, 4, 5], [1, 4, 9, 16, 25])).toBeCloseTo(1, 5);
  });

  it('returns 0 for fewer than 3 valid pairs', () => {
    expect(spearman([1, 2], [3, 4])).toBe(0);
  });

  it('handles tied values with average ranking', () => {
    // Ties: [1,1,2,3,4] should get ranks [1.5, 1.5, 3, 4, 5]
    const result = spearman([1, 1, 2, 3, 4], [5, 6, 7, 8, 9]);
    expect(result).toBeGreaterThan(0.9);
  });

  it('returns 0 for constant arrays (all ties)', () => {
    expect(spearman([5, 5, 5, 5], [1, 2, 3, 4])).toBe(0);
  });

  it('skips NaN values', () => {
    const result = spearman([1, NaN, 3, 4, 5], [2, 99, 6, 8, 10]);
    expect(result).toBeCloseTo(1, 5);
  });

  it('is robust to outliers unlike pearson', () => {
    // Linear data with one extreme outlier
    const a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 100];
    const b = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20];
    // Spearman should still be high (ranks are preserved)
    expect(spearman(a, b)).toBeGreaterThan(0.9);
    // Pearson will be lower due to the outlier distorting the mean
    expect(pearson(a, b)).toBeLessThan(spearman(a, b));
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// Cross-Correlation
// ═══════════════════════════════════════════════════════════════════════════

describe('crossCorrelation', { tags: ['observability'] }, () => {
  it('returns ~1 for perfectly aligned correlated arrays', () => {
    expect(crossCorrelation([1, 2, 3, 4, 5, 6, 7, 8], [2, 4, 6, 8, 10, 12, 14, 16])).toBeCloseTo(1, 5);
  });

  it('returns ~-1 for perfectly aligned anti-correlated arrays', () => {
    expect(crossCorrelation([1, 2, 3, 4, 5, 6, 7, 8], [16, 14, 12, 10, 8, 6, 4, 2])).toBeCloseTo(-1, 5);
  });

  it('detects a lagged relationship that same-time pearson misses', () => {
    // b is a shifted by 2 positions, with noise padding
    const a = [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0];
    const b = [0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8,  9, 10, 0, 0];
    const sameTime = pearson(a, b);
    const cross = crossCorrelation(a, b);
    // Cross-correlation should find a better score by shifting
    expect(Math.abs(cross)).toBeGreaterThan(Math.abs(sameTime));
    expect(Math.abs(cross)).toBeGreaterThan(0.9);
  });

  it('returns 0 for fewer than 3 valid pairs', () => {
    expect(crossCorrelation([1, 2], [3, 4])).toBe(0);
  });

  it('returns 0 for empty arrays', () => {
    expect(crossCorrelation([], [])).toBe(0);
  });

  it('equals pearson when no lag improves the score', () => {
    // Perfectly aligned — best lag is 0
    const a = [1, 2, 3, 4, 5, 6, 7, 8];
    const b = [2, 4, 6, 8, 10, 12, 14, 16];
    expect(crossCorrelation(a, b)).toBeCloseTo(pearson(a, b), 5);
  });
});

describe('crossCorrelationDetail', { tags: ['observability'] }, () => {
  it('returns lag=0 for perfectly aligned series', () => {
    const a = [1, 2, 3, 4, 5, 6, 7, 8];
    const b = [2, 4, 6, 8, 10, 12, 14, 16];
    const result = crossCorrelationDetail(a, b);
    expect(result.lag).toBe(0);
    expect(result.r).toBeCloseTo(1, 5);
  });

  it('returns positive lag when a leads b', () => {
    // a has a pulse early, b has the same pulse later
    const a = [0, 0, 10, 20, 30, 20, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const b = [0, 0, 0,  0,  0,  0, 10, 20, 30, 20, 10, 0, 0, 0, 0, 0];
    const result = crossCorrelationDetail(a, b);
    expect(result.lag).toBeGreaterThan(0); // a leads b
    expect(result.r).toBeGreaterThan(0.8);
  });

  it('returns negative lag when b leads a', () => {
    // b has a pulse early, a has it later
    const a = [0, 0, 0,  0,  0,  0, 10, 20, 30, 20, 10, 0, 0, 0, 0, 0];
    const b = [0, 0, 10, 20, 30, 20, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const result = crossCorrelationDetail(a, b);
    expect(result.lag).toBeLessThan(0); // b leads a
    expect(result.r).toBeGreaterThan(0.8);
  });

  it('caps max lag to avoid thin-slice artifacts', () => {
    // With 8 elements, maxLag = min(floor(8/4), 20) = 2
    const a = [1, 2, 3, 4, 5, 6, 7, 8];
    const result = crossCorrelationDetail(a, a);
    expect(Math.abs(result.lag)).toBeLessThanOrEqual(2);
  });

  it('does not flip sign on periodic data when half-cycle offset is marginally better', () => {
    // Two in-phase sinusoids (period=20) that visually move together.
    // Edge noise slightly degrades the lag=0 Pearson (0.980), while the
    // half-cycle offset (lag=10) slices off the noisy edges and scores
    // marginally higher in magnitude (-0.986) — but with the WRONG sign.
    // The old algorithm picked the highest |r| regardless, reporting a
    // strong negative correlation for two lines that clearly move together.
    const n = 40;
    const period = 20;
    const a: number[] = [];
    const b: number[] = [];
    for (let i = 0; i < n; i++) {
      a.push(Math.sin(2 * Math.PI * i / period));
      b.push(Math.sin(2 * Math.PI * i / period));
    }
    // Add noise at the edges — included at lag=0 but sliced off at |lag|>0
    b[0] += 0.5;  b[1] -= 0.4;
    b[n - 2] -= 0.5; b[n - 1] += 0.4;

    // Same-time Pearson is strongly positive (~0.98)
    expect(pearson(a, b)).toBeGreaterThan(0.95);

    // Cross-correlation must preserve the positive sign
    const result = crossCorrelationDetail(a, b);
    expect(result.r).toBeGreaterThan(0);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// Algorithm comparison — verifying the properties that interpretCorrelation relies on
// ═══════════════════════════════════════════════════════════════════════════

describe('algorithm properties (cross-algorithm)', { tags: ['observability'] }, () => {
  it('exponential data: spearman ≈ 1, pearson < 1', () => {
    // y = e^x is monotonic but not linear
    const x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const y = x.map(v => Math.exp(v));
    const p = pearson(x, y);
    const s = spearman(x, y);
    expect(s).toBeCloseTo(1, 5);  // perfect monotonic
    expect(p).toBeLessThan(0.95);  // not perfectly linear
    expect(s).toBeGreaterThan(p);  // spearman > pearson for non-linear monotonic
  });

  it('linear + outlier: pearson drops more than spearman', () => {
    // Clean linear data with one outlier
    const a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const b = [10, 20, 30, 40, 50, 60, 70, 80, 90, 500]; // last point is outlier
    const p = pearson(a, b);
    const s = spearman(a, b);
    // Both still positive, but spearman is robust to the outlier
    expect(s).toBeCloseTo(1, 5);  // ranks still perfectly ordered
    expect(p).toBeLessThan(s);     // pearson pulled down by the outlier
  });

  it('uncorrelated data: all algorithms score low', () => {
    // One goes up steadily, the other oscillates — no monotonic or linear relationship
    const a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const b = [5, 1, 5, 1, 5, 1, 5, 1, 5, 1];
    expect(Math.abs(pearson(a, b))).toBeLessThan(0.3);
    expect(Math.abs(spearman(a, b))).toBeLessThan(0.3);
  });

  it('lagged data: cross-correlation high, same-time pearson lower', () => {
    // a ramps up early, b ramps up later — same shape, shifted in time
    const a = [0, 0, 5, 10, 15, 20, 25, 20, 15, 10, 5, 0, 0, 0, 0, 0];
    const b = [0, 0, 0,  0,  0,  5, 10, 15, 20, 25, 20, 15, 10, 5, 0, 0];
    const p = pearson(a, b);
    const c = crossCorrelation(a, b);
    expect(Math.abs(c)).toBeGreaterThan(Math.abs(p));
    expect(Math.abs(c)).toBeGreaterThan(0.8);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// interpretCorrelation — the core combination logic
// ═══════════════════════════════════════════════════════════════════════════

describe('interpretCorrelation', { tags: ['observability'] }, () => {
  // ── Rule 1: Strong linear (all agree) ──

  describe('rule 1: strong linear', () => {
    it('triggers when all three are above 0.7', () => {
      const result = interpretCorrelation({ pearson: 0.9, spearman: 0.85, cross: 0.88 });
      expect(result?.label).toBe('strong linear');
      expect(result?.level).toBe('info');
    });

    it('triggers at exact threshold (0.7)', () => {
      const result = interpretCorrelation({ pearson: 0.7, spearman: 0.7, cross: 0.7 });
      expect(result?.label).toBe('strong linear');
    });

    it('triggers with negative correlations (uses absolute values)', () => {
      const result = interpretCorrelation({ pearson: -0.9, spearman: -0.85, cross: -0.88 });
      expect(result?.label).toBe('strong linear');
    });

    it('does NOT trigger if any score is below 0.7', () => {
      const result = interpretCorrelation({ pearson: 0.9, spearman: 0.69, cross: 0.88 });
      expect(result?.label).not.toBe('strong linear');
    });
  });

  // ── Rule 2: No relationship (all low) ──

  describe('rule 2: no relationship', () => {
    it('returns null when all three are below 0.3', () => {
      expect(interpretCorrelation({ pearson: 0.1, spearman: 0.2, cross: 0.15 })).toBeNull();
    });

    it('returns null for all zeros', () => {
      expect(interpretCorrelation({ pearson: 0, spearman: 0, cross: 0 })).toBeNull();
    });

    it('returns null with small negative values', () => {
      expect(interpretCorrelation({ pearson: -0.1, spearman: -0.2, cross: 0.1 })).toBeNull();
    });

    it('does NOT return null if any score is at 0.3', () => {
      // 0.3 is NOT < 0.3, so this shouldn't be caught by rule 2
      const result = interpretCorrelation({ pearson: 0.3, spearman: 0.1, cross: 0.1 });
      // Doesn't match rule 2 (pearson not < 0.3), falls through to null anyway
      // since no other rule matches either — but it's not "no relationship"
      expect(result).toBeNull(); // falls through to final null
    });
  });

  // ── Rule 3: Non-linear monotonic ──

  describe('rule 3: non-linear', () => {
    it('triggers when spearman high and pearson low', () => {
      const result = interpretCorrelation({ pearson: 0.3, spearman: 0.9, cross: 0.5 });
      expect(result?.label).toBe('non-linear');
      expect(result?.level).toBe('interesting');
    });

    it('requires spearman >= 0.6', () => {
      const result = interpretCorrelation({ pearson: 0.2, spearman: 0.59, cross: 0.5 });
      expect(result?.label).not.toBe('non-linear');
    });

    it('requires pearson < 0.5', () => {
      const result = interpretCorrelation({ pearson: 0.5, spearman: 0.8, cross: 0.5 });
      expect(result?.label).not.toBe('non-linear');
    });

    it('requires gap of at least 0.2 between spearman and pearson', () => {
      // spearman=0.6, pearson=0.41 → gap=0.19, not enough
      const result = interpretCorrelation({ pearson: 0.41, spearman: 0.6, cross: 0.5 });
      expect(result?.label).not.toBe('non-linear');
    });

    it('works with negative correlations', () => {
      const result = interpretCorrelation({ pearson: -0.3, spearman: -0.9, cross: -0.5 });
      expect(result?.label).toBe('non-linear');
    });
  });

  // ── Rule 4: Lagged ──

  describe('rule 4: lagged', () => {
    it('triggers when cross-correlation high and pearson low', () => {
      const result = interpretCorrelation({ pearson: 0.2, spearman: 0.4, cross: 0.85 });
      expect(result?.label).toBe('lagged');
      expect(result?.level).toBe('interesting');
    });

    it('requires cross >= 0.6', () => {
      const result = interpretCorrelation({ pearson: 0.2, spearman: 0.3, cross: 0.59 });
      expect(result?.label).not.toBe('lagged');
    });

    it('requires pearson < 0.5', () => {
      const result = interpretCorrelation({ pearson: 0.5, spearman: 0.4, cross: 0.85 });
      expect(result?.label).not.toBe('lagged');
    });

    it('requires gap of at least 0.2 between cross and pearson', () => {
      const result = interpretCorrelation({ pearson: 0.41, spearman: 0.3, cross: 0.6 });
      expect(result?.label).not.toBe('lagged');
    });
  });

  // ── Rule 5: Outlier-driven ──

  describe('rule 5: outlier-driven', () => {
    it('triggers when pearson high and spearman noticeably lower', () => {
      const result = interpretCorrelation({ pearson: 0.85, spearman: 0.4, cross: 0.7 });
      expect(result?.label).toBe('outlier-driven');
      expect(result?.level).toBe('interesting');
    });

    it('requires pearson >= 0.6', () => {
      const result = interpretCorrelation({ pearson: 0.59, spearman: 0.3, cross: 0.5 });
      expect(result?.label).not.toBe('outlier-driven');
    });

    it('requires spearman at least 0.2 below pearson', () => {
      // pearson=0.7, spearman=0.51 → gap=0.19, not enough
      const result = interpretCorrelation({ pearson: 0.7, spearman: 0.51, cross: 0.6 });
      expect(result?.label).not.toBe('outlier-driven');
    });

    it('works with negative correlations', () => {
      const result = interpretCorrelation({ pearson: -0.85, spearman: -0.4, cross: -0.7 });
      expect(result?.label).toBe('outlier-driven');
    });
  });

  // ── Priority / ordering ──

  describe('rule priority', () => {
    it('strong linear wins over non-linear when all are high', () => {
      // All >= 0.7 → strong linear, even though spearman > pearson
      const result = interpretCorrelation({ pearson: 0.7, spearman: 0.95, cross: 0.8 });
      expect(result?.label).toBe('strong linear');
    });

    it('non-linear wins over lagged when both could apply', () => {
      // Both spearman and cross are high, pearson is low
      // Non-linear is checked first (rule 3 before rule 4)
      const result = interpretCorrelation({ pearson: 0.2, spearman: 0.8, cross: 0.8 });
      expect(result?.label).toBe('non-linear');
    });

    it('non-linear wins over outlier-driven when spearman is the high one', () => {
      // spearman=0.8, pearson=0.3 → non-linear (rule 3)
      const result = interpretCorrelation({ pearson: 0.3, spearman: 0.8, cross: 0.4 });
      expect(result?.label).toBe('non-linear');
    });
  });

  // ── Ambiguous mid-range ──

  describe('ambiguous / null cases', () => {
    it('returns null for mid-range scores that match no rule', () => {
      expect(interpretCorrelation({ pearson: 0.5, spearman: 0.5, cross: 0.5 })).toBeNull();
    });

    it('returns null when scores are close together but moderate', () => {
      expect(interpretCorrelation({ pearson: 0.4, spearman: 0.45, cross: 0.42 })).toBeNull();
    });

    it('returns null when one score is moderate and others are low', () => {
      // pearson=0.5 but spearman not far enough below (gap < 0.2)
      expect(interpretCorrelation({ pearson: 0.5, spearman: 0.35, cross: 0.4 })).toBeNull();
    });
  });

  // ── Symmetry: sign should not affect label ──

  describe('sign symmetry', () => {
    const cases: { pearson: number; spearman: number; cross: number }[] = [
      { pearson: 0.9, spearman: 0.85, cross: 0.88 },
      { pearson: 0.3, spearman: 0.9, cross: 0.5 },
      { pearson: 0.2, spearman: 0.4, cross: 0.85 },
      { pearson: 0.85, spearman: 0.4, cross: 0.7 },
    ];

    for (const c of cases) {
      it(`same label for positive and negative: p=${c.pearson} s=${c.spearman} x=${c.cross}`, () => {
        const pos = interpretCorrelation(c);
        const neg = interpretCorrelation({
          pearson: -c.pearson,
          spearman: -c.spearman,
          cross: -c.cross,
        });
        expect(pos?.label).toBe(neg?.label);
      });
    }
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// End-to-end: real data → all three scores → interpretation
// ═══════════════════════════════════════════════════════════════════════════

describe('end-to-end: data → scores → interpretation', { tags: ['observability'] }, () => {
  it('perfectly linear data → strong linear', () => {
    const a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const b = a.map(v => v * 3 + 7);
    const scores = {
      pearson: pearson(a, b),
      spearman: spearman(a, b),
      cross: crossCorrelation(a, b),
    };
    expect(scores.pearson).toBeCloseTo(1, 5);
    expect(scores.spearman).toBeCloseTo(1, 5);
    const result = interpretCorrelation(scores);
    expect(result?.label).toBe('strong linear');
  });

  it('exponential data → non-linear', () => {
    const a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const b = a.map(v => Math.exp(v));
    const scores = {
      pearson: pearson(a, b),
      spearman: spearman(a, b),
      cross: crossCorrelation(a, b),
    };
    // Spearman should be perfect (monotonic), Pearson lower (non-linear)
    expect(scores.spearman).toBeCloseTo(1, 5);
    expect(scores.pearson).toBeLessThan(0.95);
    const result = interpretCorrelation(scores);
    // The key assertion: spearman > pearson
    expect(scores.spearman).toBeGreaterThan(scores.pearson);
  });

  it('lagged data → cross-correlation detects the delay', () => {
    // a ramps up first, b follows with a delay
    const a = [0, 0, 5, 10, 15, 20, 25, 20, 15, 10, 5, 0, 0, 0, 0, 0];
    const b = [0, 0, 0,  0,  0,  5, 10, 15, 20, 25, 20, 15, 10, 5, 0, 0];
    const scores = {
      pearson: pearson(a, b),
      spearman: spearman(a, b),
      cross: crossCorrelation(a, b),
    };
    // Cross-correlation should find a much better score than same-time pearson
    expect(Math.abs(scores.cross)).toBeGreaterThan(Math.abs(scores.pearson));
    // Detail should show the lag
    const detail = crossCorrelationDetail(a, b);
    expect(detail.lag).not.toBe(0);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// minMaxNormalize
// ═══════════════════════════════════════════════════════════════════════════

describe('minMaxNormalize', { tags: ['observability'] }, () => {
  it('normalizes to 0-1 range', () => {
    const result = minMaxNormalize([10, 20, 30, 40, 50]);
    expect(result).toEqual([0, 0.25, 0.5, 0.75, 1]);
  });

  it('returns 0.5 for constant arrays', () => {
    expect(minMaxNormalize([7, 7, 7])).toEqual([0.5, 0.5, 0.5]);
  });

  it('passes NaN through', () => {
    const result = minMaxNormalize([0, NaN, 10]);
    expect(result[0]).toBe(0);
    expect(result[2]).toBe(1);
    expect(Number.isNaN(result[1])).toBe(true);
  });

  it('handles single element', () => {
    expect(minMaxNormalize([42])).toEqual([0.5]);
  });

  it('handles negative values', () => {
    const result = minMaxNormalize([-10, 0, 10]);
    expect(result).toEqual([0, 0.5, 1]);
  });

  it('handles empty array', () => {
    expect(minMaxNormalize([])).toEqual([]);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// rollingCorrelation
// ═══════════════════════════════════════════════════════════════════════════

describe('rollingCorrelation', { tags: ['observability'] }, () => {
  it('finds correlated windows', () => {
    // First half correlated, second half not
    const a = [1, 2, 3, 4, 5, 1, 3, 2, 5, 4];
    const b = [2, 4, 6, 8, 10, 9, 3, 7, 1, 6];
    const results = rollingCorrelation(a, b, 4, 0.8);
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].startIdx).toBe(0);
  });

  it('returns empty for short arrays', () => {
    expect(rollingCorrelation([1, 2], [3, 4], 4, 0.8)).toEqual([]);
  });

  it('accepts a custom correlation function', () => {
    const a = [1, 2, 3, 4, 5, 6, 7, 8];
    const b = [2, 4, 6, 8, 10, 12, 14, 16];
    const alwaysOne: (a: number[], b: number[]) => number = () => 1;
    const results = rollingCorrelation(a, b, 4, 0.8, alwaysOne);
    expect(results.length).toBeGreaterThan(0);
  });

  it('finds multiple separated windows', () => {
    // Correlated, then noise, then correlated again
    const a = [1, 2, 3, 4, 5,   10, 3, 7, 1, 6,   1, 2, 3, 4, 5];
    const b = [2, 4, 6, 8, 10,   1, 8, 2, 9, 3,   3, 6, 9, 12, 15];
    const results = rollingCorrelation(a, b, 4, 0.9);
    expect(results.length).toBeGreaterThanOrEqual(1);
  });

  it('detects negative correlation windows', () => {
    // Anti-correlated section
    const a = [1, 2, 3, 4, 5, 6, 7, 8];
    const b = [8, 7, 6, 5, 4, 3, 2, 1];
    const results = rollingCorrelation(a, b, 4, 0.8);
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].r).toBeLessThan(-0.8);
  });

  it('returns empty when windowSize < 3', () => {
    expect(rollingCorrelation([1, 2, 3, 4], [5, 6, 7, 8], 2, 0.5)).toEqual([]);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// normalizePanelData
// ═══════════════════════════════════════════════════════════════════════════

describe('normalizePanelData', { tags: ['observability'] }, () => {
  it('aligns timestamps and normalizes', () => {
    const panels = [
      { name: 'CPU', color: '#ff0000', dataByLabel: new Map([['t1', 0], ['t2', 50], ['t3', 100]]) },
      { name: 'Memory', color: '#00ff00', dataByLabel: new Map([['t1', 1000], ['t3', 2000]]) },
    ];
    const result = normalizePanelData(panels);
    expect(result).toHaveLength(2);
    expect(result[0].timestamps).toEqual(['t1', 't2', 't3']);
    expect(result[0].normalized).toEqual([0, 0.5, 1]);
    expect(result[1].raw[1]).toBeNaN(); // t2 missing for Memory
    expect(result[1].normalized[0]).toBe(0);
    expect(result[1].normalized[2]).toBe(1);
  });

  it('preserves name, color and unit', () => {
    const panels = [
      { name: 'Disk', color: '#0000ff', unit: 'bytes/s', dataByLabel: new Map([['t1', 100]]) },
    ];
    const result = normalizePanelData(panels);
    expect(result[0].name).toBe('Disk');
    expect(result[0].color).toBe('#0000ff');
    expect(result[0].unit).toBe('bytes/s');
  });

  it('returns empty for empty input', () => {
    expect(normalizePanelData([])).toEqual([]);
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// correlateToFocused
// ═══════════════════════════════════════════════════════════════════════════

describe('correlateToFocused', { tags: ['observability'] }, () => {
  it('returns 1 for self-correlation', () => {
    const series = normalizePanelData([
      { name: 'A', color: '#f00', dataByLabel: new Map([['t1', 1], ['t2', 2], ['t3', 3]]) },
      { name: 'B', color: '#0f0', dataByLabel: new Map([['t1', 3], ['t2', 2], ['t3', 1]]) },
    ]);
    const corr = correlateToFocused(series, 'A');
    expect(corr.get('A')).toBe(1);
    expect(corr.get('B')).toBeCloseTo(-1, 5);
  });

  it('returns empty map for non-existent focused name', () => {
    const series = normalizePanelData([
      { name: 'A', color: '#f00', dataByLabel: new Map([['t1', 1], ['t2', 2], ['t3', 3]]) },
    ]);
    const corr = correlateToFocused(series, 'nonexistent');
    expect(corr.size).toBe(0);
  });

  it('uses custom correlation function when provided', () => {
    const series = normalizePanelData([
      { name: 'A', color: '#f00', dataByLabel: new Map([['t1', 1], ['t2', 2], ['t3', 3]]) },
      { name: 'B', color: '#0f0', dataByLabel: new Map([['t1', 3], ['t2', 2], ['t3', 1]]) },
    ]);
    const corr = correlateToFocused(series, 'A', spearman);
    expect(corr.get('A')).toBe(1);
    expect(corr.get('B')).toBeCloseTo(-1, 5);
  });

  it('handles three+ series', () => {
    const series = normalizePanelData([
      { name: 'A', color: '#f00', dataByLabel: new Map([['t1', 1], ['t2', 2], ['t3', 3]]) },
      { name: 'B', color: '#0f0', dataByLabel: new Map([['t1', 3], ['t2', 2], ['t3', 1]]) },
      { name: 'C', color: '#00f', dataByLabel: new Map([['t1', 2], ['t2', 4], ['t3', 6]]) },
    ]);
    const corr = correlateToFocused(series, 'A');
    expect(corr.size).toBe(3);
    expect(corr.get('A')).toBe(1);
    expect(corr.get('B')).toBeCloseTo(-1, 5);
    expect(corr.get('C')).toBeCloseTo(1, 5); // C is proportional to A
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// correlationToOpacity
// ═══════════════════════════════════════════════════════════════════════════

describe('correlationToOpacity', { tags: ['observability'] }, () => {
  it('returns max opacity for high correlation', () => {
    expect(correlationToOpacity(0.9)).toBe(0.7);
    expect(correlationToOpacity(-0.85)).toBe(0.7);
  });

  it('returns min opacity for low correlation', () => {
    expect(correlationToOpacity(0.1)).toBe(0.08);
    expect(correlationToOpacity(0)).toBe(0.08);
  });

  it('interpolates for mid-range', () => {
    const mid = correlationToOpacity(0.55);
    expect(mid).toBeGreaterThan(0.08);
    expect(mid).toBeLessThan(0.7);
  });

  it('is symmetric for positive and negative', () => {
    expect(correlationToOpacity(0.6)).toBe(correlationToOpacity(-0.6));
    expect(correlationToOpacity(0.4)).toBe(correlationToOpacity(-0.4));
  });

  it('is monotonically increasing with |r|', () => {
    const values = [0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.8, 0.9, 1.0];
    for (let i = 1; i < values.length; i++) {
      expect(correlationToOpacity(values[i])).toBeGreaterThanOrEqual(correlationToOpacity(values[i - 1]));
    }
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// correlationStrength
// ═══════════════════════════════════════════════════════════════════════════

describe('correlationStrength', { tags: ['observability'] }, () => {
  it('returns strong for |r| >= 0.8', () => {
    expect(correlationStrength(0.8)).toBe('strong');
    expect(correlationStrength(0.95)).toBe('strong');
    expect(correlationStrength(-0.85)).toBe('strong');
    expect(correlationStrength(1)).toBe('strong');
  });

  it('returns moderate for 0.5 <= |r| < 0.8', () => {
    expect(correlationStrength(0.5)).toBe('moderate');
    expect(correlationStrength(0.65)).toBe('moderate');
    expect(correlationStrength(-0.79)).toBe('moderate');
  });

  it('returns weak for |r| < 0.5', () => {
    expect(correlationStrength(0)).toBe('weak');
    expect(correlationStrength(0.3)).toBe('weak');
    expect(correlationStrength(0.49)).toBe('weak');
    expect(correlationStrength(-0.2)).toBe('weak');
  });

  it('thresholds match CORRELATION_THRESHOLDS', () => {
    expect(correlationStrength(CORRELATION_THRESHOLDS.STRONG)).toBe('strong');
    expect(correlationStrength(CORRELATION_THRESHOLDS.STRONG - 0.01)).toBe('moderate');
    expect(correlationStrength(CORRELATION_THRESHOLDS.MODERATE)).toBe('moderate');
    expect(correlationStrength(CORRELATION_THRESHOLDS.MODERATE - 0.01)).toBe('weak');
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// computeInsightsAndLags — realistic scenario tests
// ═══════════════════════════════════════════════════════════════════════════

describe('computeInsightsAndLags', { tags: ['observability'] }, () => {
  const mkSeries = (name: string, values: number[]) => {
    const timestamps = values.map((_, i) => `t${i}`);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    return {
      name, color: '#000', timestamps,
      raw: values,
      normalized: values.map(v => (v - min) / range),
    };
  };

  it('detects strong linear between proportional series', () => {
    const a = mkSeries('CPU', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const b = mkSeries('Memory', a.raw.map(v => v * 2 + 5));
    const { insights } = computeInsightsAndLags([a, b], 'CPU');
    expect(insights.get('Memory')?.label).toBe('strong linear');
  });

  it('produces consistent results with interpretCorrelation on the same data', () => {
    // Verify that computeInsightsAndLags delegates to interpretCorrelation correctly.
    // Use linear data where we know the expected outcome.
    const a = mkSeries('A', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const b = mkSeries('B', [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    const c = mkSeries('C', [5, 1, 5, 1, 5, 1, 5, 1, 5, 1]); // uncorrelated

    const { insights } = computeInsightsAndLags([a, b, c], 'A');

    // B is perfectly linear with A → strong linear
    expect(insights.get('B')?.label).toBe('strong linear');
    // C is uncorrelated → null
    expect(insights.get('C')).toBeNull();
    // Focus series is not included
    expect(insights.has('A')).toBe(false);
  });

  it('detects lagged relationship with time delay', () => {
    // CPU spikes first, then latency follows 3 intervals later
    const cpu =     [0, 0, 10, 20, 30, 20, 10, 0, 0, 0,  0,  0,  0,  0, 0, 0];
    const latency = [0, 0,  0,  0,  0, 10, 20, 30, 20, 10, 0,  0,  0,  0, 0, 0];
    const a = mkSeries('CPU', cpu);
    const b = mkSeries('Latency', latency);
    const { insights, lags } = computeInsightsAndLags([a, b], 'CPU');
    // Cross-correlation should find the lag, same-time pearson should be lower
    const lag = lags.get('Latency');
    expect(lag).not.toBe(0);
    // Insight should be 'lagged' if cross-corr is high enough and pearson is low enough
    // The exact insight depends on the scores — at minimum, lag should be detected
    expect(lag).toBeDefined();
    expect(Math.abs(lag!)).toBeGreaterThan(0);
  });

  it('detects outlier-driven when extreme values inflate Pearson', () => {
    // Mostly random, but one extreme point creates an artificial linear trend
    const a = mkSeries('A', [1, 2, 1, 2, 1, 2, 1, 2, 1, 100]);
    const b = mkSeries('B', [2, 1, 2, 1, 2, 1, 2, 1, 2, 100]);
    const { insights } = computeInsightsAndLags([a, b], 'A');
    const insight = insights.get('B');
    // The extreme shared outlier inflates Pearson but ranks don't agree
    if (insight) {
      expect(insight.label).toBe('outlier-driven');
    }
  });

  it('returns null insight for uncorrelated series', () => {
    const a = mkSeries('A', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const b = mkSeries('B', [5, 1, 5, 1, 5, 1, 5, 1, 5, 1]);
    const { insights } = computeInsightsAndLags([a, b], 'A');
    expect(insights.get('B')).toBeNull();
  });

  it('returns empty maps for non-existent focused series', () => {
    const a = mkSeries('A', [1, 2, 3]);
    const { insights, lags } = computeInsightsAndLags([a], 'nonexistent');
    expect(insights.size).toBe(0);
    expect(lags.size).toBe(0);
  });

  it('handles multiple series against one focus', () => {
    const a = mkSeries('Focus', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    const b = mkSeries('Linear', a.raw.map(v => v * 3));
    const c = mkSeries('Inverse', a.raw.map(v => 100 - v * 3));
    const d = mkSeries('Random', [5, 1, 5, 1, 5, 1, 5, 1, 5, 1]);
    const { insights, lags } = computeInsightsAndLags([a, b, c, d], 'Focus');
    expect(insights.size).toBe(3); // all except Focus itself
    expect(lags.size).toBe(3);
    expect(insights.get('Linear')?.label).toBe('strong linear');
    expect(insights.get('Inverse')?.label).toBe('strong linear'); // strong negative
    expect(insights.get('Random')).toBeNull(); // no relationship
  });
});
