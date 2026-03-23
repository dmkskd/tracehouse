/**
 * Time-series correlation utilities.
 * Used by the overlay chart to color series by similarity and highlight correlated windows.
 *
 * The correlation algorithm is pluggable via `CorrelationFn`. Pearson is the default,
 * but callers can pass any function with the same signature (e.g. Spearman, DTW-based).
 */

/**
 * A correlation function: takes two numeric arrays, returns a similarity score in [-1, 1].
 * Must handle NaN values (skip or treat as missing). Return 0 if not enough data.
 */
export type CorrelationFn = (a: number[], b: number[]) => number;

/** A single normalized time-series: values mapped to 0→1 range. */
export interface NormalizedSeries {
  name: string;
  color: string;
  unit?: string;
  /** Timestamps aligned across all series */
  timestamps: string[];
  /** Raw values (same length as timestamps, NaN for missing) */
  raw: number[];
  /** Min-max normalized to 0→1 (same length as timestamps, NaN for missing) */
  normalized: number[];
}

/** Result of pairwise correlation. */
export interface CorrelationResult {
  seriesA: string;
  seriesB: string;
  /** Correlation score in [-1, 1] */
  r: number;
}

/** A window where two series are highly correlated. */
export interface CorrelatedWindow {
  seriesA: string;
  seriesB: string;
  /** Start index into the timestamps array */
  startIdx: number;
  /** End index (exclusive) */
  endIdx: number;
  /** Correlation score for this window */
  r: number;
}

/** A named correlation algorithm with description, for UI selection. */
export interface CorrelationAlgorithm {
  id: string;
  name: string;
  description: string;
  fn: CorrelationFn;
}

/** Registry of available correlation algorithms. */
export const CORRELATION_ALGORITHMS: CorrelationAlgorithm[] = [
  { id: 'pearson', name: 'Pearson', description: 'Linear correlation — detects proportional relationships', fn: pearson },
  { id: 'spearman', name: 'Spearman', description: 'Rank correlation — detects monotonic relationships (robust to outliers)', fn: spearman },
  { id: 'cross', name: 'Cross-corr', description: 'Time-shifted correlation — finds lagged relationships (A leads/follows B)', fn: crossCorrelation },
];

/**
 * Pearson correlation coefficient between two arrays.
 * Skips indices where either value is NaN.
 * Returns 0 if fewer than 3 valid pairs.
 */
export function pearson(a: number[], b: number[]): number {
  const n = Math.min(a.length, b.length);
  let sumA = 0, sumB = 0, sumAB = 0, sumA2 = 0, sumB2 = 0, count = 0;
  for (let i = 0; i < n; i++) {
    if (Number.isNaN(a[i]) || Number.isNaN(b[i])) continue;
    sumA += a[i];
    sumB += b[i];
    sumAB += a[i] * b[i];
    sumA2 += a[i] * a[i];
    sumB2 += b[i] * b[i];
    count++;
  }
  if (count < 3) return 0;
  const num = count * sumAB - sumA * sumB;
  const den = Math.sqrt((count * sumA2 - sumA * sumA) * (count * sumB2 - sumB * sumB));
  if (den === 0) return 0;
  return num / den;
}

/**
 * Spearman rank correlation — Pearson on the ranks of the values.
 * More robust to outliers, detects monotonic (not just linear) relationships.
 */
export function spearman(a: number[], b: number[]): number {
  // Collect valid pairs
  const pairs: { a: number; b: number }[] = [];
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (Number.isNaN(a[i]) || Number.isNaN(b[i])) continue;
    pairs.push({ a: a[i], b: b[i] });
  }
  if (pairs.length < 3) return 0;

  // Rank values (average rank for ties)
  const rank = (vals: number[]): number[] => {
    const indexed = vals.map((v, i) => ({ v, i }));
    indexed.sort((x, y) => x.v - y.v);
    const ranks = new Array<number>(vals.length);
    let i = 0;
    while (i < indexed.length) {
      let j = i;
      while (j < indexed.length && indexed[j].v === indexed[i].v) j++;
      const avgRank = (i + j + 1) / 2; // 1-based average
      for (let k = i; k < j; k++) ranks[indexed[k].i] = avgRank;
      i = j;
    }
    return ranks;
  };

  const ranksA = rank(pairs.map(p => p.a));
  const ranksB = rank(pairs.map(p => p.b));
  return pearson(ranksA, ranksB);
}

/**
 * Cross-correlation: finds the lag offset that maximizes |Pearson(a, shift(b, lag))|.
 * Returns the Pearson score at the best lag. Conforms to CorrelationFn signature.
 *
 * Max lag is capped at min(floor(n/4), 20) to avoid thin-slice artifacts.
 * Positive lag means b is shifted right (a leads b).
 */
export function crossCorrelation(a: number[], b: number[]): number {
  return crossCorrelationDetail(a, b).r;
}

/** Detailed cross-correlation result including the best lag offset. */
export interface CrossCorrelationResult {
  /** Pearson score at the best lag */
  r: number;
  /** Best lag offset. Positive = a leads b, negative = b leads a, 0 = simultaneous. */
  lag: number;
}

/**
 * Cross-correlation with full detail: returns both the best score and lag.
 * Use this when you need to display the lag in the UI.
 */
export function crossCorrelationDetail(a: number[], b: number[]): CrossCorrelationResult {
  const n = Math.min(a.length, b.length);
  if (n < 3) return { r: 0, lag: 0 };

  const maxLag = Math.min(Math.floor(n / 4), 20);
  let bestR = 0;
  let bestLag = 0;

  for (let lag = -maxLag; lag <= maxLag; lag++) {
    const start = Math.max(0, lag);
    const end = Math.min(n, n + lag);
    if (end - start < 3) continue;

    const sliceA = a.slice(start, end);
    const sliceB = b.slice(start - lag, end - lag);
    const r = pearson(sliceA, sliceB);

    if (Math.abs(r) > Math.abs(bestR) || (Math.abs(r) === Math.abs(bestR) && Math.abs(lag) < Math.abs(bestLag))) {
      bestR = r;
      bestLag = lag;
    }
  }

  // Negate: internal lag>0 means a is shifted right (b leads),
  // but the public convention is positive = a leads b.
  return { r: bestR, lag: -bestLag || 0 };
}

/**
 * Rolling correlation over a sliding window.
 * Returns an array of { startIdx, endIdx, r } for windows where |r| >= threshold.
 * @param correlationFn - algorithm to use (defaults to pearson)
 */
export function rollingCorrelation(
  a: number[],
  b: number[],
  windowSize: number,
  threshold: number,
  correlationFn: CorrelationFn = pearson,
): { startIdx: number; endIdx: number; r: number }[] {
  const n = Math.min(a.length, b.length);
  if (n < windowSize || windowSize < 3) return [];

  const results: { startIdx: number; endIdx: number; r: number }[] = [];
  let prevAbove = false;
  let runStart = 0;
  let runR = 0;

  for (let i = 0; i <= n - windowSize; i++) {
    const sliceA = a.slice(i, i + windowSize);
    const sliceB = b.slice(i, i + windowSize);
    const r = correlationFn(sliceA, sliceB);
    const above = Math.abs(r) >= threshold;

    if (above && !prevAbove) {
      runStart = i;
      runR = r;
    }
    if (above) {
      runR = Math.abs(r) > Math.abs(runR) ? r : runR;
    }
    if (!above && prevAbove) {
      results.push({ startIdx: runStart, endIdx: i + windowSize - 1, r: runR });
    }
    prevAbove = above;
  }
  if (prevAbove) {
    results.push({ startIdx: runStart, endIdx: n, r: runR });
  }
  return results;
}

/**
 * Normalize a numeric array to 0→1 using min-max scaling.
 * NaN values pass through as NaN.
 */
export function minMaxNormalize(values: number[]): number[] {
  let min = Infinity, max = -Infinity;
  for (const v of values) {
    if (Number.isNaN(v)) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }
  const range = max - min;
  if (range === 0 || !Number.isFinite(range)) {
    return values.map(v => Number.isNaN(v) ? NaN : 0.5);
  }
  return values.map(v => Number.isNaN(v) ? NaN : (v - min) / range);
}

/**
 * Convert panel time-series data into aligned, normalized series.
 * All series are aligned to the union of all timestamps, with NaN for missing points.
 */
export function normalizePanelData(
  panels: { name: string; color: string; unit?: string; dataByLabel: Map<string, number> }[],
): NormalizedSeries[] {
  // Collect union of all timestamps, sorted
  const tsSet = new Set<string>();
  for (const p of panels) {
    for (const k of p.dataByLabel.keys()) tsSet.add(k);
  }
  const timestamps = Array.from(tsSet).sort();

  return panels.map(p => {
    const raw = timestamps.map(t => p.dataByLabel.get(t) ?? NaN);
    return {
      name: p.name,
      color: p.color,
      unit: p.unit,
      timestamps,
      raw,
      normalized: minMaxNormalize(raw),
    };
  });
}

/**
 * Compute pairwise correlations for a focused series against all others.
 * Returns a map from series name → correlation score relative to the focused series.
 * @param correlationFn - algorithm to use (defaults to pearson)
 */
export function correlateToFocused(
  series: NormalizedSeries[],
  focusedName: string,
  correlationFn: CorrelationFn = pearson,
): Map<string, number> {
  const focused = series.find(s => s.name === focusedName);
  const result = new Map<string, number>();
  if (!focused) return result;
  for (const s of series) {
    if (s.name === focusedName) {
      result.set(s.name, 1);
    } else {
      result.set(s.name, correlationFn(focused.normalized, s.normalized));
    }
  }
  return result;
}

/** Insight from comparing all three correlation algorithms. */
export interface CorrelationInsight {
  /** Short label for display (e.g. "non-linear") */
  label: string;
  /** Longer explanation */
  detail: string;
  /** Severity/interest level: 'info' = expected, 'interesting' = worth noting */
  level: 'info' | 'interesting';
}

/**
 * Compare Pearson, Spearman, and Cross-correlation scores to produce an interpretation.
 *
 * Why these three algorithms reveal different things:
 *
 *   Pearson   — measures linear (proportional) relationships at the same time.
 *               Sensitive to outliers because it uses raw values.
 *
 *   Spearman  — measures monotonic relationships (values move in the same direction).
 *               Robust to outliers because it operates on ranks, not raw values.
 *               High Spearman + low Pearson → the relationship is real but non-linear.
 *
 *   Cross-corr — measures time-shifted linear relationships.
 *                Slides one series against the other to find the best lag.
 *                High Cross-corr + low Pearson → the relationship exists but with a delay.
 *
 * The interpretation rules below detect patterns where the algorithms disagree,
 * which is where the interesting insights live. When they all agree, the
 * relationship is straightforward. When they all disagree, there's no relationship.
 *
 * Rule priority matters: earlier rules take precedence. The ordering is:
 *   1. Agreement (all high) — the simple, expected case
 *   2. No relationship (all low) — nothing to say
 *   3. Non-linear — Spearman sees it, Pearson doesn't
 *   4. Lagged — Cross-corr sees it, Pearson doesn't (relationship with time delay)
 *   5. Outlier-driven — Pearson inflated, Spearman disagrees
 *
 * Returns null when scores fall in ambiguous mid-range territory where no
 * confident interpretation is possible.
 */
export function interpretCorrelation(
  r: { pearson: number; spearman: number; cross: number },
): CorrelationInsight | null {
  const absPearson = Math.abs(r.pearson);
  const absSpearman = Math.abs(r.spearman);
  const absCross = Math.abs(r.cross);

  // ── Rule 1: Agreement ──
  // All three algorithms agree the relationship is strong.
  // This means it's linear, monotonic, AND simultaneous — straightforward.
  const STRONG = 0.7;
  if (absPearson >= STRONG && absSpearman >= STRONG && absCross >= STRONG) {
    return { label: 'strong linear', detail: 'All algorithms agree: strong proportional relationship', level: 'info' };
  }

  // ── Rule 2: No relationship ──
  // All three agree nothing is there. Return null (no insight to show).
  const WEAK = 0.3;
  if (absPearson < WEAK && absSpearman < WEAK && absCross < WEAK) {
    return null;
  }

  // ── Rule 3: Non-linear monotonic ──
  // Spearman (rank-based) sees a strong relationship that Pearson (linear) misses.
  // This means values consistently move in the same direction, but not proportionally.
  // Example: exponential growth, logarithmic curves, any monotonic non-linear function.
  const MODERATE = 0.6;
  const GAP = 0.2;
  if (absSpearman >= MODERATE && absPearson < 0.5 && absSpearman - absPearson >= GAP) {
    return { label: 'non-linear', detail: 'Monotonic but not proportional (e.g. exponential/log)', level: 'interesting' };
  }

  // ── Rule 4: Lagged ──
  // Cross-correlation sees a strong relationship that same-time Pearson misses.
  // This means the series are related but one leads/follows the other with a time delay.
  // Example: CPU spike → query latency increase 2 intervals later.
  if (absCross >= MODERATE && absPearson < 0.5 && absCross - absPearson >= GAP) {
    return { label: 'lagged', detail: 'Correlated with a time delay (one series leads the other)', level: 'interesting' };
  }

  // ── Rule 5: Outlier-driven ──
  // Pearson is high but Spearman disagrees. Since Spearman is robust to outliers
  // and Pearson is not, this means a few extreme points are inflating the linear score.
  // The bulk of the data doesn't actually follow the trend.
  if (absPearson >= MODERATE && absSpearman < absPearson - GAP) {
    return { label: 'outlier-driven', detail: 'Linear score inflated by extreme values', level: 'interesting' };
  }

  // ── No confident interpretation ──
  // Scores are in the ambiguous mid-range. Better to say nothing than guess.
  return null;
}

/* ── Visual mapping constants ─────────────────────────────────────────── */

/** Correlation strength thresholds for visual display. */
export const CORRELATION_THRESHOLDS = {
  /** |r| at or above this is considered strong */
  STRONG: 0.8,
  /** |r| at or above this is considered moderate */
  MODERATE: 0.5,
  /** |r| below this is considered negligible */
  WEAK: 0.3,
} as const;

/** Opacity range for dimmed overlay lines. */
const OPACITY_MAX = 0.7;
const OPACITY_MIN = 0.08;

/**
 * Map a correlation score to an opacity for dimmed overlay lines.
 * |r| ≥ STRONG → bright, |r| ≤ WEAK → very dim, linear in between.
 */
export function correlationToOpacity(r: number): number {
  const abs = Math.abs(r);
  if (abs >= CORRELATION_THRESHOLDS.STRONG) return OPACITY_MAX;
  if (abs <= CORRELATION_THRESHOLDS.WEAK) return OPACITY_MIN;
  return OPACITY_MIN + (abs - CORRELATION_THRESHOLDS.WEAK) * (OPACITY_MAX - OPACITY_MIN) / (CORRELATION_THRESHOLDS.STRONG - CORRELATION_THRESHOLDS.WEAK);
}

/** Correlation strength classification. */
export type CorrelationStrength = 'strong' | 'moderate' | 'weak';

/**
 * Classify a correlation score into a strength bucket.
 */
export function correlationStrength(r: number): CorrelationStrength {
  const abs = Math.abs(r);
  if (abs >= CORRELATION_THRESHOLDS.STRONG) return 'strong';
  if (abs >= CORRELATION_THRESHOLDS.MODERATE) return 'moderate';
  return 'weak';
}

/** Threshold: series with |r| below this get checked for rolling correlation windows. */
export const ROLLING_WINDOW_TRIGGER = 0.5;
/** Minimum |r| within a rolling window to be reported. */
export const ROLLING_WINDOW_THRESHOLD = 0.8;

/**
 * Compute insight labels and cross-correlation lag offsets for all series relative to a focused one.
 * Encapsulates the full 3-algorithm comparison logic.
 */
export function computeInsightsAndLags(
  series: NormalizedSeries[],
  focusedName: string,
): { insights: Map<string, CorrelationInsight | null>; lags: Map<string, number> } {
  const insights = new Map<string, CorrelationInsight | null>();
  const lags = new Map<string, number>();

  const focused = series.find(s => s.name === focusedName);
  if (!focused) return { insights, lags };

  for (const s of series) {
    if (s.name === focusedName) continue;
    const crossDetail = crossCorrelationDetail(focused.normalized, s.normalized);
    const scores = {
      pearson: pearson(focused.normalized, s.normalized),
      spearman: spearman(focused.normalized, s.normalized),
      cross: crossDetail.r,
    };
    insights.set(s.name, interpretCorrelation(scores));
    lags.set(s.name, crossDetail.lag);
  }

  return { insights, lags };
}
