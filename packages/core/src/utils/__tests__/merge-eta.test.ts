import { describe, it, expect } from 'vitest';
import { computeMergeEta, pickThroughputEstimate, getSizeBucketLower } from '../merge-eta.js';
import type { MergeThroughputEstimate } from '../../types/merge.js';

const MB = 1024 * 1024;
const GB = 1024 * MB;

function makeEstimate(overrides: Partial<MergeThroughputEstimate> = {}): MergeThroughputEstimate {
  return {
    merge_algorithm: 'Horizontal',
    size_bucket_lower: 0,
    merge_count: 10,
    avg_bytes_per_sec: 50 * MB,
    median_bytes_per_sec: 50 * MB,
    avg_duration_ms: 5000,
    avg_size_bytes: 100 * MB,
    ...overrides,
  };
}

describe('getSizeBucketLower', { tags: ['merge-engine'] }, () => {
  it('maps sizes < 10 MB to bucket 0', () => {
    expect(getSizeBucketLower(0)).toBe(0);
    expect(getSizeBucketLower(1 * MB)).toBe(0);
    expect(getSizeBucketLower(9.9 * MB)).toBe(0);
  });

  it('maps sizes 10–100 MB to bucket 10 MB', () => {
    expect(getSizeBucketLower(10 * MB)).toBe(10 * MB);
    expect(getSizeBucketLower(50 * MB)).toBe(10 * MB);
    expect(getSizeBucketLower(99 * MB)).toBe(10 * MB);
  });

  it('maps sizes 100 MB–1 GB to bucket 100 MB', () => {
    expect(getSizeBucketLower(100 * MB)).toBe(100 * MB);
    expect(getSizeBucketLower(500 * MB)).toBe(100 * MB);
  });

  it('maps sizes 1–10 GB to bucket 1 GB', () => {
    expect(getSizeBucketLower(1 * GB)).toBe(1 * GB);
    expect(getSizeBucketLower(5 * GB)).toBe(1 * GB);
  });

  it('maps sizes >= 10 GB to bucket 10 GB', () => {
    expect(getSizeBucketLower(10 * GB)).toBe(10 * GB);
    expect(getSizeBucketLower(100 * GB)).toBe(10 * GB);
  });
});

describe('pickThroughputEstimate', { tags: ['merge-engine'] }, () => {
  const smallHorizontal = makeEstimate({ merge_algorithm: 'Horizontal', size_bucket_lower: 0, median_bytes_per_sec: 20 * MB });
  const largeHorizontal = makeEstimate({ merge_algorithm: 'Horizontal', size_bucket_lower: 100 * MB, median_bytes_per_sec: 80 * MB });
  const smallVertical = makeEstimate({ merge_algorithm: 'Vertical', size_bucket_lower: 0, median_bytes_per_sec: 10 * MB });
  const largeVertical = makeEstimate({ merge_algorithm: 'Vertical', size_bucket_lower: 1 * GB, median_bytes_per_sec: 40 * MB });

  const allEstimates = [smallHorizontal, largeHorizontal, smallVertical, largeVertical];

  it('returns null for empty estimates', () => {
    expect(pickThroughputEstimate([], 'Horizontal', 50 * MB)).toBeNull();
  });

  it('exact match on algorithm + size bucket', () => {
    const result = pickThroughputEstimate(allEstimates, 'Horizontal', 5 * MB);
    expect(result).toBe(smallHorizontal);
  });

  it('exact match on algorithm + large size bucket', () => {
    const result = pickThroughputEstimate(allEstimates, 'Horizontal', 200 * MB);
    expect(result).toBe(largeHorizontal);
  });

  it('falls back to same algorithm, closest bucket', () => {
    // 50 MB → bucket 10 MB, no exact Horizontal match at 10 MB, closest is 0
    const result = pickThroughputEstimate(allEstimates, 'Horizontal', 50 * MB);
    expect(result?.merge_algorithm).toBe('Horizontal');
    expect(result).toBe(smallHorizontal);
  });

  it('Vertical algorithm picks Vertical estimate', () => {
    const result = pickThroughputEstimate(allEstimates, 'Vertical', 2 * GB);
    expect(result).toBe(largeVertical);
  });

  it('falls back to aggregated estimates if no algorithm match', () => {
    const result = pickThroughputEstimate([smallHorizontal], 'Vertical', 5 * MB);
    expect(result).not.toBeNull();
    expect(result!.merge_count).toBe(smallHorizontal.merge_count);
  });

  it('defaults to Horizontal when algorithm is undefined', () => {
    const result = pickThroughputEstimate(allEstimates, undefined, 5 * MB);
    expect(result).toBe(smallHorizontal);
  });

  describe('aggregation fallback for small buckets', () => {
    it('aggregates same-algorithm buckets when all have < 3 samples', () => {
      const bucket1 = makeEstimate({ merge_algorithm: 'Horizontal', size_bucket_lower: 0, merge_count: 1, median_bytes_per_sec: 20 * MB });
      const bucket2 = makeEstimate({ merge_algorithm: 'Horizontal', size_bucket_lower: 10 * MB, merge_count: 2, median_bytes_per_sec: 40 * MB });
      const result = pickThroughputEstimate([bucket1, bucket2], 'Horizontal', 5 * MB);
      expect(result).not.toBeNull();
      // Aggregated: total count = 3, weighted median = (20*1 + 40*2)/3 = 33.33 MB/s
      expect(result!.merge_count).toBe(3);
      expect(result!.median_bytes_per_sec).toBeCloseTo((20 * MB * 1 + 40 * MB * 2) / 3, 0);
    });

    it('prefers bucket with enough samples over exact bucket with too few', () => {
      const exactSmall = makeEstimate({ merge_algorithm: 'Horizontal', size_bucket_lower: 0, merge_count: 1, median_bytes_per_sec: 20 * MB });
      const otherLarge = makeEstimate({ merge_algorithm: 'Horizontal', size_bucket_lower: 100 * MB, merge_count: 50, median_bytes_per_sec: 80 * MB });
      const result = pickThroughputEstimate([exactSmall, otherLarge], 'Horizontal', 5 * MB);
      // Should pick otherLarge (enough samples) over exactSmall (too few)
      expect(result).toBe(otherLarge);
    });

    it('aggregates across algorithms as last resort', () => {
      const v1 = makeEstimate({ merge_algorithm: 'Vertical', size_bucket_lower: 0, merge_count: 2, median_bytes_per_sec: 10 * MB });
      const v2 = makeEstimate({ merge_algorithm: 'Vertical', size_bucket_lower: 10 * MB, merge_count: 2, median_bytes_per_sec: 30 * MB });
      // Requesting Horizontal, but only Vertical data exists
      const result = pickThroughputEstimate([v1, v2], 'Horizontal', 5 * MB);
      expect(result).not.toBeNull();
      expect(result!.merge_count).toBe(4);
    });
  });
});

describe('computeMergeEta', { tags: ['merge-engine'] }, () => {
  it('returns null when estimate is null', () => {
    expect(computeMergeEta(100 * MB, 0.5, 10, null)).toBeNull();
  });

  it('returns null when merge_count < 3', () => {
    const est = makeEstimate({ merge_count: 2 });
    expect(computeMergeEta(100 * MB, 0.5, 10, est)).toBeNull();
  });

  it('returns null when progress is 0', () => {
    const est = makeEstimate();
    expect(computeMergeEta(100 * MB, 0, 0, est)).toBeNull();
  });

  it('returns null when totalBytes is 0', () => {
    const est = makeEstimate();
    expect(computeMergeEta(0, 0.5, 10, est)).toBeNull();
  });

  it('returns null when historical rate is 0', () => {
    const est = makeEstimate({ median_bytes_per_sec: 0 });
    expect(computeMergeEta(100 * MB, 0.5, 10, est)).toBeNull();
  });

  it('uses pure historical rate when elapsed is 0 (no live data)', () => {
    const est = makeEstimate({ median_bytes_per_sec: 50 * MB });
    const result = computeMergeEta(100 * MB, 0.5, 0, est);
    expect(result).not.toBeNull();
    // remaining = 50 MB, rate = 50 MB/s → ~1s
    expect(result!.remainingSec).toBeCloseTo(1.0, 1);
  });

  it('sizeMatched is true when estimate bucket matches merge size', () => {
    // size_bucket_lower = 0 matches merge size 5 MB (bucket 0)
    const est = makeEstimate({ size_bucket_lower: 0 });
    const result = computeMergeEta(5 * MB, 0.5, 10, est);
    expect(result!.sizeMatched).toBe(true);
  });

  it('sizeMatched is false when estimate bucket differs from merge size', () => {
    // size_bucket_lower = 100 MB but merge is 5 MB (bucket 0)
    const est = makeEstimate({ size_bucket_lower: 100 * MB });
    const result = computeMergeEta(5 * MB, 0.5, 10, est);
    expect(result!.sizeMatched).toBe(false);
  });

  it('blends historical and live rate based on progress', () => {
    // historical: 50 MB/s, live: 100 MB/s (processed 50 MB in 0.5s)
    // at 50% progress: blend = 50*0.5 + 100*0.5 = 75 MB/s
    // remaining = 50 MB → 50/75 ≈ 0.667s
    const est = makeEstimate({ median_bytes_per_sec: 50 * MB });
    const totalBytes = 100 * MB;
    const progress = 0.5;
    const elapsed = 0.5; // processed 50 MB in 0.5s = 100 MB/s live rate
    const result = computeMergeEta(totalBytes, progress, elapsed, est);
    expect(result).not.toBeNull();
    expect(result!.remainingSec).toBeCloseTo(50 * MB / (75 * MB), 2);
  });

  it('at low progress, leans heavily on historical', () => {
    // progress = 10%, historical = 50 MB/s, live = 200 MB/s
    // blend = 50*0.9 + 200*0.1 = 65 MB/s
    const est = makeEstimate({ median_bytes_per_sec: 50 * MB });
    const totalBytes = 100 * MB;
    const progress = 0.1;
    const bytesProcessed = totalBytes * progress; // 10 MB
    const elapsed = bytesProcessed / (200 * MB); // live = 200 MB/s
    const result = computeMergeEta(totalBytes, progress, elapsed, est);
    expect(result).not.toBeNull();
    const expectedRate = 50 * MB * 0.9 + 200 * MB * 0.1;
    const expectedRemaining = (totalBytes * 0.9) / expectedRate;
    expect(result!.remainingSec).toBeCloseTo(expectedRemaining, 3);
  });

  it('at high progress, leans heavily on live rate', () => {
    // progress = 90%, historical = 50 MB/s, live = 10 MB/s
    // blend = 50*0.1 + 10*0.9 = 14 MB/s
    const est = makeEstimate({ median_bytes_per_sec: 50 * MB });
    const totalBytes = 100 * MB;
    const progress = 0.9;
    const bytesProcessed = totalBytes * progress; // 90 MB
    const elapsed = bytesProcessed / (10 * MB); // live = 10 MB/s
    const result = computeMergeEta(totalBytes, progress, elapsed, est);
    expect(result).not.toBeNull();
    const expectedRate = 50 * MB * 0.1 + 10 * MB * 0.9;
    const expectedRemaining = (totalBytes * 0.1) / expectedRate;
    expect(result!.remainingSec).toBeCloseTo(expectedRemaining, 3);
  });

  it('populates basedOnCount from estimate', () => {
    const est = makeEstimate({ merge_count: 42 });
    const result = computeMergeEta(100 * MB, 0.5, 10, est);
    expect(result!.basedOnCount).toBe(42);
  });
});
