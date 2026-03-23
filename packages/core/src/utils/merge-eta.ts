import type { MergeThroughputEstimate } from '../types/merge.js';

export interface MergeEtaInfo {
  remainingSec: number;
  basedOnCount: number;
  medianThroughput: number;
  /** Whether the estimate was based on merges in the same size bucket. */
  sizeMatched: boolean;
}

/** Minimum number of historical merges to produce an estimate. */
const MIN_SAMPLE_COUNT = 3;

// Size bucket boundaries (must match the SQL multiIf in GET_MERGE_THROUGHPUT_ESTIMATE)
const SIZE_BUCKET_BOUNDARIES = [
  10 * 1024 * 1024,       // 10 MB
  100 * 1024 * 1024,      // 100 MB
  1024 * 1024 * 1024,     // 1 GB
  10 * 1024 * 1024 * 1024, // 10 GB
];

/**
 * Map a byte size to its bucket lower bound (matching the SQL bucketing).
 */
export function getSizeBucketLower(sizeBytes: number): number {
  for (const boundary of SIZE_BUCKET_BOUNDARIES) {
    if (sizeBytes < boundary) {
      const idx = SIZE_BUCKET_BOUNDARIES.indexOf(boundary);
      return idx === 0 ? 0 : SIZE_BUCKET_BOUNDARIES[idx - 1];
    }
  }
  return SIZE_BUCKET_BOUNDARIES[SIZE_BUCKET_BOUNDARIES.length - 1];
}

/**
 * Aggregate multiple estimates into one using weighted averages (weighted by merge_count).
 */
function aggregateEstimates(estimates: MergeThroughputEstimate[]): MergeThroughputEstimate {
  const totalCount = estimates.reduce((s, e) => s + e.merge_count, 0);
  return {
    merge_algorithm: estimates[0].merge_algorithm,
    size_bucket_lower: estimates[0].size_bucket_lower,
    merge_count: totalCount,
    avg_bytes_per_sec: estimates.reduce((s, e) => s + e.avg_bytes_per_sec * e.merge_count, 0) / totalCount,
    median_bytes_per_sec: estimates.reduce((s, e) => s + e.median_bytes_per_sec * e.merge_count, 0) / totalCount,
    avg_duration_ms: estimates.reduce((s, e) => s + e.avg_duration_ms * e.merge_count, 0) / totalCount,
    avg_size_bytes: estimates.reduce((s, e) => s + e.avg_size_bytes * e.merge_count, 0) / totalCount,
  };
}

/**
 * Pick the best matching throughput estimate for a given merge algorithm and size.
 *
 * Priority:
 * 1. Exact match on algorithm + size bucket (if enough samples)
 * 2. Same algorithm, closest size bucket (if enough samples)
 * 3. Same algorithm, all buckets aggregated
 * 4. All estimates aggregated
 */
export function pickThroughputEstimate(
  estimates: MergeThroughputEstimate[],
  mergeAlgorithm: string | undefined,
  totalBytes: number,
): MergeThroughputEstimate | null {
  if (estimates.length === 0) return null;
  const algo = mergeAlgorithm || 'Horizontal';
  const bucket = getSizeBucketLower(totalBytes);

  // Priority 1: exact algorithm + size bucket with enough samples
  const exact = estimates.find(
    e => e.merge_algorithm === algo && e.size_bucket_lower === bucket && e.merge_count >= MIN_SAMPLE_COUNT,
  );
  if (exact) return exact;

  // Priority 2: same algorithm, closest size bucket with enough samples
  const sameAlgo = estimates.filter(e => e.merge_algorithm === algo);
  const sameAlgoEnough = sameAlgo.filter(e => e.merge_count >= MIN_SAMPLE_COUNT);
  if (sameAlgoEnough.length > 0) {
    return closestByBucket(sameAlgoEnough, bucket);
  }

  // Priority 3: same algorithm, aggregate all buckets
  if (sameAlgo.length > 0) {
    return aggregateEstimates(sameAlgo);
  }

  // Priority 4: aggregate everything
  return aggregateEstimates(estimates);
}

function closestByBucket(estimates: MergeThroughputEstimate[], bucket: number): MergeThroughputEstimate {
  const sorted = [...estimates].sort(
    (a, b) => Math.abs(a.size_bucket_lower - bucket) - Math.abs(b.size_bucket_lower - bucket),
  );
  return sorted[0];
}

/**
 * Compute estimated time remaining for an active merge.
 *
 * Uses a blended throughput rate: at low progress we lean on the historical
 * median from part_log; as the merge progresses we increasingly trust its own
 * live throughput (bytes processed / elapsed). The blend is linear in progress.
 *
 * Returns null if there isn't enough data (< 3 historical merges) or inputs are invalid.
 */
export function computeMergeEta(
  totalBytes: number,
  progress: number,
  elapsed: number,
  estimate: MergeThroughputEstimate | null,
): MergeEtaInfo | null {
  if (!estimate || estimate.merge_count < MIN_SAMPLE_COUNT) return null;
  if (progress <= 0 || totalBytes <= 0) return null;

  const historicalRate = estimate.median_bytes_per_sec;
  if (historicalRate <= 0) return null;

  const bytesProcessed = totalBytes * progress;
  const liveRate = elapsed > 0 ? bytesProcessed / elapsed : 0;

  // Blend: at 0% progress → 100% historical, at 100% → 100% live
  let effectiveRate: number;
  if (liveRate > 0) {
    effectiveRate = historicalRate * (1 - progress) + liveRate * progress;
  } else {
    effectiveRate = historicalRate;
  }

  const remainingBytes = totalBytes * (1 - progress);
  const remainingSec = remainingBytes / effectiveRate;

  return {
    remainingSec,
    basedOnCount: estimate.merge_count,
    medianThroughput: effectiveRate,
    sizeMatched: estimate.size_bucket_lower === getSizeBucketLower(totalBytes),
  };
}
