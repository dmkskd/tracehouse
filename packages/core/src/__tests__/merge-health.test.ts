import { describe, it, expect } from 'vitest';
import { deriveHealth, mergeThroughputHealth, worstHealth, isMergeStuck } from '../utils/merge-health.js';
import type { Health, HealthNode, ThroughputMap } from '../utils/merge-health.js';
import type { MergeInfo, MutationInfo, BackgroundPoolMetrics, MergeThroughputEstimate } from '../types/merge.js';

// ── Helpers ───────────────────────────────────────────────────────

function makeMerge(overrides: Partial<MergeInfo> = {}): MergeInfo {
  return {
    database: 'default',
    table: 'hits',
    elapsed: 5,
    progress: 0.5,
    num_parts: 3,
    source_part_names: ['p1', 'p2', 'p3'],
    result_part_name: 'p1_3_3',
    total_size_bytes_compressed: 100_000_000,
    rows_read: 1000,
    rows_written: 500,
    memory_usage: 50_000_000,
    merge_type: 'Regular',
    merge_algorithm: 'Horizontal',
    is_mutation: false,
    bytes_read_uncompressed: 200_000_000,
    bytes_written_uncompressed: 100_000_000,
    columns_written: 10,
    thread_id: 1,
    ...overrides,
  };
}

function makeMutation(overrides: Partial<MutationInfo> = {}): MutationInfo {
  return {
    database: 'default',
    table: 'hits',
    mutation_id: 'mutation_0000000001',
    command: 'ALTER TABLE hits DELETE WHERE id = 1',
    create_time: '2026-01-01 00:00:00',
    parts_to_do: 2,
    total_parts: 5,
    parts_in_progress: 1,
    parts_done: 3,
    is_done: false,
    latest_failed_part: '',
    latest_fail_time: '',
    latest_fail_reason: '',
    is_killed: false,
    status: 'running',
    progress: 0.6,
    parts_to_do_names: ['p4', 'p5'],
    parts_in_progress_names: ['p3'],
    ...overrides,
  };
}

function makePool(overrides: Partial<BackgroundPoolMetrics> = {}): BackgroundPoolMetrics {
  return {
    merge_pool_size: 16,
    merge_pool_active: 4,
    move_pool_size: 8,
    move_pool_active: 0,
    fetch_pool_size: 8,
    fetch_pool_active: 0,
    schedule_pool_size: 128,
    schedule_pool_active: 2,
    common_pool_size: 8,
    common_pool_active: 0,
    distributed_pool_size: 0,
    distributed_pool_active: 0,
    active_merges: 4,
    active_mutations: 0,
    active_parts: 100,
    outdated_parts: 10,
    outdated_parts_bytes: 500_000,
    ...overrides,
  };
}

function makeEstimate(overrides: Partial<MergeThroughputEstimate> = {}): MergeThroughputEstimate {
  return {
    merge_algorithm: 'Horizontal',
    size_bucket_lower: 100_000_000,
    merge_count: 50,
    avg_bytes_per_sec: 20_000_000,
    median_bytes_per_sec: 18_000_000,
    avg_duration_ms: 5000,
    avg_size_bytes: 100_000_000,
    ...overrides,
  };
}

// ── isMergeStuck ──────────────────────────────────────────────────

describe('isMergeStuck', { tags: ['merge-engine'] }, () => {
  it('returns false for short-running merges', () => {
    expect(isMergeStuck(makeMerge({ elapsed: 60, progress: 0.01 }))).toBe(false);
  });

  it('returns true for merge with no progress after 10 min', () => {
    expect(isMergeStuck(makeMerge({ elapsed: 700, progress: 0.0005 }))).toBe(true);
  });

  it('returns true for merge stuck at 99.95%+ after 30 min', () => {
    expect(isMergeStuck(makeMerge({ elapsed: 2000, progress: 0.9996 }))).toBe(true);
  });

  it('returns false for merge at 99.95%+ but under 30 min', () => {
    expect(isMergeStuck(makeMerge({ elapsed: 800, progress: 0.9996 }))).toBe(false);
  });

  it('returns true for merge with huge estimated remaining time', () => {
    // progress 1% after 10 min → estimated total 1000 min, remaining 990 min
    expect(isMergeStuck(makeMerge({ elapsed: 600, progress: 0.01 }))).toBe(true);
  });

  it('returns false for normally progressing merge', () => {
    // 50% done in 10 min → remaining ~10 min, not stuck
    expect(isMergeStuck(makeMerge({ elapsed: 600, progress: 0.5 }))).toBe(false);
  });
});

// ── worstHealth ───────────────────────────────────────────────────

describe('worstHealth', { tags: ['merge-engine'] }, () => {
  it('returns green when all green', () => {
    expect(worstHealth([{ health: 'green' }, { health: 'green' }])).toBe('green');
  });

  it('returns yellow when any yellow', () => {
    expect(worstHealth([{ health: 'green' }, { health: 'yellow' }])).toBe('yellow');
  });

  it('returns red when any red', () => {
    expect(worstHealth([{ health: 'green' }, { health: 'yellow' }, { health: 'red' }])).toBe('red');
  });

  it('returns green for empty array', () => {
    expect(worstHealth([])).toBe('green');
  });
});

// ── mergeThroughputHealth ─────────────────────────────────────────

describe('mergeThroughputHealth', { tags: ['merge-engine'] }, () => {
  it('returns green for healthy merge with good throughput', () => {
    const estimates: ThroughputMap = new Map([
      ['default.hits', [makeEstimate({ median_bytes_per_sec: 10_000_000 })]],
    ]);
    // 50% of 100MB in 5s → 10MB/s live rate, matches expected
    const result = mergeThroughputHealth(
      makeMerge({ progress: 0.5, elapsed: 5, total_size_bytes_compressed: 100_000_000 }),
      estimates,
    );
    expect(result.health).toBe('green');
  });

  it('returns yellow when throughput is 25-50% of expected after 10s', () => {
    const estimates: ThroughputMap = new Map([
      ['default.hits', [makeEstimate({ median_bytes_per_sec: 100_000_000 })]],
    ]);
    // 5% of 100MB in 15s → ~333KB/s live, expected 100MB/s → ratio ~0.3%
    const result = mergeThroughputHealth(
      makeMerge({ progress: 0.05, elapsed: 15, total_size_bytes_compressed: 100_000_000 }),
      estimates,
    );
    expect(result.health).toBe('yellow');
  });

  it('returns red when throughput is <25% of expected after 30s', () => {
    const estimates: ThroughputMap = new Map([
      ['default.hits', [makeEstimate({ median_bytes_per_sec: 100_000_000 })]],
    ]);
    // 1% of 100MB in 60s → ~16KB/s live, expected 100MB/s
    const result = mergeThroughputHealth(
      makeMerge({ progress: 0.01, elapsed: 60, total_size_bytes_compressed: 100_000_000 }),
      estimates,
    );
    expect(result.health).toBe('red');
  });

  it('falls back to elapsed-based check when no historical data', () => {
    const estimates: ThroughputMap = new Map();
    const result = mergeThroughputHealth(
      makeMerge({ progress: 0.5, elapsed: 5 }),
      estimates,
    );
    expect(result.health).toBe('green');
  });

  it('falls back to elapsed-based check with too few samples', () => {
    const estimates: ThroughputMap = new Map([
      ['default.hits', [makeEstimate({ merge_count: 2 })]],
    ]);
    // <10% progress after 60s with no baseline → red
    const result = mergeThroughputHealth(
      makeMerge({ progress: 0.05, elapsed: 90 }),
      estimates,
    );
    expect(result.health).toBe('red');
  });

  it('includes reason string for yellow/red health', () => {
    const estimates: ThroughputMap = new Map([
      ['default.hits', [makeEstimate({ median_bytes_per_sec: 100_000_000 })]],
    ]);
    const result = mergeThroughputHealth(
      makeMerge({ progress: 0.01, elapsed: 60, total_size_bytes_compressed: 100_000_000 }),
      estimates,
    );
    expect(result.metric).toContain('expected rate');
  });
});

// ── deriveHealth ──────────────────────────────────────────────────

describe('deriveHealth', { tags: ['merge-engine'] }, () => {
  it('returns a green tree when everything is healthy', () => {
    const tree = deriveHealth(
      [makeMerge()],
      [],
      makePool(),
      new Map(),
    );
    expect(tree.name).toBe('Merge Health');
    expect(tree.health).toBe('green');
    expect(tree.children).toHaveLength(5);
  });

  it('has the correct 5 categories', () => {
    const tree = deriveHealth([makeMerge()], [], null, new Map());
    const names = tree.children!.map(c => c.name);
    expect(names).toEqual(['Part Count', 'Throughput', 'Mutations', 'Pool Usage', 'Resources']);
  });

  it('returns placeholder children when no data', () => {
    const tree = deriveHealth([], [], null, new Map());
    expect(tree.health).toBe('green');
    // Part Count should have "no tables" placeholder
    const partCount = tree.children!.find(c => c.name === 'Part Count')!;
    expect(partCount.children![0].name).toBe('no tables');
  });

  it('marks mutations red when there are failures', () => {
    const tree = deriveHealth(
      [makeMerge()],
      [makeMutation({ latest_fail_reason: 'Code: 60. DB::Exception: Table not found' })],
      null,
      new Map(),
    );
    const mutations = tree.children!.find(c => c.name === 'Mutations')!;
    expect(mutations.health).toBe('red');
    expect(mutations.children![0].children![0].metric).toContain('FAILED');
  });

  it('marks mutations yellow when queue is long', () => {
    const muts = Array.from({ length: 6 }, (_, i) =>
      makeMutation({ mutation_id: `mutation_${i}` }),
    );
    const tree = deriveHealth([makeMerge()], muts, null, new Map());
    const mutations = tree.children!.find(c => c.name === 'Mutations')!;
    expect(mutations.health).toBe('yellow');
  });

  it('marks pool usage red when near saturation', () => {
    const tree = deriveHealth(
      [makeMerge()],
      [],
      makePool({ merge_pool_size: 16, merge_pool_active: 15 }),
      new Map(),
    );
    const pool = tree.children!.find(c => c.name === 'Pool Usage')!;
    expect(pool.health).toBe('red');
    expect(pool.children!.find(c => c.name === 'Merge/Mutation')!.metric).toContain('saturation');
  });

  it('marks resources yellow when memory exceeds 1 GB', () => {
    const tree = deriveHealth(
      [makeMerge({ memory_usage: 2_000_000_000 })],
      [],
      null,
      new Map(),
    );
    const resources = tree.children!.find(c => c.name === 'Resources')!;
    const memNode = resources.children!.find(c => c.name === 'Memory')!;
    expect(memNode.health).toBe('yellow');
    expect(memNode.metric).toContain('memory pressure');
  });

  it('marks resources red when memory exceeds 4 GB', () => {
    const tree = deriveHealth(
      [makeMerge({ memory_usage: 5_000_000_000 })],
      [],
      null,
      new Map(),
    );
    const resources = tree.children!.find(c => c.name === 'Resources')!;
    const memNode = resources.children!.find(c => c.name === 'Memory')!;
    expect(memNode.health).toBe('red');
    expect(memNode.metric).toContain('OOM');
  });

  it('includes outdated parts cleanup when pool metrics provided', () => {
    const tree = deriveHealth(
      [makeMerge()],
      [],
      makePool({ outdated_parts: 600, outdated_parts_bytes: 10_000_000_000 }),
      new Map(),
    );
    const resources = tree.children!.find(c => c.name === 'Resources')!;
    const cleanup = resources.children!.find(c => c.name === 'Pending cleanup')!;
    expect(cleanup.health).toBe('red');
    expect(cleanup.metric).toContain('cleanup falling behind');
  });

  it('groups merges by table', () => {
    const merges = [
      makeMerge({ database: 'db1', table: 't1', result_part_name: 'p1' }),
      makeMerge({ database: 'db1', table: 't1', result_part_name: 'p2' }),
      makeMerge({ database: 'db2', table: 't2', result_part_name: 'p3' }),
    ];
    const tree = deriveHealth(merges, [], null, new Map());
    const partCount = tree.children!.find(c => c.name === 'Part Count')!;
    expect(partCount.children).toHaveLength(2); // db1.t1 and db2.t2
    const t1 = partCount.children!.find(c => c.name === 'db1.t1')!;
    expect(t1.children).toHaveLength(2);
  });

  it('overall health reflects worst category', () => {
    const tree = deriveHealth(
      [makeMerge({ memory_usage: 5_000_000_000 })], // red resources
      [],
      null,
      new Map(),
    );
    expect(tree.health).toBe('red');
  });
});
