import { describe, expect, it } from 'vitest';
import { mapMergeHistoryRecord } from '../merge-mappers.js';

/**
 * Row shaped like a system.part_log MergeParts event.
 */
function partLogRow(overrides: Record<string, unknown> = {}) {
  return {
    event_time: '2026-09-06 12:00:00',
    event_type: 'MergeParts',
    database: 'db',
    table: 't',
    part_name: 'all_1_1_1',
    partition_id: 'all',
    merge_reason: 'RegularMerge',
    rows: 100,
    read_rows: 100,
    size_in_bytes: 1000,
    bytes_uncompressed: 2000,
    read_bytes: 2000,
    duration_ms: 5,
    peak_memory_usage: 0,
    merged_from: ['all_1_1_0'],
    ...overrides,
  };
}

describe('mapMergeHistoryRecord', { tags: ['merge-engine'] }, () => {
  it('reports a negative rows_diff when a TTL merge rewrites a part', () => {
    // Partially-expired part: ClickHouse reads 200 rows and writes 100.
    const r = mapMergeHistoryRecord(
      partLogRow({ merge_reason: 'TTLDeleteMerge', rows: 100, read_rows: 200 }),
    );

    expect(r.merge_reason).toBe('TTLDelete');
    expect(r.rows_diff).toBe(-100);
    expect(r.whole_part_dropped).toBeUndefined();
  });

  it('flags a whole-part drop instead of reporting it as no change', () => {
    // Fully-expired part. Since CH 26.8 the read step is skipped for TTLDrop
    // merges, so part_log reports rows = 0 AND read_rows = 0. rows_diff cannot
    // be computed, so the flag is what tells consumers rows were removed.
    const r = mapMergeHistoryRecord(
      partLogRow({ merge_reason: 'TTLDropMerge', rows: 0, read_rows: 0 }),
    );

    expect(r.merge_reason).toBe('TTLDelete');
    expect(r.rows_diff).toBe(0);
    expect(r.whole_part_dropped).toBe(true);
  });

  it('does not flag a TTL drop that still reported rows', () => {
    // Pre-26.8 servers still populate read_rows for drop merges.
    const r = mapMergeHistoryRecord(
      partLogRow({ merge_reason: 'TTLDropMerge', rows: 0, read_rows: 200 }),
    );

    expect(r.rows_diff).toBe(-200);
    expect(r.whole_part_dropped).toBeUndefined();
  });

  it('leaves regular merges unflagged with a zero rows_diff', () => {
    const r = mapMergeHistoryRecord(partLogRow());

    expect(r.merge_reason).toBe('Regular');
    expect(r.rows_diff).toBe(0);
    expect(r.whole_part_dropped).toBeUndefined();
  });

  it('classifies a regular merge that loses rows as a lightweight delete', () => {
    const r = mapMergeHistoryRecord(partLogRow({ rows: 90, read_rows: 100 }));

    expect(r.rows_diff).toBe(-10);
    expect(r.merge_reason).toBe('LightweightDelete');
  });

  it('keeps deduplicating engines classified as regular when rows are lost', () => {
    const r = mapMergeHistoryRecord(
      partLogRow({ rows: 90, read_rows: 100, engine: 'ReplicatedReplacingMergeTree' }),
    );

    expect(r.rows_diff).toBe(-10);
    expect(r.merge_reason).toBe('Regular');
  });
});
