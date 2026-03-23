import { describe, it, expect } from 'vitest';
import { parseVerticalMergeProgress } from '../vertical-merge-progress.js';
import type { MergeTextLog } from '../../types/merge.js';

/** Helper to build a MergeTextLog entry from the pasted log format */
function mkLog(time: string, microTime: string, source: string, threadId: number, message: string): MergeTextLog {
  return {
    event_time: time,
    event_time_microseconds: microTime,
    query_id: '639fc02e-e56e-4429-b82c-1041f6d54cd8::202602_0_974_4',
    level: 'Debug',
    message,
    source,
    thread_id: threadId,
    thread_name: 'MergeMutate',
  };
}

describe('parseVerticalMergeProgress', () => {
  it('detects columns that are read but never gathered (_block_number)', () => {
    // Real log from a vertical merge of synthetic_data.events.
    // _block_number is read from all 8 parts but never has a "Gathered column" entry.
    // _block_offset IS gathered. The parser must show _block_number as a separate segment.
    const logs: MergeTextLog[] = [
      // PK merge sorted
      mkLog('2026-03-20 11:22:58', '2026-03-20 11:22:58.816000',
        'MergingSortedTransform', 677,
        'Merged sorted, 60186 blocks, 487500000 rows, 6825000000 bytes in 68.14874457 sec., 7153470.002653638 rows/sec., 95.51 MiB/sec.'),

      // event_id gathered
      mkLog('2026-03-20 11:25:11', '2026-03-20 11:25:11.771000',
        'ColumnGathererStream', 675,
        'Gathered column event_id, 59510 blocks, 487500000 rows, 11471236400 bytes in 7.545000006 sec., 64612325.992356 rows/sec., 1.42 GiB/sec.'),

      // session_id gathered
      mkLog('2026-03-20 11:25:54', '2026-03-20 11:25:54.713000',
        'ColumnGathererStream', 672,
        'Gathered column session_id, 59510 blocks, 487500000 rows, 9616218776 bytes in 16.75400001 sec., 29097528.930943344 rows/sec., 547.38 MiB/sec.'),

      // page_url gathered
      mkLog('2026-03-20 11:29:05', '2026-03-20 11:29:05.385000',
        'ColumnGathererStream', 676,
        'Gathered column page_url, 59510 blocks, 487500000 rows, 21337627288 bytes in 4.125000004 sec., 118181818.06721762 rows/sec., 4.82 GiB/sec.'),

      // country_code gathered
      mkLog('2026-03-20 11:29:52', '2026-03-20 11:29:52.665000',
        'ColumnGathererStream', 666,
        'Gathered column country_code, 59510 blocks, 487500000 rows, 873999011 bytes in 43.228002184 sec., 11277412.218241226 rows/sec., 19.28 MiB/sec.'),

      // device_type gathered
      mkLog('2026-03-20 11:30:32', '2026-03-20 11:30:32.288000',
        'ColumnGathererStream', 671,
        'Gathered column device_type, 59510 blocks, 487500000 rows, 873999011 bytes in 31.683079391 sec., 15386761.936356504 rows/sec., 26.31 MiB/sec.'),

      // browser gathered
      mkLog('2026-03-20 11:31:02', '2026-03-20 11:31:02.969000',
        'ColumnGathererStream', 671,
        'Gathered column browser, 59510 blocks, 487500000 rows, 873999011 bytes in 28.346772972 sec., 17197724.780931372 rows/sec., 29.40 MiB/sec.'),

      // duration_ms gathered
      mkLog('2026-03-20 11:31:12', '2026-03-20 11:31:12.937000',
        'ColumnGathererStream', 671,
        'Gathered column duration_ms, 59510 blocks, 487500000 rows, 2870833484 bytes in 2.032000002 sec., 239911417.08670133 rows/sec., 1.32 GiB/sec.'),

      // revenue gathered
      mkLog('2026-03-20 11:31:24', '2026-03-20 11:31:24.678000',
        'ColumnGathererStream', 672,
        'Gathered column revenue, 59510 blocks, 487500000 rows, 591840304 bytes in 10.071281212 sec., 48404963.55311184 rows/sec., 56.04 MiB/sec.'),

      // _block_number: READ from 8 parts but NEVER gathered
      mkLog('2026-03-20 11:31:24', '2026-03-20 11:31:24.682000',
        'MergeTreeSequentialSource', 672,
        'Reading 13402 marks from part 202602_0_216_3, total 108500000 rows starting from the beginning of the part, column _block_number'),
      mkLog('2026-03-20 11:31:24', '2026-03-20 11:31:24.682000',
        'MergeTreeSequentialSource', 672,
        'Reading 13460 marks from part 202602_217_434_3, total 109000000 rows starting from the beginning of the part, column _block_number'),
      mkLog('2026-03-20 11:31:24', '2026-03-20 11:31:24.682000',
        'MergeTreeSequentialSource', 672,
        'Reading 1914 marks from part 202602_944_974_2, total 15500000 rows starting from the beginning of the part, column _block_number'),

      // _block_offset gathered (the NEXT column that does have a Gathered log)
      mkLog('2026-03-20 11:36:40', '2026-03-20 11:36:40.526000',
        'ColumnGathererStream', 669,
        'Gathered column _block_offset, 59510 blocks, 487500000 rows, 5737634456 bytes in 16.445853758 sec., 29642729.84385856 rows/sec., 332.72 MiB/sec.'),
    ];

    const result = parseVerticalMergeProgress(logs);
    expect(result).not.toBeNull();

    const names = result!.segments.map(s => s.name);

    // _block_number must appear as its own segment, NOT be lumped into _block_offset
    expect(names).toContain('_block_number');
    expect(names).toContain('_block_offset');
    expect(names).toContain('PK merge');

    // _block_number should come before _block_offset
    const bnIdx = names.indexOf('_block_number');
    const boIdx = names.indexOf('_block_offset');
    expect(bnIdx).toBeLessThan(boIdx);

    // _block_number should consume most of the time, not _block_offset
    const bnSeg = result!.segments[bnIdx];
    const boSeg = result!.segments[boIdx];
    expect(bnSeg.duration_sec).toBeGreaterThan(boSeg.duration_sec);

    // _block_offset should be roughly 16s (its gather time), not 315s
    expect(boSeg.duration_sec).toBeLessThan(30);
  });

  it('keeps only the last attempt when a merge is retried (two PK merges)', () => {
    // Simulates a failed merge (679) followed by a successful retry (676).
    // Both share the same query_id because the result part name is identical.
    const logs: MergeTextLog[] = [
      // --- Attempt 1 (MergeMutate 679): PK merge, slow ---
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.263000',
        'MergeTask::PrepareStage', 679,
        'Selected MergeAlgorithm: Vertical'),
      mkLog('2026-03-23 21:06:44', '2026-03-23 21:06:44.083000',
        'MergingSortedTransform', 679,
        'Merged sorted, 73118 blocks, 598500000 rows, 4788000000 bytes in 60.336 sec., 9919356.07 rows/sec., 75.68 MiB/sec.'),

      // Attempt 1 started reading trip_id but never gathered it (abandoned)
      mkLog('2026-03-23 21:06:44', '2026-03-23 21:06:44.091000',
        'MergeTreeSequentialSource', 679,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column trip_id'),

      // --- Attempt 2 (MergeMutate 676): PK merge, fast ---
      mkLog('2026-03-23 21:15:31', '2026-03-23 21:15:31.219000',
        'MergeTask::PrepareStage', 676,
        'Selected MergeAlgorithm: Vertical'),
      mkLog('2026-03-23 21:15:43', '2026-03-23 21:15:43.034000',
        'MergingSortedTransform', 676,
        'Merged sorted, 73118 blocks, 598500000 rows, 4788000000 bytes in 7.923 sec., 75539979.08 rows/sec., 576.32 MiB/sec.'),

      // Attempt 2 gathers trip_id successfully
      mkLog('2026-03-23 21:16:02', '2026-03-23 21:16:02.592000',
        'ColumnGathererStream', 676,
        'Gathered column trip_id, 73458 blocks, 598500000 rows, 6963917808 bytes in 0.604 sec., 990894039.74 rows/sec., 10.74 GiB/sec.'),

      // Attempt 2 gathers dropoff_datetime
      mkLog('2026-03-23 21:16:16', '2026-03-23 21:16:16.421000',
        'ColumnGathererStream', 676,
        'Gathered column dropoff_datetime, 73458 blocks, 598500000 rows, 3484556024 bytes in 0.524 sec., 1142175572.52 rows/sec., 6.19 GiB/sec.'),
    ];

    const result = parseVerticalMergeProgress(logs);
    expect(result).not.toBeNull();

    const names = result!.segments.map(s => s.name);
    // Should have exactly ONE PK merge (from attempt 2), not two
    expect(names.filter(n => n === 'PK merge')).toHaveLength(1);

    // Should have the gathered columns from attempt 2
    expect(names).toContain('trip_id');
    expect(names).toContain('dropoff_datetime');

    // PK merge duration should be ~7.9s (attempt 2), not ~60s (attempt 1)
    const pk = result!.segments.find(s => s.name === 'PK merge')!;
    expect(pk.duration_sec).toBeCloseTo(7.923, 1);
  });
});
