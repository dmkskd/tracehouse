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
    // Real logs from nyc_taxi.trips vertical merge of 202602_161_1357_4.
    // MergeMutate(679) was abandoned after PK merge (laptop sleep inflated time).
    // MergeMutate(676) retried and completed. Both share the same query_id.
    const logs: MergeTextLog[] = [
      // --- Attempt 1 (MergeMutate 679) ---
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.263000',
        'MergeTask::PrepareStage', 679,
        'Merging 10 parts: from 202602_161_376_3 to 202602_1325_1357_2 into Wide with storage Full'),
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.263000',
        'MergeTask::PrepareStage', 679,
        'Selected MergeAlgorithm: Vertical'),
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.264000',
        'MergeTreeSequentialSource', 679,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part'),
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.264000',
        'MergeTreeSequentialSource', 679,
        'Reading 13199 marks from part 202602_377_592_3, total 108000000 rows starting from the beginning of the part'),
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.264000',
        'MergeTreeSequentialSource', 679,
        'Reading 13200 marks from part 202602_593_808_3, total 108000000 rows starting from the beginning of the part'),
      mkLog('2026-03-23 20:30:00', '2026-03-23 20:30:00.277000',
        'MergeTreeSequentialSource', 679,
        'Reading 2206 marks from part 202602_1241_1276_2, total 18000000 rows starting from the beginning of the part'),
      // PK merge — 60s reported but wall clock was ~37min (laptop sleep)
      mkLog('2026-03-23 21:06:44', '2026-03-23 21:06:44.083000',
        'MergingSortedTransform', 679,
        'Merged sorted, 73118 blocks, 598500000 rows, 4788000000 bytes in 60.336577862 sec., 9919356.072345885 rows/sec., 75.68 MiB/sec.'),
      // Started reading trip_id but never gathered — abandoned
      mkLog('2026-03-23 21:06:44', '2026-03-23 21:06:44.091000',
        'MergeTreeSequentialSource', 679,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column trip_id'),
      mkLog('2026-03-23 21:06:44', '2026-03-23 21:06:44.091000',
        'MergeTreeSequentialSource', 679,
        'Reading 13199 marks from part 202602_377_592_3, total 108000000 rows starting from the beginning of the part, column trip_id'),
      mkLog('2026-03-23 21:06:44', '2026-03-23 21:06:44.092000',
        'MergeTreeSequentialSource', 679,
        'Reading 13200 marks from part 202602_1025_1240_3, total 108000000 rows starting from the beginning of the part, column trip_id'),

      // --- Attempt 2 (MergeMutate 676) ---
      mkLog('2026-03-23 21:15:31', '2026-03-23 21:15:31.219000',
        'MergeTask::PrepareStage', 676,
        'Merging 10 parts: from 202602_161_376_3 to 202602_1325_1357_2 into Wide with storage Full'),
      mkLog('2026-03-23 21:15:31', '2026-03-23 21:15:31.219000',
        'MergeTask::PrepareStage', 676,
        'Selected MergeAlgorithm: Vertical'),
      mkLog('2026-03-23 21:15:31', '2026-03-23 21:15:31.219000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part'),
      mkLog('2026-03-23 21:15:31', '2026-03-23 21:15:31.220000',
        'MergeTreeSequentialSource', 676,
        'Reading 2206 marks from part 202602_1241_1276_2, total 18000000 rows starting from the beginning of the part'),
      // PK merge — 7.9s, laptop awake
      mkLog('2026-03-23 21:15:43', '2026-03-23 21:15:43.034000',
        'MergingSortedTransform', 676,
        'Merged sorted, 73118 blocks, 598500000 rows, 4788000000 bytes in 7.92295692 sec., 75539979.08144628 rows/sec., 576.32 MiB/sec.'),
      // trip_id reading + gathered
      mkLog('2026-03-23 21:15:43', '2026-03-23 21:15:43.035000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column trip_id'),
      mkLog('2026-03-23 21:15:43', '2026-03-23 21:15:43.035000',
        'MergeTreeSequentialSource', 676,
        'Reading 2024 marks from part 202602_1325_1357_2, total 16500000 rows starting from the beginning of the part, column trip_id'),
      mkLog('2026-03-23 21:16:02', '2026-03-23 21:16:02.592000',
        'ColumnGathererStream', 676,
        'Gathered column trip_id, 73458 blocks, 598500000 rows, 6963917808 bytes in 0.604 sec., 990894039.7350993 rows/sec., 10.74 GiB/sec.'),
      // dropoff_datetime
      mkLog('2026-03-23 21:16:02', '2026-03-23 21:16:02.593000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column dropoff_datetime'),
      mkLog('2026-03-23 21:16:16', '2026-03-23 21:16:16.421000',
        'ColumnGathererStream', 676,
        'Gathered column dropoff_datetime, 73458 blocks, 598500000 rows, 3484556024 bytes in 0.524 sec., 1142175572.519084 rows/sec., 6.19 GiB/sec.'),
      // passenger_count
      mkLog('2026-03-23 21:16:16', '2026-03-23 21:16:16.422000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column passenger_count'),
      mkLog('2026-03-23 21:16:21', '2026-03-23 21:16:21.148000',
        'ColumnGathererStream', 676,
        'Gathered column passenger_count, 73458 blocks, 598500000 rows, 874994106 bytes in 0.471 sec., 1270700636.942675 rows/sec., 1.73 GiB/sec.'),
      // trip_distance
      mkLog('2026-03-23 21:16:21', '2026-03-23 21:16:21.149000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column trip_distance'),
      mkLog('2026-03-23 21:16:33', '2026-03-23 21:16:33.791000',
        'ColumnGathererStream', 676,
        'Gathered column trip_distance, 73458 blocks, 598500000 rows, 3484556024 bytes in 0.532 sec., 1125000000 rows/sec., 6.10 GiB/sec.'),
      // dropoff_location_id
      mkLog('2026-03-23 21:16:33', '2026-03-23 21:16:33.792000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column dropoff_location_id'),
      mkLog('2026-03-23 21:16:35', '2026-03-23 21:16:35.167000',
        'ColumnGathererStream', 676,
        'Gathered column dropoff_location_id, 73458 blocks, 598500000 rows, 1744875132 bytes in 0.467 sec., 1281584582.4411135 rows/sec., 3.48 GiB/sec.'),
      // payment_type
      mkLog('2026-03-23 21:16:35', '2026-03-23 21:16:35.168000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column payment_type'),
      mkLog('2026-03-23 21:16:44', '2026-03-23 21:16:44.225000',
        'ColumnGathererStream', 676,
        'Gathered column payment_type, 73458 blocks, 598500000 rows, 1283353210 bytes in 4.56 sec., 131250000.00000001 rows/sec., 268.40 MiB/sec.'),
      // fare_amount
      mkLog('2026-03-23 21:16:44', '2026-03-23 21:16:44.226000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column fare_amount'),
      mkLog('2026-03-23 21:16:58', '2026-03-23 21:16:58.136000',
        'ColumnGathererStream', 676,
        'Gathered column fare_amount, 73458 blocks, 598500000 rows, 6963917808 bytes in 0.599 sec., 999165275.4590986 rows/sec., 10.83 GiB/sec.'),
      // tip_amount
      mkLog('2026-03-23 21:16:58', '2026-03-23 21:16:58.136000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column tip_amount'),
      mkLog('2026-03-23 21:17:08', '2026-03-23 21:17:08.960000',
        'ColumnGathererStream', 676,
        'Gathered column tip_amount, 73458 blocks, 598500000 rows, 6963917808 bytes in 0.572000001 sec., 1046328669.4994253 rows/sec., 11.34 GiB/sec.'),
      // total_amount
      mkLog('2026-03-23 21:17:08', '2026-03-23 21:17:08.960000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column total_amount'),
      mkLog('2026-03-23 21:17:23', '2026-03-23 21:17:23.967000',
        'ColumnGathererStream', 676,
        'Gathered column total_amount, 73458 blocks, 598500000 rows, 6963917808 bytes in 0.603 sec., 992537313.4328358 rows/sec., 10.76 GiB/sec.'),
      // vendor_name
      mkLog('2026-03-23 21:17:23', '2026-03-23 21:17:23.968000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column vendor_name'),
      mkLog('2026-03-23 21:17:28', '2026-03-23 21:17:28.586000',
        'ColumnGathererStream', 676,
        'Gathered column vendor_name, 73458 blocks, 598500000 rows, 1283353210 bytes in 3.349000002 sec., 178710062.59856072 rows/sec., 365.45 MiB/sec.'),
      // trip_duration_seconds
      mkLog('2026-03-23 21:17:28', '2026-03-23 21:17:28.586000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column trip_duration_seconds'),
      mkLog('2026-03-23 21:17:39', '2026-03-23 21:17:39.763000',
        'ColumnGathererStream', 676,
        'Gathered column trip_duration_seconds, 73458 blocks, 598500000 rows, 3484556024 bytes in 0.547 sec., 1094149908.5923216 rows/sec., 5.93 GiB/sec.'),
      // rate_code
      mkLog('2026-03-23 21:17:39', '2026-03-23 21:17:39.764000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column rate_code'),
      mkLog('2026-03-23 21:18:11', '2026-03-23 21:18:11.574000',
        'ColumnGathererStream', 676,
        'Gathered column rate_code, 73458 blocks, 598500000 rows, 1283353210 bytes in 4.948 sec., 120957962.81325787 rows/sec., 247.35 MiB/sec.'),
      // inserted_at
      mkLog('2026-03-23 21:18:11', '2026-03-23 21:18:11.577000',
        'MergeTreeSequentialSource', 676,
        'Reading 13200 marks from part 202602_161_376_3, total 108000000 rows starting from the beginning of the part, column inserted_at'),
      mkLog('2026-03-23 21:18:22', '2026-03-23 21:18:22.310000',
        'ColumnGathererStream', 676,
        'Gathered column inserted_at, 73458 blocks, 598500000 rows, 3484556024 bytes in 0.494000001 sec., 1211538459.0859544 rows/sec., 6.57 GiB/sec.'),
      // Final summary + commit
      mkLog('2026-03-23 21:18:22', '2026-03-23 21:18:22.314000',
        'MergeTask::MergeProjectionsStage', 675,
        'Merge sorted 598500000 rows, containing 16 columns (3 merged, 13 gathered) in 171.09531129 sec., 3498050.2708549704 rows/sec., 206.83 MiB/sec.'),
      mkLog('2026-03-23 21:18:22', '2026-03-23 21:18:22.319000',
        'nyc_taxi.trips (2d7e47ec-1d42-41ad-a0a2-2fb37007df7e)', 675,
        'Part 202602_161_1357_4 committed to zookeeper'),
    ];

    const result = parseVerticalMergeProgress(logs);
    expect(result).not.toBeNull();

    const names = result!.segments.map(s => s.name);

    // Should have exactly ONE PK merge (from attempt 2), not two
    expect(names.filter(n => n === 'PK merge')).toHaveLength(1);

    // Should have all 13 gathered columns from attempt 2
    expect(names).toContain('trip_id');
    expect(names).toContain('dropoff_datetime');
    expect(names).toContain('passenger_count');
    expect(names).toContain('trip_distance');
    expect(names).toContain('dropoff_location_id');
    expect(names).toContain('payment_type');
    expect(names).toContain('fare_amount');
    expect(names).toContain('tip_amount');
    expect(names).toContain('total_amount');
    expect(names).toContain('vendor_name');
    expect(names).toContain('trip_duration_seconds');
    expect(names).toContain('rate_code');
    expect(names).toContain('inserted_at');
    // 1 PK + 13 columns = 14 segments
    expect(result!.segments.length).toBe(14);

    // PK merge duration should be ~7.9s (attempt 2), not ~60s (attempt 1)
    const pk = result!.segments.find(s => s.name === 'PK merge')!;
    expect(pk.duration_sec).toBeCloseTo(7.923, 1);

    // PK merge should start near 0 (re-anchored T0), not at ~2730s
    expect(pk.start_ms).toBeLessThan(1000);

    // Total wall time should be ~171s (the successful attempt), not ~2900s
    expect(result!.total_ms).toBeGreaterThan(150_000);
    expect(result!.total_ms).toBeLessThan(200_000);
  });
});
