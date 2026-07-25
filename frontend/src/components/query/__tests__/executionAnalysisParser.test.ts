import { describe, expect, it } from 'vitest';
import {
  buildExecutionFlowNodes,
  parseExecutionAnalysis,
  parseMergeTreeReadDetails,
  parseOperatorDetailFields,
} from '../executionAnalysisParser';

const OUTPUT = `Query summary:
  Time:        3.31 ms (planning 2.23 ms · execution 1.07 ms)
  Read:        5.75 thousand rows, 268.53 KB (5.35 million rows/s., 249.99 MB/s.)
  Peak memory: 1003.16 KiB

Output: bucket, count()

Expression (Projection)
│  I/O: rows 322 → 322 · 18.52 KB → 21.30 KB
│    time 22.75 us (2.1%) · parallelism 0.99/1
│      Time per processor (1): min 22.54 us · median 22.54 us · max 22.54 us · sum 22.54 us
└──Aggregating
   │  Keys: bucket
   │  Aggregates: count()
   │  I/O: rows 1.83 thousand → 322 (17.56%) · 89.29 KB → 18.52 KB
   │    Stage (partial aggregation): time 253.17 us (23.6%) · parallelism 1.61/5
   │      Time per processor (5): min 36.54 us · median 92.33 us · max 110.13 us · sum 408.46 us
   └──ReadFromMergeTree (system.part_log)
         Read type: Default
         Parts: 5 | Granules: 6
         Output: event_time, database, table, rows, size_in_bytes
         Prewhere filter
         Prewhere filter column: event_time > '2026-07-25 14:27:26' AND event_type = 'NewPart'
         Indexes:
           Min-Max
             Condition: true
             Parts: 5/5
             Granules: 13/13
           Partition
             Condition: true
             Parts: 5/5
             Granules: 13/13
           PrimaryKey
             Keys:
               event_time
             Condition: (event_time in [1784984722, +Inf))
             Parts: 5/5
             Granules: 6/13
             Search Algorithm: generic exclusion search
           Ranges: 6
         I/O: rows 0 → 1.83 thousand · 0 B → 102.13 KB
           time 517.63 us (48.2%) · parallelism 3.60/5`;

describe('parseExecutionAnalysis', () => {
  it('projects summary, operator flow, I/O, timing, and processor metrics', () => {
    const parsed = parseExecutionAnalysis(OUTPUT);

    expect(parsed.summary).toMatchObject({
      totalTime: '3.31 ms',
      planningTime: '2.23 ms',
      executionTime: '1.07 ms',
      readRows: '5.75 thousand',
      readBytes: '268.53 KB',
      rowsPerSecond: '5.35 million',
      bytesPerSecond: '249.99 MB/s.',
      peakMemory: '1003.16 KiB',
      output: 'bucket, count()',
    });
    expect(parsed.nodes.map(node => [node.name, node.depth])).toEqual([
      ['Expression', 0],
      ['Aggregating', 1],
      ['ReadFromMergeTree', 2],
    ]);
    expect(parsed.nodes[0].timings[0]).toMatchObject({
      duration: '22.75 us',
      share: 2.1,
      parallelism: 0.99,
      maxParallelism: 1,
      processors: {
        count: 1,
        median: '22.54 us',
      },
    });
    expect(parsed.nodes[1]).toMatchObject({
      io: {
        inputRows: '1.83 thousand',
        outputRows: '322',
        retainedRows: '17.56%',
        inputBytes: '89.29 KB',
        outputBytes: '18.52 KB',
      },
      timings: [{
        label: 'partial aggregation',
        share: 23.6,
      }],
      details: ['Keys: bucket', 'Aggregates: count()'],
    });
    expect(parsed.nodes[2].timings[0].share).toBe(48.2);

    expect(parseMergeTreeReadDetails(parsed.nodes[2].details)).toEqual({
      readType: 'Default',
      parts: 5,
      granules: 6,
      ranges: 6,
      outputColumns: ['event_time', 'database', 'table', 'rows', 'size_in_bytes'],
      prewhere: "event_time > '2026-07-25 14:27:26' AND event_type = 'NewPart'",
      indexes: [
        {
          type: 'Min-Max',
          keys: [],
          condition: 'true',
          parts: { selected: 5, total: 5 },
          granules: { selected: 13, total: 13 },
        },
        {
          type: 'Partition',
          keys: [],
          condition: 'true',
          parts: { selected: 5, total: 5 },
          granules: { selected: 13, total: 13 },
        },
        {
          type: 'PrimaryKey',
          keys: ['event_time'],
          condition: '(event_time in [1784984722, +Inf))',
          parts: { selected: 5, total: 5 },
          granules: { selected: 6, total: 13 },
          searchAlgorithm: 'generic exclusion search',
        },
      ],
    });
  });

  it('fails soft when a future server format is not recognized', () => {
    expect(parseExecutionAnalysis('A completely new format')).toEqual({
      summary: {},
      nodes: [],
    });
  });

  it('projects ClickHouse result-first nodes into source-to-result display order', () => {
    const parsed = parseExecutionAnalysis(OUTPUT);

    expect(
      buildExecutionFlowNodes(parsed.nodes).map(node => [node.name, node.depth]),
    ).toEqual([
      ['ReadFromMergeTree', 0],
      ['Aggregating', 1],
      ['Expression', 2],
    ]);
  });

  it('projects generic detail lines into label/value fields', () => {
    expect(parseOperatorDetailFields([
      'Keys: bucket',
      'Aggregates: count(), avg(value)',
      'Prewhere filter',
    ])).toEqual([
      { raw: 'Keys: bucket', label: 'Keys', value: 'bucket' },
      {
        raw: 'Aggregates: count(), avg(value)',
        label: 'Aggregates',
        value: 'count(), avg(value)',
      },
      { raw: 'Prewhere filter' },
    ]);
  });
});
