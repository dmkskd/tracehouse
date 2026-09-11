import { describe, it, expect } from 'vitest';
import {
  buildOperationRows,
  countOperationRows,
  describeOperationContext,
  filterOperationRowsByKind,
  findOperationRow,
  getOperationMetric,
  matchesOperationSearch,
  maxOperationMetric,
  operationKeyFor,
  operationHighlightKey,
  operationRowHighlightKey,
  operationTotals,
  parseOperationTimestamp,
  summarizeOperations,
  type OperationRowInput,
  type OperationRowOptions,
} from '../timeline-operations.js';
import type { QuerySeries, MergeSeries, MutationSeries } from '../../types/timeline.js';

const MB = 1024 * 1024;

function makeQuery(overrides: Partial<QuerySeries> = {}): QuerySeries {
  return {
    query_id: 'q1',
    label: 'SELECT … FROM analytics.events',
    user: 'th_eve',
    hostname: '40936b28',
    peak_memory: 384 * MB,
    duration_ms: 73_000,
    cpu_us: 72_000_000,
    net_send: 10,
    net_recv: 20,
    disk_read: 100,
    disk_write: 200,
    start_time: '2026-09-10 17:18:12',
    end_time: '2026-09-10 17:19:25',
    query_kind: 'Select',
    points: [],
    ...overrides,
  };
}

function makeMerge(overrides: Partial<MergeSeries> = {}): MergeSeries {
  return {
    part_name: 'all_1_5_1',
    table: 'analytics.events',
    hostname: '7c7914e4',
    peak_memory: 192 * MB,
    duration_ms: 50_000,
    cpu_us: 48_000_000,
    net_send: 0,
    net_recv: 0,
    disk_read: 5_000,
    disk_write: 6_000,
    start_time: '2026-09-10 17:17:05',
    end_time: '2026-09-10 17:17:55',
    merge_reason: 'RegularMerge',
    ...overrides,
  };
}

function makeMutation(overrides: Partial<MutationSeries> = {}): MutationSeries {
  return {
    part_name: 'all_9_9_0_12',
    table: 'analytics.sessions',
    hostname: 'ea54a891',
    peak_memory: 96 * MB,
    duration_ms: 28_000,
    cpu_us: 18_000_000,
    net_send: 0,
    net_recv: 0,
    disk_read: 1_000,
    disk_write: 2_000,
    start_time: '2026-09-10 17:20:01',
    end_time: '2026-09-10 17:20:29',
    ...overrides,
  };
}

const baseInput: OperationRowInput = {
  queries: [makeQuery()],
  merges: [makeMerge()],
  mutations: [makeMutation()],
};

const baseOptions: OperationRowOptions = {
  metricMode: 'cpu',
  sortField: 'metric',
  sortDir: 'desc',
  scope: 'window',
  pinnedMs: null,
  zoomRange: null,
};

describe('parseOperationTimestamp', () => {
  it('treats a zoneless ClickHouse datetime as UTC', () => {
    expect(parseOperationTimestamp('2026-09-10 17:18:12'))
      .toBe(Date.UTC(2026, 8, 10, 17, 18, 12));
  });

  it('respects an explicit zone', () => {
    expect(parseOperationTimestamp('2026-09-10T17:18:12Z'))
      .toBe(Date.UTC(2026, 8, 10, 17, 18, 12));
  });
});

describe('getOperationMetric', () => {
  const q = makeQuery();
  it('reads CPU microseconds', () => expect(getOperationMetric(q, 'cpu')).toBe(72_000_000));
  it('reads peak memory', () => expect(getOperationMetric(q, 'memory')).toBe(384 * MB));
  it('sums network send and receive', () => expect(getOperationMetric(q, 'network')).toBe(30));
  it('sums disk read and write', () => expect(getOperationMetric(q, 'disk')).toBe(300));
});

describe('buildOperationRows', () => {
  it('flattens all three kinds into one sorted list', () => {
    const rows = buildOperationRows(baseInput, baseOptions);
    expect(rows.map(r => r.kind)).toEqual(['query', 'merge', 'mutation']);
    expect(rows.map(r => r.metricValue)).toEqual([72_000_000, 48_000_000, 18_000_000]);
  });

  it('labels rows by kind', () => {
    const rows = buildOperationRows(baseInput, baseOptions);
    expect(rows[0].kindLabel).toBe('SELECT');
    expect(rows[0].label).toBe('SELECT … FROM analytics.events');
    expect(rows[1].kindLabel).toBe('MERGE');
    expect(rows[1].label).toBe('analytics.events');
    expect(rows[2].kindLabel).toBe('MUTATION');
  });

  it('falls back to QUERY when query_kind is missing', () => {
    const rows = buildOperationRows({ ...baseInput, queries: [makeQuery({ query_kind: undefined })] }, baseOptions);
    expect(rows[0].kindLabel).toBe('QUERY');
  });

  it('keeps the index of each item within its own kind', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ query_id: 'a', cpu_us: 1 }), makeQuery({ query_id: 'b', cpu_us: 5 })],
      merges: [],
      mutations: [],
    }, baseOptions);
    expect(rows.map(r => [r.id, r.idx])).toEqual([['b', 1], ['a', 0]]);
  });

  it('sorts ascending when asked', () => {
    const rows = buildOperationRows(baseInput, { ...baseOptions, sortDir: 'asc' });
    expect(rows.map(r => r.kind)).toEqual(['mutation', 'merge', 'query']);
  });

  it('sorts by duration and by start time', () => {
    const byDuration = buildOperationRows(baseInput, { ...baseOptions, sortField: 'duration' });
    expect(byDuration.map(r => r.durationMs)).toEqual([73_000, 50_000, 28_000]);
    const byStart = buildOperationRows(baseInput, { ...baseOptions, sortField: 'started', sortDir: 'asc' });
    expect(byStart.map(r => r.kind)).toEqual(['merge', 'query', 'mutation']);
  });

  it('re-sorts when the metric mode changes', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ query_id: 'cpu-heavy', cpu_us: 90, peak_memory: 1 })],
      merges: [makeMerge({ cpu_us: 10, peak_memory: 100 })],
      mutations: [],
    }, { ...baseOptions, metricMode: 'memory' });
    expect(rows.map(r => r.kind)).toEqual(['merge', 'query']);
  });

  it('scopes to operations active at the pin', () => {
    const pinnedMs = parseOperationTimestamp('2026-09-10 17:17:30');
    const rows = buildOperationRows(baseInput, { ...baseOptions, scope: 'pin', pinnedMs });
    expect(rows.map(r => r.kind)).toEqual(['merge']);
  });

  it('includes operations whose interval touches the pin boundary', () => {
    const pinnedMs = parseOperationTimestamp('2026-09-10 17:17:05');
    const rows = buildOperationRows(baseInput, { ...baseOptions, scope: 'pin', pinnedMs });
    expect(rows.map(r => r.kind)).toEqual(['merge']);
  });

  it('falls back to window scope when nothing is pinned', () => {
    const rows = buildOperationRows(baseInput, { ...baseOptions, scope: 'pin', pinnedMs: null });
    expect(rows).toHaveLength(3);
  });

  it('scopes to the zoom range when one is set', () => {
    const zoomRange: [number, number] = [
      parseOperationTimestamp('2026-09-10 17:19:00'),
      parseOperationTimestamp('2026-09-10 17:21:00'),
    ];
    const rows = buildOperationRows(baseInput, { ...baseOptions, zoomRange });
    expect(rows.map(r => r.kind)).toEqual(['query', 'mutation']);
  });

  it('ignores the zoom range under pin scope', () => {
    const rows = buildOperationRows(baseInput, {
      ...baseOptions,
      scope: 'pin',
      pinnedMs: parseOperationTimestamp('2026-09-10 17:17:30'),
      zoomRange: [
        parseOperationTimestamp('2026-09-10 17:19:00'),
        parseOperationTimestamp('2026-09-10 17:21:00'),
      ],
    });
    expect(rows.map(r => r.kind)).toEqual(['merge']);
  });

  it('floats hash-matched queries above everything else', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ query_id: 'small', cpu_us: 1, matched_hash: true })],
      merges: [makeMerge({ cpu_us: 999 })],
      mutations: [],
    }, { ...baseOptions, hashFilterActive: true });
    expect(rows.map(r => r.id)).toEqual(['small', 'all_1_5_1']);
  });

  it('drops non-matching operations in hash-only mode', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ query_id: 'match', matched_hash: true }), makeQuery({ query_id: 'other' })],
      merges: [makeMerge()],
      mutations: [makeMutation()],
    }, { ...baseOptions, hashFilterActive: true, hashOnly: true });
    expect(rows.map(r => r.id)).toEqual(['match']);
  });

  it('marks running and failed operations', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ is_running: true, exception_code: 241 })],
      merges: [],
      mutations: [],
    }, baseOptions);
    expect(rows[0].isRunning).toBe(true);
    expect(rows[0].failed).toBe(true);
  });

  it('carries the source series through for selection handlers', () => {
    const query = makeQuery();
    const rows = buildOperationRows({ queries: [query], merges: [], mutations: [] }, baseOptions);
    expect(rows[0].source).toBe(query);
  });
});

describe('countOperationRows / filterOperationRowsByKind', () => {
  it('counts each kind in the scoped set', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ query_id: 'a' }), makeQuery({ query_id: 'b' })],
      merges: [makeMerge()],
      mutations: [],
    }, baseOptions);
    expect(countOperationRows(rows)).toEqual({ all: 3, query: 2, merge: 1, mutation: 0 });
  });

  it('filters to one kind and leaves counts untouched', () => {
    const rows = buildOperationRows(baseInput, baseOptions);
    expect(filterOperationRowsByKind(rows, 'merge').map(r => r.kind)).toEqual(['merge']);
    expect(filterOperationRowsByKind(rows, 'all')).toHaveLength(3);
    expect(countOperationRows(rows).all).toBe(3);
  });
});

describe('operationTotals', () => {
  const totals = {
    queryTotal: 1406, mergeTotal: 903, mutationTotal: 0,
    queryLoaded: 100, mergeLoaded: 80, mutationLoaded: 0,
  };

  it('sums every kind for the All filter', () => {
    expect(operationTotals(totals, 'all')).toEqual({ total: 2309, loaded: 180 });
  });

  it('reports one kind at a time', () => {
    expect(operationTotals(totals, 'query')).toEqual({ total: 1406, loaded: 100 });
    expect(operationTotals(totals, 'merge')).toEqual({ total: 903, loaded: 80 });
    expect(operationTotals(totals, 'mutation')).toEqual({ total: 0, loaded: 0 });
  });
});

describe('summarizeOperations', () => {
  it('aggregates counts, metric total and running operations', () => {
    const rows = buildOperationRows(baseInput, baseOptions);
    const summary = summarizeOperations(rows);
    expect(summary.counts).toEqual({ all: 3, query: 1, merge: 1, mutation: 1 });
    expect(summary.metricTotal).toBe(72_000_000 + 48_000_000 + 18_000_000);
    expect(summary.runningCount).toBe(0);
  });

  it('counts running operations', () => {
    const rows = buildOperationRows({
      queries: [makeQuery({ is_running: true }), makeQuery({ query_id: 'done' })],
      merges: [],
      mutations: [],
    }, baseOptions);
    expect(summarizeOperations(rows).runningCount).toBe(1);
  });

  it('ranks hosts by metric total', () => {
    const rows = buildOperationRows({
      queries: [
        makeQuery({ query_id: 'a', hostname: 'h1', cpu_us: 10 }),
        makeQuery({ query_id: 'b', hostname: 'h2', cpu_us: 50 }),
        makeQuery({ query_id: 'c', hostname: 'h1', cpu_us: 5 }),
      ],
      merges: [],
      mutations: [],
    }, baseOptions);
    expect(summarizeOperations(rows).hosts).toEqual([
      { hostname: 'h2', count: 1, metricTotal: 50 },
      { hostname: 'h1', count: 2, metricTotal: 15 },
    ]);
  });

  it('caps the host list and skips operations with no host', () => {
    const rows = buildOperationRows({
      queries: ['h1', 'h2', 'h3', 'h4', 'h5'].map((h, i) => makeQuery({ query_id: h, hostname: h, cpu_us: 100 - i }))
        .concat([makeQuery({ query_id: 'nohost', hostname: undefined })]),
      merges: [],
      mutations: [],
    }, baseOptions);
    const summary = summarizeOperations(rows, 3);
    expect(summary.hosts.map(h => h.hostname)).toEqual(['h1', 'h2', 'h3']);
    expect(summary.counts.all).toBe(6);
  });
});

describe('operationKeyFor / findOperationRow', () => {
  it('keys a query by query_id and a part by part_name', () => {
    expect(operationKeyFor('query', makeQuery())).toEqual({ kind: 'query', id: 'q1', hostname: '40936b28' });
    expect(operationKeyFor('merge', makeMerge())).toEqual({ kind: 'merge', id: 'all_1_5_1', hostname: '7c7914e4' });
  });

  it('resolves a key against fresh data and reports the current index', () => {
    const input = {
      queries: [makeQuery({ query_id: 'other' }), makeQuery({ query_id: 'q1' })],
      merges: [],
      mutations: [],
    };
    const row = findOperationRow(input, { kind: 'query', id: 'q1' }, 'cpu');
    expect(row?.idx).toBe(1);
    expect(row?.id).toBe('q1');
  });

  it('returns null once the operation is no longer loaded', () => {
    expect(findOperationRow(baseInput, { kind: 'query', id: 'gone' }, 'cpu')).toBeNull();
    expect(findOperationRow(baseInput, null, 'cpu')).toBeNull();
  });

  it('distinguishes same-named parts on different hosts', () => {
    const input = {
      queries: [],
      merges: [makeMerge({ hostname: 'h1' }), makeMerge({ hostname: 'h2' })],
      mutations: [],
    };
    expect(findOperationRow(input, { kind: 'merge', id: 'all_1_5_1', hostname: 'h2' }, 'cpu')?.idx).toBe(1);
  });

  it('does not confuse a merge with a mutation of the same part name', () => {
    const input = {
      queries: [],
      merges: [makeMerge({ part_name: 'shared' })],
      mutations: [makeMutation({ part_name: 'shared' })],
    };
    expect(findOperationRow(input, { kind: 'mutation', id: 'shared' }, 'cpu')?.kind).toBe('mutation');
  });

  it('recomputes the metric for the current mode', () => {
    const row = findOperationRow(baseInput, { kind: 'query', id: 'q1' }, 'memory');
    expect(row?.metricValue).toBe(384 * MB);
  });
});

describe('matchesOperationSearch', () => {
  const rows = buildOperationRows(baseInput, baseOptions);
  const [query, merge] = rows;

  it('matches on query text, id, user, host and kind, case-insensitively', () => {
    expect(matchesOperationSearch(query, 'analytics')).toBe(true);
    expect(matchesOperationSearch(query, 'Q1')).toBe(true);
    expect(matchesOperationSearch(query, 'th_eve')).toBe(true);
    expect(matchesOperationSearch(query, '40936b28')).toBe(true);
    expect(matchesOperationSearch(query, 'select')).toBe(true);
  });

  it('requires every term to match', () => {
    expect(matchesOperationSearch(query, 'select analytics')).toBe(true);
    expect(matchesOperationSearch(query, 'select orders')).toBe(false);
  });

  it('treats an empty search as matching everything', () => {
    expect(matchesOperationSearch(merge, '')).toBe(true);
    expect(matchesOperationSearch(merge, '   ')).toBe(true);
  });

  it('does not match text from another operation', () => {
    expect(matchesOperationSearch(merge, 'th_eve')).toBe(false);
  });
});

describe('buildOperationRows — search', () => {
  it('filters rows by the search term', () => {
    const rows = buildOperationRows(baseInput, { ...baseOptions, search: 'sessions' });
    expect(rows.map(r => r.kind)).toEqual(['mutation']);
  });

  it('narrows the per-kind counts too, so the chips agree with the rows', () => {
    const rows = buildOperationRows(baseInput, { ...baseOptions, search: 'analytics.events' });
    expect(countOperationRows(rows)).toEqual({ all: 2, query: 1, merge: 1, mutation: 0 });
  });

  it('ignores a blank search', () => {
    expect(buildOperationRows(baseInput, { ...baseOptions, search: '  ' })).toHaveLength(3);
  });
});

describe('buildOperationRows — text sorting', () => {
  const input = {
    queries: [
      makeQuery({ query_id: 'a', user: 'zoe', hostname: 'h2', cpu_us: 10 }),
      makeQuery({ query_id: 'b', user: 'adam', hostname: 'h1', cpu_us: 20 }),
      makeQuery({ query_id: 'c', user: 'adam', hostname: 'h1', cpu_us: 90 }),
    ],
    merges: [],
    mutations: [],
  };

  it('sorts by user, heaviest first within a user', () => {
    const rows = buildOperationRows(input, { ...baseOptions, sortField: 'user', sortDir: 'asc' });
    expect(rows.map(r => r.id)).toEqual(['c', 'b', 'a']);
  });

  it('reverses the text order on descending, keeping the metric tiebreak', () => {
    const rows = buildOperationRows(input, { ...baseOptions, sortField: 'user', sortDir: 'desc' });
    expect(rows.map(r => r.id)).toEqual(['a', 'c', 'b']);
  });

  it('sorts by server', () => {
    const rows = buildOperationRows(input, { ...baseOptions, sortField: 'server', sortDir: 'asc' });
    expect(rows.map(r => r.hostname)).toEqual(['h1', 'h1', 'h2']);
  });

  it('sorts by kind label, grouping merges away from queries', () => {
    const rows = buildOperationRows(baseInput, { ...baseOptions, sortField: 'kind', sortDir: 'asc' });
    expect(rows.map(r => r.kindLabel)).toEqual(['MERGE', 'MUTATION', 'SELECT']);
  });
});

describe('describeOperationContext', () => {
  it('gives a query its user', () => {
    const [query] = buildOperationRows({ queries: [makeQuery()], merges: [], mutations: [] }, baseOptions);
    expect(describeOperationContext(query)).toEqual({ type: 'user', user: 'th_eve' });
  });

  it('reports nothing for a query with no user', () => {
    const [query] = buildOperationRows({ queries: [makeQuery({ user: '' })], merges: [], mutations: [] }, baseOptions);
    expect(describeOperationContext(query)).toEqual({ type: 'none' });
  });

  it('gives a merge its reason and its read and written bytes', () => {
    const [merge] = buildOperationRows({ queries: [], merges: [makeMerge()], mutations: [] }, baseOptions);
    expect(describeOperationContext(merge)).toEqual({
      type: 'part', reason: 'RegularMerge', readBytes: 5_000, writtenBytes: 6_000, progress: undefined,
    });
  });

  it('omits the reason for a mutation but keeps its byte counts', () => {
    const [mutation] = buildOperationRows({ queries: [], merges: [], mutations: [makeMutation()] }, baseOptions);
    expect(describeOperationContext(mutation)).toMatchObject({
      type: 'part', reason: undefined, readBytes: 1_000, writtenBytes: 2_000,
    });
  });

  it('carries progress only while the operation is running', () => {
    const [running] = buildOperationRows(
      { queries: [], merges: [makeMerge({ is_running: true, progress: 0.4 })], mutations: [] }, baseOptions);
    expect(describeOperationContext(running)).toMatchObject({ progress: 0.4 });
    const [done] = buildOperationRows(
      { queries: [], merges: [makeMerge({ is_running: false, progress: 0.4 })], mutations: [] }, baseOptions);
    expect(describeOperationContext(done)).toMatchObject({ progress: undefined });
  });
});

describe('maxOperationMetric', () => {
  it('finds the largest metric value', () => {
    const rows = buildOperationRows(baseInput, baseOptions);
    expect(maxOperationMetric(rows)).toBe(72_000_000);
  });

  it('returns zero for an empty list', () => {
    expect(maxOperationMetric([])).toBe(0);
  });
});

describe('metricEstimated', () => {
  it('flags a running merge s CPU, which the timeline derives from elapsed time', () => {
    const [merge] = buildOperationRows(
      { queries: [], merges: [makeMerge({ is_running: true })], mutations: [] }, baseOptions);
    expect(merge.metricEstimated).toBe(true);
  });

  it('does not flag a finished merge', () => {
    const [merge] = buildOperationRows({ queries: [], merges: [makeMerge()], mutations: [] }, baseOptions);
    expect(merge.metricEstimated).toBe(false);
  });

  it('does not flag a running query, whose CPU counters are real', () => {
    const [query] = buildOperationRows(
      { queries: [makeQuery({ is_running: true })], merges: [], mutations: [] }, baseOptions);
    expect(query.metricEstimated).toBe(false);
  });

  it('only applies to the CPU metric', () => {
    const [merge] = buildOperationRows(
      { queries: [], merges: [makeMerge({ is_running: true })], mutations: [] },
      { ...baseOptions, metricMode: 'memory' });
    expect(merge.metricEstimated).toBe(false);
  });
});

describe('operationHighlightKey / operationRowHighlightKey', () => {
  it('identifies a query by its id', () => {
    const query = makeQuery();
    expect(operationHighlightKey('query', query)).toBe('q1');
  });

  it('identifies a part by table, part name and host together', () => {
    expect(operationHighlightKey('merge', makeMerge()))
      .toBe('analytics.events:all_1_5_1:7c7914e4');
  });

  it('separates the same part name on different replicas', () => {
    expect(operationHighlightKey('merge', makeMerge({ hostname: 'other' })))
      .not.toBe(operationHighlightKey('merge', makeMerge()));
  });

  it('agrees with the key derived from a built row', () => {
    const rows = buildOperationRows(baseInput, baseOptions);
    for (const row of rows) {
      expect(operationRowHighlightKey(row)).toBe(operationHighlightKey(row.kind, row.source));
    }
  });
});
