import { describe, test, expect } from 'vitest';
import type { QueryDetail } from '@tracehouse/core';
import { buildScanEfficiency } from '../modal/tabs/ScanEfficiencyTab';

/** Helper to build a minimal QueryDetail with profile events */
function makeQuery(overrides: Partial<QueryDetail> & { ProfileEvents?: Record<string, number> }): QueryDetail {
  return {
    query_id: 'test-001',
    type: 'QueryFinish',
    query_start_time: '2024-01-01 00:00:00',
    query_start_time_microseconds: '2024-01-01 00:00:00.000000',
    query_duration_ms: 100,
    query: 'SELECT 1',
    formatted_query: 'SELECT 1',
    query_kind: 'Select',
    normalized_query_hash: '',
    query_hash: '',
    user: 'default',
    current_database: 'default',
    read_rows: 1000,
    read_bytes: 8000,
    written_rows: 0,
    written_bytes: 0,
    result_rows: 10,
    result_bytes: 80,
    memory_usage: 0,
    thread_ids: [],
    peak_threads_usage: 1,
    databases: [],
    tables: [],
    columns: [],
    partitions: [],
    used_functions: [],
    used_aggregate_functions: [],
    ProfileEvents: {},
    Settings: {},
    ...overrides,
  } as QueryDetail;
}

describe('buildScanEfficiency', { tags: ['query-analysis'] }, () => {
  describe('verdict calculation', () => {
    test('great partition pruning + no mark pruning → Excellent (the 1/75 parts case)', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 1,
          SelectedPartsTotal: 75,
          SelectedMarks: 1,
          SelectedMarksTotal: 1, // only 1 mark in the surviving part
          SelectedRanges: 1,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - (1/75)*(1/1) = 98.7%
      expect(result.verdict.label).toBe('Excellent');
      // Detail should mention partition pruning but not marks (only 1 total mark)
      expect(result.verdict.detail).toContain('98.7%');
      expect(result.verdict.detail).toContain('partition key (1/75 parts)');
      expect(result.verdict.detail).not.toContain('primary key');
    });

    test('great mark pruning + no partition pruning → Excellent', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 10,
          SelectedPartsTotal: 10,
          SelectedMarks: 5,
          SelectedMarksTotal: 100,
          SelectedRanges: 2,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - (10/10)*(5/100) = 95%
      expect(result.verdict.label).toBe('Excellent');
      // Detail should mention both (0% parts pruning + 95% marks pruning)
      expect(result.verdict.detail).toContain('partition key (10/10 parts)');
      expect(result.verdict.detail).toContain('primary key (5/100 marks)');
    });

    test('both partition and mark pruning combine multiplicatively', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 5,
          SelectedPartsTotal: 10,
          SelectedMarks: 10,
          SelectedMarksTotal: 20,
          SelectedRanges: 3,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - (5/10)*(10/20) = 1 - 0.25 = 75%
      expect(result.verdict.label).toBe('Good');
      expect(result.verdict.detail).toContain('75.0%');
      expect(result.verdict.detail).toContain('partition key (5/10 parts)');
      expect(result.verdict.detail).toContain('primary key (10/20 marks)');
    });

    test('moderate pruning → Fair', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 5,
          SelectedPartsTotal: 10,
          SelectedMarks: 8,
          SelectedMarksTotal: 10,
          SelectedRanges: 2,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - (5/10)*(8/10) = 1 - 0.4 = 60%
      expect(result.verdict.label).toBe('Fair');
    });

    test('near full scan → Poor', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 9,
          SelectedPartsTotal: 10,
          SelectedMarks: 95,
          SelectedMarksTotal: 100,
          SelectedRanges: 5,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - (9/10)*(95/100) = 1 - 0.855 = 14.5%
      expect(result.verdict.label).toBe('Poor');
    });

    test('full scan (all parts, all marks) → Poor', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 10,
          SelectedPartsTotal: 10,
          SelectedMarks: 100,
          SelectedMarksTotal: 100,
          SelectedRanges: 10,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - 1*1 = 0%
      expect(result.verdict.label).toBe('Poor');
    });

    test('no pruning data → N/A', () => {
      const q = makeQuery({ ProfileEvents: {} });
      const result = buildScanEfficiency(q, {});
      expect(result.verdict.label).toBe('N/A');
    });

    test('only parts data (no marks) still produces a verdict', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 1,
          SelectedPartsTotal: 100,
        },
      });
      const result = buildScanEfficiency(q, {});
      // Combined: 1 - (1/100)*1 = 99%
      expect(result.verdict.label).toBe('Excellent');
    });
  });

  describe('column selection', () => {
    test('columns from allTableColumns are merged across tables', () => {
      const q = makeQuery({
        tables: ['db.t1', 'db.t2'],
        columns: ['a', 'b'],
      });
      const result = buildScanEfficiency(q, {
        'db.t1': ['a', 'b', 'c', 'd'],
        'db.t2': ['e', 'f'],
      });
      expect(result.allColumns).toHaveLength(6);
      expect(result.columns).toEqual(['a', 'b']);
    });

    test('empty allTableColumns falls back to empty allColumns', () => {
      const q = makeQuery({ columns: ['x', 'y'] });
      const result = buildScanEfficiency(q, {});
      expect(result.allColumns).toEqual([]);
      expect(result.columns).toEqual(['x', 'y']);
    });
  });

  describe('parts and marks extraction', () => {
    test('extracts parts and marks from ProfileEvents', () => {
      const q = makeQuery({
        ProfileEvents: {
          SelectedParts: 3,
          SelectedPartsTotal: 20,
          SelectedMarks: 50,
          SelectedMarksTotal: 500,
          SelectedRanges: 4,
        },
      });
      const result = buildScanEfficiency(q, {});
      expect(result.selectedParts).toBe(3);
      expect(result.totalParts).toBe(20);
      expect(result.selectedMarks).toBe(50);
      expect(result.totalMarks).toBe(500);
      expect(result.selectedRanges).toBe(4);
    });

    test('missing ProfileEvents default to 0', () => {
      const q = makeQuery({ ProfileEvents: {} });
      const result = buildScanEfficiency(q, {});
      expect(result.selectedParts).toBe(0);
      expect(result.totalParts).toBe(0);
      expect(result.selectedMarks).toBe(0);
      expect(result.totalMarks).toBe(0);
    });
  });

  describe('mark cache hit rate', () => {
    test('calculates hit rate from hits and misses', () => {
      const q = makeQuery({
        ProfileEvents: {
          MarkCacheHits: 80,
          MarkCacheMisses: 20,
        },
      });
      const result = buildScanEfficiency(q, {});
      expect(result.markCacheHitRate).toBe(80);
    });

    test('null when no cache data', () => {
      const q = makeQuery({ ProfileEvents: {} });
      const result = buildScanEfficiency(q, {});
      expect(result.markCacheHitRate).toBeNull();
    });
  });

  describe('processing functions', () => {
    test('extracts aggregate and regular functions', () => {
      const q = makeQuery({
        used_aggregate_functions: ['count', 'sum'],
        used_functions: ['toDate', 'toString'],
      });
      const result = buildScanEfficiency(q, {});
      expect(result.aggregateFunctions).toEqual(['count', 'sum']);
      expect(result.functions).toEqual(['toDate', 'toString']);
    });
  });
});
