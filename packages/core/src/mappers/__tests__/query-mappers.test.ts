import { describe, expect, it } from 'vitest';
import { mapQueryHistoryItem } from '../query-mappers.js';

describe('mapQueryHistoryItem', { tags: ['query-analysis'] }, () => {
  it('uses combined parts and marks pruning for efficiency_score', () => {
    const item = mapQueryHistoryItem({
      query_id: 'q1',
      query_type: 'QueryFinish',
      query_kind: 'Select',
      query_start_time: '2026-03-25 10:00:00',
      query_duration_ms: 100,
      read_rows: 1000,
      read_bytes: 8000,
      result_rows: 1,
      result_bytes: 8,
      memory_usage: 0,
      query: 'SELECT count() FROM events',
      user: 'default',
      client_hostname: '',
      selected_parts: 1,
      selected_parts_total: 75,
      selected_marks: 1,
      selected_marks_total: 1,
    });

    expect(item.efficiency_score).toBeCloseTo(98.666, 2);
  });

  it('returns null efficiency_score when no pruning counters are present', () => {
    const item = mapQueryHistoryItem({
      query_id: 'q2',
      query_type: 'QueryFinish',
      query_kind: 'Select',
      query_start_time: '2026-03-25 10:00:00',
      query_duration_ms: 1,
      read_rows: 0,
      read_bytes: 0,
      result_rows: 1,
      result_bytes: 1,
      memory_usage: 0,
      query: 'SELECT 1',
      user: 'default',
      client_hostname: '',
    });

    expect(item.efficiency_score).toBeNull();
  });

  it('maps query_log object metadata arrays', () => {
    const item = mapQueryHistoryItem({
      query_id: 'q3',
      query_type: 'QueryFinish',
      query_kind: 'Select',
      query_start_time: '2026-03-25 10:00:00',
      query_duration_ms: 1,
      read_rows: 10,
      read_bytes: 100,
      result_rows: 1,
      result_bytes: 10,
      memory_usage: 0,
      query: 'SELECT name FROM system.tables',
      user: 'default',
      client_hostname: '',
      databases: ['system'],
      tables: ['system.tables'],
      columns: ['name', 'database'],
    });

    expect(item.databases).toEqual(['system']);
    expect(item.tables).toEqual(['system.tables']);
    expect(item.columns).toEqual(['name', 'database']);
  });
});
