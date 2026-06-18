import { describe, expect, it } from 'vitest';
import type { QueryHistoryItem } from '../../stores/queryStore';
import { hoverTopologyRootId, hoverTopologyRootIds } from '../queryHoverTopology';

const makeQuery = (overrides: Partial<QueryHistoryItem>): QueryHistoryItem => ({
  query_id: 'q-root',
  query_type: 'QueryFinish',
  query_kind: 'SELECT',
  query_start_time: '2026-06-18T12:00:00.000Z',
  query_duration_ms: 10,
  read_rows: 1,
  read_bytes: 1,
  result_rows: 1,
  result_bytes: 1,
  memory_usage: 1,
  query: 'SELECT 1',
  exception: null,
  user: 'default',
  client_hostname: '',
  type: 'QueryFinish',
  efficiency_score: null,
  ...overrides,
});

describe('query hover topology helpers', () => {
  it('uses query_id for coordinator rows', () => {
    expect(hoverTopologyRootId(makeQuery({ query_id: 'coordinator', is_initial_query: 1 }))).toBe('coordinator');
  });

  it('uses initial_query_id for child rows', () => {
    expect(hoverTopologyRootId(makeQuery({
      query_id: 'child',
      is_initial_query: 0,
      initial_query_id: 'parent',
    }))).toBe('parent');
  });

  it('deduplicates root ids across coordinator and child rows', () => {
    const ids = hoverTopologyRootIds([
      makeQuery({ query_id: 'parent', is_initial_query: 1 }),
      makeQuery({ query_id: 'child-a', is_initial_query: 0, initial_query_id: 'parent' }),
      makeQuery({ query_id: 'plain', is_initial_query: 1 }),
    ]);

    expect(ids).toEqual(['parent', 'plain']);
  });
});
