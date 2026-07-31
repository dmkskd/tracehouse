import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { QueryDetail, QuerySeries } from '@tracehouse/core';

import { SqlTab } from '../SqlTab';

const mocks = vi.hoisted(() => {
  const getColumnComments = vi.fn();
  return {
    getColumnComments,
    services: { queryAnalyzer: { getColumnComments } },
  };
});

vi.mock('../../../../../providers/ClickHouseProvider', () => ({
  useClickHouseServices: () => mocks.services,
}));

const QUERY: QuerySeries = {
  query_id: 'query-id',
  label: 'SELECT query_id, user_id FROM system.query_log JOIN analytics.events USING query_id',
  user: 'default',
  peak_memory: 1024,
  duration_ms: 4,
  cpu_us: 4900,
  net_send: 0,
  net_recv: 0,
  disk_read: 1024,
  disk_write: 0,
  start_time: '2026-07-31T12:00:00.000Z',
  end_time: '2026-07-31T12:00:00.004Z',
  points: [],
};

const DETAIL = {
  query_id: QUERY.query_id,
  query: QUERY.label,
  formatted_query: QUERY.label,
  query_kind: 'SELECT',
  query_duration_ms: QUERY.duration_ms,
  user: QUERY.user,
  current_database: 'system',
  hostname: 'node-1',
  is_initial_query: 1,
  databases: ['system', 'analytics'],
  tables: ['system.query_log', 'analytics.events'],
  columns: [
    'system.query_log.query_id',
    'system.query_log.ProfileEvents',
    'analytics.events.user_id',
  ],
  used_functions: [],
  used_aggregate_functions: [],
  used_table_functions: [],
  used_formats: [],
  used_storages: [],
  Settings: {},
  ProfileEvents: {},
} as QueryDetail;

describe('SqlTab column metadata', () => {
  it('compacts and color-codes columns by source table, with catalog comments on hover', async () => {
    mocks.getColumnComments.mockResolvedValue({
      'system.query_log.query_id': 'Query identifier.',
      'analytics.events.user_id': 'Authenticated user identifier.',
    });

    render(
      <SqlTab
        q={QUERY}
        queryDetail={DETAIL}
        isSelectQuery
        onNavigateToQuery={vi.fn()}
      />,
    );

    const queryLogTable = screen.getByTitle('system.query_log');
    const eventsTable = screen.getByTitle('analytics.events');
    const queryId = screen.getByText('query_id');
    const userId = screen.getByText('user_id');

    expect(queryId).toHaveStyle({ color: queryLogTable.style.color });
    expect(userId).toHaveStyle({ color: eventsTable.style.color });
    expect(queryId.style.color).not.toBe(userId.style.color);
    expect(queryId.style.color).toBe('var(--sql-table-color-1)');
    expect(queryId.style.background).toContain('color-mix');
    expect(queryId.style.border).toContain('color-mix');
    expect(screen.queryByText('system.query_log.query_id')).not.toBeInTheDocument();
    await waitFor(() => {
      expect(queryId.title).toBe('system.query_log.query_id\nTable: system.query_log\nComment: Query identifier.');
      expect(userId.title).toBe('analytics.events.user_id\nTable: analytics.events\nComment: Authenticated user identifier.');
    });
    expect(mocks.getColumnComments).toHaveBeenCalledWith(DETAIL.columns);
  });
});
