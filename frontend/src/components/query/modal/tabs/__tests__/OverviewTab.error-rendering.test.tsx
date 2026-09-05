import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import {
  summarizeObjectStorageProfile,
  type QueryDetail,
  type QuerySeries,
} from '@tracehouse/core';
import { OverviewTab } from '../OverviewTab';

const QUERY: QuerySeries = {
  query_id: 'query-id',
  label: 'SELECT * FROM events',
  user: 'default',
  hostname: 'node-1',
  peak_memory: 1024,
  duration_ms: 1270,
  cpu_us: 1000,
  net_send: 0,
  net_recv: 0,
  disk_read: 0,
  disk_write: 0,
  start_time: '2026-07-29T22:00:00.000Z',
  end_time: '2026-07-29T22:00:01.270Z',
  status: 'ExceptionWhileProcessing',
  exception: 'Code: 160. TOO_SLOW',
  points: [],
};

const DETAIL = {
  query_id: QUERY.query_id,
  type: 'ExceptionWhileProcessing',
  exception_code: 160,
  exception: QUERY.exception,
  query: QUERY.label,
  formatted_query: QUERY.label,
  query_kind: 'SELECT',
  current_database: 'default',
  query_duration_ms: QUERY.duration_ms,
  read_rows: 500_000,
  read_bytes: 1024,
  result_rows: 0,
  tables: ['events'],
  columns: [],
  hostname: QUERY.hostname,
  is_initial_query: 1,
  initial_query_id: QUERY.query_id,
} as QueryDetail;

describe('OverviewTab error rendering', () => {
  it('surfaces the exact ClickHouse type in the summary and error banner', () => {
    render(
      <OverviewTab
        q={QUERY}
        queryDetail={DETAIL}
        isSelectQuery
        subQueries={[]}
        distributedTopology={null}
        isLoadingSubQueries={false}
        similarQueries={[]}
        isLoadingSimilarQueries={false}
        objectStorageSummary={summarizeObjectStorageProfile({})}
        showLogsCard={false}
        showHistoryCard={false}
        showXRayCard={false}
        showThreadsCard={false}
        showFlamegraphCard={false}
        onOpenTab={vi.fn()}
        onNavigateToQuery={vi.fn()}
      />,
    );

    expect(screen.getByText('error')).toBeInTheDocument();
    expect(screen.getByText('ExceptionWhileProcessing')).toBeInTheDocument();
    expect(screen.getByText('during execution')).toBeInTheDocument();
    expect(screen.getByText('Code 160')).toBeInTheDocument();
    expect(screen.getByText('Code: 160. TOO_SLOW')).toBeInTheDocument();
    expect(screen.getByText('id').nextSibling).toHaveTextContent('query-id');
    expect(screen.getByText('kind').nextSibling).toHaveTextContent('select');
    expect(screen.getByText('role').nextSibling).toHaveTextContent('initiator');
    expect(document.querySelector('.cm-editor')?.parentElement)
      .toHaveStyle({ overflow: 'hidden' });
  });

  it('aligns parent query navigation with the summary facts', () => {
    const onNavigateToQuery = vi.fn();
    render(
      <OverviewTab
        q={QUERY}
        queryDetail={{
          ...DETAIL,
          is_initial_query: 0,
          initial_query_id: 'parent-query-id',
        }}
        isSelectQuery
        subQueries={[]}
        distributedTopology={null}
        isLoadingSubQueries={false}
        similarQueries={[]}
        isLoadingSimilarQueries={false}
        objectStorageSummary={summarizeObjectStorageProfile({})}
        showLogsCard={false}
        showHistoryCard={false}
        showXRayCard={false}
        showThreadsCard={false}
        showFlamegraphCard={false}
        onOpenTab={vi.fn()}
        onNavigateToQuery={onNavigateToQuery}
      />,
    );

    const parentLink = screen.getByRole('button', { name: 'parent-q' });
    expect(screen.getByText('parent').nextSibling).toBe(parentLink);

    fireEvent.click(parentLink);
    expect(onNavigateToQuery).toHaveBeenCalledWith('parent-query-id');
  });
});
