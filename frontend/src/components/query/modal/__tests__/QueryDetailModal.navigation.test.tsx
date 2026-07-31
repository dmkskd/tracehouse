import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { QuerySeries } from '@tracehouse/core';

const mocks = vi.hoisted(() => ({
  getQueryDetail: vi.fn(),
}));

vi.mock('../../../../providers/ClickHouseProvider', () => ({
  useClickHouseServices: () => ({
    queryAnalyzer: { getQueryDetail: mocks.getQueryDetail },
  }),
}));

vi.mock('../../../shared/RequiresCapability', () => ({
  useCapabilityCheck: () => ({ available: true }),
}));

vi.mock('../../../../stores/userPreferenceStore', () => ({
  useUserPreferenceStore: () => ({ experimentalEnabled: false }),
}));

vi.mock('../../../shared/ModalWrapper', () => ({
  ModalWrapper: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

vi.mock('../hooks/useQueryDetail', () => ({
  useQueryDetail: (query: QuerySeries | null) => ({
    queryDetail: query ? {
      query_id: query.query_id,
      query: query.label,
      query_kind: 'SELECT',
      query_duration_ms: query.duration_ms,
      query_start_time: query.start_time,
      is_initial_query: query.query_id === 'root-query' ? 1 : 0,
      initial_query_id: 'root-query',
      ProfileEvents: {},
    } : null,
    isLoading: false,
    error: null,
    fetchSettingsDefaults: vi.fn(),
  }),
}));

vi.mock('../hooks/useQueryTopology', () => ({
  useQueryTopology: () => ({
    subQueries: [{ query_id: 'child-query' }],
    isLoading: false,
    coordinator: null,
    distributedTopology: null,
    isResolved: true,
  }),
}));

vi.mock('../hooks/useQueryLogs', () => ({ useQueryLogs: () => ({}) }));
vi.mock('../hooks/useQuerySpans', () => ({ useQuerySpans: () => ({}) }));
vi.mock('../hooks/useQueryFlamegraph', () => ({ useQueryFlamegraph: () => ({}) }));
vi.mock('../hooks/useQueryThreads', () => ({ useQueryThreads: () => ({}) }));
vi.mock('../hooks/useSimilarQueries', () => ({
  useSimilarQueries: () => ({ similarQueries: [], isLoading: false }),
}));
vi.mock('../hooks/useQueryTimelines', () => ({ useQueryTimelines: () => ({}) }));

vi.mock('../tabs/OverviewTab', () => ({
  OverviewTab: () => <div data-testid="overview-tab">Overview</div>,
}));
vi.mock('../tabs/SqlTab', () => ({ SqlTab: () => null }));
vi.mock('../tabs/DistributedTab', () => ({
  DistributedTab: ({
    activeQueryId,
    onNavigateToQuery,
  }: {
    activeQueryId: string;
    onNavigateToQuery: (queryId: string) => void;
  }) => (
    <button onClick={() => onNavigateToQuery('child-query')}>
      distributed-{activeQueryId}
    </button>
  ),
}));
vi.mock('../tabs/DetailsTab', () => ({ DetailsTab: () => null }));
vi.mock('../tabs/AnalyticsTab', () => ({ AnalyticsTab: () => null }));
vi.mock('../tabs/ObjectStorageTab', () => ({ ObjectStorageTab: () => null }));
vi.mock('../tabs/HistoryTab', () => ({ HistoryTab: () => null }));
vi.mock('../tabs/XRayTab', () => ({ XRayTab: () => null }));
vi.mock('../tabs/SpansTab', () => ({ SpansTab: () => null }));
vi.mock('../../QueryDetail', () => ({ ThreadBreakdownSection: () => null }));
vi.mock('../../../tracing/TraceLogViewer', () => ({ TraceLogViewer: () => null }));
vi.mock('../../../tracing/SpeedscopeViewer', () => ({ SpeedscopeViewer: () => null }));
vi.mock('../../../tracing/PipelineProfileTab', () => ({ PipelineProfileTab: () => null }));

import { QueryDetailModal } from '../QueryDetailModal';

const ROOT_QUERY: QuerySeries = {
  query_id: 'root-query',
  label: 'SELECT * FROM distributed_table',
  user: 'default',
  peak_memory: 0,
  duration_ms: 100,
  cpu_us: 0,
  net_send: 0,
  net_recv: 0,
  disk_read: 0,
  disk_write: 0,
  start_time: '2026-07-31T12:00:00.000Z',
  end_time: '2026-07-31T12:00:00.100Z',
  points: [],
};

describe('QueryDetailModal related-query navigation', () => {
  beforeEach(() => {
    mocks.getQueryDetail.mockReset();
    mocks.getQueryDetail.mockResolvedValue({
      query_id: 'child-query',
      query: 'SELECT * FROM local_table',
      user: 'default',
      query_duration_ms: 80,
      query_start_time: '2026-07-31T12:00:00.010Z',
      read_bytes: 100,
      ProfileEvents: {},
    });
  });

  it('stays on Distributed when selecting another topology node', async () => {
    render(
      <MemoryRouter>
        <QueryDetailModal
          query={ROOT_QUERY}
          initialTab="distributed"
          onClose={vi.fn()}
        />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole('button', { name: 'distributed-root-query' }));

    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'distributed-child-query' })).toBeInTheDocument();
    });
    expect(screen.queryByTestId('overview-tab')).not.toBeInTheDocument();
  });
});
