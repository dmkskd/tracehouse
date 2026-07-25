import React, { useState } from 'react';
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  QueryExecutionAnalysisError,
  type QueryDetail,
} from '@tracehouse/core';
import {
  ClickHouseContext,
  type ClickHouseServices,
} from '@tracehouse/ui-shared';
import { useMonitoringCapabilitiesStore } from '../../../../../stores/monitoringCapabilitiesStore';
import {
  AnalyticsTab,
  type AnalyticsSubTab,
} from '../AnalyticsTab';

const connectionStoreState = vi.hoisted(() => ({
  profiles: [] as Array<{
    id: string;
    config: { send_receive_timeout: number };
  }>,
  activeProfileId: null as string | null,
  setConnectionFormOpen: vi.fn(),
}));

vi.mock('../../../../../stores/connectionStore', () => ({
  useConnectionStore: (
    selector: (state: typeof connectionStoreState) => unknown,
  ) => selector(connectionStoreState),
}));

const QUERY_DETAIL = {
  query_id: 'historical-query-id',
  query_kind: 'SELECT',
  query: 'SELECT count() FROM events',
  formatted_query: 'SELECT count()\nFROM events',
  current_database: 'analytics',
  query_duration_ms: 56,
  memory_usage: 15 * 1024 * 1024,
  ProfileEvents: {
    UserTimeMicroseconds: 60_000,
    SystemTimeMicroseconds: 4_400,
  },
} as QueryDetail;

function setExplainAnalyzeCapability(available: boolean, serverVersion = '26.7.1.1') {
  const current = useMonitoringCapabilitiesStore.getState();
  useMonitoringCapabilitiesStore.setState({
    flags: { ...current.flags, hasExplainAnalyze: available },
    probeStatus: 'done',
    capabilities: {
      probedAt: new Date(),
      serverVersion,
      capabilities: [{
        id: 'explain_analyze',
        label: 'EXPLAIN ANALYZE',
        description: 'Runtime plan',
        available,
        category: 'profiling',
        detail: available
          ? `Available (v${serverVersion})`
          : `Requires ClickHouse 26.7+ (current: v${serverVersion})`,
        source: 'server version ≥ 26.7',
      }],
    },
  });
}

function Harness({ services }: { services: ClickHouseServices }) {
  const [tab, setTab] = useState<AnalyticsSubTab>('scan_efficiency');
  return (
    <ClickHouseContext.Provider value={services}>
      <AnalyticsTab
        analyticsSubTab={tab}
        onSubTabChange={setTab}
        queryDetail={QUERY_DETAIL}
        isLoadingDetail={false}
      />
    </ClickHouseContext.Provider>
  );
}

function StaticAnalytics({
  services,
  queryDetail,
  analyticsSubTab = 'scan_efficiency',
}: {
  services: ClickHouseServices;
  queryDetail: QueryDetail;
  analyticsSubTab?: AnalyticsSubTab;
}) {
  return (
    <ClickHouseContext.Provider value={services}>
      <AnalyticsTab
        analyticsSubTab={analyticsSubTab}
        onSubTabChange={vi.fn()}
        queryDetail={queryDetail}
        isLoadingDetail={false}
      />
    </ClickHouseContext.Provider>
  );
}

describe('RuntimeAnalysisTab', () => {
  beforeEach(() => {
    setExplainAnalyzeCapability(true);
  });

  afterEach(() => {
    cleanup();
    connectionStoreState.profiles = [];
    connectionStoreState.activeProfileId = null;
    connectionStoreState.setConnectionFormOpen.mockReset();
    useMonitoringCapabilitiesStore.setState(
      useMonitoringCapabilitiesStore.getInitialState(),
      true,
    );
  });

  it('orders Runtime Analysis before Column Cost and replays historical SQL only after confirmation', async () => {
    const analyze = vi.fn().mockResolvedValue({
      kind: 'explain_analyze',
      query: QUERY_DETAIL.query,
      output: [
        'Query summary:',
        '  Time:        1.00 ms (planning 0.40 ms · execution 0.60 ms)',
        '  Read:        10 rows, 80 B (10 thousand rows/s., 80 KB/s.)',
        '  Peak memory: 1.00 KiB',
        '',
        'Output: count()',
        '',
        'ReadFromMergeTree (events)',
        '│  I/O: rows 0 → 10 · 0 B → 80 B',
        '│    time 400.00 us (66.7%) · parallelism 1.00/1',
      ].join('\n'),
      processors: false,
      queryId: 'analysis-query-id',
    });
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze,
      },
    } as unknown as ClickHouseServices;

    render(<Harness services={services} />);

    const subTabs = screen.getAllByRole('button')
      .filter(button => ['Scan Efficiency', 'Explain Analyze', 'Column Cost'].includes(button.textContent ?? ''));
    expect(subTabs.map(button => button.textContent)).toEqual([
      'Scan Efficiency',
      'Explain Analyze',
      'Column Cost',
    ]);

    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }));
    expect(screen.getByRole('link', { name: /EXPLAIN ANALYZE documentation/i })).toHaveAttribute(
      'href',
      'https://clickhouse.com/docs/reference/statements/explain#explain-analyze',
    );
    expect(screen.getByText(QUERY_DETAIL.query)).toBeInTheDocument();
    expect(screen.getByText('Previous execution')).toBeInTheDocument();
    expect(screen.getByText('56ms')).toBeInTheDocument();
    expect(screen.getByText('64.4ms')).toBeInTheDocument();
    expect(screen.getByText('15 MB')).toBeInTheDocument();
    expect(screen.getByText(/next run may differ/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('checkbox', { name: /processor-level timing stats/i }));
    fireEvent.click(screen.getByRole('button', { name: /run execution analysis/i }));

    const dialog = screen.getByRole('dialog');
    expect(within(dialog).getByText('Confirm query execution?')).toBeInTheDocument();
    expect(within(dialog).queryByText(QUERY_DETAIL.query)).not.toBeInTheDocument();
    expect(within(dialog).queryByText('Previous execution')).not.toBeInTheDocument();
    expect(within(dialog).queryByRole('link')).not.toBeInTheDocument();
    expect(within(dialog).queryByRole('checkbox')).not.toBeInTheDocument();
    expect(analyze).not.toHaveBeenCalled();

    fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm' }));

    await waitFor(() => expect(analyze).toHaveBeenCalledOnce());
    expect(analyze).toHaveBeenCalledWith(
      QUERY_DETAIL.query,
      'TraceHouse:Queries:historicalQueryExecutionAnalysis',
      {
        database: 'analytics',
        processors: true,
        queryId: expect.any(String),
      },
    );
    expect(await screen.findByText('ReadFromMergeTree')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Scan Efficiency' }));
    expect(screen.queryByText('ReadFromMergeTree')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }));
    expect(await screen.findByText('ReadFromMergeTree')).toBeInTheDocument();
    expect(analyze).toHaveBeenCalledOnce();

    fireEvent.click(screen.getByRole('tab', { name: 'Raw plan' }));
    expect(await screen.findByText(/Query summary:/)).toHaveTextContent('Peak memory: 1.00 KiB');
    expect(screen.getByRole('button', { name: 'Run again' })).toBeInTheDocument();
  });

  it('explains why runtime analysis is unavailable on an older server', () => {
    setExplainAnalyzeCapability(false, '23.8.16.40');
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze: vi.fn(),
      },
    } as unknown as ClickHouseServices;

    render(<Harness services={services} />);
    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }));

    const capabilityAlert = screen.getByRole('alert');
    expect(capabilityAlert).toHaveTextContent('Runtime Analysis is unavailable');
    expect(capabilityAlert).toHaveTextContent('MISSING CAPABILITY');
    expect(capabilityAlert).toHaveTextContent('ClickHouse 26.7 or later');
    expect(capabilityAlert).toHaveTextContent('ClickHouse 23.8.16.40');
    expect(capabilityAlert).toHaveTextContent('Requires ClickHouse 26.7+ (current: v23.8.16.40)');
    expect(screen.queryByRole('button', { name: /run execution analysis/i })).not.toBeInTheDocument();
  });

  it('points connection timeouts to the existing Send/Recv Timeout setting', async () => {
    connectionStoreState.profiles = [{
        id: 'connection-1',
        config: {
          send_receive_timeout: 30,
        },
      }];
    connectionStoreState.activeProfileId = 'connection-1';
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze: vi.fn().mockRejectedValue(
          new QueryExecutionAnalysisError(
            'Failed to analyze query execution: Timeout error.',
            'timeout',
          ),
        ),
      },
    } as unknown as ClickHouseServices;

    render(<Harness services={services} />);
    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }));
    fireEvent.click(screen.getByRole('button', { name: /run execution analysis/i }));
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm' }));

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('Timeout error.');
    expect(alert).toHaveTextContent('current Send/Recv Timeout is 30s');
    expect(alert).toHaveTextContent('Advanced Settings');

    fireEvent.click(within(alert).getByRole('button', { name: 'Edit connection' }));
    expect(connectionStoreState.setConnectionFormOpen).toHaveBeenCalledWith(
      true,
      'connection-1',
    );
  });

  it('does not present query failures as connection timeouts', async () => {
    connectionStoreState.profiles = [{
      id: 'connection-1',
      config: {
        send_receive_timeout: 30,
      },
    }];
    connectionStoreState.activeProfileId = 'connection-1';
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze: vi.fn().mockRejectedValue(
          new QueryExecutionAnalysisError(
            'Failed to analyze query execution: [Code 160] The maximum sleep time is 3000000 microseconds.',
            'query',
          ),
        ),
      },
    } as unknown as ClickHouseServices;

    render(<Harness services={services} />);
    fireEvent.click(screen.getByRole('button', { name: 'Explain Analyze' }));
    fireEvent.click(screen.getByRole('button', { name: /run execution analysis/i }));
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'Confirm' }));

    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('maximum sleep time');
    expect(alert).not.toHaveTextContent('Send/Recv Timeout');
    expect(alert).not.toHaveTextContent('Advanced Settings');
    expect(within(alert).queryByRole('button', { name: 'Edit connection' })).not.toBeInTheDocument();
  });

  it('does not offer Runtime Analysis for non-SELECT history entries', () => {
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze: vi.fn(),
      },
    } as unknown as ClickHouseServices;

    render(
      <StaticAnalytics
        services={services}
        analyticsSubTab="runtime_analysis"
        queryDetail={{
          ...QUERY_DETAIL,
          query_kind: 'INSERT',
          query: 'INSERT INTO events SELECT * FROM staging_events',
        } as QueryDetail}
      />,
    );

    expect(screen.queryByRole('button', { name: 'Explain Analyze' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /run execution analysis/i })).not.toBeInTheDocument();
    const subTabs = screen
      .getAllByRole('button')
      .filter(button => ['Scan Efficiency', 'Explain Analyze', 'Column Cost'].includes(button.textContent ?? ''));
    expect(subTabs.map(button => button.textContent)).toEqual(['Scan Efficiency', 'Column Cost']);
  });
});
