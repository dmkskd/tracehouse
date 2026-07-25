import React, { useState } from 'react';
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { QueryDetail } from '@tracehouse/core';
import {
  ClickHouseContext,
  type ClickHouseServices,
} from '@tracehouse/ui-shared';
import { useMonitoringCapabilitiesStore } from '../../../../../stores/monitoringCapabilitiesStore';
import {
  AnalyticsTab,
  type AnalyticsSubTab,
} from '../AnalyticsTab';

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
    useMonitoringCapabilitiesStore.setState(
      useMonitoringCapabilitiesStore.getInitialState(),
      true,
    );
  });

  it('orders Runtime Analysis before Column Cost and replays historical SQL only after confirmation', async () => {
    const analyze = vi.fn().mockResolvedValue({
      kind: 'explain_analyze',
      query: QUERY_DETAIL.query,
      output: 'Query summary:\n  Peak memory: 1.00 KiB\nReadFromMergeTree',
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
