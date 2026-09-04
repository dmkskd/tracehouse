import React from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { inferDistributedTopology, type DistributedQueryExecutionInput } from '@tracehouse/core';
import { DistributedFlowDiagram } from '../DistributedFlowDiagram';

afterEach(cleanup);

function row(overrides: Partial<DistributedQueryExecutionInput>): DistributedQueryExecutionInput {
  return {
    queryId: 'q',
    initialQueryId: 'root',
    isInitialQuery: false,
    hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
    queryKind: 'Select',
    queryStartTimeMicroseconds: '2026-06-21 12:00:00.000000',
    queryDurationMs: 10,
    readRows: 0,
    readBytes: 0,
    writtenRows: 0,
    writtenBytes: 0,
    profileEvents: {},
    tables: ['synthetic_data.events'],
    ...overrides,
  };
}

function topology() {
  return inferDistributedTopology({
    rootQueryId: 'root',
    clusterHosts: [
      { hostName: 'chi-dev-cluster-dev-0-0', shardNum: 1, replicaNum: 1 },
      { hostName: 'chi-dev-cluster-dev-1-0', shardNum: 2, replicaNum: 1 },
    ],
    executions: [
      row({ queryId: 'root', isInitialQuery: true, queryDurationMs: 69, readRows: 55 }),
      row({
        queryId: 'child-a',
        hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
        queryDurationMs: 21,
        readRows: 55,
        readBytes: 2900,
      }),
    ],
  });
}

const shortHost = (hostname: string) => hostname.split('.')[0].replace('chi-dev-cluster-dev-', 'node-');

describe('DistributedFlowDiagram', () => {
  it('draws a labelled cube per participant and an edge carrying the rows read', () => {
    const { container } = render(
      <DistributedFlowDiagram
        topology={topology()}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    expect(screen.getByText('node-0-0')).toBeTruthy();
    expect(screen.getByText('node-1-0')).toBeTruthy();
    // Once on the coordinator cube, once in the shard legend.
    expect(screen.getAllByText('Coordinator')).toHaveLength(2);
    expect(screen.getByText('Shard 2')).toBeTruthy();
    // Three polygons make one cube, so two participants means six faces.
    expect(container.querySelectorAll('polygon')).toHaveLength(6);
    expect(container.querySelectorAll('path[marker-end]')).toHaveLength(1);
    expect(screen.getByText('55 rows · 2.83 KB')).toBeTruthy();
  });

  it('navigates when a participant is clicked', () => {
    const onNavigate = vi.fn();
    render(
      <DistributedFlowDiagram
        topology={topology()}
        activeQueryId="root"
        onNavigate={onNavigate}
        hostLabel={shortHost}
      />,
    );

    fireEvent.click(screen.getByLabelText(/node-1-0/));
    expect(onNavigate).toHaveBeenCalledWith('child-a');
  });

  it('opens a fact panel on hover instead of a native tooltip', () => {
    const { container } = render(
      <DistributedFlowDiagram
        topology={topology()}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    expect(container.querySelector('title')).toBeNull();
    expect(screen.queryByRole('tooltip')).toBeNull();

    fireEvent.mouseEnter(screen.getByLabelText(/node-1-0/));
    const tooltip = screen.getByRole('tooltip');
    expect(tooltip.textContent).toContain('child-a');
    expect(tooltip.textContent).toContain('Shard 2 child');

    fireEvent.mouseLeave(screen.getByLabelText(/node-1-0/));
    expect(screen.queryByRole('tooltip')).toBeNull();
  });

  it('reports when there is nothing to draw', () => {
    render(
      <DistributedFlowDiagram
        topology={{ ...topology(), nodes: [], shards: [] }}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    expect(screen.getByText(/No participants to draw/)).toBeTruthy();
  });
});
