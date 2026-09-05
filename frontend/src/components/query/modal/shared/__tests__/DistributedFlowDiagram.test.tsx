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
  it('falls back to the hostname, marked as unplaced, when system.clusters could not place a host', () => {
    // No clusterHosts, so no shard and replica were attributed. The machine's
    // own name is all that is left, and it is rendered in italics so it does
    // not read as a coordinate: "we could not place this host" and "this host
    // is s1r2" must not look like the same statement.
    const unplaced = inferDistributedTopology({
      rootQueryId: 'root',
      executions: [
        row({ queryId: 'root', isInitialQuery: true, queryDurationMs: 69, readRows: 55 }),
        row({
          queryId: 'child-a',
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          queryDurationMs: 21,
          readRows: 55,
        }),
      ],
    });

    render(
      <DistributedFlowDiagram
        topology={unplaced}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    expect(screen.queryByText('s2r1')).toBeNull();
    const fallback = screen.getByText('node-1-0');
    expect(fallback.getAttribute('font-style')).toBe('italic');
  });

  it('draws a labelled cube per participant and an edge carrying the rows read', () => {
    const { container } = render(
      <DistributedFlowDiagram
        topology={topology()}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    // Participants are named by where they sit in the cluster, which
    // system.clusters gave us, rather than by a hostname that is a container id
    // or a cloud hash as often as it is a name.
    expect(screen.getByText('s1r1')).toBeTruthy();
    expect(screen.getByText('s2r1')).toBeTruthy();
    expect(screen.queryByText('node-1-0')).toBeNull();
    // Once on the coordinator cube, once in the shard legend.
    expect(screen.getAllByText('Initiator')).toHaveLength(2);
    // The legend is drawn in the canvas, inside the cluster box, so it pans and
    // zooms with the cubes it describes rather than sitting under them as HTML.
    const legend = screen.getByText('replicas are lighter shades');
    expect(legend.closest('svg')).toBeTruthy();
    const [box] = container.querySelectorAll('rect[stroke-dasharray]');
    expect(Number(legend.getAttribute('y'))).toBeLessThan(
      Number(box.getAttribute('y')) + Number(box.getAttribute('height')),
    );
    // Three polygons make one cube, so two participants means six faces.
    const cubes = '[role="button"] polygon';
    expect(
      container.querySelectorAll(`${cubes}:not([data-gauge]):not([data-gauge-track])`),
    ).toHaveLength(6);
    // A gauge track per metric per cube, in two segments because the bar wraps
    // the cube's front corner, and a fill only where the node reported that
    // metric: neither participant reported memory here.
    expect(container.querySelectorAll(`${cubes}[data-gauge-track="memory"]`)).toHaveLength(4);
    expect(container.querySelectorAll(`${cubes}[data-gauge="duration"]`).length).toBeGreaterThan(0);
    expect(container.querySelectorAll(`${cubes}[data-gauge="memory"]`)).toHaveLength(0);
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

    fireEvent.click(screen.getByLabelText(/s2r1/));
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

    fireEvent.mouseEnter(screen.getByLabelText(/s2r1/));
    const tooltip = screen.getByRole('tooltip');
    expect(tooltip.textContent).toContain('child-a');
    expect(tooltip.textContent).toContain('Shard 2 remote node');

    fireEvent.mouseLeave(screen.getByLabelText(/s2r1/));
    expect(screen.queryByRole('tooltip')).toBeNull();
  });

  it('names each stripe beside the bars while the cube is hovered', () => {
    const { container } = render(
      <DistributedFlowDiagram
        topology={topology()}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    const captions = (label: string) =>
      [...container.querySelectorAll('text')].filter(node => node.textContent === label);

    // The bars are always drawn; their names appear only while pointed at, so
    // three captions per cube never become the loudest thing on the canvas.
    expect(captions('duration')).toHaveLength(0);

    fireEvent.mouseEnter(screen.getByLabelText(/s2r1/));
    expect(captions('duration')).toHaveLength(1);
    expect(captions('rows read')).toHaveLength(1);

    fireEvent.mouseLeave(screen.getByLabelText(/s2r1/));
    expect(captions('duration')).toHaveLength(0);
  });

  it('lifts the hovered cube so it is clear which one the panel describes', () => {
    render(
      <DistributedFlowDiagram
        topology={topology()}
        activeQueryId="root"
        onNavigate={vi.fn()}
        hostLabel={shortHost}
      />,
    );

    const node = screen.getByLabelText(/s2r1/).querySelector('[data-node-visual]')!;
    // Both states are the same chain of transform functions so the browser can
    // interpolate between them; at rest the scale is simply 1.
    expect(node.getAttribute('style')).toContain('scale(1)');

    // The hover target itself never moves: a cube that grew its own hit area
    // would shrink out from under the pointer and flicker.
    const target = screen.getByLabelText(/s2r1/).querySelector('rect')!;
    const before = target.getAttribute('x');

    fireEvent.mouseEnter(screen.getByLabelText(/s2r1/));
    expect(node.getAttribute('style')).toContain('scale(1.07)');
    expect(target.getAttribute('x')).toBe(before);

    fireEvent.mouseLeave(screen.getByLabelText(/s2r1/));
    expect(node.getAttribute('style')).toContain('scale(1)');
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
