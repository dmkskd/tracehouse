/**
 * The X-Ray can be driven by a source that cannot supply every field. These
 * tests pin the two rules the display layer must obey:
 *   - a field declared missing is dropped, never rendered as zero
 *   - a rate with no thread ceiling is disclosed, not shown as if it were solid
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { ProcessSample, QueryXRaySourceSelection } from '@tracehouse/core';
import { Query2DCharts } from '../Query2DCharts';
import { XRaySourceProvider } from '../../XRaySourceContext';

function sample(overrides: Partial<ProcessSample> = {}): ProcessSample {
  return {
    t: 0, elapsed: 0, thread_count: 4,
    memory_mb: 100, peak_memory_mb: 100,
    read_rows: 1000, written_rows: 0, read_bytes: 4096,
    cpu_us: 1_000_000, io_wait_us: 0, cpu_wait_us: 0,
    net_recv_wait_us: 0, net_send_wait_us: 0,
    net_send_bytes: 0, net_recv_bytes: 0,
    d_cpu_cores: 2, d_io_wait_s: 0, d_cpu_wait_s: 0,
    d_net_recv_wait_s: 0, d_net_send_wait_s: 0,
    d_read_mb: 12, d_read_rows: 500, d_written_rows: 0,
    d_net_send_kb: 0, d_net_recv_kb: 0,
    rate_clamped: false, rate_unclamped: false,
    ...overrides,
  };
}

const SAMPLES = [sample({ t: 0 }), sample({ t: 1, d_cpu_cores: 3 })];

function meta(overrides: Partial<QueryXRaySourceSelection> = {}): QueryXRaySourceSelection {
  return {
    source: 'processes_history',
    reason: 'auto',
    missing: [],
    lagMs: 0,
    ...overrides,
  };
}

function renderCharts(selection: QueryXRaySourceSelection, samples = SAMPLES) {
  return render(
    <XRaySourceProvider meta={selection}>
      <Query2DCharts samples={samples} highlightTime={null} />
    </XRaySourceProvider>,
  );
}

describe('Query2DCharts source degradation', () => {
  it('shows the read chart when the source supplies real progress counters', () => {
    renderCharts(meta());
    expect(screen.getByText(/read_bytes/i)).toBeTruthy();
  });

  it('hides the read chart rather than plotting a different quantity', () => {
    renderCharts(meta({
      source: 'query_metric_log',
      missing: ['d_read_mb', 'read_rows', 'thread_count'],
    }));
    expect(screen.queryByText(/read_bytes/i)).toBeNull();
  });

  it('always renders the CPU chart, which every source can supply', () => {
    renderCharts(meta({ source: 'query_metric_log', missing: ['d_read_mb'] }));
    expect(screen.getByText(/cpu cores/i)).toBeTruthy();
  });

  it('warns that rates are unclamped when the query reports no thread ceiling', () => {
    renderCharts(
      meta({ source: 'query_metric_log' }),
      [sample({ t: 0, rate_unclamped: true })],
    );
    expect(screen.getByText(/unclamped/i)).toBeTruthy();
  });

  it('does not warn when the samples report a thread ceiling', () => {
    renderCharts(meta({ source: 'query_metric_log' }), SAMPLES);
    expect(screen.queryByText(/unclamped/i)).toBeNull();
  });

  it('does not warn about clamping when nothing was clamped', () => {
    renderCharts(meta());
    expect(screen.queryByText(/clamped/i)).toBeNull();
  });

  it('marks clamped samples when a ceiling was applied', () => {
    renderCharts(meta(), [sample({ t: 0 }), sample({ t: 1, rate_clamped: true })]);
    expect(screen.getByText(/clamped samples marked/i)).toBeTruthy();
  });

  it('prefers the unclamped warning over the clamped one — they mean opposite things', () => {
    renderCharts(
      meta(),
      [sample({ t: 0, rate_clamped: true, rate_unclamped: true })],
    );
    expect(screen.getByText(/unclamped/i)).toBeTruthy();
    expect(screen.queryByText(/clamped samples marked/i)).toBeNull();
  });
});

describe('XRaySourceProvider default', () => {
  it('renders at full fidelity outside a provider, so existing call sites are unaffected', () => {
    render(<Query2DCharts samples={SAMPLES} highlightTime={null} />);
    expect(screen.getByText(/read_bytes/i)).toBeTruthy();
    expect(screen.queryByText(/unclamped/i)).toBeNull();
  });
});
