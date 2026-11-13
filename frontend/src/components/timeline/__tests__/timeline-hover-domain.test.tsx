import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import type { MemoryTimeline } from '@tracehouse/core';
import { TimelineChart } from '../TimelineChart';

const base = Date.parse('2026-11-12T17:29:00Z');
const at = (seconds: number) => new Date(base + seconds * 1000).toISOString();
beforeAll(() => {
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  // jsdom has no PointerEvent: without it fireEvent.pointer* drops clientX/clientY
  vi.stubGlobal('PointerEvent', class extends MouseEvent {
    pointerId: number;
    constructor(type: string, init: PointerEventInit = {}) { super(type, init); this.pointerId = init.pointerId ?? 1; }
  });
});
function timeline(rising = false): MemoryTimeline {
  return {
    window_start: at(0), window_end: at(60), target: at(30),
    server_cpu: [{ t: at(50), v: 1_000_000 }, { t: at(55), v: 1_000_000 }],
    server_memory: [], server_network_send: [], server_network_recv: [],
    server_total_ram: 1e9, cpu_cores: 14, host_count: 1,
    queries: [{ query_id: 'visible-query', label: 'SELECT visible', user: 'default',
      query_kind: 'Select', peak_memory: 100, duration_ms: 1000, cpu_us: 10_000_000,
      net_send: 0, net_recv: 0, disk_read: 0, disk_write: 0,
      start_time: at(rising ? 54 : 50), end_time: at(rising ? 59 : 54) }],
    merges: [], mutations: [], query_count: 1, merge_count: 0, merge_peak_total: 0, mutation_count: 0,
  };
}
function hover(data: MemoryTimeline, seconds: number, y: number) {
  const onBandClick = vi.fn();
  const onHighlightItem = vi.fn();
  const view = render(<TimelineChart data={data} metricMode="cpu" height={300}
    hoverMs={base + seconds * 1000} pinnedMs={null} onHover={() => {}} onPin={() => {}}
    zoomRange={null} onZoom={() => {}} highlightedItem={null}
    onHighlightItem={onHighlightItem} onBandClick={onBandClick} />);
  const svg = view.container.querySelector('svg')!;
  vi.spyOn(svg, 'getBoundingClientRect').mockReturnValue({
    x: 0, y: 0, left: 0, top: 0, width: 1000, height: 300, right: 1000, bottom: 300, toJSON() {},
  });
  const coords = { clientX: 52 + seconds / 60 * 858, clientY: y };
  fireEvent.pointerMove(svg, coords);
  return { svg, coords, onBandClick, onHighlightItem };
}

describe('timeline hover stays within drawn samples', () => {
  it.each([{ seconds: 20, rising: false }, { seconds: 58, rising: true }])(
    'does not extrapolate an operation at $seconds seconds', ({ seconds, rising }) => {
      const { svg, coords, onBandClick, onHighlightItem } = hover(timeline(rising), seconds, 100);
      expect(screen.getByText(/Activity at/)).toBeInTheDocument();
      expect(screen.getByText('No server samples at this time')).toBeInTheDocument();
      expect(screen.queryByText('Click for details')).not.toBeInTheDocument();
      expect(onHighlightItem).not.toHaveBeenCalledWith(expect.objectContaining({ type: 'query' }));
      fireEvent.pointerDown(svg, coords);
      fireEvent.pointerUp(svg, coords);
      expect(onBandClick).not.toHaveBeenCalled();
    },
  );
  it('still selects an operation inside its drawn band', () => {
    const { onHighlightItem } = hover(timeline(), 52.5, 220);
    expect(screen.getByText('Select')).toBeInTheDocument();
    expect(screen.getByText('Click for details')).toBeInTheDocument();
    expect(onHighlightItem).toHaveBeenCalledWith(expect.objectContaining({ type: 'query', idx: 0 }));
  });
});
