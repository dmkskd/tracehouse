import { createRef, useState } from 'react';
import { act, fireEvent, render, screen, within, waitFor } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { attributeTimeline, buildOperationRows, type MemoryTimeline, type QuerySeries } from '@tracehouse/core';
import { ResourceExplorationModal } from '../ResourceExplorationModal';
import { explorationMeasurement, explorationServerSeries, resourceConsumers, groupResourceTables } from '../resource-exploration-model';
import { TimelineChart } from '../../TimelineChart';
import { PinnedResourcePreview } from '../PinnedResourcePreview';

const base = Date.parse('2026-11-17T11:30:00Z');
const at = (seconds: number) => new Date(base + seconds * 1000).toISOString();
const query: QuerySeries = {
  query_id: 'sampled-query', query_kind: 'Select', tables: ['default.events'], label: 'SELECT sampled', user: 'default', hostname: 'ch-01',
  start_time: at(0), end_time: at(60), duration_ms: 60000, peak_memory: 999999, cpu_us: 60000000,
  net_send: 10, net_recv: 20, disk_read: 30, disk_write: 40, points: [],
  zoomSamples: [{ ms: base + 30000, memory: 100, cpu_cores: 2, net_rate: 3, disk_rate: 4 }],
};
function fixture(): MemoryTimeline {
  return {
    window_start: at(0), window_end: at(60), target: at(30), host_count: 2, cpu_cores: 4, total_cpu_cores: 8,
    server_total_ram: 1024, total_ram: 2048,
    server_cpu: [{ t: at(29), v: 3000000 }, { t: at(30), v: 4000000 }, { t: at(31), v: 5000000 }],
    per_host_cpu: { 'ch-01': [{ t: at(30), v: 1000000 }], 'ch-02': [{ t: at(30), v: 3000000 }] },
    per_host_cpu_cores: { 'ch-01': 4, 'ch-02': 4 },
    server_memory: [{ t: at(30), v: 800 }], server_network_send: [], server_network_recv: [], server_disk_read: [], server_disk_write: [],
    queries: [query, { ...query, query_id: 'average-query', label: 'SELECT average', hostname: 'ch-02', zoomSamples: undefined }],
    merges: [], mutations: [], query_count: 2, merge_count: 0, merge_peak_total: 0, mutation_count: 0,
  };
}
beforeAll(() => {
  HTMLElement.prototype.scrollTo = vi.fn();
  vi.stubGlobal('matchMedia', vi.fn(() => ({ matches: false })));
  vi.stubGlobal('ResizeObserver', class { observe() {} unobserve() {} disconnect() {} });
  vi.stubGlobal('PointerEvent', class extends MouseEvent { pointerId = 1; });
});

describe('resource exploration accounting', () => {
  it('keeps sampled interval CPU distinct from lifetime-average contributions', () => {
    const { consumers } = resourceConsumers(fixture(), base + 30000, 'cpu');
    expect(consumers.map(c => [c.value, c.basis])).toEqual([[2000000, 'sampled'], [1000000, 'average']]);
  });
  it('never adds memory peaks to concurrent memory, including stale/missing samples', () => {
    const data = fixture();
    expect(resourceConsumers(data, base + 30000, 'memory')).toMatchObject({ missingMemory: 1, consumers: [{ value: 100, basis: 'sampled' }] });
    expect(resourceConsumers(data, base + 33000, 'memory')).toEqual({ consumers: [], missingMemory: 2 });
    data.queries[0] = { ...query, zoomMissing: ['memory'] };
    expect(resourceConsumers(data, base + 30000, 'memory')).toEqual({ consumers: [], missingMemory: 2 });
  });
  it('respects lifetime boundaries, host scope and hidden activity kinds', () => {
    expect(resourceConsumers(fixture(), base - 1, 'cpu').consumers).toEqual([]);
    expect(resourceConsumers(fixture(), base + 30000, 'cpu', 'ch-02').consumers).toMatchObject([{ host: 'ch-02', value: 1000000 }]);
    expect(resourceConsumers(fixture(), base + 30000, 'cpu', '', new Set(['query'])).consumers).toEqual([]);
  });
  it('groups tables without duplicating multi-table query costs or inferring missing tables', () => {
    const data = fixture();
    data.queries = [
      { ...query, tables: ['db.a', 'db.b', 'db.a'] },
      { ...query, query_id: 'reordered', tables: ['db.b', 'db.a'] },
      { ...query, query_id: 'single', tables: ['db.a'] },
      { ...query, query_id: 'unknown', tables: undefined },
    ];
    const { consumers } = resourceConsumers(data, base + 30000, 'cpu');
    const groups = groupResourceTables(consumers);
    expect(groups).toHaveLength(3);
    expect(groups.find(g => g.label === 'db.a + db.b')).toMatchObject({ value: 4000000, tables: ['db.a', 'db.b'] });
    expect(groups.find(g => g.label === 'Table metadata unavailable')).toBeDefined();
    expect(groups.reduce((s, g) => s + g.value, 0)).toBe(consumers.reduce((s, c) => s + c.value, 0));
    const capped = groupResourceTables(consumers, 1);
    expect(capped).toHaveLength(2);
    expect(capped[1].remainder).toBe(true);
    expect(capped.flatMap(g => g.rows)).toHaveLength(4);
  });
  it('distinguishes running-query metadata gaps from an explicitly empty table list', () => {
    const data = fixture();
    data.queries = [{ ...query, tables: undefined, is_running: true }, { ...query, query_id: 'no-table', tables: [] }];
    expect(resourceConsumers(data, base + 30000, 'cpu').consumers.map(c => c.tableLabel).sort()).toEqual(['No table recorded', 'Not reported (running query)']);
  });
  it('uses bucket-contained query rates, never a future second, with stable identities after reordering', () => {
    const data = fixture();
    const initial = resourceConsumers(data, base + 30900, 'cpu').consumers;
    expect(initial.find(c => c.source === data.queries[0])?.value).toBe(2e6);
    const next = resourceConsumers(data, base + 31000, 'cpu').consumers;
    expect(next.find(c => c.source === data.queries[0])?.basis).toBe('average');
    const reordered = resourceConsumers({ ...data, queries: [...data.queries].reverse() }, base + 30900, 'cpu').consumers;
    expect(reordered.map(c => c.id).sort()).toEqual(initial.map(c => c.id).sort());
  });
  it('keeps background CPU filled from lifetime averages labelled estimated even with memory samples', () => {
    const data = fixture(); data.queries = [];
    data.merges = [{ ...query, table: 'db.events', part_name: 'part-1', zoomSamples: [{ ms: base + 30000, cpu_cores: 99, memory: 100, net_rate: 0, disk_rate: 2 }] }];
    expect(resourceConsumers(data, base + 30000, 'cpu').consumers[0]).toMatchObject({ value: 1000000, basis: 'average' });
  });
  it('does not compare host-filtered operations with aggregate memory', () => {
    const data = fixture();
    expect(explorationServerSeries(data, 'memory', 'ch-01')).toEqual([]);
    expect(explorationMeasurement(explorationServerSeries(data, 'cpu', 'ch-01'), base + 30000)).toEqual({ value: 1000000, sampleMs: base + 30000 });
    expect(explorationMeasurement(data.server_cpu, base + 40000)).toEqual({ value: null, sampleMs: null });
  });
  it('joins IO directions by timestamp rather than array position', () => {
    const data = fixture();
    data.server_disk_read = [{ t: at(30), v: 3 }, { t: at(31), v: 4 }];
    data.server_disk_write = [{ t: at(31), v: 20 }, { t: at(30), v: 10 }];
    expect(explorationServerSeries(data, 'disk', '')).toEqual([{ t: at(30), v: 13 }, { t: at(31), v: 24 }]);
  });
});

describe('pinned activity exploration', () => {
  function Chart({ splitOwner = true }: { splitOwner?: boolean }) {
    const [pin, setPin] = useState<number | null>(base + 30000);
    const [hover, setHover] = useState<number | null>(null);
    return <TimelineChart data={fixture()} metricMode="cpu" height={300} pinnedMs={pin} hoverMs={hover}
      onHover={setHover} onPin={setPin} onUnpin={() => setPin(null)} showPinnedActivity={splitOwner}
      zoomRange={null} onZoom={() => {}} highlightedItem={null} />;
  }
  it('keeps the activity interactive after chart leave; returns from the modal to the same pin', () => {
    const view = render(<Chart />);
    const card = screen.getByTestId('pinned-activity');
    expect(card).toHaveStyle({ pointerEvents: 'auto' });
    const chart = view.container.querySelector('svg')!;
    fireEvent.mouseLeave(chart);
    expect(screen.getByTestId('pinned-activity')).toBeInTheDocument();
    const trigger = screen.getByRole('button', { name: 'Explore resource usage…' });
    expect(within(card).queryByText('Active operations', { selector: 'div' })).not.toBeInTheDocument();
    expect(within(card).getByRole('img', { name: /Mini Sankey/ })).toBeInTheDocument();
    trigger.focus(); fireEvent.click(trigger);
    const dialog = screen.getByRole('dialog', { name: 'Resource usage at the selected time' });
    expect(dialog).toHaveTextContent('1 sampled · 1 estimated');
    expect(within(dialog).queryByRole('table')).not.toBeInTheDocument();
    const accounting = within(dialog).getByRole('button', { name: /Measurement details/ });
    expect(accounting).toHaveAttribute('aria-expanded', 'false');
    const details = document.getElementById(accounting.getAttribute('aria-controls')!)!;
    expect(details).not.toBeVisible();
    fireEvent.click(accounting);
    expect(details).toBeVisible();
    expect(details.parentElement).toBe(accounting.closest('.controls')?.parentElement);
    expect(accounting).toHaveAttribute('aria-expanded', 'true');
    fireEvent.change(within(dialog).getByRole('combobox', { name: 'Resource' }), { target: { value: 'memory' } });
    expect(dialog).toHaveTextContent('Unsampled per-operation peaks are excluded');
    expect(dialog).toHaveTextContent('1 active operations lack concurrent memory samples');
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
    expect(screen.getByTestId('pinned-activity')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Unpin' }));
    expect(screen.queryByTestId('pinned-activity')).not.toBeInTheDocument();
  });
  it('disables the experiment without requesting resource data or rendering its controls', () => {
    const loader = vi.fn();
    render(<TimelineChart data={fixture()} metricMode="cpu" height={300} pinnedMs={base + 30000} hoverMs={null}
      resourceSankeyEnabled={false} onLoadResource={loader} onHover={() => {}} onPin={() => {}}
      zoomRange={null} onZoom={() => {}} highlightedItem={null} />);
    expect(screen.queryByTestId('pinned-activity')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Explore resource usage…' })).not.toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(loader).not.toHaveBeenCalled();
  });
  it('expands the same loaded snapshot without refetching or silently dropping a sampled-zero INSERT', async () => {
    const original = fixture();
    original.queries[0] = { ...original.queries[0], query_kind: 'Insert', zoomSamples: undefined };
    const sampled = { ...original, queries: original.queries.map((q, i) => i ? q : { ...q, zoomSamples: [{ ms: base + 30000, cpu_cores: 0, memory: 10, net_rate: 0, disk_rate: 0 }] }) };
    let finish!: (result: { data: MemoryTimeline }) => void;
    const loader = vi.fn(() => new Promise<{ data: MemoryTimeline }>(resolve => { finish = resolve; }));
    render(<TimelineChart data={original} metricMode="cpu" height={300} pinnedMs={base + 30000} hoverMs={null}
      onLoadResource={loader} onHover={() => {}} onPin={() => {}} zoomRange={null} onZoom={() => {}} highlightedItem={null} />);
    fireEvent.click(screen.getByRole('button', { name: 'Explore resource usage…' }));
    expect(screen.getByText('Loading the pinned resource snapshot…')).toBeInTheDocument();
    await act(async () => { finish({ data: sampled }); });
    const pin = screen.getByTestId('pinned-activity');
    const dialog = screen.getByRole('dialog', { name: 'Resource usage at the selected time' });
    expect(within(pin).getByRole('note', { name: 'Zero resource contributions' })).toHaveTextContent('INSERT · 1 operation · 0 cores (sampled)');
    expect(within(dialog).getByRole('note', { name: 'Zero resource contributions' })).toHaveTextContent('INSERT · 1 operation · 0 cores (sampled)');
    expect(dialog).toHaveTextContent('1 sampled · 1 estimated');
    expect(loader).toHaveBeenCalledTimes(1);
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close resource exploration' }));
    fireEvent.click(screen.getByRole('button', { name: 'Explore resource usage…' }));
    expect(loader).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('dialog')).toHaveTextContent('INSERT · 1 operation · 0 cores (sampled)');
  });
  it('uses exact pin-time membership, matching Operations even when a nearby bucket contains short queries', () => {
    const data = fixture();
    data.queries = data.queries.map(q => ({ ...q, start_time: at(29.9), end_time: at(30.1) }));
    const pin = base + 29800;
    expect(buildOperationRows(data, { metricMode: 'cpu', sortField: 'metric', sortDir: 'desc', scope: 'pin', pinnedMs: pin, zoomRange: null })).toHaveLength(0);
    render(<TimelineChart data={data} metricMode="cpu" height={300} pinnedMs={pin} hoverMs={null}
      onHover={() => {}} onPin={() => {}} onUnpin={() => {}} zoomRange={null} onZoom={() => {}} highlightedItem={null} />);
    expect(within(screen.getByTestId('pinned-activity')).getByText('No loaded operations active at this time.')).toBeInTheDocument();
    expect(within(screen.getByTestId('pinned-activity')).queryByText(/Queries ·/)).not.toBeInTheDocument();
  });
  it('expands a table into queries and requests smooth horizontal scrolling', async () => {
    render(<ResourceExplorationModal data={fixture()} atMs={base + 30000} initialMode="cpu" onClose={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Expand default.events in SELECT' }));
    expect(screen.getByRole('button', { name: 'Collapse queries' })).toBeInTheDocument();
    expect(screen.getByText('Queries / operations')).toBeInTheDocument();
    await waitFor(() => expect(HTMLElement.prototype.scrollTo).toHaveBeenCalledWith(expect.objectContaining({ behavior: 'smooth' })));
    fireEvent.click(screen.getByRole('button', { name: 'Collapse queries' }));
    expect(screen.queryByText('Queries / operations')).not.toBeInTheDocument();
  });
  it('loads memory history on resource change and ignores a superseded response', async () => {
    const original = fixture(); original.server_memory = [];
    let finishMemory!: (result: { data: MemoryTimeline }) => void;
    const loader = vi.fn(async (_source: MemoryTimeline, mode: string) => mode === 'memory'
      ? new Promise<{ data: MemoryTimeline }>(resolve => { finishMemory = resolve; }) : { data: original });
    render(<ResourceExplorationModal data={original} atMs={base + 30000} initialMode="cpu" onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(screen.queryByRole('status')).not.toBeInTheDocument());
    fireEvent.change(screen.getByRole('combobox', { name: 'Resource' }), { target: { value: 'memory' } });
    expect(screen.getByRole('status')).toHaveTextContent('Loading memory history');
    await act(async () => { finishMemory({ data: fixture() }); });
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    expect(screen.getByRole('dialog')).toHaveTextContent('800 B');
    fireEvent.change(screen.getByRole('combobox', { name: 'Resource' }), { target: { value: 'cpu' } });
    await waitFor(() => expect(screen.queryByRole('status')).not.toBeInTheDocument());
    fireEvent.change(screen.getByRole('combobox', { name: 'Resource' }), { target: { value: 'memory' } });
    expect(screen.getByRole('status')).toBeInTheDocument();
    fireEvent.change(screen.getByRole('combobox', { name: 'Resource' }), { target: { value: 'cpu' } });
    await waitFor(() => expect(screen.queryByRole('status')).not.toBeInTheDocument());
    await act(async () => { finishMemory({ data: fixture() }); });
    expect(screen.getByRole('combobox', { name: 'Resource' })).toHaveValue('cpu');
  });
  it('reuses the Time Travel navigator for time selection, dragging and keyboard navigation', () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(base + 120000);
    render(<ResourceExplorationModal data={fixture()} atMs={base + 30000} initialMode="cpu" onClose={() => {}} />);
    const slider = screen.getByRole('slider', { name: 'Exploration time' });
    fireEvent.keyDown(slider, { key: 'ArrowRight' });
    expect(slider).toHaveAttribute('aria-valuenow', String(base + 31000));
    const navigator = screen.getByTestId('timeline-navigator');
    vi.spyOn(navigator, 'getBoundingClientRect').mockReturnValue({ x: 0, y: 0, left: 0, top: 0, right: 880, bottom: 80, width: 880, height: 80, toJSON() {} });
    const grip = screen.getByTestId('navigator-viewport');
    expect(grip).toHaveStyle({ cursor: 'grab' });
    fireEvent.mouseDown(grip, { clientX: 440, clientY: 25 });
    expect(grip).toHaveStyle({ cursor: 'grabbing' });
    fireEvent.mouseMove(window, { clientX: 460, clientY: 25 });
    fireEvent.mouseUp(window);
    expect(Number(slider.getAttribute('aria-valuenow'))).toBeGreaterThan(base + 31000);
    expect(grip).toHaveStyle({ cursor: 'grab' });
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    now.mockRestore();
  });
  it('keeps a fixed CPU scale while the cursor moves, including above-capacity estimates', () => {
    const data = fixture();
    data.queries[0] = { ...data.queries[0], zoomSamples: [...data.queries[0].zoomSamples!, { ...data.queries[0].zoomSamples![0], ms: base + 34000, cpu_cores: 16 }] };
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" onClose={() => {}} />);
    const totalBar = () => screen.getByTestId('resource-flow-total');
    const initialHeight = Number(totalBar().getAttribute('height'));
    expect(initialHeight).toBeCloseTo(230 * 3 / 8);
    const slider = screen.getByRole('slider', { name: 'Exploration time' });
    for (let i = 0; i < 4; i++) fireEvent.keyDown(slider, { key: 'ArrowRight' });
    expect(Number(totalBar().getAttribute('height'))).toBeCloseTo(230 * 17 / 8);
    for (let i = 0; i < 4; i++) fireEvent.keyDown(slider, { key: 'ArrowLeft' });
    expect(Number(totalBar().getAttribute('height'))).toBe(initialHeight);
  });
  it('hatches only failed resource shares in the preview, grouped flow, and expanded leaves', () => {
    const data = fixture();
    data.queries[0] = { ...data.queries[0], exception_code: 241, status: 'ExceptionWhileProcessing' };
    data.merges = [{ ...query, table: 'db.failed_merge', part_name: 'failed-part', error: 243, zoomSamples: undefined }];
    const preview = render(<PinnedResourcePreview data={data} atMs={base + 30000} mode="cpu" width={350} onExplore={() => {}} buttonRef={createRef<HTMLButtonElement>()} />);
    expect(screen.getAllByTestId('failed-resource-preview')).toHaveLength(2);
    preview.unmount();
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" onClose={() => {}} />);
    expect(screen.getByText(/Hatched: failed outcome/)).toHaveTextContent('2 operations');
    expect(screen.getAllByTestId('failed-resource-flow')).toHaveLength(4);
    // The SELECT table mixes 2 failed cores with 1 successful core: only 2 are hatched.
    const sourceHatches = document.querySelectorAll('rect[x="12"][fill^="url("]');
    expect(Number(sourceHatches[0].getAttribute('height'))).toBeCloseTo(230 * 2 / 8);
    fireEvent.click(screen.getByRole('button', { name: 'Expand default.events in SELECT' }));
    expect(screen.getByText('2 cores · Failed · code 241')).toBeInTheDocument();
    expect(screen.getAllByTestId('failed-resource-flow')).toHaveLength(5);
    fireEvent.click(screen.getByRole('button', { name: 'Expand db.failed_merge in MERGE' }));
    expect(screen.getByText('1 cores · Failed · code 243')).toBeInTheDocument();
  });
  it('uses the same instant contributions for the ordinary activity list and Sankey', () => {
    const data = fixture();
    const expected = attributeTimeline(data, { selection: { kind: 'instant', atMs: base + 30000 }, mode: 'cpu' });
    const view = render(<TimelineChart data={data} metricMode="cpu" height={300} resourceSankeyEnabled={false}
      hoverMs={base + 30000} pinnedMs={null} onHover={() => {}} onPin={() => {}} zoomRange={null} onZoom={() => {}} highlightedItem={null} />);
    const svg = view.container.querySelector('svg')!;
    vi.spyOn(svg, 'getBoundingClientRect').mockReturnValue({ left: 0, top: 0, width: 1000, height: 300, right: 1000, bottom: 300, x: 0, y: 0, toJSON() {} });
    fireEvent.pointerMove(svg, { clientX: 481, clientY: 1 });
    const preview = screen.getByTestId('activity-preview');
    expect(preview).toHaveTextContent('1 sampled · 1 estimated');
    expect(within(preview).getByText('2 cores')).toBeInTheDocument();
    expect(within(preview).getByText('1 cores')).toBeInTheDocument();
    expect(resourceConsumers(data, base + 30000, 'cpu').consumers.map(row => row.value)).toEqual(expected.operations.map(row => row.value));
  });
  it('offers a full-hour range from a short pin and loads that exact selection', async () => {
    const data = fixture();
    const loader = vi.fn(async () => ({ data }));
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(screen.queryByText('Loading cpu history and operation samples…')).not.toBeInTheDocument());
    fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
    fireEvent.click(within(screen.getByRole('group', { name: 'Attribution range' })).getByRole('button', { name: '1h' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, undefined,
      { kind: 'range', startMs: base + 30000 - 1800000, endMs: base + 30000 + 1800000 }));
    await waitFor(() => expect(screen.queryByText('Loading cpu history and operation samples…')).not.toBeInTheDocument());
    expect(screen.getByRole('group', { name: 'CPU comparison' })).toHaveTextContent('Server / capacity');
    expect(screen.getByText(/Attributed total = 100%/)).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Resource attribution flow' })).toHaveTextContent('100%');
    fireEvent.click(screen.getByRole('button', { name: 'Selected instant' }));
    await waitFor(() => expect(screen.getByRole('group', { name: 'CPU comparison' })).toHaveTextContent('Server CPU usage'));
  });
  it('reuses the Time Travel server selector for multiple hosts and requests that exact scope', async () => {
    const data = fixture();
    const loader = vi.fn(async () => ({ data }));
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" availableHosts={['ch-01', 'ch-02']} onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(loader).toHaveBeenCalledTimes(1));
    const selector = screen.getByRole('group', { name: 'Servers' });
    fireEvent.click(within(selector).getByRole('button', { name: 'ch-01' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01'], undefined));
    fireEvent.click(within(selector).getByRole('button', { name: 'ch-02' }), { shiftKey: true });
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01', 'ch-02'], undefined));
  });
  it('opens query or merge details while preserving the exploration and expansion', () => {
    const data = fixture();
    data.merges = [{ ...query, table: 'db.merged', part_name: 'part-1', zoomSamples: undefined }];
    const open = vi.fn(), close = vi.fn();
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" onOpenOperation={open} onClose={close} />);
    fireEvent.click(screen.getByRole('button', { name: 'Expand default.events in SELECT' }));
    fireEvent.click(screen.getByRole('button', { name: /Open operation sampled-quer/ }));
    expect(close).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Collapse queries' })).toBeInTheDocument();
    expect(open).toHaveBeenLastCalledWith('query', data.queries[0]);
    fireEvent.click(screen.getByRole('button', { name: 'Expand db.merged in MERGE' }));
    fireEvent.click(screen.getByRole('button', { name: /Open operation db.merged/ }));
    expect(open).toHaveBeenLastCalledWith('merge', data.merges[0]);
  });
  it('unpins with Escape when exploration is closed', () => {
    render(<Chart />);
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByTestId('pinned-activity')).not.toBeInTheDocument();
  });
  it('does not duplicate the sticky activity in other split-view charts', () => {
    render(<Chart splitOwner={false} />);
    expect(screen.queryByTestId('pinned-activity')).not.toBeInTheDocument();
  });
  it('allows exploration of loaded operations even without a measured server line', () => {
    const data = fixture(); data.server_cpu = [];
    render(<TimelineChart data={data} metricMode="cpu" height={300} pinnedMs={base + 30000} hoverMs={null}
      onHover={() => {}} onPin={() => {}} onUnpin={() => {}} zoomRange={null} onZoom={() => {}} highlightedItem={null} />);
    expect(screen.getByTestId('pinned-activity')).toHaveTextContent('Server: Unavailable');
    fireEvent.click(screen.getByRole('button', { name: 'Explore resource usage…' }));
    expect(screen.getByRole('dialog')).toHaveTextContent('Unavailable');
  });
  it('does not present missing memory history as a peak-based snapshot', () => {
    const data = fixture(); data.queries = [{ ...query, zoomSamples: undefined }];
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="memory" onClose={() => {}} />);
    expect(screen.getByText(/Concurrent memory attribution is unavailable/)).toBeInTheDocument();
    expect(screen.queryByRole('img', { name: /Loaded operation usage/ })).not.toBeInTheDocument();
  });
});
