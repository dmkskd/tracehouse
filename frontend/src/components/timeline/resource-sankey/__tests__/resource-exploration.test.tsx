import { createRef, useState } from 'react';
import { act, fireEvent, render, screen, within, waitFor } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { attributeTimeline, buildOperationRows, type MemoryTimeline, type QuerySeries, type OperationalEvent } from '@tracehouse/core';
import { ResourceExplorationModal } from '../ResourceExplorationModal';
import { explorationMeasurement, explorationServerSeries, resourceConsumers, groupResourceTables, groupResourceActivitiesByTable } from '../resource-exploration-model';
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
    expect(details.parentElement).toBe(accounting.parentElement);
    expect(accounting).toHaveAttribute('aria-expanded', 'true');
    fireEvent.keyDown(accounting, { key: 'Escape' });
    expect(details).not.toBeVisible();
    expect(dialog).toBeInTheDocument();
    fireEvent.click(accounting);
    fireEvent.pointerDown(within(dialog).getByRole('button', { name: 'Reset to pin' }));
    expect(details).not.toBeVisible();
    fireEvent.click(accounting);
    fireEvent.change(within(dialog).getByRole('combobox', { name: 'Resource' }), { target: { value: 'memory' } });
    expect(dialog).toHaveTextContent('Missing samples are excluded; peak memory is not substituted');
    expect(dialog).toHaveTextContent('1 missing memory samples');
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(details).not.toBeVisible();
    expect(dialog).toBeInTheDocument();
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
  it('ranks tables by combined activity usage and expands through types to operation details', () => {
    const data = fixture();
    data.queries.push({ ...query, query_id: 'larger-single-type', tables: ['db.other'], cpu_us: 210000000, zoomSamples: undefined });
    data.merges = [{ ...query, table: 'default.events', part_name: 'part-1', cpu_us: 120000000, error: 1, zoomSamples: undefined }];
    const open = vi.fn();
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" onOpenOperation={open} onClose={() => {}} />);
    const initialHeight = screen.getByTestId('resource-flow-total').getAttribute('height');
    const grouping = screen.getByRole('group', { name: 'Sankey grouping' });
    expect(within(grouping).getByRole('button', { name: 'Activity' })).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(within(grouping).getByRole('button', { name: 'Table' }));
    expect(screen.getAllByRole('button', { name: /^Expand table / }).map(node => node.getAttribute('aria-label')))
      .toEqual(['Expand table default.events', 'Expand table db.other']);
    expect(screen.getByRole('button', { name: 'Expand table default.events' })).toHaveTextContent('5 cores · 58.8%');
    expect(screen.getByRole('button', { name: 'Expand table db.other' })).toHaveTextContent('3.5 cores · 41.2%');
    expect(screen.getByTestId('resource-flow-total')).toHaveAttribute('height', initialHeight!);
    expect(screen.queryByText('Activity', { selector: 'text' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Expand table default.events' }));
    expect(screen.getByRole('button', { name: 'Expand SELECT in default.events' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Expand MERGE in default.events' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Expand SELECT in default.events' }));
    fireEvent.click(screen.getByRole('button', { name: /Open operation sampled-quer/ }));
    expect(open).toHaveBeenLastCalledWith('query', data.queries[0]);
    fireEvent.click(screen.getByRole('button', { name: 'Collapse queries' }));
    fireEvent.click(screen.getByRole('button', { name: 'Expand MERGE in default.events' }));
    fireEvent.click(screen.getByRole('button', { name: /Open operation default.events/ }));
    expect(open).toHaveBeenLastCalledWith('merge', data.merges[0]);
    expect(screen.getAllByTestId('failed-resource-flow').length).toBeGreaterThan(0);
    fireEvent.click(within(grouping).getByRole('button', { name: 'Activity' }));
    expect(screen.queryByRole('button', { name: /Open operation/ })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Expand default.events in SELECT' })).toBeInTheDocument();
  });
  it('preserves every contribution and shared table-set cost when pivoting', () => {
    const data = fixture();
    data.queries = Array.from({ length: 9 }, (_, i) => ({ ...query, query_id: `q-${i}`, tables: [`db.t${i}`] }));
    data.queries.push({ ...query, query_id: 'shared', tables: ['db.t0', 'db.t1'] });
    const { consumers } = resourceConsumers(data, base + 30000, 'cpu');
    const groups = groupResourceActivitiesByTable(consumers);
    expect(groups).toHaveLength(10);
    expect(groups.reduce((sum, g) => sum + g.value, 0)).toBe(consumers.reduce((sum, c) => sum + c.value, 0));
    expect(groups.flatMap(g => g.rows).filter(c => c.source === data.queries[9])).toHaveLength(1);
    expect(groups.find(g => g.rows.some(c => c.source === data.queries[9]))?.primaryLabel).toBe('db.t0 + db.t1');
  });
  it('limits operation leaves by contribution without changing the total or collapsing the branch', () => {
    const data = fixture();
    data.queries = Array.from({ length: 12 }, (_, i) => ({ ...query, query_id: `q-${i}`, cpu_us: (i + 1) * 60000000, zoomSamples: undefined }));
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" onClose={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Expand default.events in SELECT' }));
    const height = screen.getByTestId('resource-flow-total').getAttribute('height');
    expect(screen.getAllByRole('button', { name: /^Open operation q-/ })).toHaveLength(5);
    expect(screen.getByText('7 other operations')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^Open operation q-11/ })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Open operation q-0 / })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Show 5 more operations in default.events' }));
    expect(screen.getAllByRole('button', { name: /^Open operation q-/ })).toHaveLength(10);
    fireEvent.click(screen.getByRole('button', { name: 'Show 2 more operations in default.events' }));
    expect(screen.getAllByRole('button', { name: /^Open operation q-/ })).toHaveLength(12);
    expect(screen.queryByText(/^[0-9]+ other operations$/)).not.toBeInTheDocument();
    expect(screen.getByTestId('resource-flow-total')).toHaveAttribute('height', height!);
    expect(screen.getByRole('button', { name: 'Collapse queries' })).toBeInTheDocument();
  });
  it('reveals only five more tables and preserves totals', () => {
    const data = fixture();
    data.queries = Array.from({ length: 17 }, (_, i) => ({ ...query, query_id: `q-${i}`, tables: [`db.t${i}`], cpu_us: (18-i)*60000000, zoomSamples: undefined }));
    render(<ResourceExplorationModal data={data} atMs={base+30000} initialMode="cpu" onClose={() => {}} />);
    const count = () => screen.getAllByRole('button', { name: /^Expand db.t/ }).length;
    const height = screen.getByTestId('resource-flow-total').getAttribute('height');
    expect(count()).toBe(5);
    fireEvent.click(screen.getByRole('button', { name: 'Show 5 more tables in SELECT' }));
    expect(count()).toBe(10);
    fireEvent.keyDown(screen.getByRole('button', { name: 'Show 5 more tables in SELECT' }), { key: 'Enter' });
    expect(count()).toBe(15);
    fireEvent.click(screen.getByRole('button', { name: 'Show 2 more tables in SELECT' }));
    expect(count()).toBe(17);
    expect(screen.getByTestId('resource-flow-total')).toHaveAttribute('height',height!);
    expect(screen.queryByRole('combobox', { name: 'Operations per type' })).not.toBeInTheDocument();
  });
  it('collapses low-usage activity types and reveals them five at a time', () => {
    const data = fixture();
    data.queries = [{ ...query, cpu_us: 60000000000, zoomSamples: undefined },
      ...Array.from({ length: 12 }, (_,i) => ({ ...query, query_id: `small-${i}`, query_kind: `SMALL${i}`, cpu_us: 6000, zoomSamples: undefined }))];
    render(<ResourceExplorationModal data={data} atMs={base+30000} initialMode="cpu" onClose={() => {}} />);
    const count = () => screen.queryAllByRole('button', { name: /^Inspect SMALL/ }).length;
    const height = screen.getByTestId('resource-flow-total').getAttribute('height');
    expect(count()).toBe(0);
    fireEvent.click(screen.getByRole('button', { name: 'Show 5 more activity types' }));
    expect(count()).toBe(5);
    fireEvent.click(screen.getByRole('button', { name: 'Show 5 more activity types' }));
    expect(count()).toBe(10);
    fireEvent.click(screen.getByRole('button', { name: 'Show 2 more activity types' }));
    expect(count()).toBe(12);
    expect(screen.getByTestId('resource-flow-total')).toHaveAttribute('height',height!);
  });
  it('closes the range options on selection or Escape without closing exploration', () => {
    render(<ResourceExplorationModal data={fixture()} atMs={base+30000} initialMode="cpu" onClose={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
    expect(screen.queryByRole('group', { name: 'Attribution range' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Time range' })).toHaveTextContent('1m');
    fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
    fireEvent.click(within(screen.getByRole('group', { name: 'Attribution range' })).getByRole('button', { name: '5m' }));
    expect(screen.queryByRole('group', { name: 'Attribution range' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Time range' })).toHaveTextContent('5m');
    fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByRole('group', { name: 'Attribution range' })).not.toBeInTheDocument();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });
  it('loads individual operations only when expanded and keeps aggregate totals while revealing more', async () => {
    const data = fixture();
    data.queries = Array.from({ length: 12 }, (_,i) => ({ ...query, query_id: `q-${i}`, cpu_us: (i+1)*60000000, zoomSamples: undefined }));
    const attribution = attributeTimeline(data,{ selection: { kind: 'instant', atMs: base+30000 }, mode: 'cpu' });
    const summary = { mode: 'cpu' as const, selection: attribution.selection, warnings: [], groups: [{
      id: 'select-events', kind: 'query' as const, activity: 'SELECT', tables: ['default.events'], tableState: 'recorded' as const,
      value: attribution.total, failureValue: 0, operationCount: 12, failedCount: 0, sampledCount: 0, estimatedCount: 12,
      unavailableCount: 0, zeroCount: 0, sampledMs: 0, estimatedMs: 0, missingMs: 0,
    }] };
    const details = vi.fn(async ({ limit }: { limit: number }) => ({ operations: [...attribution.operations].sort((a,b) => b.value!-a.value!).slice(0,limit), warnings: [] }));
    const loader = Object.assign(vi.fn(async () => ({ data, summary })), { loadDetails: details });
    render(<ResourceExplorationModal data={data} atMs={base+30000} initialMode="cpu" onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Expand default.events in SELECT' })).toBeInTheDocument());
    expect(details).not.toHaveBeenCalled();
    const height = screen.getByTestId('resource-flow-total').getAttribute('height');
    fireEvent.click(screen.getByRole('button', { name: 'Expand default.events in SELECT' }));
    await waitFor(() => expect(screen.getAllByRole('button', { name: /^Open operation q-/ })).toHaveLength(5));
    expect(details).toHaveBeenLastCalledWith(expect.objectContaining({ limit: 5 }));
    fireEvent.click(screen.getByRole('button', { name: 'Show 5 more operations in default.events' }));
    await waitFor(() => expect(screen.getAllByRole('button', { name: /^Open operation q-/ })).toHaveLength(10));
    expect(details).toHaveBeenLastCalledWith(expect.objectContaining({ limit: 10 }));
    expect(loader).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('resource-flow-total')).toHaveAttribute('height',height!);
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
    expect(screen.getByText('2 cores · 50% · Failed · code 241')).toBeInTheDocument();
    expect(screen.getAllByTestId('failed-resource-flow')).toHaveLength(5);
    fireEvent.click(screen.getByRole('button', { name: 'Expand db.failed_merge in MERGE' }));
    expect(screen.getByText('1 cores · 25% · Failed · code 243')).toBeInTheDocument();
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
    expect(screen.queryByRole('group', { name: 'Attribution range' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Time range' })).toHaveTextContent('1m');
    fireEvent.click(screen.getByRole('button', { name: 'Time range' }));
    fireEvent.click(within(screen.getByRole('group', { name: 'Attribution range' })).getByRole('button', { name: '1h' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, undefined,
      { kind: 'range', startMs: base + 30000 - 1800000, endMs: base + 30000 + 1800000 }));
    await waitFor(() => expect(screen.queryByText('Loading cpu history and operation samples…')).not.toBeInTheDocument());
    expect(screen.getByRole('group', { name: 'CPU comparison' })).toHaveTextContent('Server / capacity');
    expect(screen.queryByText(/Shares of accumulated/)).not.toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Resource attribution flow' })).toHaveTextContent('100%');
    fireEvent.click(screen.getByRole('button', { name: 'Selected instant' }));
    await waitFor(() => expect(screen.getByRole('group', { name: 'CPU comparison' })).toHaveTextContent('Server CPU usage'));
  });
  it('reuses the Time Travel server selector for multiple hosts and requests that exact scope', async () => {
    const data = fixture();
    const loader = vi.fn(async () => ({ data }));
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" availableHosts={['ch-01', 'ch-02', 'ch-03', 'ch-04']} onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(loader).toHaveBeenCalledTimes(1));
    const selector = screen.getByRole('group', { name: 'Servers' });
    expect(within(selector).queryByRole('checkbox')).not.toBeInTheDocument();
    fireEvent.click(within(selector).getByRole('button', { name: 'All servers', expanded: false }));
    fireEvent.click(within(selector).getByRole('button', { name: 'Select only ch-01' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01'], undefined));
    fireEvent.click(within(selector).getByRole('checkbox', { name: 'ch-02' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01', 'ch-02'], undefined));
  });
  it('starts on the pinned server, allows switching across the cluster, and restores the pinned scope', async () => {
    const data = fixture();
    const loader = vi.fn(async () => ({ data }));
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu" hostLabel="ch-01"
      availableHosts={['ch-01', 'ch-02', 'ch-03']} onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01'], undefined));
    const selector = screen.getByRole('group', { name: 'Servers' });
    fireEvent.click(within(selector).getByRole('button', { name: 'ch-01', expanded: false }));
    expect(within(selector).getAllByRole('checkbox')).toHaveLength(3);
    expect(within(selector).getByRole('checkbox', { name: 'ch-01' })).toBeChecked();
    expect(within(selector).getByRole('checkbox', { name: 'ch-02' })).not.toBeChecked();
    fireEvent.click(within(selector).getByRole('button', { name: 'Select only ch-02' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-02'], undefined));
    fireEvent.click(within(selector).getByRole('button', { name: 'All servers (3)' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, [], undefined));
    fireEvent.click(screen.getByRole('button', { name: 'Reset to pin' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01'], undefined));
  });
  it('keeps an aggregate pin’s initial selection separate from its available servers', async () => {
    const data = fixture();
    const loader = vi.fn(async () => ({ data }));
    render(<ResourceExplorationModal data={data} atMs={base + 30000} initialMode="cpu"
      initialHosts={['ch-01', 'ch-02']} availableHosts={['ch-01', 'ch-02', 'ch-03']} onLoadResource={loader} onClose={() => {}} />);
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01', 'ch-02'], undefined));
    const selector = screen.getByRole('group', { name: 'Servers' });
    fireEvent.click(within(selector).getByRole('button', { name: '2 servers' }));
    expect(within(selector).getAllByRole('checkbox')).toHaveLength(3);
    fireEvent.click(within(selector).getByRole('button', { name: 'All servers (3)' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, [], undefined));
    fireEvent.click(screen.getByRole('button', { name: 'Reset to pin' }));
    await waitFor(() => expect(loader).toHaveBeenLastCalledWith(data, 'cpu', base + 30000, ['ch-01', 'ch-02'], undefined));
  });
  it('opens loaded event summaries without moving exploration time or repinning', () => {
    const event = (id: string, seconds: number, hostname?: string): OperationalEvent => ({
      id, title: id, occurred_at: at(seconds), hostname, kind: 'ddl', category: 'changes', severity: 'info',
      precision: 'exact', source: 'system.query_log', capability: 'query_log',
    });
    const onPin = vi.fn();
    render(<TimelineChart data={fixture()} metricMode="cpu" height={300} pinnedMs={base + 30000} hoverMs={null}
      resourceEvents={[event('Host one change', 10, 'ch-01'), event('Host two change', 30, 'ch-02'), event('Cluster change', 50), event('Outside loaded view', 120)]}
      onHover={() => {}} onPin={onPin} zoomRange={null} onZoom={() => {}} highlightedItem={null} />);
    fireEvent.click(screen.getByRole('button', { name: 'Explore resource usage…' }));
    const dialog = screen.getByRole('dialog');
    const toggle = within(dialog).getByRole('button', { name: 'Events' });
    expect(toggle).toHaveAttribute('aria-pressed', 'false');
    expect(within(dialog).queryByRole('button', { name: 'Host two change' })).not.toBeInTheDocument();
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-pressed', 'true');
    expect(within(dialog).getByRole('button', { name: 'Host two change' })).toBeInTheDocument();
    expect(within(dialog).queryByRole('button', { name: 'Outside loaded view' })).not.toBeInTheDocument();
    const marker = within(dialog).getByRole('button', { name: 'Host one change' });
    const selection = within(dialog).getByRole('slider', { name: 'Exploration time' });
    expect(selection.contains(marker)).toBe(false);
    fireEvent.mouseDown(marker);
    fireEvent.click(marker);
    expect(selection).toHaveAttribute('aria-valuenow', String(base + 30000));
    const popup = within(dialog).getByTestId('timeline-event-popover');
    expect(popup.parentElement).toBe(dialog);
    expect(popup).toHaveTextContent('Host one change');
    expect(popup).toHaveTextContent('ch-01');
    expect(popup).toHaveTextContent('info');
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(dialog).toBeInTheDocument();
    expect(within(dialog).queryByTestId('timeline-event-popover')).not.toBeInTheDocument();
    expect(onPin).not.toHaveBeenCalled();
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-pressed', 'false');
    expect(within(dialog).queryByRole('button', { name: 'Host one change' })).not.toBeInTheDocument();
    fireEvent.click(toggle);
    const servers = within(dialog).getByRole('group', { name: 'Servers' });
    fireEvent.click(within(servers).getByRole('button', { name: 'All servers' }));
    fireEvent.click(within(servers).getByRole('button', { name: 'Select only ch-01' }));
    expect(within(dialog).getByRole('button', { name: 'Host one change' })).toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: 'Cluster change' })).toBeInTheDocument();
    expect(within(dialog).queryByRole('button', { name: 'Host two change' })).not.toBeInTheDocument();
  });
  it('highlights event hover and replaces the current popup when another marker opens', () => {
    const event = (id: string, seconds: number): OperationalEvent => ({
      id, title: id, occurred_at: at(seconds), kind: 'ddl', category: 'changes', severity: 'info',
      precision: 'exact', source: 'system.query_log', capability: 'query_log', hostname: 'ch-01',
    });
    const single = event('Single event', 30);
    const viewDetails = vi.fn();
    render(<ResourceExplorationModal data={fixture()} atMs={base + 30000} initialMode="cpu" onClose={() => {}}
      events={[event('First group A', 10), event('First group B', 10.1), single, event('Second group A', 50), event('Second group B', 50.1)]}
      onViewEventDetails={viewDetails} />);
    fireEvent.click(screen.getByRole('button', { name: 'Events' }));
    const groups = screen.getAllByRole('button', { name: '2 events' });
    const border = groups[0].style.border;
    fireEvent.mouseEnter(groups[0]);
    expect(groups[0].style.border).not.toBe(border);
    expect(groups[0]).toHaveStyle({ cursor: 'pointer' });
    fireEvent.click(groups[0]);
    expect(screen.getByTestId('timeline-event-popover')).toHaveTextContent('First group A');
    fireEvent.click(screen.getByRole('button', { name: 'Single event' }));
    expect(screen.getAllByTestId('timeline-event-popover')).toHaveLength(1);
    expect(screen.getByTestId('timeline-event-popover')).toHaveTextContent('Single event');
    expect(screen.queryByText('First group A')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'View details →' }));
    expect(viewDetails).toHaveBeenCalledWith(single);
    fireEvent.click(groups[1]);
    expect(screen.getAllByTestId('timeline-event-popover')).toHaveLength(1);
    expect(screen.getByTestId('timeline-event-popover')).toHaveTextContent('Second group A');
    expect(screen.queryByRole('button', { name: 'Clear selected event' })).not.toBeInTheDocument();
    expect(screen.getByRole('slider', { name: 'Exploration time' })).toHaveAttribute('aria-valuenow', String(base + 30000));
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
