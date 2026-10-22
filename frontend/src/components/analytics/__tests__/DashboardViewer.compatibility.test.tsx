import { QueryXRayDefaultContext } from '../../query/query-xray-preference';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter, useNavigate } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { useMonitoringCapabilitiesStore } from '../../../stores/monitoringCapabilitiesStore';
import { DashboardViewer } from '../DashboardViewer';
import { exportDashboardJson, loadDashboards } from '../dashboards';

const mocks = vi.hoisted(() => ({
  run: vi.fn().mockResolvedValue([]),
  copyToClipboard: vi.fn(),
}));

vi.mock('@tracehouse/ui-shared', async importOriginal => ({
  ...await importOriginal<typeof import('@tracehouse/ui-shared')>(),
  copyToClipboard: mocks.copyToClipboard,
}));

vi.mock('../editor/SqlEditor', () => ({
  SqlEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <textarea aria-label="Query SQL" value={value} onChange={event => onChange(event.target.value)} />
  ),
}));

const storedValues = new Map<string, string>();
const localStorageMock: Storage = {
  get length() {
    return storedValues.size;
  },
  clear: () => storedValues.clear(),
  getItem: key => storedValues.get(key) ?? null,
  key: index => Array.from(storedValues.keys())[index] ?? null,
  removeItem: key => {
    storedValues.delete(key);
  },
  setItem: (key, value) => {
    storedValues.set(key, value);
  },
};
vi.stubGlobal('localStorage', localStorageMock);

vi.mock('../../../providers/ClickHouseProvider', () => {
  const services = { interactiveQueryService: { run: mocks.run } };
  return { useClickHouseServices: () => services };
});

function setServerVersion(serverVersion: string): void {
  act(() => {
    useMonitoringCapabilitiesStore.getState().setCapabilities({
      probedAt: new Date('2026-07-30T00:00:00Z'),
      serverVersion,
      capabilities: [],
    });
  });
}

function NavigationControls() {
  const navigate = useNavigate();
  return <>
    <button onClick={() => navigate('/?dashboard=ops-overview')}>Navigate to overview</button>
    <button onClick={() => navigate(-1)}>Browser back</button>
  </>;
}

describe('DashboardViewer query version compatibility', () => {
  beforeEach(() => {
    localStorage.clear();
    mocks.run.mockReset().mockResolvedValue([]);
  });

  afterEach(() => {
    cleanup();
    useMonitoringCapabilitiesStore.getState().reset();
    mocks.run.mockReset().mockResolvedValue([]);
  });

  test.each([
    [true, 'Dashboard JSON copied to clipboard'],
    [false, 'Copy failed: clipboard unavailable'],
  ])('shows the export result outside the dashboard container (clipboard success: %s)', async (success, message) => {
    mocks.copyToClipboard.mockResolvedValue(success);
    render(<MemoryRouter><DashboardViewer initialDashboardId="merge-analytics" /></MemoryRouter>);
    fireEvent.click(screen.getByRole('button', { name: 'Export' }));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Dashboard only' }));

    const status = await screen.findByText(message);
    expect(status).toHaveAttribute('role', 'status');
    expect(status.parentElement).toBe(document.body);
    expect(status).toHaveStyle({ position: 'fixed', bottom: '24px', right: '24px', zIndex: '100001' });
    expect(JSON.parse(mocks.copyToClipboard.mock.lastCall![0])).toHaveProperty('title', 'Merge Analytics');
  });

  test.each(['url', 'prop'])('keeps clone, cancel, and save stable with dashboard selected by %s', async mode => {
    render(<MemoryRouter initialEntries={[mode === 'url' ? '/?dashboard=merge-analytics' : '/']}><DashboardViewer initialDashboardId={mode === 'prop' ? 'merge-analytics' : undefined} /></MemoryRouter>);
    fireEvent.click(screen.getByRole('button', { name: 'Clone' }));
    expect(await screen.findByText('Edit Dashboard')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Merge Analytics (copy)')).toBeInTheDocument();
    fireEvent.change(screen.getByDisplayValue('Merge Analytics (copy)'), { target: { value: 'My merge dashboard' } });
    expect(screen.getByDisplayValue('My merge dashboard')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(await screen.findByText('Merge Analytics', { exact: true })).toBeInTheDocument();
    expect(localStorage.getItem('tracehouse-dashboards-user')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Clone', exact: true }));
    expect(await screen.findByText('Edit Dashboard')).toBeInTheDocument();
    fireEvent.change(screen.getByDisplayValue('Merge Analytics (copy)'), { target: { value: 'Saved merge dashboard' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));
    expect(await screen.findByText('Saved merge dashboard', { exact: true })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Edit', exact: true }));
    expect(await screen.findByText('Edit Dashboard')).toBeInTheDocument();
    fireEvent.change(screen.getByDisplayValue('Saved merge dashboard'), { target: { value: 'Edited merge dashboard' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));
    expect(await screen.findByText('Edited merge dashboard', { exact: true })).toBeInTheDocument();
    expect(screen.queryByText('Edit Dashboard')).not.toBeInTheDocument();
  });

  test('exports references by default and offers embedded queries in the export dropdown', async () => {
    mocks.copyToClipboard.mockResolvedValue(true);
    render(<MemoryRouter><DashboardViewer initialDashboardId="merge-analytics" /></MemoryRouter>);
    expect(screen.queryByText('Include queries')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Export', exact: true }));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Dashboard only' }));
    expect(JSON.parse(mocks.copyToClipboard.mock.lastCall![0]).queries).toBeUndefined();
    fireEvent.click(screen.getByRole('button', { name: 'Export', exact: true }));
    fireEvent.click(screen.getByRole('menuitem', { name: 'Include queries' }));
    const bundled = JSON.parse(mocks.copyToClipboard.mock.lastCall![0]);
    expect(bundled.queries.length).toBeGreaterThan(bundled.panels.length);
    expect(screen.queryByRole('menu')).not.toBeInTheDocument();
    await screen.findByText('Dashboard JSON copied to clipboard');
  });

  test('edits embedded SQL inside a dashboard without creating a global custom query', async () => {
    render(<MemoryRouter initialEntries={['/?dashboard=merge-analytics']}><DashboardViewer /></MemoryRouter>);
    fireEvent.click(screen.getByRole('button', { name: 'Clone' }));
    fireEvent.click(screen.getByRole('button', { name: '+ New embedded query' }));
    const editor = screen.getByRole('textbox', { name: 'Query SQL', hidden: true });
    fireEvent.change(editor, { target: { value: String((editor as HTMLTextAreaElement).value).replace('SELECT 1 AS value', 'SELECT 42 AS value') } });
    fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));
    expect(await screen.findByText('Merge Analytics (copy)', { exact: true })).toBeInTheDocument();
    const saved = JSON.parse(localStorage.getItem('tracehouse-dashboards-user')!);
    expect(saved).toHaveLength(1);
    expect(saved[0].queries[0]).toContain('SELECT 42 AS value');
    expect(saved[0].filters).toHaveLength(1);
    expect(localStorage.getItem('tracehouse-custom-queries')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Edit', exact: true }));
    expect(screen.getByRole('textbox', { name: 'Query SQL', hidden: true })).toHaveValue(saved[0].queries[0]);
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(await screen.findByText('Merge Analytics (copy)', { exact: true })).toBeInTheDocument();
  });

  test('restores external URL navigation and browser back without reopening the editor', async () => {
    render(<MemoryRouter initialEntries={['/?dashboard=merge-analytics']}><NavigationControls /><DashboardViewer /></MemoryRouter>);
    fireEvent.click(screen.getByRole('button', { name: 'Edit', exact: true }));
    expect(await screen.findByText('Edit Dashboard')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(await screen.findByText('Merge Analytics', { exact: true })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Navigate to overview' }));
    expect(await screen.findByText('Operations Overview', { exact: true })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Browser back' }));
    expect(await screen.findByText('Merge Analytics', { exact: true })).toBeInTheDocument();
    expect(screen.queryByText('Edit Dashboard')).not.toBeInTheDocument();
  });

  test.each(['builtin', 'custom'])('prefills the imported name, flags a %s name clash, and imports a renamed copy', async kind => {
    const builtin = loadDashboards().find(dashboard => dashboard.id === 'wait-breakdown')!;
    const source = kind === 'builtin' ? builtin : {
      ...builtin, id: 'team-waits', title: 'Team Wait Breakdown', builtin: false,
    };
    if (kind === 'custom') localStorage.setItem('tracehouse-dashboards-user', JSON.stringify([source]));
    const payload = JSON.parse(exportDashboardJson(source, true));
    payload.queries[0] += '\n-- imported wait-breakdown SQL';
    const before = loadDashboards();

    render(<MemoryRouter><DashboardViewer /></MemoryRouter>);
    fireEvent.click(screen.getByRole('button', { name: 'Import JSON' }));
    fireEvent.change(screen.getByPlaceholderText('Paste dashboard JSON here…'), { target: { value: JSON.stringify(payload) } });
    const name = screen.getByRole('textbox', { name: 'Dashboard name' });
    expect(name).toHaveValue(source.title);
    expect(name).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByRole('alert')).toHaveTextContent('already exists');
    expect(screen.getByRole('button', { name: 'Import', exact: true })).toBeDisabled();
    expect(loadDashboards()).toEqual(before);

    fireEvent.change(name, { target: { value: ` ${source.title.toUpperCase()} ` } });
    expect(screen.getByRole('button', { name: 'Import', exact: true })).toBeDisabled();
    const newTitle = `${source.title} imported`;
    fireEvent.change(name, { target: { value: newTitle } });
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Import', exact: true })).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: 'Import', exact: true }));

    expect(await screen.findByText(newTitle, { exact: true })).toBeInTheDocument();
    await waitFor(() => expect(mocks.run.mock.calls.some(([sql]) => sql.includes('-- imported wait-breakdown SQL'))).toBe(true));
    const reloaded = loadDashboards();
    expect(reloaded).toHaveLength(before.length + 1);
    expect(reloaded.find(dashboard => dashboard.id === source.id)).toEqual(source);
    const imported = reloaded.find(dashboard => dashboard.title === newTitle)!;
    expect(imported.id).not.toBe(source.id);
    expect(imported.queries).toEqual(payload.queries);
    expect(localStorage.getItem('tracehouse-custom-queries')).toBeNull();
  });

  test('table drill updates the dashboard filter and later table selections reach the drilled panel', async () => {
    localStorage.setItem('tracehouse-dashboards-user', JSON.stringify([{
      id: 'scoped', title: 'Scoped dashboard', columns: 2,
      panels: [{ queryName: 'Custom#Root' }, { queryName: 'Custom#Companion' }],
      filters: [{ param: 'tbl', label: 'Table', query: 'SELECT filter_options' }],
      queries: [
        "-- @meta: title='Root' group='Custom'\n-- @drill: on=tbl into='Detail'\nSELECT root WHERE {{drill:tbl | 1=1}}",
        "-- @meta: title='Detail' group='Custom'\nSELECT detail WHERE {{drill:tbl | 1=1}}",
        "-- @meta: title='Companion' group='Custom'\nSELECT companion WHERE {{drill:tbl | 1=1}}",
      ],
    }]));
    mocks.run.mockImplementation(async (sql: string) => {
      if (sql.includes('SELECT filter_options')) return [{ tbl: 'app.events' }, { tbl: 'app.other' }];
      if (sql.includes('SELECT root')) return [{ tbl: 'app.events', cpu_seconds: 3 }];
      if (sql.includes('SELECT detail')) return [{ size: 'small', cpu_seconds: 2 }];
      return [{ cpu_seconds: 1 }];
    });
    const { container } = render(<MemoryRouter initialEntries={['/?dashboard=scoped']}><DashboardViewer /></MemoryRouter>);
    const cell = await within(container).findByText('app.events', { selector: 'td' });
    fireEvent.click(cell);
    await waitFor(() => expect(screen.getByRole('combobox')).toHaveValue('app.events'));
    await waitFor(() => expect(mocks.run.mock.calls.some(([sql]) => sql.includes("SELECT companion WHERE tbl = 'app.events'"))).toBe(true));
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'app.other' } });
    await waitFor(() => expect(mocks.run.mock.calls.some(([sql]) => sql.includes("SELECT detail WHERE tbl = 'app.other'"))).toBe(true));
  });

  test('keeps dashboard parameters editable in single-panel and expanded focus views', async () => {
    const originalScroll = HTMLElement.prototype.scrollIntoView;
    HTMLElement.prototype.scrollIntoView = vi.fn();
    try {
      localStorage.setItem('tracehouse-dashboards-user', JSON.stringify([{
        id: 'focus-filters', title: 'Focus filters', columns: 2,
        panels: [{ queryName: 'Custom#First' }, { queryName: 'Custom#Second' }],
        filters: [{ param: 'tbl', label: 'Table', query: 'SELECT filter_options' }],
        queries: [
          "-- @meta: title='First' group='Custom'\nSELECT first_panel WHERE {{drill:tbl | 1=1}}",
          "-- @meta: title='Second' group='Custom'\nSELECT second_panel WHERE {{drill:tbl | 1=1}}",
        ],
      }]));
      mocks.run.mockImplementation(async (sql: string) => sql.includes('SELECT filter_options')
        ? [{ tbl: 'app.events' }, { tbl: 'app.other' }] : []);
      render(<MemoryRouter initialEntries={['/?dashboard=focus-filters']}><DashboardViewer /></MemoryRouter>);
      await screen.findByRole('option', { name: 'app.events' });
      fireEvent.change(screen.getByRole('combobox', { name: 'Table' }), { target: { value: 'app.events' } });
      fireEvent.click(screen.getByRole('button', { name: 'Focus', exact: true }));
      let controls = screen.getByRole('region', { name: 'Focus view controls' });
      await within(controls).findByRole('option', { name: 'app.other' });
      expect(within(controls).getByRole('combobox', { name: 'Table' })).toHaveValue('app.events');
      fireEvent.change(within(controls).getByRole('combobox', { name: 'Table' }), { target: { value: 'app.other' } });
      await waitFor(() => expect(mocks.run.mock.calls.some(([sql]) => sql.includes("SELECT first_panel WHERE tbl = 'app.other'"))).toBe(true));
      await waitFor(() => expect(mocks.run.mock.calls.some(([sql]) => sql.includes("SELECT second_panel WHERE tbl = 'app.other'"))).toBe(true));
      fireEvent.click(within(controls).getByRole('button', { name: 'Expand all' }));
      controls = screen.getByRole('region', { name: 'Focus view controls' });
      await within(controls).findByRole('option', { name: 'app.events' });
      fireEvent.change(within(controls).getByRole('combobox', { name: 'Table' }), { target: { value: 'app.events' } });
      fireEvent.click(within(controls).getByRole('button', { name: 'Close ✕' }));
      expect(screen.queryByRole('region', { name: 'Focus view controls' })).not.toBeInTheDocument();
      await screen.findByRole('option', { name: 'app.events' });
      expect(screen.getByRole('combobox', { name: 'Table' })).toHaveValue('app.events');
    } finally {
      HTMLElement.prototype.scrollIntoView = originalScroll;
    }
  });

  test('shows the compatibility reason and does not execute an unsupported panel', async () => {
    setServerVersion('24.3.18.7');

    render(
      <MemoryRouter>
        <DashboardViewer initialDashboardId="cloud-monitoring" />
      </MemoryRouter>,
    );

    expect(await screen.findByText(
      'Not run · Requires ClickHouse ≥ 24.8 · connected server 24.3.18.7',
    )).toBeInTheDocument();

    await waitFor(() => expect(mocks.run).toHaveBeenCalled());
    expect(
      mocks.run.mock.calls.some(([sql]) => (
        typeof sql === 'string' && sql.includes('ProfileEvent_MergeTotalMilliseconds')
      )),
    ).toBe(false);
  });

  test('executes the panel at its minimum supported version', async () => {
    setServerVersion('24.8.14.39');

    render(
      <MemoryRouter>
        <DashboardViewer initialDashboardId="cloud-monitoring" />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(
        mocks.run.mock.calls.some(([sql]) => (
          typeof sql === 'string' && sql.includes('ProfileEvent_MergeTotalMilliseconds')
        )),
      ).toBe(true);
    });

    expect(screen.queryByText(/Not run · Requires ClickHouse ≥ 24\.8/)).not.toBeInTheDocument();
  });
});


describe('Series dashboard source preference', () => {
  afterEach(() => { cleanup(); useMonitoringCapabilitiesStore.getState().reset(); });

  test('reruns supported panels when the runtime default changes and explains unavailable progress bytes', async () => {
    mocks.run.mockClear();
    useMonitoringCapabilitiesStore.setState(state => ({
      probeStatus: 'done',
      flags: { ...state.flags, hasProcessesHistory: true, hasQueryMetricLogXRay: true },
    }));
    const { rerender } = render(
      <MemoryRouter><QueryXRayDefaultContext.Provider value="processes_history">
        <DashboardViewer initialDashboardId="xray" />
      </QueryXRayDefaultContext.Provider></MemoryRouter>,
    );
    await waitFor(() => expect(mocks.run.mock.calls.filter(([sql]) => sql.includes('tracehouse.processes_history'))).toHaveLength(4));
    mocks.run.mockClear();
    rerender(
      <MemoryRouter><QueryXRayDefaultContext.Provider value="query_metric_log">
        <DashboardViewer initialDashboardId="xray" />
      </QueryXRayDefaultContext.Provider></MemoryRouter>,
    );
    await waitFor(() => expect(mocks.run.mock.calls.filter(([sql]) => sql.includes('system.query_metric_log'))).toHaveLength(3));
    expect(mocks.run.mock.calls.every(([sql]) => !sql.includes('{{query_xray_overlay:'))).toBe(true);
    expect(await screen.findByText(/Sampled read_bytes is unavailable/)).toBeInTheDocument();
  });
});
