import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Capture, ObjectKind, Source } from '@tracehouse/core/services/object-browser';
const { browse, collect, update, state } = vi.hoisted(() => ({ browse: vi.fn(), collect: vi.fn(), update: vi.fn(),
  state: { capture: null as Capture | null, url: {} as Record<string, string> } }));
vi.mock('../useObjectCapture', () => ({ useObjectCapture: () => ({ browse, collect, capture: state.capture, loading: false, error: null, connected: true }) }));
vi.mock('../../../hooks/useUrlState', () => ({ useUrlState: () => ({ state: state.url, update }) }));
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { ObjectBrowserPage } from '../ObjectBrowserPage';
// Stands in for another page: shows the URL and the origin it was opened with.
function Landed() {
  const location = useLocation();
  return <p>{`${location.pathname}${location.search} from ${(location.state as { from?: { path: string } } | null)?.from?.path}`}</p>;
}
const renderPage = (from?: { path: string; label: string }) => render(<MemoryRouter initialEntries={[{ pathname: '/objects', state: from ? { from } : null }]}>
  <Routes><Route path="/objects" element={<ObjectBrowserPage />} /><Route path="/analytics" element={<p>Analytics page</p>} />
    <Route path="/queries" element={<Landed />} /></Routes>
</MemoryRouter>);
// Diagram menu choices persist to localStorage; Node's own global replaces jsdom's.
const memory = new Map<string, string>();
vi.stubGlobal('localStorage', { getItem: (k: string) => memory.get(k) ?? null, setItem: (k: string, v: string) => memory.set(k, v), removeItem: (k: string) => memory.delete(k) });
describe('object browser entry point', () => {
  beforeEach(() => { vi.clearAllMocks();state.capture = null;state.url = { ob_selected: '' }; });
  it('loads the catalog on entry without executing the restored scoped question', () => {
    renderPage();
    expect(browse).toHaveBeenCalledTimes(1);
    expect(collect).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Refresh catalog' })).toBeTruthy();
  });
  it('explains a source without SELECT in its column, not in the coverage notes', () => {
    state.capture = {
      schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
      startedAt: '', completedAt: '', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], verdict: { kind: 'not-checked', reason: 'provenance' },
      coverage: [{ source: 'users', state: 'not-granted', hosts: [], visibility: 'unknown', reason: 'Missing SELECT permission on system.users' }],
      observations: [{ key: 'trips', name: 'trips', kind: 'table', source: 'tables', host: 'ch-1', attributes: {} }], relationships: [],
    };
    renderPage();
    const lane = [...document.querySelectorAll('.lane')].find(element => element.querySelector('.lane-title')?.textContent === 'Users')!;
    expect(lane.querySelector('.lane-notice')?.textContent).toBe('Missing SELECT permission on system.users');
    expect(screen.queryByText('users: not-granted')).toBeNull();
    expect(screen.queryByText(/^users: Missing SELECT/)).toBeNull();
  });
  it('keeps navigation separate from explicit, transitive focus', () => {
    const observation = (key: string, kind: ObjectKind, source: Source) => ({ key, name: key, kind, source, host: 'ch-1', attributes: {} });
    state.capture = {
      schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
      startedAt: '', completedAt: '', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: 'provenance' },
      observations: [observation('alice', 'user', 'users'), observation('reader', 'role', 'roles'), observation('scope', 'grant-scope', 'grants'), observation('trips', 'table', 'tables'),
        observation('bob', 'user', 'users'), observation('writer', 'role', 'roles'), observation('tenant-filter', 'row-policy', 'row_policies'), observation('other-filter', 'row-policy', 'row_policies')],
      relationships: [
        { id: 'assignment', host: 'ch-1', from: 'alice', to: 'reader', kind: 'has-role', label: 'has role', source: 'role_grants', evidence: {} },
        { id: 'grant', host: 'ch-1', from: 'reader', to: 'scope', kind: 'grant', label: 'SELECT grant', source: 'grants', evidence: {} },
        { id: 'match', host: 'ch-1', from: 'scope', to: 'trips', kind: 'candidate-scope', label: 'candidate name match', source: 'grants', evidence: {} },
        { id: 'bob-role', host: 'ch-1', from: 'bob', to: 'reader', kind: 'has-role', label: 'has role', source: 'role_grants', evidence: {} },
        { id: 'other-grant', host: 'ch-1', from: 'writer', to: 'scope', kind: 'grant', label: 'grant', source: 'grants', evidence: {} },
        { id: 'policy-recipient', host: 'ch-1', from: 'tenant-filter', to: 'alice', kind: 'policy-recipient', label: 'configured selector', source: 'row_policies', evidence: {} },
        { id: 'policy-target', host: 'ch-1', from: 'tenant-filter', to: 'trips', kind: 'policy-target', label: 'configured filter target', source: 'row_policies', evidence: {} },
        { id: 'other-target', host: 'ch-1', from: 'other-filter', to: 'trips', kind: 'policy-target', label: 'configured filter target', source: 'row_policies', evidence: {} },
      ],
    };
    renderPage();
    const cards = () => [...document.querySelectorAll('.node-select')].map(node => node.textContent);
    const initialOrder = cards();
    fireEvent.click(screen.getByRole('button', { name: /alice user/i }));
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(cards()).toEqual(initialOrder);
    // Clicking the selected card again clears the selection.
    expect(screen.getByRole('button', { name: /alice user/i }).getAttribute('aria-pressed')).toBe('true');
    fireEvent.click(screen.getByRole('button', { name: /alice user/i }));
    expect(screen.getByRole('button', { name: /alice user/i }).getAttribute('aria-pressed')).toBe('false');
    fireEvent.click(screen.getByRole('button', { name: 'Focus alice' }));
    const dialog = screen.getByRole('dialog', { name: 'Object focus' });
    expect(within(dialog).getByRole('button', { name: /trips table/i })).toBeTruthy();
    expect(within(dialog).getByRole('button', { name: /tenant-filter row policy/i })).toBeTruthy();
    expect(within(dialog).queryByRole('button', { name: /bob user/i })).toBeNull();
    expect(within(dialog).queryByRole('button', { name: /writer role/i })).toBeNull();
    expect(within(dialog).queryByRole('button', { name: /other-filter row policy/i })).toBeNull();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Zoom in object graph' }));
    expect(within(dialog).getByRole('button', { name: 'Reset object graph zoom' }).textContent).toBe('125%');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Reset object graph zoom' }));
    expect(within(dialog).getByRole('button', { name: 'Reset object graph zoom' }).textContent).toBe('100%');
    // A click inside focus re-roots it: reader's members, including bob, are now upstream.
    fireEvent.click(within(dialog).getByRole('button', { name: /reader role/i }));
    expect(within(dialog).getByRole('heading', { level: 2 }).textContent).toContain('reader');
    expect(within(dialog).getByRole('button', { name: /bob user/i })).toBeTruthy();
    fireEvent.click(within(dialog).getByRole('button', { name: /Back to alice/ }));
    expect(within(dialog).getByRole('heading', { level: 2 }).textContent).toContain('alice');
    expect(within(dialog).queryByRole('button', { name: /bob user/i })).toBeNull();
    expect(within(dialog).queryByRole('button', { name: /Back to/ })).toBeNull();
    // A longer walk keeps the origin and the last two steps; the middle folds into "…".
    for (const name of [/reader role/i, /scope\s*privilege/i, /trips table/i, /tenant-filter row policy/i]) fireEvent.click(within(dialog).getByRole('button', { name }));
    const trail = () => within(dialog).getByRole('heading', { level: 2 }).textContent;
    expect(trail()).toBe('Focus♙alice›…›▦trips›⊟tenant-filter');
    // "…" opens a menu of the hidden steps; the header stays on one line.
    fireEvent.click(within(dialog).getByRole('button', { name: 'Show 2 more steps' }));
    expect(within(within(dialog).getByRole('menu')).getAllByRole('button').map(b => b.getAttribute('aria-label'))).toEqual(['Back to reader', 'Back to scope']);
    fireEvent.click(within(dialog).getByRole('button', { name: 'Back to reader' }));
    expect(trail()).toBe('Focus♙alice›◇reader');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close focus' }));
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(cards()).toEqual(initialOrder);
    // Double-click in the catalog opens focus; double-click on a focus card opens its detail above focus.
    fireEvent.doubleClick(screen.getByRole('button', { name: /alice user/i }));
    const focus = screen.getByRole('dialog', { name: 'Object focus' });
    fireEvent.doubleClick(within(focus).getByRole('button', { name: /alice user/i }));
    const detail = screen.getByRole('dialog', { name: 'Object detail' });
    // Relationships read from the object's side, one row per wording; a name opens that object's detail.
    expect(within(detail).getByText('Has role')).toBeTruthy();
    fireEvent.click(within(detail).getByRole('button', { name: 'reader' }));
    expect(within(screen.getByRole('dialog', { name: 'Object detail' })).getByRole('heading', { level: 2 }).textContent).toBe('reader');
    expect(within(screen.getByRole('dialog', { name: 'Object detail' })).getByText('Granted to')).toBeTruthy();
    fireEvent.doubleClick(within(screen.getByRole('dialog', { name: 'Object focus' })).getByRole('button', { name: /alice user/i }));
    expect(within(detail).getByText('ch-1 · system.users')).toBeTruthy();
    fireEvent.click(within(detail).getByRole('button', { name: 'Close' }));
    expect(screen.queryByRole('dialog', { name: 'Object detail' })).toBeNull();
    expect(screen.getByRole('dialog', { name: 'Object focus' })).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Close focus' }));
    expect(cards()).toEqual(initialOrder);
  });
  it('restores a re-rooted focus, including links that exist only for its starting table', () => {
    // Global grant: the scope links to a table only through catalog expansion for one table.
    const evidence = { access_type: 'SELECT', access_object: '', database: null, table: null, column: null, is_wildcard: 0, is_partial_revoke: 0 };
    state.capture = {
      schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
      startedAt: '', completedAt: '', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: 'provenance' },
      observations: [{ key: 'read_only', name: 'read_only', kind: 'user', source: 'users', host: 'ch-1', attributes: {} },
        { key: 'scope', name: 'SELECT ON *.*', kind: 'grant-scope', source: 'grants', host: 'ch-1', attributes: { access_type: 'SELECT', database: null, table: null, table_operations: ['SELECT'] } },
        { key: 'sampler', name: 'tracehouse.sampler', kind: 'table', source: 'tables', host: 'ch-1', attributes: { database: 'tracehouse', name: 'sampler', engine: 'MaterializedView' } }],
      relationships: [{ id: 'grant', host: 'ch-1', from: 'read_only', to: 'scope', kind: 'grant', label: 'SELECT grant', source: 'grants', evidence }],
    };
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: 'Focus tracehouse.sampler' }));
    const dialog = screen.getByRole('dialog', { name: 'Object focus' });
    expect(within(dialog).getByRole('button', { name: /read_only user/i })).toBeTruthy();
    fireEvent.click(within(dialog).getByRole('button', { name: /read_only user/i }));
    expect(within(dialog).getByRole('heading', { level: 2 }).textContent).toContain('read_only');
    // Rooted at the user, both links of the path are drawn: user → privilege and privilege → table.
    expect(within(dialog).getByRole('button', { name: /sampler\s*Materialized view/i })).toBeTruthy();
    expect(dialog.querySelectorAll('.wire-layer > path')).toHaveLength(2);
    fireEvent.click(within(dialog).getByRole('button', { name: /Back to tracehouse.sampler/ }));
    expect(within(dialog).getByRole('heading', { level: 2 }).textContent).toContain('tracehouse.sampler');
    expect(within(dialog).getByRole('button', { name: /read_only user/i })).toBeTruthy();
    expect(within(dialog).getByRole('button', { name: /SELECT\s*All tables/ })).toBeTruthy();
    // Essential lines: the global privilege's per-table line becomes one stub ending at the data band.
    fireEvent.click(within(dialog).getByRole('button', { name: 'Diagram' }));
    fireEvent.click(within(dialog).getByRole('checkbox', { name: 'Essential lines' }));
    expect(dialog.querySelectorAll('.wire-layer circle')).toHaveLength(1);
    fireEvent.click(within(dialog).getByRole('checkbox', { name: 'Essential lines' }));
    expect(dialog.querySelectorAll('.wire-layer circle')).toHaveLength(0);
  });
  it('focused on a storage server, shows the tables stored there and who can read them', () => {
    const evidence = { access_type: 'SELECT', access_object: '', database: null, table: null, column: null, is_wildcard: 0, is_partial_revoke: 0 };
    state.capture = {
      schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
      startedAt: '', completedAt: '', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: 'provenance' },
      observations: [{ key: 'reader', name: 'reader', kind: 'user', source: 'users', host: 'ch-1', attributes: {} },
        { key: 'auditor', name: 'auditor', kind: 'user', source: 'users', host: 'ch-1', attributes: {} },
        { key: 'scope', name: 'SELECT ON *.*', kind: 'grant-scope', source: 'grants', host: 'ch-1', attributes: { table_operations: ['SELECT'] } },
        { key: 'trips', name: 'lake.trips', kind: 'table', source: 'tables', host: 'ch-1', attributes: { database: 'lake', name: 'trips', engine: 'IcebergS3' } },
        { key: 'minio', name: 'http://minio:9000', kind: 'location', source: 'tables', host: 'ch-1', attributes: { server: 'http://minio:9000', backend: 'S3' } }],
      relationships: [{ id: 'grant', host: 'ch-1', from: 'reader', to: 'scope', kind: 'grant', label: 'SELECT grant', source: 'grants', evidence },
        { id: 'auditor-grant', host: 'ch-1', from: 'auditor', to: 'scope', kind: 'grant', label: 'SELECT grant', source: 'grants', evidence },
        { id: 'stored', host: 'ch-1', from: 'trips', to: 'minio', kind: 'points-to', label: 'stored on', source: 'tables', evidence: { method: 'engine-location' } }],
    };
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: 'Focus http://minio:9000' }));
    const dialog = screen.getByRole('dialog', { name: 'Object focus' });
    expect(within(dialog).getByRole('button', { name: /trips\s*IcebergS3/i })).toBeTruthy();
    expect(within(dialog).getByRole('button', { name: /SELECT.*privilege/i })).toBeTruthy();
    expect(within(dialog).getByRole('button', { name: /reader\s*user/i })).toBeTruthy();
    // Dividers where privileges end and tables start, and where external storage starts.
    expect([...dialog.querySelectorAll('.band-start')].map(lane => lane.getAttribute('data-band'))).toEqual(['data', 'external']);
    // The Diagram menu holds independent checkboxes: highlight on, routing off by default.
    fireEvent.click(within(dialog).getByRole('button', { name: /Diagram/ }));
    const toggle = (name: string) => within(dialog).getByRole('checkbox', { name });
    expect([toggle('Route lines'), toggle('Highlight on hover')].map(b => (b as HTMLInputElement).checked)).toEqual([false, true]);
    const opacities = () => [...dialog.querySelectorAll('.wire-layer > path')].map(path => path.getAttribute('opacity'));
    const before = opacities().map(Number);
    fireEvent.mouseEnter(within(dialog).getByRole('button', { name: /reader\s*user/i }).closest('.node')!);
    // The reader's lineage runs to the privilege, the table and storage; the other user's grant dims.
    // Hovering dims exactly one line: the other user's grant, outside the reader's lineage.
    const after = opacities().map(Number);
    expect(after.filter((value, i) => value < before[i])).toHaveLength(1);
    fireEvent.click(toggle('Route lines'));
    expect([toggle('Route lines'), toggle('Highlight on hover')].map(b => (b as HTMLInputElement).checked)).toEqual([true, true]);
    expect(within(dialog).getByRole('button', { name: /http:\/\/minio:9000\s*S3/ })).toBeTruthy();
    // The drawing style is picked in the same menu and applies to the diagram.
    fireEvent.click(within(dialog).getByRole('radio', { name: 'Bars' }));
    expect(dialog.querySelector('.graph-frame')!.getAttribute('data-style')).toBe('bars');
  });
  it('answers a guided question by focusing the pick, or by collecting provenance for a user and table', () => {
    const k = (kind: ObjectKind, ...names: string[]) => JSON.stringify(['test', kind, ...names]);
    state.capture = {
      schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
      startedAt: '', completedAt: '', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: 'provenance' },
      observations: [{ key: k('user', 'reader'), name: 'reader', kind: 'user', source: 'users', host: 'ch-1', attributes: {} },
        { key: k('table', 'lake', 'trips'), name: 'lake.trips', kind: 'table', source: 'tables', host: 'ch-1', attributes: { database: 'lake', name: 'trips', engine: 'IcebergS3' } },
        { key: k('location', 'http://minio:9000'), name: 'http://minio:9000', kind: 'location', source: 'tables', host: 'ch-1', attributes: { backend: 'S3' } }],
      relationships: [{ id: 'stored', host: 'ch-1', from: k('table', 'lake', 'trips'), to: k('location', 'http://minio:9000'), kind: 'points-to', label: 'stored on', source: 'tables', evidence: { method: 'engine-location' } }],
    };
    renderPage();
    expect(screen.getByRole('button', { name: /Which tables are stored externally\?.*1 storage server · 1 table/ })).toBeTruthy();
    // Shown by default; the Explore label above them collapses the row.
    const toggle = () => screen.getByRole('button', { name: /^Explore/ });
    expect(toggle().getAttribute('aria-expanded')).toBe('true');
    fireEvent.click(toggle());
    expect(screen.queryByRole('button', { name: /Which tables are stored externally\?/ })).toBeNull();
    fireEvent.click(toggle());
    fireEvent.click(screen.getByRole('button', { name: /Which tables are stored externally\?/ }));
    fireEvent.click(within(screen.getByRole('group', { name: 'Which tables are stored externally?' })).getByRole('button', { name: /http:\/\/minio:9000/ }));
    const dialog = screen.getByRole('dialog', { name: 'Object focus' });
    expect(within(dialog).getByRole('heading', { level: 2 }).textContent).toContain('http://minio:9000');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close focus' }));
    fireEvent.click(screen.getByRole('button', { name: /How does a user reach a table\?/ }));
    const picker = () => screen.getByRole('group', { name: 'How does a user reach a table?' });
    fireEvent.click(within(picker()).getByRole('button', { name: /reader/ }));
    fireEvent.change(within(picker()).getByRole('searchbox', { name: 'Find table' }), { target: { value: 'trips' } });
    fireEvent.click(within(picker()).getByRole('button', { name: /lake\.trips/ }));
    expect(collect).toHaveBeenCalledWith({ principal: 'reader', database: 'lake', table: 'trips', operation: 'SELECT' });
  });
  describe('opened by an object link', () => {
    const observation = (key: string, kind: ObjectKind, source: Source, attributes: Record<string, unknown> = {}) => ({ key, name: key, kind, source, host: 'ch-1', attributes });
    const linked = (): Capture => ({
      schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
      startedAt: '', completedAt: 'done', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: 'provenance' },
      observations: [observation('alice', 'user', 'users'), observation('bob', 'user', 'users'), observation('reader', 'role', 'roles'),
        observation('writer', 'role', 'roles'), observation('scope', 'grant-scope', 'grants'),
        { ...observation('trips', 'table', 'tables', { database: 'demo', name: 'trips', engine: 'MergeTree' }), name: 'demo.trips' }],
      relationships: [
        { id: 'a', host: 'ch-1', from: 'alice', to: 'reader', kind: 'has-role', label: 'has role', source: 'role_grants', evidence: {} },
        { id: 'w', host: 'ch-1', from: 'bob', to: 'writer', kind: 'has-role', label: 'has role', source: 'role_grants', evidence: {} },
        { id: 'g1', host: 'ch-1', from: 'reader', to: 'scope', kind: 'grant', label: 'SELECT grant', source: 'grants', evidence: {} },
        { id: 'g2', host: 'ch-1', from: 'writer', to: 'scope', kind: 'grant', label: 'SELECT grant', source: 'grants', evidence: {} },
        { id: 'm', host: 'ch-1', from: 'scope', to: 'trips', kind: 'candidate-scope', label: 'candidate name match', source: 'grants', evidence: {} }],
    });
    it('opens focus on the table for one user, shows everyone on request, and closes back to the page it came from', () => {
      state.capture = linked();
      state.url = { ob_selected: '', ob_kind: 'table', ob_db: 'demo', ob_tbl: 'trips', ob_user: 'alice' };
      renderPage({ path: '/analytics', label: 'Analytics' });
      const dialog = screen.getByRole('dialog', { name: 'Object focus' });
      expect(within(dialog).getByRole('heading', { level: 2 }).textContent).toContain('demo.trips');
      expect(within(dialog).getByText(/for alice · 1 grant path/)).toBeTruthy();
      expect(within(dialog).getByRole('button', { name: /alice user/i })).toBeTruthy();
      expect(within(dialog).queryByRole('button', { name: /bob user/i })).toBeNull();
      fireEvent.click(within(dialog).getByRole('button', { name: 'Show all principals' }));
      expect(within(dialog).getByRole('button', { name: /bob user/i })).toBeTruthy();
      fireEvent.click(within(dialog).getByRole('button', { name: 'Close focus' }));
      expect(screen.getByText('Analytics page')).toBeTruthy();
    });
    it('names what the catalog does not hold and offers similar names; without an origin closing clears the link', () => {
      state.capture = linked();
      state.url = { ob_selected: '', ob_kind: 'table', ob_db: 'demo', ob_tbl: 'trips', ob_user: 'alise' };
      renderPage();
      const alert = screen.getByRole('alert');
      expect(within(alert).getByText('Link not opened: demo.trips for alise')).toBeTruthy();
      expect(within(alert).getByText(/Not in the collected catalog: user alise/)).toBeTruthy();
      expect(screen.queryByRole('dialog')).toBeNull();
      // The similar user opens the table for that user.
      fireEvent.click(within(alert).getByRole('button', { name: 'demo.trips for alice' }));
      expect(within(screen.getByRole('dialog', { name: 'Object focus' })).getByText(/for alice · 1 grant path/)).toBeTruthy();
      state.url = { ob_selected: '', ob_kind: 'user', ob_user: 'bob' };
      cleanup();renderPage();
      fireEvent.click(within(screen.getByRole('dialog', { name: 'Object focus' })).getByRole('button', { name: 'Close focus' }));
      expect(update).toHaveBeenCalledWith({ ob_kind: '', ob_db: '', ob_tbl: '', ob_user: '', ob_role: '' });
    });
    it('carries the user a focus chain starts from into the table detail links, and back', () => {
      state.capture = linked();
      state.url = { ob_selected: '', ob_kind: 'user', ob_user: 'alice' };
      renderPage();
      const dialog = screen.getByRole('dialog', { name: 'Object focus' });
      fireEvent.click(within(dialog).getByRole('button', { name: /trips\s*MergeTree/i }));
      fireEvent.doubleClick(within(dialog).getByRole('button', { name: /trips\s*MergeTree/i }));
      const detail = screen.getByRole('dialog', { name: 'Object detail' });
      const openIn = within(detail).getByRole('group', { name: 'Open in' });
      expect(within(openIn).getByRole('button', { name: 'Merges of demo.trips' })).toBeTruthy();
      fireEvent.click(within(openIn).getByRole('button', { name: 'Failed queries by alice on demo.trips' }));
      expect(screen.getByText('/queries?user=alice&tableName=demo.trips&status=error from /objects?ob_kind=table&ob_db=demo&ob_tbl=trips&ob_user=alice')).toBeTruthy();
    });
  });
});
