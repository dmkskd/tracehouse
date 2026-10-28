import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { QueryExplorer } from '../QueryExplorer';

const mocks = vi.hoisted(() => ({ run: vi.fn() }));
vi.mock('../../../providers/ClickHouseProvider', () => {
  const services = {
    interactiveQueryService: { run: mocks.run, supportsExplicitQueryId: () => false },
    databaseExplorer: { getFunctions: async () => [] },
  };
  return { useClickHouseServices: () => services };
});
vi.mock('../editor/SqlEditor', () => ({
  SqlEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) =>
    <textarea aria-label="Query SQL" value={value} onChange={event => onChange(event.target.value)} />,
}));
vi.mock('../charts3d', () => ({ Chart3DCanvas: () => <div data-testid="3d-chart" /> }));
const values = new Map<string, string>();
vi.stubGlobal('localStorage', { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => values.set(key, value), removeItem: (key: string) => values.delete(key) });

const query = (type = 'bar', style = '2d') => `-- @chart: type=${type} style=${style} group_by=region value=metric series=kind axes=a:metric,b:total ranges=a:0..100,b:0..100
-- @summary: value=total label='Total' unit=s
-- @summary: value=count label='Count'
SELECT 1`;
const rows = [{ region: 'West', metric: 2, kind: 'Work', total: 64, count: '454' }];
beforeEach(() => { values.clear(); mocks.run.mockReset().mockResolvedValue(rows); });
afterEach(cleanup);

describe('Query Explorer structured summaries', () => {
  it.each(['bar', 'line', 'area', 'pie', 'grouped_bar', 'stacked_bar', 'grouped_line', 'radar'])(
    'renders the shared summary for its supported %s chart', async type => {
      render(<QueryExplorer urlState={{ sql: query(type), view: 'chart' }} />);
      expect(await screen.findByLabelText('Total: 64 s')).toHaveTextContent('1.1 min');
      expect(screen.getByLabelText('Count: 454')).toHaveTextContent('454');
      fireEvent.click(screen.getByRole('button', { name: 'Table', exact: true }));
      expect(screen.getByLabelText('Total: 64 s')).toBeInTheDocument();
    },
  );

  it('binds summary configuration to executed SQL instead of unexecuted editor changes', async () => {
    render(<QueryExplorer urlState={{ sql: query(), view: 'chart' }} />);
    await screen.findByLabelText('Total: 64 s');
    fireEvent.change(screen.getByLabelText('Query SQL'), { target: { value: query().replace("label='Total'", "label='Unexecuted'") } });
    expect(screen.getByLabelText('Total: 64 s')).toBeInTheDocument();
    expect(screen.queryByLabelText('Unexecuted: 64 s')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /Run Query/ }));
    expect(await screen.findByLabelText('Unexecuted: 64 s')).toBeInTheDocument();
  });

  it('keeps the newest summary after an older request finishes', async () => {
    let completeOld!: (rows: Record<string, unknown>[]) => void;
    mocks.run.mockImplementationOnce(() => new Promise(resolve => { completeOld = resolve; }));
    render(<QueryExplorer urlState={{ sql: query(), view: 'chart' }} />);
    fireEvent.change(screen.getByLabelText('Query SQL'), { target: { value: query().replace("label='Total'", "label='New result'") } });
    // A pending request disables Run Query; changing the time range starts a replacement request.
    fireEvent.click(screen.getByRole('button', { name: '6h', exact: true }));
    await screen.findByLabelText('New result: 64 s');
    await act(async () => { completeOld([{ ...rows[0], total: 999 }]); });
    expect(screen.getByLabelText('New result: 64 s')).toBeInTheDocument();
    expect(screen.queryByLabelText('Total: 999 s')).not.toBeInTheDocument();
  });

  it('does not display metrics in 3D but retains them when switching to table view', async () => {
    render(<QueryExplorer urlState={{ sql: query('bar', '3d'), view: 'chart' }} />);
    expect(await screen.findByText('Summaries are available in 2D or table view.')).toBeInTheDocument();
    expect(screen.queryByLabelText('Result summary')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Table', exact: true }));
    expect(screen.getByLabelText('Total: 64 s')).toBeInTheDocument();
  });
});

describe('Query Explorer merge links', () => {
  function MergesPage() {
    const location = useLocation();
    return <output aria-label="Merges page">{location.pathname + location.search}</output>;
  }

  it('navigates from a merge cell to that merge on the Merges page, on the row\'s replica', async () => {
    mocks.run.mockResolvedValue([{ database: 'web', table: 'pv', part_name: 'all_1_8_3', last_failure_host: 'ch-s1r2' }]);
    const sql = `-- @meta: title='Failing merges' group='Custom'
-- @merge_link: on=part_name database=database table=table part=part_name host=last_failure_host
SELECT 1`;
    render(
      <MemoryRouter initialEntries={['/analytics']}>
        <Routes>
          <Route path="/analytics" element={<QueryExplorer urlState={{ sql, view: 'table' }} />} />
          <Route path="/merges" element={<MergesPage />} />
        </Routes>
      </MemoryRouter>,
    );
    fireEvent.click(await screen.findByRole('link', { name: 'all_1_8_3' }));
    expect(screen.getByLabelText('Merges page')).toHaveTextContent('/merges?md_db=web&md_tbl=pv&md_part=all_1_8_3&md_host=ch-s1r2');
  });
});
