import { act, render } from '@testing-library/react';
import { useEffect } from 'react';
import { MemoryRouter, useLocation, useNavigate, type Location, type NavigateFunction } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import { useSearchParams } from '../useAppLocation';
import { useUrlState } from '../useUrlState';

interface Seen { setter: unknown; write: ReturnType<typeof useSearchParams>[1]; navigate: NavigateFunction; location: Location }
/** Reports what the hooks return on every render. */
function Probe({ report }: { report: (seen: Seen) => void }) {
  const [, setSearchParams] = useSearchParams();
  const navigate = useNavigate(), location = useLocation();
  useEffect(() => report({ setter: setSearchParams, write: setSearchParams, navigate, location }));
  return null;
}
const origin = () => ({ from: { path: '/objects', label: 'Object browser' } });

describe('useSearchParams', () => {
  it('keeps the navigation state on URL writes, and a new state object does not change the setter', () => {
    const setters = new Set<unknown>();
    let seen: Seen | null = null;
    render(<MemoryRouter initialEntries={[{ pathname: '/timetravel', state: origin() }]}>
      <Probe report={s => { setters.add(s.setter);seen = s; }} /></MemoryRouter>);
    // A browser history clones state on every write; a new object with the same content stands in for it.
    act(() => seen!.navigate('/timetravel', { replace: true, state: origin() }));
    // Same URL, new state object: the setter is unchanged, so effects that depend on it do not rerun.
    expect(setters.size).toBe(1);
    act(() => seen!.write({ tt_user: 'bob' }, { replace: true }));
    expect(seen!.location.search).toBe('?tt_user=bob');
    expect((seen!.location.state as { from: { path: string } }).from.path).toBe('/objects');
  });

  it('keeps the navigation state when a page writes its URL through useUrlState', () => {
    let page: { update: (patch: { tab?: string }) => void; location: Location } | null = null;
    function Page({ report }: { report: (p: NonNullable<typeof page>) => void }) {
      const { update } = useUrlState({ tab: { type: 'string', default: 'activity' } } as const);
      const location = useLocation();
      useEffect(() => report({ update, location }));
      return null;
    }
    render(<MemoryRouter initialEntries={[{ pathname: '/queries', state: origin() }]}><Page report={p => { page = p; }} /></MemoryRouter>);
    act(() => page!.update({ tab: 'health' }));
    expect(page!.location.search).toContain('tab=health');
    expect((page!.location.state as { from: { path: string } }).from.path).toBe('/objects');
  });
});
