import { fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
const memory = new Map<string, string>();
vi.stubGlobal('localStorage', { getItem: (k: string) => memory.get(k) ?? null, setItem: (k: string, v: string) => memory.set(k, v), removeItem: (k: string) => memory.delete(k) });
import { OperationsMenu } from '../OperationsMenu';
import { TimelineFiltersDefaultContext } from '../timeline-filters-preference';
import { useUserPreferenceStore } from '../../../stores/userPreferenceStore';

function Search() { return <output aria-label="search">{useLocation().search}</output>; }

describe('Time Travel operations menu', () => {
  it('summarises what is listed, shows a link value, and replaces it with a personal choice', () => {
    useUserPreferenceStore.setState({ timeTravelFilters: {} });
    render(<MemoryRouter initialEntries={['/timetravel?tt_min_dur=1000&tt_user=bob&tt_failed_start=1']}>
      <TimelineFiltersDefaultContext.Provider value={{ minMemoryBytes: 16_777_216 }}>
        <OperationsMenu activityLimit={100} onActivityLimit={() => {}} includeRunning onIncludeRunning={() => {}} isLive metric="cpu" />
        <Search />
      </TimelineFiltersDefaultContext.Provider>
    </MemoryRouter>);
    const button = screen.getByRole('button', { name: 'Listed operations' });
    expect(button.textContent).toContain('top 100 by CPU · over 16 MB or 1 s');
    fireEvent.click(button);
    const panel = screen.getByRole('dialog', { name: 'Listed operations' });
    expect(within(panel).getByText('Thresholds set by the link')).toBeTruthy();
    fireEvent.click(within(panel).getByRole('button', { name: '100 ms' }));
    expect(useUserPreferenceStore.getState().timeTravelFilters).toEqual({ minDurationMs: 100 });
    // Only the changed link value is dropped; the link's other values stay in the URL and are not saved.
    expect(screen.getByLabelText('search').textContent).toBe('?tt_user=bob&tt_failed_start=1');
    expect(button.textContent).toContain('over 16 MB or 100 ms');
    fireEvent.click(within(panel).getByRole('button', { name: 'Reset to default' }));
    expect(button.textContent).toContain('top 100 by CPU · over 16 MB');
    expect(screen.getByLabelText('search').textContent).toBe('?tt_user=bob');
  });
});
