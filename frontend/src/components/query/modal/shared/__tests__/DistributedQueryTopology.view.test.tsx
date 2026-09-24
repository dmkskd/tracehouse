/**
 * The timeline/flow choice is a share coordinate: a copied link must open on the
 * view it was shared from, not on the recipient's stored preference.
 */
import React from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DistributedQueryTopology } from '../DistributedQueryTopology';
import type { SubQueryInfo } from '@tracehouse/core';

const storedValues = new Map<string, string>();
const localStorageMock: Storage = {
  get length() { return storedValues.size; },
  clear: () => storedValues.clear(),
  getItem: key => storedValues.get(key) ?? null,
  key: index => Array.from(storedValues.keys())[index] ?? null,
  removeItem: key => { storedValues.delete(key); },
  setItem: (key, value) => { storedValues.set(key, value); },
};
vi.stubGlobal('localStorage', localStorageMock);

afterEach(cleanup);
beforeEach(() => localStorage.clear());

const coordinator = {
  query_id: 'query-root',
  hostname: 'node-a',
  query_duration_ms: 20,
  query_start_time_microseconds: '2026-06-18 12:00:00.000000',
  memory_usage: 1024,
  read_rows: 10,
};

const subQueries = [{
  query_id: 'query-child',
  hostname: 'node-b',
  query_duration_ms: 10,
  query_start_time_microseconds: '2026-06-18 12:00:00.001000',
  memory_usage: 512,
  read_rows: 10,
  read_bytes: 100,
  query_preview: 'SELECT * FROM db.table',
  exception_code: 0,
}] as SubQueryInfo[];

const Search: React.FC = () => <div data-testid="location-search">{useLocation().search}</div>;

function renderAt(search: string) {
  return render(
    <MemoryRouter initialEntries={[`/queries${search}`]}>
      <DistributedQueryTopology
        coordinator={coordinator}
        subQueries={subQueries}
        activeQueryId={coordinator.query_id}
        onNavigate={vi.fn()}
      />
      <Search />
    </MemoryRouter>,
  );
}

const viewButton = (name: 'timeline' | 'flow') => screen.getByRole('button', { name });

describe('DistributedQueryTopology view coordinate', () => {
  it('opens on the view named by qd_topo, overriding the stored preference', () => {
    localStorage.setItem('tracehouse.distributedTopology.view', 'timeline');
    renderAt('?qd_topo=flow');

    expect(viewButton('flow')).toHaveAttribute('aria-pressed', 'true');
    expect(viewButton('timeline')).toHaveAttribute('aria-pressed', 'false');
  });

  it('falls back to the stored preference when qd_topo is absent or junk', () => {
    localStorage.setItem('tracehouse.distributedTopology.view', 'flow');
    renderAt('?qd_topo=nonsense');

    expect(viewButton('flow')).toHaveAttribute('aria-pressed', 'true');
  });

  it('writes the chosen view to the URL so the link carries it', () => {
    renderAt('');
    expect(viewButton('timeline')).toHaveAttribute('aria-pressed', 'true');

    fireEvent.click(viewButton('flow'));

    expect(screen.getByTestId('location-search')).toHaveTextContent('qd_topo=flow');
    expect(viewButton('flow')).toHaveAttribute('aria-pressed', 'true');
    expect(localStorage.getItem('tracehouse.distributedTopology.view')).toBe('flow');
  });
});
