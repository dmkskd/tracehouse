import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, cleanup, render, renderHook, screen, fireEvent } from '@testing-library/react';
import { create } from 'zustand';
import { useUserPreferenceStore } from '../userPreferenceStore';
import { QueryXRaySourceControl } from '../../components/query/QueryXRayPreference';
import { QueryXRayDefaultContext, useQueryXRayPreference } from '../../components/query/query-xray-preference';

vi.hoisted(() => {
  const values = new Map<string, string>();
  Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
  } });
});

const connection = create<{ activeProfileId: string | null }>(() => ({ activeProfileId: 'a' }));
vi.mock('../connectionStore', () => ({ useConnectionStore: (selector: (state: { activeProfileId: string | null }) => unknown) => connection(selector) }));

beforeEach(() => {
  cleanup();
  localStorage.clear();
  connection.setState({ activeProfileId: 'a' });
  useUserPreferenceStore.setState({ queryXraySource: {} });
});

describe('Query X-Ray preference layers', () => {
  it('inherits changing admin defaults without persisting them and honours explicit auto', () => {
    let source: 'query_metric_log' | 'processes_history' = 'query_metric_log';
    const { result, rerender } = renderHook(() => useQueryXRayPreference(), {
      wrapper: ({ children }) => <QueryXRayDefaultContext.Provider value={source}>{children}</QueryXRayDefaultContext.Provider>,
    });
    expect(result.current.preference).toBe('query_metric_log');
    expect(useUserPreferenceStore.getState().queryXraySource).toEqual({});
    source = 'processes_history';
    rerender();
    expect(result.current.preference).toBe('processes_history');
    act(() => result.current.setPreference('auto'));
    expect(result.current.preference).toBe('auto');
    act(() => result.current.setPreference(undefined));
    expect(result.current.preference).toBe('processes_history');
  });

  it('keeps pins isolated across connections and persists them across hydration', async () => {
    const { result } = renderHook(() => useQueryXRayPreference());
    act(() => result.current.setPreference('query_metric_log'));
    act(() => connection.setState({ activeProfileId: 'b' }));
    expect(result.current.preference).toBe('auto');
    act(() => result.current.setPreference('processes_history'));
    const stored = localStorage.getItem('tracehouse-view-preference')!;
    act(() => useUserPreferenceStore.setState({ queryXraySource: {} }));
    localStorage.setItem('tracehouse-view-preference', stored);
    await act(() => useUserPreferenceStore.persist.rehydrate());
    expect(useUserPreferenceStore.getState().queryXraySource).toEqual({ a: 'query_metric_log', b: 'processes_history' });
  });

  it('drops the legacy browser-wide pin and keeps the rest of the persisted state', async () => {
    localStorage.setItem('tracehouse-view-preference', JSON.stringify({ version: 0, state: { xraySource: 'query_metric_log', preferredViewMode: '2d' } }));
    await act(() => useUserPreferenceStore.persist.rehydrate());
    // A browser-wide string cannot be attributed to a connection, so it is not
    // converted into pins: everyone lands on automatic.
    expect(useUserPreferenceStore.getState().queryXraySource).toEqual({});
    expect(useUserPreferenceStore.getState()).not.toHaveProperty('xraySource');
    expect(useUserPreferenceStore.getState().preferredViewMode).toBe('2d');
  });

  it('disables editing while disconnected and exposes an inherit option', () => {
    render(<QueryXRaySourceControl />);
    fireEvent.click(screen.getByRole('button', { name: 'sampler' }));
    expect(useUserPreferenceStore.getState().queryXraySource).toEqual({ a: 'processes_history' });
    fireEvent.click(screen.getByRole('button', { name: 'auto' }));
    expect(useUserPreferenceStore.getState().queryXraySource).toEqual({});
    act(() => connection.setState({ activeProfileId: null }));
    for (const button of screen.getAllByRole('button')) expect(button).toBeDisabled();
  });

  it('offers an explicit auto pin only when it would override an admin-pinned table', () => {
    const segments = () => screen.getAllByRole('button').map(b => b.textContent);
    render(<QueryXRaySourceControl />);
    expect(segments()).toEqual(['auto', 'sampler', 'metric log']);
    cleanup();
    render(<QueryXRayDefaultContext.Provider value="query_metric_log"><QueryXRaySourceControl /></QueryXRayDefaultContext.Provider>);
    expect(segments()).toEqual(['default', 'auto', 'sampler', 'metric log']);
    fireEvent.click(screen.getByRole('button', { name: 'auto' }));
    expect(useUserPreferenceStore.getState().queryXraySource).toEqual({ a: 'auto' });
    expect(screen.getByRole('button', { name: 'auto' })).toHaveAttribute('aria-pressed', 'true');
  });
});
