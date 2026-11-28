import type { PropsWithChildren } from 'react';
import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ClickHouseContext, type ClickHouseServices } from '@tracehouse/ui-shared';
import type { OperationalEvent } from '@tracehouse/core';
import { useArenaEvents } from '../useArenaEvents';
import { useMonitoringCapabilitiesStore } from '../../../stores/monitoringCapabilitiesStore';
import { useRefreshSettingsStore, useGlobalLastUpdatedStore } from '../../../stores/refreshSettingsStore';

const event = (id: string): OperationalEvent => ({ id, occurred_at: new Date().toISOString(), hostname: 'node-02',
  kind: 'query_failure', category: 'queries', severity: 'error', precision: 'exact', title: 'Failure', source: 'system.query_log', capability: 'query_log' });
beforeEach(() => {
  const memory = new Map<string, string>();
  vi.stubGlobal('localStorage', { getItem: (key: string) => memory.get(key) ?? null, setItem: (key: string, value: string) => memory.set(key, value), removeItem: (key: string) => memory.delete(key) });
  vi.useFakeTimers();
  useRefreshSettingsStore.setState({ refreshRateSeconds: 5 });
  useMonitoringCapabilitiesStore.setState({ capabilities: { capabilities: [{ id: 'query_log', available: true }] } as never });
});
afterEach(() => { cleanup(); vi.useRealTimers(); useMonitoringCapabilitiesStore.getState().reset(); vi.unstubAllGlobals(); });
const flush = async () => { await act(async () => { await vi.advanceTimersByTimeAsync(0); }); };

describe('arena event collection', () => {
  it('keeps initial history quiet, pulses only new observations, and stops while hidden', async () => {
    const one = event('one'), two = event('two');
    const getEvents = vi.fn().mockResolvedValue({ events: [one], coverage: [] });
    const services = { eventsService: { getEvents } } as unknown as ClickHouseServices;
    const wrapper = ({ children }: PropsWithChildren) => <ClickHouseContext.Provider value={services}>{children}</ClickHouseContext.Provider>;
    const hook = renderHook(({ enabled }) => useArenaEvents(enabled, ['node-01', 'node-02']), { wrapper, initialProps: { enabled: true } });
    await flush();
    expect(getEvents).toHaveBeenCalledWith(expect.objectContaining({ hostname: ['node-01', 'node-02'], origin: 'overview' }));
    expect(hook.result.current.arrivals.size).toBe(0);
    getEvents.mockResolvedValue({ events: [one, two], coverage: [] });
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    const arrival = hook.result.current.arrivals.get('two');
    expect(arrival).toBeDefined();
    expect(hook.result.current.arrivals.has('one')).toBe(false);
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(hook.result.current.arrivals.get('two')).toBe(arrival);
    getEvents.mockResolvedValue({ events: [], coverage: [] });
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    getEvents.mockResolvedValue({ events: [one, two], coverage: [] });
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(hook.result.current.arrivals.get('two')).toBe(arrival);
    hook.rerender({ enabled: false });
    const calls = getEvents.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(30000); });
    expect(getEvents).toHaveBeenCalledTimes(calls);
    expect(hook.result.current.events).toEqual([]);
  });
  it('ignores late responses from a prior connection and exposes fetch failures', async () => {
    let finish!: (result: unknown) => void;
    const getA = vi.fn(() => new Promise(resolve => { finish = resolve; }));
    const getB = vi.fn().mockResolvedValue({ events: [event('b')], coverage: [] });
    const a = { eventsService: { getEvents: getA } } as unknown as ClickHouseServices;
    const b = { eventsService: { getEvents: getB } } as unknown as ClickHouseServices;
    let services = a;
    const wrapper = ({ children }: PropsWithChildren) => <ClickHouseContext.Provider value={services}>{children}</ClickHouseContext.Provider>;
    const hook = renderHook(() => useArenaEvents(true, []), { wrapper });
    services = b; hook.rerender(); await flush();
    await act(async () => { finish({ events: [event('old')], coverage: [] }); });
    expect(hook.result.current.events.map(e => e.id)).toEqual(['b']);
    getB.mockRejectedValue(new Error('Connection lost'));
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(hook.result.current.error).toBe('Connection lost');
    expect(hook.result.current.events).toEqual([]);
  });
  it('honors paused refresh while allowing an explicit manual refresh', async () => {
    useRefreshSettingsStore.setState({ refreshRateSeconds: 0 });
    const getEvents = vi.fn().mockResolvedValue({ events: [], coverage: [] });
    const services = { eventsService: { getEvents } } as unknown as ClickHouseServices;
    const wrapper = ({ children }: PropsWithChildren) => <ClickHouseContext.Provider value={services}>{children}</ClickHouseContext.Provider>;
    renderHook(() => useArenaEvents(true, []), { wrapper }); await flush();
    await act(async () => { await vi.advanceTimersByTimeAsync(30000); });
    expect(getEvents).toHaveBeenCalledTimes(1);
    act(() => useGlobalLastUpdatedStore.setState(s => ({ manualRefreshTick: s.manualRefreshTick + 1 })));
    await flush(); expect(getEvents).toHaveBeenCalledTimes(2);
  });
});
