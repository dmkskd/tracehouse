import { afterEach, describe, expect, it, vi } from 'vitest';
import { createTracehouseStorage, storageKey } from '../utils/browser-storage.js';

function memoryStorage() {
  const data = new Map<string, string>();
  return {
    data,
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => { data.set(key, value); },
    removeItem: (key: string) => { data.delete(key); },
  };
}

describe('TraceHouse browser storage', () => {
  afterEach(() => vi.restoreAllMocks());

  it('migrates exact legacy values without interpreting their contents', () => {
    const storage = memoryStorage();
    const saved = JSON.stringify({ state: { dashboards: ['saved'] }, version: 0 });
    storage.setItem('tracehouse-dashboards-user', saved);
    storage.setItem('another-plugin', 'unrelated');
    const owned = createTracehouseStorage(() => storage);
    expect(owned.getItem('tracehouse-dashboards-user')).toBe(saved);
    expect(storage.getItem(storageKey('tracehouse-dashboards-user'))).toBe(saved);
    expect(storage.getItem('tracehouse-dashboards-user')).toBe(saved);
    expect(storage.getItem('another-plugin')).toBe('unrelated');
  });

  it('prefers the namespaced value and does not resurrect legacy data after removal', () => {
    const storage = memoryStorage();
    storage.setItem('tracehouse-datasource', 'old');
    storage.setItem(storageKey('tracehouse-datasource'), 'new');
    const owned = createTracehouseStorage(() => storage);
    expect(owned.getItem('tracehouse-datasource')).toBe('new');
    owned.removeItem('tracehouse-datasource');
    expect(owned.getItem('tracehouse-datasource')).toBeNull();
  });

  it('keeps legacy data readable if copying fails', () => {
    const storage = memoryStorage();
    storage.setItem('tracehouse-connections', 'profiles');
    const error = new Error('quota');
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const owned = createTracehouseStorage(() => ({ ...storage, setItem: () => { throw error; } }));
    expect(owned.getItem('tracehouse-connections')).toBe('profiles');
    expect(storage.getItem('tracehouse-connections')).toBe('profiles');
    expect(warn).toHaveBeenCalledWith(expect.stringContaining(storageKey('tracehouse-connections')), error);
    expect(storage.getItem(storageKey('tracehouse-connections'))).toBeNull();
  });

  it('retains the pre-upgrade snapshot for rollback without writing new data to legacy keys', () => {
    const storage = memoryStorage();
    storage.setItem('tracehouse-connections', 'pre-upgrade');
    const owned = createTracehouseStorage(() => storage);
    expect(owned.getItem('tracehouse-connections')).toBe('pre-upgrade');
    owned.setItem('tracehouse-connections', 'updated');
    expect(owned.getItem('tracehouse-connections')).toBe('updated');
    expect(storage.getItem('tracehouse-connections')).toBe('pre-upgrade');
  });

  it('keeps local and session values separate and supports already-prefixed keys', () => {
    const local = memoryStorage(), session = memoryStorage();
    const localOwned = createTracehouseStorage(() => local);
    const sessionOwned = createTracehouseStorage(() => session);
    session.setItem('obsmap:selectedTable', 'db.table');
    expect(sessionOwned.getItem('obsmap:selectedTable')).toBe('db.table');
    expect(localOwned.getItem('obsmap:selectedTable')).toBeNull();
    localOwned.setItem(storageKey('th_tooltip_blur'), 'off');
    expect(local.getItem('dmkskd-tracehouse-app:th_tooltip_blur')).toBe('off');
    expect(local.getItem('th_tooltip_blur')).toBeNull();
  });
});
