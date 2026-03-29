/**
 * Unit tests for ConnectionManager CRUD operations (localStorage-based).
 *
 * testConnection tests have been moved to integration tests
 * (connection-manager.integration.test.ts) which test against a real ClickHouse container.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { ConnectionManager } from '../connection-manager.js';
import type { AdapterFactory } from '../connection-manager.js';
import type { ConnectionConfig, ConnectionProfile } from '../../types/connection.js';

// --- localStorage mock ---

function createLocalStorageMock(): Storage {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => { store.set(key, value); },
    removeItem: (key: string) => { store.delete(key); },
    clear: () => { store.clear(); },
    get length() { return store.size; },
    key: (index: number) => [...store.keys()][index] ?? null,
  };
}

function makeConfig(overrides: Partial<ConnectionConfig> = {}): ConnectionConfig {
  return {
    host: 'localhost',
    port: 8123,
    user: 'default',
    password: 'secret123',
    database: 'default',
    secure: false,
    connect_timeout: 10,
    send_receive_timeout: 30,
    ...overrides,
  };
}

function makeProfile(overrides: Partial<ConnectionProfile> = {}): ConnectionProfile {
  return {
    id: 'prof-1',
    name: 'Local Dev',
    config: makeConfig(),
    created_at: '2024-01-01T00:00:00Z',
    updated_at: '2024-01-01T00:00:00Z',
    last_connected_at: null,
    ...overrides,
  };
}

/** Dummy factory — CRUD tests never call testConnection. */
const dummyFactory: AdapterFactory = () => ({
  adapter: { executeQuery: async () => [] },
  close: async () => {},
});

beforeEach(() => {
  (globalThis as Record<string, unknown>).localStorage = createLocalStorageMock();
});

describe('ConnectionManager', { tags: ['connectivity'] }, () => {
  describe('listProfiles', () => {
    it('returns empty array when no profiles stored', () => {
      const mgr = new ConnectionManager(dummyFactory);
      expect(mgr.listProfiles()).toEqual([]);
    });

    it('returns empty array when localStorage has invalid JSON', () => {
      localStorage.setItem('tracehouse-connections', '{bad json');
      const mgr = new ConnectionManager(dummyFactory);
      expect(mgr.listProfiles()).toEqual([]);
    });

    it('returns stored profiles', () => {
      const profile = makeProfile();
      const safe = { ...profile, config: { ...profile.config, password: '' } };
      localStorage.setItem('tracehouse-connections', JSON.stringify([safe]));
      const mgr = new ConnectionManager(dummyFactory);
      const result = mgr.listProfiles();
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe('prof-1');
    });
  });

  describe('saveProfile', () => {
    it('adds a new profile', () => {
      const mgr = new ConnectionManager(dummyFactory);
      mgr.saveProfile(makeProfile());

      const stored = mgr.listProfiles();
      expect(stored).toHaveLength(1);
      expect(stored[0].id).toBe('prof-1');
      expect(stored[0].name).toBe('Local Dev');
    });

    it('strips password before storing', () => {
      const mgr = new ConnectionManager(dummyFactory);
      const profile = makeProfile({ config: makeConfig({ password: 'super-secret' }) });
      mgr.saveProfile(profile);

      const stored = mgr.listProfiles();
      expect(stored[0].config.password).toBe('');

      const raw = localStorage.getItem('tracehouse-connections')!;
      expect(raw).not.toContain('super-secret');
    });

    it('updates existing profile by id', () => {
      const mgr = new ConnectionManager(dummyFactory);
      mgr.saveProfile(makeProfile({ name: 'Original' }));
      mgr.saveProfile(makeProfile({ name: 'Updated' }));

      const stored = mgr.listProfiles();
      expect(stored).toHaveLength(1);
      expect(stored[0].name).toBe('Updated');
    });

    it('adds multiple profiles with different ids', () => {
      const mgr = new ConnectionManager(dummyFactory);
      mgr.saveProfile(makeProfile({ id: 'a' }));
      mgr.saveProfile(makeProfile({ id: 'b' }));

      const stored = mgr.listProfiles();
      expect(stored).toHaveLength(2);
      expect(stored.map(p => p.id)).toEqual(['a', 'b']);
    });

    it('preserves all non-password config fields', () => {
      const mgr = new ConnectionManager(dummyFactory);
      const config = makeConfig({
        host: 'ch.example.com',
        port: 8443,
        user: 'admin',
        database: 'analytics',
        secure: true,
      });
      mgr.saveProfile(makeProfile({ config }));

      const stored = mgr.listProfiles()[0];
      expect(stored.config.host).toBe('ch.example.com');
      expect(stored.config.port).toBe(8443);
      expect(stored.config.user).toBe('admin');
      expect(stored.config.database).toBe('analytics');
      expect(stored.config.secure).toBe(true);
      expect(stored.config.password).toBe('');
    });
  });

  describe('removeProfile', () => {
    it('removes a profile by id', () => {
      const mgr = new ConnectionManager(dummyFactory);
      mgr.saveProfile(makeProfile({ id: 'a' }));
      mgr.saveProfile(makeProfile({ id: 'b' }));

      mgr.removeProfile('a');
      const stored = mgr.listProfiles();
      expect(stored).toHaveLength(1);
      expect(stored[0].id).toBe('b');
    });

    it('does nothing when id not found', () => {
      const mgr = new ConnectionManager(dummyFactory);
      mgr.saveProfile(makeProfile({ id: 'a' }));
      mgr.removeProfile('nonexistent');
      expect(mgr.listProfiles()).toHaveLength(1);
    });

    it('results in empty list when last profile removed', () => {
      const mgr = new ConnectionManager(dummyFactory);
      mgr.saveProfile(makeProfile({ id: 'only' }));
      mgr.removeProfile('only');
      expect(mgr.listProfiles()).toEqual([]);
    });
  });
});
