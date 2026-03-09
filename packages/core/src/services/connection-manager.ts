import type { ConnectionConfig, ConnectionProfile, ConnectionTestResult } from '../types/connection.js';
import type { IClickHouseAdapter } from '../adapters/types.js';
import { AdapterError } from '../adapters/types.js';

const STORAGE_KEY = 'tracehouse-connections';

export class ConnectionManagerError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'ConnectionManagerError';
  }
}

/** Factory that creates an adapter and returns it along with a close function. */
export type AdapterFactory = (config: ConnectionConfig) => {
  adapter: IClickHouseAdapter;
  close: () => Promise<void>;
};

export class ConnectionManager {
  private readonly createAdapter: AdapterFactory;

  /**
   * Creates a ConnectionManager with the specified adapter factory.
   * The factory is required to avoid bundling Node.js-only dependencies.
   * 
   * For browser: use BrowserAdapter from '@tracehouse/core/adapters/browser-adapter'
   * For Node.js: use HttpAdapter from '@tracehouse/core/adapters/http-adapter'
   * For Grafana: use GrafanaAdapter from '@tracehouse/core/adapters/grafana-adapter'
   */
  constructor(adapterFactory: AdapterFactory) {
    this.createAdapter = adapterFactory;
  }

  listProfiles(): ConnectionProfile[] {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : [];
    } catch {
      return [];
    }
  }

  saveProfile(profile: ConnectionProfile): void {
    const profiles = this.listProfiles();
    const idx = profiles.findIndex(p => p.id === profile.id);
    const safeProfile: ConnectionProfile = {
      ...profile,
      config: { ...profile.config, password: '' },
    };
    if (idx >= 0) {
      profiles[idx] = safeProfile;
    } else {
      profiles.push(safeProfile);
    }
    localStorage.setItem(STORAGE_KEY, JSON.stringify(profiles));
  }

  removeProfile(id: string): void {
    const profiles = this.listProfiles().filter(p => p.id !== id);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(profiles));
  }

  async testConnection(config: ConnectionConfig): Promise<ConnectionTestResult> {
    const { adapter, close } = this.createAdapter(config);
    const start = Date.now();
    try {
      const rows = await adapter.executeQuery<{ version: string; timezone: string; hostname: string }>(
        'SELECT version() as version, timezone() as timezone, hostName() as hostname',
      );
      const latency = Date.now() - start;
      const row = rows[0];
      return {
        success: true,
        server_version: row?.version ?? '',
        server_timezone: row?.timezone ?? '',
        server_display_name: row?.hostname ?? '',
        latency_ms: latency,
      };
    } catch (error) {
      const latency = Date.now() - start;
      const isAdapterError = error instanceof AdapterError;
      return {
        success: false,
        error_message: error instanceof Error ? error.message : String(error),
        error_type: isAdapterError ? error.category : 'unknown',
        latency_ms: latency,
      };
    } finally {
      await close();
    }
  }
}
