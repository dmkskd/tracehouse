/**
 * Connection Store Shim for Grafana Plugin
 * 
 * This provides a compatible interface with the frontend's connectionStore
 * but uses Grafana's datasource system instead of direct connections.
 */

import { create } from 'zustand';

// Re-export types for compatibility
export interface ConnectionConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  secure: boolean;
  connect_timeout: number;
  send_receive_timeout: number;
}

export interface ConnectionConfigResponse {
  host: string;
  port: number;
  user: string;
  database: string;
  secure: boolean;
  connect_timeout: number;
  send_receive_timeout: number;
}

export interface ConnectionProfile {
  id: string;
  name: string;
  config: ConnectionConfigResponse;
  created_at: string;
  updated_at: string;
  last_connected_at: string | null;
  is_connected: boolean;
}

export interface ConnectionTestResult {
  success: boolean;
  server_version: string | null;
  server_timezone: string | null;
  server_display_name: string | null;
  error_message: string | null;
  error_type: string | null;
  latency_ms: number | null;
}

interface ConnectionState {
  profiles: ConnectionProfile[];
  activeProfileId: string | null;
  isLoading: boolean;
  isTestingConnection: boolean;
  error: string | null;
  testResult: ConnectionTestResult | null;
  isConnectionFormOpen: boolean;
  
  // Internal: set from Grafana datasource
  _setGrafanaDatasource: (uid: string, name: string) => void;
  
  // Actions (mostly no-ops in Grafana context)
  fetchProfiles: () => Promise<void>;
  createProfile: (name: string, config: ConnectionConfig, testConnection?: boolean, connect?: boolean) => Promise<ConnectionProfile>;
  deleteProfile: (profileId: string) => Promise<void>;
  testConnection: (config: ConnectionConfig) => Promise<ConnectionTestResult>;
  connectProfile: (profileId: string) => Promise<void>;
  disconnectProfile: (profileId: string) => Promise<void>;
  setActiveProfile: (profileId: string | null) => void;
  setConnectionFormOpen: (isOpen: boolean) => void;
  clearError: () => void;
  clearTestResult: () => void;
}

export const defaultConnectionConfig: ConnectionConfig = {
  host: 'localhost',
  port: 8123,
  user: 'default',
  password: '',
  database: 'default',
  secure: false,
  connect_timeout: 10,
  send_receive_timeout: 300,
};

/**
 * Grafana-compatible connection store.
 * In Grafana, the "connection" is managed by the datasource selector,
 * so most of these methods are no-ops or simplified.
 */
export const useConnectionStore = create<ConnectionState>()((set, get) => ({
  profiles: [],
  activeProfileId: null,
  isLoading: false,
  isTestingConnection: false,
  error: null,
  testResult: null,
  isConnectionFormOpen: false,

  // Internal method to sync with Grafana datasource selection
  _setGrafanaDatasource: (uid: string, name: string) => {
    const now = new Date().toISOString();
    const profile: ConnectionProfile = {
      id: uid,
      name: name,
      config: {
        host: 'grafana-datasource',
        port: 0,
        user: 'grafana',
        database: 'default',
        secure: true,
        connect_timeout: 10,
        send_receive_timeout: 300,
      },
      created_at: now,
      updated_at: now,
      last_connected_at: now,
      is_connected: true,
    };
    
    set({
      profiles: [profile],
      activeProfileId: uid,
    });
  },

  fetchProfiles: async () => {
    // No-op in Grafana - profiles come from datasource selector
  },

  createProfile: async () => {
    throw new Error('Use Grafana datasource configuration to add connections');
  },

  deleteProfile: async () => {
    throw new Error('Use Grafana datasource configuration to manage connections');
  },

  testConnection: async () => {
    return {
      success: true,
      server_version: null,
      server_timezone: null,
      server_display_name: null,
      error_message: null,
      error_type: null,
      latency_ms: null,
    };
  },

  connectProfile: async (profileId: string) => {
    set({ activeProfileId: profileId });
  },

  disconnectProfile: async () => {
    set({ activeProfileId: null });
  },

  setActiveProfile: (profileId: string | null) => {
    set({ activeProfileId: profileId });
  },

  setConnectionFormOpen: (isOpen: boolean) => {
    set({ isConnectionFormOpen: isOpen });
  },

  clearError: () => {
    set({ error: null });
  },

  clearTestResult: () => {
    set({ testResult: null });
  },
}));

export default useConnectionStore;
