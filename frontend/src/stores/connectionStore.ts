/**
 * Connection Store - Zustand store for managing ClickHouse connections
 * 
 * This store handles connection profiles, active connections, and connection testing.
 * Uses BrowserAdapter from @tracehouse/core for direct ClickHouse connections.
 * 
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { BrowserAdapter } from '@tracehouse/core/adapters/browser-adapter';
import { ProxyAdapter } from '@tracehouse/core';
import { useProxyStore } from './proxyStore';

// LocalStorage key for persisting connection profiles
const STORAGE_KEY = 'tracehouse-connections';

// Connection configuration for creating/testing connections
export interface ConnectionConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  secure: boolean;
  connect_timeout: number;
  send_receive_timeout: number;
  /** Enable ClickHouse Cloud sticky routing via *.sticky.* subdomain */
  useCloudStickyRouting?: boolean;
}

// Connection configuration response (without password)
export interface ConnectionConfigResponse {
  host: string;
  port: number;
  user: string;
  database: string;
  secure: boolean;
  connect_timeout: number;
  send_receive_timeout: number;
  useCloudStickyRouting?: boolean;
}

// Connection profile from the backend
export interface ConnectionProfile {
  id: string;
  name: string;
  config: ConnectionConfigResponse;
  created_at: string;
  updated_at: string;
  last_connected_at: string | null;
  is_connected: boolean;
}

// Connection test result
export interface ConnectionTestResult {
  success: boolean;
  server_version: string | null;
  server_timezone: string | null;
  server_display_name: string | null;
  error_message: string | null;
  error_type: string | null;
  latency_ms: number | null;
}

// Create connection response
export interface CreateConnectionResponse {
  profile: ConnectionProfile;
  test_result: ConnectionTestResult | null;
}

// Connection store state
interface ConnectionState {
  // Connection profiles
  profiles: ConnectionProfile[];
  activeProfileId: string | null;
  
  // UI state
  isLoading: boolean;
  isTestingConnection: boolean;
  error: string | null;
  testResult: ConnectionTestResult | null;
  
  // Modal state
  isConnectionFormOpen: boolean;
  editingProfileId: string | null;
  
  // Actions
  fetchProfiles: () => Promise<void>;
  createProfile: (name: string, config: ConnectionConfig, testConnection?: boolean, connect?: boolean) => Promise<ConnectionProfile>;
  updateProfile: (profileId: string, name: string, config: ConnectionConfig) => Promise<ConnectionProfile>;
  deleteProfile: (profileId: string) => Promise<void>;
  testConnection: (config: ConnectionConfig) => Promise<ConnectionTestResult>;
  connectProfile: (profileId: string) => Promise<void>;
  disconnectProfile: (profileId: string) => Promise<void>;
  setActiveProfile: (profileId: string | null) => void;
  setConnectionFormOpen: (isOpen: boolean, editProfileId?: string | null) => void;
  clearError: () => void;
  clearTestResult: () => void;
}

// Default connection configuration.
// Build-time env vars (VITE_DEFAULT_CH_*) override the defaults,
// e.g. for Docker quickstart where ClickHouse is at 'clickhouse:8123'.
export const defaultConnectionConfig: ConnectionConfig = {
  host: import.meta.env.VITE_DEFAULT_CH_HOST ?? 'localhost',
  port: parseInt(import.meta.env.VITE_DEFAULT_CH_PORT ?? '8123', 10),
  user: import.meta.env.VITE_DEFAULT_CH_USER ?? 'default',
  password: import.meta.env.VITE_DEFAULT_CH_PASSWORD ?? '',
  database: import.meta.env.VITE_DEFAULT_CH_DATABASE ?? 'default',
  secure: import.meta.env.VITE_DEFAULT_CH_SECURE === 'true',
  connect_timeout: 10,
  send_receive_timeout: 30,
};

export const useConnectionStore = create<ConnectionState>()(
  persist(
    (set, get) => ({
      // Initial state
      profiles: [],
      activeProfileId: null,
      isLoading: false,
      isTestingConnection: false,
      error: null,
      testResult: null,
      isConnectionFormOpen: false,
      editingProfileId: null,

      // Fetch all connection profiles - now just returns local state (no backend)
      fetchProfiles: async () => {
        console.log('[ConnectionStore] fetchProfiles: using local profiles');
        // Profiles are already loaded from localStorage via persist middleware
        // Just ensure state is consistent
        const { profiles, activeProfileId } = get();
        
        // If activeProfileId doesn't exist in profiles, reset it
        if (activeProfileId && !profiles.find(p => p.id === activeProfileId)) {
          set({ activeProfileId: null });
        }
      },

      // Create a new connection profile - stored locally
      createProfile: async (name: string, config: ConnectionConfig, testConnection = true, connect = false) => {
        set({ isLoading: true, error: null });
        
        try {
          let testResult: ConnectionTestResult | null = null;
          
          // Test connection if requested
          if (testConnection) {
            testResult = await get().testConnection(config);
            if (!testResult.success) {
              set({ isLoading: false });
              throw new Error(testResult.error_message || 'Connection test failed');
            }
          }
          
          // Create profile locally
          const now = new Date().toISOString();
          const profile: ConnectionProfile = {
            id: crypto.randomUUID(),
            name,
            config: {
              host: config.host,
              port: config.port,
              user: config.user,
              database: config.database,
              secure: config.secure,
              connect_timeout: config.connect_timeout,
              send_receive_timeout: config.send_receive_timeout,
              // Store password in config for reconnection (localStorage only)
              password: config.password,
            } as ConnectionConfigResponse & { password: string },
            created_at: now,
            updated_at: now,
            last_connected_at: connect ? now : null,
            is_connected: connect,
          };
          
          set(state => ({
            profiles: [...state.profiles, profile],
            isLoading: false,
            testResult,
            activeProfileId: connect ? profile.id : state.activeProfileId,
          }));
          
          return profile;
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : 'Failed to create connection profile';
          set({ error: errorMessage, isLoading: false });
          throw error;
        }
      },

      // Update an existing connection profile
      updateProfile: async (profileId: string, name: string, config: ConnectionConfig) => {
        set({ isLoading: true, error: null });
        try {
          const now = new Date().toISOString();
          let updatedProfile: ConnectionProfile | undefined;
          set(state => {
            const profiles = state.profiles.map(p => {
              if (p.id !== profileId) return p;
              updatedProfile = {
                ...p,
                name,
                config: {
                  host: config.host,
                  port: config.port,
                  user: config.user,
                  database: config.database,
                  secure: config.secure,
                  connect_timeout: config.connect_timeout,
                  send_receive_timeout: config.send_receive_timeout,
                  password: config.password,
                } as ConnectionConfigResponse & { password: string },
                updated_at: now,
              };
              return updatedProfile;
            });
            return { profiles, isLoading: false };
          });
          if (!updatedProfile) throw new Error('Profile not found');
          return updatedProfile;
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : 'Failed to update profile';
          set({ error: errorMessage, isLoading: false });
          throw error;
        }
      },

      // Delete a connection profile - local only
      deleteProfile: async (profileId: string) => {
        set({ isLoading: true, error: null });
        set(state => ({
          profiles: state.profiles.filter(p => p.id !== profileId),
          activeProfileId: state.activeProfileId === profileId ? null : state.activeProfileId,
          isLoading: false,
        }));
      },

      // Test a connection without saving - uses BrowserAdapter or ProxyAdapter
      testConnection: async (config: ConnectionConfig) => {
        set({ isTestingConnection: true, error: null, testResult: null });
        const startTime = performance.now();
        const proxyState = useProxyStore.getState();

        try {
          const adapter = proxyState.enabled
            ? new ProxyAdapter(config, proxyState.url)
            : new BrowserAdapter(config);

          // Race the real query against a 10s timeout so CORS-blocked requests
          // don't hang for the full send_receive_timeout (300s).
          const timeout = new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error('Connection timed out')), 10_000),
          );
          const rows = await Promise.race([
            adapter.executeQuery<{ version: string; timezone: string; display_name: string }>(
              `SELECT version() as version, timezone() as timezone, hostName() as display_name`
            ),
            timeout,
          ]);

          await adapter.close();

          const latencyMs = performance.now() - startTime;
          const row = rows[0];

          const result: ConnectionTestResult = {
            success: true,
            server_version: row?.version ?? null,
            server_timezone: row?.timezone ?? null,
            server_display_name: row?.display_name ?? null,
            error_message: null,
            error_type: null,
            latency_ms: latencyMs,
          };

          set({ testResult: result, isTestingConnection: false });
          return result;
        } catch (error: unknown) {
          const latencyMs = performance.now() - startTime;
          let errorMessage = 'Failed to connect to ClickHouse';
          let errorType = 'unknown';

          if (error instanceof Error) {
            errorMessage = error.message;
            if ('category' in error) {
              errorType = (error as { category: string }).category;
            }
          }

          // Detect mixed-content: HTTPS page trying to reach an HTTP ClickHouse.
          // Browsers silently block these, surfacing only a generic network error.
          const isMixedContent =
            !proxyState.enabled &&
            window.location.protocol === 'https:' &&
            !config.secure &&
            (errorType === 'network' || errorMessage === 'Connection timed out');

          if (isMixedContent) {
            errorType = 'mixed_content';
            errorMessage =
              'Your browser blocked this request because the page is served over HTTPS but ClickHouse is using plain HTTP (mixed content).';
          }
          // When proxy is off and we get a network error or timeout,
          // it's likely a CORS issue — tag it so the UI can show a hint.
          else if (!proxyState.enabled && (errorType === 'network' || errorMessage === 'Connection timed out')) {
            errorType = 'cors';
          }

          const failedResult: ConnectionTestResult = {
            success: false,
            server_version: null,
            server_timezone: null,
            server_display_name: null,
            error_message: errorMessage,
            error_type: errorType,
            latency_ms: latencyMs,
          };

          set({ testResult: failedResult, isTestingConnection: false });
          return failedResult;
        }
      },

      // Connect to a saved profile - just marks it as active
      connectProfile: async (profileId: string) => {
        set({ isLoading: true, error: null });
        
        const profile = get().profiles.find(p => p.id === profileId);
        if (!profile) {
          set({ error: 'Profile not found', isLoading: false });
          return;
        }
        
        const now = new Date().toISOString();
        set(state => ({
          profiles: state.profiles.map(p => 
            p.id === profileId 
              ? { ...p, is_connected: true, last_connected_at: now }
              : { ...p, is_connected: false }
          ),
          activeProfileId: profileId,
          isLoading: false,
        }));
      },

      // Disconnect from a profile
      disconnectProfile: async (profileId: string) => {
        set({ isLoading: true, error: null });
        set(state => ({
          profiles: state.profiles.map(p => 
            p.id === profileId ? { ...p, is_connected: false } : p
          ),
          activeProfileId: state.activeProfileId === profileId ? null : state.activeProfileId,
          isLoading: false,
        }));
      },

      // Set the active profile
      setActiveProfile: (profileId: string | null) => {
        set({ activeProfileId: profileId });
      },

      // Toggle connection form modal
      setConnectionFormOpen: (isOpen: boolean, editProfileId: string | null = null) => {
        set({ isConnectionFormOpen: isOpen, editingProfileId: isOpen ? editProfileId : null, testResult: null, error: null });
      },

      // Clear error
      clearError: () => {
        set({ error: null });
      },

      // Clear test result
      clearTestResult: () => {
        set({ testResult: null });
      },
    }),
    {
      name: STORAGE_KEY,
      // Only persist profiles and activeProfileId, not transient UI state
      partialize: (state) => ({
        profiles: state.profiles,
        activeProfileId: state.activeProfileId,
      }),
    }
  )
);

export default useConnectionStore;
