/**
 * Proxy Store — manages the CORS proxy configuration.
 *
 * When enabled, the app routes all ClickHouse requests through a local
 * proxy server instead of connecting directly from the browser.
 * This is needed when connecting to remote ClickHouse servers that
 * don't set CORS headers (most managed services).
 *
 * In "bundled" mode (buildConfig.bundledProxy), the proxy is co-located
 * with the frontend (e.g. inside a Docker image) and always active.
 * The user never sees proxy configuration in the UI.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

const DEFAULT_PROXY_URL = 'http://localhost:8990/proxy';
const BUNDLED_PROXY_URL = '/proxy';
const STORAGE_KEY = 'tracehouse-proxy';

import { buildConfig } from '../buildConfig';

/** True when the frontend was built with a co-located proxy (Docker image) */
const IS_BUNDLED = buildConfig.bundledProxy;

interface ProxyState {
  /** Whether the proxy is co-located (build-time flag, read-only) */
  bundled: boolean;
  enabled: boolean;
  url: string;
  /** Whether the proxy is reachable (last check result) */
  available: boolean | null;

  setEnabled: (enabled: boolean) => void;
  setUrl: (url: string) => void;
  /** Ping the proxy to check if it's running */
  checkAvailability: () => Promise<boolean>;
}

export const useProxyStore = create<ProxyState>()(
  persist(
    (set, get) => ({
      bundled: IS_BUNDLED,
      enabled: IS_BUNDLED ? true : false,
      url: IS_BUNDLED ? BUNDLED_PROXY_URL : DEFAULT_PROXY_URL,
      available: null,

      setEnabled: (enabled: boolean) => {
        if (IS_BUNDLED) return; // can't disable in bundled mode
        set({ enabled });
      },
      setUrl: (url: string) => {
        if (IS_BUNDLED) return; // can't change in bundled mode
        set({ url: url.replace(/\/$/, '') });
      },

      checkAvailability: async () => {
        const { url } = get();
        try {
          const resp = await fetch(`${url}/ping`, { signal: AbortSignal.timeout(3000) });
          const ok = resp.ok;
          set({ available: ok });
          return ok;
        } catch {
          set({ available: false });
          return false;
        }
      },
    }),
    {
      name: STORAGE_KEY,
      partialize: (state) => ({
        // Don't persist bundled-mode values — they're fixed at build time
        enabled: state.bundled ? undefined : state.enabled,
        url: state.bundled ? undefined : state.url,
      }),
      merge: (persisted, current) => {
        const p = (persisted ?? {}) as Partial<ProxyState>;
        // bundled is always derived from the build-time flag, never from storage
        delete p.bundled;
        return {
          ...current,
          ...(IS_BUNDLED ? {} : p),
        };
      },
    },
  ),
);

export const DEFAULT_PROXY_URL_VALUE = DEFAULT_PROXY_URL;
