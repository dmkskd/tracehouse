/**
 * Build-time configuration.
 *
 * Reads compile-time globals injected by the build tool (see vite.buildDefines.ts).
 * No build-tool-specific APIs (import.meta.env, process.env, …) are used here,
 * so swapping Vite for another bundler only requires changing the define mapping.
 */

/* eslint-disable no-undef */
declare const __TH_DEFAULT_CH_HOST__: string | undefined;
declare const __TH_DEFAULT_CH_PORT__: string | undefined;
declare const __TH_DEFAULT_CH_USER__: string | undefined;
declare const __TH_DEFAULT_CH_PASSWORD__: string | undefined;
declare const __TH_DEFAULT_CH_DATABASE__: string | undefined;
declare const __TH_DEFAULT_CH_SECURE__: boolean;
declare const __TH_DEFAULT_CH_CLUSTER__: string | undefined;
declare const __TH_AUTO_CONNECT__: boolean;
declare const __TH_BUNDLED_PROXY__: boolean;
declare const __TH_DASHBOARD_PREVIEW__: boolean;

export const buildConfig = {
  /** Default ClickHouse connection parameters */
  defaultConnection: {
    host: typeof __TH_DEFAULT_CH_HOST__ !== 'undefined' ? __TH_DEFAULT_CH_HOST__ : 'localhost',
    port: parseInt(typeof __TH_DEFAULT_CH_PORT__ !== 'undefined' ? __TH_DEFAULT_CH_PORT__ : '8123', 10),
    user: typeof __TH_DEFAULT_CH_USER__ !== 'undefined' ? __TH_DEFAULT_CH_USER__ : 'default',
    password: typeof __TH_DEFAULT_CH_PASSWORD__ !== 'undefined' ? __TH_DEFAULT_CH_PASSWORD__ : '',
    database: typeof __TH_DEFAULT_CH_DATABASE__ !== 'undefined' ? __TH_DEFAULT_CH_DATABASE__ : 'default',
    secure: typeof __TH_DEFAULT_CH_SECURE__ !== 'undefined' ? __TH_DEFAULT_CH_SECURE__ : false,
    cluster: typeof __TH_DEFAULT_CH_CLUSTER__ !== 'undefined' ? __TH_DEFAULT_CH_CLUSTER__ : undefined,
  },

  /** Automatically connect on startup using the default connection */
  autoConnect: typeof __TH_AUTO_CONNECT__ !== 'undefined' ? __TH_AUTO_CONNECT__ : false,

  /** Proxy is co-located with the frontend (bundled mode) */
  bundledProxy: typeof __TH_BUNDLED_PROXY__ !== 'undefined' ? __TH_BUNDLED_PROXY__ : false,

  /** Show dashboard hover-preview in the analytics dashboard list */
  dashboardPreview: typeof __TH_DASHBOARD_PREVIEW__ !== 'undefined' ? __TH_DASHBOARD_PREVIEW__ : false,
} as const;
