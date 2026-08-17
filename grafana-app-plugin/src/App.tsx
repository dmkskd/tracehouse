import React, { lazy, Suspense, useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { AppRootProps, PageLayoutType } from '@grafana/data';
import { config, PluginPage } from '@grafana/runtime';
import { ServiceProvider, useServices } from './ServiceProvider';
import { PluginConfigProvider, usePluginConfig } from './PluginConfigContext';
import type { AppPluginSettings } from './types';
import { ClusterSelector } from './components/ClusterSelector';
import { DatasourceSelector } from './components/DatasourceSelector';
import { LocationContext, AppLocation } from './hooks/useAppLocation';
import { useUserPreferenceStore } from '@frontend/stores/userPreferenceStore';
import { useRefreshSettingsStore, useGlobalLastUpdatedStore } from '@frontend/stores/refreshSettingsStore';
import {
  TRACEHOUSE_OVERFLOW_ITEMS,
  TRACEHOUSE_OVERFLOW_NAVIGATION,
  TRACEHOUSE_NAVIGATION,
  TRACEHOUSE_PRIMARY_NAVIGATION,
  useRefreshConfig,
  type RefreshRateOption,
} from '@tracehouse/ui-shared';
import pluginJson from './plugin.json';

// eslint-disable-next-line @typescript-eslint/no-require-imports
require('./styles.css');

const pluginVersion = (pluginJson as { info?: { version?: string } }).info?.version ?? 'dev';
const reactRuntimeVersion = React.version ?? 'unknown';
const grafanaRuntimeVersion = (config as unknown as { buildInfo?: { version?: string } }).buildInfo?.version;

/**
 * Bridge Grafana's theme to the frontend's data-theme attribute.
 * The frontend CSS variables are driven by [data-theme="dark"|"light"] on <html>.
 */
function useGrafanaThemeBridge() {
  useEffect(() => {
    const isDark = config.theme2.isDark;
    document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
    return () => {
      // Clean up on unmount (restore default)
      document.documentElement.removeAttribute('data-theme');
    };
  }, []);
}

// Lazy load pages
const Overview = lazy(() => import('@frontend/pages/Overview').then(m => ({ default: m.Overview })));

/** Settings gear dropdown — view mode, refresh rate, experimental features */
const SettingsDropdown: React.FC = () => {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const { preferredViewMode, setPreferredViewMode, experimentalEnabled, setExperimentalEnabled } = useUserPreferenceStore();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds, setRefreshRate } = useRefreshSettingsStore();

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const sectionLabel = (text: string) => (
    <div style={{
      fontSize: 9, fontWeight: 600, color: 'var(--text-muted)',
      textTransform: 'uppercase', letterSpacing: '1.5px', marginBottom: 6,
    }}>
      {text}
    </div>
  );

  const segmentedControl = (options: { key: string; label: string }[], activeKey: string, onSelect: (key: string) => void) => (
    <div style={{
      display: 'flex', gap: 0,
      background: 'var(--bg-primary)',
      borderRadius: 6,
      border: '1px solid var(--border-primary)',
      padding: 2,
    }}>
      {options.map(opt => (
        <button
          key={opt.key}
          onClick={() => onSelect(opt.key)}
          style={{
            flex: 1, padding: '4px 0', border: 'none', cursor: 'pointer',
            borderRadius: 4, fontSize: 11, fontWeight: 600,
            fontFamily: "'Share Tech Mono', monospace",
            transition: 'all 0.15s ease',
            ...(activeKey === opt.key
              ? { background: 'var(--bg-card-hover)', color: 'var(--text-primary)', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }
              : { background: 'transparent', color: 'var(--text-muted)' }),
          }}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(!open)}
        title="Settings"
        style={{
          background: open ? 'var(--bg-card-hover)' : 'var(--bg-card)',
          border: '1px solid var(--border-primary)',
          borderRadius: 6,
          padding: '4px 7px',
          cursor: 'pointer',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: open ? 'var(--text-secondary)' : 'var(--text-tertiary)',
          transition: 'all 0.15s ease',
        }}
      >
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
        </svg>
      </button>
      {open && (
        <div style={{
          position: 'absolute',
          top: 'calc(100% + 8px)',
          right: 0,
          background: 'var(--bg-secondary)',
          border: '1px solid var(--border-primary)',
          borderRadius: 10,
          boxShadow: '0 8px 24px rgba(0,0,0,0.15)',
          backdropFilter: 'blur(12px)',
          minWidth: 200,
          zIndex: 1000,
          overflow: 'hidden',
          padding: '10px 12px',
          display: 'flex',
          flexDirection: 'column',
          gap: 12,
        }}>
          {/* View mode */}
          <div>
            {sectionLabel('View')}
            {segmentedControl(
              [{ key: '3d', label: '3D' }, { key: '2d', label: '2D' }],
              preferredViewMode,
              (k) => setPreferredViewMode(k as '3d' | '2d'),
            )}
          </div>

          {/* Refresh rate */}
          <div>
            {sectionLabel('Refresh Rate')}
            <div style={{
              display: 'flex', flexWrap: 'wrap', gap: 2,
              background: 'var(--bg-primary)',
              borderRadius: 6,
              border: '1px solid var(--border-primary)',
              padding: 2,
            }}>
              {refreshConfig.refreshRateOptions.map((opt: RefreshRateOption) => (
                <button
                  key={opt.seconds}
                  onClick={() => setRefreshRate(opt.seconds)}
                  style={{
                    padding: '4px 8px', border: 'none', cursor: 'pointer',
                    borderRadius: 4, fontSize: 11, fontWeight: 600,
                    fontFamily: "'Share Tech Mono', monospace",
                    transition: 'all 0.15s ease',
                    ...(refreshRateSeconds === opt.seconds
                      ? { background: 'var(--bg-card-hover)', color: 'var(--text-primary)', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }
                      : { background: 'transparent', color: 'var(--text-muted)' }),
                  }}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Experimental features (user-level) */}
          <label style={{
            display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer',
            fontSize: 12, color: 'var(--text-secondary)',
          }}>
            <input
              type="checkbox"
              checked={experimentalEnabled}
              onChange={(e) => setExperimentalEnabled(e.target.checked)}
              style={{ accentColor: '#a855f7' }}
            />
            <span>Experimental Features</span>
            <span style={{
              fontSize: 9, fontWeight: 700, color: '#a855f7',
              background: 'rgba(168,85,247,0.15)',
              border: '1px solid rgba(168,85,247,0.3)',
              borderRadius: 3, padding: '1px 4px',
              textTransform: 'uppercase', letterSpacing: '0.5px',
            }}>Beta</span>
          </label>

          <div style={{
            borderTop: '1px solid var(--border-primary)',
            paddingTop: 8,
            textAlign: 'center',
            fontSize: 10,
            lineHeight: 1.5,
            color: 'var(--text-tertiary)',
            fontFamily: "'Share Tech Mono', monospace",
          }}>
            <div>TraceHouse v{pluginVersion}</div>
            <div>React runtime {reactRuntimeVersion}</div>
            {grafanaRuntimeVersion && <div>Grafana {grafanaRuntimeVersion}</div>}
          </div>
        </div>
      )}
    </div>
  );
};
const EngineInternals = lazy(() => import('@frontend/pages/EngineInternals').then(m => ({ default: m.EngineInternals })));
const DatabaseExplorer = lazy(() => import('@frontend/pages/DatabaseExplorer').then(m => ({ default: m.DatabaseExplorer })));
const MergeTracker = lazy(() => import('@frontend/pages/MergeTracker').then(m => ({ default: m.MergeTracker })));
const QueryMonitor = lazy(() => import('@frontend/pages/QueryMonitor').then(m => ({ default: m.QueryMonitor })));
const TimeTravelPage = lazy(() => import('@frontend/pages/TimeTravelPage').then(m => ({ default: m.TimeTravelPage })));
const Events = lazy(() => import('@frontend/pages/Events').then(m => ({ default: m.Events })));
const Analytics = lazy(() => import('@frontend/pages/Analytics').then(m => ({ default: m.Analytics })));
const ClusterOverview = lazy(() => import('@frontend/pages/ClusterOverview').then(m => ({ default: m.ClusterOverview })));
const Replication = lazy(() => import('@frontend/pages/Replication').then(m => ({ default: m.Replication })));

// Route mapping based on plugin.json paths (matching main app order)
const ROUTES: Record<string, React.LazyExoticComponent<React.ComponentType>> = {
  'overview': Overview,
  'engine-internals': EngineInternals,
  'cluster': ClusterOverview,
  'databases': DatabaseExplorer,
  'timetravel': TimeTravelPage,
  'events': Events,
  'queries': QueryMonitor,
  'merges': MergeTracker,
  'replication': Replication,
  'analytics': Analytics,
};

function NoDatasourceMessage() {
  const { setDatasourceUid } = useServices();
  
  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      height: '100%',
      minHeight: 400,
      padding: 40,
      background: 'linear-gradient(180deg, #0a0a1a 0%, #0c0c1a 100%)',
    }}>
      <div style={{
        maxWidth: 400,
        textAlign: 'center',
      }}>
        <div style={{ fontSize: 48, marginBottom: 24 }}>🔌</div>
        <h2 style={{
          color: 'white',
          fontSize: 20,
          fontWeight: 600,
          marginBottom: 12,
          fontFamily: 'system-ui, sans-serif',
        }}>
          Select a ClickHouse Datasource
        </h2>
        <p style={{
          color: 'rgba(255,255,255,0.6)',
          fontSize: 14,
          marginBottom: 24,
          fontFamily: 'system-ui, sans-serif',
        }}>
          Choose a configured ClickHouse datasource to start monitoring.
        </p>
        <DatasourceSelector
          value={null}
          onChange={(uid, name) => setDatasourceUid(uid, name)}
        />
      </div>
    </div>
  );
}


/** Global refresh indicator for Grafana header */
const GrafanaRefreshIndicator: React.FC = () => {
  const { lastUpdated, status, triggerManualRefresh } = useGlobalLastUpdatedStore();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const [, forceUpdate] = useState(0);

  useEffect(() => {
    const id = setInterval(() => forceUpdate(n => n + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const label = (() => {
    if (refreshRateSeconds === 0) return 'Paused';
    if (!lastUpdated) return 'Connecting...';
    const secsAgo = Math.round((Date.now() - lastUpdated.getTime()) / 1000);
    if (secsAgo < 2) return 'Just now';
    if (secsAgo < 60) return `${secsAgo}s ago`;
    return `${Math.floor(secsAgo / 60)}m ago`;
  })();

  const dotColor = status === 'polling' ? '#3fb950' : status === 'error' ? '#f85149' : 'rgba(255,255,255,0.3)';

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: 'var(--text-tertiary, rgba(255,255,255,0.4))', minWidth: 72 }}>
      <button
        onClick={triggerManualRefresh}
        title="Refresh now"
        style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'inherit', display: 'flex', alignItems: 'center' }}
      >
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="23 4 23 10 17 10" />
          <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
        </svg>
      </button>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: dotColor, display: 'inline-block' }} />
      <span style={{ fontFamily: 'monospace' }}>{label}</span>
    </div>
  );
};

const GrafanaOverflowNavigation: React.FC<{ routeKey: string }> = ({ routeKey }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const activeItem = TRACEHOUSE_OVERFLOW_ITEMS.find(item => item.key === routeKey);

  useEffect(() => {
    if (!open) return;
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (ref.current && !ref.current.contains(event.target as Node)) setOpen(false);
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', closeOnOutsideClick);
    document.addEventListener('keydown', closeOnEscape);
    return () => {
      document.removeEventListener('mousedown', closeOnOutsideClick);
      document.removeEventListener('keydown', closeOnEscape);
    };
  }, [open]);

  return (
    <div ref={ref} style={{ position: 'relative', flexShrink: 0 }}>
      <button
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen(value => !value)}
        style={{
          padding: '4px 8px',
          border: 'none',
          borderRadius: 4,
          background: activeItem ? 'rgba(168,85,247,0.1)' : 'transparent',
          color: activeItem ? '#a855f7' : 'var(--text-secondary)',
          fontSize: 12,
          cursor: 'pointer',
          whiteSpace: 'nowrap',
        }}
      >
        {activeItem?.label ?? 'More'} <span aria-hidden="true">⌄</span>
      </button>

      {open && (
        <div
          role="menu"
          style={{
            position: 'absolute',
            top: 'calc(100% + 8px)',
            left: 0,
            zIndex: 2100,
            width: 210,
            padding: 8,
            border: '1px solid var(--border-primary)',
            borderRadius: 8,
            background: 'var(--bg-secondary)',
            boxShadow: '0 12px 32px rgba(0,0,0,0.28)',
          }}
        >
          {TRACEHOUSE_OVERFLOW_NAVIGATION.map((group, groupIndex) => (
            <div
              key={group.label}
              style={{
                paddingTop: groupIndex === 0 ? 0 : 8,
                marginTop: groupIndex === 0 ? 0 : 6,
                borderTop: groupIndex === 0 ? 'none' : '1px solid var(--border-primary)',
              }}
            >
              <div style={{
                padding: '3px 8px 5px',
                color: 'var(--text-muted)',
                fontSize: 9,
                fontWeight: 600,
                letterSpacing: '0.08em',
                textTransform: 'uppercase',
              }}>
                {group.label}
              </div>
              {group.items.map(item => {
                const active = routeKey === item.key;
                return (
                  <a
                    key={item.key}
                    href={`/a/dmkskd-tracehouse-app${item.path}`}
                    role="menuitem"
                    onClick={() => setOpen(false)}
                    style={{
                      display: 'block',
                      padding: '7px 8px',
                      borderRadius: 5,
                      color: active ? '#a855f7' : 'var(--text-secondary)',
                      background: active ? 'rgba(168,85,247,0.1)' : 'transparent',
                      fontSize: 12,
                      textDecoration: 'none',
                    }}
                  >
                    {item.label}
                  </a>
                );
              })}
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

const PLUGIN_BASE_PATH = '/a/dmkskd-tracehouse-app';

const pluginIncludes = (pluginJson as {
  includes?: Array<{ path?: string; addToNav?: boolean; defaultNav?: boolean }>;
}).includes ?? [];

const routeKeyOf = (path?: string) => path?.split('/').pop();

/** The `defaultNav` page — Grafana represents it as the section root itself, not a child. */
const DEFAULT_NAV_ROUTE = routeKeyOf(pluginIncludes.find(include => include.defaultNav)?.path);

/** Pages Grafana renders as children of the section, and can therefore resolve on its own. */
const SECTION_CHILD_ROUTES = new Set(
  pluginIncludes.filter(include => include.addToNav).map(include => routeKeyOf(include.path)),
);
SECTION_CHILD_ROUTES.delete(DEFAULT_NAV_ROUTE);

const pluginUrl = (path: string) => `${config.appSubUrl ?? ''}${PLUGIN_BASE_PATH}${path}`;

/**
 * Breadcrumb leaf for the active page.
 *
 * Grafana only refreshes the left-nav highlight and the breadcrumbs when a `<Page>` renders
 * (it pushes `sectionNav`/`pageNav` into the app chrome from a layout effect). `AppRootPage`
 * skips that render entirely for plugins that never call `onNavChanged`, so without
 * `<PluginPage>` the chrome stays frozen on whatever page was active during the initial load.
 *
 * What the leaf has to supply depends on how much of the trail Grafana can already build.
 * `getAppPluginRoutes` hands `AppRootPage` the *root* section ("More apps"), and
 * `buildPluginSectionNav` walks down it looking for a nav item whose url prefixes the current
 * one, so the three cases are:
 *
 * - a page in `includes` with `addToNav` resolves to its own nav item, and the parent chain
 *   already reads "More apps > TraceHouse > Page". Adding a leaf here would only duplicate it.
 * - the `defaultNav` page resolves to the section root, which stops at "More apps > TraceHouse".
 *   It needs a leaf, but no url: the url it would carry is the section root's own, and
 *   `buildBreadcrumbs` would then dedupe away the "TraceHouse" crumb instead of a duplicate.
 * - a page not in the nav tree resolves to nothing, so the trail collapses to "More apps".
 *   It needs both a leaf and an explicit `parentItem` to put "TraceHouse" back.
 */
function usePageNav(routeKey: string) {
  return useMemo(() => {
    const item = TRACEHOUSE_NAVIGATION.find(entry => entry.key === routeKey);
    if (!item || SECTION_CHILD_ROUTES.has(routeKey)) return undefined;

    if (routeKey === DEFAULT_NAV_ROUTE) {
      return { text: item.label };
    }

    return {
      text: item.label,
      url: pluginUrl(item.path),
      parentItem: {
        text: (pluginJson as { name?: string }).name ?? 'TraceHouse',
        url: pluginUrl(`/${DEFAULT_NAV_ROUTE}`),
      },
    };
  }, [routeKey]);
}

interface AppContentProps {
  path: string;
}

function AppContent({ path }: AppContentProps) {
  const { services, datasourceUid, setDatasourceUid } = useServices();
  const pluginConfig = usePluginConfig();

  // Sync plugin-level killQueriesEnabled into the user preference store
  // so existing components (QueryRunningTable) pick it up without changes
  useEffect(() => {
    useUserPreferenceStore.getState().setKillQueriesEnabled(pluginConfig.killQueriesEnabled);
  }, [pluginConfig.killQueriesEnabled]);

  // Extract route from path: /a/dmkskd-tracehouse-app/overview -> overview
  const routeKey = path.split('/').pop() || 'overview';
  const PageComponent = ROUTES[routeKey] || Overview;
  const pageOwnsScroll = routeKey === 'queries' || routeKey === 'merges';
  const pageNav = usePageNav(routeKey);

  return (
    <PluginPage layout={PageLayoutType.Custom} pageNav={pageNav}>
    <div
      className="grafana-app-container"
      style={{
        display: 'flex',
        flexDirection: 'column',
        // Grafana's app page parent is content-sized rather than height-constrained.
        // Give nested tracker scrollers a definite viewport so sticky preview rails
        // bind to the element that actually scrolls.
        height: 'calc(100dvh - 64px)',
        maxHeight: 'calc(100dvh - 64px)',
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      {/* Header with datasource selector - always visible */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '8px 16px',
        background: 'var(--bg-secondary)',
        borderBottom: '1px solid var(--border-primary)',
        flexShrink: 0,
      }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          minWidth: 0,
        }}>
          <span style={{ 
            fontSize: 14, 
            fontWeight: 500, 
            color: 'var(--text-primary)',
            fontFamily: 'system-ui, sans-serif',
          }}>
            TraceHouse
          </span>
          
          {/* Priority navigation shared with the standalone app */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: 2,
            marginLeft: 8,
            minWidth: 0,
            whiteSpace: 'nowrap',
          }}>
            {TRACEHOUSE_PRIMARY_NAVIGATION.map(item => (
              <a
                key={item.key}
                href={`/a/dmkskd-tracehouse-app${item.path}`}
                style={{
                  color: routeKey === item.key ? '#a855f7' : 'var(--text-secondary)',
                  fontSize: 12,
                  textDecoration: 'none',
                  padding: '4px 8px',
                  borderRadius: 4,
                  background: routeKey === item.key ? 'rgba(168, 85, 247, 0.1)' : 'transparent',
                  fontFamily: 'system-ui, sans-serif',
                }}
              >
                {item.label}
              </a>
            ))}
            <GrafanaOverflowNavigation routeKey={routeKey} />
          </div>
        </div>
        
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          flexShrink: 0,
        }}>
          {/* Inline datasource + cluster selectors */}
          <DatasourceSelector
            value={datasourceUid}
            onChange={(uid, name) => setDatasourceUid(uid, name)}
          />
          <ClusterSelector />
          
          {/* Global refresh indicator */}
          <GrafanaRefreshIndicator />

          {/* GitHub link */}
          <a
            href="https://github.com/dmkskd/tracehouse"
            target="_blank"
            rel="noopener noreferrer"
            title="GitHub"
            style={{
              background: 'var(--bg-card)',
              border: '1px solid var(--border-primary)',
              borderRadius: 6,
              padding: '4px 7px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'var(--text-tertiary)',
              transition: 'all 0.15s ease',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'var(--bg-card-hover)';
              e.currentTarget.style.color = 'var(--text-secondary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'var(--bg-card)';
              e.currentTarget.style.color = 'var(--text-tertiary)';
            }}
          >
            <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
            </svg>
          </a>

          {/* Settings (view mode, refresh rate, experimental) */}
          <SettingsDropdown />
        </div>
      </div>

      {/* Main content */}
      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflow: pageOwnsScroll ? 'hidden' : 'auto',
        }}
      >
        {!services ? (
          <NoDatasourceMessage />
        ) : (
          <Suspense fallback={
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center', 
              height: '100%',
              color: 'var(--text-muted)',
            }}>
              Loading...
            </div>
          }>
            <PageComponent />
          </Suspense>
        )}
      </div>
    </div>
    </PluginPage>
  );
}

export function App(props: AppRootProps<AppPluginSettings>) {
  // Bridge Grafana theme to frontend CSS variables
  useGrafanaThemeBridge();

  // Our own location state - no react-router needed
  const [location, setLocation] = useState<AppLocation>(() => ({
    pathname: props.path || '/overview',
    search: '',
    hash: '',
    state: null,
  }));

  // Grafana's own nav links navigate client-side, which re-renders us with a new
  // props.path but never goes through `navigate()`. Keep our location state in sync
  // so consumers of useAppLocation() don't read a stale pathname.
  useEffect(() => {
    setLocation(current =>
      current.pathname === props.path
        ? current
        : { pathname: props.path, search: '', hash: '', state: null },
    );
  }, [props.path]);

  const navigate = useCallback((to: string, options?: { state?: unknown; replace?: boolean }) => {
    setLocation({
      pathname: to,
      search: '',
      hash: '',
      state: options?.state ?? null,
    });
  }, []);

  const locationContextValue = useMemo(() => ({
    location,
    navigate,
  }), [location, navigate]);

  return (
    <LocationContext.Provider value={locationContextValue}>
      <PluginConfigProvider jsonData={props.meta.jsonData}>
        <ServiceProvider>
          <AppContent path={props.path} />
        </ServiceProvider>
      </PluginConfigProvider>
    </LocationContext.Provider>
  );
}
