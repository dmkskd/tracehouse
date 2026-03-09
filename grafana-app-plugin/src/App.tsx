import React, { lazy, Suspense, useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { AppRootProps } from '@grafana/data';
import { config } from '@grafana/runtime';
import { ServiceProvider, useServices } from './ServiceProvider';
import { PluginConfigProvider } from './PluginConfigContext';
import { DatasourceSelector } from './components/DatasourceSelector';
import { LocationContext, AppLocation } from './hooks/useAppLocation';
import { useViewPreferenceStore } from '@frontend/stores/viewPreferenceStore';
import { useRefreshSettingsStore, useGlobalLastUpdatedStore } from '@frontend/stores/refreshSettingsStore';
import { useRefreshConfig, type RefreshRateOption } from '@tracehouse/ui-shared';

// Import CSS with Tailwind
import './styles.css';

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

/** Compact 2D/3D toggle with gear icon for Grafana header */
const ViewModeToggle: React.FC = () => {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const { preferredViewMode, setPreferredViewMode } = useViewPreferenceStore();

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(!open)}
        title="View Settings"
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
          minWidth: 140,
          zIndex: 1000,
          overflow: 'hidden',
          padding: '10px 12px',
        }}>
          <div style={{
            fontSize: 9, fontWeight: 600, color: 'var(--text-muted)',
            textTransform: 'uppercase', letterSpacing: '1.5px', marginBottom: 6,
          }}>
            View
          </div>
          <div style={{
            display: 'flex', gap: 0,
            background: 'var(--bg-primary)',
            borderRadius: 6,
            border: '1px solid var(--border-primary)',
            padding: 2,
          }}>
            {(['3d', '2d'] as const).map(mode => (
              <button
                key={mode}
                onClick={() => setPreferredViewMode(mode)}
                style={{
                  flex: 1, padding: '4px 0', border: 'none', cursor: 'pointer',
                  borderRadius: 4, fontSize: 11, fontWeight: 600,
                  fontFamily: "'Share Tech Mono', monospace",
                  transition: 'all 0.15s ease',
                  ...(preferredViewMode === mode
                    ? { background: 'var(--bg-card-hover)', color: 'var(--text-primary)', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }
                    : { background: 'transparent', color: 'var(--text-muted)' }),
                }}
              >
                {mode.toUpperCase()}
              </button>
            ))}
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

/** Compact refresh rate selector for Grafana header */
const GrafanaRefreshSelector: React.FC = () => {
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds, setRefreshRate } = useRefreshSettingsStore();
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const currentLabel = refreshConfig.refreshRateOptions.find(
    (o: RefreshRateOption) => o.seconds === refreshRateSeconds
  )?.label || `${refreshRateSeconds}s`;

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: 'rgba(255,255,255,0.06)',
          border: '1px solid rgba(255,255,255,0.1)',
          borderRadius: 4,
          padding: '3px 8px',
          fontSize: 11,
          fontFamily: 'monospace',
          color: 'var(--text-secondary, rgba(255,255,255,0.7))',
          cursor: 'pointer',
        }}
      >
        ⟳ {currentLabel}
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: 'calc(100% + 4px)', right: 0,
          background: 'var(--bg-secondary, #1e1e2e)', border: '1px solid var(--border-primary, rgba(255,255,255,0.1))',
          borderRadius: 6, boxShadow: '0 4px 12px rgba(0,0,0,0.3)', zIndex: 1000, overflow: 'hidden', minWidth: 80,
        }}>
          {refreshConfig.refreshRateOptions.map((opt: RefreshRateOption) => (
            <button
              key={opt.seconds}
              onClick={() => { setRefreshRate(opt.seconds); setOpen(false); }}
              style={{
                display: 'block', width: '100%', padding: '6px 12px', border: 'none', cursor: 'pointer',
                fontSize: 11, fontFamily: 'monospace', textAlign: 'left',
                background: refreshRateSeconds === opt.seconds ? 'rgba(168,85,247,0.15)' : 'transparent',
                color: refreshRateSeconds === opt.seconds ? '#a855f7' : 'var(--text-secondary, rgba(255,255,255,0.7))',
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

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

interface AppContentProps {
  path: string;
}

function AppContent({ path }: AppContentProps) {
  const { services, datasourceUid, setDatasourceUid } = useServices();

  // Extract route from path: /a/tracehouse-app/overview -> overview
  const routeKey = path.split('/').pop() || 'overview';
  const PageComponent = ROUTES[routeKey] || Overview;

  return (
    <div className="grafana-app-container" style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
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
        }}>
          <span style={{ 
            fontSize: 14, 
            fontWeight: 500, 
            color: 'var(--text-primary)',
            fontFamily: 'system-ui, sans-serif',
          }}>
            TraceHouse
          </span>
          
          {/* Navigation breadcrumb - matching main app order */}
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: 4,
            marginLeft: 8,
          }}>
            {[
              { key: 'overview', label: 'Overview', path: '/a/tracehouse-app/overview' },
              { key: 'engine-internals', label: 'Engine Internals', path: '/a/tracehouse-app/engine-internals' },
              { key: 'cluster', label: 'Cluster', path: '/a/tracehouse-app/cluster' },
              { key: 'databases', label: 'Explorer', path: '/a/tracehouse-app/databases' },
              { key: 'timetravel', label: 'Time Travel', path: '/a/tracehouse-app/timetravel' },
              { key: 'queries', label: 'Queries', path: '/a/tracehouse-app/queries' },
              { key: 'merges', label: 'Merges', path: '/a/tracehouse-app/merges' },
              { key: 'replication', label: 'Replication', path: '/a/tracehouse-app/replication' },
              { key: 'analytics', label: 'Analytics', path: '/a/tracehouse-app/analytics' },
            ].map((item, idx) => (
              <React.Fragment key={item.key}>
                {idx > 0 && <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>|</span>}
                <a
                  href={item.path}
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
              </React.Fragment>
            ))}
          </div>
        </div>
        
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
        }}>
          {/* Inline datasource selector */}
          <DatasourceSelector
            value={datasourceUid}
            onChange={(uid, name) => setDatasourceUid(uid, name)}
          />
          
          {/* Connection status indicator */}
          {services && (
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              padding: '4px 10px',
              background: 'rgba(34, 197, 94, 0.1)',
              border: '1px solid rgba(34, 197, 94, 0.3)',
              borderRadius: 4,
              fontSize: 11,
              fontFamily: 'monospace',
            }}>
              <span style={{
                width: 6,
                height: 6,
                borderRadius: '50%',
                background: '#22c55e',
                boxShadow: '0 0 6px #22c55e',
              }} />
              <span style={{ color: '#22c55e' }}>Connected</span>
            </div>
          )}

          {/* Refresh rate selector */}
          <GrafanaRefreshSelector />

          {/* Global refresh indicator */}
          <GrafanaRefreshIndicator />

          {/* View mode settings (2D/3D) */}
          <ViewModeToggle />
        </div>
      </div>

      {/* Main content */}
      <div style={{ flex: 1, overflow: 'auto' }}>
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
  );
}

export function App(props: AppRootProps) {
  // Bridge Grafana theme to frontend CSS variables
  useGrafanaThemeBridge();

  // Our own location state - no react-router needed
  const [location, setLocation] = useState<AppLocation>(() => ({
    pathname: props.path || '/overview',
    search: '',
    hash: '',
    state: null,
  }));

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
      <PluginConfigProvider>
        <ServiceProvider>
          <AppContent path={props.path} />
        </ServiceProvider>
      </PluginConfigProvider>
    </LocationContext.Provider>
  );
}
