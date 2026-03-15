/**
 * Layout - Main dashboard layout with top navigation
 * Supports dark/light themes via CSS variables
 */

import React, { useState, useRef, useEffect } from 'react';
import { NavLink } from 'react-router-dom';
import { ConnectionSelector } from './connection/ConnectionSelector';
import { useTheme } from '../providers/ThemeProvider';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import { useRefreshConfig, type RefreshRateOption } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore, useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';

interface LayoutProps {
  children: React.ReactNode;
}

const navItems = [
  { path: '/overview', label: 'Overview' },
  { path: '/engine-internals', label: 'Engine Internals' },
  { path: '/cluster', label: 'Cluster' },
  { path: '/databases', label: 'Explorer' },
  { path: '/timetravel', label: 'Time Travel' },
  { path: '/queries', label: 'Queries' },
  { path: '/merges', label: 'Merges' },
  { path: '/replication', label: 'Replication' },
  { path: '/analytics', label: 'Analytics' },
];

/** Settings popover with theme + view mode + refresh rate */
const SettingsPopover: React.FC = () => {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const { theme, toggleTheme } = useTheme();
  const { preferredViewMode, setPreferredViewMode, killQueriesEnabled, setKillQueriesEnabled } = useUserPreferenceStore();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds, setRefreshRate } = useRefreshSettingsStore();

  // Close on outside click
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
      {/* Gear icon button */}
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
        onMouseEnter={(e) => {
          e.currentTarget.style.background = 'var(--bg-card-hover)';
          e.currentTarget.style.color = 'var(--text-secondary)';
        }}
        onMouseLeave={(e) => {
          if (!open) {
            e.currentTarget.style.background = 'var(--bg-card)';
            e.currentTarget.style.color = 'var(--text-tertiary)';
          }
        }}
      >
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
        </svg>
      </button>

      {/* Dropdown */}
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
          minWidth: 160,
          zIndex: 1000,
          overflow: 'hidden',
        }}>
          {/* View Mode */}
          <div style={{ padding: '10px 12px 6px' }}>
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

          <div style={{ height: 1, background: 'var(--border-primary)', margin: '4px 12px' }} />

          {/* Theme */}
          <div style={{ padding: '6px 12px 10px' }}>
            <div style={{
              fontSize: 9, fontWeight: 600, color: 'var(--text-muted)',
              textTransform: 'uppercase', letterSpacing: '1.5px', marginBottom: 6,
            }}>
              Theme
            </div>
            <div style={{
              display: 'flex', gap: 0,
              background: 'var(--bg-primary)',
              borderRadius: 6,
              border: '1px solid var(--border-primary)',
              padding: 2,
            }}>
              {(['dark', 'light'] as const).map(t => (
                <button
                  key={t}
                  onClick={() => { if (theme !== t) toggleTheme(); }}
                  style={{
                    flex: 1, padding: '4px 0', border: 'none', cursor: 'pointer',
                    borderRadius: 4, fontSize: 11, fontWeight: 600,
                    display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4,
                    transition: 'all 0.15s ease',
                    ...(theme === t
                      ? { background: 'var(--bg-card-hover)', color: 'var(--text-primary)', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }
                      : { background: 'transparent', color: 'var(--text-muted)' }),
                  }}
                >
                  {t === 'dark' ? (
                    <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
                    </svg>
                  ) : (
                    <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <circle cx="12" cy="12" r="5" />
                      <line x1="12" y1="1" x2="12" y2="3" />
                      <line x1="12" y1="21" x2="12" y2="23" />
                      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
                      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
                      <line x1="1" y1="12" x2="3" y2="12" />
                      <line x1="21" y1="12" x2="23" y2="12" />
                      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
                      <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
                    </svg>
                  )}
                  {t === 'dark' ? 'Dark' : 'Light'}
                </button>
              ))}
            </div>
          </div>

          <div style={{ height: 1, background: 'var(--border-primary)', margin: '4px 12px' }} />

          {/* Refresh Rate */}
          <div style={{ padding: '6px 12px 10px' }}>
            <div style={{
              fontSize: 9, fontWeight: 600, color: 'var(--text-muted)',
              textTransform: 'uppercase', letterSpacing: '1.5px', marginBottom: 6,
            }}>
              Refresh Rate
            </div>
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
                    flex: '1 0 auto', padding: '4px 6px', border: 'none', cursor: 'pointer',
                    borderRadius: 4, fontSize: 10, fontWeight: 600,
                    fontFamily: "'Share Tech Mono', monospace",
                    transition: 'all 0.15s ease',
                    minWidth: 32,
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

          <div style={{ height: 1, background: 'var(--border-primary)', margin: '4px 12px' }} />

          {/* Kill Queries Toggle */}
          <div style={{ padding: '6px 12px' }}>
            <label style={{
              display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer',
              fontSize: 11, color: 'var(--text-secondary)',
            }}>
              <input
                type="checkbox"
                checked={killQueriesEnabled}
                onChange={(e) => setKillQueriesEnabled(e.target.checked)}
                style={{ accentColor: '#f85149' }}
              />
              Allow Kill Query
            </label>
          </div>

          <div style={{ height: 1, background: 'var(--border-primary)', margin: '4px 12px' }} />

          {/* Clear Tracehouse Data */}
          <div style={{ padding: '6px 12px 10px' }}>
            <button
              onClick={() => {
                if (!confirm('Remove all Tracehouse data? This includes saved connections, credentials, dashboards, and preferences.')) return;
                const PREFIXES = ['tracehouse-', 'hdx-'];
                [localStorage, sessionStorage].forEach(store => {
                  const keys = Array.from({ length: store.length }, (_, i) => store.key(i)!);
                  keys.forEach(k => { if (PREFIXES.some(p => k.startsWith(p))) store.removeItem(k); });
                });
                setOpen(false);
                window.location.reload();
              }}
              style={{
                width: '100%', padding: '5px 0', border: '1px solid rgba(248,81,73,0.3)',
                borderRadius: 6, fontSize: 10, fontWeight: 600, cursor: 'pointer',
                background: 'var(--bg-primary)', color: '#f85149',
                transition: 'all 0.15s ease',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'rgba(248,81,73,0.1)';
                e.currentTarget.style.borderColor = 'rgba(248,81,73,0.5)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'var(--bg-primary)';
                e.currentTarget.style.borderColor = 'rgba(248,81,73,0.3)';
              }}
            >
              Clear Tracehouse Data
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

/** Global refresh status shown in the header */
const GlobalRefreshIndicator: React.FC = () => {
  const { lastUpdated, status, triggerManualRefresh } = useGlobalLastUpdatedStore();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const [, forceUpdate] = useState(0);

  // Re-render every second so "Xs ago" stays fresh
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

  const dotColor = status === 'polling' ? '#3fb950' : status === 'error' ? '#f85149' : 'var(--text-muted)';
  const shouldPulse = status === 'polling' && refreshRateSeconds > 0;

  return (
    <div className="flex items-center gap-2" style={{ fontSize: 11, color: 'var(--text-tertiary)', minWidth: 72 }}>
      <button
        onClick={triggerManualRefresh}
        title="Refresh now"
        style={{
          background: 'none', border: 'none', padding: 0, cursor: 'pointer',
          color: 'var(--text-tertiary)', display: 'flex', alignItems: 'center',
          transition: 'color 0.15s ease',
        }}
        onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; }}
        onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-tertiary)'; }}
      >
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <polyline points="23 4 23 10 17 10" />
          <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
        </svg>
      </button>
      <div style={{ position: 'relative', width: 7, height: 7 }}>
        <div style={{
          width: 7, height: 7, borderRadius: '50%',
          backgroundColor: dotColor,
        }} />
        {shouldPulse && (
          <div className="animate-ping" style={{
            position: 'absolute', inset: 0, borderRadius: '50%',
            backgroundColor: dotColor, opacity: 0.4,
          }} />
        )}
      </div>
      <span style={{ fontFamily: "'Share Tech Mono', monospace", display: 'inline-block', minWidth: 64 }}>{label}</span>
    </div>
  );
};

export const Layout: React.FC<LayoutProps> = ({ children }) => {
  const { theme } = useTheme();

  return (
    <div 
      className="flex flex-col h-screen" 
      style={{ 
        background: 'var(--bg-primary)',
        color: 'var(--text-primary)',
        transition: 'background 0.2s ease, color 0.2s ease',
      }}
    >
      {/* Top Navigation Bar */}
      <header 
        className="h-12 flex items-center justify-between pr-4 flex-shrink-0"
        style={{ 
          paddingLeft: 48,
          background: theme === 'dark' ? 'rgba(10, 10, 26, 0.95)' : 'var(--bg-secondary)',
          borderBottom: '1px solid var(--border-primary)',
          backdropFilter: 'blur(8px)',
          transition: 'background 0.2s ease, border-color 0.2s ease',
          position: 'relative',
          zIndex: 2000,
        }}
      >
        {/* Left: Nav */}
        <div className="flex items-center gap-8">
          <nav className="flex items-center gap-6">
            {navItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                style={{ fontFamily: "'Orbitron', 'Rajdhani', monospace" }}
                className={(_navData) =>
                  `py-1.5 text-[11px] font-medium transition-all uppercase tracking-widest`
                }
              >
                {({ isActive }) => (
                  <span style={{
                    color: isActive ? 'var(--text-primary)' : 'var(--text-muted)',
                    textShadow: isActive && theme === 'dark' ? 'var(--shadow-glow)' : 'none',
                    borderBottom: isActive ? '1px solid var(--accent-secondary)' : '1px solid transparent',
                    paddingBottom: '4px',
                    transition: 'color 0.15s ease',
                  }}>
                    {item.label}
                  </span>
                )}
              </NavLink>
            ))}
          </nav>
        </div>
        
        {/* Right: Connection + Refresh Status + Settings */}
        <div className="flex items-center gap-4">
          <ConnectionSelector />
          <div className="w-px h-5" style={{ background: 'var(--border-primary)' }} />
          <GlobalRefreshIndicator />
          <div className="w-px h-5" style={{ background: 'var(--border-primary)' }} />
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
          <SettingsPopover />
        </div>
      </header>

      {/* Page Content */}
      <main className="flex-1 overflow-auto">
        {children}
      </main>
    </div>
  );
};

export default Layout;
