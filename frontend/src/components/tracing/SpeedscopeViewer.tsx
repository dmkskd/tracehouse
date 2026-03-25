/**
 * React wrapper around the speedscope-widget Web Component.
 * Renders an interactive flamegraph with left-heavy, sandwich, and time-ordered views.
 *
 * IMPORTANT: Do NOT import speedscope-widget's CSS — it's a global reset that
 * would break the host app.  The widget uses CSS-in-JS (aphrodite) internally.
 */
import React, { useRef, useEffect, useCallback, useState } from 'react';

// Side-effect import: registers <speedscope-widget> custom element
import 'speedscope-widget';

// Extend JSX to accept the custom element
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace JSX {
    interface IntrinsicElements {
      'speedscope-widget': React.DetailedHTMLProps<React.HTMLAttributes<HTMLElement>, HTMLElement>;
    }
  }
}

export type FlamegraphType = 'CPU' | 'Real' | 'Memory';

interface SpeedscopeViewerProps {
  /** Folded-stack text: "frame1;frame2 count\n..." */
  folded: string;
  isLoading: boolean;
  error: string | null;
  unavailableReason?: string;
  onRefresh: (type: FlamegraphType) => void;
  profileType?: FlamegraphType;
  onTypeChange?: (type: FlamegraphType) => void;
}

const ProfileTypeToggle: React.FC<{
  profileType: FlamegraphType;
  onTypeChange?: (type: FlamegraphType) => void;
}> = ({ profileType, onTypeChange }) => {
  if (!onTypeChange) return null;
  const types: FlamegraphType[] = ['CPU', 'Real', 'Memory'];
  return (
    <div style={{ display: 'flex', gap: 1, background: 'var(--bg-tertiary)', borderRadius: 6, padding: 3 }}>
      {types.map(t => (
        <button
          key={t}
          onClick={() => onTypeChange(t)}
          title={t === 'Real' ? 'Wall-clock time (includes I/O waits)' : undefined}
          style={{
            padding: '6px 14px',
            fontSize: 12,
            fontWeight: 500,
            borderRadius: 4,
            border: 'none',
            background: profileType === t ? 'var(--bg-primary)' : 'transparent',
            color: profileType === t ? 'var(--text-primary)' : 'var(--text-tertiary)',
            cursor: 'pointer',
            boxShadow: profileType === t ? '0 1px 2px rgba(0,0,0,0.1)' : 'none',
          }}
        >
          {t}
        </button>
      ))}
    </div>
  );
};

export const SpeedscopeViewer: React.FC<SpeedscopeViewerProps> = ({
  folded,
  isLoading,
  error,
  unavailableReason,
  onRefresh,
  profileType = 'CPU',
  onTypeChange,
}) => {
  const widgetRef = useRef<HTMLElement | null>(null);
  const loadedFoldedRef = useRef<string | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const unsubscribeRef = useRef<(() => void) | null>(null);

  // Ref callback: mark the widget as "started" immediately so its
  // connectedCallback won't auto-init with empty data. We handle init ourselves.
  const setWidgetRef = useCallback((el: HTMLElement | null) => {
    widgetRef.current = el;
    if (el) {
      (el as any).started = true;
    } else {
      // Widget unmounted — reset so next mount does a full init
      loadedFoldedRef.current = null;
    }
  }, []);

  // Feed data into the widget exactly once per folded value
  useEffect(() => {
    const el = widgetRef.current;
    if (!el || !folded || folded === loadedFoldedRef.current) return;

    const trySetInput = () => {
      const rect = el.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0) {
        // Reset started so setInput does a full init on first call
        if (!loadedFoldedRef.current) {
          (el as any).started = false;
        }
        (el as any).setInput(folded);
        loadedFoldedRef.current = folded;

        // Subscribe to speedscope state changes to work around a bug in
        // speedscope-widget: switching between Time Order and Left Heavy
        // calls toggleLoadingPage() which sets loading=true, but nothing
        // ever sets it back to false. We detect view mode changes and
        // trigger a reload so loadProfile runs and resets loading properly.
        if (unsubscribeRef.current) unsubscribeRef.current();
        const api = (window as any).speedscopeAPI;
        if (api?.subscribe && api?.reload && api?.getViewMode) {
          let lastViewMode = api.getViewMode();
          let lastReverse = api.getReverseFlamegraph?.() ?? false;
          unsubscribeRef.current = api.subscribe(() => {
            const currentViewMode = api.getViewMode();
            const currentReverse = api.getReverseFlamegraph?.() ?? false;
            if (currentViewMode !== lastViewMode || currentReverse !== lastReverse) {
              lastViewMode = currentViewMode;
              lastReverse = currentReverse;
              // Delay so toggleLoadingPage() runs first, then our reload
              // triggers loadProfile which sets loading=false at the end.
              setTimeout(() => {
                if (loadedFoldedRef.current) {
                  api.reload(loadedFoldedRef.current);
                }
              }, 50);
            }
          });
        }
      }
    };

    // Try immediately, retry on next frame if dimensions aren't ready yet
    trySetInput();
    if (loadedFoldedRef.current !== folded) {
      const raf = requestAnimationFrame(trySetInput);
      return () => cancelAnimationFrame(raf);
    }
  }, [folded]);

  // Clean up subscription on unmount
  useEffect(() => {
    return () => {
      if (unsubscribeRef.current) {
        unsubscribeRef.current();
        unsubscribeRef.current = null;
      }
    };
  }, []);

  // Escape key exits fullscreen
  useEffect(() => {
    if (!isFullscreen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setIsFullscreen(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isFullscreen]);

  // Notify the widget to resize when fullscreen toggles
  useEffect(() => {
    const el = widgetRef.current;
    if (!el) return;
    // Give the DOM a frame to apply the new layout, then trigger resize
    const raf = requestAnimationFrame(() => {
      window.dispatchEvent(new Event('resize'));
    });
    return () => cancelAnimationFrame(raf);
  }, [isFullscreen]);

  // Reset when profile type changes (widget gets fresh data)
  const handleTypeChange = useCallback((type: FlamegraphType) => {
    loadedFoldedRef.current = null;
    onTypeChange?.(type);
  }, [onTypeChange]);

  const hasData = folded.length > 0;

  const header = (
    <div style={{
      padding: '12px 16px',
      borderBottom: '1px solid var(--border-primary)',
      background: 'var(--bg-secondary)',
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <ProfileTypeToggle profileType={profileType} onTypeChange={handleTypeChange} />
        <a
          href="https://github.com/jlfwong/speedscope"
          target="_blank"
          rel="noopener noreferrer"
          style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none' }}
          title="speedscope — MIT License"
        >
          via <span style={{ color: 'var(--text-tertiary)' }}>speedscope</span>
        </a>
      </div>
      <div style={{ display: 'flex', gap: 6 }}>
        <button
          onClick={() => onRefresh(profileType)}
          style={{
            padding: '6px 12px',
            fontSize: 11,
            borderRadius: 4,
            border: '1px solid var(--border-primary)',
            background: 'var(--bg-tertiary)',
            color: 'var(--text-secondary)',
            cursor: 'pointer',
          }}
        >
          Refresh
        </button>
        <button
          onClick={() => setIsFullscreen(f => !f)}
          title={isFullscreen ? 'Exit fullscreen (Esc)' : 'Fullscreen'}
          style={{
            padding: '6px 12px',
            fontSize: 11,
            borderRadius: 4,
            border: '1px solid var(--border-primary)',
            background: isFullscreen ? 'rgba(88, 166, 255, 0.2)' : 'var(--bg-tertiary)',
            color: isFullscreen ? '#58a6ff' : 'var(--text-secondary)',
            cursor: 'pointer',
          }}
        >
          {isFullscreen ? '⤓ Exit Fullscreen' : '⤢ Fullscreen'}
        </button>
      </div>
    </div>
  );

  // Determine which overlay to show (if any) on top of the widget
  const overlay = isLoading ? (
    <div style={{ position: 'absolute', inset: 0, zIndex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--bg-primary)' }}>
      <div style={{ textAlign: 'center' }}>
        <div style={{
          width: 32, height: 32,
          borderWidth: 3, borderStyle: 'solid',
          borderColor: 'var(--border-primary)',
          borderTopColor: 'var(--accent-primary)',
          borderRadius: '50%',
          animation: 'spin 1s linear infinite',
          margin: '0 auto 12px',
        }} />
        <span style={{ fontSize: 13, color: 'var(--text-muted)' }}>Loading {profileType} profile...</span>
        <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      </div>
    </div>
  ) : error ? (
    <div style={{ position: 'absolute', inset: 0, zIndex: 1, background: 'var(--bg-primary)', padding: 24 }}>
      <div style={{
        padding: 20, borderRadius: 8,
        background: 'rgba(248, 81, 73, 0.1)',
        border: '1px solid rgba(248, 81, 73, 0.3)',
      }}>
        <div style={{ fontWeight: 600, color: '#f85149', marginBottom: 8 }}>Error loading flamegraph</div>
        <div style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>{error}</div>
      </div>
    </div>
  ) : unavailableReason ? (
    <div style={{ position: 'absolute', inset: 0, zIndex: 1, background: 'var(--bg-primary)', padding: 48, textAlign: 'center' }}>
      <div style={{ fontSize: 15, color: 'var(--text-tertiary)', marginBottom: 12 }}>Flamegraph unavailable</div>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 500, margin: '0 auto', lineHeight: 1.7 }}>
        {unavailableReason}
      </div>
      <pre style={{
        margin: '16px auto', padding: 16,
        background: 'var(--bg-tertiary)', borderRadius: 8,
        textAlign: 'left', fontSize: 11, color: '#58a6ff',
        overflow: 'auto', maxWidth: 500,
      }}>
        {`SET allow_introspection_functions = 1`}
      </pre>
    </div>
  ) : !hasData ? (
    <div style={{ position: 'absolute', inset: 0, zIndex: 1, background: 'var(--bg-primary)' }}>
      <EmptyState profileType={profileType} onTypeChange={handleTypeChange} />
    </div>
  ) : null;

  // Only mount the widget when we have data to avoid the connectedCallback
  // race: it auto-inits with empty attribute data, showing "Invalid input".
  const content = (
    <div style={{ flex: 1, overflow: 'hidden', minHeight: isFullscreen ? 0 : 400, position: 'relative' }}>
      {overlay}
      {hasData && (
        <speedscope-widget
          ref={setWidgetRef}
          style={{ display: 'block', width: '100%', height: '100%' }}
        />
      )}
    </div>
  );

  if (isFullscreen) {
    return (
      <div style={{
        position: 'fixed',
        top: 0, left: 0, right: 0, bottom: 0,
        zIndex: 10000,
        display: 'flex',
        flexDirection: 'column',
        background: 'var(--bg-primary)',
      }}>
        {header}
        {content}
      </div>
    );
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--bg-primary)' }}>
      {header}
      {content}
    </div>
  );
};

const EmptyState: React.FC<{ profileType: FlamegraphType; onTypeChange?: (type: FlamegraphType) => void }> = ({ profileType, onTypeChange }) => {
  const hints: Record<FlamegraphType, { text: string; code: string }> = {
    Memory: {
      text: 'Memory profiling requires memory_profiler_sample_probability to be set.',
      code: `-- Enable memory profiling\nSET memory_profiler_sample_probability = 1;\nSET max_untracked_memory = 1;\n\n-- Then run your query\nSELECT ...`,
    },
    Real: {
      text: 'Real (wall-clock) profiling shows where time is spent including I/O waits.',
      code: `-- Enable real-time profiling (default: 1 sample/sec)\nSET query_profiler_real_time_period_ns = 100000000;\n\n-- Then run your query\nSELECT ...`,
    },
    CPU: {
      text: 'CPU profiling is enabled by default (1 sample/sec). If no data appears, the query may have been too fast to capture samples.',
      code: `-- For more granular CPU profiling (10ms intervals)\nSET query_profiler_cpu_time_period_ns = 10000000;\n\n-- Then run your query\nSELECT ...`,
    },
  };
  const { text, code } = hints[profileType];
  return (
    <div style={{ padding: 48, textAlign: 'center' }}>
      <div style={{ fontSize: 15, color: 'var(--text-tertiary)', marginBottom: 12 }}>No {profileType} profile data available</div>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 500, margin: '0 auto', lineHeight: 1.7 }}>
        {text}
        <pre style={{
          margin: '16px 0', padding: 16,
          background: 'var(--bg-tertiary)', borderRadius: 8,
          textAlign: 'left', fontSize: 11, color: '#58a6ff', overflow: 'auto',
        }}>
          {code}
        </pre>
      </div>
      {onTypeChange && (
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', marginTop: 16 }}>
          {(['CPU', 'Real', 'Memory'] as FlamegraphType[]).filter(t => t !== profileType).map(t => (
            <button
              key={t}
              onClick={() => onTypeChange(t)}
              style={{
                padding: '8px 20px', fontSize: 13, borderRadius: 6,
                border: '1px solid var(--border-primary)',
                background: 'var(--bg-tertiary)',
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              Try {t}
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default SpeedscopeViewer;
