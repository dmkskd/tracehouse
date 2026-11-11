import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  OperationalEvent,
  EventSeverity,
  EventSourceCoverage,
} from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_SEVERITY_COLORS,
  EVENT_SEVERITY_VALUES,
  eventFilterCount,
  eventKindName,
  observedEventFilterCategories,
  observedEventFilterKinds,
  severityPresetFilter,
  toggleSetValue,
  type EventFilter,
} from './event-model';

interface EventFilterProps {
  filterUniverse: OperationalEvent[];
  coverage: EventSourceCoverage[];
  filter: EventFilter;
  onFilterChange: (filter: EventFilter) => void;
}

interface EventLayerControlsProps extends EventFilterProps {
  visible: boolean;
  shownCount: number;
  totalCount: number;
  onVisibilityChange: (visible: boolean) => void;
}

function coverageSummary(coverage: readonly EventSourceCoverage[]) {
  return {
    loaded: coverage.filter(item => item.status === 'loaded').length,
    problems: coverage.filter(item => item.status === 'failed' || item.truncated),
    title: coverage.map(item => {
      const suffix = item.truncated ? ', truncated' : '';
      return `${item.source}: ${item.status}${suffix}`;
    }).join('\n'),
  };
}

function useDismiss(open: boolean, ref: React.RefObject<HTMLElement | null>, close: () => void) {
  useEffect(() => {
    if (!open) return;
    const onMouseDown = (event: MouseEvent) => {
      if (ref.current && !ref.current.contains(event.target as Node)) close();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open, ref, close]);
}

/**
 * Event visibility, severity presets, the category/type filter, and source
 * coverage as one inline bar (Time Travel).
 */
export const EventLayerControls: React.FC<EventLayerControlsProps> = ({
  visible,
  shownCount,
  totalCount,
  filterUniverse,
  coverage,
  filter,
  onVisibilityChange,
  onFilterChange,
}) => {
  const filterRef = useRef<HTMLDivElement>(null);
  const [showFilters, setShowFilters] = useState(false);
  const closeFilters = useMemo(() => () => setShowFilters(false), []);
  useDismiss(showFilters, filterRef, closeFilters);

  const hiddenCount = eventFilterCount(filter);
  const sources = coverageSummary(coverage);

  if (!visible) {
    return (
      <button
        type="button"
        onClick={() => onVisibilityChange(true)}
        title="Show event annotations"
        style={{
          ...controlButtonStyle(false),
          display: 'flex',
          alignItems: 'center',
          gap: 7,
          height: 32,
          padding: '0 11px',
          background: 'var(--bg-tertiary)',
        }}
      >
        <span style={{
          width: 7,
          height: 7,
          borderRadius: '50%',
          border: '1px solid var(--text-muted)',
          opacity: 0.7,
        }} />
        Events hidden
        {totalCount > 0 && <span style={{ opacity: 0.75 }}>· {totalCount}</span>}
      </button>
    );
  }

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: 4,
      minHeight: 32,
      padding: 3,
      borderRadius: 8,
      background: 'var(--bg-tertiary)',
    }}>
      <EventCount shownCount={shownCount} totalCount={totalCount} />
      <SeverityPresets filter={filter} onFilterChange={onFilterChange} />

      <div ref={filterRef} style={{ position: 'relative' }}>
        <button
          type="button"
          onClick={() => setShowFilters(value => !value)}
          aria-expanded={showFilters}
          style={{
            ...controlButtonStyle(showFilters || hiddenCount > 0),
            color: showFilters || hiddenCount > 0 ? '#58a6ff' : 'var(--text-muted)',
          }}
        >
          Filters{hiddenCount > 0 ? ` · ${hiddenCount}` : ''}
          {sources.problems.length > 0 && <CoverageDot title={sources.title} />}
        </button>
        {showFilters && (
          <div style={popoverStyle}>
            <EventFilterPanel
              filterUniverse={filterUniverse}
              coverage={coverage}
              filter={filter}
              onFilterChange={onFilterChange}
            />
          </div>
        )}
      </div>

      <button
        type="button"
        onClick={() => {
          setShowFilters(false);
          onVisibilityChange(false);
        }}
        title="Hide all event annotations"
        style={{
          ...controlButtonStyle(false),
          marginLeft: 2,
          paddingInline: 8,
        }}
      >
        Hide
      </button>
    </div>
  );
};

/** How long the menu stays open after the pointer leaves, so it can move from button to menu. */
const MENU_CLOSE_DELAY_MS = 200;

interface EventLayerMenuProps extends EventLayerControlsProps {
  /** Class name for the toggle button; receives whether events are on. */
  buttonClassName?: (active: boolean) => string;
  buttonStyle?: React.CSSProperties;
  /** Whole-fetch failure, shown in the menu. */
  error?: string | null;
}

/**
 * The same controls as EventLayerControls behind one toggle button, for
 * headers without room for the inline bar (Analytics). Clicking toggles
 * events; hovering while events are on opens presets, filters and coverage.
 */
export const EventLayerMenu: React.FC<EventLayerMenuProps> = ({
  visible,
  shownCount,
  totalCount,
  filterUniverse,
  coverage,
  filter,
  onVisibilityChange,
  onFilterChange,
  buttonClassName,
  buttonStyle,
  error,
}) => {
  const rootRef = useRef<HTMLDivElement>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [open, setOpen] = useState(false);
  const close = useMemo(() => () => setOpen(false), []);
  useDismiss(open, rootRef, close);
  useEffect(() => () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
  }, []);

  const hiddenCount = eventFilterCount(filter);
  const sources = coverageSummary(coverage);
  const cancelClose = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    closeTimer.current = null;
  };

  return (
    <div
      ref={rootRef}
      style={{ position: 'relative', display: 'inline-flex' }}
      onMouseEnter={() => {
        cancelClose();
        if (visible) setOpen(true);
      }}
      onMouseLeave={() => {
        cancelClose();
        closeTimer.current = setTimeout(close, MENU_CLOSE_DELAY_MS);
      }}
    >
      <button
        type="button"
        onClick={() => {
          const next = !visible;
          onVisibilityChange(next);
          setOpen(next);
        }}
        className={buttonClassName?.(visible)}
        style={buttonStyle}
        aria-pressed={visible}
        aria-expanded={visible ? open : undefined}
        title={visible ? 'Hide operational events' : 'Show operational events (restarts, failures, DDL, ...) on time-series panels'}
      >
        Events
        {visible && (
          <span style={{ fontSize: 10, fontWeight: 500, opacity: 0.75 }}>
            {shownCount === totalCount ? totalCount : `${shownCount}/${totalCount}`}
          </span>
        )}
        {visible && (hiddenCount > 0 || sources.problems.length > 0 || error) && (
          <CoverageDot
            title={error ?? (sources.problems.length > 0 ? sources.title : `${hiddenCount} filters active`)}
            color={error || sources.problems.length > 0 ? '#d29922' : '#58a6ff'}
          />
        )}
      </button>
      {visible && open && (
        <div style={popoverStyle}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 10 }}>
            <EventCount shownCount={shownCount} totalCount={totalCount} />
            <SeverityPresets filter={filter} onFilterChange={onFilterChange} />
          </div>
          {error && (
            <div style={{ marginBottom: 8, color: '#d29922', fontSize: 10 }}>Events unavailable: {error}</div>
          )}
          <EventFilterPanel
            filterUniverse={filterUniverse}
            coverage={coverage}
            filter={filter}
            onFilterChange={onFilterChange}
          />
        </div>
      )}
    </div>
  );
};

const EventCount: React.FC<{ shownCount: number; totalCount: number }> = ({ shownCount, totalCount }) => (
  <span style={{
    display: 'flex',
    alignItems: 'baseline',
    gap: 6,
    padding: '0 5px 0 7px',
    whiteSpace: 'nowrap',
  }}>
    <span style={{
      color: 'var(--text-muted)',
      fontSize: 10,
      fontWeight: 700,
      letterSpacing: '0.5px',
      textTransform: 'uppercase',
    }}>
      Events
    </span>
    <span style={{ fontSize: 10, color: 'var(--text-secondary)' }}>
      {shownCount === totalCount ? totalCount : `${shownCount}/${totalCount}`}
    </span>
  </span>
);

const SeverityPresets: React.FC<Pick<EventFilterProps, 'filter' | 'onFilterChange'>> = ({ filter, onFilterChange }) => {
  const hiddenCount = eventFilterCount(filter);
  const applySeverityPreset = (severities: ReadonlySet<EventSeverity>) => {
    onFilterChange(severityPresetFilter(severities));
  };
  return (
    <>
      <button
        type="button"
        onClick={() => applySeverityPreset(new Set(EVENT_SEVERITY_VALUES))}
        style={controlButtonStyle(hiddenCount === 0)}
      >
        All
      </button>
      <button
        type="button"
        onClick={() => applySeverityPreset(new Set(['critical', 'error']))}
        style={controlButtonStyle(
          filter.hiddenSeverities.has('warning')
          && filter.hiddenSeverities.has('info')
          && !filter.hiddenSeverities.has('critical')
          && !filter.hiddenSeverities.has('error')
          && filter.hiddenCategories.size === 0
          && filter.hiddenKinds.size === 0,
        )}
      >
        Errors+
      </button>
      <button
        type="button"
        onClick={() => applySeverityPreset(new Set(['critical']))}
        style={controlButtonStyle(
          filter.hiddenSeverities.size === 3
          && !filter.hiddenSeverities.has('critical')
          && filter.hiddenCategories.size === 0
          && filter.hiddenKinds.size === 0,
        )}
      >
        Critical
      </button>
    </>
  );
};

/** Severity, category and event-type checkboxes plus source coverage. */
export const EventFilterPanel: React.FC<EventFilterProps> = ({
  filterUniverse,
  coverage,
  filter,
  onFilterChange,
}) => {
  const observedCategories = useMemo(
    () => observedEventFilterCategories(filterUniverse),
    [filterUniverse],
  );
  const observedKinds = useMemo(
    () => observedEventFilterKinds(filterUniverse),
    [filterUniverse],
  );
  const sources = coverageSummary(coverage);
  return (
    <>
      <FilterSection title="Severity">
        {EVENT_SEVERITY_VALUES.map(severity => (
          <FilterCheckbox
            key={severity}
            checked={!filter.hiddenSeverities.has(severity)}
            label={severity}
            color={EVENT_SEVERITY_COLORS[severity]}
            onChange={() => onFilterChange({
              ...filter,
              hiddenSeverities: toggleSetValue(filter.hiddenSeverities, severity),
            })}
          />
        ))}
      </FilterSection>
      {observedCategories.length > 0 && (
        <FilterSection title="Category">
          {observedCategories.map(category => (
            <FilterCheckbox
              key={category}
              checked={!filter.hiddenCategories.has(category)}
              label={EVENT_CATEGORY_LABELS[category]}
              onChange={() => onFilterChange({
                ...filter,
                hiddenCategories: toggleSetValue(filter.hiddenCategories, category),
              })}
            />
          ))}
        </FilterSection>
      )}
      {observedKinds.length > 0 && (
        <FilterSection title="Event type">
          {observedKinds.map(kind => (
            <FilterCheckbox
              key={kind}
              checked={!filter.hiddenKinds.has(kind)}
              label={eventKindName(kind)}
              onChange={() => onFilterChange({
                ...filter,
                hiddenKinds: toggleSetValue(filter.hiddenKinds, kind),
              })}
            />
          ))}
        </FilterSection>
      )}
      {coverage.length > 0 && (
        <div
          title={sources.title}
          style={{
            paddingTop: 8,
            borderTop: '1px solid var(--border-primary)',
            color: sources.problems.length > 0 ? '#d29922' : 'var(--text-muted)',
            fontSize: 10,
          }}
        >
          {sources.loaded}/{coverage.length} event sources loaded
          {sources.problems.length > 0 ? ' · partial coverage' : ''}
        </div>
      )}
    </>
  );
};

const CoverageDot: React.FC<{ title: string; color?: string }> = ({ title, color = '#d29922' }) => (
  <span
    title={title}
    style={{
      display: 'inline-block',
      width: 6,
      height: 6,
      marginLeft: 5,
      borderRadius: '50%',
      background: color,
    }}
  />
);

const popoverStyle: React.CSSProperties = {
  position: 'absolute',
  top: 'calc(100% + 7px)',
  right: 0,
  zIndex: 80,
  width: 470,
  maxWidth: 'min(470px, calc(100vw - 40px))',
  maxHeight: 380,
  overflow: 'auto',
  padding: 12,
  borderRadius: 8,
  background: 'var(--bg-secondary)',
  border: '1px solid var(--border-primary)',
  boxShadow: '0 8px 28px rgba(0,0,0,0.35)',
  textAlign: 'left',
};

const controlButtonStyle = (active: boolean): React.CSSProperties => ({
  height: 26,
  padding: '0 9px',
  borderRadius: 6,
  border: active
    ? '1px solid rgba(88,166,255,0.35)'
    : '1px solid var(--border-primary)',
  background: active ? 'rgba(88,166,255,0.1)' : 'var(--bg-secondary)',
  color: active ? '#58a6ff' : 'var(--text-muted)',
  fontSize: 10,
  cursor: 'pointer',
  whiteSpace: 'nowrap',
});

const FilterSection: React.FC<{
  title: string;
  children: React.ReactNode;
}> = ({ title, children }) => (
  <section style={{ marginBottom: 10 }}>
    <div style={{
      marginBottom: 5,
      color: 'var(--text-muted)',
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: '0.5px',
      textTransform: 'uppercase',
    }}>
      {title}
    </div>
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
      gap: '3px 8px',
    }}>
      {children}
    </div>
  </section>
);

const FilterCheckbox: React.FC<{
  checked: boolean;
  label: string;
  color?: string;
  onChange: () => void;
}> = ({ checked, label, color, onChange }) => (
  <label style={{
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '3px 4px',
    color: checked ? 'var(--text-secondary)' : 'var(--text-muted)',
    fontSize: 10,
    cursor: 'pointer',
    textTransform: 'capitalize',
  }}>
    <input
      type="checkbox"
      checked={checked}
      onChange={onChange}
      style={{ margin: 0, accentColor: color ?? '#58a6ff' }}
    />
    {color && <span aria-hidden="true" style={filterMarkerStyle(color)} />}
    <span>{label}</span>
  </label>
);

function filterMarkerStyle(color: string): React.CSSProperties {
  return {
    width: 7,
    height: 7,
    flexShrink: 0,
    background: color,
    borderRadius: '50%',
  };
}
