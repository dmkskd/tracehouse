import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  OperationalEvent,
  EventSeverity,
  EventSourceCoverage,
} from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_SEVERITY_COLORS,
  EVENT_SEVERITY_MARKER_SHAPES,
  EVENT_SEVERITY_VALUES,
  timelineEventFilterCount,
  type EventSeverityMarkerShape,
  type TimelineEventFilter,
} from './timeline-event-model';
import {
  buildSeverityPresetFilter,
  observedTimelineEventCategories,
  observedTimelineEventKinds,
  timelineEventKindLabel,
  toggleSetValue,
} from './timeline-event-rail-model';

interface TimelineEventControlsProps {
  visible: boolean;
  shownCount: number;
  totalCount: number;
  filterUniverse: OperationalEvent[];
  coverage: EventSourceCoverage[];
  filter: TimelineEventFilter;
  onVisibilityChange: (visible: boolean) => void;
  onFilterChange: (filter: TimelineEventFilter) => void;
}

export const TimelineEventControls: React.FC<TimelineEventControlsProps> = ({
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

  useEffect(() => {
    if (!showFilters) return;
    const onMouseDown = (event: MouseEvent) => {
      if (filterRef.current && !filterRef.current.contains(event.target as Node)) {
        setShowFilters(false);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setShowFilters(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [showFilters]);

  const observedCategories = useMemo(
    () => observedTimelineEventCategories(filterUniverse),
    [filterUniverse],
  );
  const observedKinds = useMemo(
    () => observedTimelineEventKinds(filterUniverse),
    [filterUniverse],
  );
  const hiddenCount = timelineEventFilterCount(filter);
  const loadedSources = coverage.filter(item => item.status === 'loaded').length;
  const coverageProblems = coverage.filter(item =>
    item.status === 'failed' || item.truncated,
  );
  const coverageTitle = coverage.map(item => {
    const suffix = item.truncated ? ', truncated' : '';
    return `${item.source}: ${item.status}${suffix}`;
  }).join('\n');

  const applySeverityPreset = (severities: ReadonlySet<EventSeverity>) => {
    onFilterChange(buildSeverityPresetFilter(severities));
  };

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
          {coverageProblems.length > 0 && (
            <span
              title={coverageTitle}
              style={{
                display: 'inline-block',
                width: 6,
                height: 6,
                marginLeft: 5,
                borderRadius: '50%',
                background: '#d29922',
              }}
            />
          )}
        </button>
        {showFilters && (
          <div style={{
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
          }}>
            <FilterSection title="Severity">
              {EVENT_SEVERITY_VALUES.map(severity => (
                <FilterCheckbox
                  key={severity}
                  checked={!filter.hiddenSeverities.has(severity)}
                  label={severity}
                  color={EVENT_SEVERITY_COLORS[severity]}
                  markerShape={EVENT_SEVERITY_MARKER_SHAPES[severity]}
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
                    label={timelineEventKindLabel(kind)}
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
                title={coverageTitle}
                style={{
                  paddingTop: 8,
                  borderTop: '1px solid var(--border-primary)',
                  color: coverageProblems.length > 0 ? '#d29922' : 'var(--text-muted)',
                  fontSize: 10,
                }}
              >
                {loadedSources}/{coverage.length} event sources loaded
                {coverageProblems.length > 0 ? ' · partial coverage' : ''}
              </div>
            )}
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
  markerShape?: EventSeverityMarkerShape;
  onChange: () => void;
}> = ({ checked, label, color, markerShape = 'circle', onChange }) => (
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
    {color && <span aria-hidden="true" style={filterMarkerStyle(color, markerShape)} />}
    <span>{label}</span>
  </label>
);

function filterMarkerStyle(
  color: string,
  shape: EventSeverityMarkerShape,
): React.CSSProperties {
  const base: React.CSSProperties = {
    width: 7,
    height: 7,
    flexShrink: 0,
    background: color,
  };

  if (shape === 'circle') return { ...base, borderRadius: '50%' };
  if (shape === 'diamond') {
    return {
      ...base,
      borderRadius: 1,
      transform: 'rotate(45deg) scale(0.78)',
    };
  }
  if (shape === 'triangle') {
    return {
      width: 0,
      height: 0,
      flexShrink: 0,
      background: 'transparent',
      borderLeft: '4px solid transparent',
      borderRight: '4px solid transparent',
      borderBottom: `8px solid ${color}`,
    };
  }
  return { ...base, borderRadius: 1 };
}
