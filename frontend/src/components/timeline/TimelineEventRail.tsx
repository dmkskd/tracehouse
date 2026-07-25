import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  TimelineEvent,
  TimelineEventSeverity,
  TimelineEventSourceCoverage,
} from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_SEVERITY_COLORS,
  TIMELINE_EVENT_SEVERITIES,
  clusterTimelineEvents,
  timelineEventClusterLabel,
  timelineEventFilterCount,
  type TimelineEventFilter,
} from './timeline-event-model';
import {
  buildSeverityPresetFilter,
  formatTimelineEventTime,
  observedTimelineEventCategories,
  observedTimelineEventKinds,
  timelineEventMarkerTitle,
  timelineEventKindLabel,
  toggleSetValue,
} from './timeline-event-rail-model';

interface TimelineEventRailProps {
  events: TimelineEvent[];
  windowEventCount: number;
  filterUniverse: TimelineEvent[];
  coverage: TimelineEventSourceCoverage[];
  filter: TimelineEventFilter;
  onFilterChange: (filter: TimelineEventFilter) => void;
  rangeStartMs: number;
  rangeEndMs: number;
  selectedEventId?: string | null;
  onSelectEvent: (event: TimelineEvent) => void;
  onClearEventSelection?: () => void;
  onViewEventDetails?: (event: TimelineEvent) => void;
}

export const TimelineEventRail: React.FC<TimelineEventRailProps> = ({
  events,
  windowEventCount,
  filterUniverse,
  coverage,
  filter,
  onFilterChange,
  rangeStartMs,
  rangeEndMs,
  selectedEventId,
  onSelectEvent,
  onClearEventSelection,
  onViewEventDetails,
}) => {
  const railRef = useRef<HTMLDivElement>(null);
  const filterRef = useRef<HTMLDivElement>(null);
  const [railWidth, setRailWidth] = useState(1000);
  const [showFilters, setShowFilters] = useState(false);
  const [expandedClusterId, setExpandedClusterId] = useState<string | null>(null);

  useEffect(() => {
    const element = railRef.current;
    if (!element) return;
    const update = () => setRailWidth(element.clientWidth || 1000);
    update();
    const observer = new ResizeObserver(update);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

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

  const clusters = useMemo(
    () => clusterTimelineEvents(events, rangeStartMs, rangeEndMs, railWidth, 64),
    [events, rangeStartMs, rangeEndMs, railWidth],
  );
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
  const selectedEvent = filterUniverse.find(event => event.id === selectedEventId);
  const rangeMs = Math.max(1, rangeEndMs - rangeStartMs);

  const applySeverityPreset = (visible: ReadonlySet<TimelineEventSeverity>) => {
    onFilterChange(buildSeverityPresetFilter(visible));
  };

  const coverageTitle = coverage.map(item => {
    const suffix = item.truncated ? ', truncated' : '';
    return `${item.source}: ${item.status}${suffix}`;
  }).join('\n');

  return (
    <div style={{ marginTop: 10 }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 7,
        marginBottom: 5,
        minHeight: 26,
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
          {events.length === windowEventCount
            ? `${windowEventCount} events`
            : `${events.length} of ${windowEventCount} events`}
        </span>

        <button
          onClick={() => applySeverityPreset(new Set(TIMELINE_EVENT_SEVERITIES))}
          style={presetButtonStyle(hiddenCount === 0)}
        >
          All
        </button>
        <button
          onClick={() => applySeverityPreset(new Set(['critical', 'error']))}
          style={presetButtonStyle(
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
          onClick={() => applySeverityPreset(new Set(['critical']))}
          style={presetButtonStyle(
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
            onClick={() => setShowFilters(value => !value)}
            aria-expanded={showFilters}
            style={{
              ...presetButtonStyle(showFilters || hiddenCount > 0),
              color: showFilters || hiddenCount > 0 ? '#58a6ff' : 'var(--text-muted)',
            }}
          >
            Filters{hiddenCount > 0 ? ` · ${hiddenCount}` : ''}
          </button>
          {showFilters && (
            <div style={{
              position: 'absolute',
              top: 'calc(100% + 5px)',
              left: 0,
              zIndex: 80,
              width: 470,
              maxHeight: 380,
              overflow: 'auto',
              padding: 12,
              borderRadius: 8,
              background: 'var(--bg-secondary)',
              border: '1px solid var(--border-primary)',
              boxShadow: '0 8px 28px rgba(0,0,0,0.35)',
            }}>
              <FilterSection title="Severity">
                {TIMELINE_EVENT_SEVERITIES.map(severity => (
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
                      label={timelineEventKindLabel(kind)}
                      onChange={() => onFilterChange({
                        ...filter,
                        hiddenKinds: toggleSetValue(filter.hiddenKinds, kind),
                      })}
                    />
                  ))}
                </FilterSection>
              )}
            </div>
          )}
        </div>

        <span
          title={coverageTitle}
          style={{
            marginLeft: 'auto',
            fontSize: 10,
            color: coverageProblems.length > 0 ? '#d29922' : 'var(--text-muted)',
            cursor: 'help',
          }}
        >
          {loadedSources}/{coverage.length} sources
          {coverageProblems.length > 0 ? ' · partial' : ''}
        </span>
      </div>

      <div
        ref={railRef}
        style={{
          position: 'relative',
          height: selectedEvent ? 68 : 42,
          borderRadius: 7,
          background: 'var(--bg-tertiary)',
          border: '1px solid var(--border-primary)',
          overflow: 'visible',
          transition: 'height 0.15s ease',
        }}
      >
        <div style={{
          position: 'absolute',
          left: 10,
          right: 10,
          top: 30,
          height: 1,
          background: 'var(--border-secondary)',
        }} />
        {clusters.map(cluster => {
          const left = Math.max(0, Math.min(
            100,
            ((cluster.occurredAtMs - rangeStartMs) / rangeMs) * 100,
          ));
          const selected = cluster.events.some(event => event.id === selectedEventId);
          const expanded = expandedClusterId === cluster.id;
          const markerTitle = timelineEventMarkerTitle(cluster);
          return (
            <div
              key={cluster.id}
              style={{
                position: 'absolute',
                left: `${left}%`,
                top: 4,
                transform: 'translateX(-50%)',
                zIndex: selected || expanded ? 20 : 5,
              }}
            >
              <button
                title={markerTitle}
                aria-label={cluster.events.length === 1
                  ? cluster.primaryEvent.title
                  : `${cluster.events.length} events`}
                onClick={() => {
                  if (cluster.events.length === 1) {
                    if (selected && onClearEventSelection) onClearEventSelection();
                    else onSelectEvent(cluster.primaryEvent);
                    setExpandedClusterId(null);
                  } else {
                    setExpandedClusterId(expanded ? null : cluster.id);
                  }
                }}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: 4,
                  minWidth: 30,
                  height: 22,
                  padding: '0 7px',
                  borderRadius: 6,
                  background: 'var(--bg-secondary)',
                  border: selected
                    ? `2px solid ${EVENT_SEVERITY_COLORS[cluster.severity]}`
                    : `1px solid ${EVENT_SEVERITY_COLORS[cluster.severity]}99`,
                  color: EVENT_SEVERITY_COLORS[cluster.severity],
                  boxShadow: selected
                    ? `0 0 0 3px ${EVENT_SEVERITY_COLORS[cluster.severity]}44`
                    : '0 2px 5px rgba(0,0,0,0.35)',
                  cursor: 'pointer',
                  fontSize: 9,
                  fontWeight: 700,
                  whiteSpace: 'nowrap',
                }}
              >
                {cluster.events.length > 1 && new Set(
                  cluster.events.map(event => event.severity),
                ).size > 1 && (
                  <span style={{ display: 'inline-flex', gap: 2 }}>
                    {[...new Set(cluster.events.map(event => event.severity))]
                      .slice(0, 3)
                      .map(severity => (
                        <span
                          key={severity}
                          style={{
                            width: 5,
                            height: 5,
                            borderRadius: '50%',
                            background: EVENT_SEVERITY_COLORS[severity],
                          }}
                        />
                      ))}
                  </span>
                )}
                {timelineEventClusterLabel(cluster)}
              </button>
              <span style={{
                position: 'absolute',
                top: 22,
                left: '50%',
                width: 1,
                height: 7,
                transform: 'translateX(-50%)',
                background: EVENT_SEVERITY_COLORS[cluster.severity],
                opacity: 0.75,
                pointerEvents: 'none',
              }} />
              {expanded && (
                <div style={{
                  position: 'absolute',
                  top: 32,
                  left: '50%',
                  transform: 'translateX(-50%)',
                  width: 300,
                  maxHeight: 230,
                  overflow: 'auto',
                  padding: 6,
                  borderRadius: 7,
                  background: 'var(--bg-secondary)',
                  border: '1px solid var(--border-primary)',
                  boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
                }}>
                  {cluster.events.map(event => (
                    <button
                      key={event.id}
                      onClick={() => {
                        if (event.id === selectedEventId && onClearEventSelection) {
                          onClearEventSelection();
                        } else {
                          onSelectEvent(event);
                        }
                        setExpandedClusterId(null);
                      }}
                      style={{
                        width: '100%',
                        display: 'grid',
                        gridTemplateColumns: '8px 1fr auto',
                        gap: 7,
                        alignItems: 'center',
                        padding: '6px 7px',
                        border: 'none',
                        borderRadius: 5,
                        background: event.id === selectedEventId
                          ? 'var(--bg-hover)'
                          : 'transparent',
                        color: 'var(--text-primary)',
                        cursor: 'pointer',
                        textAlign: 'left',
                      }}
                    >
                      <span style={{
                        width: 7,
                        height: 7,
                        borderRadius: '50%',
                        background: EVENT_SEVERITY_COLORS[event.severity],
                      }} />
                      <span style={{ minWidth: 0 }}>
                        <span style={{
                          display: 'block',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          fontSize: 10,
                        }}>
                          {event.title}
                        </span>
                        <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>
                          {event.hostname ?? 'cluster'} · {timelineEventKindLabel(event.kind)}
                        </span>
                      </span>
                      <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>
                        {formatTimelineEventTime(event.occurred_at)}
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          );
        })}
        {clusters.length === 0 && (
          <div style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: 'var(--text-muted)',
            fontSize: 10,
          }}>
          {windowEventCount > 0
              ? 'No events match the current filters in this window'
              : 'No events recorded in this window'}
          </div>
        )}
        {selectedEvent && (
          <div style={{
            position: 'absolute',
            left: 10,
            right: 10,
            bottom: 5,
            display: 'flex',
            alignItems: 'center',
            gap: 7,
            minWidth: 0,
            color: 'var(--text-secondary)',
            fontSize: 10,
          }}>
            <span style={{
              width: 7,
              height: 7,
              borderRadius: '50%',
              flexShrink: 0,
              background: EVENT_SEVERITY_COLORS[selectedEvent.severity],
            }} />
            <strong style={{
              color: 'var(--text-primary)',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}>
              {selectedEvent.title}
            </strong>
            <span>{formatTimelineEventTime(selectedEvent.occurred_at)}</span>
            {selectedEvent.hostname && <span>{selectedEvent.hostname}</span>}
            {selectedEvent.query_id && (
              <span style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}>
                query {selectedEvent.query_id}
              </span>
            )}
            <span style={{ marginLeft: 'auto', color: 'var(--text-muted)' }}>
              pinned · showing activity running then
            </span>
            {onClearEventSelection && (
              <button
                onClick={onClearEventSelection}
                style={{
                  padding: '2px 7px',
                  borderRadius: 5,
                  border: '1px solid var(--border-primary)',
                  background: 'var(--bg-secondary)',
                  color: 'var(--text-muted)',
                  fontSize: 9,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                }}
              >
                Clear event
              </button>
            )}
            {onViewEventDetails && (
              <button
                onClick={() => onViewEventDetails(selectedEvent)}
                style={{
                  padding: '2px 7px',
                  borderRadius: 5,
                  border: '1px solid rgba(88,166,255,0.35)',
                  background: 'rgba(88,166,255,0.08)',
                  color: '#58a6ff',
                  fontSize: 9,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                }}
              >
                View details →
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

const presetButtonStyle = (active: boolean): React.CSSProperties => ({
  padding: '3px 8px',
  borderRadius: 5,
  border: active
    ? '1px solid rgba(88,166,255,0.35)'
    : '1px solid var(--border-primary)',
  background: active ? 'rgba(88,166,255,0.1)' : 'var(--bg-tertiary)',
  color: active ? '#58a6ff' : 'var(--text-muted)',
  fontSize: 10,
  cursor: 'pointer',
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
    {color && <span style={{ width: 6, height: 6, borderRadius: '50%', background: color }} />}
    <span>{label}</span>
  </label>
);
