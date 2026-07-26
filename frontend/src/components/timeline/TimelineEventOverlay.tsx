import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { OperationalEvent } from '@tracehouse/core';
import {
  EVENT_SEVERITY_COLORS,
  clusterTimelineEvents,
  timelineEventClusterLabel,
} from './timeline-event-model';
import {
  formatTimelineEventTime,
  timelineEventKindLabel,
  timelineEventMarkerTitle,
} from './timeline-event-rail-model';

interface TimelineEventOverlayProps {
  events: OperationalEvent[];
  rangeStartMs: number;
  rangeEndMs: number;
  selectedEventId?: string | null;
  onSelectEvent: (event: OperationalEvent) => void;
  onClearEventSelection?: () => void;
  onViewEventDetails?: (event: OperationalEvent) => void;
}

export const TimelineEventOverlay: React.FC<TimelineEventOverlayProps> = ({
  events,
  rangeStartMs,
  rangeEndMs,
  selectedEventId,
  onSelectEvent,
  onClearEventSelection,
  onViewEventDetails,
}) => {
  const overlayRef = useRef<HTMLDivElement>(null);
  const [overlayWidth, setOverlayWidth] = useState(800);
  const [expandedClusterId, setExpandedClusterId] = useState<string | null>(null);
  const [hoveredClusterId, setHoveredClusterId] = useState<string | null>(null);
  const [activeListEventId, setActiveListEventId] = useState<string | null>(null);

  useEffect(() => {
    const element = overlayRef.current;
    if (!element) return;
    const update = () => setOverlayWidth(element.clientWidth || 800);
    update();
    const observer = new ResizeObserver(update);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!expandedClusterId && !selectedEventId) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      if (expandedClusterId) {
        setExpandedClusterId(null);
        setActiveListEventId(null);
      } else {
        onClearEventSelection?.();
      }
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [expandedClusterId, selectedEventId, onClearEventSelection]);

  const clusters = useMemo(
    () => clusterTimelineEvents(events, rangeStartMs, rangeEndMs, overlayWidth, 72),
    [events, rangeStartMs, rangeEndMs, overlayWidth],
  );
  const rangeMs = Math.max(1, rangeEndMs - rangeStartMs);

  if (clusters.length === 0) return null;

  return (
    <div
      ref={overlayRef}
      aria-label="Timeline event annotations"
      style={{
        position: 'absolute',
        inset: 0,
        zIndex: 7,
        pointerEvents: 'none',
      }}
    >
      {clusters.map(cluster => {
        const left = Math.max(0, Math.min(
          100,
          ((cluster.occurredAtMs - rangeStartMs) / rangeMs) * 100,
        ));
        const selectedEvent = cluster.events.find(event => event.id === selectedEventId);
        const selected = !!selectedEvent;
        const expanded = expandedClusterId === cluster.id;
        const hovered = hoveredClusterId === cluster.id;
        const color = EVENT_SEVERITY_COLORS[cluster.severity];
        const alignLeft = left < 18;
        const alignRight = left > 82;

        return (
          <div
            key={cluster.id}
            style={{
              position: 'absolute',
              left: `${left}%`,
              top: 4,
              bottom: 0,
              zIndex: selected || expanded ? 30 : hovered ? 20 : 5,
            }}
          >
            <span style={{
              position: 'absolute',
              top: 22,
              bottom: 0,
              left: 0,
              width: 1,
              background: color,
              opacity: selected || expanded || hovered ? 0.55 : 0.18,
              boxShadow: selected ? `0 0 5px ${color}` : 'none',
              pointerEvents: 'none',
              transition: 'opacity 0.12s ease',
            }} />
            <button
              type="button"
              title={timelineEventMarkerTitle(cluster)}
              aria-label={cluster.events.length === 1
                ? cluster.primaryEvent.title
                : `${cluster.events.length} events`}
              aria-expanded={cluster.events.length > 1 ? expanded : undefined}
              onMouseDown={event => event.stopPropagation()}
              onMouseEnter={() => setHoveredClusterId(cluster.id)}
              onMouseLeave={() => setHoveredClusterId(current =>
                current === cluster.id ? null : current,
              )}
              onClick={event => {
                event.stopPropagation();
                if (cluster.events.length === 1) {
                  if (selected && onClearEventSelection) onClearEventSelection();
                  else onSelectEvent(cluster.primaryEvent);
                  setExpandedClusterId(null);
                } else {
                  setExpandedClusterId(expanded ? null : cluster.id);
                }
              }}
              style={{
                position: 'absolute',
                left: 0,
                top: 0,
                transform: 'translateX(-50%)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 4,
                minWidth: 30,
                height: 22,
                padding: '0 7px',
                borderRadius: 6,
                background: 'color-mix(in srgb, var(--bg-secondary), transparent 8%)',
                border: selected
                  ? `2px solid ${color}`
                  : `1px solid ${color}99`,
                color,
                boxShadow: selected
                  ? `0 0 0 3px ${color}33, 0 3px 8px rgba(0,0,0,0.35)`
                  : hovered || expanded
                    ? '0 4px 12px rgba(0,0,0,0.38)'
                    : '0 2px 5px rgba(0,0,0,0.28)',
                backdropFilter: 'blur(6px)',
                cursor: 'pointer',
                fontSize: 9,
                fontWeight: 700,
                whiteSpace: 'nowrap',
                pointerEvents: 'auto',
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

            {expanded && (
              <div style={{
                position: 'absolute',
                top: 29,
                left: alignLeft ? 0 : alignRight ? 'auto' : '50%',
                right: alignRight ? 0 : 'auto',
                transform: alignLeft || alignRight ? 'none' : 'translateX(-50%)',
                width: 310,
                maxHeight: 250,
                overflow: 'auto',
                padding: 6,
                borderRadius: 8,
                background: 'var(--bg-secondary)',
                border: '1px solid var(--border-primary)',
                boxShadow: '0 10px 28px rgba(0,0,0,0.45)',
                pointerEvents: 'auto',
              }}>
                <div style={{
                  padding: '4px 7px 7px',
                  color: 'var(--text-muted)',
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: '0.4px',
                  textTransform: 'uppercase',
                }}>
                  {cluster.events.length} events at this time
                </div>
                {cluster.events.map(event => {
                  const active = activeListEventId === event.id;
                  const eventColor = EVENT_SEVERITY_COLORS[event.severity];
                  return (
                    <button
                      type="button"
                      key={event.id}
                      onMouseDown={mouseEvent => mouseEvent.stopPropagation()}
                      onMouseEnter={() => setActiveListEventId(event.id)}
                      onMouseLeave={() => setActiveListEventId(current =>
                        current === event.id ? null : current,
                      )}
                      onFocus={() => setActiveListEventId(event.id)}
                      onBlur={() => setActiveListEventId(current =>
                        current === event.id ? null : current,
                      )}
                      onClick={mouseEvent => {
                        mouseEvent.stopPropagation();
                        onSelectEvent(event);
                        setExpandedClusterId(null);
                        setActiveListEventId(null);
                      }}
                      style={{
                        width: '100%',
                        display: 'grid',
                        gridTemplateColumns: '8px minmax(0, 1fr) auto',
                        gap: 7,
                        alignItems: 'center',
                        padding: '6px 7px',
                        border: active
                          ? `1px solid ${eventColor}55`
                          : '1px solid transparent',
                        borderRadius: 5,
                        background: event.id === selectedEventId
                          ? 'var(--bg-hover)'
                          : active
                            ? `color-mix(in srgb, ${eventColor}, transparent 88%)`
                            : 'transparent',
                        boxShadow: 'none',
                        color: 'var(--text-primary)',
                        cursor: 'pointer',
                        textAlign: 'left',
                        transform: active ? 'translateX(2px)' : 'translateX(0)',
                        transition: 'background 0.1s ease, border-color 0.1s ease, transform 0.1s ease',
                      }}
                    >
                      <span style={{
                        width: active ? 8 : 7,
                        height: active ? 8 : 7,
                        borderRadius: '50%',
                        background: eventColor,
                        boxShadow: active ? `0 0 6px ${eventColor}` : 'none',
                      }} />
                      <span style={{ minWidth: 0 }}>
                        <span style={{
                          display: 'block',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          fontSize: 10,
                          fontWeight: active ? 600 : 400,
                        }}>
                          {event.title}
                        </span>
                        <span style={{ fontSize: 9, color: active ? 'var(--text-secondary)' : 'var(--text-muted)' }}>
                          {event.hostname ?? 'cluster'} · {timelineEventKindLabel(event.kind)}
                        </span>
                      </span>
                      <span style={{ fontSize: 9, color: active ? 'var(--text-secondary)' : 'var(--text-muted)' }}>
                        {formatTimelineEventTime(event.occurred_at)}
                      </span>
                    </button>
                  );
                })}
              </div>
            )}

            {selectedEvent && !expanded && (
              <div style={{
                position: 'absolute',
                top: 29,
                left: alignLeft ? 0 : alignRight ? 'auto' : '50%',
                right: alignRight ? 0 : 'auto',
                transform: alignLeft || alignRight ? 'none' : 'translateX(-50%)',
                width: 290,
                padding: 10,
                borderRadius: 8,
                background: 'var(--bg-secondary)',
                border: `1px solid ${EVENT_SEVERITY_COLORS[selectedEvent.severity]}88`,
                boxShadow: '0 10px 28px rgba(0,0,0,0.45)',
                pointerEvents: 'auto',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
                  <span style={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    flexShrink: 0,
                    background: EVENT_SEVERITY_COLORS[selectedEvent.severity],
                  }} />
                  <strong style={{
                    minWidth: 0,
                    flex: 1,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    color: 'var(--text-primary)',
                    fontSize: 11,
                  }}>
                    {selectedEvent.title}
                  </strong>
                  {onClearEventSelection && (
                    <button
                      type="button"
                      onClick={onClearEventSelection}
                      aria-label="Clear selected event"
                      style={{
                        border: 'none',
                        background: 'transparent',
                        color: 'var(--text-muted)',
                        cursor: 'pointer',
                        fontSize: 13,
                      }}
                    >
                      ×
                    </button>
                  )}
                </div>
                <div style={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  gap: '3px 8px',
                  marginTop: 7,
                  color: 'var(--text-muted)',
                  fontSize: 9,
                }}>
                  <span>{formatTimelineEventTime(selectedEvent.occurred_at)}</span>
                  <span>{timelineEventKindLabel(selectedEvent.kind)}</span>
                  {selectedEvent.hostname && <span>{selectedEvent.hostname}</span>}
                </div>
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: 8,
                  marginTop: 8,
                  paddingTop: 7,
                  borderTop: '1px solid var(--border-primary)',
                }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>
                    Activity pinned at this time
                  </span>
                  {onViewEventDetails && (
                    <button
                      type="button"
                      onClick={() => onViewEventDetails(selectedEvent)}
                      style={{
                        padding: '3px 7px',
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
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};
