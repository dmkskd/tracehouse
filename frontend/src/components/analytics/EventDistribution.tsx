import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { TimelineEvent } from '@tracehouse/core';
import {
  clusterTimelineEvents,
  EVENT_CATEGORY_LABELS,
  EVENT_SEVERITY_COLORS,
  timelineEventClusterLabel,
} from '../timeline/timeline-event-model';
import {
  buildEventDistributionLanes,
  buildEventHoverCardModel,
  EVENT_CATEGORY_COLORS,
  EVENT_CATEGORY_SYMBOLS,
  EVENT_DISTRIBUTION_LAYOUT,
  eventMarkerShape,
  formatEventDistributionTick,
  groupEventsByCategory,
  isTimelineStateEpisode,
} from './event-distribution-model';

interface EventDistributionProps {
  events: readonly TimelineEvent[];
  rangeStartMs: number;
  rangeEndMs: number;
  selectedEventId?: string;
  onSelectEvent: (event: TimelineEvent) => void;
  onSelectCluster?: (
    events: TimelineEvent[],
    primaryEvent: TimelineEvent,
  ) => void;
  onRangeSelect?: (startMs: number, endMs: number) => void;
}

interface HoveredMarker {
  id: string;
  x: number;
  y: number;
  events: TimelineEvent[];
  primaryEvent: TimelineEvent;
  label: string;
}

const {
  labelWidth: LABEL_WIDTH,
  rightPadding: RIGHT_PADDING,
  topPadding: TOP_PADDING,
  axisHeight: AXIS_HEIGHT,
} = EVENT_DISTRIBUTION_LAYOUT;

export const EventDistribution: React.FC<EventDistributionProps> = ({
  events,
  rangeStartMs,
  rangeEndMs,
  selectedEventId,
  onSelectEvent,
  onSelectCluster,
  onRangeSelect,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(900);
  const [dragStartX, setDragStartX] = useState<number | null>(null);
  const [dragCurrentX, setDragCurrentX] = useState<number | null>(null);
  const [hoveredMarker, setHoveredMarker] = useState<HoveredMarker | null>(null);
  const spanMs = Math.max(1, rangeEndMs - rangeStartMs);
  const plotWidth = Math.max(1, width - LABEL_WIDTH - RIGHT_PADDING);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const updateWidth = () => setWidth(Math.max(320, element.clientWidth));
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const xForMs = (ms: number) => LABEL_WIDTH
    + ((ms - rangeStartMs) / spanMs) * plotWidth;
  const msForX = (x: number) => rangeStartMs
    + ((x - LABEL_WIDTH) / plotWidth) * spanMs;
  const clampPlotX = (x: number) => Math.max(
    LABEL_WIDTH,
    Math.min(width - RIGHT_PADDING, x),
  );

  const byCategory = useMemo(() => {
    return groupEventsByCategory(events);
  }, [events]);

  const lanes = useMemo(
    () => buildEventDistributionLanes(byCategory),
    [byCategory],
  );
  const laneAreaHeight = lanes.reduce((sum, lane) => sum + lane.laneHeight, 0);
  const height = TOP_PADDING + laneAreaHeight + AXIS_HEIGHT;

  const ticks = useMemo(
    () => Array.from({ length: 5 }, (_, index) =>
      rangeStartMs + (spanMs * index) / 4),
    [rangeStartMs, spanMs],
  );

  const pointerX = (event: React.PointerEvent<SVGSVGElement>) =>
    clampPlotX(event.clientX - event.currentTarget.getBoundingClientRect().left);
  const showMarkerHover = (
    event: React.PointerEvent<SVGElement>,
    id: string,
    markerEvents: TimelineEvent[],
    primaryEvent: TimelineEvent,
    label: string,
  ) => {
    const bounds = containerRef.current?.getBoundingClientRect();
    if (!bounds) return;
    setHoveredMarker({
      id,
      x: event.clientX - bounds.left,
      y: event.clientY - bounds.top,
      events: markerEvents,
      primaryEvent,
      label,
    });
  };

  return (
    <div
      ref={containerRef}
      style={{
        position: 'relative',
        border: '1px solid var(--border-primary)',
        borderRadius: 8,
        background: 'var(--bg-secondary)',
        overflow: 'hidden',
      }}
    >
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '7px 10px 5px',
        color: 'var(--text-muted)',
        fontSize: 9,
        textTransform: 'uppercase',
        letterSpacing: '0.6px',
      }}>
        <span>Events over time</span>
        <span style={{
          display: 'flex',
          alignItems: 'center',
          gap: 10,
          textTransform: 'none',
          letterSpacing: 0,
        }}>
          <span>● point</span>
          <span>━ state episode</span>
          <span>Click an event · drag to narrow</span>
        </span>
      </div>
      <svg
        width={width}
        height={height}
        role="img"
        aria-label="Events grouped by category over time"
        style={{ display: 'block', touchAction: 'none', userSelect: 'none' }}
        onPointerDown={event => {
          if (!onRangeSelect || pointerX(event) < LABEL_WIDTH) return;
          event.currentTarget.setPointerCapture(event.pointerId);
          const x = pointerX(event);
          setDragStartX(x);
          setDragCurrentX(x);
        }}
        onPointerMove={event => {
          if (dragStartX == null) return;
          setDragCurrentX(pointerX(event));
        }}
        onPointerUp={event => {
          if (dragStartX == null || !onRangeSelect) return;
          const endX = pointerX(event);
          if (Math.abs(endX - dragStartX) >= 6) {
            const startMs = msForX(Math.min(dragStartX, endX));
            const endMs = msForX(Math.max(dragStartX, endX));
            onRangeSelect(startMs, endMs);
          }
          setDragStartX(null);
          setDragCurrentX(null);
        }}
        onPointerCancel={() => {
          setDragStartX(null);
          setDragCurrentX(null);
        }}
      >
        {ticks.map((tick, index) => {
          const x = xForMs(tick);
          return (
            <g key={tick}>
              <line
                x1={x}
                x2={x}
                y1={TOP_PADDING}
                y2={height - AXIS_HEIGHT + 2}
                stroke="var(--border-primary)"
                strokeDasharray={index === 0 || index === 4 ? undefined : '2 4'}
                opacity={0.7}
              />
              <text
                x={x}
                y={height - 7}
                textAnchor={index === 0 ? 'start' : index === 4 ? 'end' : 'middle'}
                fill="var(--text-muted)"
                fontSize={9}
              >
                {formatEventDistributionTick(tick, spanMs)}
              </text>
            </g>
          );
        })}

        {lanes.map(({
          category,
          eventCount,
          laneHeight,
          yTop,
          y,
        }, categoryIndex) => {
          const categoryEvents = byCategory.get(category) ?? [];
          const categoryColor = EVENT_CATEGORY_COLORS[category];
          const active = eventCount > 0;
          const intervalEvents = categoryEvents.filter(isTimelineStateEpisode);
          const pointEvents = categoryEvents.filter(event => !intervalEvents.includes(event));
          const clusters = clusterTimelineEvents(
            pointEvents,
            rangeStartMs,
            rangeEndMs,
            plotWidth,
            12,
          );
          return (
            <g key={category}>
              <rect
                x={0}
                y={yTop}
                width={width}
                height={laneHeight}
                fill={active ? categoryColor : 'var(--text-muted)'}
                opacity={active ? 0.055 : categoryIndex % 2 === 0 ? 0.018 : 0.008}
              />
              <rect
                x={0}
                y={yTop + 3}
                width={3}
                height={Math.max(4, laneHeight - 6)}
                rx={1.5}
                fill={categoryColor}
                opacity={active ? 0.9 : 0.2}
              />
              <line
                x1={LABEL_WIDTH}
                x2={width - RIGHT_PADDING}
                y1={y}
                y2={y}
                stroke={active ? categoryColor : 'var(--border-primary)'}
                opacity={active ? 0.3 : 0.65}
              />
              <text
                x={13}
                y={y + 3}
                fill={categoryColor}
                fontSize={11}
                fontWeight={700}
              >
                {EVENT_CATEGORY_SYMBOLS[category]}
              </text>
              <text
                x={28}
                y={y + 3}
                fill={active ? 'var(--text-primary)' : 'var(--text-muted)'}
                fontSize={9}
                fontWeight={active ? 600 : 400}
              >
                {EVENT_CATEGORY_LABELS[category]}
              </text>
              {active && (
                <g>
                  <rect
                    x={LABEL_WIDTH - 31}
                    y={y - 7}
                    width={23}
                    height={14}
                    rx={7}
                    fill={categoryColor}
                    opacity={0.14}
                  />
                  <text
                    x={LABEL_WIDTH - 19.5}
                    y={y + 3}
                    textAnchor="middle"
                    fill={categoryColor}
                    fontSize={8}
                    fontWeight={700}
                  >
                    {eventCount > 999 ? '999+' : eventCount}
                  </text>
                </g>
              )}

              {intervalEvents.map(event => {
                const start = Math.max(rangeStartMs, Date.parse(event.occurred_at));
                const parsedEnd = Date.parse(event.ended_at ?? '');
                const end = Math.min(
                  rangeEndMs,
                  Number.isFinite(parsedEnd) ? parsedEnd : rangeEndMs,
                );
                if (!Number.isFinite(start) || !Number.isFinite(end) || end < rangeStartMs) {
                  return null;
                }
                const x = xForMs(start);
                const endX = xForMs(end);
                const selected = event.id === selectedEventId;
                return (
                  <rect
                    key={event.id}
                    x={x}
                    y={y - 4}
                    width={Math.max(3, endX - x)}
                    height={8}
                    rx={4}
                    fill={EVENT_SEVERITY_COLORS[event.severity]}
                    opacity={selected ? 1 : 0.7}
                    stroke={selected ? 'var(--text-primary)' : 'none'}
                    strokeWidth={selected ? 1.5 : 0}
                    style={{ cursor: 'pointer' }}
                    onPointerEnter={event_ => showMarkerHover(
                      event_,
                      event.id,
                      [event],
                      event,
                      event.title,
                    )}
                    onPointerLeave={() => setHoveredMarker(null)}
                    onPointerDown={event_ => event_.stopPropagation()}
                    onClick={() => onSelectEvent(event)}
                  />
                );
              })}

              {clusters.map(cluster => {
                const x = xForMs(cluster.occurredAtMs);
                const selected = cluster.events.some(event => event.id === selectedEventId);
                const hovered = hoveredMarker?.id === cluster.id;
                const radius = cluster.events.length > 1 ? 7 : 4.5;
                const shape = eventMarkerShape(cluster.primaryEvent);
                const markerLabel = timelineEventClusterLabel(cluster);
                return (
                  <g
                    key={cluster.id}
                    transform={`translate(${x}, ${y})`}
                    style={{ cursor: 'pointer' }}
                    onPointerEnter={event => showMarkerHover(
                      event,
                      cluster.id,
                      cluster.events,
                      cluster.primaryEvent,
                      markerLabel,
                    )}
                    onPointerLeave={() => setHoveredMarker(null)}
                    onPointerDown={event => event.stopPropagation()}
                    onClick={() => {
                      if (cluster.events.length > 1 && onSelectCluster) {
                        onSelectCluster(
                          cluster.events,
                          cluster.primaryEvent,
                        );
                      } else {
                        onSelectEvent(cluster.primaryEvent);
                      }
                    }}
                  >
                    <line
                      x1={0}
                      x2={0}
                      y1={radius}
                      y2={laneHeight / 2 - 3}
                      stroke={EVENT_SEVERITY_COLORS[cluster.severity]}
                      strokeWidth={1}
                      opacity={0.35}
                    />
                    {cluster.events.length > 1 || shape === 'circle' ? (
                      <circle
                        r={radius}
                        fill={EVENT_SEVERITY_COLORS[cluster.severity]}
                        stroke={selected || hovered ? 'var(--text-primary)' : 'var(--bg-secondary)'}
                        strokeWidth={selected || hovered ? 2 : 1}
                      />
                    ) : shape === 'diamond' ? (
                      <rect
                        x={-4}
                        y={-4}
                        width={8}
                        height={8}
                        rx={1}
                        transform="rotate(45)"
                        fill={EVENT_SEVERITY_COLORS[cluster.severity]}
                        stroke={selected || hovered ? 'var(--text-primary)' : 'var(--bg-secondary)'}
                        strokeWidth={selected || hovered ? 2 : 1}
                      />
                    ) : shape === 'square' ? (
                      <rect
                        x={-4.5}
                        y={-4.5}
                        width={9}
                        height={9}
                        rx={2}
                        fill={EVENT_SEVERITY_COLORS[cluster.severity]}
                        stroke={selected || hovered ? 'var(--text-primary)' : 'var(--bg-secondary)'}
                        strokeWidth={selected || hovered ? 2 : 1}
                      />
                    ) : (
                      <path
                        d="M 0 -5 L 5 4 L -5 4 Z"
                        fill={EVENT_SEVERITY_COLORS[cluster.severity]}
                        stroke={selected || hovered ? 'var(--text-primary)' : 'var(--bg-secondary)'}
                        strokeWidth={selected || hovered ? 2 : 1}
                      />
                    )}
                    {cluster.events.length > 1 && (
                      <text
                        y={2.7}
                        textAnchor="middle"
                        fill="#fff"
                        fontSize={7}
                        fontWeight={700}
                      >
                        {cluster.events.length > 99 ? '99+' : cluster.events.length}
                      </text>
                    )}
                  </g>
                );
              })}
            </g>
          );
        })}

        {dragStartX != null && dragCurrentX != null && (
          <rect
            x={Math.min(dragStartX, dragCurrentX)}
            y={TOP_PADDING}
            width={Math.abs(dragCurrentX - dragStartX)}
            height={laneAreaHeight}
            fill="rgba(88,166,255,0.13)"
            stroke="#58a6ff"
            strokeWidth={1}
            pointerEvents="none"
          />
        )}
      </svg>
      {hoveredMarker && (
        <EventHoverCard
          marker={hoveredMarker}
          containerWidth={width}
        />
      )}
    </div>
  );
};

const HOVER_CARD_WIDTH = 270;

const EventHoverCard: React.FC<{
  marker: HoveredMarker;
  containerWidth: number;
}> = ({ marker, containerWidth }) => {
  const { events, primaryEvent } = marker;
  const severityColor = EVENT_SEVERITY_COLORS[primaryEvent.severity];
  const model = buildEventHoverCardModel(events, primaryEvent, marker.label);
  const left = marker.x + HOVER_CARD_WIDTH + 22 > containerWidth
    ? marker.x - HOVER_CARD_WIDTH - 14
    : marker.x + 14;
  const top = marker.y > 135 ? marker.y - 118 : marker.y + 16;

  return (
    <div style={{
      position: 'absolute',
      left: Math.max(8, left),
      top: Math.max(8, top),
      width: HOVER_CARD_WIDTH,
      zIndex: 20,
      pointerEvents: 'none',
      overflow: 'hidden',
      border: `1px solid ${severityColor}`,
      borderRadius: 7,
      background: 'var(--bg-primary)',
      color: 'var(--text-primary)',
      boxShadow: '0 8px 24px rgba(0,0,0,0.22)',
    }}>
      <div style={{ height: 3, background: severityColor }} />
      <div style={{ padding: '9px 11px 8px' }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 8,
          marginBottom: 5,
          color: 'var(--text-muted)',
          fontSize: 8,
          fontWeight: 700,
          letterSpacing: '0.55px',
          textTransform: 'uppercase',
        }}>
          <span>{model.categoryLabel}</span>
          <span style={{ color: severityColor }}>
            {model.severityLabel}
            {events.length > 1 ? ` · ${events.length} events` : ''}
          </span>
        </div>
        <div style={{
          fontSize: 12,
          lineHeight: 1.25,
          fontWeight: 700,
          marginBottom: 5,
        }}>
          {model.title}
        </div>
        <div style={{
          color: 'var(--text-secondary)',
          fontSize: 9,
          lineHeight: 1.35,
          marginBottom: model.distinctTitles.length > 1 ? 6 : 0,
        }}>
          {model.timeLabel}
          {model.hostsLabel ? ` · ${model.hostsLabel}` : ''}
        </div>
        {model.distinctTitles.length > 1 && (
          <div style={{
            display: 'flex',
            flexDirection: 'column',
            gap: 3,
            paddingTop: 6,
            borderTop: '1px solid var(--border-primary)',
          }}>
            {model.distinctTitles.slice(0, 3).map(title => (
              <div key={title} style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                minWidth: 0,
                fontSize: 9,
                color: 'var(--text-secondary)',
              }}>
                <span style={{
                  width: 5,
                  height: 5,
                  flexShrink: 0,
                  borderRadius: '50%',
                  background: severityColor,
                }} />
                <span style={{
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}>
                  {title}
                </span>
              </div>
            ))}
            {model.distinctTitles.length > 3 && (
              <div style={{ color: 'var(--text-muted)', fontSize: 8 }}>
                +{model.distinctTitles.length - 3} more event types
              </div>
            )}
          </div>
        )}
        {model.detail && (
          <div style={{
            marginTop: 6,
            color: 'var(--text-muted)',
            fontSize: 8,
            lineHeight: 1.35,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}>
            {model.detail}
          </div>
        )}
        <div style={{
          marginTop: 7,
          paddingTop: 6,
          borderTop: '1px solid var(--border-primary)',
          color: 'var(--text-muted)',
          fontSize: 8,
        }}>
          {model.actionLabel}
        </div>
      </div>
    </div>
  );
};
