import React, { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { DefaultZIndexes, ZIndexLayer, usePlotArea, useXAxisDomain } from 'recharts';
import type { OperationalEvent } from '@tracehouse/core';
import { EVENT_SEVERITY_COLORS, eventKindName, type EventCluster } from './event-model';
import { buildEventLayerMarks, type EventBand } from './event-layer-model';
import { formatEventDateTime } from './events-dashboard-model';
import { eventClusterMarkerRadius } from './event-distribution-model';

type HoveredMark =
  | { kind: 'point'; cluster: EventCluster }
  | { kind: 'band'; band: EventBand };

interface HoverState {
  mark: HoveredMark;
  clientX: number;
  clientY: number;
}

/** Half width of the hover target around a point marker, in pixels. */
const POINT_HIT_HALF_WIDTH = 6;
/** Height of the hover strip on top of an episode band, in pixels. */
const BAND_HIT_HEIGHT = 8;
const HOVER_CARD_MAX_EVENTS = 6;
const HOVER_CARD_WIDTH = 320;

// Guides sit over area fills and under series lines; markers and hover targets
// sit over everything, so hovering a marker reaches the event before the data point.
const GUIDE_Z = DefaultZIndexes.area + 1;
const MARKER_Z = DefaultZIndexes.activeDot + 1;

export interface ChartEventLayerProps {
  events: readonly OperationalEvent[];
  /** Called when the pointer enters or leaves an event; charts hide their data tooltip meanwhile. */
  onHoverChange?: (hovered: boolean) => void;
  /** Time format for the hover card; defaults to the Events page format. */
  formatTime?: (iso: string) => string;
  /** Click on a marker or band: the event, or a cluster's most severe event. */
  onEventClick?: (event: OperationalEvent) => void;
}

/**
 * Events over a recharts chart whose x axis is epoch ms: episode bands and
 * point markers, with a hover card that takes priority over the data tooltip.
 * Must be rendered inside a recharts cartesian chart.
 */
export const ChartEventLayer: React.FC<ChartEventLayerProps> = ({ events, onHoverChange, formatTime = formatEventDateTime, onEventClick }) => {
  const xDomain = useXAxisDomain() as unknown[] | undefined;
  const plotArea = usePlotArea();
  const [hover, setHover] = useState<HoverState | null>(null);
  const startMs = xDomain?.[0];
  const endMs = xDomain?.[xDomain.length - 1];
  const drawable = !!plotArea && typeof startMs === 'number' && typeof endMs === 'number' && endMs > startMs;
  const { points, bands } = drawable
    ? buildEventLayerMarks(events, { startMs, endMs }, plotArea.width)
    : { points: [], bands: [] };

  // A hovered mark can disappear without a mouseleave (filter change, zoom, new
  // window). The chart must get its data tooltip back when that happens.
  const hoveredId = hover
    ? hover.mark.kind === 'point' ? hover.mark.cluster.id : hover.mark.band.event.id
    : null;
  const hoverPresent = hoveredId !== null
    && (points.some(point => point.id === hoveredId) || bands.some(band => band.event.id === hoveredId));
  const onHoverChangeRef = useRef(onHoverChange);
  useEffect(() => {
    onHoverChangeRef.current = onHoverChange;
  });
  useEffect(() => {
    if (!hoverPresent) return;
    return () => onHoverChangeRef.current?.(false);
  }, [hoverPresent]);

  if (!drawable) return null;
  const toX = (ms: number) => plotArea.x + ((ms - startMs) / (endMs - startMs)) * plotArea.width;
  const top = plotArea.y;
  const bottom = plotArea.y + plotArea.height;

  const enter = (mark: HoveredMark) => (e: React.MouseEvent) => {
    setHover({ mark, clientX: e.clientX, clientY: e.clientY });
    onHoverChange?.(true);
  };
  const move = (e: React.MouseEvent) => {
    setHover(current => current && { ...current, clientX: e.clientX, clientY: e.clientY });
  };
  const leave = () => {
    setHover(null);
    onHoverChange?.(false);
  };
  // Marker clicks never reach the chart's own click handler (drill-down).
  const click = (event: OperationalEvent) => (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!onEventClick) return;
    leave();
    onEventClick(event);
  };
  const cursor = onEventClick ? 'pointer' : 'default';

  return (
    <>
      <ZIndexLayer zIndex={GUIDE_Z}>
        <g pointerEvents="none">
          {bands.map(band => {
            const color = EVENT_SEVERITY_COLORS[band.event.severity];
            const x1 = toX(band.startMs);
            const x2 = toX(band.endMs);
            const hovered = hoveredId === band.event.id;
            return (
              <g key={band.event.id}>
                <rect x={x1} y={top} width={Math.max(1, x2 - x1)} height={plotArea.height} fill={color} fillOpacity={hovered ? 0.2 : 0.12} />
                {!band.startsBefore && <line x1={x1} x2={x1} y1={top} y2={bottom} stroke={color} strokeOpacity={0.7} />}
                {!band.open && <line x1={x2} x2={x2} y1={top} y2={bottom} stroke={color} strokeOpacity={0.7} />}
              </g>
            );
          })}
          {points.map(point => {
            const x = toX(point.occurredAtMs);
            return (
              <line key={point.id} x1={x} x2={x} y1={top} y2={bottom}
                stroke={EVENT_SEVERITY_COLORS[point.severity]}
                strokeWidth={hoveredId === point.id ? 2 : 1.5}
                strokeOpacity={hoveredId === point.id ? 1 : 0.8}
                strokeDasharray="3 2" />
            );
          })}
        </g>
      </ZIndexLayer>
      <ZIndexLayer zIndex={MARKER_Z}>
        <g>
          {bands.map(band => {
            const x1 = toX(band.startMs);
            const x2 = toX(band.endMs);
            return (
              <rect key={band.event.id} x={x1} y={top} width={Math.max(6, x2 - x1)} height={BAND_HIT_HEIGHT}
                fill={EVENT_SEVERITY_COLORS[band.event.severity]} fillOpacity={0.85}
                style={{ cursor }}
                onMouseEnter={enter({ kind: 'band', band })} onMouseMove={move} onMouseLeave={leave} onClick={click(band.event)} />
            );
          })}
          {points.map(point => {
            const x = toX(point.occurredAtMs);
            const color = EVENT_SEVERITY_COLORS[point.severity];
            const hovered = hoveredId === point.id;
            const radius = eventClusterMarkerRadius(point.events.length);
            return (
              <g key={point.id}>
                {/* Same bubble as the Events page: area grows with the count, the count is written inside. */}
                <circle cx={x} cy={top + radius + 1} r={radius} fill={color} pointerEvents="none"
                  stroke={hovered ? 'var(--text-primary)' : 'var(--bg-secondary)'} strokeWidth={hovered ? 2 : 1} />
                {point.events.length > 1 && (
                  <text x={x} y={top + radius + 3.7} textAnchor="middle" fill="#fff"
                    fontSize={radius >= 10 ? 8 : 7} fontWeight={700} pointerEvents="none">
                    {point.events.length > 99 ? '99+' : point.events.length}
                  </text>
                )}
                {/* Full-height target: anywhere on the marker line shows the event, not the data point. */}
                <rect x={x - POINT_HIT_HALF_WIDTH} y={top} width={POINT_HIT_HALF_WIDTH * 2} height={plotArea.height}
                  fill="transparent" pointerEvents="all" style={{ cursor }}
                  onMouseEnter={enter({ kind: 'point', cluster: point })} onMouseMove={move} onMouseLeave={leave} onClick={click(point.primaryEvent)} />
              </g>
            );
          })}
        </g>
      </ZIndexLayer>
      {hover && hoverPresent && createPortal(
        <EventHoverCard mark={hover.mark} clientX={hover.clientX} clientY={hover.clientY} formatTime={formatTime} clickable={!!onEventClick} />,
        document.body,
      )}
    </>
  );
};

const EventHoverCard: React.FC<{
  mark: HoveredMark;
  clientX: number;
  clientY: number;
  formatTime: (iso: string) => string;
  clickable: boolean;
}> = ({ mark, clientX, clientY, formatTime, clickable }) => {
  const events = mark.kind === 'point' ? mark.cluster.events : [mark.band.event];
  const shown = events.slice(0, HOVER_CARD_MAX_EVENTS);
  let left = clientX + 16;
  if (left + HOVER_CARD_WIDTH > window.innerWidth) left = clientX - HOVER_CARD_WIDTH - 16;
  const topPx = Math.max(10, Math.min(clientY + 12, window.innerHeight - 260));
  return (
    <div style={{
      position: 'fixed', left, top: topPx, zIndex: 10001, pointerEvents: 'none',
      width: HOVER_CARD_WIDTH, padding: '8px 10px', borderRadius: 8,
      background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
      boxShadow: '0 8px 24px rgba(0,0,0,0.3)', fontSize: 11, lineHeight: 1.4,
    }}>
      {events.length > 1 && (
        <div style={{ color: 'var(--text-muted)', fontSize: 9, fontWeight: 700, letterSpacing: '0.4px', textTransform: 'uppercase', marginBottom: 6 }}>
          {events.length} events
        </div>
      )}
      {shown.map((event, i) => (
        <div key={event.id} style={{ marginTop: i > 0 ? 7 : 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 7, height: 7, borderRadius: '50%', flexShrink: 0, background: EVENT_SEVERITY_COLORS[event.severity] }} />
            <span style={{ color: 'var(--text-primary)', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{event.title}</span>
          </div>
          <div style={{ color: 'var(--text-muted)', fontSize: 10, paddingLeft: 13 }}>
            {mark.kind === 'band'
              ? `${formatTime(event.started_at ?? event.occurred_at)} – ${event.ended_at ? formatTime(event.ended_at) : 'ongoing'}`
              : formatTime(event.occurred_at)}
            {' · '}{event.hostname ?? 'cluster'} · {eventKindName(event.kind)}
          </div>
          {event.detail && (
            <div style={{
              color: 'var(--text-secondary)', fontSize: 10, paddingLeft: 13, marginTop: 2,
              display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden',
            }}>
              {event.detail}
            </div>
          )}
        </div>
      ))}
      {events.length > shown.length && (
        <div style={{ color: 'var(--text-muted)', fontSize: 10, marginTop: 6 }}>+{events.length - shown.length} more</div>
      )}
      {clickable && (
        <div style={{ color: '#a78bfa', fontSize: 10, marginTop: 7, paddingTop: 6, borderTop: '1px solid var(--border-primary)' }}>
          {events.length > 1 ? 'Click to open the most severe event in Events' : 'Click to open in Events'}
        </div>
      )}
    </div>
  );
};
