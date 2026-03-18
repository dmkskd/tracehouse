/**
 * MergeTimeline — Gantt-style timeline of merge events for a part's lineage.
 *
 * Extracted from PartInspector.tsx so the inspector stays focused on
 * orchestrating tabs and the timeline rendering is independently maintainable.
 */

import React, { useState, useMemo } from 'react';
import { formatDurationMs } from '../../utils/formatters';
import type { PartLineageInfo, LineageNode } from '../../stores/databaseStore';

// Colors for lineage levels (shared with LineageVisualization)
export const LINEAGE_LEVEL_COLORS = [
  { bg: '#ec4899', border: '#f472b6', text: '#fce7f3' }, // L0 - Pink
  { bg: '#8b5cf6', border: '#a78bfa', text: '#ede9fe' }, // L1 - Purple
  { bg: '#3b82f6', border: '#60a5fa', text: '#dbeafe' }, // L2 - Blue
  { bg: '#06b6d4', border: '#22d3ee', text: '#cffafe' }, // L3 - Cyan
  { bg: '#10b981', border: '#34d399', text: '#d1fae5' }, // L4 - Emerald
  { bg: '#f59e0b', border: '#fbbf24', text: '#fef3c7' }, // L5 - Amber
  { bg: '#ef4444', border: '#f87171', text: '#fee2e2' }, // L6 - Red
  { bg: '#6366f1', border: '#818cf8', text: '#e0e7ff' }, // L7+ - Indigo
];

export const getLevelColor = (level: number) =>
  LINEAGE_LEVEL_COLORS[Math.min(level, LINEAGE_LEVEL_COLORS.length - 1)];

export interface TimelineEvent {
  level: number;
  partName: string;
  startTime: Date;
  endTime: Date;
  durationMs: number;
  sourceParts: number;
  sizeBytes: number;
  mergeAlgorithm: string;
  isMutation: boolean;
}

/**
 * Collect timeline events from a lineage tree.
 */
export function collectTimelineEvents(lineage: PartLineageInfo): TimelineEvent[] {
  const events: TimelineEvent[] = [];

  const walk = (node: LineageNode) => {
    if (node.merge_event && node.event_time) {
      const endTime = new Date(node.event_time);
      const startTime = new Date(endTime.getTime() - node.merge_event.duration_ms);
      events.push({
        level: node.level,
        partName: node.part_name,
        startTime,
        endTime,
        durationMs: node.merge_event.duration_ms,
        sourceParts: node.merge_event.merged_from.length,
        sizeBytes: node.size_in_bytes,
        mergeAlgorithm: node.merge_event.merge_algorithm || 'Horizontal',
        isMutation: node.merge_event.event_type === 'MutatePart',
      });
    }
    node.children.forEach(walk);
  };

  if (lineage.root) walk(lineage.root);

  return events.sort((a, b) => {
    const timeDiff = a.startTime.getTime() - b.startTime.getTime();
    if (timeDiff !== 0) return timeDiff;
    return a.partName.localeCompare(b.partName);
  });
}


function formatTime(date: Date): string {
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}


export const MergeTimeline: React.FC<{ lineage: PartLineageInfo }> = ({ lineage }) => {
  const [hoveredEvent, setHoveredEvent] = useState<{
    event: TimelineEvent;
    x: number;
    y: number;
  } | null>(null);
  
  const timelineEvents = useMemo(() => collectTimelineEvents(lineage), [lineage]);
  
  const eventsByLevel = useMemo(() => {
    const grouped = new Map<number, TimelineEvent[]>();
    timelineEvents.forEach(event => {
      if (!grouped.has(event.level)) {
        grouped.set(event.level, []);
      }
      grouped.get(event.level)!.push(event);
    });
    return grouped;
  }, [timelineEvents]);
  
  const levels = useMemo(() => {
    return Array.from(eventsByLevel.keys()).sort((a, b) => a - b);
  }, [eventsByLevel]);
  
  const timeStats = useMemo(() => {
    if (timelineEvents.length === 0) return null;
    
    let minTime = Infinity;
    let maxTime = -Infinity;
    
    timelineEvents.forEach(event => {
      minTime = Math.min(minTime, event.startTime.getTime());
      maxTime = Math.max(maxTime, event.endTime.getTime());
    });
    
    return {
      firstTime: new Date(minTime),
      lastTime: new Date(maxTime),
      totalSpanMs: maxTime - minTime,
    };
  }, [timelineEvents]);
  
  const timeMarkers = useMemo(() => {
    if (!timeStats) return [];
    const markers: { time: Date; position: number }[] = [];
    const numMarkers = 5;
    for (let i = 0; i < numMarkers; i++) {
      const position = i / (numMarkers - 1);
      const time = new Date(timeStats.firstTime.getTime() + position * timeStats.totalSpanMs);
      markers.push({ time, position: position * 100 });
    }
    return markers;
  }, [timeStats]);
  
  if (!timeStats || timelineEvents.length === 0) {
    return (
      <div style={{ 
        background: 'var(--bg-card)', 
        border: '1px solid var(--border-secondary)', 
        borderRadius: 8, 
        padding: 16,
        marginBottom: 16,
        textAlign: 'center',
      }}>
        <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          Timeline data unavailable — merge events have no timestamps
        </div>
      </div>
    );
  }
  
  const rowHeight = 28;
  const labelWidth = 50;

  
  return (
    <div style={{ 
      background: 'var(--bg-card)', 
      border: '1px solid var(--border-secondary)', 
      borderRadius: 8, 
      padding: 16,
      marginBottom: 16,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px' }}>
          Merge Timeline
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <div style={{ fontSize: 10, color: 'var(--text-tertiary)' }}>
            <span style={{ color: '#22d3ee', fontWeight: 600, fontFamily: 'monospace' }}>
              {formatDurationMs(timeStats.totalSpanMs)}
            </span>
            <span style={{ marginLeft: 6 }}>elapsed</span>
          </div>
        </div>
      </div>
      
      <div style={{ position: 'relative' }}>
        {levels.map((level, rowIndex) => {
          const events = eventsByLevel.get(level) || [];
          const levelColor = getLevelColor(level);
          const totalDuration = events.reduce((sum, e) => sum + e.durationMs, 0);
          
          return (
            <div 
              key={level} 
              style={{ 
                display: 'flex', 
                alignItems: 'center', 
                height: rowHeight, 
                marginBottom: rowIndex < levels.length - 1 ? 4 : 0,
              }}
            >
              <div style={{ 
                width: labelWidth, 
                flexShrink: 0,
                display: 'flex',
                alignItems: 'center',
                gap: 6,
              }}>
                <div style={{
                  padding: '2px 6px',
                  borderRadius: 3,
                  background: levelColor.bg,
                  color: 'white',
                  fontSize: 9,
                  fontWeight: 600,
                  fontFamily: 'monospace',
                }}>
                  L{level}
                </div>
                <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>
                  ({events.length})
                </span>
              </div>
              
              <div style={{ 
                flex: 1, 
                position: 'relative', 
                height: '100%',
                background: 'var(--bg-card)',
                borderRadius: 4,
                overflow: 'hidden',
              }}>
                <div style={{
                  position: 'absolute',
                  top: '50%',
                  left: 0,
                  right: 0,
                  height: 1,
                  background: 'var(--border-secondary)',
                  transform: 'translateY(-50%)',
                }} />
                
                {events.map((event, i) => {
                  const startOffset = timeStats.totalSpanMs > 0
                    ? ((event.startTime.getTime() - timeStats.firstTime.getTime()) / timeStats.totalSpanMs) * 100
                    : 0;
                  const widthPercent = timeStats.totalSpanMs > 0
                    ? Math.max((event.durationMs / timeStats.totalSpanMs) * 100, 0.5)
                    : 10;
                  
                  const blockMatch = event.partName.match(/_(\d+)_\d+_\d+$/);
                  const blockNum = blockMatch ? parseInt(blockMatch[1], 10) : i;
                  const baseZIndex = 1000 - Math.min(blockNum, 999);
                  const barColor = event.isMutation ? '#f59e0b' : levelColor.bg;
                  
                  return (
                    <div
                      key={`${event.partName}-${i}`}
                      style={{
                        position: 'absolute',
                        left: `${startOffset}%`,
                        width: `${Math.min(widthPercent, 100 - startOffset)}%`,
                        top: 4,
                        bottom: 4,
                        background: event.isMutation 
                          ? `repeating-linear-gradient(45deg, ${barColor}, ${barColor} 2px, ${barColor}80 2px, ${barColor}80 4px)`
                          : barColor,
                        borderRadius: 2,
                        cursor: 'pointer',
                        transition: 'all 0.15s ease',
                        minWidth: 3,
                        zIndex: baseZIndex,
                      }}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.transform = 'scaleY(1.3)';
                        e.currentTarget.style.zIndex = '10000';
                        e.currentTarget.style.boxShadow = `0 0 8px ${barColor}`;
                        const rect = e.currentTarget.getBoundingClientRect();
                        setHoveredEvent({ event, x: rect.left + rect.width / 2, y: rect.top });
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.transform = 'scaleY(1)';
                        e.currentTarget.style.zIndex = String(baseZIndex);
                        e.currentTarget.style.boxShadow = 'none';
                        setHoveredEvent(null);
                      }}
                    />
                  );
                })}
              </div>
              
              <div style={{ 
                width: 60, 
                flexShrink: 0, 
                textAlign: 'right',
                fontSize: 9,
                color: 'var(--text-muted)',
                fontFamily: 'monospace',
                paddingLeft: 8,
              }}>
                Σ {formatDurationMs(totalDuration)}
              </div>
            </div>
          );
        })}
        
        <div style={{ 
          position: 'relative',
          marginTop: 8,
          marginLeft: labelWidth,
          marginRight: 68,
          height: 16,
        }}>
          {timeMarkers.map((marker, i) => (
            <div
              key={i}
              style={{
                position: 'absolute',
                left: `${marker.position}%`,
                transform: 'translateX(-50%)',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
              }}
            >
              <div style={{
                width: 1,
                height: 4,
                background: 'var(--border-primary)',
                marginBottom: 2,
              }} />
              <div style={{
                fontSize: 8,
                color: 'var(--text-muted)',
                fontFamily: 'monospace',
                whiteSpace: 'nowrap',
              }}>
                {formatTime(marker.time)}
              </div>
            </div>
          ))}
        </div>
      </div>
      
      {hoveredEvent && (
        <div
          style={{
            position: 'fixed',
            left: hoveredEvent.x,
            top: hoveredEvent.y - 10,
            transform: 'translate(-50%, -100%)',
            background: 'var(--bg-secondary)',
            border: '1px solid var(--border-primary)',
            borderRadius: 6,
            padding: '8px 12px',
            zIndex: 100000,
            pointerEvents: 'none',
            minWidth: 180,
          }}
        >
          <div style={{ fontSize: 11, color: 'var(--text-primary)', fontWeight: 600, marginBottom: 4, fontFamily: 'monospace', display: 'flex', alignItems: 'center', gap: 6 }}>
            {hoveredEvent.event.partName}
            {hoveredEvent.event.isMutation && (
              <span style={{ fontSize: 8, padding: '1px 4px', borderRadius: 2, background: '#f59e0b30', color: '#fbbf24' }}>
                MUT
              </span>
            )}
          </div>
          <div style={{ fontSize: 10, color: 'var(--text-secondary)', marginBottom: 2 }}>
            Duration: <span style={{ color: '#22d3ee' }}>{formatDurationMs(hoveredEvent.event.durationMs)}</span>
          </div>
          <div style={{ fontSize: 10, color: 'var(--text-secondary)' }}>
            {hoveredEvent.event.sourceParts} source part{hoveredEvent.event.sourceParts !== 1 ? 's' : ''} → L{hoveredEvent.event.level}
            {hoveredEvent.event.isMutation && ' (mutation)'}
          </div>
        </div>
      )}
    </div>
  );
};

export default MergeTimeline;
