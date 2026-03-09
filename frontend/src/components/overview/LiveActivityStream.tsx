/**
 * LiveActivityStream — live htop-style view of server activity
 *
 * Full-width bars for each running query and merge. Bar fill represents
 * relative CPU/memory usage. Bars pulse while active, flash on completion.
 * Dark background with color-coded glow — feels alive, not like a spreadsheet.
 */

import React, { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import { Link } from 'react-router-dom';
import type { RunningQueryInfo, ActiveMergeInfo } from '@tracehouse/core';
import { formatElapsed, truncateQuery, formatBytes } from '../../utils/formatters';

const BAR_H = 28;
const BAR_GAP = 2;
const MERGE_BAR_H = 24;

const COLORS: Record<string, string> = {
  SELECT: '#3B82F6',
  INSERT: '#10B981',
  ALTER: '#F59E0B',
  SYSTEM: '#EC4899',
  OTHER: '#8B5CF6',
  merge: '#F59E0B',
  mutation: '#EF4444',
};

function queryColor(kind: string): string {
  return COLORS[kind.toUpperCase()] ?? COLORS.OTHER;
}

interface LiveActivityStreamProps {
  queries: RunningQueryInfo[];
  merges: ActiveMergeInfo[];
  cpuUsage: number;
  memoryPct: number;
  onQueryClick?: (queryId: string) => void;
}

export const LiveActivityStream: React.FC<LiveActivityStreamProps> = ({
  queries,
  merges,
  cpuUsage,
  memoryPct,
  onQueryClick,
}) => {
  const prevQueryIds = useRef<Set<string>>(new Set());
  const [flashIds, setFlashIds] = useState<Set<string>>(new Set());
  const [tick, setTick] = useState(0);

  // Gentle pulse tick (~4fps, just for the pulse effect)
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 250);
    return () => clearInterval(id);
  }, []);

  // Detect new queries → flash them
  useEffect(() => {
    const currentIds = new Set(queries.map(q => q.queryId));
    const newIds = new Set<string>();
    for (const id of currentIds) {
      if (!prevQueryIds.current.has(id)) newIds.add(id);
    }
    prevQueryIds.current = currentIds;
    if (newIds.size > 0) {
      setFlashIds(newIds);
      setTimeout(() => setFlashIds(new Set()), 600);
    }
  }, [queries]);

  // Sort: longest-running first
  const sortedQueries = useMemo(() =>
    [...queries].sort((a, b) => b.elapsed - a.elapsed),
  [queries]);

  const sortedMerges = useMemo(() =>
    [...merges].sort((a, b) => b.elapsed - a.elapsed),
  [merges]);

  // Find max CPU among queries for relative bar sizing
  const maxCpu = useMemo(() => {
    const vals = queries.map(q => q.cpuCores);
    return Math.max(1, ...vals);
  }, [queries]);

  const cpuIntensity = Math.min(1, cpuUsage / 100);

  const handleQueryClick = useCallback((queryId: string) => {
    onQueryClick?.(queryId);
  }, [onQueryClick]);

  const hasActivity = queries.length > 0 || merges.length > 0;

  return (
    <div
      style={{
        position: 'relative',
        borderRadius: 10,
        overflow: 'hidden',
        background: '#0c0c14',
        border: '1px solid rgba(255,255,255,0.06)',
      }}
    >
      {/* CPU ambient glow on left edge */}
      <div style={{
        position: 'absolute',
        top: 0, left: 0, bottom: 0,
        width: 3,
        background: `rgba(59,130,246,${0.15 + cpuIntensity * 0.7})`,
        boxShadow: `0 0 ${8 + cpuIntensity * 20}px rgba(59,130,246,${cpuIntensity * 0.5})`,
        zIndex: 5,
        transition: 'all 0.5s',
      }} />

      {/* Header bar */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '8px 14px 6px',
        borderBottom: '1px solid rgba(255,255,255,0.04)',
      }}>
        <div style={{ display: 'flex', gap: 16, alignItems: 'center' }}>
          <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'rgba(255,255,255,0.35)', fontWeight: 600, letterSpacing: 1 }}>
            QUERIES
            <span style={{ marginLeft: 6, color: queries.length > 0 ? '#3B82F6' : 'rgba(255,255,255,0.15)', fontWeight: 700 }}>
              {queries.length}
            </span>
          </span>
          {merges.length > 0 && (
            <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'rgba(255,255,255,0.35)', fontWeight: 600, letterSpacing: 1 }}>
              MERGES
              <span style={{ marginLeft: 6, color: '#F59E0B', fontWeight: 700 }}>
                {merges.length}
              </span>
            </span>
          )}
        </div>
        <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
          <span style={{ fontSize: 10, fontFamily: 'monospace', color: `rgba(59,130,246,${0.5 + cpuIntensity * 0.5})`, fontWeight: 600 }}>
            CPU {cpuUsage.toFixed(1)}%
          </span>
          <span style={{ fontSize: 10, fontFamily: 'monospace', color: `rgba(139,92,246,${0.5 + (memoryPct / 100) * 0.5})`, fontWeight: 600 }}>
            MEM {memoryPct.toFixed(1)}%
          </span>
          <Link
            to="/timetravel"
            state={{ from: { path: '/overview', label: 'Overview' } }}
            style={{ fontSize: 9, fontFamily: 'monospace', color: 'rgba(255,255,255,0.2)', textDecoration: 'none' }}
          >
            Time Travel →
          </Link>
        </div>
      </div>

      {/* Query rows */}
      <div style={{ padding: '4px 6px' }}>
        {!hasActivity && (
          <div style={{
            height: 80,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}>
            <span style={{ fontSize: 11, fontFamily: 'monospace', color: 'rgba(255,255,255,0.12)' }}>
              idle
            </span>
          </div>
        )}

        {sortedQueries.map((q, i) => {
          const color = queryColor(q.queryKind);
          // Bar fill: relative CPU usage (wider = more CPU)
          const fillPct = Math.max(8, (q.cpuCores / maxCpu) * 100);
          // Pulse: subtle sine wave
          const pulse = 0.85 + Math.sin((tick + i) * 0.8) * 0.15;
          const isNew = flashIds.has(q.queryId);

          return (
            <div
              key={q.queryId}
              onClick={() => handleQueryClick(q.queryId)}
              style={{
                position: 'relative',
                height: BAR_H,
                marginBottom: BAR_GAP,
                borderRadius: 5,
                cursor: 'pointer',
                overflow: 'hidden',
                transition: 'transform 0.15s',
              }}
            >
              {/* Background fill — CPU-proportional */}
              <div style={{
                position: 'absolute',
                left: 0, top: 0, bottom: 0,
                width: `${fillPct}%`,
                borderRadius: 5,
                background: `linear-gradient(90deg, ${color}30 0%, ${color}18 70%, transparent 100%)`,
                opacity: pulse,
                transition: 'width 1s ease, opacity 0.25s',
              }} />

              {/* Flash on appear */}
              {isNew && (
                <div style={{
                  position: 'absolute', inset: 0,
                  borderRadius: 5,
                  background: `${color}20`,
                  animation: 'flash-in 0.6s ease-out forwards',
                }} />
              )}

              {/* Left accent */}
              <div style={{
                position: 'absolute',
                left: 0, top: 2, bottom: 2,
                width: 3,
                borderRadius: 2,
                background: color,
                opacity: pulse * 0.7,
                boxShadow: `0 0 6px ${color}80`,
              }} />

              {/* Content */}
              <div style={{
                position: 'relative',
                height: '100%',
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                padding: '0 10px 0 12px',
                overflow: 'hidden',
              }}>
                <span style={{
                  fontSize: 9, fontWeight: 700, fontFamily: 'monospace',
                  color, opacity: 0.9, flexShrink: 0,
                  width: 48,
                }}>
                  {q.queryKind}
                </span>
                <span style={{
                  fontSize: 10, fontFamily: 'monospace',
                  color: 'rgba(255,255,255,0.55)',
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                  flex: 1, minWidth: 0,
                }}>
                  {truncateQuery(q.query, 100)}
                </span>
                <span style={{ fontSize: 9, fontFamily: 'monospace', color: 'rgba(255,255,255,0.3)', flexShrink: 0 }}>
                  {q.user}
                </span>
                <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'rgba(255,255,255,0.45)', flexShrink: 0, width: 40, textAlign: 'right' }}>
                  {formatElapsed(q.elapsed)}
                </span>
                <span style={{ fontSize: 10, fontFamily: 'monospace', color: `${color}aa`, flexShrink: 0, width: 36, textAlign: 'right' }}>
                  {q.cpuCores.toFixed(2)}
                </span>
                <span style={{ fontSize: 9, fontFamily: 'monospace', color: 'rgba(139,92,246,0.5)', flexShrink: 0, width: 52, textAlign: 'right' }}>
                  {formatBytes(q.memoryUsage)}
                </span>
                {q.progress > 0 && (
                  <span style={{ fontSize: 9, fontFamily: 'monospace', color: 'rgba(255,255,255,0.25)', flexShrink: 0, width: 30, textAlign: 'right' }}>
                    {q.progress.toFixed(0)}%
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Merge rows */}
      {sortedMerges.length > 0 && (
        <div style={{
          padding: '2px 6px 6px',
          borderTop: '1px solid rgba(255,255,255,0.04)',
        }}>
          {sortedMerges.map((m, i) => {
            const progressPct = m.progress * 100;
            const pulse = 0.85 + Math.sin((tick + i) * 0.6) * 0.15;
            const color = m.isMutation ? COLORS.mutation : COLORS.merge;

            return (
              <div
                key={`${m.database}.${m.table}.${m.partName}`}
                title={`${m.database}.${m.table} → ${m.partName}`}
                style={{
                  position: 'relative',
                  height: MERGE_BAR_H,
                  marginBottom: BAR_GAP,
                  borderRadius: 4,
                  overflow: 'hidden',
                }}
              >
                {/* Progress fill */}
                <div style={{
                  position: 'absolute',
                  left: 0, top: 0, bottom: 0,
                  width: `${Math.max(4, progressPct)}%`,
                  borderRadius: 4,
                  background: `linear-gradient(90deg, ${color}28 0%, ${color}12 100%)`,
                  opacity: pulse,
                  transition: 'width 1s ease',
                }} />

                {/* Left accent */}
                <div style={{
                  position: 'absolute',
                  left: 0, top: 2, bottom: 2,
                  width: 3, borderRadius: 2,
                  background: color, opacity: 0.5,
                }} />

                {/* Content */}
                <div style={{
                  position: 'relative',
                  height: '100%',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  padding: '0 10px 0 12px',
                  overflow: 'hidden',
                }}>
                  <span style={{ fontSize: 8, fontWeight: 700, fontFamily: 'monospace', color, opacity: 0.8, flexShrink: 0, width: 48 }}>
                    {m.isMutation ? 'MUTATION' : 'MERGE'}
                  </span>
                  <span style={{
                    fontSize: 10, fontFamily: 'monospace',
                    color: 'rgba(255,255,255,0.4)',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    flex: 1, minWidth: 0,
                  }}>
                    <span style={{ color: 'rgba(255,255,255,0.2)' }}>{m.database}.</span>{m.table}
                  </span>
                  <span style={{ fontSize: 9, fontFamily: 'monospace', color: 'rgba(255,255,255,0.3)', flexShrink: 0 }}>
                    {m.numParts}→1
                  </span>
                  <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'rgba(255,255,255,0.35)', flexShrink: 0, width: 40, textAlign: 'right' }}>
                    {formatElapsed(m.elapsed)}
                  </span>
                  <span style={{ fontSize: 10, fontFamily: 'monospace', color: `${color}90`, flexShrink: 0, width: 36, textAlign: 'right' }}>
                    {progressPct.toFixed(0)}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* CSS animations */}
      <style>{`
        @keyframes flash-in {
          0% { opacity: 1; }
          100% { opacity: 0; }
        }
      `}</style>
    </div>
  );
};
