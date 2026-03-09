/**
 * TraceLogViewer - Clean, minimal trace log viewer
 * 
 * Scandinavian-inspired design with focus on readability and clarity.
 */

import React, { useState, useMemo, useEffect } from 'react';
import type { TraceLog, TraceLogFilter, LogLevel } from '../../stores/traceStore';
import {
  filterTraceLogs,
  VALID_LOG_LEVELS,
} from '../../stores/traceStore';
import { useClickStackStore, buildClickStackUrl, buildClickStackUrlNoSource } from '../../stores/clickStackStore';
import { useConnectionStore } from '../../stores/connectionStore';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import { ZoomPanContainer } from '../common/ZoomPanContainer';
import { useThemeDetection } from '../../hooks/useThemeDetection';
import { getLevelColor, stringToHslColor, getThreadIdColor, processLogsWithDuration } from './traceUtils';
import type { ProcessedLog } from './traceUtils';

interface TraceLogViewerProps {
  logs: TraceLog[];
  isLoading: boolean;
  error: string | null;
  filter: TraceLogFilter;
  onFilterChange: (filter: Partial<TraceLogFilter>) => void;
  onRefresh?: () => void;
  /** Query ID for ClickStack deep link */
  queryId?: string;
  /** Query start time (ISO string) for ClickStack time window */
  queryStartTime?: string;
  /** Query end time (ISO string) for ClickStack time window */
  queryEndTime?: string;
}


function formatTime(ts: string): string {
  if (!ts) return '';
  const match = ts.match(/(\d{2}:\d{2}:\d{2})\.?(\d+)?/);
  if (match) {
    return `${match[1]}${match[2] ? '.' + match[2].slice(0, 3) : ''}`;
  }
  return ts;
}

/**
 * DAG Timeline visualization - each log entry is a node
 * Y position = time (bottlenecks show as large gaps)
 * Shows fork/join pattern: coordinator → workers → coordinator
 */
const TimelineView: React.FC<{ logs: ProcessedLog[] }> = ({ logs }) => {
  const [viewMode, setViewMode] = useState<'graph' | 'analysis'>('analysis');

  if (logs.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
        No trace logs to visualize
      </div>
    );
  }

  // Parse timestamps
  const timestamps = logs.map(l => new Date(l.event_time_microseconds?.replace(' ', 'T') + 'Z').getTime());
  const minTime = Math.min(...timestamps);
  const maxTime = Math.max(...timestamps);
  const totalDuration = maxTime - minTime || 1;

  // Find coordinator thread (TCPHandler)
  const coordinatorThreadId = logs.find(l => l.thread_name?.includes('TCPHandler'))?.thread_id ?? logs[0]?.thread_id;

  const fmtDur = (ms: number) => ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(0)}ms`;

  // Analysis: find thread transitions and phases
  const analysis = useMemo(() => {
    const phases: Array<{
      type: 'coordinator' | 'parallel' | 'transition';
      startIdx: number;
      endIdx: number;
      startTime: number;
      endTime: number;
      threads: Set<number>;
      description: string;
    }> = [];

    let currentPhase: typeof phases[0] | null = null;
    
    logs.forEach((log, i) => {
      const time = timestamps[i];
      const isCoord = log.thread_id === coordinatorThreadId;
      const prevLog = i > 0 ? logs[i - 1] : null;
      const prevIsCoord = prevLog?.thread_id === coordinatorThreadId;

      // Detect phase transitions
      if (i === 0) {
        currentPhase = {
          type: isCoord ? 'coordinator' : 'parallel',
          startIdx: 0,
          endIdx: 0,
          startTime: time,
          endTime: time,
          threads: new Set([log.thread_id]),
          description: isCoord ? 'Coordinator init' : 'Worker start',
        };
      } else if (prevIsCoord && !isCoord) {
        // Coordinator → Worker = FORK
        if (currentPhase) {
          currentPhase.endIdx = i - 1;
          currentPhase.endTime = timestamps[i - 1];
          phases.push(currentPhase);
        }
        currentPhase = {
          type: 'parallel',
          startIdx: i,
          endIdx: i,
          startTime: timestamps[i - 1], // Workers START at last coordinator log
          endTime: time,
          threads: new Set([log.thread_id]),
          description: `Fork @ ${prevLog?.source}`,
        };
      } else if (!prevIsCoord && isCoord) {
        // Worker → Coordinator = JOIN
        if (currentPhase) {
          currentPhase.endIdx = i - 1;
          currentPhase.endTime = time; // Workers END at first coordinator log after
          phases.push(currentPhase);
        }
        currentPhase = {
          type: 'coordinator',
          startIdx: i,
          endIdx: i,
          startTime: time,
          endTime: time,
          threads: new Set([log.thread_id]),
          description: `Join @ ${log.source}`,
        };
      }

      if (currentPhase) {
        currentPhase.endIdx = i;
        currentPhase.endTime = time;
        currentPhase.threads.add(log.thread_id);
      }
    });

    if (currentPhase) {
      phases.push(currentPhase);
    }

    return phases;
  }, [logs, timestamps, coordinatorThreadId]);

  // Fork/Join analysis - find the key points
  const forkJoinAnalysis = useMemo(() => {
    // Find last coordinator log before first worker
    let forkIdx = -1;
    let joinIdx = -1;
    
    for (let i = 0; i < logs.length; i++) {
      if (logs[i].thread_id !== coordinatorThreadId) {
        forkIdx = i - 1; // Last coordinator log
        break;
      }
    }
    
    // Find first coordinator log after workers
    for (let i = logs.length - 1; i >= 0; i--) {
      if (logs[i].thread_id !== coordinatorThreadId) {
        joinIdx = i + 1; // First coordinator log after
        break;
      }
    }
    
    if (forkIdx < 0 || joinIdx < 0 || joinIdx >= logs.length) {
      return null;
    }
    
    const forkTime = timestamps[forkIdx];
    const joinTime = timestamps[joinIdx];
    const parallelDuration = joinTime - forkTime;
    
    // All worker threads
    const workerThreads = new Set<number>();
    for (let i = forkIdx + 1; i < joinIdx; i++) {
      if (logs[i].thread_id !== coordinatorThreadId) {
        workerThreads.add(logs[i].thread_id);
      }
    }
    
    return {
      forkIdx,
      joinIdx,
      forkTime,
      joinTime,
      forkLog: logs[forkIdx],
      joinLog: logs[joinIdx],
      parallelDuration,
      workerCount: workerThreads.size,
      workerThreads: Array.from(workerThreads),
    };
  }, [logs, timestamps, coordinatorThreadId]);

  // Find significant gaps (bottlenecks)
  const gaps = useMemo(() => {
    const result: Array<{ fromIdx: number; toIdx: number; durationMs: number; fromLog: ProcessedLog; toLog: ProcessedLog }> = [];
    for (let i = 1; i < logs.length; i++) {
      const gap = timestamps[i] - timestamps[i - 1];
      if (gap > 100) { // > 100ms
        result.push({
          fromIdx: i - 1,
          toIdx: i,
          durationMs: gap,
          fromLog: logs[i - 1],
          toLog: logs[i],
        });
      }
    }
    return result.sort((a, b) => b.durationMs - a.durationMs);
  }, [logs, timestamps]);

  // Thread summary
  const threadSummary = useMemo(() => {
    const threads = new Map<number, { name: string; firstSeen: number; lastSeen: number; count: number; sources: Set<string> }>();
    logs.forEach((log, i) => {
      const t = timestamps[i];
      if (!threads.has(log.thread_id)) {
        threads.set(log.thread_id, { name: log.thread_name, firstSeen: t, lastSeen: t, count: 0, sources: new Set() });
      }
      const entry = threads.get(log.thread_id)!;
      entry.lastSeen = t;
      entry.count++;
      entry.sources.add(log.source);
    });
    return Array.from(threads.entries())
      .map(([id, data]) => ({ id, ...data, duration: data.lastSeen - data.firstSeen }))
      .sort((a, b) => a.firstSeen - b.firstSeen);
  }, [logs, timestamps]);

  // Use CSS variable RGB values for theme-consistent colors
  const analysisColors = {
    info: 'var(--color-info)',
    success: 'var(--color-success)',
    infoBg: 'rgba(var(--color-info-rgb), 0.08)',
    infoBorder: 'rgba(var(--color-info-rgb), 0.25)',
    activeBtn: 'rgba(var(--color-info-rgb), 0.15)',
  };

  if (viewMode === 'analysis') {
    return (
      <div style={{ padding: 16, fontSize: 12, fontFamily: 'monospace', overflow: 'auto' }}>
        {/* View toggle */}
        <div style={{ marginBottom: 16, display: 'flex', gap: 8 }}>
          <button onClick={() => setViewMode('analysis')} style={{ padding: '4px 12px', background: analysisColors.activeBtn, color: analysisColors.info, border: 'none', borderRadius: 4, cursor: 'pointer' }}>Analysis</button>
          <button onClick={() => setViewMode('graph')} style={{ padding: '4px 12px', background: 'var(--bg-hover)', color: 'var(--text-muted)', border: 'none', borderRadius: 4, cursor: 'pointer' }}>Graph</button>
        </div>

        {/* Fork/Join Summary */}
        {forkJoinAnalysis && (
          <div style={{ marginBottom: 20, padding: 12, background: analysisColors.infoBg, borderRadius: 6, border: `1px solid ${analysisColors.infoBorder}` }}>
            <div style={{ color: analysisColors.info, fontWeight: 600, marginBottom: 8 }}>Fork/Join Model</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '4px 16px', color: 'var(--text-secondary)' }}>
              <span style={{ color: 'var(--text-muted)' }}>Fork point:</span>
              <span>[{forkJoinAnalysis.forkIdx + 1}] {forkJoinAnalysis.forkLog.source}</span>
              
              <span style={{ color: 'var(--text-muted)' }}>Join point:</span>
              <span>[{forkJoinAnalysis.joinIdx + 1}] {forkJoinAnalysis.joinLog.source}</span>
              
              <span style={{ color: 'var(--text-muted)' }}>Parallel duration:</span>
              <span style={{ color: analysisColors.success, fontWeight: 600 }}>{fmtDur(forkJoinAnalysis.parallelDuration)}</span>
              
              <span style={{ color: 'var(--text-muted)' }}>Worker threads:</span>
              <span>{forkJoinAnalysis.workerCount} threads</span>
            </div>
            
            <div style={{ marginTop: 12, padding: 8, background: 'var(--bg-hover)', borderRadius: 4, fontSize: 10 }}>
              <div style={{ color: 'var(--text-muted)', marginBottom: 4 }}>DAG Structure:</div>
              <div style={{ color: analysisColors.info }}>
                ● Coordinator [{1}-{forkJoinAnalysis.forkIdx + 1}] → 
                <span style={{ color: analysisColors.success }}> {forkJoinAnalysis.workerCount} workers [{forkJoinAnalysis.forkIdx + 2}-{forkJoinAnalysis.joinIdx}] </span>
                → ● Coordinator [{forkJoinAnalysis.joinIdx + 1}-{logs.length}]
              </div>
            </div>
          </div>
        )}

        {/* Summary */}
        <div style={{ marginBottom: 20, padding: 12, background: 'var(--bg-card)', borderRadius: 6, border: '1px solid var(--border-secondary)' }}>
          <div style={{ color: 'var(--text-secondary)', marginBottom: 8 }}>Summary</div>
          <div style={{ color: 'var(--text-muted)' }}>
            {logs.length} logs · {threadSummary.length} threads · {fmtDur(totalDuration)} total
          </div>
          <div style={{ color: analysisColors.info, marginTop: 4 }}>
            Coordinator: thread {coordinatorThreadId} (TCPHandler)
          </div>
        </div>

        {/* Phases */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ color: 'var(--text-secondary)', marginBottom: 8 }}>Execution Phases</div>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: 'var(--text-muted)', textAlign: 'left', background: 'var(--bg-card)' }}>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Phase</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Rows</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Duration</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Threads</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Description</th>
              </tr>
            </thead>
            <tbody>
              {analysis.map((phase, i) => (
                <tr key={i} style={{ borderTop: '1px solid var(--border-secondary)', background: i % 2 === 0 ? 'transparent' : 'var(--bg-card)' }}>
                  <td style={{ padding: '8px 8px', color: phase.type === 'coordinator' ? analysisColors.info : phase.type === 'parallel' ? analysisColors.success : 'var(--color-warning)' }}>
                    {phase.type}
                  </td>
                  <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                    {phase.startIdx + 1}-{phase.endIdx + 1}
                  </td>
                  <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                    {fmtDur(phase.endTime - phase.startTime)}
                  </td>
                  <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                    {phase.threads.size}
                  </td>
                  <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                    {phase.description}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Bottlenecks */}
        <div style={{ marginBottom: 20 }}>
          <div style={{ color: 'var(--text-secondary)', marginBottom: 8 }}>Bottlenecks (gaps &gt; 100ms)</div>
          {gaps.length === 0 ? (
            <div style={{ color: 'var(--text-muted)' }}>No significant gaps found</div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ color: 'var(--text-muted)', textAlign: 'left', background: 'var(--bg-card)' }}>
                  <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Gap</th>
                  <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>From</th>
                  <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>To</th>
                </tr>
              </thead>
              <tbody>
                {gaps.slice(0, 10).map((gap, i) => (
                  <tr key={i} style={{ borderTop: '1px solid var(--border-secondary)', background: i % 2 === 0 ? 'transparent' : 'var(--bg-card)' }}>
                    <td style={{ padding: '8px 8px', color: gap.durationMs > 1000 ? 'var(--color-error)' : 'var(--color-warning)', fontWeight: 600 }}>
                      {fmtDur(gap.durationMs)}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                      [{gap.fromIdx + 1}] {gap.fromLog.source}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                      [{gap.toIdx + 1}] {gap.toLog.source}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Thread timeline */}
        <div>
          <div style={{ color: 'var(--text-secondary)', marginBottom: 8 }}>Thread Timeline</div>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: 'var(--text-muted)', textAlign: 'left', background: 'var(--bg-card)' }}>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Thread</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Name</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Logs</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }} title="Estimated start: fork point for workers, actual for coordinator">Start</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }} title="Estimated end: join point for workers, actual for coordinator">End</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }} title="Estimated work duration (from fork to join for workers)">Work</th>
                <th style={{ padding: '8px 8px', borderBottom: '1px solid var(--border-secondary)' }}>Sources</th>
              </tr>
            </thead>
            <tbody>
              {threadSummary.map((t, idx) => {
                const isCoord = t.id === coordinatorThreadId;
                const firstOffset = t.firstSeen - minTime;
                const lastOffset = t.lastSeen - minTime;
                
                // For workers: estimate start=fork, end=join
                // For coordinator: use actual log times
                let estStart = firstOffset;
                let estEnd = lastOffset;
                let estWork = t.duration;
                
                if (!isCoord && forkJoinAnalysis) {
                  // Worker started at fork point
                  estStart = forkJoinAnalysis.forkTime - minTime;
                  // Worker ended at join point
                  estEnd = forkJoinAnalysis.joinTime - minTime;
                  estWork = forkJoinAnalysis.parallelDuration;
                }
                
                const isLateLogger = !isCoord && firstOffset > (forkJoinAnalysis?.forkTime ?? minTime) - minTime + 1000;
                
                return (
                  <tr key={t.id} style={{ borderTop: '1px solid var(--border-secondary)', background: idx % 2 === 0 ? 'transparent' : 'var(--bg-card)' }}>
                    <td style={{ padding: '8px 8px', color: isCoord ? analysisColors.info : 'var(--text-secondary)' }}>
                      {isCoord && '● '}{t.id}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                      {t.name}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)' }}>
                      {t.count}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)', fontFamily: 'monospace', fontSize: 10 }}>
                      +{fmtDur(estStart)}
                      {isLateLogger && <span style={{ color: 'var(--color-warning)', marginLeft: 4 }} title={`First log at +${fmtDur(firstOffset)}`}>*</span>}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)', fontFamily: 'monospace', fontSize: 10 }}>
                      +{fmtDur(estEnd)}
                    </td>
                    <td style={{ padding: '8px 8px', color: analysisColors.success, fontFamily: 'monospace', fontSize: 10, fontWeight: 500 }}>
                      {fmtDur(estWork)}
                    </td>
                    <td style={{ padding: '8px 8px', color: 'var(--text-secondary)', fontSize: 10 }}>
                      {Array.from(t.sources).slice(0, 3).join(', ')}{t.sources.size > 3 ? '...' : ''}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <div style={{ marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
            Worker threads assumed to start at fork point and end at join point.
            <span style={{ color: 'var(--color-warning)', marginLeft: 8 }}>*</span> = thread logged late (first log after fork)
          </div>
        </div>
      </div>
    );
  }

  // Graph view (existing code)
  // Get unique worker threads (non-coordinator), ordered by first appearance
  const seenThreads = new Set<number>();
  const workerThreadOrder: number[] = [];
  logs.forEach(log => {
    if (log.thread_id !== coordinatorThreadId && !seenThreads.has(log.thread_id)) {
      seenThreads.add(log.thread_id);
      workerThreadOrder.push(log.thread_id);
    }
  });

  // Assign column index: 0 = coordinator, 1+ = workers
  const threadToColumn = new Map<number, number>();
  threadToColumn.set(coordinatorThreadId, 0);
  workerThreadOrder.forEach((tid, i) => threadToColumn.set(tid, i + 1));

  const totalColumns = workerThreadOrder.length + 1;
  const nodeWidth = 150;
  const nodeHeight = 28;
  const colGap = 30;
  const timeAxisWidth = 60;
  const minNodeGap = 8; // Minimum vertical gap between nodes in same column
  const headerHeight = 40;

  const getThreadColor = (threadId: number) => {
    if (threadId === coordinatorThreadId) return 'var(--color-info)';
    // Cycle through theme accent colors for workers
    const idx = workerThreadOrder.indexOf(threadId);
    const workerPalette = [
      'var(--accent-green)',
      'var(--accent-purple)',
      'var(--accent-orange)',
      'var(--accent-yellow)',
      'var(--accent-red)',
      'var(--color-mutation)',
    ];
    return workerPalette[idx % workerPalette.length];
  };

  // Resolved hex colors for SVG fill operations (CSS vars don't work in hex+opacity patterns)
  // These map to the same palette but as concrete values for node backgrounds
  const getThreadFillBg = (threadId: number) => {
    if (threadId === coordinatorThreadId) return 'rgba(var(--color-info-rgb), 0.07)';
    const idx = workerThreadOrder.indexOf(threadId);
    const bgPalette = [
      'rgba(var(--color-success-rgb), 0.07)',
      'rgba(var(--accent-primary-rgb), 0.07)',
      'rgba(var(--color-merge-rgb), 0.07)',
      'rgba(var(--color-warning-rgb), 0.07)',
      'rgba(var(--color-error-rgb), 0.07)',
      'rgba(var(--color-mutation-rgb), 0.07)',
    ];
    return bgPalette[idx % bgPalette.length];
  };

  // Build nodes with collision-free Y positions.
  // Strategy: compute time-proportional Y first, then push nodes down
  // per-column so they never overlap. Gaps > 100ms are preserved
  // proportionally so bottlenecks remain visible.
  const rawNodes = logs.map((log, i) => {
    const col = threadToColumn.get(log.thread_id) ?? 0;
    const t = timestamps[i];
    return { log, index: i, col, time: t };
  });

  // Two-pass layout:
  // 1. Assign ideal Y from time, with a guaranteed minimum spacing
  // 2. Per-column push-down to resolve overlaps
  const idealSpacing = nodeHeight + minNodeGap; // 36px minimum per node slot
  const baseHeight = Math.max(600, Math.min(3000, logs.length * idealSpacing + 100));

  // Time → Y with proportional mapping
  const timeToY = (t: number) => {
    const ratio = (t - minTime) / totalDuration;
    return ratio * (baseHeight - 60) + headerHeight;
  };

  // First pass: ideal positions
  const nodeYs: number[] = rawNodes.map(n => timeToY(n.time));

  // Second pass: per-column overlap resolution (push down)
  const lastYByCol = new Map<number, number>();
  // Process in global order (already sorted by time from ClickHouse)
  for (let i = 0; i < rawNodes.length; i++) {
    const col = rawNodes[i].col;
    const prevY = lastYByCol.get(col);
    if (prevY !== undefined) {
      const minY = prevY + idealSpacing;
      if (nodeYs[i] < minY) {
        nodeYs[i] = minY;
      }
    }
    lastYByCol.set(col, nodeYs[i]);
  }

  const nodes = rawNodes.map((n, i) => ({
    ...n,
    x: timeAxisWidth + n.col * (nodeWidth + colGap),
    y: nodeYs[i],
  }));

  // Build edges: connect to previous log in same thread
  const edges: Array<{ from: number; to: number; type: 'seq' | 'fork' | 'join'; durationMs: number }> = [];
  const lastLogByThread = new Map<number, number>();
  
  nodes.forEach((node, i) => {
    const tid = node.log.thread_id;
    const prevInThread = lastLogByThread.get(tid);
    
    if (prevInThread !== undefined) {
      // Connect to previous log in same thread
      const dur = node.time - nodes[prevInThread].time;
      edges.push({ from: prevInThread, to: i, type: 'seq', durationMs: dur });
    } else if (tid !== coordinatorThreadId) {
      // First log in worker thread - find last coordinator log before this
      let lastCoord = -1;
      for (let j = i - 1; j >= 0; j--) {
        if (nodes[j].log.thread_id === coordinatorThreadId) {
          lastCoord = j;
          break;
        }
      }
      if (lastCoord >= 0) {
        const dur = node.time - nodes[lastCoord].time;
        edges.push({ from: lastCoord, to: i, type: 'fork', durationMs: dur });
      }
    }
    
    lastLogByThread.set(tid, i);
  });

  // Find joins: last log of each worker thread → next coordinator log
  workerThreadOrder.forEach(tid => {
    const lastWorkerIdx = lastLogByThread.get(tid);
    if (lastWorkerIdx === undefined) return;
    
    // Find next coordinator log after this worker's last log
    for (let j = lastWorkerIdx + 1; j < nodes.length; j++) {
      if (nodes[j].log.thread_id === coordinatorThreadId) {
        const dur = nodes[j].time - nodes[lastWorkerIdx].time;
        edges.push({ from: lastWorkerIdx, to: j, type: 'join', durationMs: dur });
        break;
      }
    }
  });

  // Compute final SVG dimensions from actual node positions
  const maxNodeY = nodes.length > 0 ? Math.max(...nodes.map(n => n.y)) : baseHeight;
  const svgHeight = maxNodeY + nodeHeight + 40;
  const svgWidth = timeAxisWidth + totalColumns * (nodeWidth + colGap) + 60; // extra space for FORK/JOIN badges

  // Time axis ticks (use actual node extent for accurate labels)
  const tickCount = 5;
  const ticks = Array.from({ length: tickCount + 1 }, (_, i) => {
    const t = minTime + (totalDuration * i) / tickCount;
    // Map tick to the same Y scale as nodes (use timeToY, not pushed positions)
    return { time: t, y: timeToY(t), label: fmtDur((t - minTime)) };
  });

  // Theme-aware colors for SVG — using CSS variables where possible
  const axisColor = 'var(--border-secondary)';
  const tickColor = 'var(--border-primary)';
  const tickTextColor = 'var(--text-muted)';
  const seqEdgeColor = 'var(--border-primary)';
  const nodeTextMuted = 'var(--text-muted)';
  const forkEdgeColor = 'rgba(var(--color-info-rgb), 0.4)';
  const joinEdgeColor = 'rgba(var(--color-success-rgb), 0.4)';
  const bottleneckColor = 'var(--color-error)';
  const slowColor = 'var(--color-warning)';

  return (
    <div style={{ padding: 16 }}>
      {/* View toggle */}
      <div style={{ marginBottom: 16, display: 'flex', gap: 8 }}>
        <button onClick={() => setViewMode('analysis')} style={{ padding: '4px 12px', background: 'var(--bg-hover)', color: 'var(--text-muted)', border: 'none', borderRadius: 4, cursor: 'pointer' }}>Analysis</button>
        <button onClick={() => setViewMode('graph')} style={{ padding: '4px 12px', background: 'rgba(var(--color-info-rgb), 0.15)', color: 'var(--color-info)', border: 'none', borderRadius: 4, cursor: 'pointer' }}>Graph</button>
      </div>

      <ZoomPanContainer height={Math.min(svgHeight + 40, 800)} minScale={0.1} maxScale={4} initialScale={1}>
      <svg width={svgWidth} height={svgHeight} style={{ display: 'block' }}>
        {/* Arrow markers for fork/join edges */}
        <defs>
          <marker id="arrow-fork" markerWidth="6" markerHeight="5" refX="5" refY="2.5" orient="auto">
            <polygon points="0,0 6,2.5 0,5" fill={forkEdgeColor} />
          </marker>
          <marker id="arrow-join" markerWidth="6" markerHeight="5" refX="5" refY="2.5" orient="auto">
            <polygon points="0,0 6,2.5 0,5" fill={joinEdgeColor} />
          </marker>
        </defs>

        {/* Column separator lines — subtle vertical guides */}
        {Array.from({ length: totalColumns }, (_, c) => {
          const cx = timeAxisWidth + c * (nodeWidth + colGap) + nodeWidth / 2;
          return (
            <line key={`col-${c}`} x1={cx} y1={headerHeight} x2={cx} y2={svgHeight - 20}
              stroke={axisColor} strokeWidth={1} strokeDasharray="2,6" opacity={0.4} />
          );
        })}

        {/* Time axis */}
        <line x1={timeAxisWidth - 10} y1={headerHeight} x2={timeAxisWidth - 10} y2={svgHeight - 20} stroke={axisColor} />
        {ticks.map((tick, i) => (
          <g key={i}>
            <line x1={timeAxisWidth - 15} y1={tick.y} x2={timeAxisWidth - 5} y2={tick.y} stroke={tickColor} />
            <text x={timeAxisWidth - 18} y={tick.y + 3} fontSize={9} fill={tickTextColor} textAnchor="end" fontFamily="monospace">
              {tick.label}
            </text>
          </g>
        ))}

        {/* Column headers */}
        <text x={timeAxisWidth + nodeWidth / 2} y={20} fontSize={10} fill="var(--color-info)" textAnchor="middle" fontWeight={500}>
          ● Coordinator
        </text>
        {workerThreadOrder.map((tid, i) => (
          <text 
            key={tid} 
            x={timeAxisWidth + (i + 1) * (nodeWidth + colGap) + nodeWidth / 2} 
            y={20} 
            fontSize={10} 
            fill={getThreadColor(tid)} 
            textAnchor="middle"
          >
            Worker {i + 1}
          </text>
        ))}

        {/* Edges — drawn first so nodes render on top */}
        {edges.map((edge, i) => {
          const from = nodes[edge.from];
          const to = nodes[edge.to];
          
          const isBottleneck = edge.durationMs > 1000;
          const isSlow = edge.durationMs > 100;
          const baseColor = edge.type === 'seq' ? seqEdgeColor 
            : edge.type === 'fork' ? forkEdgeColor 
            : joinEdgeColor;
          const color = isBottleneck ? bottleneckColor : isSlow ? slowColor : baseColor;
          const strokeWidth = edge.type === 'fork' || edge.type === 'join'
            ? (isBottleneck ? 2.5 : isSlow ? 2 : 1.5)
            : (isBottleneck ? 2 : isSlow ? 1.5 : 1);
          const dashArray = edge.type === 'fork' || edge.type === 'join' ? '6,4' : undefined;
          const markerEnd = edge.type === 'fork' ? 'url(#arrow-fork)'
            : edge.type === 'join' ? 'url(#arrow-join)' : undefined;
          
          // Cross-column: exit from side of node, gentle S-curve
          if (from.col !== to.col) {
            const goingRight = to.col > from.col;
            const x1 = goingRight ? from.x + nodeWidth : from.x;
            const y1 = from.y + nodeHeight / 2;
            const x2 = goingRight ? to.x : to.x + nodeWidth;
            const y2 = to.y + nodeHeight / 2;
            const dx = Math.abs(x2 - x1);
            const cpOff = Math.min(dx * 0.35, 50);
            const cx1 = goingRight ? x1 + cpOff : x1 - cpOff;
            const cx2 = goingRight ? x2 - cpOff : x2 + cpOff;
            return (
              <g key={i}>
                <path
                  d={`M ${x1} ${y1} C ${cx1} ${y1}, ${cx2} ${y2}, ${x2} ${y2}`}
                  fill="none" stroke={color} strokeWidth={strokeWidth}
                  strokeDasharray={dashArray} opacity={0.75}
                  markerEnd={markerEnd}
                />
                {isSlow && (
                  <text x={(x1 + x2) / 2} y={(y1 + y2) / 2 - 5}
                    fontSize={8} fill={color} fontFamily="monospace" textAnchor="middle" opacity={0.85}>
                    {fmtDur(edge.durationMs)}
                  </text>
                )}
              </g>
            );
          }
          
          // Same-column: thin vertical connector along left side
          const x1 = from.x + 10;
          const y1 = from.y + nodeHeight;
          const x2 = to.x + 10;
          const y2 = to.y;
          return (
            <g key={i}>
              <line x1={x1} y1={y1} x2={x2} y2={y2} stroke={color} strokeWidth={strokeWidth} opacity={0.5} />
              {isSlow && (
                <text x={x1 + 14} y={(y1 + y2) / 2 + 3} fontSize={8} fill={color} fontFamily="monospace" opacity={0.85}>
                  {fmtDur(edge.durationMs)}
                </text>
              )}
            </g>
          );
        })}

        {/* Nodes */}
        {(() => {
          // Pre-compute which nodes are fork sources / join targets
          const forkSourceNodes = new Set<number>();
          const joinTargetNodes = new Set<number>();
          edges.forEach(e => {
            if (e.type === 'fork') forkSourceNodes.add(e.from);
            if (e.type === 'join') joinTargetNodes.add(e.to);
          });

          return nodes.map((node) => {
            const color = getThreadColor(node.log.thread_id);
            const fillBg = getThreadFillBg(node.log.thread_id);
            const isCoord = node.log.thread_id === coordinatorThreadId;
            const isForkSource = forkSourceNodes.has(node.index);
            const isJoinTarget = joinTargetNodes.has(node.index);
            
            return (
              <g key={node.index} transform={`translate(${node.x}, ${node.y})`}>
                <rect
                  width={nodeWidth}
                  height={nodeHeight}
                  rx={5}
                  fill={fillBg}
                  stroke={color}
                  strokeWidth={isCoord ? 1.5 : 1}
                  strokeOpacity={0.4}
                />
                {/* Source label — clipped to node width */}
                <clipPath id={`clip-${node.index}`}>
                  <rect x={6} y={0} width={nodeWidth - 12} height={nodeHeight} />
                </clipPath>
                <g clipPath={`url(#clip-${node.index})`}>
                  <text x={8} y={12} fontSize={10} fill={color} fontWeight={500}>
                    {node.log.source.slice(0, 20)}
                  </text>
                  <text x={8} y={23} fontSize={7.5} fill={nodeTextMuted}>
                    {node.log.message.slice(0, 28)}{node.log.message.length > 28 ? '…' : ''}
                  </text>
                </g>
                {/* Fork/Join badge — right side of coordinator node */}
                {isForkSource && (
                  <g transform={`translate(${nodeWidth + 4}, 2)`}>
                    <rect width={36} height={14} rx={3}
                      fill="rgba(var(--color-info-rgb), 0.12)"
                      stroke={forkEdgeColor} strokeWidth={0.5} />
                    <text x={18} y={10} fontSize={8} fontWeight={600}
                      fill="var(--color-info)" textAnchor="middle" fontFamily="monospace">
                      FORK
                    </text>
                  </g>
                )}
                {isJoinTarget && (
                  <g transform={`translate(${nodeWidth + 4}, ${nodeHeight - 16})`}>
                    <rect width={34} height={14} rx={3}
                      fill="rgba(var(--color-success-rgb), 0.12)"
                      stroke={joinEdgeColor} strokeWidth={0.5} />
                    <text x={17} y={10} fontSize={8} fontWeight={600}
                      fill="var(--color-success)" textAnchor="middle" fontFamily="monospace">
                      JOIN
                    </text>
                  </g>
                )}
                {/* Tooltip */}
                <title>{`${node.log.source}\n${node.log.message}\nThread: ${node.log.thread_name} (${node.log.thread_id})\nTime: ${formatTime(node.log.event_time_microseconds)}${isForkSource ? '\n⑂ Fork point — workers spawned here' : ''}${isJoinTarget ? '\n⑃ Join point — workers merged here' : ''}`}</title>
              </g>
            );
          });
        })()}
      </svg>

      {/* Legend */}
      <div style={{ marginTop: 12, paddingTop: 10, borderTop: '1px solid var(--border-secondary)', fontSize: 10, color: 'var(--text-muted)', display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <span>{logs.length} nodes</span>
        <span>{totalColumns} threads</span>
        <span>Total: {fmtDur(totalDuration)}</span>
        <span style={{ color: bottleneckColor }}>━ bottleneck (&gt;1s)</span>
        <span style={{ color: slowColor }}>━ slow (&gt;100ms)</span>
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
          <span style={{ color: 'var(--color-info)' }}>⇢ fork</span>
          <span style={{ color: 'var(--text-disabled)' }}>/</span>
          <span style={{ color: 'var(--color-success)' }}>⇢ join</span>
        </span>
      </div>
      </ZoomPanContainer>
    </div>
  );
};

// Clean pill badge for log levels - clickable to toggle filter
const LevelBadge: React.FC<{ 
  level: string; 
  count?: number; 
  isActive?: boolean;
  onClick?: () => void;
}> = ({ level, count, isActive = true, onClick }) => (
  <span
    onClick={onClick}
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      gap: 6,
      padding: '3px 10px',
      borderRadius: 12,
      fontSize: 11,
      fontWeight: 500,
      background: isActive ? `${getLevelColor(level)}18` : 'transparent',
      color: isActive ? getLevelColor(level) : 'var(--text-muted)',
      border: `1px solid ${getLevelColor(level)}${isActive ? '30' : '15'}`,
      cursor: onClick ? 'pointer' : 'default',
      transition: 'all 0.15s ease',
      opacity: isActive ? 1 : 0.6,
    }}
  >
    {level}
    {count !== undefined && (
      <span style={{ 
        background: isActive ? `${getLevelColor(level)}25` : 'var(--bg-card)',
        padding: '1px 6px',
        borderRadius: 8,
        fontSize: 10,
      }}>
        {count}
      </span>
    )}
  </span>
);

// No duration bar - trace logs don't have duration info
// Duration is in the message text or use OpenTelemetry spans for timing

const LogRow: React.FC<{
  log: ProcessedLog;
  isEven: boolean;
}> = ({ log, isEven }) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const theme = useThemeDetection();
  const isLight = theme === 'light';
  
  const sourceColor = stringToHslColor(log.source, isLight);
  const threadColor = getThreadIdColor(log.thread_id, isLight);
  const levelColor = getLevelColor(log.level);

  return (
    <div
      style={{
        background: isEven ? 'transparent' : 'var(--bg-card)',
        borderBottom: '1px solid var(--border-secondary)',
        transition: 'background 0.15s ease',
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-card-hover)'; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = isEven ? 'transparent' : 'var(--bg-card)'; }}
    >
      {/* Main row */}
      <div
        onClick={() => setIsExpanded(!isExpanded)}
        style={{
          display: 'grid',
          gridTemplateColumns: '90px 80px minmax(100px, 140px) minmax(120px, 160px) 70px 1fr',
          gap: 16,
          padding: '10px 16px',
          alignItems: 'center',
          cursor: 'pointer',
          fontSize: 12,
        }}
      >
        {/* Time */}
        <span style={{ fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 11 }}>
          {formatTime(log.event_time_microseconds || log.event_time)}
        </span>

        {/* Thread gap duration */}
        <span style={{ 
          fontFamily: 'monospace', 
          fontSize: 11,
          color: log.durationMs === 0 ? 'var(--text-disabled)' 
            : log.durationMs >= 1000 ? 'var(--color-error)' 
            : log.durationMs >= 100 ? 'var(--color-warning)' 
            : 'var(--text-tertiary)',
          fontWeight: log.durationMs >= 1000 ? 600 : 400,
        }}>
          {log.durationMs === 0 ? '—' 
            : log.durationMs >= 1000 ? `${(log.durationMs/1000).toFixed(2)}s` 
            : `${log.durationMs.toFixed(0)}ms`}
        </span>

        {/* Source */}
        <span style={{ 
          color: sourceColor, 
          fontWeight: 500,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }} title={log.source}>
          {log.source}
        </span>

        {/* Thread */}
        <span style={{ 
          color: threadColor,
          fontFamily: 'monospace',
          fontSize: 11,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }} title={`${log.thread_name} (${log.thread_id})${log.isCoordinator ? ' - Coordinator' : ''}`}>
          {log.isCoordinator && <span style={{ color: 'var(--color-info)', marginRight: 4 }}>●</span>}
          {log.thread_name}
          <span style={{ opacity: 0.6 }}>({log.thread_id})</span>
        </span>

        {/* Level */}
        <span style={{ color: levelColor, fontWeight: 500, fontSize: 11 }}>
          {log.level}
        </span>

        {/* Message */}
        <span style={{ 
          color: 'var(--text-secondary)',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }} title={log.message}>
          {log.message}
        </span>
      </div>

      {/* Expanded details */}
      {isExpanded && (
        <div style={{ 
          padding: '0 16px 16px 16px',
          marginLeft: 80,
        }}>
          {/* Message box */}
          {log.message && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ 
                fontSize: 10, 
                color: 'var(--text-muted)', 
                textTransform: 'uppercase',
                letterSpacing: '0.5px',
                marginBottom: 6,
              }}>
                Message
              </div>
              <pre style={{
                margin: 0,
                padding: 12,
                background: 'var(--bg-code)',
                border: '1px solid var(--border-secondary)',
                borderRadius: 6,
                fontSize: 12,
                fontFamily: 'monospace',
                color: 'var(--text-primary)',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                lineHeight: 1.5,
              }}>
                {log.message}
              </pre>
            </div>
          )}

          {/* Metadata grid */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
            gap: '8px 24px',
          }}>
            {Object.entries(log)
              .filter(([key, value]) => {
                if (['message', 'durationMs', 'isCoordinator'].includes(key)) return false;
                if (value === null || value === undefined || value === '') return false;
                if (typeof value === 'number' && value === 0 && key !== 'thread_id') return false;
                return true;
              })
              .map(([key, value]) => {
                let valueColor = 'var(--text-secondary)';
                if (key === 'source') valueColor = sourceColor;
                else if (key.includes('thread')) valueColor = threadColor;
                else if (key === 'level') valueColor = levelColor;
                else if (key === 'query_id') valueColor = 'var(--color-info)';

                const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

                return (
                  <div key={key} style={{ fontSize: 12 }}>
                    <span style={{ color: 'var(--text-muted)' }}>{label}: </span>
                    <span style={{ color: valueColor, fontFamily: 'monospace' }}>{String(value)}</span>
                  </div>
                );
              })}
          </div>
        </div>
      )}
    </div>
  );
};

export const TraceLogViewer: React.FC<TraceLogViewerProps> = ({
  logs,
  isLoading,
  error,
  filter,
  onFilterChange,
  onRefresh,
  queryId,
  queryStartTime,
  queryEndTime,
}) => {
  const [activeView, setActiveView] = useState<'logs' | 'timeline'>('logs');
  const [showFilter, setShowFilter] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');

  // ClickStack integration
  const clickStackEnabled = useClickStackStore(s => s.enabled);
  const clickStackSourceId = useClickStackStore(s => s.sourceId);
  const setClickStackEnabled = useClickStackStore(s => s.setEnabled);
  const setClickStackSourceId = useClickStackStore(s => s.setSourceId);
  const [showClickStackConfig, setShowClickStackConfig] = useState(false);
  const hasClickStack = useMonitoringCapabilitiesStore(s => s.flags.hasClickStack);

  // Reset manual override when connecting to a server without ClickStack
  useEffect(() => {
    if (!hasClickStack && clickStackEnabled) {
      setClickStackEnabled(false);
    }
  }, [hasClickStack, clickStackEnabled, setClickStackEnabled]);

  // Show ClickStack link only when version detected OR manually enabled
  const clickStackAvailable = hasClickStack || clickStackEnabled;

  const activeProfile = useConnectionStore(s => {
    const id = s.activeProfileId;
    return id ? s.profiles.find(p => p.id === id) : null;
  });

  const clickStackUrl = useMemo(() => {
    if (!clickStackAvailable || !queryId) return null;
    if (!activeProfile) return null;
    const { host, port, secure } = activeProfile.config;
    const protocol = secure ? 'https' : 'http';
    const base = `${protocol}://${host}:${port}`;
    const start = queryStartTime || new Date(Date.now() - 300_000).toISOString();
    const end = queryEndTime || new Date().toISOString();
    if (clickStackSourceId) {
      return buildClickStackUrl(base, queryId, clickStackSourceId, start, end);
    }
    // Fallback: open without source, ClickStack will use whatever is selected
    return buildClickStackUrlNoSource(base, queryId, start, end);
  }, [clickStackAvailable, clickStackSourceId, queryId, queryStartTime, queryEndTime, activeProfile]);

  const filteredLogs = useMemo(() => {
    let result = filterTraceLogs(logs, filter);
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      result = result.filter(log =>
        log.message.toLowerCase().includes(term) ||
        log.source.toLowerCase().includes(term) ||
        log.thread_name?.toLowerCase().includes(term) ||
        String(log.thread_id).includes(term)
      );
    }
    return result;
  }, [logs, filter, searchTerm]);

  const processedLogs = useMemo(() => processLogsWithDuration(filteredLogs), [filteredLogs]);

  const levelCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    logs.forEach(log => { counts[log.level] = (counts[log.level] || 0) + 1; });
    return counts;
  }, [logs]);

  const threadCount = useMemo(() => new Set(logs.map(l => l.thread_id)).size, [logs]);

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--bg-code)' }}>
      {/* Header */}
      <div style={{ 
        padding: '16px 20px',
        borderBottom: '1px solid var(--border-secondary)',
        background: 'var(--bg-card)',
      }}>
        {/* Title row with view toggle */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <h3 style={{ 
              margin: 0, 
              fontSize: 14, 
              fontWeight: 600, 
              color: 'var(--text-primary)',
              letterSpacing: '0.3px',
            }}>
              Trace Logs
            </h3>
            {/* View toggle */}
            <div style={{ display: 'flex', background: 'var(--bg-card)', borderRadius: 6, padding: 2 }}>
              {(['logs', 'timeline'] as const).map((view) => (
                <button
                  key={view}
                  onClick={() => setActiveView(view)}
                  style={{
                    padding: '4px 12px',
                    fontSize: 11,
                    fontWeight: 500,
                    borderRadius: 4,
                    border: 'none',
                    background: activeView === view ? 'rgba(var(--color-info-rgb), 0.2)' : 'transparent',
                    color: activeView === view ? 'var(--color-info)' : 'var(--text-tertiary)',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                    textTransform: 'capitalize',
                  }}
                >
                  {view}
                </button>
              ))}
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
              {filteredLogs.length} / {logs.length} logs · {threadCount} threads
            </span>
            <button
              onClick={() => setShowFilter(!showFilter)}
              style={{
                padding: '5px 12px',
                fontSize: 11,
                fontWeight: 500,
                borderRadius: 6,
                border: 'none',
                background: showFilter ? 'rgba(var(--color-info-rgb), 0.15)' : 'var(--bg-card)',
                color: showFilter ? 'var(--color-info)' : 'var(--text-tertiary)',
                cursor: 'pointer',
                transition: 'all 0.15s ease',
              }}
            >
              Filter
            </button>
            {onRefresh && (
              <button
                onClick={onRefresh}
                disabled={isLoading}
                style={{
                  padding: '5px 12px',
                  fontSize: 11,
                  fontWeight: 500,
                  borderRadius: 6,
                  border: 'none',
                  background: 'var(--bg-card)',
                  color: 'var(--text-tertiary)',
                  cursor: 'pointer',
                  opacity: isLoading ? 0.5 : 1,
                }}
              >
                Refresh
              </button>
            )}
            {/* ClickStack deep link */}
            {queryId && clickStackAvailable && (
              <div style={{ position: 'relative' }}>
                <a
                  href={clickStackUrl || '#'}
                  target="_blank"
                  rel="noopener noreferrer"
                  title={clickStackSourceId ? 'Open in ClickStack' : 'Open in ClickStack (no source configured — will use last selected)'}
                  onClick={(e) => {
                    if (!clickStackUrl) e.preventDefault();
                  }}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 4,
                    padding: '5px 12px',
                    fontSize: 11,
                    fontWeight: 500,
                    borderRadius: 6,
                    border: 'none',
                    background: 'rgba(var(--accent-primary-rgb), 0.12)',
                    color: 'var(--accent-primary)',
                    cursor: 'pointer',
                    textDecoration: 'none',
                    transition: 'all 0.15s ease',
                  }}
                >
                  ↗ ClickStack
                </a>
                <button
                  onClick={() => setShowClickStackConfig(!showClickStackConfig)}
                  title="Configure ClickStack source"
                  style={{
                    padding: '5px 6px',
                    fontSize: 10,
                    borderRadius: 4,
                    border: 'none',
                    background: 'transparent',
                    color: 'var(--text-muted)',
                    cursor: 'pointer',
                    marginLeft: 2,
                  }}
                >
                  ⚙
                </button>
                {showClickStackConfig && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    right: 0,
                    marginTop: 8,
                    padding: 16,
                    background: 'var(--bg-card)',
                    border: '1px solid var(--border-secondary)',
                    borderRadius: 8,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    zIndex: 100,
                    width: 320,
                    fontSize: 12,
                  }}>
                    <div style={{ marginBottom: 12, color: 'var(--text-secondary)', fontWeight: 600 }}>
                      ClickStack Source
                    </div>
                    <div style={{ marginBottom: 8, color: 'var(--text-muted)', fontSize: 11, lineHeight: 1.5 }}>
                      Create a data source in ClickStack pointing to <code style={{ background: 'var(--bg-code)', padding: '1px 4px', borderRadius: 3 }}>system.text_log</code> with
                      timestamp column <code style={{ background: 'var(--bg-code)', padding: '1px 4px', borderRadius: 3 }}>event_date</code>.
                      The source ID is optional — without it, ClickStack uses your last selected source.
                    </div>
                    <div>
                      <label style={{ display: 'block', marginBottom: 4, color: 'var(--text-muted)', fontSize: 11 }}>
                        Source ID <span style={{ color: 'var(--text-disabled)' }}>(optional, from <code style={{ background: 'var(--bg-code)', padding: '1px 3px', borderRadius: 2 }}>source=</code> in URL)</span>
                      </label>
                      <input
                        type="text"
                        value={clickStackSourceId}
                        onChange={(e) => setClickStackSourceId(e.target.value)}
                        placeholder="l1933286829"
                        style={{
                          width: '100%',
                          padding: '6px 10px',
                          fontSize: 12,
                          fontFamily: 'monospace',
                          borderRadius: 6,
                          border: '1px solid var(--border-primary)',
                          background: 'var(--bg-input)',
                          color: 'var(--text-primary)',
                          outline: 'none',
                          boxSizing: 'border-box',
                        }}
                      />
                      <div style={{ marginTop: 4, fontSize: 10, color: 'var(--text-disabled)', lineHeight: 1.4 }}>
                        Tip: in ClickStack DevTools console, run: <code style={{ background: 'var(--bg-code)', padding: '1px 3px', borderRadius: 2 }}>localStorage.getItem('hdx-last-selected-source-id')</code>
                      </div>
                    </div>
                    <button
                      onClick={() => setShowClickStackConfig(false)}
                      style={{
                        marginTop: 12,
                        padding: '4px 12px',
                        fontSize: 11,
                        borderRadius: 4,
                        border: 'none',
                        background: 'var(--bg-hover)',
                        color: 'var(--text-tertiary)',
                        cursor: 'pointer',
                      }}
                    >
                      Done
                    </button>
                  </div>
                )}
              </div>
            )}
            {queryId && !clickStackAvailable && (
              <div style={{ position: 'relative' }}>
                <button
                  onClick={() => setShowClickStackConfig(!showClickStackConfig)}
                  title="Enable ClickStack integration (requires CH 26.2+)"
                  style={{
                    padding: '5px 12px',
                    fontSize: 11,
                    fontWeight: 500,
                    borderRadius: 6,
                    border: '1px dashed var(--border-primary)',
                    background: showClickStackConfig ? 'rgba(var(--accent-primary-rgb), 0.08)' : 'transparent',
                    color: 'var(--text-muted)',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                  }}
                >
                  ↗ ClickStack
                </button>
                {showClickStackConfig && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    right: 0,
                    marginTop: 8,
                    padding: 16,
                    background: 'var(--bg-card)',
                    border: '1px solid var(--border-secondary)',
                    borderRadius: 8,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    zIndex: 100,
                    width: 320,
                    fontSize: 12,
                  }}>
                    <div style={{ marginBottom: 12, color: 'var(--text-secondary)', fontWeight: 600 }}>
                      ClickStack Integration
                    </div>
                    <div style={{ marginBottom: 8, color: 'var(--text-muted)', fontSize: 11, lineHeight: 1.5 }}>
                      Requires ClickHouse 26.2+ with ClickStack enabled.
                      Create a data source in ClickStack pointing to <code style={{ background: 'var(--bg-code)', padding: '1px 4px', borderRadius: 3 }}>system.text_log</code> with
                      timestamp column <code style={{ background: 'var(--bg-code)', padding: '1px 4px', borderRadius: 3 }}>event_date</code>.
                    </div>
                    <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12, cursor: 'pointer' }}>
                      <input
                        type="checkbox"
                        checked={clickStackEnabled}
                        onChange={(e) => setClickStackEnabled(e.target.checked)}
                        style={{ accentColor: 'var(--accent-primary)' }}
                      />
                      <span style={{ color: 'var(--text-secondary)' }}>Enable manually (version not detected as 26.2+)</span>
                    </label>
                    {clickStackEnabled && (
                      <div>
                        <label style={{ display: 'block', marginBottom: 4, color: 'var(--text-muted)', fontSize: 11 }}>
                          Source ID <span style={{ color: 'var(--text-disabled)' }}>(optional)</span>
                        </label>
                        <input
                          type="text"
                          value={clickStackSourceId}
                          onChange={(e) => setClickStackSourceId(e.target.value)}
                          placeholder="l1933286829"
                          style={{
                            width: '100%',
                            padding: '6px 10px',
                            fontSize: 12,
                            fontFamily: 'monospace',
                            borderRadius: 6,
                            border: '1px solid var(--border-primary)',
                            background: 'var(--bg-input)',
                            color: 'var(--text-primary)',
                            outline: 'none',
                            boxSizing: 'border-box',
                          }}
                        />
                      </div>
                    )}
                    <button
                      onClick={() => setShowClickStackConfig(false)}
                      style={{
                        marginTop: 12,
                        padding: '4px 12px',
                        fontSize: 11,
                        borderRadius: 4,
                        border: 'none',
                        background: 'var(--bg-hover)',
                        color: 'var(--text-tertiary)',
                        cursor: 'pointer',
                      }}
                    >
                      Done
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Search */}
        <input
          type="text"
          placeholder="Search message, source, thread name or ID..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          style={{
            width: '100%',
            padding: '8px 12px',
            fontSize: 12,
            borderRadius: 6,
            border: '1px solid var(--border-primary)',
            background: 'var(--bg-input)',
            color: 'var(--text-primary)',
            outline: 'none',
          }}
        />

        {/* Filter panel */}
        {showFilter && (
          <div style={{ marginTop: 12, padding: 12, background: 'var(--bg-card)', borderRadius: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
              <span style={{ fontSize: 11, color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                Log Levels
              </span>
              <div style={{ display: 'flex', gap: 8 }}>
                <button 
                  onClick={() => onFilterChange({ logLevels: [...VALID_LOG_LEVELS] })}
                  style={{ fontSize: 11, color: 'var(--color-info)', background: 'none', border: 'none', cursor: 'pointer' }}
                >
                  All
                </button>
                <button 
                  onClick={() => onFilterChange({ logLevels: [] })}
                  style={{ fontSize: 11, color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer' }}
                >
                  Clear
                </button>
              </div>
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {VALID_LOG_LEVELS.map(level => {
                const isSelected = !filter.logLevels?.length || filter.logLevels.includes(level);
                return (
                  <button
                    key={level}
                    onClick={() => {
                      const currentLevels = filter.logLevels || [];
                      if (currentLevels.length === 0) {
                        // No filter - clicking means "show only this level"
                        onFilterChange({ logLevels: [level] });
                      } else if (currentLevels.includes(level)) {
                        // Remove this level
                        const newLevels = currentLevels.filter(l => l !== level);
                        onFilterChange({ logLevels: newLevels });
                      } else {
                        // Add this level
                        onFilterChange({ logLevels: [...currentLevels, level] });
                      }
                    }}
                    style={{
                      padding: '4px 10px',
                      fontSize: 11,
                      borderRadius: 12,
                      border: `1px solid ${getLevelColor(level)}${isSelected ? '50' : '20'}`,
                      background: isSelected ? `${getLevelColor(level)}15` : 'transparent',
                      color: isSelected ? getLevelColor(level) : 'var(--text-muted)',
                      cursor: 'pointer',
                      transition: 'all 0.15s ease',
                    }}
                  >
                    {level}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Level summary - clickable to filter */}
        {Object.keys(levelCounts).length > 0 && (
          <div style={{ marginTop: 12, display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {Object.entries(levelCounts).map(([level, count]) => {
              const isActive = !filter.logLevels?.length || filter.logLevels.includes(level as LogLevel);
              return (
                <LevelBadge 
                  key={level} 
                  level={level} 
                  count={count}
                  isActive={isActive}
                  onClick={() => {
                    const currentLevels = filter.logLevels || [];
                    if (currentLevels.length === 0) {
                      // No filter active - clicking a level means "show only this level"
                      onFilterChange({ logLevels: [level as LogLevel] });
                    } else if (currentLevels.includes(level as LogLevel)) {
                      // Level is in filter - remove it
                      const newLevels = currentLevels.filter(l => l !== level);
                      onFilterChange({ logLevels: newLevels });
                    } else {
                      // Level not in filter - add it
                      onFilterChange({ logLevels: [...currentLevels, level as LogLevel] });
                    }
                  }}
                />
              );
            })}
          </div>
        )}
      </div>

      {/* Timeline View */}
      {activeView === 'timeline' && (
        <div style={{ flex: 1, overflow: 'auto' }}>
          {isLoading ? (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 120 }}>
              <span style={{ color: 'var(--text-muted)' }}>Loading...</span>
            </div>
          ) : (
            <TimelineView logs={processedLogs} />
          )}
        </div>
      )}

      {/* Logs View */}
      {activeView === 'logs' && (
        <>
          {/* Column headers */}
          {!isLoading && !error && processedLogs.length > 0 && (
            <div style={{
              display: 'grid',
              gridTemplateColumns: '90px 80px minmax(100px, 140px) minmax(120px, 160px) 70px 1fr',
              gap: 16,
              padding: '8px 16px',
              fontSize: 10,
              fontWeight: 600,
              color: 'var(--text-muted)',
              textTransform: 'uppercase',
              letterSpacing: '0.5px',
              borderBottom: '1px solid var(--border-secondary)',
              background: 'var(--bg-card)',
            }}>
              <span>Time</span>
              <span title="Time since previous log in same thread">Duration</span>
              <span>Source</span>
              <span>Thread</span>
              <span>Level</span>
              <span>Message</span>
            </div>
          )}

          {/* Content */}
          <div style={{ flex: 1, overflow: 'auto' }}>
            {isLoading && (
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 120 }}>
                <div style={{ textAlign: 'center' }}>
                  <div style={{
                    width: 24,
                    height: 24,
                    border: '2px solid var(--border-primary)',
                    borderTopColor: 'var(--text-tertiary)',
                    borderRadius: '50%',
                    animation: 'spin 1s linear infinite',
                    margin: '0 auto 8px',
                  }} />
                  <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading...</span>
                  <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
                </div>
              </div>
            )}

            {error && !isLoading && (
              <div style={{ padding: 20 }}>
                <div style={{ 
                  padding: 16, 
                  borderRadius: 8, 
                  background: 'rgba(var(--color-error-rgb), 0.1)', 
                  border: '1px solid rgba(var(--color-error-rgb), 0.2)',
                }}>
                  <div style={{ fontWeight: 500, color: 'var(--color-error)', marginBottom: 4 }}>Error loading logs</div>
                  <div style={{ fontSize: 12, color: 'var(--text-tertiary)' }}>{error}</div>
                </div>
              </div>
            )}

            {!isLoading && !error && processedLogs.length === 0 && (
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 120 }}>
                <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
                  <div style={{ fontSize: 24, marginBottom: 8 }}>—</div>
                  <div style={{ fontSize: 12 }}>
                    {logs.length === 0 ? 'No trace logs found' : 'No logs match filter'}
                  </div>
                </div>
              </div>
            )}

            {!isLoading && !error && processedLogs.length > 0 && (
              <div>
                {processedLogs.map((log, i) => (
                  <LogRow key={`${log.event_time_microseconds}-${i}`} log={log} isEven={i % 2 === 0} />
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
};

export default TraceLogViewer;
