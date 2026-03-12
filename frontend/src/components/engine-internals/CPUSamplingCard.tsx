/**
 * CPUSamplingCard - Shows CPU attribution from trace_log sampling data.
 * Aggregates stack trace samples by thread pool to show where CPU
 * is actually being spent across all engine thread pools.
 */

import { useState, useEffect, useRef } from 'react';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../../stores/refreshSettingsStore';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { EngineInternalsService } from '@tracehouse/core';
import type { CPUSamplingData, CPUSamplingByThread, TopCPUFunction, IClickHouseAdapter } from '@tracehouse/core';

const POOL_COLORS: Record<CPUSamplingByThread['pool'], string> = {
  queries: '#3b82f6',
  merges: '#f59e0b',
  mutations: '#ef4444',
  merge_mutate: '#e87830',
  replication: '#8b5cf6',
  io: '#22c55e',
  schedule: '#06b6d4',
  handler: '#64748b',
  other: '#94a3b8',
};

const POOL_LABELS: Record<CPUSamplingByThread['pool'], string> = {
  queries: 'Queries',
  merges: 'Merges',
  mutations: 'Mutations',
  merge_mutate: 'Merges & Mutations',
  replication: 'Replication',
  io: 'IO',
  schedule: 'Schedule',
  handler: 'Handlers',
  other: 'Other',
};

interface CPUSamplingCardProps {
  className?: string;
  /** Override adapter for host-targeted queries in cluster mode */
  adapter?: IClickHouseAdapter;
}

export function CPUSamplingCard({ className = '', adapter: adapterOverride }: CPUSamplingCardProps) {
  const services = useClickHouseServices();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const { available: hasTraceLog, missing, probing } = useCapabilityCheck(['trace_log', 'introspection_functions']);
  const [data, setData] = useState<CPUSamplingData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showFunctions, setShowFunctions] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!services || !hasTraceLog) {
      setData(null);
      return;
    }

    const svc = new EngineInternalsService(adapterOverride ?? services.adapter);
    let cancelled = false;

    const fetch = async () => {
      try {
        const result = await svc.getCPUSamplingData(60, 60);
        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to fetch sampling data');
        }
      }
    };

    fetch();
    // CPU sampling is heavier, use at least 10s or the global rate (whichever is larger)
    const effectiveRate = refreshRateSeconds > 0 ? Math.max(10, refreshRateSeconds) : 10;
    intervalRef.current = setInterval(fetch, clampToAllowed(effectiveRate, refreshConfig) * 1000);

    return () => {
      cancelled = true;
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [services, hasTraceLog, refreshRateSeconds, refreshConfig, adapterOverride]);

  if (!hasTraceLog && !probing) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>CPU Sampling Attribution</h3>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
          Requires {missing.map(m => `system.${m}`).join(', ')} (not available on this server)
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>CPU Sampling Attribution</h3>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--accent-red)' }}>{error}</div>
      </div>
    );
  }

  if (!data || data.totalSamples === 0) {
    // Check capability details to give an actionable hint
    const caps = useMonitoringCapabilitiesStore.getState().capabilities;
    const traceLogCap = caps?.capabilities.find(c => c.id === 'trace_log');
    const cpuProfilerCap = caps?.capabilities.find(c => c.id === 'cpu_profiler_active');
    let profilerHint: string;
    if (traceLogCap?.detail?.includes('CPU profiler: off')) {
      profilerHint = 'CPU profiler is disabled. Set query_profiler_cpu_time_period_ns > 0 to enable sampling.';
    } else if (cpuProfilerCap && !cpuProfilerCap.available && cpuProfilerCap.detail?.includes('SYS_PTRACE')) {
      profilerHint = 'CPU profiler is enabled but no samples are being collected. The container is likely missing the SYS_PTRACE capability — add it to the pod securityContext in your Kubernetes manifest.';
    } else {
      profilerHint = `No CPU samples in the last ${data?.windowSeconds || 30}s`;
    }

    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>CPU Sampling Attribution</h3>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
          {profilerHint}
        </div>
      </div>
    );
  }

  // Aggregate by pool category
  const byPool = new Map<CPUSamplingByThread['pool'], number>();
  for (const t of data.byThread) {
    byPool.set(t.pool, (byPool.get(t.pool) || 0) + t.cpuSamples);
  }
  const poolEntries = Array.from(byPool.entries()).sort((a, b) => b[1] - a[1]);

  // Low sample count = statistically noisy. At 1 sample/s/core with 12 cores and 180s window,
  // a fully loaded system would produce ~2160 samples. Under 100 samples means the system
  // is mostly idle and proportions are noise.
  const isLowConfidence = data.totalSamples < 100;

  if (isLowConfidence) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>CPU Sampling Attribution</h3>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
          System is mostly idle — only {data.totalSamples} CPU samples in the last {data.windowSeconds}s.
          <br />
          <span style={{ fontSize: 10 }}>
            Attribution requires sustained CPU activity to be statistically meaningful.
          </span>
        </div>
      </div>
    );
  }

  return (
    <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
      {/* Header */}
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)', display: 'flex', alignItems: 'center', gap: 4 }}>
          CPU Sampling Attribution
          <span
            title="These percentages show the relative share of sampled CPU time across thread pools — not absolute server load. If the server is mostly idle, even a small amount of background work (like regular part merges) can appear as a large percentage because it dominates the few samples collected."
            style={{
              display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
              width: 14, height: 14, borderRadius: '50%', fontSize: 9, fontWeight: 600,
              color: 'var(--text-muted)', border: '1px solid var(--border-secondary)',
              cursor: 'help', flexShrink: 0,
            }}
          >?</span>
          <span style={{ fontWeight: 400, color: 'var(--text-muted)', marginLeft: 2, fontSize: 10 }}>
            trace_log · may lag ~60s under load
          </span>
        </h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 10 }}>
          <span style={{ color: 'var(--text-muted)' }}>
            <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{data.totalSamples.toLocaleString()}</span> samples / {data.windowSeconds}s
          </span>
        </div>
      </div>

      <div style={{ padding: 16 }}>
        {/* Stacked bar */}
        <div style={{ display: 'flex', height: 20, borderRadius: 4, overflow: 'hidden', marginBottom: 12, border: '1px solid var(--border-secondary)' }}>
          {poolEntries.map(([pool, samples]) => {
            const pct = (samples / data.totalSamples) * 100;
            if (pct < 0.5) return null;
            return (
              <div
                key={pool}
                title={`${POOL_LABELS[pool]}: ${samples} samples (${pct.toFixed(1)}%)`}
                style={{
                  width: `${pct}%`,
                  background: POOL_COLORS[pool],
                  opacity: 0.8,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  fontSize: 9,
                  fontWeight: 600,
                  color: '#fff',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  borderRight: '1px solid rgba(0,0,0,0.2)',
                  transition: 'width 0.5s ease',
                }}
              >
                {pct > 8 ? `${POOL_LABELS[pool]} ${pct.toFixed(0)}%` : pct > 4 ? `${pct.toFixed(0)}%` : ''}
              </div>
            );
          })}
        </div>

        {/* Per-thread breakdown */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
          {data.byThread.slice(0, 10).map((t: CPUSamplingByThread, index: number) => {
            const pct = (t.cpuSamples / data.totalSamples) * 100;
            return (
              <div key={t.threadName} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 0', borderBottom: index < Math.min(data.byThread.length, 10) - 1 ? '1px solid var(--border-secondary)' : 'none' }}>
                <span style={{
                  fontSize: 10,
                  fontFamily: 'monospace',
                  color: POOL_COLORS[t.pool],
                  minWidth: 130,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }} title={t.threadName}>
                  {t.threadName}
                </span>
                <div style={{ flex: 1, height: 6, borderRadius: 3, overflow: 'hidden', background: 'var(--bg-tertiary)' }}>
                  <div style={{
                    width: `${Math.min(100, pct)}%`,
                    height: '100%',
                    borderRadius: 3,
                    background: POOL_COLORS[t.pool],
                    opacity: 0.7,
                    transition: 'width 0.5s ease',
                  }} />
                </div>
                <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-primary)', minWidth: 40, textAlign: 'right' }}>
                  {pct.toFixed(1)}%
                </span>
                <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-muted)', minWidth: 50, textAlign: 'right' }}>
                  {t.cpuSamples}
                </span>
              </div>
            );
          })}
        </div>

        {/* Top functions toggle */}
        {data.topFunctions.length > 0 && (
          <div style={{ marginTop: 10 }}>
            <button
              onClick={() => setShowFunctions(!showFunctions)}
              style={{
                background: 'none', border: 'none', cursor: 'pointer',
                fontSize: 10, color: 'var(--text-muted)', padding: '2px 0',
                display: 'flex', alignItems: 'center', gap: 4,
              }}
            >
              <span style={{ fontSize: 8, transition: 'transform 0.15s', transform: showFunctions ? 'rotate(90deg)' : 'rotate(0deg)', display: 'inline-block' }}>▶</span>
              Top functions
            </button>
            {showFunctions && (
              <div style={{ marginTop: 6, display: 'flex', flexDirection: 'column', gap: 2 }}>
                {data.topFunctions.slice(0, 10).map((f: TopCPUFunction, i: number) => (
                  <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 10 }}>
                    <span style={{ fontFamily: 'monospace', color: POOL_COLORS[classifyThreadPool(f.threadName)], minWidth: 35, textAlign: 'right' }}>
                      {f.samples}
                    </span>
                    <span style={{ fontFamily: 'monospace', color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={f.functionName}>
                      {f.functionName}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Legend */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, marginTop: 10, paddingTop: 8, borderTop: '1px solid var(--border-secondary)' }}>
          {poolEntries.map(([pool]) => (
            <div key={pool} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10 }}>
              <div style={{ width: 8, height: 8, borderRadius: 2, background: POOL_COLORS[pool], opacity: 0.8 }} />
              <span style={{ color: 'var(--text-muted)' }}>{POOL_LABELS[pool]}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

/** Local helper matching the service-side classification */
function classifyThreadPool(threadName: string): CPUSamplingByThread['pool'] {
  const name = threadName.toLowerCase();
  if (name.includes('querypipeline') || name.includes('querypool') || name.includes('parallel')) return 'queries';
  // MergeMutate is ClickHouse's shared pool for both background merges and mutations — classify it distinctly
  if (name.includes('merge') && name.includes('mutat')) return 'merge_mutate';
  if (name.includes('merge')) return 'merges';
  if (name.includes('mutat')) return 'mutations';
  if (name.includes('fetch') || name.includes('replic') || name.includes('repl')) return 'replication';
  if (name.includes('io') || name.includes('disk') || name.includes('read') || name.includes('write')) return 'io';
  if (name.includes('sched') || name.includes('bgsch')) return 'schedule';
  if (name.includes('http') || name.includes('tcp') || name.includes('handler')) return 'handler';
  return 'other';
}

export default CPUSamplingCard;
