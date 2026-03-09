/**
 * DeepDiveWidgets - Overview landing page teaser cards
 *
 * Each widget is a compact card that surfaces a key insight and links to
 * a deeper page for further investigation.
 */

import { Link } from 'react-router-dom';
import { formatBytes, formatNumberCompact, formatDurationMs } from '../../utils/formatters';
import { useClusterStore } from '../../stores/clusterStore';
import type {
  TopTableInfo,
  EngineHealthInfo,
  SlowQueriesSummary,
  WorstOrderingKey,
  CpuSpikesInfo,
} from '@tracehouse/core';

// ── Shared widget card shell ─────────────────────────────────────────────

const WidgetCard: React.FC<{
  title: string;
  linkTo: string;
  linkLabel: string;
  children: React.ReactNode;
  className?: string;
  linkState?: Record<string, unknown>;
}> = ({ title, linkTo, linkLabel, children, className = '', linkState }) => (
  <div
    className={`rounded-lg border overflow-hidden ${className}`}
    style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%', display: 'flex', flexDirection: 'column' }}
  >
    <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--border-secondary)', flexShrink: 0 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 4px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>{title}</h3>
          <Link
            to={linkTo}
            state={{ from: { path: '/overview', label: 'Overview' }, ...linkState }}
            title={linkLabel}
            style={{
              fontSize: 11,
              color: 'var(--text-muted)',
              textDecoration: 'none',
              display: 'flex',
              alignItems: 'center',
              gap: 3,
            }}
          >
            <span>→</span>
          </Link>
        </div>
      </div>
    </div>
    <div style={{ padding: '12px 16px', flex: 1, display: 'flex', flexDirection: 'column' }}>{children}</div>
  </div>
);

// ── 1. Top Tables → Database Explorer ────────────────────────────────────

export function TopTablesWidget({ tables }: { tables: TopTableInfo[] }) {
  if (tables.length === 0) return null;
  const maxBytes = tables[0]?.totalBytes || 1;

  return (
    <div className="rounded-lg border overflow-hidden" style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--border-secondary)', flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 4px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>Top Tables</h3>
            <Link to="/databases" state={{ from: { path: '/overview', label: 'Overview' } }} title="Databases" style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 3 }}>
              <span>→</span>
            </Link>
          </div>
        </div>
      </div>
      <div style={{ flex: 1, overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, tableLayout: 'fixed' }}>
          <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <th style={{ padding: '6px 8px', textAlign: 'left', fontWeight: 500, fontSize: 10, color: 'var(--text-muted)' }}>Table</th>
              <th style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 500, fontSize: 10, color: 'var(--text-muted)', width: 52 }}>Rows</th>
              <th style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 500, fontSize: 10, color: 'var(--text-muted)', width: 40 }}>Parts</th>
              <th style={{ padding: '6px 8px', textAlign: 'right', fontWeight: 500, fontSize: 10, color: 'var(--text-muted)', width: 90 }}>Size</th>
            </tr>
          </thead>
          <tbody>
            {tables.map((t, i) => {
              const pct = (t.totalBytes / maxBytes) * 100;
              return (
                <tr key={`${t.database}.${t.table}`} style={{ borderBottom: '1px solid var(--border-secondary)', background: i % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)' }}>
                  <td style={{ padding: '5px 8px', fontFamily: 'monospace', fontSize: 11, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={`${t.database}.${t.table}`}>
                    <span style={{ color: 'var(--text-muted)' }}>{t.database}.</span><span style={{ color: 'var(--text-primary)' }}>{t.table}</span>
                  </td>
                  <td style={{ padding: '5px 8px', fontFamily: 'monospace', fontSize: 11, textAlign: 'right', color: 'var(--text-muted)' }}>{formatNumberCompact(t.totalRows)}</td>
                  <td style={{ padding: '5px 8px', fontFamily: 'monospace', fontSize: 11, textAlign: 'right', color: 'var(--text-muted)' }}>{t.partCount}</td>
                  <td style={{ padding: '5px 8px', position: 'relative', overflow: 'hidden' }}>
                    <div style={{
                      position: 'absolute', left: 4, top: 3, bottom: 3, right: 4,
                      borderRadius: 3,
                      background: `linear-gradient(90deg, rgba(99, 102, 241, 0.15) ${pct}%, transparent ${pct}%)`,
                      transition: 'all 0.3s ease',
                    }} />
                    <span style={{ position: 'relative', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'nowrap', float: 'right' }}>
                      {formatBytes(t.totalBytes)}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── 2. Engine Health → Engine Internals ──────────────────────────────────

function barColor(pct: number): string {
  if (pct > 85) return '#ef4444';
  if (pct > 60) return '#f59e0b';
  return '#3b82f6';
}

function LabeledBar({ label, value, pct, color }: { label: string; value: string; pct: number; color?: string }) {
  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, marginBottom: 3 }}>
        <span style={{ color: 'var(--text-muted)' }}>{label}</span>
        <span style={{ color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{value}</span>
      </div>
      <div style={{ height: 6, borderRadius: 3, background: 'var(--bg-tertiary)' }}>
        <div style={{ width: `${Math.min(100, Math.max(1, pct))}%`, height: '100%', borderRadius: 3, background: color ?? barColor(pct), transition: 'width 0.3s ease' }} />
      </div>
    </div>
  );
}

export function EngineHealthWidget({ health }: { health: EngineHealthInfo }) {
  const fragmentation =
    health.jemallocResident > 0
      ? ((health.jemallocResident - health.jemallocAllocated) / health.jemallocResident) * 100
      : 0;

  const memoryUsedPct =
    health.memoryTotal > 0 ? (health.memoryResident / health.memoryTotal) * 100 : 0;

  const pools = health.pools
    .map(p => ({ name: p.name, pct: p.size > 0 ? (p.active / p.size) * 100 : 0 }))
    .filter(p => p.name);

  return (
    <WidgetCard title="Engine Health" linkTo="/engine-internals" linkLabel="Engine Internals">
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10, flex: 1 }}>
        <LabeledBar
          label="Memory"
          value={`${formatBytes(health.memoryResident)} / ${formatBytes(health.memoryTotal)}`}
          pct={memoryUsedPct}
        />
        <LabeledBar
          label="Jemalloc overhead"
          value={`${fragmentation.toFixed(1)}%`}
          pct={fragmentation}
          color={fragmentation > 20 ? '#f59e0b' : '#3b82f6'}
        />
        {pools.map(p => (
          <LabeledBar
            key={p.name}
            label={`${p.name} pool`}
            value={`${p.pct.toFixed(0)}%`}
            pct={p.pct}
            color={p.pct > 80 ? '#ef4444' : p.pct > 50 ? '#f59e0b' : '#3b82f6'}
          />
        ))}
      </div>
    </WidgetCard>
  );
}

// ── 3. Cluster Status → Cluster ──────────────────────────────────────────

export function ClusterStatusWidget() {
  const { clusterName, shardCount, replicaCount, detected } = useClusterStore();

  if (!detected || !clusterName) return null;

  return (
    <WidgetCard title="Cluster" linkTo="/cluster" linkLabel="Cluster">
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10, flex: 1 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
          <span style={{ color: 'var(--text-muted)' }}>Name</span>
          <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{clusterName}</span>
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
          <span style={{ color: 'var(--text-muted)' }}>Topology</span>
          <span style={{ fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
            {shardCount} shard{shardCount > 1 ? 's' : ''} × {replicaCount} replica{replicaCount > 1 ? 's' : ''}
          </span>
        </div>
        {/* Shard × Replica matrix */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center', gap: 4, marginTop: 4 }}>
          {/* Column labels (replicas) */}
          <div style={{ display: 'flex', gap: 4, paddingLeft: 28 }}>
            {Array.from({ length: replicaCount }, (_, r) => (
              <div key={r} style={{ flex: 1, fontSize: 9, color: 'var(--text-muted)', textAlign: 'center' }}>
                R{r + 1}
              </div>
            ))}
          </div>
          {Array.from({ length: shardCount }, (_, s) => (
            <div key={s} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ fontSize: 9, color: 'var(--text-muted)', width: 24, textAlign: 'right', flexShrink: 0 }}>S{s + 1}</span>
              {Array.from({ length: replicaCount }, (_, r) => (
                <div
                  key={r}
                  style={{
                    flex: 1,
                    height: 18,
                    borderRadius: 3,
                    background: '#22c55e20',
                    border: '1px solid #22c55e40',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                  }}
                  title={`Shard ${s + 1}, Replica ${r + 1}`}
                >
                  <div style={{ width: 6, height: 6, borderRadius: '50%', background: '#22c55e' }} />
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </WidgetCard>
  );
}

// ── 4. Ordering Key Spotlight → Analytics ────────────────────────────────

export function OrderingKeySpotlight({ data }: { data: WorstOrderingKey }) {
  const marksPerQuery = Math.round(data.selectedMarks / Math.max(1, data.queryCount));
  const marksPerPart =
    data.selectedParts > 0 ? data.selectedMarks / data.selectedParts : data.selectedMarks;

  // High marks-per-part means the ordering key isn't pruning well
  const severity = marksPerPart > 1000 ? 'warn' : 'ok';

  // Efficiency gauge: lower marks/query = better. Cap at 10k for visual scale.
  const efficiencyPct = Math.min(100, (marksPerQuery / 10000) * 100);

  return (
    <WidgetCard title="Ordering Key Spotlight" linkTo="/analytics" linkLabel="Analytics">
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8, flex: 1 }}>
        <div style={{ fontSize: 11, fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          <span style={{ color: 'var(--text-muted)' }}>{data.database}.</span>
          <span style={{ color: 'var(--text-primary)' }}>{data.table}</span>
        </div>

        {/* Marks/query gauge */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, marginBottom: 4 }}>
            <span style={{ color: 'var(--text-muted)' }}>Marks scanned / query</span>
            <span style={{ fontFamily: 'monospace', color: severity === 'warn' ? '#f59e0b' : 'var(--text-secondary)' }}>
              {formatNumberCompact(marksPerQuery)}
            </span>
          </div>
          <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-tertiary)' }}>
            <div style={{
              width: `${Math.max(2, efficiencyPct)}%`,
              height: '100%',
              borderRadius: 4,
              background: severity === 'warn' ? '#f59e0b' : '#22c55e',
              transition: 'width 0.3s ease',
            }} />
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, marginTop: 2, color: 'var(--text-muted)' }}>
            <span>efficient</span>
            <span>excessive</span>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
          <span style={{ color: 'var(--text-muted)' }}>Queries (24h)</span>
          <span style={{ fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
            {formatNumberCompact(data.queryCount)}
          </span>
        </div>
        <div style={{ fontSize: 10, color: severity === 'warn' ? '#f59e0b' : 'var(--text-muted)' }}>
          {severity === 'warn'
            ? 'High marks/query — consider reviewing ordering key'
            : 'Ordering key efficiency looks reasonable'}
        </div>
      </div>
    </WidgetCard>
  );
}

// ── 5. Recent Hotspots → Time Travel ─────────────────────────────────────

function CpuGauge({ pct }: { pct: number }) {
  const clamped = Math.min(100, pct);
  const r = 40;
  const stroke = 8;
  const cx = 50;
  const cy = 50;
  // Semi-circle arc from 180° to 0°
  const startAngle = Math.PI;
  const range = Math.PI;
  const valueAngle = startAngle - (clamped / 100) * range;

  const arcPath = (angle: number) => {
    const x = cx + r * Math.cos(angle);
    const y = cy - r * Math.sin(angle);
    return `${x} ${y}`;
  };

  const bgD = `M ${arcPath(startAngle)} A ${r} ${r} 0 1 1 ${arcPath(0)}`;
  const valD = `M ${arcPath(startAngle)} A ${r} ${r} 0 ${clamped > 50 ? 1 : 0} 1 ${arcPath(valueAngle)}`;
  const color = pct > 85 ? '#ef4444' : pct > 60 ? '#f59e0b' : '#3b82f6';

  return (
    <svg viewBox="0 0 100 55" style={{ width: '100%', maxWidth: 120 }}>
      <path d={bgD} fill="none" stroke="var(--bg-tertiary)" strokeWidth={stroke} strokeLinecap="round" />
      {clamped > 0 && <path d={valD} fill="none" stroke={color} strokeWidth={stroke} strokeLinecap="round" />}
      <text x={cx} y={cy - 4} textAnchor="middle" fontSize="14" fontWeight="700" fontFamily="monospace" fill={color}>
        {pct.toFixed(0)}%
      </text>
    </svg>
  );
}

export function RecentHotspotsWidget({ spikes }: { spikes: CpuSpikesInfo }) {
  return (
    <WidgetCard title="Recent Hotspots" linkTo="/timetravel" linkLabel="Time Travel">
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6, flex: 1 }}>
        {spikes.spikeCount > 0 ? (
          <>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
              <span
                style={{
                  fontSize: 20,
                  fontWeight: 700,
                  fontFamily: 'monospace',
                  color: spikes.spikeCount >= 5 ? '#ef4444' : '#f59e0b',
                }}
              >
                {spikes.spikeCount}
              </span>
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                CPU spike{spikes.spikeCount !== 1 ? 's' : ''} in last 15m
              </span>
            </div>
            {/* CPU gauge */}
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <CpuGauge pct={spikes.maxCpu} />
            </div>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', textAlign: 'center' }}>
              Peak CPU — explore timeline to correlate
            </div>
          </>
        ) : (
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
            <svg viewBox="0 0 24 24" width="32" height="32" fill="none" stroke="#22c55e" strokeWidth="2">
              <circle cx="12" cy="12" r="10" />
              <path d="M8 12l3 3 5-5" />
            </svg>
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>No CPU spikes in last 15 min</span>
          </div>
        )}
      </div>
    </WidgetCard>
  );
}

// ── 6. Slow Queries → Query Monitor ──────────────────────────────────────

export function SlowQueriesWidget({ summary }: { summary: SlowQueriesSummary }) {
  return (
    <WidgetCard title="Slow Queries" linkTo="/queries" linkLabel="Queries" linkState={{
      tab: 'history',
      filter: {
        minDurationMs: 10000,
        startTime: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
      },
    }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8, flex: 1 }}>
        {summary.count > 0 ? (
          <>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
              <span
                style={{
                  fontSize: 20,
                  fontWeight: 700,
                  fontFamily: 'monospace',
                  color: summary.count >= 10 ? '#ef4444' : '#f59e0b',
                }}
              >
                {summary.count}
              </span>
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                quer{summary.count !== 1 ? 'ies' : 'y'} &gt; 10s in last hour
              </span>
            </div>
            {/* Duration comparison bars */}
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center', gap: 10 }}>
              <LabeledBar
                label="Slowest"
                value={formatDurationMs(summary.maxDurationMs)}
                pct={100}
                color={summary.maxDurationMs > 60000 ? '#ef4444' : '#f59e0b'}
              />
              <LabeledBar
                label="Average"
                value={formatDurationMs(summary.avgDurationMs)}
                pct={summary.maxDurationMs > 0 ? (summary.avgDurationMs / summary.maxDurationMs) * 100 : 0}
                color="#3b82f6"
              />
            </div>
          </>
        ) : (
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
            <svg viewBox="0 0 24 24" width="32" height="32" fill="none" stroke="#22c55e" strokeWidth="2">
              <circle cx="12" cy="12" r="10" />
              <path d="M8 12l3 3 5-5" />
            </svg>
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>No slow queries in last hour</span>
          </div>
        )}
      </div>
    </WidgetCard>
  );
}
