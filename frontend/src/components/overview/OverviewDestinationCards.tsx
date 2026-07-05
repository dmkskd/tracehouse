import React, { useMemo } from 'react';
import { Link } from 'react-router-dom';
import type { OverviewData, ResourceAttribution } from '@tracehouse/core';
import { loadDashboards } from '../analytics/dashboards';
import { useClusterStore } from '../../stores/clusterStore';
import { OVERVIEW_COLORS, RESOURCE_COLORS } from '../../styles/overviewColors';
import { formatBytesPerSec, formatDuration, formatNumber } from '../../utils/formatters';

interface OverviewDestinationCardsProps {
  data: OverviewData | null;
  cpuUsage: number;
  memoryPct: number;
  cpuHistory: number[];
  isLoading?: boolean;
}

interface CardProps {
  title: string;
  href: string;
  icon: React.ReactNode;
  accent: string;
  primary: string;
  secondary: string;
  children?: React.ReactNode;
}

const cardGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))',
  gap: 12,
};

function Icon({ children }: { children: React.ReactNode }) {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {children}
    </svg>
  );
}

function MiniSparkline({ values, color }: { values: number[]; color: string }) {
  const points = values.slice(-18);
  if (points.length < 2) {
    return <div style={{ height: 20 }} />;
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = Math.max(max - min, 1);
  const d = points
    .map((value, index) => {
      const x = (index / (points.length - 1)) * 100;
      const y = 28 - ((value - min) / span) * 24 - 2;
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');

  return (
    <svg viewBox="0 0 100 32" preserveAspectRatio="none" style={{ width: '100%', height: 24, display: 'block' }}>
      <path d={d} fill="none" stroke={color} strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

function SegmentedBar({ segments }: { segments: Array<{ value: number; color: string; label: string }> }) {
  const total = segments.reduce((sum, segment) => sum + Math.max(0, segment.value), 0);
  return (
    <div style={{ height: 9, borderRadius: 999, overflow: 'hidden', display: 'flex', background: 'var(--bg-tertiary)' }}>
      {segments
        .filter(segment => segment.value > 0.5)
        .map(segment => (
          <div
            key={segment.label}
            title={`${segment.label}: ${segment.value.toFixed(1)}%`}
            style={{
              width: `${total > 0 ? (segment.value / total) * 100 : 0}%`,
              background: segment.color,
            }}
          />
        ))}
    </div>
  );
}

function ProgressBar({ value, color }: { value: number; color: string }) {
  const pct = Math.max(0, Math.min(100, value));
  return (
    <div style={{ height: 9, borderRadius: 999, overflow: 'hidden', background: 'var(--bg-tertiary)' }}>
      <div style={{ width: `${pct}%`, height: '100%', borderRadius: 999, background: color, transition: 'width 0.2s ease' }} />
    </div>
  );
}

function LoadingPreview() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 7 }}>
      <div style={{ height: 9, width: '100%', borderRadius: 999, background: 'var(--bg-tertiary)' }} />
      <div style={{ height: 9, width: '62%', borderRadius: 999, background: 'var(--bg-tertiary)', opacity: 0.75 }} />
    </div>
  );
}

function DotRow({ count, active, color }: { count: number; active: number; color: string }) {
  const visible = Math.max(1, Math.min(count, 8));
  return (
    <div style={{ display: 'flex', gap: 6, alignItems: 'center', height: 22 }}>
      {Array.from({ length: visible }, (_, index) => (
        <span
          key={index}
          style={{
            width: 11,
            height: 11,
            borderRadius: '50%',
            background: index < active ? color : 'transparent',
            border: `1px solid ${index < active ? color : 'var(--border-primary)'}`,
            boxShadow: index < active ? `0 0 0 3px ${color}18` : 'none',
          }}
        />
      ))}
    </div>
  );
}

function DestinationCard({ title, href, icon, accent, primary, secondary, children }: CardProps) {
  return (
    <Link
      to={href}
      state={{ from: { path: '/overview', label: 'Overview' } }}
      style={{
        minHeight: 126,
        padding: '13px 14px',
        borderRadius: 8,
        border: '1px solid var(--border-primary)',
        background: 'var(--bg-card)',
        color: 'inherit',
        textDecoration: 'none',
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
        boxShadow: 'var(--shadow-sm)',
        transition: 'border-color 0.15s ease, transform 0.15s ease, background 0.15s ease',
      }}
      onMouseEnter={event => {
        event.currentTarget.style.borderColor = accent;
        event.currentTarget.style.background = 'var(--bg-card-hover)';
        event.currentTarget.style.transform = 'translateY(-1px)';
      }}
      onMouseLeave={event => {
        event.currentTarget.style.borderColor = 'var(--border-primary)';
        event.currentTarget.style.background = 'var(--bg-card)';
        event.currentTarget.style.transform = 'none';
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10, color: 'var(--text-secondary)' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 9, minWidth: 0 }}>
          <span style={{ color: accent, display: 'flex', alignItems: 'center', flexShrink: 0 }}>{icon}</span>
          <span style={{ fontSize: 14, fontWeight: 700, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{title}</span>
        </span>
        <span style={{ color: 'var(--accent-blue)', fontSize: 15, fontWeight: 700, flexShrink: 0 }}>→</span>
      </div>

      <div>
        <div style={{ color: 'var(--text-primary)', fontSize: 27, lineHeight: 1.05, fontWeight: 750, letterSpacing: 0 }}>
          {primary}
        </div>
        <div style={{ marginTop: 4, color: 'var(--text-secondary)', fontSize: 12, lineHeight: 1.25, minHeight: 15, maxHeight: 32, overflow: 'hidden' }}>
          {secondary}
        </div>
      </div>

      <div style={{ flex: 1, minHeight: 24, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
        {children}
      </div>
    </Link>
  );
}

function dominantCpu(attribution?: ResourceAttribution): { label: string; value: number } {
  if (!attribution) return { label: 'other', value: 0 };
  const entries = Object.entries(attribution.cpu.breakdown) as Array<[keyof ResourceAttribution['cpu']['breakdown'], number]>;
  const [label, value] = entries.reduce((best, current) => current[1] > best[1] ? current : best, entries[0]);
  return { label, value };
}

function formatPct(value: number): string {
  return `${value.toFixed(value >= 10 ? 1 : 2)}%`;
}

export function OverviewDestinationCards({ data, cpuUsage, memoryPct, cpuHistory, isLoading = false }: OverviewDestinationCardsProps) {
  const cluster = useClusterStore();
  const dashboards = useMemo(() => {
    try {
      return loadDashboards();
    } catch {
      return [];
    }
  }, []);

  const hosts = data?.serverInfo.clusterHosts ?? [];
  const nodeCount = hosts.length || (data?.serverInfo.hostname ? 1 : 0);
  const shardCount = cluster.detected ? cluster.shardCount : 1;
  const replicaCount = cluster.detected ? cluster.replicaCount : Math.max(1, nodeCount);
  const runningQueries = data?.queryConcurrency.running ?? data?.runningQueries.length ?? 0;
  const qps = data?.queryConcurrency.qpsHistory.at(-1)?.qps ?? 0;
  const qpsHistory = data?.queryConcurrency.qpsHistory.map(point => point.qps) ?? [];
  const slowestQuery = Math.max(0, ...(data?.runningQueries.map(query => query.elapsed) ?? [0]));
  const activeMerges = data?.activeMerges ?? [];
  const mergeCount = activeMerges.length;
  const mutationCount = activeMerges.filter(merge => merge.isMutation).length;
  const mergeThroughput = activeMerges.reduce((sum, merge) => sum + merge.readBytesPerSec + merge.writeBytesPerSec, 0);
  const avgMergeProgress = activeMerges.length > 0
    ? activeMerges.reduce((sum, merge) => sum + merge.progress, 0) / activeMerges.length * 100
    : 0;
  const activeMergeTables = new Set(activeMerges.map(merge => `${merge.database}.${merge.table}`));
  const replication = data?.replication;
  const healthyTables = replication?.healthyTables ?? 0;
  const totalTables = replication?.totalTables ?? 0;
  const replicationPct = totalTables > 0 ? (healthyTables / totalTables) * 100 : 100;
  const cpuLeader = dominantCpu(data?.resourceAttribution);
  const estimatedQueriesInWindow = Math.round(qpsHistory.reduce((sum, value) => sum + value * 15, 0));
  const peakQps = Math.max(0, ...qpsHistory);
  const dashboardTitles = dashboards.slice(0, 3).map(dashboard => dashboard.title).join(' · ');
  const waitingForLiveData = isLoading && !data;
  const primaryOrLoading = (value: string) => waitingForLiveData ? 'Loading...' : value;
  const secondaryOrLoading = (value: string) => waitingForLiveData ? 'fetching overview data' : value;

  return (
    <section>
      <div style={cardGridStyle}>
        <DestinationCard
          title="Cluster"
          href="/cluster"
          accent={OVERVIEW_COLORS.ok}
          icon={<Icon><path d="M12 3v4" /><path d="M12 17v4" /><path d="M5.6 7.5l3.5 2" /><path d="M14.9 14l3.5 2" /><path d="M18.4 7.5l-3.5 2" /><path d="M9.1 14l-3.5 2" /><circle cx="12" cy="12" r="3" /><circle cx="12" cy="3" r="1" /><circle cx="12" cy="21" r="1" /><circle cx="4.5" cy="7" r="1" /><circle cx="19.5" cy="7" r="1" /><circle cx="4.5" cy="17" r="1" /><circle cx="19.5" cy="17" r="1" /></Icon>}
          primary={primaryOrLoading(nodeCount > 0 ? `${nodeCount} node${nodeCount === 1 ? '' : 's'}` : 'No nodes')}
          secondary={secondaryOrLoading(`${shardCount} shard${shardCount === 1 ? '' : 's'} · ${replicaCount} replica${replicaCount === 1 ? '' : 's'}`)}
        >
          {waitingForLiveData ? <LoadingPreview /> : <DotRow count={nodeCount || 1} active={nodeCount || 0} color={OVERVIEW_COLORS.ok} />}
        </DestinationCard>

        <DestinationCard
          title="Queries"
          href="/queries"
          accent={OVERVIEW_COLORS.queries}
          icon={<Icon><path d="M3 5h8" /><path d="M3 12h5" /><path d="M3 19h8" /><circle cx="17" cy="11" r="4" /><path d="M20 14l2 2" /></Icon>}
          primary={primaryOrLoading(`${formatNumber(runningQueries)} running`)}
          secondary={secondaryOrLoading(`${qps.toFixed(1)} q/s · slowest ${slowestQuery > 0 ? formatDuration(slowestQuery) : 'none'}`)}
        >
          {waitingForLiveData ? (
            <LoadingPreview />
          ) : (
            <>
              <MiniSparkline values={qpsHistory} color={OVERVIEW_COLORS.queries} />
              <div style={{ marginTop: 3, fontSize: 12, color: 'var(--text-muted)' }}>
                {data?.queryConcurrency.queued ?? 0} queued · {data?.queryConcurrency.rejectedRecent ?? 0} rejected
              </div>
            </>
          )}
        </DestinationCard>

        <DestinationCard
          title="Merges"
          href="/merges"
          accent={OVERVIEW_COLORS.merges}
          icon={<Icon><path d="M4 7h7" /><path d="M4 17h7" /><path d="M11 7l4 5-4 5" /><path d="M15 12h5" /></Icon>}
          primary={primaryOrLoading(`${formatNumber(mergeCount)} active`)}
          secondary={secondaryOrLoading(`${mutationCount} mutation${mutationCount === 1 ? '' : 's'} · ${formatBytesPerSec(mergeThroughput)}`)}
        >
          {waitingForLiveData ? (
            <LoadingPreview />
          ) : (
            <>
              <ProgressBar value={avgMergeProgress} color={OVERVIEW_COLORS.merges} />
              <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-muted)' }}>
                {avgMergeProgress.toFixed(1)}% avg progress
              </div>
            </>
          )}
        </DestinationCard>

        <DestinationCard
          title="Time Travel"
          href="/timetravel"
          accent={OVERVIEW_COLORS.ok}
          icon={<Icon><path d="M3 12a9 9 0 1 0 3-6.7" /><path d="M3 4v5h5" /><path d="M12 7v5l3 2" /></Icon>}
          primary={primaryOrLoading(`${formatNumber(estimatedQueriesInWindow)} queries`)}
          secondary={secondaryOrLoading(`last 15m · peak ${peakQps.toFixed(1)} q/s`)}
        >
          {waitingForLiveData ? <LoadingPreview /> : <MiniSparkline values={qpsHistory.length > 0 ? qpsHistory : cpuHistory} color={OVERVIEW_COLORS.ok} />}
        </DestinationCard>

        <DestinationCard
          title="Replication"
          href="/replication"
          accent={replication?.readonlyReplicas ? OVERVIEW_COLORS.crit : OVERVIEW_COLORS.ok}
          icon={<Icon><rect x="4" y="4" width="12" height="12" rx="2" /><rect x="8" y="8" width="12" height="12" rx="2" /></Icon>}
          primary={primaryOrLoading(totalTables > 0 ? `${healthyTables}/${totalTables} healthy` : 'No replicas')}
          secondary={secondaryOrLoading(`queue ${replication?.queueSize ?? 0} · max delay ${replication?.maxDelay ?? 0}s`)}
        >
          {waitingForLiveData ? (
            <LoadingPreview />
          ) : (
            <>
              <ProgressBar value={replicationPct} color={replicationPct >= 100 ? OVERVIEW_COLORS.ok : OVERVIEW_COLORS.warn} />
              <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-muted)' }}>
                {replication?.readonlyReplicas ?? 0} read-only · {replication?.fetchesActive ?? 0} fetches
              </div>
            </>
          )}
        </DestinationCard>

        <DestinationCard
          title="Engine Internals"
          href="/engine-internals"
          accent={cpuUsage > 85 ? OVERVIEW_COLORS.crit : OVERVIEW_COLORS.queries}
          icon={<Icon><rect x="7" y="7" width="10" height="10" rx="2" /><path d="M4 10h3" /><path d="M4 14h3" /><path d="M17 10h3" /><path d="M17 14h3" /><path d="M10 4v3" /><path d="M14 4v3" /><path d="M10 17v3" /><path d="M14 17v3" /></Icon>}
          primary={primaryOrLoading(`${formatPct(cpuUsage)} CPU`)}
          secondary={secondaryOrLoading(`${cpuLeader.label} ${cpuLeader.value.toFixed(1)}% · memory ${formatPct(memoryPct)}`)}
        >
          {waitingForLiveData ? (
            <LoadingPreview />
          ) : (
            <SegmentedBar
              segments={[
                { label: 'Queries', value: data?.resourceAttribution.cpu.breakdown.queries ?? 0, color: RESOURCE_COLORS.cpu.queries },
                { label: 'Merges', value: data?.resourceAttribution.cpu.breakdown.merges ?? 0, color: RESOURCE_COLORS.cpu.merges },
                { label: 'Mutations', value: data?.resourceAttribution.cpu.breakdown.mutations ?? 0, color: RESOURCE_COLORS.cpu.mutations },
                { label: 'Other', value: data?.resourceAttribution.cpu.breakdown.other ?? 0, color: RESOURCE_COLORS.cpu.other },
              ]}
            />
          )}
        </DestinationCard>

        <DestinationCard
          title="Explorer"
          href="/databases"
          accent={OVERVIEW_COLORS.replication}
          icon={<Icon><ellipse cx="12" cy="5" rx="7" ry="3" /><path d="M5 5v6c0 1.7 3.1 3 7 3s7-1.3 7-3V5" /><path d="M5 11v6c0 1.7 3.1 3 7 3s7-1.3 7-3v-6" /></Icon>}
          primary={primaryOrLoading(`${formatNumber(activeMergeTables.size)} table${activeMergeTables.size === 1 ? '' : 's'}`)}
          secondary={secondaryOrLoading('with active merge activity')}
        >
          {waitingForLiveData ? (
            <LoadingPreview />
          ) : (
            <SegmentedBar
              segments={[
                { label: 'Merge tables', value: activeMergeTables.size, color: OVERVIEW_COLORS.merges },
                { label: 'Idle', value: Math.max(1, 8 - activeMergeTables.size), color: 'var(--bg-tertiary)' },
              ]}
            />
          )}
        </DestinationCard>

        <DestinationCard
          title="Analytics"
          href="/analytics"
          accent="#a78bfa"
          icon={<Icon><path d="M4 19V9" /><path d="M10 19V5" /><path d="M16 19v-8" /><path d="M3 19h18" /><rect x="3" y="9" width="3" height="10" rx="1" /><rect x="9" y="5" width="3" height="14" rx="1" /><rect x="15" y="11" width="3" height="8" rx="1" /></Icon>}
          primary={`${dashboards.length} dashboard${dashboards.length === 1 ? '' : 's'}`}
          secondary={dashboardTitles || 'saved diagnostic dashboards'}
        >
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {dashboards.slice(0, 3).map(dashboard => (
              <span
                key={dashboard.id}
                style={{
                  maxWidth: '100%',
                  padding: '3px 7px',
                  borderRadius: 6,
                  background: 'rgba(139, 92, 246, 0.12)',
                  color: '#a78bfa',
                  fontSize: 11,
                  fontWeight: 650,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {dashboard.category ?? dashboard.group ?? 'Dashboard'}
              </span>
            ))}
          </div>
        </DestinationCard>
      </div>
    </section>
  );
}

export default OverviewDestinationCards;
