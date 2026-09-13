import { useMemo } from 'react';
import { css } from '@emotion/css';
import { Link } from 'react-router-dom';
import type { OverviewData } from '@tracehouse/core';
import { loadDashboards } from '../analytics/dashboards';
import { useClusterStore } from '../../stores/clusterStore';
import { formatNumber } from '../../utils/formatters';

interface OverviewDestinationCardsProps {
  data: OverviewData | null;
  cpuUsage: number;
  memoryPct: number;
  cpuHistory: number[];
  isLoading?: boolean;
}

const grid = css`
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 12px;
  @media (max-width: 1200px) { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  @media (max-width: 850px) { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  @media (max-width: 500px) { grid-template-columns: minmax(0, 1fr); }
`;
const card = css`
  display: flex; flex-direction: column; min-width: 0;
  padding: 15px; border-radius: 8px;
  border: 1px solid var(--border-primary);
  background: var(--bg-card); color: var(--text-primary); text-decoration: none;
  transition: background 0.15s ease, border-color 0.15s ease;
  [data-theme="light"] & { background: #fff; }
  &:hover { background: var(--bg-card-hover); border-color: var(--accent-blue); }
  [data-theme="light"] &:hover { background: #f8fbff; }
  &:focus-visible { outline: 2px solid var(--accent-blue); outline-offset: 3px; }
  h3 { font-size: 13px; font-weight: 600; line-height: 1.4; margin: 12px 0 7px; }
  p { font-size: 11px; color: var(--text-secondary); line-height: 1.5; margin: 0 0 15px; flex: 1; }
  small { font: 10px monospace; color: var(--text-muted); line-height: 1.5; }
`;

export function OverviewDestinationCards({ data, isLoading = false }: OverviewDestinationCardsProps) {
  const cluster = useClusterStore();
  const dashboards = useMemo(() => { try { return loadDashboards(); } catch { return []; } }, []);
  const hosts = data?.serverInfo.clusterHosts ?? [];
  const nodes = hosts.length || (data?.serverInfo.hostname ? 1 : 0);
  const running = data?.queryConcurrency.running ?? data?.runningQueries.length ?? 0;
  const history = data?.queryConcurrency.qpsHistory ?? [];
  const qps = history.at(-1)?.qps ?? 0;
  const estimated = Math.round(history.reduce((sum, point) => sum + point.qps * 15, 0));
  const merges = data?.activeMerges ?? [];
  const replication = data?.replication;
  const cards = [
    ['Time Travel', '/timetravel', 'Which queries ran during this period?', 'Historical execution timeline, overlaps and resource usage', `${formatNumber(estimated)} estimated queries · last 15m`],
    ['Queries', '/queries', 'Which queries are running or slow?', 'Running queries, execution details and query history', `${formatNumber(running)} running · ${qps.toFixed(1)} q/s`],
    ['Merges', '/merges', 'Which merges and mutations are active?', 'Background work, progress and throughput', `${merges.length} active · ${merges.filter(merge => merge.isMutation).length} mutations`],
    ['Analytics', '/analytics', 'What do the diagnostic dashboards show?', 'Query performance, insert health and storage metrics', `${dashboards.length} dashboards`],
    ['Events', '/events', 'Which operational events were recorded?', 'Failures, changes and server events', `${data?.alerts.length ?? 0} active signals · ${data?.alerts.filter(alert => alert.severity === 'crit').length ?? 0} critical`],
    ['Explorer', '/databases', 'How are databases and tables structured?', 'Schemas, parts, storage and table-level activity', 'Databases · tables · parts'],
    ['Cluster', '/cluster', 'How is the cluster configured?', 'Nodes, shards, replicas and server configuration', `${nodes} nodes · ${cluster.detected ? cluster.shardCount : 1} shards · ${cluster.detected ? cluster.replicaCount : Math.max(1, nodes)} replicas`],
    ['Replication', '/replication', 'Are replicas healthy and in sync?', 'Replication queues, delays and replica status', replication?.totalTables ? `${replication.healthyTables}/${replication.totalTables} healthy · queue ${replication.queueSize}` : 'No replicas'],
    ['Engine Internals', '/engine-internals', 'Which engine resources are under pressure?', 'Thread pools, caches and monitoring capabilities', 'Thread pools · caches · capabilities'],
  ];
  return (
    <section aria-labelledby="overview-explore-heading">
      <h2 id="overview-explore-heading" style={{ fontSize: 15, fontWeight: 600, margin: '0 0 15px', color: 'var(--text-primary)' }}>Explore the cluster</h2>
      <div className={grid}>
        {cards.map(([title, href, question, description, signal]) => (
          <Link key={href} to={href} state={{ from: { path: '/overview', label: 'Overview' } }} className={card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, color: 'var(--accent-blue)', fontSize: 12 }}><strong>{title}</strong><span aria-hidden="true">↗</span></div>
            <h3>{question}</h3><p>{description}</p>
            <small>{isLoading && !data && !['Analytics', 'Explorer', 'Engine Internals'].includes(title) ? 'Loading...' : signal}</small>
          </Link>
        ))}
      </div>
    </section>
  );
}
export default OverviewDestinationCards;
