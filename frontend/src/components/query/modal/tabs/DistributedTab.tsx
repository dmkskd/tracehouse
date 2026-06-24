import React from 'react';
import type {
  DistributedExecutionFlowEvent,
  DistributedExecutionFlowStep,
  DistributedReadDistributionEntry,
  DistributedSkewMetric,
  DistributedTopology,
  DistributedTopologyNode,
  SubQueryInfo,
} from '@tracehouse/core';
import {
  buildDistributedExecutionFlowSteps,
  distributedQueryKindLabel,
  distributedReadMetricValue,
  topologyNodeRoleLabel,
  topologyNodeRoleText,
} from '@tracehouse/core';
import { formatBytes } from '../../../../stores/databaseStore';
import { formatDurationMs } from '../../../../utils/formatters';
import { SqlHighlight } from '../../../common/SqlHighlight';
import { DistributedQueryTopology, type TopologyCoordinator } from '../shared/DistributedQueryTopology';

interface DistributedTabProps {
  topologyCoordinator: TopologyCoordinator | null;
  subQueries: SubQueryInfo[];
  distributedTopology: DistributedTopology | null;
  activeQueryId: string;
  isLoading: boolean;
  onNavigateToQuery: (queryId: string) => void;
}

const MUTED = 'var(--text-muted)';
const COORD_COLOR = '#58a6ff';
const REMOTE_COLOR = '#d29922';

function stableHostColor(host: string, index = 0): string {
  let hash = 0;
  for (let i = 0; i < host.length; i += 1) {
    hash = ((hash << 5) - hash + host.charCodeAt(i)) | 0;
  }
  const hue = Math.abs(hash + index * 47) % 360;
  return `hsl(${hue} 72% 48%)`;
}

function parseUs(ts: string): number {
  if (!ts) return 0;
  const dotIdx = ts.lastIndexOf('.');
  const baseDateStr = dotIdx >= 0 ? ts.substring(0, dotIdx) : ts;
  const usFrac = dotIdx >= 0 ? ts.substring(dotIdx + 1) : '0';
  const baseMs = new Date(baseDateStr.replace(' ', 'T') + 'Z').getTime();
  return baseMs * 1000 + parseInt(usFrac.padEnd(6, '0').substring(0, 6), 10);
}

function shortHost(hostname: string): string {
  const short = hostname.split('.')[0] || hostname;
  const tail = short.match(/-(\d+)-(\d+)-\d+$/);
  if (short.startsWith('chi-') && tail) return `s${Number(tail[1]) + 1}r${Number(tail[2]) + 1}`;
  const cloudServerMatch = short.match(/-server-([a-z0-9]+)-\d+$/);
  if (cloudServerMatch) return cloudServerMatch[1];
  return short;
}

function roleLabel(node: DistributedTopologyNode): string {
  return topologyNodeRoleLabel(node.role);
}

function scanText(selected: number, total: number): string {
  if (selected <= 0 && total <= 0) return '-';
  if (total <= 0) return selected.toLocaleString();
  return `${selected.toLocaleString()} / ${total.toLocaleString()}`;
}

function nodeScan(node: DistributedTopologyNode): { parts: string; marks: string; ranges: string } {
  const pe = node.profileEvents ?? {};
  return {
    parts: scanText(Number(pe.SelectedParts ?? 0), Number(pe.SelectedPartsTotal ?? 0)),
    marks: scanText(Number(pe.SelectedMarks ?? 0), Number(pe.SelectedMarksTotal ?? 0)),
    ranges: Number(pe.SelectedRanges ?? 0) > 0 ? Number(pe.SelectedRanges).toLocaleString() : '-',
  };
}

function subQueryScan(subQuery: SubQueryInfo): { parts: string; marks: string; ranges: string } {
  return {
    parts: scanText(subQuery.selected_parts, subQuery.selected_parts_total),
    marks: scanText(subQuery.selected_marks, subQuery.selected_marks_total),
    ranges: subQuery.selected_ranges > 0 ? subQuery.selected_ranges.toLocaleString() : '-',
  };
}

function detailLevelText(topology: DistributedTopology | null, subQueries: SubQueryInfo[]): string {
  if (!topology && subQueries.length === 0) return 'No child queries';
  if (!topology) return 'Query log only';
  if (topology.clusterAllReplicas) return 'Forced all replicas';
  if (topology.shards.length > 0) return 'Shard mapping';
  if ((topology.nodes.length > 1 || subQueries.length > 0) && topology.capabilities.queryLog) return 'Child queries';
  return 'Query log only';
}

function detailLevelExplanation(topology: DistributedTopology | null, subQueries: SubQueryInfo[]): string {
  if (!topology && subQueries.length > 0) {
    return 'Using query_log child queries. No shard metadata found.';
  }
  if (!topology) return 'No query_log child queries found.';
  if (topology.clusterAllReplicas) {
    const expected = topology.clusterAllReplicas.expectedParticipants;
    const local = topology.clusterAllReplicas.localParticipantsOnInitiator;
    return [
      'Detected clusterAllReplicas(): all configured replicas are targeted.',
      expected != null ? `${expected} expected participant${expected === 1 ? '' : 's'}.` : '',
      local > 0 ? `${local} participant${local === 1 ? '' : 's'} folded into the coordinator row.` : '',
    ].filter(Boolean).join(' ');
  }
  const textLog = topology.capabilities.textLog && topology.executionPhases.length > 0
    ? ' and text_log execution events'
    : '';
  if (topology.shards.length > 0) return `Using query_log child queries${textLog} and system.clusters shard metadata.`;
  if (topology.nodes.length > 1 || subQueries.length > 0) {
    return `Using query_log child queries${textLog}. No shard metadata found.`;
  }
  return 'Using the top-level query_log row only.';
}

function decisionMessage(code: string, message: string): string {
  if (code === 'cluster-hosts-loaded' || code === 'system-clusters-loaded') {
    return 'system.clusters shard data found.';
  }
  return message;
}

function decisionSourceLabel(source: string): string {
  if (source === 'system_clusters') return 'system.clusters';
  if (source === 'query_log') return 'query_log';
  if (source === 'text_log') return 'text_log';
  if (source === 'asynchronous_insert_log') return 'async_insert_log';
  if (source === 'processors_profile_log') return 'processors_profile_log';
  if (source === 'profile_events') return 'ProfileEvents';
  return source;
}

function hostIdentity(hostname: string): string {
  return hostname.split('.')[0] || hostname;
}

function hostMatches(a: string | undefined, b: string | undefined): boolean {
  if (!a || !b) return false;
  const left = hostIdentity(a);
  const right = hostIdentity(b);
  return left === right || left.startsWith(`${right}-`) || right.startsWith(`${left}-`);
}

function hostColorMap(hostnames: string[]): Map<string, string> {
  const hosts = [...new Set(hostnames.filter(Boolean).map(hostIdentity))].sort();
  const colors = new Map<string, string>();
  hosts.forEach((host, index) => colors.set(host, stableHostColor(host, index)));
  return colors;
}

export const DistributedTab: React.FC<DistributedTabProps> = ({
  topologyCoordinator,
  subQueries,
  distributedTopology,
  activeQueryId,
  isLoading,
  onNavigateToQuery,
}) => {
  const nodes = distributedTopology?.nodes ?? [];
  const childNodes = nodes.filter(node => node.role !== 'coordinator' && node.role !== 'insert_client');
  const childHosts = new Set(childNodes.map(node => node.hostname));
  const childRows = childNodes.length || subQueries.length;
  const shape = distributedTopology ? distributedQueryKindLabel(distributedTopology.kind) : (subQueries.length > 0 ? 'Distributed' : 'Local or unknown');
  const expectedParticipants = distributedTopology?.clusterAllReplicas?.expectedParticipants;
  const localParticipants = distributedTopology?.clusterAllReplicas?.localParticipantsOnInitiator ?? 0;
  const dataSourceDecisions = distributedTopology?.decisions
    .filter(decision => decision.level === 'info' && !decision.code.startsWith('missing-'))
    .slice(0, 8) ?? [];
  const dataGapDecisions = distributedTopology?.decisions
    .filter(decision => decision.level === 'degraded' && !decision.code.startsWith('missing-object-storage') && !decision.code.startsWith('missing-hybrid'))
    .slice(0, 8) ?? [];

  if (isLoading) {
    return <div style={{ padding: 24, color: MUTED, fontSize: 12 }}>Loading distributed topology...</div>;
  }

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, minmax(0, 1fr))', gap: 12, marginBottom: 18 }}>
        <SummaryMetric label="Execution Type" value={shape} />
        <SummaryMetric
          label="Topology Detail"
          value={detailLevelText(distributedTopology, subQueries)}
          title={detailLevelExplanation(distributedTopology, subQueries)}
        />
        <SummaryMetric label="Child Queries" value={String(childRows)} />
        <SummaryMetric label="Nodes" value={String(childHosts.size || new Set(subQueries.map(sq => sq.hostname)).size || 1)} />
        <SummaryMetric
          label="Participants"
          value={expectedParticipants != null ? String(expectedParticipants) : String(distributedTopology?.readDistribution.entries.length || '-')}
          title={localParticipants > 0 ? `${localParticipants} participant is folded into the coordinator row.` : undefined}
        />
      </div>

      {childRows === 0 && (
        <Panel title="Execution Shape">
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, lineHeight: 1.6 }}>
            No distributed child executions were found for this query. This can still be a parallel local query, or a cloud/shared-storage scan, but there is no query-log evidence that it spanned multiple ClickHouse nodes.
          </div>
        </Panel>
      )}

      {distributedTopology && distributedTopology.asyncInsertLinks.length > 0 && (
        <AsyncInsertLinksPanel
          topology={distributedTopology}
          activeQueryId={activeQueryId}
          onNavigateToQuery={onNavigateToQuery}
        />
      )}

      {topologyCoordinator && subQueries.length > 0 && (
        <Panel title="Timeline">
          <DistributedQueryTopology
            coordinator={topologyCoordinator}
            subQueries={subQueries}
            inferredTopology={distributedTopology}
            activeQueryId={activeQueryId}
            onNavigate={onNavigateToQuery}
          />
        </Panel>
      )}

      {distributedTopology && distributedTopology.readDistribution.entries.length > 0 && (
        <ResourceSkewPanel topology={distributedTopology} />
      )}

      {distributedTopology && distributedTopology.executionFlow.length > 0 && (
        <Panel title="Execution Flow">
          {topologyCoordinator ? (
            <ExecutionFlowSteps
              coordinator={topologyCoordinator}
              subQueries={subQueries}
              topology={distributedTopology}
              activeQueryId={activeQueryId}
              onNavigateToQuery={onNavigateToQuery}
            />
          ) : null}
        </Panel>
      )}

      {distributedTopology && (
        <Panel title="Data Sources">
          <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5, marginBottom: 10 }}>
            {detailLevelExplanation(distributedTopology, subQueries)}
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: dataGapDecisions.length > 0 ? 'minmax(0, 1fr) minmax(0, 1fr)' : 'minmax(0, 1fr)', gap: 14 }}>
            <div>
              <div style={{ fontSize: 9, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: 6 }}>Data Used</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                {dataSourceDecisions
                  .map(decision => (
                    <div key={`${decision.code}:${decision.message}`} style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.4 }}>
                      <span style={{ color: MUTED, fontFamily: 'var(--font-mono, monospace)' }}>[{decisionSourceLabel(decision.source)}]</span>{' '}
                      {decisionMessage(decision.code, decision.message)}
                    </div>
                ))}
              </div>
            </div>
            {dataGapDecisions.length > 0 && (
              <div>
                <div style={{ fontSize: 9, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: 6 }}>Data Gaps</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
                  {dataGapDecisions.map(decision => (
                    <div key={`${decision.code}:${decision.message}`} style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.4 }}>
                      <span style={{ color: MUTED, fontFamily: 'var(--font-mono, monospace)' }}>[{decisionSourceLabel(decision.source)}]</span>{' '}
                      {decisionMessage(decision.code, decision.message)}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </Panel>
      )}
    </div>
  );
};

const SummaryMetric: React.FC<{ label: string; value: string; title?: string }> = ({ label, value, title }) => (
  <div style={{
    border: '1px solid var(--border-secondary)',
    borderRadius: 6,
    background: 'var(--bg-card)',
    padding: '10px 12px',
    minWidth: 0,
  }} title={title}>
    <div style={{ fontSize: 9, color: MUTED, textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 5 }}>{label}</div>
    <div style={{ fontSize: 13, color: 'var(--text-primary)', fontFamily: 'var(--font-mono, monospace)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{value}</div>
  </div>
);

const Panel: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <section style={{ marginTop: 18 }}>
    <div style={{ fontSize: 10, color: MUTED, textTransform: 'uppercase', letterSpacing: '1.2px', marginBottom: 8 }}>{title}</div>
    {children}
  </section>
);

function skewMetricLabel(metric: DistributedSkewMetric): string {
  switch (metric) {
    case 'read_rows': return 'Rows';
    case 'read_bytes': return 'Bytes';
    case 'duration_ms': return 'Duration';
    case 'memory_usage': return 'Memory';
    case 'selected_parts': return 'Parts';
    case 'selected_marks': return 'Marks';
    default: return metric;
  }
}

function skewValue(metric: DistributedSkewMetric, value: number): string {
  if (metric === 'read_bytes' || metric === 'memory_usage') return formatBytes(value);
  if (metric === 'duration_ms') return formatDurationMs(value);
  return Math.round(value).toLocaleString();
}

function percentText(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function participantLabel(hostname?: string, shardNum?: number, replicaNum?: number, foldedIntoCoordinator?: boolean): string {
  const shardReplica = shardNum != null && replicaNum != null ? `s${shardNum}r${replicaNum}` : '';
  const host = hostname ? hostIdentity(hostname) : '';
  const short = host ? shortHost(host) : '';
  const folded = foldedIntoCoordinator ? ' local' : '';
  if (shardReplica && short && shardReplica !== short) return `${shardReplica} · ${short}${folded}`;
  return `${shardReplica || short || '-'}${folded}`;
}

function defaultDistributionMetric(topology: DistributedTopology): DistributedSkewMetric {
  if (topology.readDistribution.totalReadRows > 0) return 'read_rows';
  if (topology.readDistribution.totalReadBytes > 0) return 'read_bytes';
  return topology.readDistribution.skew.metrics.find(metric => metric.total > 0)?.metric ?? 'read_rows';
}

function distributionSeverity(ratioToAverage: number): string {
  if (ratioToAverage >= 3) return '#dc2626';
  if (ratioToAverage >= 2) return '#d29922';
  if (ratioToAverage >= 1.5) return '#58a6ff';
  return '#36b34a';
}

function roleText(entry: DistributedReadDistributionEntry): string {
  return topologyNodeRoleText(entry.foldedIntoCoordinator ? 'local_reader' : entry.role);
}

function metricColor(metric: DistributedSkewMetric): string {
  switch (metric) {
    case 'read_rows': return '#58a6ff';
    case 'read_bytes': return '#d29922';
    case 'duration_ms': return '#7c3aed';
    case 'memory_usage': return '#36b34a';
    case 'selected_parts': return '#ea580c';
    case 'selected_marks': return '#0891b2';
    default: return '#58a6ff';
  }
}

function shardRollupsForEntries(entries: DistributedReadDistributionEntry[]) {
  const totalRows = entries.reduce((sum, entry) => sum + entry.readRows, 0);
  const totalBytes = entries.reduce((sum, entry) => sum + entry.readBytes, 0);
  const byShard = new Map<number, DistributedReadDistributionEntry[]>();
  for (const entry of entries) {
    if (entry.shardNum == null) continue;
    const shardEntries = byShard.get(entry.shardNum);
    if (shardEntries) shardEntries.push(entry);
    else byShard.set(entry.shardNum, [entry]);
  }

  return [...byShard.entries()]
    .map(([shardNum, shardEntries]) => {
      const readRows = shardEntries.reduce((sum, entry) => sum + entry.readRows, 0);
      const readBytes = shardEntries.reduce((sum, entry) => sum + entry.readBytes, 0);
      return {
        shardNum,
        readRows,
        readBytes,
        rowShare: totalRows > 0 ? readRows / totalRows : 0,
        byteShare: totalBytes > 0 ? readBytes / totalBytes : 0,
      };
    })
    .sort((a, b) => a.shardNum - b.shardNum);
}

const ResourceSkewPanel: React.FC<{ topology: DistributedTopology }> = ({ topology }) => {
  const shapeGroups = topology.readDistribution.groups;
  const [activeShapeKey, setActiveShapeKey] = React.useState<string>(() => shapeGroups[0]?.key ?? 'unknown');
  const activeShape = shapeGroups.find(group => group.key === activeShapeKey) ?? shapeGroups[0];
  const scopedEntries = activeShape?.entries ?? topology.readDistribution.entries;
  const metricSummaries = (activeShape?.skew.metrics ?? topology.readDistribution.skew.metrics).filter(summary => summary.total > 0);
  const metricOptions = metricSummaries.map(summary => summary.metric);
  const primaryMetric = metricOptions.includes('read_rows') ? 'read_rows' : metricOptions[0] ?? defaultDistributionMetric(topology);
  const entries = [...scopedEntries]
    .sort((a, b) => distributedReadMetricValue(b, primaryMetric) - distributedReadMetricValue(a, primaryMetric));
  const shards = shardRollupsForEntries(scopedEntries).filter(shard => shard.readRows > 0 || shard.readBytes > 0);
  const maxShardBytes = Math.max(1, ...shards.map(shard => shard.readBytes));
  const maxShardRows = Math.max(1, ...shards.map(shard => shard.readRows));
  const shapeTopology = [
    activeShape?.shardCoordinatorCount ? `${activeShape.shardCoordinatorCount} shard coordinator${activeShape.shardCoordinatorCount === 1 ? '' : 's'}` : '',
    activeShape?.nestedCoordinatorCount ? `${activeShape.nestedCoordinatorCount} nested coordinator${activeShape.nestedCoordinatorCount === 1 ? '' : 's'}` : '',
    activeShape?.readerCount ? `${activeShape.readerCount} reader${activeShape.readerCount === 1 ? '' : 's'}` : '',
    activeShape?.remoteChildCount && !activeShape.shardCoordinatorCount && !activeShape.nestedCoordinatorCount ? `${activeShape.remoteChildCount} child quer${activeShape.remoteChildCount === 1 ? 'y' : 'ies'}` : '',
  ].filter(Boolean).join(' · ');

  if (entries.length === 0 && shards.length === 0) return null;

  return (
    <Panel title="Read Distribution">
      <div style={{
        border: '1px solid var(--border-secondary)',
        borderRadius: 6,
        background: 'var(--bg-card)',
        overflow: 'hidden',
      }}>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(4, minmax(0, 1fr))',
          gap: 0,
          borderBottom: '1px solid var(--border-secondary)',
        }}>
          <SkewHeaderStat label="Participants" value={String(entries.length)} />
          <SkewHeaderStat label="Query Shape" value={activeShape?.label ?? 'Ungrouped'} />
          <SkewHeaderStat label="Shape Topology" value={shapeTopology || 'Flat fan-out'} />
          <SkewHeaderStat label="Read Mode" value={topology.readDistribution.hasPerReplicaReading ? 'Per-replica' : 'Per-node'} />
        </div>

        <div style={{ padding: '10px 12px 8px' }}>
          {shapeGroups.length > 1 && (
            <div style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 6,
              marginBottom: 10,
            }}>
              {shapeGroups.map(group => (
                <button
                  key={group.key}
                  type="button"
                  onClick={() => setActiveShapeKey(group.key)}
                  title={[
                    group.key === 'local_reader' ? 'Local participant folded into the coordinator query_log row.' : group.entries.find(entry => entry.queryPreview)?.queryPreview,
                    group.key !== 'unknown' && group.key !== 'local_reader' ? `normalized_query_hash ${group.key}` : '',
                  ].filter(Boolean).join('\n')}
                  style={{
                    border: `1px solid ${group.key === activeShape?.key ? '#58a6ff' : 'var(--border-secondary)'}`,
                    borderRadius: 4,
                    background: group.key === activeShape?.key ? 'color-mix(in srgb, #58a6ff 12%, var(--bg-card))' : 'var(--bg-secondary)',
                    color: group.key === activeShape?.key ? '#58a6ff' : 'var(--text-secondary)',
                    padding: '4px 8px',
                    fontSize: 10,
                    fontFamily: 'var(--font-mono, monospace)',
                    maxWidth: 260,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {group.label} · {group.entries.length}
                </button>
              ))}
            </div>
          )}

          <div style={{
            display: 'grid',
            gridTemplateColumns: '190px repeat(auto-fit, minmax(150px, 1fr))',
            gap: 12,
            alignItems: 'center',
            marginBottom: 7,
          }}>
            <span style={{ color: MUTED, fontSize: 8, fontFamily: 'var(--font-mono, monospace)', textTransform: 'uppercase', letterSpacing: '0.7px' }}>Node</span>
            {metricSummaries.map(summary => (
              <span
                key={`metric-header:${summary.metric}`}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  gap: 8,
                  color: MUTED,
                  fontSize: 8,
                  fontFamily: 'var(--font-mono, monospace)',
                  textTransform: 'uppercase',
                  letterSpacing: '0.7px',
                }}
              >
                <span>{skewMetricLabel(summary.metric)}</span>
                <span title="Largest node share">{percentText(summary.maxShare)}</span>
              </span>
            ))}
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
            {entries.map(entry => {
              const fullHost = hostIdentity(entry.hostname);
              return (
                <div
                  key={entry.participantId}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '190px repeat(auto-fit, minmax(150px, 1fr))',
                    gap: 12,
                    alignItems: 'center',
                    minHeight: 34,
                  }}
                >
                  <span
                    title={fullHost}
                    style={{
                      color: 'var(--text-primary)',
                      fontSize: 11,
                      fontFamily: 'var(--font-mono, monospace)',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {participantLabel(entry.hostname, entry.shardNum, entry.replicaNum, entry.foldedIntoCoordinator)}
                    <span style={{ color: MUTED, marginLeft: 7, fontSize: 9 }}>{roleText(entry)}</span>
                  </span>

                  {metricSummaries.map(summary => {
                    const value = distributedReadMetricValue(entry, summary.metric);
                    const share = summary.total > 0 ? value / summary.total : 0;
                    const maxRatio = summary.max > 0 ? value / summary.max : 0;
                    const avgRatio = summary.average > 0 ? value / summary.average : 0;
                    const color = metricColor(summary.metric);
                    const averagePct = summary.max > 0 ? Math.min(100, (summary.average / summary.max) * 100) : 0;
                    return (
                      <span
                        key={`${entry.participantId}:${summary.metric}`}
                        title={`${skewMetricLabel(summary.metric)}: ${skewValue(summary.metric, value)} · ${percentText(share)} of total · ${avgRatio.toFixed(1)}x average`}
                        style={{ minWidth: 0 }}
                      >
                        <span style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          gap: 8,
                          alignItems: 'baseline',
                          marginBottom: 3,
                        }}>
                          <span style={{
                            color: 'var(--text-secondary)',
                            fontSize: 10,
                            fontFamily: 'var(--font-mono, monospace)',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                          }}>{skewValue(summary.metric, value)}</span>
                          <span style={{
                            color: avgRatio >= 1.5 ? distributionSeverity(avgRatio) : MUTED,
                            fontSize: 9,
                            fontFamily: 'var(--font-mono, monospace)',
                            flexShrink: 0,
                          }}>{percentText(share)}</span>
                        </span>
                        <span style={{
                          position: 'relative',
                          display: 'block',
                          height: 7,
                          borderRadius: 4,
                          background: 'var(--bg-tertiary)',
                          overflow: 'hidden',
                        }}>
                          <span style={{
                            display: 'block',
                            width: `${Math.max(value > 0 ? 3 : 0, Math.min(100, maxRatio * 100))}%`,
                            height: '100%',
                            borderRadius: 4,
                            background: color,
                            opacity: avgRatio >= 1.5 ? 0.85 : 0.48,
                          }} />
                          <span style={{
                            position: 'absolute',
                            left: `${averagePct}%`,
                            top: 0,
                            bottom: 0,
                            width: 2,
                            background: 'var(--text-primary)',
                            opacity: 0.35,
                          }} />
                        </span>
                      </span>
                    );
                  })}
                </div>
              );
            })}
          </div>

          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            gap: 12,
            marginTop: 8,
            color: MUTED,
            fontSize: 9,
            fontFamily: 'var(--font-mono, monospace)',
          }}>
            <span>bars scale to the largest node per metric</span>
            <span>thin marker = expected average</span>
          </div>
        </div>

        {shards.length > 1 && (
          <div style={{
            padding: '8px 12px 10px',
            borderTop: '1px solid var(--border-secondary)',
          }}>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              gap: 12,
              alignItems: 'center',
              fontSize: 9,
              color: MUTED,
              textTransform: 'uppercase',
              letterSpacing: '0.8px',
              marginBottom: 7,
            }}>
              <span>Shard Balance</span>
              <span style={{ display: 'flex', gap: 12 }}>
                <span style={{ color: '#58a6ff' }}>Rows</span>
                <span style={{ color: '#d29922' }}>Bytes</span>
              </span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: 8 }}>
              {shards.map(shard => {
                const bytesPct = Math.max(3, Math.min(100, (shard.readBytes / maxShardBytes) * 100));
                const rowsPct = Math.max(3, Math.min(100, (shard.readRows / maxShardRows) * 100));
                return (
                  <div
                    key={`shard:${shard.shardNum}`}
                    style={{
                      display: 'grid',
                      gridTemplateColumns: '52px minmax(0, 1fr) 86px',
                      gap: 8,
                      alignItems: 'center',
                      padding: '6px 8px',
                      border: '1px solid var(--border-secondary)',
                      borderRadius: 5,
                      background: 'var(--bg-secondary)',
                    }}
                  >
                    <span style={{
                      color: 'var(--text-primary)',
                      fontSize: 11,
                      fontFamily: 'var(--font-mono, monospace)',
                    }}>Shard {shard.shardNum}</span>
                    <span style={{ display: 'flex', flexDirection: 'column', gap: 4, minWidth: 0 }}>
                      <span style={{ height: 5, borderRadius: 3, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
                        <span style={{ display: 'block', width: `${rowsPct}%`, height: '100%', background: '#58a6ff' }} />
                      </span>
                      <span style={{ height: 5, borderRadius: 3, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
                        <span style={{ display: 'block', width: `${bytesPct}%`, height: '100%', background: '#d29922' }} />
                      </span>
                    </span>
                    <span style={{
                      color: MUTED,
                      fontSize: 9,
                      fontFamily: 'var(--font-mono, monospace)',
                      textAlign: 'right',
                      whiteSpace: 'nowrap',
                    }}>
                      {percentText(shard.rowShare)} rows · {formatBytes(shard.readBytes)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </Panel>
  );
};

const SkewHeaderStat: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div style={{ padding: '9px 12px', minWidth: 0, borderRight: '1px solid var(--border-secondary)' }}>
    <div style={{ fontSize: 8, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: 4 }}>{label}</div>
    <div style={{
      color: 'var(--text-primary)',
      fontSize: 11,
      fontFamily: 'var(--font-mono, monospace)',
      overflow: 'hidden',
      textOverflow: 'ellipsis',
      whiteSpace: 'nowrap',
      textTransform: 'capitalize',
    }}>{value}</div>
  </div>
);

const AsyncInsertLinksPanel: React.FC<{
  topology: DistributedTopology;
  activeQueryId: string;
  onNavigateToQuery: (queryId: string) => void;
}> = ({ topology, activeQueryId, onNavigateToQuery }) => {
  const nodeByQueryId = new Map(topology.nodes.map(node => [node.queryId, node]));
  const coordinatorNode = topology.coordinator ?? topology.nodes.find(node => node.role === 'insert_client' || node.role === 'coordinator');
  const settingNames = [
    'async_insert',
    'wait_for_async_insert',
    'async_insert_busy_timeout_ms',
    'async_insert_max_data_size',
    'async_insert_max_query_number',
  ];

  return (
    <Panel title="Async Insert Links">
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'minmax(180px, 0.9fr) 34px minmax(180px, 0.9fr) minmax(180px, 1.1fr) 90px',
        gap: 10,
        alignItems: 'center',
        fontSize: 11,
      }}>
        <div style={{ color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', fontSize: 9 }}>INSERT query</div>
        <div />
        <div style={{ color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', fontSize: 9 }}>Async insert flush</div>
        <div style={{ color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', fontSize: 9 }}>Target / volume</div>
        <div style={{ color: MUTED, textTransform: 'uppercase', letterSpacing: '0.8px', fontSize: 9, textAlign: 'right' }}>Status</div>

        {topology.asyncInsertLinks.map(link => {
          const insertNode = nodeByQueryId.get(link.queryId);
          const flushNode = nodeByQueryId.get(link.flushQueryId);
          const tableName = [link.database, link.table].filter(Boolean).join('.') || flushNode?.tables?.[0] || '-';
          const rowText = link.rows != null && link.rows > 0 ? `${link.rows.toLocaleString()} rows` : '';
          const byteText = link.bytes != null && link.bytes > 0 ? formatBytes(link.bytes) : '';
          const status = link.status || (link.exception ? 'Error' : 'Linked');
          const statusColor = link.exception ? '#ef4444' : status === 'Ok' ? '#2ea043' : REMOTE_COLOR;
          const settings = settingNames
            .map(name => {
              const value = insertNode?.settings?.[name] ?? flushNode?.settings?.[name] ?? coordinatorNode?.settings?.[name];
              return value != null && String(value) !== '' ? `${name}=${String(value)}` : '';
            })
            .filter(Boolean);

          return (
            <React.Fragment key={`${link.queryId}:${link.flushQueryId}:${link.hostname ?? ''}`}>
              <button
                type="button"
                onClick={() => onNavigateToQuery(link.queryId)}
                title={insertNode?.queryPreview || link.queryId}
                style={{
                  minWidth: 0,
                  border: `1px solid ${link.queryId === activeQueryId ? '#58a6ff' : 'var(--border-secondary)'}`,
                  background: link.queryId === activeQueryId ? 'rgba(88, 166, 255, 0.10)' : 'var(--bg-secondary)',
                  borderRadius: 6,
                  padding: '8px 10px',
                  textAlign: 'left',
                  cursor: 'pointer',
                  color: 'var(--text-primary)',
                  fontFamily: 'var(--font-mono, monospace)',
                }}
              >
                <div style={{ color: '#58a6ff', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {link.queryId.slice(0, 12)}
                </div>
                <div style={{ color: MUTED, marginTop: 3, fontSize: 9 }}>
                  {insertNode ? roleLabel(insertNode) : 'Insert'}
                </div>
              </button>

              <div style={{
                color: REMOTE_COLOR,
                fontFamily: 'var(--font-mono, monospace)',
                textAlign: 'center',
                fontSize: 18,
              }}>→</div>

              <button
                type="button"
                onClick={() => onNavigateToQuery(link.flushQueryId)}
                title={flushNode?.queryPreview || link.flushQueryId}
                style={{
                  minWidth: 0,
                  border: `1px solid ${link.flushQueryId === activeQueryId ? '#58a6ff' : 'var(--border-secondary)'}`,
                  background: link.flushQueryId === activeQueryId ? 'rgba(88, 166, 255, 0.10)' : 'var(--bg-secondary)',
                  borderRadius: 6,
                  padding: '8px 10px',
                  textAlign: 'left',
                  cursor: 'pointer',
                  color: 'var(--text-primary)',
                  fontFamily: 'var(--font-mono, monospace)',
                }}
              >
                <div style={{ color: REMOTE_COLOR, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {link.flushQueryId.slice(0, 12)}
                </div>
                <div style={{ color: MUTED, marginTop: 3, fontSize: 9 }}>
                  {flushNode ? roleLabel(flushNode) : 'Async insert flush'}
                </div>
              </button>

              <div style={{
                minWidth: 0,
                color: 'var(--text-secondary)',
                fontFamily: 'var(--font-mono, monospace)',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }} title={[tableName, rowText, byteText].filter(Boolean).join(' · ')}>
                <span style={{ color: 'var(--text-primary)' }}>{tableName}</span>
                {(rowText || byteText) && (
                  <span style={{ color: MUTED }}> · {[rowText, byteText].filter(Boolean).join(' · ')}</span>
                )}
                <span style={{ display: 'block', color: MUTED, fontSize: 9, marginTop: 3 }}>
                  {settings.length > 0 ? settings.join(' · ') : 'async settings not logged'}
                </span>
              </div>

              <div style={{
                justifySelf: 'end',
                color: statusColor,
                border: `1px solid color-mix(in srgb, ${statusColor} 35%, transparent)`,
                background: `color-mix(in srgb, ${statusColor} 12%, transparent)`,
                borderRadius: 6,
                padding: '5px 8px',
                fontFamily: 'var(--font-mono, monospace)',
                fontSize: 10,
                maxWidth: 90,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }} title={link.exception || status}>
                {status}
              </div>
            </React.Fragment>
          );
        })}
      </div>
    </Panel>
  );
};

interface FlowStepDetail {
  id: string;
  actor: string;
  actorType: 'coordinator' | 'local' | 'remote';
  queryId?: string;
  hostname?: string;
  role: string;
  startMs: number;
  durationMs: number;
  color: string;
  rows: number;
  bytes: number;
  scan?: { parts: string; marks: string; ranges: string };
  preview?: string;
}

interface FlowStep {
  event: DistributedExecutionFlowEvent;
  detail: FlowStepDetail;
  node?: DistributedTopologyNode;
  showPreview: boolean;
  groupId?: string;
  parentNodeId?: string;
  depth: number;
}

function buildStepDetails(
  coordinator: TopologyCoordinator,
  subQueries: SubQueryInfo[],
  topology: DistributedTopology,
): FlowStepDetail[] {
  const rootStartUs = parseUs(coordinator.query_start_time_microseconds);
  const hostColors = hostColorMap([
    ...topology.nodes
      .filter(node => node.role !== 'coordinator' && node.role !== 'insert_client')
      .map(node => node.hostname),
    ...subQueries.map(subQuery => subQuery.hostname),
  ]);
  const details: FlowStepDetail[] = [{
    id: 'coordinator',
    actor: 'Coordinator',
    actorType: 'coordinator',
    queryId: coordinator.query_id,
    hostname: coordinator.hostname,
    role: 'Coordinator',
    startMs: 0,
    durationMs: coordinator.query_duration_ms,
    color: COORD_COLOR,
    rows: coordinator.read_rows,
    bytes: 0,
  }];

  if (topology.localRead) {
    details.push({
      id: `local:${topology.localRead.queryId}:${topology.localRead.hostname}`,
      actor: 'Local reader',
      actorType: 'local',
      queryId: topology.localRead.queryId,
      hostname: topology.localRead.hostname,
      role: topology.localRead.shardNum != null && topology.localRead.replicaNum != null
        ? `Local read s${topology.localRead.shardNum}r${topology.localRead.replicaNum}`
        : 'Local read',
      startMs: 0,
      durationMs: coordinator.query_duration_ms,
      color: COORD_COLOR,
      rows: topology.localRead.readRows ?? 0,
      bytes: topology.localRead.readBytes ?? 0,
      preview: topology.coordinator?.queryPreview ?? '',
    });
  }

  const childNodes = topology.nodes.filter(node => node.role !== 'coordinator' && node.role !== 'insert_client');
  const rows = childNodes.length > 0
    ? childNodes.map(node => ({ kind: 'node' as const, node }))
    : subQueries.map(subQuery => ({ kind: 'subQuery' as const, subQuery }));

  rows.forEach((row, index) => {
    const node = row.kind === 'node' ? row.node : undefined;
    const subQuery = row.kind === 'subQuery'
      ? row.subQuery
      : subQueries.find(sq => sq.query_id === row.node.queryId && hostMatches(sq.hostname, row.node.hostname));
    const queryId = node?.queryId ?? subQuery?.query_id ?? '';
    const hostname = node?.hostname ?? subQuery?.hostname ?? '';
    const startUs = parseUs(subQuery?.query_start_time_microseconds || node?.queryStartTimeMicroseconds || '');
    const startMs = Math.max(0, startUs > 0 && rootStartUs > 0 ? (startUs - rootStartUs) / 1000 : 0);
    const durationMs = node?.queryDurationMs ?? subQuery?.query_duration_ms ?? 0;
    const scan = node ? nodeScan(node) : subQuery ? subQueryScan(subQuery) : undefined;
    details.push({
      id: node?.id ?? `${queryId}:${hostname}:${index}`,
      actor: shortHost(hostname),
      actorType: 'remote',
      queryId,
      hostname,
      role: node ? roleLabel(node) : 'Remote child',
      startMs,
      durationMs,
      color: hostColors.get(hostIdentity(hostname)) ?? REMOTE_COLOR,
      rows: node?.readRows ?? subQuery?.read_rows ?? 0,
      bytes: node?.readBytes ?? subQuery?.read_bytes ?? 0,
      scan,
      preview: node?.queryPreview ?? subQuery?.query_preview ?? '',
    });
  });

  return details.sort((a, b) =>
    a.actorType === b.actorType
      ? a.startMs - b.startMs || b.durationMs - a.durationMs
      : a.actorType === 'coordinator' ? -1 : b.actorType === 'coordinator' ? 1 : a.actorType === 'local' ? -1 : 1,
  );
}

function detailForEvent(
  step: DistributedExecutionFlowStep,
  details: FlowStepDetail[],
): FlowStepDetail {
  const { event, node } = step;
  if (event.actorType === 'coordinator') {
    return {
      ...details[0],
      preview: node?.queryPreview ?? details[0].preview,
    };
  }
  if (event.actorType === 'local') {
    return details.find(detail => detail.actorType === 'local') ?? {
      ...details[0],
      id: `local:${event.queryId ?? event.hostname ?? event.offsetMs}`,
      actor: 'Local reader',
      actorType: 'local',
      role: 'Local read',
      rows: event.rows ?? 0,
      bytes: 0,
    };
  }
  const remoteDetails = details.filter(detail => detail.actorType === 'remote');
  if (node) {
    const nodeDetail = remoteDetails.find(detail => detail.id === node.id);
    if (nodeDetail) return nodeDetail;
  }
  return remoteDetails.find(detail =>
    event.queryId &&
    detail.queryId === event.queryId &&
    hostMatches(event.hostname, detail.hostname)
  ) ?? remoteDetails.find(detail =>
    hostMatches(event.hostname, detail.hostname)
  ) ?? remoteDetails.find(detail =>
    event.queryId && detail.queryId === event.queryId
  ) ?? {
    id: `${event.queryId ?? event.hostname ?? event.title}:${event.offsetMs}`,
    actor: shortHost(event.hostname ?? 'remote'),
    actorType: 'remote',
    queryId: event.queryId,
    hostname: event.hostname,
    role: 'Remote child',
    startMs: event.offsetMs,
    durationMs: 0,
    color: event.hostname ? hostColorMap([event.hostname]).get(hostIdentity(event.hostname)) ?? REMOTE_COLOR : REMOTE_COLOR,
    rows: 0,
    bytes: 0,
  };
}

function buildFlowSteps(
  coordinator: TopologyCoordinator,
  subQueries: SubQueryInfo[],
  topology: DistributedTopology,
): FlowStep[] {
  const details = buildStepDetails(coordinator, subQueries, topology);

  return buildDistributedExecutionFlowSteps(topology).map(step => {
    const detail = detailForEvent(step, details);
    return {
      event: step.event,
      detail,
      node: step.node,
      showPreview: step.showQueryPreview,
      groupId: step.groupId,
      parentNodeId: step.parentNodeId,
      depth: step.depth,
    };
  });
}

function eventHasWorkStats(event: DistributedExecutionFlowEvent): boolean {
  return event.kind === 'remote_read_completed' || event.kind === 'coordinator_read_completed' || event.kind === 'local_read_completed';
}

function flowEventTitle(event: DistributedExecutionFlowEvent, node?: DistributedTopologyNode): string {
  switch (event.kind) {
    case 'coordinator_started': return 'Coordinator accepted query';
    case 'async_insert_buffered': return 'Async insert linked to flush';
    case 'local_read_started': return 'Local read started';
    case 'local_read_completed': return 'Local read folded into coordinator';
    case 'remote_started':
      if (node?.role === 'insert_forwarder') return 'Remote table INSERT started';
      if (node?.role === 'async_insert_flush') return 'Async insert flush started';
      if (node?.role === 'shard_leader') return 'Shard coordinator started';
      if (node?.role === 'nested_coordinator') return 'Nested coordinator started';
      if (node?.role === 'replica_reader') return 'Reader query started';
      return 'Remote query started';
    case 'remote_read_completed':
      if (node?.hostname) {
        const host = hostIdentity(node.hostname);
        if (node.role === 'insert_forwarder') return `Remote table INSERT completed on ${host}`;
        if (node.role === 'async_insert_flush') return `Async insert flush completed on ${host}`;
        if (node.role === 'shard_leader') return `Shard coordinator completed on ${host}`;
        if (node.role === 'nested_coordinator') return `Nested coordinator completed on ${host}`;
        if (node.role === 'replica_reader') return `Reader query completed on ${host}`;
        return `Remote query completed on ${host}`;
      }
      if (node?.role === 'insert_forwarder') return 'Remote table INSERT completed';
      if (node?.role === 'async_insert_flush') return 'Async insert flush completed';
      if (node?.role === 'shard_leader') return 'Shard coordinator completed';
      if (node?.role === 'nested_coordinator') return 'Nested coordinator completed';
      if (node?.role === 'replica_reader') return 'Reader query completed';
      return 'Remote query completed';
    case 'coordinator_merge': return 'Coordinator merged remote results';
    case 'coordinator_output': return 'Coordinator produced output';
    case 'coordinator_read_completed': return 'Coordinator completed query';
    default: return event.title;
  }
}

function remoteStartedPrefix(node?: DistributedTopologyNode): string {
  if (node?.role === 'insert_forwarder') return 'Remote table INSERT started on ';
  if (node?.role === 'async_insert_flush') return 'Async insert flush started on ';
  if (node?.role === 'shard_leader') return 'Shard coordinator started on ';
  if (node?.role === 'nested_coordinator') return 'Nested coordinator started on ';
  if (node?.role === 'replica_reader') return 'Reader query started on ';
  return 'Remote query started on ';
}

function remoteCompletedPrefix(node?: DistributedTopologyNode): string {
  if (node?.role === 'insert_forwarder') return 'Remote table INSERT completed on ';
  if (node?.role === 'async_insert_flush') return 'Async insert flush completed on ';
  if (node?.role === 'shard_leader') return 'Shard coordinator completed on ';
  if (node?.role === 'nested_coordinator') return 'Nested coordinator completed on ';
  if (node?.role === 'replica_reader') return 'Reader query completed on ';
  return 'Remote query completed on ';
}

function renderFlowEventTitle(
  event: DistributedExecutionFlowEvent,
  detail: FlowStepDetail,
  node?: DistributedTopologyNode,
): React.ReactNode {
  const hostname = node?.hostname ?? detail.hostname;
  if (event.kind === 'coordinator_started' && hostname) {
    return (
      <>
        Coordinator accepted query on <span style={{ color: detail.color }}>{hostIdentity(hostname)}</span>
      </>
    );
  }
  if (event.kind !== 'remote_started' && event.kind !== 'remote_read_completed') {
    return flowEventTitle(event, node);
  }
  if (!hostname) return flowEventTitle(event, node);

  return (
    <>
      {event.kind === 'remote_started' ? remoteStartedPrefix(node) : remoteCompletedPrefix(node)}
      <span style={{ color: detail.color }}>{hostIdentity(hostname)}</span>
    </>
  );
}

function flowEventDetail(event: DistributedExecutionFlowEvent, detail: FlowStepDetail): string {
  if (event.kind === 'local_read_started') {
    return event.detail || 'Local participant work began inside the coordinator query.';
  }
  if (event.kind === 'local_read_completed') {
    return 'Folded into the coordinator row.';
  }
  if (event.kind === 'async_insert_buffered') {
    return event.detail || 'Client insert was linked to a later async insert flush.';
  }
  if (event.kind === 'remote_started') {
    if (detail.role === 'Remote table INSERT') {
      return detail.hostname ? `Remote table INSERT on ${hostIdentity(detail.hostname)}.` : 'Remote table INSERT began.';
    }
    if (detail.role === 'Async insert flush') {
      return detail.hostname ? `Async insert flush on ${hostIdentity(detail.hostname)}.` : 'Async insert flush began.';
    }
    return detail.hostname ? `Sent to ${hostIdentity(detail.hostname)}.` : 'Remote execution began.';
  }
  if (event.kind === 'remote_read_completed') {
    if (detail.role === 'Remote table INSERT') {
      return detail.hostname ? `Remote table INSERT completed on ${hostIdentity(detail.hostname)}.` : 'Remote table INSERT completed.';
    }
    if (detail.role === 'Async insert flush') {
      return detail.hostname ? `Async insert flush completed on ${hostIdentity(detail.hostname)}.` : 'Async insert flush completed.';
    }
    return detail.hostname ? `Completed on ${hostIdentity(detail.hostname)}.` : 'Remote child completed.';
  }
  return event.detail || flowEventTitle(event);
}

function compactEventMeta(event: DistributedExecutionFlowEvent): string {
  if (event.kind === 'remote_read_completed') {
    return event.source;
  }
  return event.source;
}

function shouldShowEventDetail(event: DistributedExecutionFlowEvent): boolean {
  return event.kind !== 'remote_read_completed'
    && event.kind !== 'remote_started'
    && event.kind !== 'coordinator_started';
}

const ExecutionFlowSteps: React.FC<{
  coordinator: TopologyCoordinator;
  subQueries: SubQueryInfo[];
  topology: DistributedTopology;
  activeQueryId: string;
  onNavigateToQuery: (queryId: string) => void;
}> = ({ coordinator, subQueries, topology, activeQueryId, onNavigateToQuery }) => {
  const steps = buildFlowSteps(coordinator, subQueries, topology);
  const totalMs = Math.max(1, coordinator.query_duration_ms);

  return (
    <div style={{
      border: '1px solid var(--border-secondary)',
      borderRadius: 6,
      background: 'var(--bg-card)',
      overflow: 'hidden',
    }}>
      <div style={{
        display: 'grid',
        gridTemplateColumns: '82px 22px minmax(0, 1fr)',
        gap: 10,
        padding: '8px 12px',
        borderBottom: '1px solid var(--border-secondary)',
        color: MUTED,
        fontSize: 9,
        fontFamily: 'var(--font-mono, monospace)',
      }}>
        <span>Time</span>
        <span />
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>0</span>
          <span>{formatDurationMs(totalMs / 2)}</span>
          <span>{formatDurationMs(totalMs)}</span>
        </div>
      </div>
      {steps.map((step, index) => (
        <FlowStepRow
          key={`${step.event.kind}:${step.event.offsetMs}:${step.detail.id}:${index}`}
          step={step}
          totalMs={totalMs}
          isActive={step.detail.queryId === activeQueryId}
          isFirst={index === 0}
          isLast={index === steps.length - 1}
          onNavigateToQuery={onNavigateToQuery}
        />
      ))}
    </div>
  );
};

const FlowStepRow: React.FC<{
  step: FlowStep;
  totalMs: number;
  isActive: boolean;
  isFirst: boolean;
  isLast: boolean;
  onNavigateToQuery: (queryId: string) => void;
}> = ({ step, totalMs, isActive, isFirst, isLast, onNavigateToQuery }) => {
  const { event, detail } = step;
  const eventPct = Math.min(100, Math.max(0, (event.offsetMs / totalMs) * 100));
  const leftPct = Math.min(100, Math.max(0, (detail.startMs / totalMs) * 100));
  const widthPct = Math.max(0.8, Math.min(100 - leftPct, (detail.durationMs / totalMs) * 100));
  const markerColor = event.actorType === 'coordinator' ? COORD_COLOR : detail.color;
  const depthIndent = Math.min(2, Math.max(0, step.depth)) * 18;
  const showStats = eventHasWorkStats(event);
  const showDetail = shouldShowEventDetail(event);
  const rowMinHeight = step.showPreview ? 150 : showDetail ? 64 : 48;
  const metricParts = [
    showStats && detail.durationMs > 0 ? formatDurationMs(detail.durationMs) : '',
    showStats && detail.rows > 0 ? `${detail.rows.toLocaleString()} rows` : '',
    showStats && detail.bytes > 0 ? formatBytes(detail.bytes) : '',
  ].filter(Boolean);

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: '82px 22px minmax(0, 1fr)',
      gap: 10,
      padding: '0 12px',
      borderTop: isFirst ? 'none' : '1px solid var(--border-secondary)',
      background: isActive ? 'var(--bg-hover)' : 'transparent',
    }}>
      <div style={{
        paddingTop: 11,
        color: MUTED,
        fontFamily: 'var(--font-mono, monospace)',
        fontSize: 10,
      }}>
        +{formatDurationMs(event.offsetMs)}
      </div>

      <div style={{
        position: 'relative',
        display: 'flex',
        justifyContent: 'center',
        minHeight: rowMinHeight,
      }}>
        {!isFirst && (
          <span style={{
            position: 'absolute',
            top: 0,
            bottom: '50%',
            width: 1,
            background: 'var(--border-secondary)',
          }} />
        )}
        {!isLast && (
          <span style={{
            position: 'absolute',
            top: '50%',
            bottom: 0,
            width: 1,
            background: 'var(--border-secondary)',
          }} />
        )}
        <span style={{
          position: 'absolute',
          top: 11,
          width: 10,
          height: 10,
          borderRadius: '50%',
          background: markerColor,
          boxShadow: `0 0 0 3px color-mix(in srgb, ${markerColor} 18%, transparent)`,
        }} />
      </div>

      <div style={{ padding: step.showPreview ? '8px 0 10px' : '8px 0', minWidth: 0 }}>
        <button
          onClick={() => detail.queryId && onNavigateToQuery(detail.queryId)}
          disabled={!detail.queryId}
          style={{
            width: '100%',
            display: 'grid',
            gridTemplateColumns: '130px minmax(0, 1fr) 230px',
            gap: 12,
            alignItems: 'start',
            textAlign: 'left',
            background: 'transparent',
            border: 'none',
            padding: 0,
            cursor: detail.queryId ? 'pointer' : 'default',
          }}
        >
          <span style={{
            display: 'flex',
            alignItems: 'flex-start',
            gap: 8,
            minWidth: 0,
            paddingLeft: depthIndent,
            position: 'relative',
          }}>
            {step.parentNodeId && (
              <span style={{
                position: 'absolute',
                left: depthIndent - 15,
                top: 14,
                width: 12,
                height: 1,
                background: 'var(--border-secondary)',
              }} />
            )}
            <span style={{
              width: 3,
              minHeight: showDetail ? 34 : 28,
              borderRadius: 2,
              background: markerColor,
              flexShrink: 0,
            }} />
            <span style={{ minWidth: 0 }}>
              <span style={{
                display: 'block',
                color: event.actorType === 'coordinator' || event.actorType === 'local' ? COORD_COLOR : 'var(--text-primary)',
                fontFamily: 'var(--font-mono, monospace)',
                fontSize: 11,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}>{detail.actor}</span>
              <span style={{
                display: 'block',
                color: MUTED,
                fontSize: 9,
                marginTop: 2,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}>{detail.role}</span>
            </span>
          </span>

          <span style={{ minWidth: 0 }}>
            <span style={{
              display: 'block',
              marginBottom: 7,
            }}>
              <span style={{
                color: 'var(--text-primary)',
                fontFamily: 'var(--font-mono, monospace)',
                fontSize: 12,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}>{renderFlowEventTitle(event, detail, step.node)}</span>
            </span>
            {showDetail && (
              <span style={{
                display: 'block',
                color: 'var(--text-secondary)',
                fontSize: 11,
                lineHeight: 1.45,
                marginBottom: 10,
              }}>{flowEventDetail(event, detail)}</span>
            )}
          </span>

          <span style={{
            minWidth: 0,
            paddingTop: 2,
          }}>
            <span style={{
              display: 'grid',
              gridTemplateColumns: 'auto minmax(72px, 1fr) auto',
              alignItems: 'center',
              gap: 8,
              color: MUTED,
              fontFamily: 'var(--font-mono, monospace)',
              fontSize: 8,
            }}>
              <span style={{ whiteSpace: 'nowrap' }}>{compactEventMeta(event)} · {formatDurationMs(event.offsetMs)}</span>
              <span style={{
                position: 'relative',
                display: 'block',
                height: 9,
                borderRadius: 5,
                background: 'color-mix(in srgb, var(--bg-tertiary) 72%, var(--bg-card))',
                border: '1px solid var(--border-secondary)',
                overflow: 'hidden',
              }}>
                {showStats && detail.durationMs > 0 && (
                  <span style={{
                    position: 'absolute',
                    left: `${leftPct}%`,
                    width: `${widthPct}%`,
                    top: 2,
                    height: 3,
                    borderRadius: 2,
                    background: markerColor,
                    opacity: 0.24,
                  }} />
                )}
                <span style={{
                  position: 'absolute',
                  left: `${eventPct}%`,
                  top: 1,
                  width: 3,
                  height: 7,
                  borderRadius: 2,
                  transform: 'translateX(-50%)',
                  background: markerColor,
                }} />
              </span>
              <span style={{ whiteSpace: 'nowrap', textAlign: 'right' }}>{formatDurationMs(totalMs)}</span>
            </span>
          </span>
        </button>

        {showStats && (metricParts.length > 0 || detail.scan) && (
          <div style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '6px 12px',
            marginTop: 9,
            marginLeft: 142,
            color: MUTED,
            fontFamily: 'var(--font-mono, monospace)',
            fontSize: 10,
          }}>
            {metricParts.length > 0 && <span>{metricParts.join(' · ')}</span>}
            {detail.scan && <span>parts {detail.scan.parts}</span>}
            {detail.scan && <span>marks {detail.scan.marks}</span>}
            {detail.scan && <span>ranges {detail.scan.ranges}</span>}
          </div>
        )}

        {step.showPreview && detail.preview && (
          <div
            title={detail.preview}
            style={{ marginTop: 9, marginLeft: 142, border: '1px solid var(--border-secondary)', borderRadius: 5, overflow: 'hidden' }}
          >
            <SqlHighlight maxHeight={96} style={{
              padding: '8px 10px',
              fontSize: 10,
              lineHeight: 1.45,
              color: 'var(--text-secondary)',
              background: 'var(--bg-code)',
            }}>
              {detail.preview}
            </SqlHighlight>
          </div>
        )}
      </div>
    </div>
  );
};

export default DistributedTab;
