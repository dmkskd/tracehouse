/**
 * ReplicationSummary - Replication health summary
 * Styled to match ActiveMergesTable / RunningQueriesTable card pattern.
 */

import { Link } from 'react-router-dom';
import { StatusDot } from './StatusDot';
import { OVERVIEW_COLORS } from '../../styles/overviewColors';
import type { ReplicationSummary as ReplicationSummaryType } from '@tracehouse/core';

interface ReplicationSummaryProps {
  replication: ReplicationSummaryType;
  className?: string;
}

function formatDelay(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

export function ReplicationSummary({ replication, className = '' }: ReplicationSummaryProps) {
  const isHealthy = replication.readonlyReplicas === 0 && replication.maxDelay < 300;
  const status = replication.readonlyReplicas > 0 ? 'crit' : replication.maxDelay > 60 ? 'warn' : 'ok';

  return (
    <div className={`rounded-lg border overflow-hidden ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header — matches ActiveMergesTable / RunningQueriesTable */}
      <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--border-secondary)', flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 4px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
              Replication
            </h3>
            <Link to="/replication" state={{ from: { path: '/overview', label: 'Overview' } }} title="Replication" style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 3 }}>
              <span>→</span>
            </Link>
          </div>
          <StatusDot status={status} label={isHealthy ? 'Healthy' : 'Issues'} />
        </div>
      </div>

      {/* Body — table layout matching the other overview tables */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead style={{ background: 'var(--bg-card)' }}>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Tables</th>
              <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Readonly</th>
              <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Max Delay</th>
              <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Queue</th>
              {replication.fetchesActive > 0 && (
                <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Fetches</th>
              )}
            </tr>
          </thead>
          <tbody>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                {replication.healthyTables}
                <span style={{ color: 'var(--text-muted)' }}>/{replication.totalTables}</span>
              </td>
              <td style={{
                padding: '5px 8px',
                textAlign: 'right',
                fontFamily: 'monospace',
                color: replication.readonlyReplicas > 0 ? OVERVIEW_COLORS.crit : 'var(--text-muted)',
              }}>
                {replication.readonlyReplicas}
              </td>
              <td style={{
                padding: '5px 8px',
                textAlign: 'right',
                fontFamily: 'monospace',
                color: replication.maxDelay > 60 ? OVERVIEW_COLORS.warn : 'var(--text-muted)',
              }}>
                {formatDelay(replication.maxDelay)}
              </td>
              <td style={{
                padding: '5px 8px',
                textAlign: 'right',
                fontFamily: 'monospace',
                color: replication.queueSize > 0 ? OVERVIEW_COLORS.warn : 'var(--text-muted)',
              }}>
                {replication.queueSize}
              </td>
              {replication.fetchesActive > 0 && (
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-purple, #a78bfa)' }}>
                  {replication.fetchesActive}
                </td>
              )}
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default ReplicationSummary;
