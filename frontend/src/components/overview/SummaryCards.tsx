/**
 * SummaryCards - Three summary cards for CPU, Memory, and Disk/Replication
 */

import { ProgressRing } from '../overview/ProgressRing';
import { OVERVIEW_COLORS } from '../../styles/overviewColors';
import { formatBytes } from '../../utils/formatters';
import type { ResourceAttribution, ReplicationSummary } from '@tracehouse/core';

interface SummaryCardsProps {
  attribution: ResourceAttribution;
  replication: ReplicationSummary;
  className?: string;
}

export function SummaryCards({ attribution, replication, className = '' }: SummaryCardsProps) {
  const memoryPct = attribution.memory.totalRAM > 0
    ? (attribution.memory.totalRSS / attribution.memory.totalRAM) * 100
    : 0;

  const replicationHealthy = replication.totalTables > 0
    ? (replication.healthyTables / replication.totalTables) * 100
    : 100;

  return (
    <div className={`grid grid-cols-1 md:grid-cols-3 gap-4 ${className}`}>
      {/* CPU Card */}
      <div className="rounded-lg p-4 border" style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium" style={{ color: 'var(--text-secondary)' }}>CPU</h3>
            <p className="text-2xl font-mono mt-1" style={{ color: 'var(--text-primary)' }}>
              {attribution.cpu.totalPct.toFixed(1)}%
            </p>
            <p className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
              of {attribution.cpu.cores} cores
            </p>
          </div>
          <ProgressRing
            pct={attribution.cpu.totalPct}
            size={56}
            stroke={5}
            color={OVERVIEW_COLORS.queries}
          />
        </div>
        <div className="mt-3 pt-3 border-t grid grid-cols-2 gap-2 text-xs" style={{ borderColor: 'var(--border-secondary)' }}>
          <div>
            <span style={{ color: 'var(--text-muted)' }}>Queries:</span>
            <span className="ml-1" style={{ color: 'var(--text-primary)' }}>{attribution.cpu.breakdown.queries.toFixed(1)}%</span>
          </div>
          <div>
            <span style={{ color: 'var(--text-muted)' }}>Merges:</span>
            <span className="ml-1" style={{ color: 'var(--text-primary)' }}>{attribution.cpu.breakdown.merges.toFixed(1)}%</span>
          </div>
        </div>
      </div>

      {/* Memory Card */}
      <div className="rounded-lg p-4 border" style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium" style={{ color: 'var(--text-secondary)' }}>Memory</h3>
            <p className="text-2xl font-mono mt-1" style={{ color: 'var(--text-primary)' }}>
              {formatBytes(attribution.memory.totalRSS)}
            </p>
            <p className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
              of {formatBytes(attribution.memory.totalRAM)}
            </p>
          </div>
          <ProgressRing
            pct={memoryPct}
            size={56}
            stroke={5}
            color={OVERVIEW_COLORS.queryMem}
          />
        </div>
        <div className="mt-3 pt-3 border-t grid grid-cols-2 gap-2 text-xs" style={{ borderColor: 'var(--border-secondary)' }}>
          <div>
            <span style={{ color: 'var(--text-muted)' }}>Tracked:</span>
            <span className="ml-1" style={{ color: 'var(--text-primary)' }}>{formatBytes(attribution.memory.tracked)}</span>
          </div>
          <div>
            <span style={{ color: 'var(--text-muted)' }}>Caches:</span>
            <span className="ml-1" style={{ color: 'var(--text-primary)' }}>
              {formatBytes(attribution.memory.breakdown.markCache + attribution.memory.breakdown.uncompressedCache)}
            </span>
          </div>
        </div>
      </div>

      {/* Replication Card */}
      <div className="rounded-lg p-4 border" style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium" style={{ color: 'var(--text-secondary)' }}>Replication</h3>
            <p className="text-2xl font-mono mt-1" style={{ color: 'var(--text-primary)' }}>
              {replication.healthyTables}/{replication.totalTables}
            </p>
            <p className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
              tables healthy
            </p>
          </div>
          <ProgressRing
            pct={replicationHealthy}
            size={56}
            stroke={5}
            color={replication.readonlyReplicas > 0 ? OVERVIEW_COLORS.crit : OVERVIEW_COLORS.ok}
          />
        </div>
        <div className="mt-3 pt-3 border-t grid grid-cols-2 gap-2 text-xs" style={{ borderColor: 'var(--border-secondary)' }}>
          <div>
            <span style={{ color: 'var(--text-muted)' }}>Max Delay:</span>
            <span className="ml-1" style={{ color: 'var(--text-primary)' }}>{replication.maxDelay}s</span>
          </div>
          <div>
            <span style={{ color: 'var(--text-muted)' }}>Queue:</span>
            <span className="ml-1" style={{ color: 'var(--text-primary)' }}>{replication.queueSize}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default SummaryCards;
