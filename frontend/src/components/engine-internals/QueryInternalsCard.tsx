/**
 * QueryInternalsCard - Per-query deep dive with memory breakdown and ProfileEvents
 */

import type { QueryInternals } from '@tracehouse/core';
import {
  calculateParallelismFactor,
  calculateIndexPruning,
} from '@tracehouse/core';
import { formatBytes } from '../../utils/formatters';

interface QueryInternalsCardProps {
  query: QueryInternals;
  className?: string;
}

function formatMicroseconds(us: number): string {
  if (us < 1000) return `${us}µs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function truncateQuery(query: string, maxLength: number = 100): string {
  const cleaned = query.replace(/\s+/g, ' ').trim();
  if (cleaned.length <= maxLength) return cleaned;
  return cleaned.substring(0, maxLength) + '...';
}

export function QueryInternalsCard({ query, className = '' }: QueryInternalsCardProps) {
  const { profileEvents } = query;
  
  // Calculate derived metrics
  const parallelism = calculateParallelismFactor(
    profileEvents.userTimeMicroseconds,
    profileEvents.systemTimeMicroseconds,
    profileEvents.realTimeMicroseconds
  );
  
  const indexPruning = calculateIndexPruning(
    profileEvents.totalMarks,
    profileEvents.selectedMarks
  );
  
  const markCacheHitRate = (profileEvents.markCacheHits + profileEvents.markCacheMisses) > 0
    ? (profileEvents.markCacheHits / (profileEvents.markCacheHits + profileEvents.markCacheMisses)) * 100
    : 0;
  
  const ioWaitRatio = profileEvents.realTimeMicroseconds > 0
    ? (profileEvents.osIOWaitMicroseconds / profileEvents.realTimeMicroseconds) * 100
    : 0;

  return (
    <div 
      className={`rounded-lg border ${className}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      {/* Header */}
      <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--border-primary)' }}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span
              className="px-1.5 py-0.5 text-xs font-medium rounded"
              style={{
                backgroundColor: 'rgba(59, 130, 246, 0.2)',
                color: 'var(--accent-blue)',
              }}
            >
              {query.kind}
            </span>
            <span className="text-sm" style={{ color: 'var(--text-secondary)' }}>{query.user}</span>
          </div>
          <span className="text-sm font-mono" style={{ color: 'var(--text-secondary)' }}>
            {formatElapsed(query.elapsed)}
          </span>
        </div>
        <p className="text-xs font-mono mt-2 truncate" style={{ color: 'var(--text-tertiary)' }}>
          {truncateQuery(query.query)}
        </p>
      </div>

      <div className="p-4 space-y-4">
        {/* Memory */}
        <div>
          <h4 className="text-xs font-medium uppercase tracking-wider mb-2" style={{ color: 'var(--text-tertiary)' }}>
            Memory Usage
          </h4>
          <p className="text-lg font-mono" style={{ color: 'var(--text-primary)' }}>
            {formatBytes(query.totalMemory)}
          </p>
        </div>

        {/* ProfileEvents Grid */}
        <div>
          <h4 className="text-xs font-medium uppercase tracking-wider mb-2" style={{ color: 'var(--text-tertiary)' }}>
            Performance Metrics
          </h4>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            <div className="rounded p-2" style={{ background: 'var(--bg-tertiary)' }}>
              <p className="text-xs" style={{ color: 'var(--text-tertiary)' }}>CPU Time</p>
              <p className="text-sm font-mono" style={{ color: 'var(--text-primary)' }}>
                {formatMicroseconds(profileEvents.userTimeMicroseconds + profileEvents.systemTimeMicroseconds)}
              </p>
            </div>
            <div className="rounded p-2" style={{ background: 'var(--bg-tertiary)' }}>
              <p className="text-xs" style={{ color: 'var(--text-tertiary)' }}>Parallelism</p>
              <p className="text-sm font-mono" style={{ color: 'var(--text-primary)' }}>
                {parallelism.toFixed(2)}x
              </p>
            </div>
            <div className="rounded p-2" style={{ background: 'var(--bg-tertiary)' }}>
              <p className="text-xs" style={{ color: 'var(--text-tertiary)' }}>IO Wait</p>
              <p
                className="text-sm font-mono"
                style={{ color: ioWaitRatio > 50 ? 'var(--color-warning)' : 'var(--text-primary)' }}
              >
                {ioWaitRatio.toFixed(1)}%
              </p>
            </div>
            <div className="rounded p-2" style={{ background: 'var(--bg-tertiary)' }}>
              <p className="text-xs" style={{ color: 'var(--text-tertiary)' }}>Data Scanned</p>
              <p className="text-sm font-mono" style={{ color: 'var(--text-primary)' }}>
                {formatBytes(profileEvents.readCompressedBytes)}
              </p>
            </div>
            <div className="rounded p-2" style={{ background: 'var(--bg-tertiary)' }}>
              <p className="text-xs" style={{ color: 'var(--text-tertiary)' }}>Index Pruning</p>
              <p
                className="text-sm font-mono"
                style={{ color: indexPruning > 90 ? 'var(--color-success)' : indexPruning > 50 ? 'var(--color-warning)' : 'var(--color-error)' }}
              >
                {indexPruning.toFixed(1)}%
              </p>
            </div>
            <div className="rounded p-2" style={{ background: 'var(--bg-tertiary)' }}>
              <p className="text-xs" style={{ color: 'var(--text-tertiary)' }}>Mark Cache Hit</p>
              <p
                className="text-sm font-mono"
                style={{ color: markCacheHitRate > 90 ? 'var(--color-success)' : markCacheHitRate > 50 ? 'var(--color-warning)' : 'var(--color-error)' }}
              >
                {markCacheHitRate.toFixed(1)}%
              </p>
            </div>
          </div>
        </div>

        {/* Thread Info */}
        <div className="flex items-center gap-4 text-xs" style={{ color: 'var(--text-tertiary)' }}>
          <span>Threads: <span style={{ color: 'var(--text-secondary)' }}>{query.threads}</span></span>
          <span>Parts: <span style={{ color: 'var(--text-secondary)' }}>{profileEvents.selectedParts}</span></span>
          <span>Marks: <span style={{ color: 'var(--text-secondary)' }}>{profileEvents.selectedMarks}/{profileEvents.totalMarks}</span></span>
        </div>
      </div>
    </div>
  );
}

export default QueryInternalsCard;
