import React from 'react';
import type { MergeInfo } from '@tracehouse/core';

export interface MergeCardProps {
  merge: MergeInfo;
  className?: string;
  style?: React.CSSProperties;
  renderProgress?: (progress: number) => React.ReactNode;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}m ${secs.toFixed(0)}s`;
}

function defaultProgressBar(progress: number): React.ReactNode {
  return (
    <div
      style={{ position: 'relative', height: 6, borderRadius: 3, overflow: 'hidden', opacity: 0.3, backgroundColor: 'currentColor' }}
      role="progressbar"
      aria-valuenow={Math.round(progress * 100)}
      aria-valuemin={0}
      aria-valuemax={100}
    >
      <div
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          height: '100%',
          width: `${Math.min(progress * 100, 100)}%`,
          backgroundColor: 'currentColor',
          opacity: 1,
          borderRadius: 3,
        }}
      />
    </div>
  );
}

export function MergeCard({ merge, className, style, renderProgress }: MergeCardProps) {
  const throughput = merge.elapsed > 0 ? merge.rows_written / merge.elapsed : 0;

  return (
    <div className={className} style={style} role="article" aria-label={`Merge ${merge.result_part_name}`}>
      {/* Result part name */}
      <div data-testid="result-part" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <span>{merge.result_part_name}</span>
        <span data-testid="progress-pct">{(merge.progress * 100).toFixed(1)}%</span>
      </div>

      {/* Progress bar */}
      <div data-testid="progress-bar" style={{ margin: '4px 0' }}>
        {renderProgress ? renderProgress(merge.progress) : defaultProgressBar(merge.progress)}
      </div>

      {/* Elapsed and source parts */}
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span data-testid="elapsed">{formatElapsed(merge.elapsed)}</span>
        <span data-testid="source-parts">{merge.num_parts} source parts</span>
      </div>

      {/* Throughput and memory */}
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span data-testid="throughput">{Math.round(throughput).toLocaleString()} rows/s</span>
        <span data-testid="memory">{formatBytes(merge.memory_usage)}</span>
      </div>
    </div>
  );
}
