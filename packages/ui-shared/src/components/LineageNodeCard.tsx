import React from 'react';
import type { LineageNode } from '@tracehouse/core';

export interface LineageNodeCardProps {
  node: LineageNode;
  className?: string;
  style?: React.CSSProperties;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export function LineageNodeCard({ node, className, style }: LineageNodeCardProps) {
  const mergeEvent = node.merge_event;

  return (
    <div className={className} style={style} role="article" aria-label={`Part ${node.part_name}`}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <span data-testid="part-name">{node.part_name}</span>
        <span data-testid="level">L{node.level}</span>
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span data-testid="size">{formatBytes(node.size_in_bytes)}</span>
        <span data-testid="rows">{node.rows.toLocaleString()} rows</span>
      </div>
      {mergeEvent && (
        <div data-testid="merge-info" style={{ marginTop: 4 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span data-testid="merge-reason">{mergeEvent.merge_reason}</span>
            <span data-testid="merge-duration">{formatDuration(mergeEvent.duration_ms)}</span>
          </div>
          <div data-testid="merge-sources">
            {mergeEvent.merged_from.length} source part{mergeEvent.merged_from.length !== 1 ? 's' : ''}
          </div>
        </div>
      )}
    </div>
  );
}
