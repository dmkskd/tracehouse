/**
 * ResourceAttributionBar - Three resource bars displayed side by side
 * Shows CPU, Memory, and IO breakdown in separate columns
 */

import { OVERVIEW_COLORS, RESOURCE_COLORS } from '../../styles/overviewColors';
import { formatBytes, formatBytesPerSec } from '../../utils/formatters';
import type { ResourceAttribution } from '@tracehouse/core';

// Segment data for the bar
export interface BarSegment {
  label: string;
  value: number;
  color: string;
}

interface ResourceAttributionBarProps {
  attribution: ResourceAttribution;
  selectedResource?: 'cpu' | 'memory' | 'io'; // kept for compatibility but not used
  onResourceChange?: (resource: 'cpu' | 'memory' | 'io') => void; // kept for compatibility
  height?: number;
  className?: string;
  showOnly?: 'cpu' | 'memory' | 'io'; // Show only one resource type
  compact?: boolean; // Slim inline strip — no card chrome, no legends
}

// Minimum percentage to show a segment (hide if < 0.5%)
const MIN_SEGMENT_PCT = 0.5;

/**
 * Get segments for CPU breakdown
 */
function getCpuSegments(breakdown: ResourceAttribution['cpu']['breakdown']): BarSegment[] {
  return [
    { label: 'Queries', value: breakdown.queries, color: RESOURCE_COLORS.cpu.queries },
    { label: 'Merges', value: breakdown.merges, color: RESOURCE_COLORS.cpu.merges },
    { label: 'Mutations', value: breakdown.mutations, color: RESOURCE_COLORS.cpu.mutations },
    { label: 'Other', value: breakdown.other, color: RESOURCE_COLORS.cpu.other },
  ];
}

/**
 * Get segments for Memory breakdown
 */
function getMemorySegments(
  breakdown: ResourceAttribution['memory']['breakdown'],
  totalRSS: number
): BarSegment[] {
  const toPct = (bytes: number) => totalRSS > 0 ? (bytes / totalRSS) * 100 : 0;
  
  return [
    { label: 'Queries', value: toPct(breakdown.queries), color: RESOURCE_COLORS.memory.queries },
    { label: 'Merges', value: toPct(breakdown.merges), color: RESOURCE_COLORS.memory.merges },
    { label: 'Mark Cache', value: toPct(breakdown.markCache), color: RESOURCE_COLORS.memory.markCache },
    { label: 'Uncomp Cache', value: toPct(breakdown.uncompressedCache), color: RESOURCE_COLORS.memory.uncompressedCache },
    { label: 'Primary Keys', value: toPct(breakdown.primaryKeys), color: RESOURCE_COLORS.memory.primaryKeys },
    { label: 'Dictionaries', value: toPct(breakdown.dictionaries), color: RESOURCE_COLORS.memory.dictionaries },
    { label: 'Other', value: toPct(breakdown.other), color: RESOURCE_COLORS.memory.other },
  ];
}

/**
 * Get segments for IO breakdown
 */
function getIoSegments(
  breakdown: ResourceAttribution['io']['breakdown'],
  totalRead: number,
  totalWrite: number
): BarSegment[] {
  const total = totalRead + totalWrite;
  // Use the larger of OS-level total or per-operation sum as denominator,
  // so percentages never exceed 100%
  const opSum = breakdown.queryRead + breakdown.queryWrite
    + breakdown.mergeRead + breakdown.mergeWrite
    + breakdown.replicationRead + breakdown.replicationWrite;
  const denom = Math.max(total, opSum);
  const toPct = (bytes: number) => denom > 0 ? (bytes / denom) * 100 : 0;

  return [
    { label: 'Query Read', value: toPct(breakdown.queryRead), color: RESOURCE_COLORS.io.queryRead },
    { label: 'Query Write', value: toPct(breakdown.queryWrite), color: RESOURCE_COLORS.io.queryWrite },
    { label: 'Merge Read', value: toPct(breakdown.mergeRead), color: RESOURCE_COLORS.io.mergeRead },
    { label: 'Merge Write', value: toPct(breakdown.mergeWrite), color: RESOURCE_COLORS.io.mergeWrite },
    { label: 'Repl Read', value: toPct(breakdown.replicationRead), color: RESOURCE_COLORS.io.replicationRead },
    { label: 'Repl Write', value: toPct(breakdown.replicationWrite), color: RESOURCE_COLORS.io.replicationWrite },
  ];
}

/**
 * Filter segments to hide those below minimum percentage
 */
function filterSmallSegments(segments: BarSegment[]): BarSegment[] {
  return segments.filter(s => s.value >= MIN_SEGMENT_PCT);
}

/**
 * Single resource bar component
 */
function ResourceBar({
  title,
  segments,
  totalLabel,
  height = 24,
  useRawPct = false,
}: {
  title: string;
  segments: BarSegment[];
  totalLabel: string;
  height?: number;
  useRawPct?: boolean;
}) {
  const visibleSegments = filterSmallSegments(segments);
  const totalValue = segments.reduce((sum, s) => sum + s.value, 0);

  return (
    <div className="flex-1 min-w-0">
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-medium" style={{ color: 'var(--text-secondary)' }}>
          {title}
        </span>
        <span className="text-xs font-mono" style={{ color: 'var(--text-muted)' }}>
          {totalLabel}
        </span>
      </div>

      {/* Stacked bar */}
      <div
        className="w-full rounded overflow-hidden flex"
        style={{ height, backgroundColor: useRawPct ? 'var(--bg-tertiary)' : OVERVIEW_COLORS.bgDeep }}
      >
        {visibleSegments.map((segment, index) => {
          // useRawPct: segment.value is already a percentage of the full bar (e.g. CPU % of total cores)
          // otherwise: normalize segments to fill the bar proportionally
          const widthPct = useRawPct
            ? Math.min(segment.value, 100)
            : (totalValue > 0 ? (segment.value / totalValue) * 100 : 0);
          return (
            <div
              key={`${segment.label}-${index}`}
              className="h-full transition-all duration-300 relative group"
              style={{
                width: `${widthPct}%`,
                backgroundColor: segment.color,
              }}
            >
              {/* Tooltip on hover */}
              <div 
                className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2 py-1 rounded text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10" 
                style={{ background: 'var(--bg-secondary)', color: 'var(--text-primary)' }}
              >
                {segment.label}: {segment.value.toFixed(1)}%
              </div>
            </div>
          );
        })}
      </div>

      {/* Legend - compact */}
      <div className="flex flex-wrap gap-x-3 gap-y-1 mt-2">
        {visibleSegments.map((segment, index) => (
          <div key={`legend-${segment.label}-${index}`} className="flex items-center gap-1">
            <div
              className="w-2 h-2 rounded-sm"
              style={{ backgroundColor: segment.color }}
            />
            <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
              {segment.label}
              <span className="font-mono" style={{ marginLeft: 4 }}>{segment.value.toFixed(1)}%</span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

/**
 * Compact inline bar — thin bar with tooltip dots, no legend
 */
function CompactBar({
  title,
  segments,
  totalLabel,
  height = 14,
  useRawPct = false,
}: {
  title: string;
  segments: BarSegment[];
  totalLabel: string;
  height?: number;
  useRawPct?: boolean;
}) {
  const visibleSegments = filterSmallSegments(segments);
  const totalValue = segments.reduce((sum, s) => sum + s.value, 0);

  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-secondary)' }}>{title}</span>
        <span style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-muted)' }}>{totalLabel}</span>
      </div>
      <div
        style={{
          height,
          borderRadius: height / 2,
          overflow: 'hidden',
          display: 'flex',
          backgroundColor: useRawPct ? 'var(--bg-tertiary)' : OVERVIEW_COLORS.bgDeep,
        }}
      >
        {visibleSegments.map((segment, index) => {
          const widthPct = useRawPct
            ? Math.min(segment.value, 100)
            : (totalValue > 0 ? (segment.value / totalValue) * 100 : 0);
          return (
            <div
              key={`${segment.label}-${index}`}
              className="relative group"
              style={{
                width: `${widthPct}%`,
                height: '100%',
                backgroundColor: segment.color,
                transition: 'width 0.3s ease',
              }}
            >
              <div
                className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2 py-1 rounded text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10"
                style={{ background: 'var(--bg-secondary)', color: 'var(--text-primary)', fontSize: 10 }}
              >
                {segment.label}: {segment.value.toFixed(1)}%
              </div>
            </div>
          );
        })}
      </div>
      {/* Inline dot legend — only segments ≥ 2% */}
      <div style={{ display: 'flex', gap: 8, marginTop: 3, overflow: 'hidden' }}>
        {visibleSegments.filter(s => s.value >= 2).map((segment, index) => (
          <span key={`dot-${segment.label}-${index}`} style={{ display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: 9, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
            <span style={{ width: 5, height: 5, borderRadius: '50%', background: segment.color, flexShrink: 0 }} />
            {segment.label} {segment.value.toFixed(1)}%
          </span>
        ))}
      </div>
    </div>
  );
}

export function ResourceAttributionBar({
  attribution,
  height = 24,
  className = '',
  showOnly,
  compact,
}: ResourceAttributionBarProps) {
  // CPU data
  const cpuSegments = getCpuSegments(attribution.cpu.breakdown);
  const cpuLabel = `${attribution.cpu.totalPct.toFixed(1)}% of ${attribution.cpu.cores} cores`;

  // Memory data
  const memorySegments = getMemorySegments(attribution.memory.breakdown, attribution.memory.totalRSS);
  const memPct = attribution.memory.totalRAM > 0 
    ? (attribution.memory.totalRSS / attribution.memory.totalRAM) * 100 
    : 0;
  const memoryLabel = `${formatBytes(attribution.memory.totalRSS)} (${memPct.toFixed(1)}%)`;

  // IO data
  const ioSegments = getIoSegments(
    attribution.io.breakdown,
    attribution.io.readBytesPerSec,
    attribution.io.writeBytesPerSec
  );
  const ioLabel = `R: ${formatBytesPerSec(attribution.io.readBytesPerSec)} W: ${formatBytesPerSec(attribution.io.writeBytesPerSec)}`;

  // Compact strip: all three bars in a tight row, no card chrome
  if (compact) {
    return (
      <div className={className} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>
        <CompactBar title="CPU" segments={cpuSegments} totalLabel={cpuLabel} height={height} useRawPct />
        <CompactBar title="Memory" segments={memorySegments} totalLabel={memoryLabel} height={height} />
        <CompactBar title="I/O" segments={ioSegments} totalLabel={ioLabel} height={height} />
      </div>
    );
  }

  // If showOnly is specified, render only that resource
  if (showOnly === 'cpu') {
    return (
      <div className={className}>
        <ResourceBar
          title="CPU"
          segments={cpuSegments}
          totalLabel={cpuLabel}
          height={height}
          useRawPct
        />
      </div>
    );
  }

  if (showOnly === 'memory') {
    return (
      <div className={className}>
        <ResourceBar
          title="Memory"
          segments={memorySegments}
          totalLabel={memoryLabel}
          height={height}
        />
      </div>
    );
  }

  if (showOnly === 'io') {
    return (
      <div className={className}>
        <ResourceBar
          title="I/O"
          segments={ioSegments}
          totalLabel={ioLabel}
          height={height}
        />
      </div>
    );
  }

  // Default: show all three
  return (
    <div className={`grid grid-cols-1 md:grid-cols-3 gap-6 ${className}`}>
      <ResourceBar
        title="CPU"
        segments={cpuSegments}
        totalLabel={cpuLabel}
        height={height}
        useRawPct
      />
      <ResourceBar
        title="Memory"
        segments={memorySegments}
        totalLabel={memoryLabel}
        height={height}
      />
      <ResourceBar
        title="I/O"
        segments={ioSegments}
        totalLabel={ioLabel}
        height={height}
      />
    </div>
  );
}

export default ResourceAttributionBar;
