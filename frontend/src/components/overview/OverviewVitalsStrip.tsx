import React, { useMemo } from 'react';
import type { OverviewData, ServerMetrics } from '@tracehouse/core';
import { normalizeRadarValue, radarShapeLayout, shortRadarLabel } from '../analytics/radarModel';
import { OVERVIEW_COLORS, RESOURCE_COLORS } from '../../styles/overviewColors';
import { formatBytes, formatBytesPerSec } from '../../utils/formatters';

interface OverviewVitalsStripProps {
  data: OverviewData | null;
  metrics: ServerMetrics | null;
  cpuHistory: number[];
  memoryHistory: number[];
  diskReadHistory: number[];
  diskWriteHistory: number[];
  isLoading?: boolean;
}

const ioRange = { low: '1Ki', high: '1Gi' };
const queryRange = { low: '1', high: '1000' };
const mergeRange = { low: '1', high: '128' };
const CARD_BORDER = 'rgba(148, 163, 184, 0.18)';

function pct(value: number): string {
  return `${value.toFixed(value >= 10 ? 1 : 2)}%`;
}

function clampPct(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function VitalsCard({
  children,
  style,
}: {
  children: React.ReactNode;
  style?: React.CSSProperties;
}) {
  return (
    <div
      style={{
        minHeight: 132,
        borderRadius: 8,
        border: `1px solid ${CARD_BORDER}`,
        background: 'var(--bg-card)',
        padding: 14,
        overflow: 'hidden',
        outline: 'none',
        boxShadow: 'none',
        ...style,
      }}
    >
      {children}
    </div>
  );
}

function PressureRadar({
  values,
  labels,
  rawValues,
  color,
  title,
}: {
  values: number[];
  labels: string[];
  rawValues: string[];
  color: string;
  title: string;
}) {
  const layout = radarShapeLayout(values, labels, 'chart');
  const viewBox = '-10 -6 120 112';

  return (
    <svg
      viewBox={viewBox}
      preserveAspectRatio="xMidYMid meet"
      role="img"
      aria-label={title}
      style={{
        width: '100%',
        height: '100%',
        minHeight: 112,
        maxHeight: 132,
        display: 'block',
        overflow: 'visible',
        userSelect: 'none',
      }}
    >
      <title>{title}</title>
      <circle cx={layout.center.x} cy={layout.center.y} r={layout.radius + 1} fill="var(--bg-secondary)" stroke={CARD_BORDER} />
      {layout.spokes.map((point, index) => (
        <line
          key={index}
          x1={layout.center.x}
          y1={layout.center.y}
          x2={point.x}
          y2={point.y}
          stroke={CARD_BORDER}
          strokeWidth="1"
        />
      ))}
      <polygon points={layout.polygonPoints} fill={`${color}2b`} stroke={color} strokeWidth="2.2" />
      {layout.labels.map((label, index) => (
        <g key={`${label.label}-${index}`}>
          <title>{rawValues[index]}</title>
          <text
            x={label.x}
            y={label.y}
            textAnchor={label.anchor}
            dominantBaseline={label.baseline}
            fill="var(--text-muted)"
            fontSize="7.5"
            fontWeight="750"
            letterSpacing="0"
            stroke="var(--bg-card)"
            strokeWidth="2.8"
            paintOrder="stroke"
          >
            <tspan x={label.x} dy="0">{`${shortRadarLabel(labels[index])} ${values[index]?.toFixed(2) ?? '0.00'}`}</tspan>
            {rawValues[index] ? <tspan x={label.x} dy="8" fontSize="6.5" fontWeight="650">{rawValues[index]}</tspan> : null}
          </text>
        </g>
      ))}
    </svg>
  );
}

function Header({ label, right }: { label: string; right?: React.ReactNode }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10, color: 'var(--text-muted)', fontSize: 12, fontWeight: 700 }}>
      <span>{label}</span>
      {right}
    </div>
  );
}

function LoadingLines() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 16 }}>
      <div style={{ height: 10, width: '100%', borderRadius: 999, background: 'var(--bg-tertiary)' }} />
      <div style={{ height: 10, width: '64%', borderRadius: 999, background: 'var(--bg-tertiary)', opacity: 0.75 }} />
    </div>
  );
}

function LoadingRadar() {
  return (
    <div
      style={{
        width: 122,
        height: 122,
        borderRadius: '50%',
        border: '1px solid var(--border-primary)',
        display: 'grid',
        placeItems: 'center',
        opacity: 0.55,
      }}
    >
      <div
        style={{
          width: 72,
          height: 72,
          borderRadius: '50%',
          border: '1px solid var(--border-secondary)',
          background: 'var(--bg-tertiary)',
        }}
      />
    </div>
  );
}

function HeatStrip({ values, color }: { values: number[]; color: string }) {
  const samples = values.slice(-30);
  if (samples.length < 2) return <LoadingLines />;
  const max = Math.max(...samples, 1);
  return (
    <div style={{ display: 'flex', gap: 3, height: 26, alignItems: 'end', marginTop: 12 }}>
      {samples.map((value, index) => {
        const intensity = Math.max(0.12, Math.min(1, value / max));
        return (
          <span
            key={`${index}-${value}`}
            title={pct(value)}
            style={{
              flex: 1,
              minWidth: 3,
              height: 6 + intensity * 18,
              borderRadius: 2,
              background: color,
              opacity: 0.28 + intensity * 0.62,
            }}
          />
        );
      })}
    </div>
  );
}

function MemoryTrendBand({
  valuePct,
  peakPct,
  values,
  color,
}: {
  valuePct: number;
  peakPct: number;
  values: number[];
  color: string;
}) {
  const value = clampPct(valuePct);
  const peak = clampPct(Math.max(peakPct, valuePct));
  const samples = values.slice(-30).map(clampPct);
  const trend = samples.length >= 2 ? samples : [value, value];
  const width = 160;
  const height = 28;
  const linePath = buildSparkPath(trend, 0, 100, width, height);
  const areaPath = linePath ? `${linePath} L ${width} ${height} L 0 ${height} Z` : '';
  const peakY = height - (peak / 100) * (height - 4) - 2;

  return (
    <div style={{ marginTop: 10 }}>
      <div
        title={`${pct(value)} used`}
        style={{
          height: 8,
          borderRadius: 999,
          overflow: 'hidden',
          background: 'var(--bg-tertiary)',
        }}
      >
        <div style={{ width: `${value}%`, height: '100%', borderRadius: 999, background: color }} />
      </div>

      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" style={{ display: 'block', width: '100%', height: 28, marginTop: 6 }}>
        {areaPath && <path d={areaPath} fill={color} opacity="0.16" />}
        <line x1="0" y1={peakY} x2={width} y2={peakY} stroke={OVERVIEW_COLORS.merges} strokeWidth="1" strokeDasharray="3 4" opacity="0.65" vectorEffect="non-scaling-stroke" />
        {linePath && (
          <path
            d={linePath}
            fill="none"
            stroke={color}
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
            vectorEffect="non-scaling-stroke"
          />
        )}
        {trend.length > 0 && (
          <circle
            cx={width}
            cy={height - (trend[trend.length - 1] / 100) * (height - 4) - 2}
            r="2"
            fill={color}
          />
        )}
      </svg>

    </div>
  );
}

function buildSparkPath(values: number[], min: number, max: number, width = 160, height = 36): string {
  if (values.length < 2) return '';
  const span = Math.max(1, max - min);
  return values.map((value, index) => {
    const x = (index / (values.length - 1)) * width;
    const y = height - ((value - min) / span) * (height - 4) - 2;
    return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(' ');
}

function DiskLines({ read, write }: { read: number[]; write: number[] }) {
  const readValues = read.slice(-30);
  const writeValues = write.slice(-30);
  const values = [...readValues, ...writeValues];
  if (values.length < 2) return <LoadingLines />;
  const min = Math.min(...values);
  const max = Math.max(...values, 1);
  const readPath = buildSparkPath(readValues, min, max);
  const writePath = buildSparkPath(writeValues, min, max);

  return (
    <svg viewBox="0 0 160 36" preserveAspectRatio="none" style={{ display: 'block', width: '100%', height: 36, marginTop: 10 }}>
      <path d={readPath} fill="none" stroke={OVERVIEW_COLORS.ok} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
      <path d={writePath} fill="none" stroke={OVERVIEW_COLORS.merges} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

function SegmentedBar({ segments }: { segments: Array<{ label: string; value: number; color: string }> }) {
  const visible = segments.filter(segment => segment.value >= 0.5);
  const total = segments.reduce((sum, segment) => sum + Math.max(0, segment.value), 0);
  return (
    <div style={{ height: 8, borderRadius: 999, overflow: 'hidden', display: 'flex', background: 'var(--bg-tertiary)', marginTop: 12 }}>
      {visible.map(segment => (
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

export function OverviewVitalsStrip({
  data,
  metrics,
  cpuHistory,
  memoryHistory,
  diskReadHistory,
  diskWriteHistory,
  isLoading = false,
}: OverviewVitalsStripProps) {
  const ra = data?.resourceAttribution;
  const waitingForLiveData = isLoading && !data;

  const cpuPct = ra?.cpu.totalPct ?? metrics?.cpu_usage ?? 0;
  const cores = ra?.cpu.cores ?? data?.serverInfo.cores ?? 0;
  const memoryUsed = ra?.memory.totalRSS ?? metrics?.memory_used ?? 0;
  const memoryTotal = ra?.memory.totalRAM ?? metrics?.memory_total ?? 0;
  const memoryPct = memoryTotal > 0 ? (memoryUsed / memoryTotal) * 100 : 0;
  const readBps = ra?.io.readBytesPerSec ?? diskReadHistory.at(-1) ?? 0;
  const writeBps = ra?.io.writeBytesPerSec ?? diskWriteHistory.at(-1) ?? 0;
  const ioBps = readBps + writeBps;
  const runningQueries = data?.queryConcurrency.running ?? 0;
  const activeMerges = data?.activeMerges.length ?? 0;
  const peakMemory = Math.max(memoryPct, ...memoryHistory);

  const radar = useMemo(() => {
    const values = [
      clampPct(memoryPct) / 100,
      clampPct(cpuPct) / 100,
      normalizeRadarValue(ioBps, ioRange, 'log'),
      normalizeRadarValue(activeMerges, mergeRange, 'log'),
      normalizeRadarValue(runningQueries, queryRange, 'log'),
    ];
    return {
      values,
      labels: ['memory', 'cpu', 'io', 'merges', 'queries'],
      color: OVERVIEW_COLORS.merges,
    };
  }, [activeMerges, cpuPct, ioBps, memoryPct, runningQueries]);

  const pressureTooltip = [
    `Memory ${pct(memoryPct)}${memoryTotal > 0 ? ` (${formatBytes(memoryUsed)} / ${formatBytes(memoryTotal)})` : ''}`,
    `CPU ${pct(cpuPct)}${cores > 0 ? ` (${cores} cores)` : ''}`,
    `I/O ${formatBytesPerSec(ioBps)} (read ${formatBytesPerSec(readBps)}, write ${formatBytesPerSec(writeBps)})`,
    `Merges ${activeMerges}`,
    `Queries ${runningQueries}`,
  ].join('\n');
  const pressureVisibleValues = [
    pct(memoryPct),
    pct(cpuPct),
    formatBytesPerSec(ioBps),
    `${activeMerges}`,
    `${runningQueries}`,
  ];

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 12 }}>
      <VitalsCard style={{ padding: '12px 14px', display: 'flex', flexDirection: 'column' }}>
        <Header
          label="Resource Radar"
          right={waitingForLiveData ? <span style={{ color: 'var(--text-muted)', fontFamily: 'monospace' }}>polling</span> : undefined}
        />
        <div
          title={pressureTooltip}
          style={{
            flex: 1,
            minHeight: 100,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            marginTop: 2,
          }}
        >
          {waitingForLiveData ? (
            <LoadingRadar />
          ) : (
            <PressureRadar
              values={radar.values}
              labels={radar.labels}
              rawValues={pressureVisibleValues}
              color={radar.color}
              title={pressureTooltip}
            />
          )}
        </div>
      </VitalsCard>

      <VitalsCard>
        <Header label="CPU" right={cores > 0 ? <span style={{ fontFamily: 'monospace' }}>{cores} cores</span> : undefined} />
        <div style={{ marginTop: 8, fontSize: 27, lineHeight: 1, fontWeight: 750, fontFamily: 'monospace', color: 'var(--text-primary)' }}>
          {waitingForLiveData && !metrics ? 'Loading...' : pct(cpuPct)}
        </div>
        <div style={{ marginTop: 5, fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {ra ? `queries ${ra.cpu.breakdown.queries.toFixed(1)}% · merges ${ra.cpu.breakdown.merges.toFixed(1)}%` : 'waiting for attribution'}
        </div>
        {ra && (
          <SegmentedBar
            segments={[
              { label: 'Queries', value: ra.cpu.breakdown.queries, color: RESOURCE_COLORS.cpu.queries },
              { label: 'Merges', value: ra.cpu.breakdown.merges, color: RESOURCE_COLORS.cpu.merges },
              { label: 'Mutations', value: ra.cpu.breakdown.mutations, color: RESOURCE_COLORS.cpu.mutations },
              { label: 'Other', value: ra.cpu.breakdown.other, color: RESOURCE_COLORS.cpu.other },
            ]}
          />
        )}
        <HeatStrip values={cpuHistory} color={OVERVIEW_COLORS.queries} />
      </VitalsCard>

      <VitalsCard>
        <Header label="Memory" right={peakMemory > 0 ? <span style={{ fontFamily: 'monospace' }}>peak {pct(peakMemory)}</span> : undefined} />
        <div style={{ marginTop: 8, fontSize: 27, lineHeight: 1, fontWeight: 750, fontFamily: 'monospace', color: 'var(--text-primary)' }}>
          {waitingForLiveData && !metrics ? 'Loading...' : formatBytes(memoryUsed)}
        </div>
        <div style={{ marginTop: 5, fontSize: 11, color: 'var(--text-muted)' }}>
          {memoryTotal > 0 ? `${pct(memoryPct)} of ${formatBytes(memoryTotal)}` : 'waiting for memory totals'}
        </div>
        <MemoryTrendBand valuePct={memoryPct} peakPct={peakMemory} values={memoryHistory} color={OVERVIEW_COLORS.replication} />
      </VitalsCard>

      <VitalsCard>
        <Header label="Disk I/O" right={<span style={{ color: 'var(--text-muted)' }}>read + write</span>} />
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginTop: 9 }}>
          <div>
            <div style={{ fontSize: 11, color: OVERVIEW_COLORS.ok, fontWeight: 700 }}>READ</div>
            <div style={{ marginTop: 2, fontSize: 20, lineHeight: 1, fontFamily: 'monospace', color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>
              {waitingForLiveData && !metrics ? '...' : formatBytesPerSec(readBps)}
            </div>
          </div>
          <div>
            <div style={{ fontSize: 11, color: OVERVIEW_COLORS.merges, fontWeight: 700 }}>WRITE</div>
            <div style={{ marginTop: 2, fontSize: 20, lineHeight: 1, fontFamily: 'monospace', color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>
              {waitingForLiveData && !metrics ? '...' : formatBytesPerSec(writeBps)}
            </div>
          </div>
        </div>
        <DiskLines read={diskReadHistory} write={diskWriteHistory} />
      </VitalsCard>
    </div>
  );
}

export default OverviewVitalsStrip;
