import { useId, useMemo, type RefObject } from 'react';
import { attributeTimeline, type MemoryTimeline, type OperationKind } from '@tracehouse/core';
import { css } from '@emotion/css';
import { formatBytes } from '../../../utils/formatters';
import { ZeroResourceActivity } from './ZeroResourceActivity';
import { attributionConsumers } from './resource-exploration-model';
import { METRIC_CONFIG, segmentColor, type MetricMode } from '../timeline-constants';

const previewStyle = css`
  display: block; width: 100%; text-align: left; border: 0; padding: 0; background: transparent;
  color: var(--text-primary); cursor: pointer; border-radius: 4px;
  &:focus-visible { outline: 2px solid #58a6ff; outline-offset: 4px; }
  &:hover .explore-hint { text-decoration: underline; }
  svg { width: 100%; height: auto; display: block; }
  text { fill: var(--text-primary); font-size: 11px; }
  .value { fill: var(--text-muted); font-size: 10px; }
`;

/** A pinned selection becomes a resource preview, rather than a duplicate hover list. */
export function PinnedResourcePreview({ data, atMs, mode, width, hiddenCategories, onExplore, buttonRef }: {
  data: MemoryTimeline; atMs: number; mode: MetricMode; width: number; hiddenCategories?: ReadonlySet<OperationKind>;
  onExplore: () => void; buttonRef: RefObject<HTMLButtonElement>;
}) {
  const failurePattern = `preview-failure-${useId().replace(/:/g, '')}`;
  const attribution = useMemo(() => attributeTimeline(data, { selection: { kind: 'instant', atMs }, mode, hiddenKinds: hiddenCategories }), [data, atMs, mode, hiddenCategories]);
  const { consumers, missingMemory } = useMemo(() => attributionConsumers(attribution), [attribution]);
  const measured = attribution.measured;
  const total = consumers.reduce((s, c) => s + c.value, 0);
  const sampled = attribution.coverage.sampled;
  const fmt = (v: number) => mode === 'cpu' ? `${(v / 1e6).toLocaleString(undefined, { maximumFractionDigits: 2 })} cores` : `${formatBytes(v)}${mode === 'memory' ? '' : '/s'}`;
  const groups = [...new Set(consumers.map(c => c.group))].map(key => {
    const rows = consumers.filter(c => c.group === key);
    return { key, value: rows.reduce((s, c) => s + c.value, 0), color: segmentColor(key, rows[0].kind), count: rows.length, failureValue: rows.reduce((sum, c) => sum + (c.failureCode ? c.value : 0), 0), failureCodes: [...new Set(rows.map(c => c.failureCode).filter(Boolean))] };
  });
  const shown = groups.slice(0, 4);
  if (groups.length > 4) shown.push({ key: 'Other activity', value: groups.slice(4).reduce((s, g) => s + g.value, 0), color: 'var(--text-muted)', count: groups.slice(4).reduce((s, g) => s + g.count, 0), failureValue: groups.slice(4).reduce((s, g) => s + g.failureValue, 0), failureCodes: [...new Set(groups.slice(4).flatMap(g => g.failureCodes))] });
  const W = Math.max(200, width), rootY = 35, rootH = 120;
  let sourceY = rootY, targetY = 35;
  const nodes = shown.map(g => {
    const height = total > 0 ? rootH * g.value / total : 0, slot = Math.max(height, 38);
    const y = targetY + (slot - height) / 2, labelY = targetY + slot / 2, start = sourceY;
    targetY += slot + 8; sourceY += height;
    return { ...g, height, y, labelY, start };
  });
  const H = Math.max(180, targetY + 25);
  return <>
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 8, fontSize: 12 }}>
      <strong>{METRIC_CONFIG[mode].label}</strong><span>Server: {measured.value === null ? 'Unavailable' : fmt(measured.value)}</span>
    </div>
    <button ref={buttonRef} className={previewStyle} onClick={onExplore} aria-label="Explore resource usage…">
      {total > 0 ? <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Mini Sankey of loaded operation usage by activity">
        <defs><pattern id={failurePattern} width={6} height={6} patternUnits="userSpaceOnUse" patternTransform="rotate(35)"><path d="M0 0V6" stroke="#e06c65" strokeWidth={2} /></pattern></defs>
        <text x={4} y={15}>Active operations</text><text x={W * .49} y={15}>Activity</text>
        <rect x={5} y={rootY} width={7} height={rootH} fill="var(--text-muted)" />
        {nodes.map(g => {
          const x = W * .48;
          const failedHeight = rootH * g.failureValue / total;
          const offset = g.height - failedHeight;
          return <g key={g.key}><title>{g.key}: {fmt(g.value)} · {g.count} operations{g.failureCodes.length ? ` · Failed outcome · codes ${g.failureCodes.join(", ")}` : ''}</title>
            <path d={`M12 ${g.start} C${x * .5} ${g.start} ${x * .6} ${g.y} ${x} ${g.y} L${x} ${g.y + g.height} C${x * .6} ${g.y + g.height} ${x * .5} ${g.start + g.height} 12 ${g.start + g.height} Z`} fill={g.color} opacity={.25} />
            <rect x={x} y={g.y} width={6} height={g.height} fill={g.color} />
            {failedHeight > 0 && <>
              <rect x={5} y={g.start + offset} width={7} height={failedHeight} fill={`url(#${failurePattern})`} />
              <path data-testid="failed-resource-preview" d={`M12 ${g.start + offset} C${x * .5} ${g.start + offset} ${x * .6} ${g.y + offset} ${x} ${g.y + offset} L${x} ${g.y + g.height} C${x * .6} ${g.y + g.height} ${x * .5} ${g.start + g.height} 12 ${g.start + g.height} Z`} fill={`url(#${failurePattern})`} />
              <rect x={x} y={g.y + offset} width={6} height={failedHeight} fill={`url(#${failurePattern})`} />
            </>}
            <text x={x + 15} y={g.labelY - 3}>{g.key}</text><text className="value" x={x + 15} y={g.labelY + 11}>{fmt(g.value)}{W >= 320 ? ` · ${g.count} ${g.count === 1 ? 'operation' : 'operations'}` : ''}</text>
          </g>;
        })}
        <text x={4} y={H - 10}>{fmt(total)}</text>
      </svg> : <div style={{ padding: '18px 8px', color: 'var(--text-muted)', fontSize: 11, lineHeight: 1.6 }}>{mode === 'memory' && missingMemory ? 'Concurrent memory samples are not loaded. Open exploration to load available history.' : attribution.coverage.active > 0 ? 'Active operations have no positive attributed usage at this instant.' : 'No loaded operations active at this time.'}</div>}
      <ZeroResourceActivity attribution={attribution} formattedZero={fmt(0)} />
      <div className="explore-hint" style={{ marginTop: 4, color: '#58a6ff', fontSize: 11 }}>Click the diagram to explore tables and queries →</div>
    </button>
    <div style={{ marginTop: 8, color: 'var(--text-muted)', fontSize: 9, lineHeight: 1.5 }}>{sampled} sampled · {attribution.coverage.estimated} estimated contributions{missingMemory ? ` · ${missingMemory} without memory samples` : ''}. Loaded operations only.{consumers.some(c => c.failureCode) ? ' Hatching marks failed outcomes.' : ''}</div>
  </>;
}
