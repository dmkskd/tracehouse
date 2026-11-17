import { useEffect, useId, useRef } from 'react';
import type { MetricMode } from '../timeline-constants';
import { segmentColor } from '../timeline-constants';
import type { ResourceTableGroup } from './resource-exploration-model';

export interface ResourceFlowSelection { activity: string; tableId?: string; consumerId?: string }

/** Preserve readable columns; reveal the selected table's operations without scaling the whole graph down. */
export function ResourceAttributionFlow({ groups, mode, selection, onSelect, format, scaleReference, selectionKind = 'instant' }: {
  selectionKind?: 'instant' | 'range'; scaleReference: number; groups: ResourceTableGroup[]; mode: MetricMode; selection: ResourceFlowSelection | null;
  onSelect: (selection: ResourceFlowSelection | null) => void; format: (value: number, mode: MetricMode) => string;
}) {
  const failurePattern = `resource-failure-${useId().replace(/:/g, '')}`;
  const viewport = useRef<HTMLDivElement>(null);
  const expanded = groups.find(g => g.id === selection?.tableId);
  const expandedId = expanded?.id;
  useEffect(() => {
    const frame = requestAnimationFrame(() => {
      const el = viewport.current;
      if (el) el.scrollTo({ left: expandedId ? el.scrollWidth - el.clientWidth : 0,
        behavior: window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
    });
    return () => cancelAnimationFrame(frame);
  }, [expandedId]);
  const failedCount = groups.reduce((sum, group) => sum + group.rows.filter(row => row.failureCode !== 0).length, 0);
  const total = groups.reduce((s, g) => s + g.value, 0);
  if (!total) return null;
  const scale = 230 / Math.max(scaleReference, 1);
  const sourceHeight = total * scale;
  let nextY = 45, sourceY = 50;
  const activities = [...new Set(groups.map(g => g.activity))].map(activity => {
    const tables = groups.filter(g => g.activity === activity);
    const value = tables.reduce((s, g) => s + g.value, 0);
    const top = nextY;
    const tableNodes = tables.map(table => {
      const height = table.value * scale;
      const selected = table.id === expanded?.id;
      const failureValue = table.rows.reduce((sum, c) => sum + (c.failureCode ? c.value : 0), 0);
      const ordered = [...table.rows].sort((a, b) => Number(!!a.failureCode) - Number(!!b.failureCode));
      const rows = ordered.slice(0, 6).map(c => ({ id: c.id, label: c.label, value: c.value, failureValue: c.failureCode ? c.value : 0, failureCode: c.failureCode }));
      if (table.rows.length > 6) rows.push({ id: 'remainder', label: `${table.rows.length - 6} other operations`, value: ordered.slice(6).reduce((s, c) => s + c.value, 0), failureValue: ordered.slice(6).reduce((s, c) => s + (c.failureCode ? c.value : 0), 0), failureCode: 0 });
      const rowSpace = rows.reduce((s, r) => s + Math.max(34, r.value * scale), 0) + Math.max(0, rows.length - 1) * 10;
      const slot = Math.max(height, selected ? rowSpace : 0, table.tables.length > 1 && !table.remainder ? 60 : 44);
      const nodeY = nextY + (slot - height) / 2;
      let rowY = nextY;
      const leaves = rows.map(row => {
        const h = row.value * scale, rh = Math.max(34, h);
        const y = rowY + (rh - h) / 2, labelY = rowY + rh / 2;
        rowY += rh + 10;
        return { ...row, height: h, y, labelY };
      });
      const labelY = nextY + slot / 2;
      nextY += slot + 14;
      return { ...table, selected, failureValue, height, y: nodeY, labelY, leaves };
    });
    const block = nextY - top - 14, height = value * scale;
    const node = { activity, value, height, y: top + (block - height) / 2, labelY: top + block / 2,
      sourceY, tables: tableNodes, color: segmentColor(activity, tables[0].rows[0].kind) };
    sourceY += height; nextY += 18;
    return node;
  });
  const width = expanded ? 1440 : 1000, height = Math.max(340, nextY + 10, 50 + sourceHeight + 38);
  const ribbon = (x1: number, y1: number, x2: number, y2: number, h: number) => {
    const mid = (x1 + x2) / 2;
    return `M${x1} ${y1} C${mid} ${y1} ${mid} ${y2} ${x2} ${y2} L${x2} ${y2 + h} C${mid} ${y2 + h} ${mid} ${y1 + h} ${x1} ${y1 + h} Z`;
  };
  const short = (label: string, n: number) => label.length > n ? `${label.slice(0, n - 1)}…` : label;
  const hatch = (x1: number, y1: number, x2: number, y2: number, h: number) => h > 0
    ? <path d={ribbon(x1, y1, x2, y2, h)} fill={`url(#${failurePattern})`} data-testid="failed-resource-flow" /> : null;
  return <div className="flow">
    {failedCount > 0 && <div className="muted" style={{ margin: '6px 12px' }}>▨ Hatched: failed outcome · {failedCount} {failedCount === 1 ? 'operation' : 'operations'}</div>}
    {expanded && <div className="heading flow-navigation"><span className="muted">{expanded.label} → {expanded.rows[0].kind === 'query' ? 'queries' : 'operations'}</span>
      <button onClick={() => onSelect({ activity: expanded.activity })}>Collapse {expanded.rows[0].kind === 'query' ? 'queries' : 'operations'}</button>
    </div>}
    <div ref={viewport} className="flow-viewport" role="region" aria-label="Resource attribution flow" tabIndex={0}>
      <svg viewBox={`0 0 ${width} ${height}`} style={{ width, minWidth: '100%' }} role="img" aria-label="Loaded operation usage by activity and table; select a table to expand queries">
        <defs><pattern id={failurePattern} width={6} height={6} patternUnits="userSpaceOnUse" patternTransform="rotate(35)"><path d="M0 0V6" stroke="#e06c65" strokeWidth={2} /></pattern></defs>
        <text x={12} y={20}>{selectionKind === 'range' ? 'Attributed work' : 'Active operations'}</text><text x={240} y={20}>Activity</text><text x={560} y={20}>Tables / table sets</text>{expanded && <text x={1020} y={20}>Queries / operations</text>}
        <rect x={12} y={50} width={8} height={230} fill="var(--text-muted)" opacity={0.1} aria-hidden="true" />
        <rect data-testid="resource-flow-total" x={12} y={50} width={8} height={sourceHeight} fill="var(--text-muted)"><title>{format(total, mode)} · fixed scale while exploring time</title></rect>
        <text x={12} y={50 + sourceHeight + 28}>{format(total, mode)}</text>
        {activities.map(a => {
          let tableSourceY = a.y;
          return <g key={a.activity}>
            <path d={ribbon(20, a.sourceY, 240, a.y, a.height)} fill={a.color} opacity={0.24} />
            <rect x={240} y={a.y} width={8} height={a.height} fill={a.color} />
            {a.tables.map(table => {
              const start = tableSourceY; tableSourceY += table.height;
              let leafSourceY = table.y;
              const failedHeight = table.failureValue * scale;
              const failedOffset = table.height - failedHeight;
              const sourceOffset = start - a.y;
              return <g key={table.id}>
                <path d={ribbon(248, start, 560, table.y, table.height)} fill={a.color} opacity={table.selected ? 0.3 : 0.15} />
                <rect x={560} y={table.y} width={8} height={table.height} fill={a.color} />
                {failedHeight > 0 && <>
                  <rect x={12} y={a.sourceY + sourceOffset + failedOffset} width={8} height={failedHeight} fill={`url(#${failurePattern})`} />
                  {hatch(20, a.sourceY + sourceOffset + failedOffset, 240, start + failedOffset, failedHeight)}
                  <rect x={240} y={start + failedOffset} width={8} height={failedHeight} fill={`url(#${failurePattern})`} />
                  {hatch(248, start + failedOffset, 560, table.y + failedOffset, failedHeight)}
                  <rect x={560} y={table.y + failedOffset} width={8} height={failedHeight} fill={`url(#${failurePattern})`} />
                </>}
                {table.selected && table.leaves.map(leaf => {
                  const startY = leafSourceY; leafSourceY += leaf.height;
                  return <g key={leaf.id} role="button" tabIndex={0} aria-label={`Open operation ${leaf.label}${leaf.failureCode ? ` · Failed · code ${leaf.failureCode}` : ''}`} onClick={() => onSelect({ activity: a.activity, tableId: table.id, ...(leaf.id === 'remainder' ? {} : { consumerId: leaf.id }) })}
                    onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect({ activity: a.activity, tableId: table.id, ...(leaf.id === 'remainder' ? {} : { consumerId: leaf.id }) }); } }}>
                    <title>{leaf.label}: {format(leaf.value, mode)}{leaf.failureCode ? ` · Failed · code ${leaf.failureCode}` : leaf.failureValue > 0 ? ' · Includes failed operations' : ''}</title>
                    <path d={ribbon(568, startY, 1000, leaf.y, leaf.height)} fill={a.color} opacity={selection?.consumerId === leaf.id ? 0.35 : 0.15} />
                    <rect x={1000} y={leaf.y} width={8} height={leaf.height} fill={a.color} />
                    {leaf.failureValue > 0 && <>
                      {hatch(568, startY + leaf.height - leaf.failureValue * scale, 1000, leaf.y + leaf.height - leaf.failureValue * scale, leaf.failureValue * scale)}
                      <rect x={1000} y={leaf.y + leaf.height - leaf.failureValue * scale} width={8} height={leaf.failureValue * scale} fill={`url(#${failurePattern})`} />
                    </>}
                    <rect x={1015} y={leaf.labelY - 18} width={400} height={34} fill="transparent" />
                    <text x={1020} y={leaf.labelY - 3}>{short(leaf.label, 53)}</text><text className="value" x={1020} y={leaf.labelY + 13}>{format(leaf.value, mode)}{leaf.failureCode ? ` · Failed · code ${leaf.failureCode}` : ''}</text>
                  </g>;
                })}
                <g role="button" tabIndex={0} aria-label={`Expand ${table.label} in ${a.activity}`} aria-expanded={table.selected}
                  onClick={() => onSelect({ activity: a.activity, tableId: table.id })} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect({ activity: a.activity, tableId: table.id }); } }}>
                  <title>{table.label}{table.failureValue > 0 ? ` · ${table.rows.filter(r => r.failureCode).length} failed operations · codes ${[...new Set(table.rows.map(r => r.failureCode).filter(Boolean))].join(", ")}` : ''}{table.tables.length ? `: ${table.tables.join(', ')}` : ''} · {table.rows.length} operations · {format(table.value, mode)}</title>
                  <rect x={575} y={table.labelY - 28} width={expanded ? 410 : 405} height={54} fill="transparent" />
                  {table.tables.length > 1 && !table.remainder ? <>
                    <text x={580} y={table.labelY - 17}>{short(table.tables[0], 46)}</text>
                    <text x={580} y={table.labelY - 2}>+ {short(table.tables[1], 40)}{table.tables.length > 2 ? ` (+${table.tables.length - 2})` : ''} →</text>
                  </> : <text x={580} y={table.labelY - 3}>{short(table.label, expanded ? 46 : 50)} →</text>}
                  <text className="value" x={580} y={table.labelY + 14}>{format(table.value, mode)} · {table.rows.length} {table.rows.length === 1 ? 'operation' : 'operations'}{table.tables.length > 1 && !table.remainder ? ' · shared cost' : ''}</text>
                </g>
              </g>;
            })}
            <g role="button" tabIndex={0} aria-label={`Inspect ${a.activity}`} onClick={() => onSelect({ activity: a.activity })} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect({ activity: a.activity }); } }}>
              <rect x={255} y={a.labelY - 24} width={285} height={48} fill="transparent" />
              <text x={260} y={a.labelY - 3}>{a.activity}</text><text className="value" x={260} y={a.labelY + 13}>{format(a.value, mode)}</text>
            </g>
          </g>;
        })}
      </svg>
    </div>
  </div>;
}
