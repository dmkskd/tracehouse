import { useEffect, useId, useRef } from 'react';
import type { MetricMode } from '../timeline-constants';
import { buildResourceFlowLayout, resourceRibbon as ribbon, type ResourceFlowSelection } from './resource-flow-layout';
import { ResourceFailureLegend } from './ResourceFailureLegend';
import { flowGroupCount, flowGroupKind, flowGroupFailedCount, type ResourceFlowGroup } from './resource-exploration-model';

export type { ResourceFlowSelection } from './resource-flow-layout';

/** Preserve readable columns; reveal the selected table's operations without scaling the whole graph down. */
export function ResourceAttributionFlow({ groups, mode, selection, onSelect, format, scaleReference, selectionKind = 'instant', groupBy = 'activity', operationLimit = 5, onShowMoreTables, onShowMoreOperations, detailsLoading = false, onShowMoreActivities, showFailureLegend = true }: {
  groupBy?: 'activity' | 'table';
  operationLimit?: number;
  onShowMoreTables?: (primaryId: string) => void;
  onShowMoreOperations?: (groupId: string) => void;
  detailsLoading?: boolean;
  showFailureLegend?: boolean;
  onShowMoreActivities?: (nextLimit: number) => void;
  selectionKind?: 'instant' | 'range'; scaleReference: number; groups: ResourceFlowGroup[]; mode: MetricMode; selection: ResourceFlowSelection | null;
  onSelect: (selection: ResourceFlowSelection | null) => void; format: (value: number, mode: MetricMode) => string;
}) {
  const failurePattern = `resource-failure-${useId().replace(/:/g, '')}`;
  const viewport = useRef<HTMLDivElement>(null);
  const { expanded, tableFirst, primaryExpanded, failedCount, total, scale, sourceHeight, primaries, width, height } =
    buildResourceFlowLayout(groups, { selection, groupBy, scaleReference, operationLimit });
  const expandedId = expanded?.id;
  useEffect(() => {
    const frame = requestAnimationFrame(() => {
      const el = viewport.current;
      if (el) el.scrollTo({ left: expandedId || primaryExpanded ? el.scrollWidth - el.clientWidth : 0,
        behavior: window.matchMedia?.('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
    });
    return () => cancelAnimationFrame(frame);
  }, [expandedId, primaryExpanded]);
  if (!total) return null;
  const short = (label: string, n: number) => label.length > n ? `${label.slice(0, n - 1)}…` : label;
  const hatch = (x1: number, y1: number, x2: number, y2: number, h: number) => h > 0
    ? <path d={ribbon(x1, y1, x2, y2, h)} fill={`url(#${failurePattern})`} data-testid="failed-resource-flow" /> : null;
  return <div className="flow">
    {showFailureLegend && <ResourceFailureLegend count={failedCount} />}
    {(expanded || primaryExpanded) && <div className="heading flow-navigation" style={{ justifyContent: 'flex-end' }}>
      <button type="button" className="resource-action" aria-label={expanded ? `Collapse ${flowGroupKind(expanded) === 'query' ? 'queries' : 'operations'}` : 'Collapse table'}
        onClick={() => onSelect(expanded ? { primaryId: expanded.primaryId } : null)}>Collapse</button>
    </div>}
    <div ref={viewport} className="flow-viewport" role="region" aria-label="Resource attribution flow" tabIndex={0}>
      <svg viewBox={`0 0 ${width} ${height}`} style={{ width, minWidth: '100%' }} role="img" aria-label={tableFirst ? 'Loaded operation usage ranked by table; select a table to expand activity types' : 'Loaded operation usage by activity and table; select a table to expand queries'}>
        <defs><pattern id={failurePattern} width={6} height={6} patternUnits="userSpaceOnUse" patternTransform="rotate(35)"><path d="M0 0V6" stroke="#e06c65" strokeWidth={2} /></pattern></defs>
        <text x={12} y={20}>{selectionKind === 'range' ? 'Attributed work' : 'Active operations'}</text><text x={240} y={20}>{tableFirst ? 'Tables / table sets' : 'Activity'}</text>{(!tableFirst || primaryExpanded) && <text x={560} y={20}>{tableFirst ? 'Activity' : 'Tables / table sets'}</text>}{expanded && <text x={1020} y={20}>Queries / operations</text>}
        <rect x={12} y={50} width={8} height={230} fill="var(--text-muted)" opacity={0.1} aria-hidden="true" />
        <rect data-testid="resource-flow-total" x={12} y={50} width={8} height={sourceHeight} fill="var(--text-muted)"><title>{format(total, mode)} · fixed scale while exploring time</title></rect>
        <text x={12} y={50 + sourceHeight + 28}>{format(total, mode)}</text>
        {primaries.map(a => {
          let tableSourceY = a.y;
          return <g key={a.primaryId}>
            {a.segments.map(segment => {
              const source = a.sourceY + segment.offset, target = a.y + segment.offset;
              const failedHeight = segment.failureValue * scale, failedOffset = segment.height - failedHeight;
              return <g key={segment.id}>
                <rect x={12} y={source} width={8} height={segment.height} fill={segment.color} />
                <path d={ribbon(20, source, 240, target, segment.height)} fill={segment.color} opacity={0.24} />
                <rect x={240} y={target} width={8} height={segment.height} fill={segment.color} />
                {failedHeight > 0 && <>
                  <rect x={12} y={source + failedOffset} width={8} height={failedHeight} fill={`url(#${failurePattern})`} />
                  {hatch(20, source + failedOffset, 240, target + failedOffset, failedHeight)}
                  <rect x={240} y={target + failedOffset} width={8} height={failedHeight} fill={`url(#${failurePattern})`} />
                </>}
              </g>;
            })}
            {a.tables.map(table => {
              const color = table.color;
              const start = tableSourceY; tableSourceY += table.height;
              let leafSourceY = table.y;
              const failedHeight = table.failureValue * scale;
              const failedOffset = table.height - failedHeight;
              return <g key={table.id}>
                <path d={ribbon(248, start, 560, table.y, table.height)} fill={color} opacity={table.selected ? 0.3 : 0.15} />
                <rect x={560} y={table.y} width={8} height={table.height} fill={color} />
                {failedHeight > 0 && <>
                  {hatch(248, start + failedOffset, 560, table.y + failedOffset, failedHeight)}
                  <rect x={560} y={table.y + failedOffset} width={8} height={failedHeight} fill={`url(#${failurePattern})`} />
                </>}
                {table.selected && table.leaves.map(leaf => {
                  const startY = leafSourceY; leafSourceY += leaf.height;
                  const isMore = leaf.id === 'remainder';
                  const activate = () => { if (isMore) { if (!detailsLoading) onShowMoreOperations?.(table.id); }
                    else onSelect({ primaryId: a.primaryId, groupId: table.id, consumerId: leaf.id }); };
                  return <g key={leaf.id} role="button" tabIndex={0} aria-disabled={isMore && detailsLoading || undefined}
                    aria-label={isMore ? `Show ${Math.min(5,leaf.remainingCount)} more operations in ${table.secondaryLabel}` : `Open operation ${leaf.label}${leaf.failureCode ? ` · Failed · code ${leaf.failureCode}` : ''}`}
                    onClick={activate} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activate(); } }}>
                    <title>{leaf.id === 'remainder' ? 'Additional operations' : `Open ${flowGroupKind(table) === 'query' ? 'query' : flowGroupKind(table)} details`} · {leaf.label}: {format(leaf.value, mode)}{leaf.failureCode ? ` · Failed · code ${leaf.failureCode}` : leaf.failureValue > 0 ? ' · Includes failed operations' : ''}</title>
                    <path d={ribbon(568, startY, 1000, leaf.y, leaf.height)} fill={color} opacity={selection?.consumerId === leaf.id ? 0.35 : 0.15} />
                    <rect x={1000} y={leaf.y} width={8} height={leaf.height} fill={color} />
                    {leaf.failureValue > 0 && <>
                      {hatch(568, startY + leaf.height - leaf.failureValue * scale, 1000, leaf.y + leaf.height - leaf.failureValue * scale, leaf.failureValue * scale)}
                      <rect x={1000} y={leaf.y + leaf.height - leaf.failureValue * scale} width={8} height={leaf.failureValue * scale} fill={`url(#${failurePattern})`} />
                    </>}
                    <rect x={1015} y={leaf.labelY - 18} width={400} height={34} fill="transparent" />
                    <text className={leaf.id === 'remainder' ? undefined : 'operation-link'} x={1020} y={leaf.labelY - 3}>{short(leaf.label, 53)}</text><text className="value" x={1020} y={leaf.labelY + 13}>{format(leaf.value, mode)}{leaf.failureCode ? ` · Failed · code ${leaf.failureCode}` : ''}{leaf.id === 'remainder' && <tspan className="operation-hint"> · {detailsLoading ? 'Loading…' : `Show ${Math.min(5,leaf.remainingCount)} more`}</tspan>}{leaf.id !== 'remainder' && <tspan className="operation-hint"> · View details →</tspan>}</text>
                  </g>;
                })}
                <g role="button" tabIndex={0} aria-label={table.remainingTables && !tableFirst ? `Show ${Math.min(5,table.remainingTables)} more tables in ${a.label}` : `Expand ${table.secondaryLabel} in ${a.label}`} aria-expanded={table.selected}
                  onClick={() => table.remainingTables && !tableFirst ? onShowMoreTables?.(a.primaryId) : onSelect({ primaryId: a.primaryId, groupId: table.id })} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); if (table.remainingTables && !tableFirst) onShowMoreTables?.(a.primaryId); else onSelect({ primaryId: a.primaryId, groupId: table.id }); } }}>
                  <title>{table.secondaryLabel}{table.failureValue > 0 ? ` · ${flowGroupFailedCount(table)} failed operations · codes ${[...new Set(table.rows.map(r => r.failureCode).filter(Boolean))].join(", ")}` : ''}{table.tables.length ? `: ${table.tables.join(', ')}` : ''} · {flowGroupCount(table)} operations · {format(table.value, mode)}</title>
                  <rect x={575} y={table.labelY - 28} width={expanded ? 410 : 405} height={54} fill="transparent" />
                  {!tableFirst && table.tables.length > 1 && !table.remainder ? <>
                    <text x={580} y={table.labelY - 17}>{short(table.tables[0], 46)}</text>
                    <text x={580} y={table.labelY - 2}>+ {short(table.tables[1], 40)}{table.tables.length > 2 ? ` (+${table.tables.length - 2})` : ''} →</text>
                  </> : <text x={580} y={table.labelY - 3}>{table.remainingTables && !tableFirst ? `${table.remainingTables} more tables` : `${short(table.secondaryLabel, expanded ? 46 : 50)} →`}</text>}
                  <text className="value" x={580} y={table.labelY + 14}>{format(table.value, mode)} · {flowGroupCount(table)} {flowGroupCount(table) === 1 ? 'operation' : 'operations'}{!tableFirst && table.tables.length > 1 && !table.remainder ? ' · shared cost' : ''}{table.remainingTables && !tableFirst ? <tspan className="operation-hint"> · Show {Math.min(5,table.remainingTables)} more</tspan> : null}</text>
                </g>
              </g>;
            })}
            <g role="button" tabIndex={0} aria-label={a.remainingActivities ? `Show ${Math.min(5,a.remainingActivities)} more activity types` : tableFirst && a.remainingTables ? `Show ${Math.min(5,a.remainingTables)} more tables` : tableFirst ? `Expand table ${a.label}` : `Inspect ${a.primaryId}`} aria-expanded={tableFirst ? selection?.primaryId === a.primaryId : undefined} onClick={() => a.remainingActivities ? onShowMoreActivities?.((a.visibleActivities ?? 0)+5) : tableFirst && a.remainingTables ? onShowMoreTables?.(a.primaryId) : onSelect({ primaryId: a.primaryId })} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); if (a.remainingActivities) onShowMoreActivities?.((a.visibleActivities ?? 0)+5); else if (tableFirst && a.remainingTables) onShowMoreTables?.(a.primaryId); else onSelect({ primaryId: a.primaryId }); } }}>
              <rect x={255} y={a.labelY - 24} width={285} height={48} fill="transparent" />
              <title>{a.label} · {format(a.value, mode)}{tableFirst ? ' · Expand activity types' : ''}</title>
              <text x={260} y={a.labelY - 3}>{tableFirst && a.remainingTables ? `${a.remainingTables} more tables` : short(a.label, tableFirst ? 38 : 46)}{tableFirst && !a.remainingTables ? ' →' : ''}</text><text className="value" x={260} y={a.labelY + 13}>{format(a.value, mode)}{tableFirst ? ` · ${a.count} ${a.count === 1 ? 'operation' : 'operations'}${a.sharedCost ? ' · shared cost' : ''}` : ''}{a.remainingActivities ? <tspan className="operation-hint"> · Show {Math.min(5,a.remainingActivities)} more</tspan> : null}{tableFirst && a.remainingTables ? <tspan className="operation-hint"> · Show {Math.min(5,a.remainingTables)} more</tspan> : null}</text>
            </g>
          </g>;
        })}
      </svg>
    </div>
  </div>;
}
