import { segmentColor } from '../timeline-constants';
import { flowGroupCount, flowGroupDetailCount, flowGroupFailureValue, flowGroupFailedCount, flowGroupKind, type ResourceFlowGroup } from './resource-exploration-model';

export interface ResourceFlowSelection { primaryId: string; groupId?: string; consumerId?: string }

/** Pure presentation layout. Resource values and membership are supplied by core attribution. */
export function buildResourceFlowLayout(groups: ResourceFlowGroup[], {
  selection, groupBy, scaleReference, operationLimit,
}: {
  selection: ResourceFlowSelection | null; groupBy: 'activity' | 'table'; scaleReference: number; operationLimit: number;
}) {
  const expanded = groups.find(g => g.id === selection?.groupId && g.primaryId === selection.primaryId);
  const tableFirst = groupBy === 'table';
  const primaryExpanded = tableFirst && groups.some(g => g.primaryId === selection?.primaryId);
  const failedCount = groups.reduce((sum, group) => sum + flowGroupFailedCount(group), 0);
  const total = groups.reduce((s, g) => s + g.value, 0);
  const scale = 230 / Math.max(scaleReference, 1);
  const sourceHeight = total * scale;
  let nextY = 45, sourceY = 50;
  const primaryGroups = new Map<string, ResourceFlowGroup[]>();
  for (const group of groups) {
    const children = primaryGroups.get(group.primaryId) ?? [];
    children.push(group); primaryGroups.set(group.primaryId, children);
  }
  const primaries = [...primaryGroups.entries()].map(([primaryId, tables]) => {
    const value = tables.reduce((s, g) => s + g.value, 0);
    const top = nextY;
    const showChildren = !tables[0].remainingActivities && (!tableFirst || selection?.primaryId === primaryId);
    let offset = 0;
    const segments = tables.map(table => {
      const height = table.value * scale;
      const failureValue = flowGroupFailureValue(table);
      const segment = { id: table.id, offset, height, failureValue, value: table.value,
        color: segmentColor(tableFirst || table.remainingActivities ? table.secondaryLabel : primaryId, flowGroupKind(table)) };
      offset += height;
      return segment;
    });
    const tableNodes = (showChildren ? tables : []).map(table => {
      const height = table.value * scale;
      const selected = table.id === expanded?.id;
      const failureValue = flowGroupFailureValue(table);
      const ordered = (selected ? [...table.rows] : []).sort((a, b) => b.value - a.value || a.id.localeCompare(b.id));
      const rows = ordered.slice(0, operationLimit).map(c => ({ id: c.id, label: c.label, value: c.value, failureValue: c.failureCode ? c.value : 0, failureCode: c.failureCode, remainingCount: 0 }));
      const shownValue = rows.reduce((sum, row) => sum + row.value, 0);
      const remainingCount = flowGroupDetailCount(table) - rows.length;
      if (selected && remainingCount > 0) rows.push({ id: 'remainder', label: `${remainingCount} other operations`,
        value: Math.max(0,table.value-shownValue), failureValue: Math.max(0,failureValue-rows.reduce((sum,row) => sum+row.failureValue,0)), failureCode: 0, remainingCount });
      // Keep failed portions contiguous through the parent ribbon, including
      // a remainder containing both successful and failed operations.
      rows.sort((a, b) => Number(a.failureValue > 0) - Number(b.failureValue > 0)
        || Number(a.failureValue === a.value) - Number(b.failureValue === b.value));
      const rowSpace = rows.reduce((s, r) => s + Math.max(34, r.value * scale), 0) + Math.max(0, rows.length - 1) * 10;
      const slot = Math.max(height, selected ? rowSpace : 0, !tableFirst && table.tables.length > 1 && !table.remainder ? 60 : 44);
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
      return { ...table, selected, failureValue, height, y: nodeY, labelY, leaves,
        color: segmentColor(tableFirst || table.remainingActivities ? table.secondaryLabel : primaryId, flowGroupKind(table)) };
    });
    if (!showChildren) nextY += Math.max(value * scale, 54) + 14;
    const block = nextY - top - 14, height = value * scale;
    const node = { primaryId, value, height, y: top + (block - height) / 2, labelY: top + block / 2,
      label: tables[0].primaryLabel, count: tables.reduce((sum, t) => sum + flowGroupCount(t), 0),
      sharedCost: !tables[0].remainder && tables[0].tables.length > 1,
      remainingTables: tables[0].remainingTables,
      remainingActivities: tables[0].remainingActivities, visibleActivities: tables[0].visibleActivities,
      failureValue: tables.reduce((sum, t) => sum + flowGroupFailureValue(t), 0),
      sourceY, tables: tableNodes, segments };
    sourceY += height; nextY += 18;
    return node;
  });
  const width = expanded ? 1440 : tableFirst && !primaryExpanded ? 680 : 1000, height = Math.max(340, nextY + 10, 50 + sourceHeight + 38);
  return { expanded, tableFirst, primaryExpanded, failedCount, total, scale, sourceHeight, primaries, width, height };
}

export const resourceRibbon = (x1: number, y1: number, x2: number, y2: number, h: number) => {
  const mid = (x1 + x2) / 2;
  return `M${x1} ${y1} C${mid} ${y1} ${mid} ${y2} ${x2} ${y2} L${x2} ${y2 + h} C${mid} ${y2 + h} ${mid} ${y1 + h} ${x1} ${y1 + h} Z`;
};
