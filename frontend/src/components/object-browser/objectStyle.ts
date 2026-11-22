import type { CSSProperties } from 'react';
import type { ObjectType, RelationshipFamily } from '@tracehouse/core/services/object-browser';

export const OBJECT_SYMBOLS: Record<ObjectType, string> = {
  user: '♙', role: '◇', 'grant-scope': '⚿', 'row-policy': '⊟', 'merge-tree': '▦', view: '▤',
  'materialized-view': '▧', distributed: '◈', dictionary: '▥', buffer: '▦', table: '▦',
  iceberg: '◭', lake: '◭', 'object-storage': '▭', location: '⌂',
};

const ACCESS = 'var(--accent-purple, #9672c9)', DATA = 'var(--accent-teal, #26948c)', LAKE = '#3f88c5';
/** Icon and card accent color per type. */
const OBJECT_COLORS: Record<ObjectType, string> = {
  user: ACCESS, role: ACCESS, 'grant-scope': ACCESS, 'row-policy': ACCESS,
  'merge-tree': '#a88712', view: 'var(--text-secondary)', 'materialized-view': ACCESS, distributed: '#d56920',
  dictionary: 'var(--accent-blue, #6696cf)', buffer: DATA, table: DATA,
  iceberg: LAKE, lake: LAKE, 'object-storage': LAKE, location: '#5b8fb0',
};
/** Colour of a partial revoke: its card and its lines. */
const REVOKE_COLOR = '#d29922';
/** Sets `--tone`, which the card border and the type icon read. A revoke takes the warning colour. */
export const toneStyle = (type: ObjectType | 'revoke') => ({ '--tone': type === 'revoke' ? REVOKE_COLOR : OBJECT_COLORS[type] }) as CSSProperties;
/** Lines of a partial revoke: dashed, in the warning colour. */
export const REVOKE_EDGE_STYLE = { stroke: REVOKE_COLOR, dash: '5 3', arrow: false };

/** Connector style per relationship family. Only data flow has a direction arrow. */
export const EDGE_STYLES: Record<RelationshipFamily, { stroke: string; dash?: string; arrow: boolean }> = {
  access: { stroke: '#9672c9', arrow: false },
  candidate: { stroke: '#9672c9', arrow: false },
  policy: { stroke: '#799aab', dash: '4 4', arrow: false },
  'data-flow': { stroke: '#26948c', arrow: true },
  storage: { stroke: '#5b8fb0', dash: '2 3', arrow: false },
};
