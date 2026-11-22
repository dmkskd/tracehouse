import type { RelationshipFamily } from '@tracehouse/core/services/object-browser';
import type { ObjectDiagramStyle } from '../../stores/userPreferenceStore';

/** How one relationship family is stroked in a style. Color and dashes come from EDGE_STYLES. */
export interface LineWeight { width: number; opacity: number; arrow: boolean }

export interface DiagramStyle {
  label: string;
  hint: string;
  /** One line segment from the current point (x1, y1) to (x2, y2), as SVG path commands. */
  segment: (x1: number, y1: number, x2: number, y2: number) => string;
  weight: (family: RelationshipFamily) => LineWeight;
}

const curve = (x1: number, y1: number, x2: number, y2: number) => { const cx = (x1 + x2) / 2; return ` C${cx} ${y1} ${cx} ${y2} ${x2} ${y2}`; };
/** Horizontal, then vertical at the midpoint, then horizontal, with rounded corners. */
const elbow = (x1: number, y1: number, x2: number, y2: number) => {
  const mx = (x1 + x2) / 2, dx = Math.sign(x2 - x1) || 1, dy = Math.sign(y2 - y1);
  const r = Math.min(8, Math.abs(y2 - y1) / 2, Math.abs(mx - x1));
  if (!dy || !r) return ` L${x2} ${y2}`;
  return ` L${mx - dx * r} ${y1} Q${mx} ${y1} ${mx} ${y1 + dy * r} L${mx} ${y2 - dy * r} Q${mx} ${y2} ${mx + dx * r} ${y2} L${x2} ${y2}`;
};
const line = (width: number, opacity: number, arrow = false): LineWeight => ({ width, opacity, arrow });
/** Name-matched privilege links are possible matches, often to every table: faint in every style. */
const CANDIDATE = line(1, .16);

/** Diagram styles under trial. Each says how lines are drawn; the cards' look is the
 * `[data-style]` CSS in objectBrowserStyles.ts. Layout and behavior are shared. */
export const DIAGRAM_STYLES: Record<ObjectDiagramStyle, DiagramStyle> = {
  cards: { label: 'Cards', hint: 'Card per object; curved connectors with arrows', segment: curve,
    weight: family => family === 'data-flow' ? line(1.75, .6, true) : family === 'candidate' ? CANDIDATE : line(1.25, .5) },
  bars: { label: 'Bars', hint: 'Node bars with flow ribbons, Sankey layout', segment: curve,
    weight: family => family === 'data-flow' ? line(10, .18) : family === 'storage' ? line(1.25, .7) : family === 'candidate' ? CANDIDATE : line(4, .18) },
  columns: { label: 'Columns', hint: 'Grouped list per column; curved connectors', segment: curve,
    weight: family => family === 'data-flow' ? line(1.75, .6, true) : family === 'candidate' ? CANDIDATE : line(1.25, .5) },
  metro: { label: 'Metro', hint: 'Pill nodes; orthogonal connectors with rounded corners', segment: elbow,
    weight: family => family === 'data-flow' ? line(2.5, .75) : family === 'candidate' ? CANDIDATE : line(1.5, .55) },
};
