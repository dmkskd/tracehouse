/**
 * Main Time Travel chart height, derived from the space available below the
 * chart's top edge so the overview navigator and the top of the Operations
 * table stay visible on short screens.
 */

const MAX_CHART_HEIGHT = 500;
const MIN_CHART_HEIGHT = 260;
/** Vertical space kept below the chart for the overview navigator, the Operations header and its first rows. */
const RESERVED_BELOW = 300;

/** `availableHeight` is the distance from the chart's top to the bottom of its scroll viewport (null = not measured yet). */
export function getTimelineChartHeight(availableHeight: number | null): number {
  if (availableHeight == null) return MAX_CHART_HEIGHT;
  return Math.min(MAX_CHART_HEIGHT, Math.max(MIN_CHART_HEIGHT, availableHeight - RESERVED_BELOW));
}
