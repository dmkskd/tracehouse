/**
 * Main Time Travel chart height, derived from the viewport so the overview
 * navigator and the top of the Operations table stay visible on short screens.
 */

const MAX_CHART_HEIGHT = 500;
const MIN_CHART_HEIGHT = 260;
/** Vertical space taken by app nav, page header, metric tabs, overview navigator and Operations header. */
const RESERVED_HEIGHT = 560;

export function getTimelineChartHeight(viewportHeight: number): number {
  return Math.min(MAX_CHART_HEIGHT, Math.max(MIN_CHART_HEIGHT, viewportHeight - RESERVED_HEIGHT));
}
