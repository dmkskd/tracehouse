import type { DashboardPanel } from './dashboards';

export interface DashboardPanelSection {
  name: string | null;
  panels: { panel: DashboardPanel; globalIndex: number }[];
}

/**
 * Preserve the dashboard's linear panel order while grouping panels under the
 * most recently declared section. This mirrors the grid's section semantics.
 */
export function groupDashboardPanels(panels: DashboardPanel[]): DashboardPanelSection[] {
  const sections: DashboardPanelSection[] = [];
  let current: DashboardPanelSection = { name: null, panels: [] };
  sections.push(current);
  panels.forEach((panel, globalIndex) => {
    if (panel.section) {
      current = { name: panel.section, panels: [] };
      sections.push(current);
    }
    current.panels.push({ panel, globalIndex });
  });
  return sections.filter(section => section.panels.length > 0);
}

/**
 * Narrow the rail to sections/panels matching a free-text query. A match on the
 * section name keeps the whole section; otherwise only matching panels survive.
 * Global panel indexes are preserved so selection still addresses the dashboard.
 */
export function filterDashboardPanelSections(
  sections: DashboardPanelSection[],
  query: string,
  panelTitle: (panel: DashboardPanel) => string,
): DashboardPanelSection[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return sections;
  const terms = needle.split(/\s+/);
  const matches = (haystack: string) => {
    const lower = haystack.toLowerCase();
    return terms.every(term => lower.includes(term));
  };

  return sections.reduce<DashboardPanelSection[]>((kept, section) => {
    if (section.name && matches(section.name)) {
      kept.push(section);
      return kept;
    }
    const panels = section.panels.filter(entry => matches(panelTitle(entry.panel)));
    if (panels.length > 0) kept.push({ name: section.name, panels });
    return kept;
  }, []);
}

/**
 * Step to the neighbouring panel within `sections`, wrapping at either end.
 * `sections` is the navigable set, so a rail filter narrows what ↑/↓ reach.
 * A current panel outside that set (filtered out while focused) still lands on
 * the nearest panel in the travel direction.
 */
export function adjacentPanelIndex(
  sections: DashboardPanelSection[],
  currentPanelIndex: number,
  direction: -1 | 1,
): number {
  const indexes = sections.flatMap(section => section.panels.map(entry => entry.globalIndex));
  if (indexes.length === 0) return currentPanelIndex;
  return direction === 1
    ? indexes.find(index => index > currentPanelIndex) ?? indexes[0]
    : [...indexes].reverse().find(index => index < currentPanelIndex) ?? indexes[indexes.length - 1];
}

/** Return the first panel in the adjacent section, wrapping at either end. */
export function adjacentSectionPanelIndex(
  sections: DashboardPanelSection[],
  currentPanelIndex: number,
  direction: -1 | 1,
): number {
  if (sections.length === 0) return currentPanelIndex;
  const currentSectionIndex = sections.findIndex(section =>
    section.panels.some(entry => entry.globalIndex === currentPanelIndex));
  if (currentSectionIndex < 0) return sections[0]?.panels[0]?.globalIndex ?? currentPanelIndex;
  const nextSectionIndex = (currentSectionIndex + direction + sections.length) % sections.length;
  return sections[nextSectionIndex]?.panels[0]?.globalIndex ?? currentPanelIndex;
}

/** Only the active expanded panel or a visible hovered grid panel owns shortcuts. */
export function panelOwnsShortcut(expanded: boolean, hovered: boolean, hidden: boolean): boolean {
  return expanded || (hovered && !hidden);
}

/** Elements that should consume Escape before dashboard-level navigation. */
export const DASHBOARD_ESCAPE_LAYER_SELECTOR = [
  '[role="dialog"]',
  '[aria-modal="true"]',
  '[role="menu"]',
  '[role="listbox"]',
  '[data-dashboard-escape-layer]',
].join(',');

/** Use Escape for dashboard-list navigation only when no more local UI layer owns it. */
export function dashboardOwnsEscape(
  event: Pick<KeyboardEvent, 'key' | 'defaultPrevented' | 'isComposing' | 'target'>,
  expandedPanel: boolean,
  escapeLayerOpen: boolean,
): boolean {
  if (event.key !== 'Escape' || event.defaultPrevented || event.isComposing) return false;
  if (expandedPanel || escapeLayerOpen) return false;

  const target = event.target;
  return !(
    target instanceof HTMLInputElement ||
    target instanceof HTMLTextAreaElement ||
    target instanceof HTMLSelectElement ||
    (target instanceof HTMLElement && (
      target.isContentEditable ||
      target.closest(DASHBOARD_ESCAPE_LAYER_SELECTOR) !== null
    ))
  );
}

/** Let Focus Stage own navigation keys only when no foreground control is active. */
export function dashboardOwnsFocusNavigation(
  event: Pick<KeyboardEvent, 'defaultPrevented' | 'isComposing' | 'target'>,
  escapeLayerOpen: boolean,
): boolean {
  if (event.defaultPrevented || event.isComposing || escapeLayerOpen) return false;

  const target = event.target;
  return !(
    target instanceof HTMLInputElement ||
    target instanceof HTMLTextAreaElement ||
    target instanceof HTMLSelectElement ||
    (target instanceof HTMLElement && (
      target.isContentEditable ||
      target.closest(DASHBOARD_ESCAPE_LAYER_SELECTOR) !== null
    ))
  );
}
