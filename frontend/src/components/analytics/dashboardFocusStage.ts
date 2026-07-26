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
