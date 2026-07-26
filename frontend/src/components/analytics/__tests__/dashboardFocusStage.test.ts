import { describe, expect, test } from 'vitest';
import type { DashboardPanel } from '../dashboards';
import { adjacentSectionPanelIndex, groupDashboardPanels, panelOwnsShortcut } from '../dashboardFocusStage';

function panel(queryName: string, section?: string): DashboardPanel {
  return { queryName, ...(section ? { section } : {}) };
}

describe('dashboard focus stage', { tags: ['analytics'] }, () => {
  const panels = [
    panel('General#Pressure'),
    panel('Queries#Rate', 'Query pressure'),
    panel('Queries#CPU'),
    panel('Queries#Running'),
    panel('Storage#Merges', 'Storage & merges'),
    panel('Storage#Reads'),
    panel('Storage#Writes'),
    panel('Storage#Disk'),
    panel('Memory#Usage', 'Memory & network'),
    panel('Network#Receive'),
    panel('Network#Send'),
    panel('Memory#Cache'),
  ];

  test('groups a 12-panel dashboard without losing global panel positions', () => {
    const sections = groupDashboardPanels(panels);

    expect(sections.map(section => ({
      name: section.name,
      indexes: section.panels.map(entry => entry.globalIndex),
    }))).toEqual([
      { name: null, indexes: [0] },
      { name: 'Query pressure', indexes: [1, 2, 3] },
      { name: 'Storage & merges', indexes: [4, 5, 6, 7] },
      { name: 'Memory & network', indexes: [8, 9, 10, 11] },
    ]);
  });

  test('jumps to the first panel of the adjacent section and wraps', () => {
    const sections = groupDashboardPanels(panels);

    expect(adjacentSectionPanelIndex(sections, 2, 1)).toBe(4);
    expect(adjacentSectionPanelIndex(sections, 6, -1)).toBe(1);
    expect(adjacentSectionPanelIndex(sections, 11, 1)).toBe(0);
    expect(adjacentSectionPanelIndex(sections, 0, -1)).toBe(8);
  });

  test('prevents a hidden panel with stale hover state from handling shortcuts', () => {
    expect(panelOwnsShortcut(false, true, true)).toBe(false);
    expect(panelOwnsShortcut(false, true, false)).toBe(true);
    expect(panelOwnsShortcut(true, false, false)).toBe(true);
  });
});
