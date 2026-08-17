import { describe, expect, test } from 'vitest';
import type { DashboardPanel } from '../dashboards';
import {
  adjacentSectionPanelIndex,
  dashboardOwnsEscape,
  dashboardOwnsFocusNavigation,
  filterDashboardPanelSections,
  groupDashboardPanels,
  panelOwnsShortcut,
} from '../dashboardFocusStage';

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

  test('filters rail panels by title while keeping global indexes', () => {
    const sections = groupDashboardPanels(panels);
    const title = (panel: DashboardPanel) => panel.queryName.split('#').pop() ?? panel.queryName;

    const shape = (query: string) =>
      filterDashboardPanelSections(sections, query, title).map(section => ({
        name: section.name,
        indexes: section.panels.map(entry => entry.globalIndex),
      }));

    expect(shape('reads')).toEqual([{ name: 'Storage & merges', indexes: [5] }]);
    expect(shape('send')).toEqual([{ name: 'Memory & network', indexes: [10] }]);
    expect(shape('disk')).toEqual([{ name: 'Storage & merges', indexes: [7] }]);
    expect(shape('nothing-here')).toEqual([]);
  });

  test('keeps every panel of a section whose name matches', () => {
    const sections = groupDashboardPanels(panels);
    const title = (panel: DashboardPanel) => panel.queryName.split('#').pop() ?? panel.queryName;

    expect(filterDashboardPanelSections(sections, 'query pressure', title)).toEqual([
      { name: 'Query pressure', panels: sections[1].panels },
    ]);
  });

  test('matches all whitespace-separated terms and is case-insensitive', () => {
    const sections = groupDashboardPanels([panel('A#Peak memory usage', 'Sec')]);
    const title = (p: DashboardPanel) => p.queryName.split('#').pop() ?? p.queryName;

    expect(filterDashboardPanelSections(sections, 'MEMORY usage', title)).toHaveLength(1);
    expect(filterDashboardPanelSections(sections, 'memory disk', title)).toEqual([]);
  });

  test('returns the original sections for an empty or blank query', () => {
    const sections = groupDashboardPanels(panels);
    const title = (panel: DashboardPanel) => panel.queryName;

    expect(filterDashboardPanelSections(sections, '', title)).toBe(sections);
    expect(filterDashboardPanelSections(sections, '   ', title)).toBe(sections);
  });

  test('prevents a hidden panel with stale hover state from handling shortcuts', () => {
    expect(panelOwnsShortcut(false, true, true)).toBe(false);
    expect(panelOwnsShortcut(false, true, false)).toBe(true);
    expect(panelOwnsShortcut(true, false, false)).toBe(true);
  });

  test('uses Escape for list navigation only after local UI layers', () => {
    const plainEscape = new KeyboardEvent('keydown', { key: 'Escape' });
    expect(dashboardOwnsEscape(plainEscape, false, false)).toBe(true);
    expect(dashboardOwnsEscape(plainEscape, true, false)).toBe(false);
    expect(dashboardOwnsEscape(plainEscape, false, true)).toBe(false);

    const input = document.createElement('input');
    const inputEscape = new KeyboardEvent('keydown', { key: 'Escape' });
    input.dispatchEvent(inputEscape);
    expect(dashboardOwnsEscape({ ...inputEscape, target: input }, false, false)).toBe(false);

    const handledEscape = new KeyboardEvent('keydown', { key: 'Escape', cancelable: true });
    handledEscape.preventDefault();
    expect(dashboardOwnsEscape(handledEscape, false, false)).toBe(false);
    expect(dashboardOwnsEscape(new KeyboardEvent('keydown', { key: 'Enter' }), false, false)).toBe(false);
  });

  test('yields focus navigation to a foreground dialog', () => {
    const arrowDown = new KeyboardEvent('keydown', { key: 'ArrowDown' });
    expect(dashboardOwnsFocusNavigation(arrowDown, false)).toBe(true);
    expect(dashboardOwnsFocusNavigation(arrowDown, true)).toBe(false);

    const dialog = document.createElement('div');
    dialog.setAttribute('role', 'dialog');
    const dialogButton = document.createElement('button');
    dialog.appendChild(dialogButton);
    expect(dashboardOwnsFocusNavigation(
      { defaultPrevented: false, isComposing: false, target: dialogButton },
      false,
    )).toBe(false);
  });
});
