import { describe, expect, it } from 'vitest';
import { accessConnector, previewWaypoints } from '../accessConnector';
import { DIAGRAM_STYLES } from '../diagramStyles';

describe('access connectors through database cards', () => {
  const scope = { left: 0, right: 176, y: 40 };
  const database = { left: 216, right: 392, y: 120 };
  const table = { left: 432, right: 608, y: 260 };
  for (const [name, style] of Object.entries(DIAGRAM_STYLES)) {
    it(`enters and exits the database and terminates at the table in ${name}`, () => {
      const path = accessConnector(scope, database, table, style.segment);
      expect(path).toBe(`M176 40${style.segment(176, 40, 216, 120)} L392 120${style.segment(392, 120, 432, 260)}`);
    });
  }
  it('keeps separate table endpoints when a broad scope shares the database stop', () => {
    const segment = DIAGRAM_STYLES.cards.segment;
    const a = accessConnector(scope, database, table, segment);
    const b = accessConnector(scope, database, { ...table, y: 340 }, segment);
    expect(a).not.toBe(b);
    expect(a).toContain('L392 120');expect(b).toContain('L392 120');
    expect(a.endsWith('432 260')).toBe(true);
    expect(b.endsWith('432 340')).toBe(true);
  });
  it('continues through downstream waypoints to a view in another database', () => {
    const targetDatabase = { left: 216, right: 392, y: 420 };
    const view = { left: 864, right: 1040, y: 460 };
    const path = accessConnector(scope, targetDatabase, view, DIAGRAM_STYLES.cards.segment,
      [{ left: 10, right: 100, y: 0 }, { left: 432, right: 608, y: 440 }]);
    expect(path).toContain('216 420');expect(path).toContain('L392 420');
    expect(path).toContain('L608 440');expect(path.endsWith('864 460')).toBe(true);
    expect(path).not.toContain('L100 0');
  });
});


describe('hover route waypoints without inserted rows', () => {
  const from = { left: 0, right: 100, y: 50 }, to = { left: 500, right: 600, y: 50 };
  const lanes = [{ left: 150, right: 250, cards: [{ top: 10, bottom: 70 }, { top: 90, bottom: 150 }] },
    { left: 300, right: 400, cards: [{ top: 20, bottom: 60 }, { top: 80, bottom: 140 }] }];
  it('crosses intermediate columns in free gaps and preserves card coordinates', () => {
    const before = JSON.stringify(lanes);
    const points = previewWaypoints(from, to, lanes);
    expect(points).toHaveLength(2);
    for (let i = 0; i < points.length; i++) {
      expect(lanes[i].cards.every(card => points[i].y < card.top || points[i].y > card.bottom)).toBe(true);
    }
    expect(JSON.stringify(lanes)).toBe(before);
    expect(accessConnector({ left: -200, right: -100, y: 50 }, from, to, DIAGRAM_STYLES.cards.segment, points)).toContain(`L250 ${points[0].y}`);
  });
  it('orders reverse routes correctly and needs no waypoint for adjacent columns', () => {
    expect(previewWaypoints(to, from, lanes).map(p => p.left)).toEqual([300, 150]);
    expect(previewWaypoints(from, { left: 120, right: 220, y: 50 }, lanes)).toEqual([]);
  });
});
