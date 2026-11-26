export interface ConnectorPoint { left: number; right: number; y: number }
export interface ConnectorLane {
  left: number; right: number;
  cards: { top: number; bottom: number }[];
}

/** Hover-only edges use existing gaps rather than inserting rows and moving cards. */
export function previewWaypoints(from: ConnectorPoint, to: ConnectorPoint, lanes: ConnectorLane[]): ConnectorPoint[] {
  const forward = to.left > from.right;
  const start = forward ? from.right : from.left, end = forward ? to.left : to.right;
  return lanes.filter(lane => lane.left > Math.min(start, end) && lane.right < Math.max(start, end))
    .sort((a, b) => forward ? a.left - b.left : b.left - a.left).map(lane => {
      const desired = from.y + (to.y - from.y) * ((lane.left + lane.right) / 2 - start) / (end - start);
      const cards = [...lane.cards].sort((a, b) => a.top - b.top);
      if (!cards.some(card => desired >= card.top - 2 && desired <= card.bottom + 2)) return { ...lane, y: desired };
      const gaps = cards.flatMap((card, i) => {
        const next = cards[i + 1];
        return next ? next.top - card.bottom >= 2 ? [(card.bottom + next.top) / 2] : [] : [card.bottom + 4];
      });
      gaps.push(cards[0].top - 4);
      return { left: lane.left, right: lane.right, y: gaps.reduce((best, gap) => Math.abs(gap - desired) < Math.abs(best - desired) ? gap : best) };
    });
}

/** Database is a routing stop, not an additional grant. Each original privilege-to-table
 * edge keeps its own table endpoint and identity, including revokes and narrow scopes. */
export function accessConnector(from: ConnectorPoint, database: ConnectorPoint, table: ConnectorPoint,
  segment: (x1: number, y1: number, x2: number, y2: number) => string, waypoints: ConnectorPoint[] = []): string {
  let path = `M${from.right} ${from.y}${segment(from.right, from.y, database.left, database.y)} L${database.right} ${database.y}`;
  let x = database.right, y = database.y;
  for (const point of waypoints) {
    if (point.left <= x || point.right >= table.left) continue;
    path += `${segment(x, y, point.left, point.y)} L${point.right} ${point.y}`;
    x = point.right;y = point.y;
  }
  return path + segment(x, y, table.left, table.y);
}
