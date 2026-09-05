/**
 * distributedFlowCubeGeometry — the isometric cube the flow diagram draws a
 * participant as, and the gauge bars painted on its two front faces.
 *
 * Pure arithmetic on an anchor point: no React, no colours, no data. It lives
 * apart from the component because the stripe order it defines is a contract
 * the labels beside the cube depend on — the nth line of text names the nth
 * bar — and a contract two files share should be testable without rendering
 * either of them.
 */

/** Cube dimensions. The anchor (x, y) is the horizontal centre of the base. */
export const CUBE_WIDTH = 62;
export const CUBE_DEPTH = 32;
export const CUBE_HEIGHT = 68;

/**
 * Gauge bars painted across both front faces, in face-local units: `u` runs
 * 0..1 across the whole front, `v` runs downward from the top of the faces.
 * Bars sit on the lower half so the shard hue still reads as a block of colour
 * above them, and wrapping the corner buys twice the bar length.
 */
export const GAUGE_U0 = 0.09;
export const GAUGE_U1 = 0.91;
export const GAUGE_THICKNESS = 5;
export const GAUGE_GAP = 4;
/** Distance from the cube's base to the bottom of the lowest bar. */
export const GAUGE_BOTTOM_INSET = 10;
/** A share this small still gets a visible sliver, so "a little" != "none". */
export const GAUGE_MIN_FILL = 0.05;

export type CubeFace = 'left' | 'right';

/**
 * A point on one of the cube's two front faces. `u` runs 0..1 across the whole
 * front of the cube — the first half on the left face, the second on the right —
 * and `v` runs downward from the top of the faces. Both faces are
 * parallelograms, so moving along u also moves the point vertically.
 */
export function frontFacePoint(x: number, base: number, u: number, v: number): [number, number] {
  const halfWidth = CUBE_WIDTH / 2;
  const topOfFaces = base - CUBE_HEIGHT;
  if (u <= 0.5) {
    const t = u * 2;
    return [x - halfWidth + t * halfWidth, topOfFaces + CUBE_DEPTH / 2 + t * (CUBE_DEPTH / 2) + v];
  }
  const t = u * 2 - 1;
  return [x + t * halfWidth, topOfFaces + CUBE_DEPTH - t * (CUBE_DEPTH / 2) + v];
}

/**
 * Polygon points for one isometric bar segment lying on a front face. Segments
 * stop at the cube's front corner: a single quad spanning both faces would cut
 * the corner off and stop looking like paint on a solid.
 */
export function frontFaceBar(x: number, base: number, u0: number, u1: number, v: number): string {
  return [
    frontFacePoint(x, base, u0, v),
    frontFacePoint(x, base, u1, v),
    frontFacePoint(x, base, u1, v + GAUGE_THICKNESS),
    frontFacePoint(x, base, u0, v + GAUGE_THICKNESS),
  ]
    .map(([px, py]) => `${px.toFixed(2)},${py.toFixed(2)}`)
    .join(' ');
}

/** Splits a bar at the front corner, so each piece sits on exactly one face. */
export function barSegments(u0: number, u1: number): { face: CubeFace; u0: number; u1: number }[] {
  const segments: { face: CubeFace; u0: number; u1: number }[] = [];
  if (u0 < 0.5) segments.push({ face: 'left', u0, u1: Math.min(u1, 0.5) });
  if (u1 > 0.5) segments.push({ face: 'right', u0: Math.max(u0, 0.5), u1 });
  return segments;
}

/**
 * Vertical offset of the nth bar, counted so the stack sits above the base.
 *
 * Index 0 is the topmost bar, which is what makes the bars read in the same
 * order as the metric lines beside the cube.
 */
export function gaugeRowY(index: number, count: number): number {
  const fromBottom = (count - index) * GAUGE_THICKNESS + (count - 1 - index) * GAUGE_GAP;
  return CUBE_HEIGHT - GAUGE_BOTTOM_INSET - fromBottom;
}

/**
 * Where a bar's caption goes: level with the bar, off the cube's left corner,
 * which is the side no label block occupies.
 */
export function gaugeLabelAnchor(x: number, base: number, index: number, count: number): [number, number] {
  const [px, py] = frontFacePoint(x, base, GAUGE_U0, gaugeRowY(index, count) + GAUGE_THICKNESS / 2);
  return [px - 8, py];
}
