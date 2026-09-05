import { describe, expect, it } from 'vitest';
import {
  barSegments,
  frontFacePoint,
  gaugeLabelAnchor,
  gaugeRowY,
  CUBE_HEIGHT,
  CUBE_WIDTH,
  GAUGE_THICKNESS,
  GAUGE_U0,
  GAUGE_U1,
} from '../distributedFlowCubeGeometry';

describe('distributedFlowCubeGeometry', () => {
  it('stacks the gauges top to bottom, so the nth bar pairs with the nth label', () => {
    const rows = [0, 1, 2].map(index => gaugeRowY(index, 3));

    // v grows downward, so index 0 must be the smallest: the labels beside the
    // cube are written in the same order and the pairing is the whole contract.
    expect(rows[0]).toBeLessThan(rows[1]);
    expect(rows[1]).toBeLessThan(rows[2]);
    // Every bar sits on the cube, below its top face and above its base.
    for (const v of rows) {
      expect(v).toBeGreaterThan(0);
      expect(v + GAUGE_THICKNESS).toBeLessThan(CUBE_HEIGHT);
    }
  });

  it('keeps the stack clear of the base whatever the gauge count', () => {
    const lowestOfThree = gaugeRowY(2, 3);
    const lowestOfOne = gaugeRowY(0, 1);
    // The stack is anchored to the base, so the bottom bar lands in the same
    // place however many bars are above it.
    expect(lowestOfOne).toBe(lowestOfThree);
  });

  it('splits a bar at the cube front corner so no quad spans both faces', () => {
    // A bar crossing the corner becomes one piece per face; each piece stays
    // inside its own half, which is what keeps the corner from being cut off.
    const crossing = barSegments(GAUGE_U0, GAUGE_U1);
    expect(crossing.map(segment => segment.face)).toEqual(['left', 'right']);
    expect(crossing[0].u1).toBe(0.5);
    expect(crossing[1].u0).toBe(0.5);

    // A bar that stops short of the corner stays a single piece.
    const shortFill = barSegments(GAUGE_U0, 0.3);
    expect(shortFill).toHaveLength(1);
    expect(shortFill[0].face).toBe('left');
  });

  it('measures the front faces from the cube anchor', () => {
    const [leftX] = frontFacePoint(100, 200, 0, 0);
    const [cornerX] = frontFacePoint(100, 200, 0.5, 0);
    const [rightX] = frontFacePoint(100, 200, 1, 0);

    expect(leftX).toBe(100 - CUBE_WIDTH / 2);
    expect(cornerX).toBe(100);
    expect(rightX).toBe(100 + CUBE_WIDTH / 2);
  });

  it('anchors a gauge caption off the cube left corner, level with its bar', () => {
    const [labelX, labelY] = gaugeLabelAnchor(100, 200, 0, 3);
    const [barX, barY] = frontFacePoint(100, 200, GAUGE_U0, gaugeRowY(0, 3) + GAUGE_THICKNESS / 2);

    // Left of the bar it names — the side no label block occupies — and on the
    // same line as it.
    expect(labelX).toBeLessThan(barX);
    expect(labelY).toBe(barY);
  });
});
