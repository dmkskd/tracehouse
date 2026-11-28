import { describe, expect, it } from 'vitest';
import { CAMERA_HOME, fitCamera, homeCamera, groundPoint, projectPoint, MAX_ZOOM, MIN_ZOOM, orbitCamera, orbitCameraAtCentre, projectedBounds, zoomCamera, type CameraState } from '../useCamera';

const home: CameraState = { ...CAMERA_HOME, panX: 0, panY: 0, scale: 1 };

describe('spatial camera controls', () => {
  it('starts at tf-view angles and fits both projected dimensions into the viewport', () => {
    expect(home.tilt).toBe(54);expect(home.rotation).toBe(-45);
    const fitted = fitCamera({ ...home, panX: 123, panY: -76 }, 1400, 1800, { width: 1000, height: 600 });
    const bounds = projectedBounds(1400, 1800, home.tilt, home.rotation);
    expect(bounds.width * fitted.scale).toBeLessThanOrEqual(952);
    expect(bounds.height * fitted.scale).toBeLessThanOrEqual(552);
    expect(fitted.panX + (bounds.left + bounds.width / 2) * fitted.scale).toBeCloseTo(0);
    expect(fitted.panY + (bounds.top + bounds.height / 2) * fitted.scale).toBeCloseTo(0);
  });
  it('starts large catalogs at a readable scale and maps viewport points back to the ground plane', () => {
    const size = { width: 1500, height: 900 };
    const camera = homeCamera(2400, 6000, size);
    expect(camera.scale).toBeGreaterThanOrEqual(.8);
    const point = projectPoint(2400, 6000, camera.tilt, camera.rotation, 1200, 3000);
    expect(point.x * camera.scale + camera.panX).toBeCloseTo(0);
    expect(point.y * camera.scale + camera.panY).toBeCloseTo(0);
    for (const model of [{ x: 100, y: 100 }, { x: 2200, y: 5800 }]) {
      const projected = projectPoint(2400, 6000, camera.tilt, camera.rotation, model.x, model.y);
      const inverse = groundPoint(2400, 6000, camera, size, {
        x: size.width / 2 + camera.panX + projected.x * camera.scale,
        y: size.height / 2 + camera.panY + projected.y * camera.scale,
      });
      expect(inverse.x).toBeCloseTo(model.x);expect(inverse.y).toBeCloseTo(model.y);
    }
  });
  it('orbits continuously around the scene and clamps tilt without limiting horizontal rotation', () => {
    expect(orbitCamera(home, 100, 50)).toMatchObject({ rotation: -80, tilt: 39 });
    expect(orbitCamera(home, 10000, 10000).rotation).toBeGreaterThanOrEqual(-180);
    expect(orbitCamera(home, 10000, 10000).rotation).toBeLessThan(180);
    expect(orbitCamera(home, 0, 10000).tilt).toBe(20);
    expect(orbitCamera(home, 0, -10000).tilt).toBe(72);
  });
  it('keeps the visible centre stationary during orbit for long diagrams and after navigation', () => {
    const size = { width: 1500, height: 900 };
    for (const camera of [homeCamera(2400, 6000, size), { ...home, panX: 850, panY: -320, scale: .8 }]) {
      const anchor = groundPoint(2400, 6000, camera, size, { x: 750, y: 450 });
      for (const [dx, dy] of [[100, 50], [600, -150], [-1100, 200]]) {
        const next = orbitCameraAtCentre(camera, dx, dy, 2400, 6000, size);
        const point = projectPoint(2400, 6000, next.tilt, next.rotation, anchor.x, anchor.y);
        expect(point.x * next.scale + next.panX).toBeCloseTo(0);
        expect(point.y * next.scale + next.panY).toBeCloseTo(0);
        expect(next.scale).toBe(camera.scale);
      }
    }
  });
  it('keeps a model point under the cursor fixed when zooming after pan, including at limits', () => {
    const size = { width: 1000, height: 600 }, pointer = { x: 130, y: 420 };
    const start = { ...home, panX: 175, panY: -43, scale: .7 };
    for (const factor of [1.5, .2, 1000, .00001]) {
      const next = zoomCamera(start, factor, pointer, size);
      expect((pointer.x - size.width / 2 - next.panX) / next.scale).toBeCloseTo((pointer.x - size.width / 2 - start.panX) / start.scale);
      expect((pointer.y - size.height / 2 - next.panY) / next.scale).toBeCloseTo((pointer.y - size.height / 2 - start.panY) / start.scale);
      expect(next.scale).toBeGreaterThanOrEqual(MIN_ZOOM);expect(next.scale).toBeLessThanOrEqual(MAX_ZOOM);
    }
  });
});
