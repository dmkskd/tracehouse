import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { SceneOverview } from '../SceneOverview';
import { CAMERA_HOME } from '../useCamera';
import type { SpatialScene } from '../layout';

afterEach(() => { cleanup();vi.unstubAllGlobals(); });
it('drags the viewport without snapping its centre, and stops on release or cancellation', () => {
  vi.stubGlobal('PointerEvent', MouseEvent);
  vi.stubGlobal('DOMPoint', class {
    constructor(public x: number, public y: number) {}
    matrixTransform() { return this; }
  });
  const navigate = vi.fn();
  const scene: SpatialScene = { width: 1000, height: 1000, lanes: [], groups: [], tiles: [], anchors: new Map() };
  render(<SceneOverview scene={scene} camera={{ ...CAMERA_HOME, scale: 1, panX: 0, panY: 0 }} frame={{ width: 800, height: 600 }} onNavigate={navigate} />);
  const map = screen.getByRole('button');
  Object.assign(map, { getScreenCTM: () => ({ inverse: () => ({}) }), setPointerCapture: vi.fn(), hasPointerCapture: () => true, releasePointerCapture: vi.fn() });
  fireEvent.pointerDown(map.querySelector('polygon')!, { button: 0, clientX: 100, clientY: 200 });
  expect(navigate).not.toHaveBeenCalled();
  fireEvent.pointerMove(map, { clientX: 140, clientY: 230 });
  expect(navigate).toHaveBeenLastCalledWith(540, 530);
  fireEvent.pointerUp(map);
  fireEvent.pointerMove(map, { clientX: 160, clientY: 260 });
  expect(navigate).toHaveBeenCalledTimes(1);
  fireEvent.pointerDown(map, { button: 0, clientX: 300, clientY: 400 });
  expect(navigate).toHaveBeenLastCalledWith(300, 400);
  fireEvent.pointerMove(map, { clientX: 350, clientY: 450 });
  expect(navigate).toHaveBeenLastCalledWith(350, 450);
  fireEvent.pointerCancel(map);
  fireEvent.pointerMove(map, { clientX: 370, clientY: 470 });
  expect(navigate).toHaveBeenCalledTimes(3);
});
