import type { ReactElement, RefObject } from 'react';
import { cleanup, render, renderHook } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { useFrame, useThree } from '@react-three/fiber';
import * as THREE from 'three';
import { ArenaEventMarker, ArenaEventPins } from '../ArenaEvents3D';
import type { ArenaEventMark } from '../arena-events';

vi.mock('@react-three/fiber', () => ({ useFrame: vi.fn(), useThree: vi.fn() }));
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

it('stacks event pins by camera depth and updates the order when the camera orbits', () => {
  const now = Date.now();
  vi.spyOn(Date, 'now').mockReturnValue(now);
  const camera = new THREE.PerspectiveCamera(50, 1, .1, 100);
  const aim = (x: number) => {
    camera.position.set(x, 5, 6);
    camera.lookAt(-2, 0, 0);
    camera.updateMatrixWorld();
  };
  aim(5);
  vi.mocked(useThree).mockReturnValue({ camera, size: { width: 1000, height: 1000 } } as ReturnType<typeof useThree>);
  const mark = (id: string, age: number): ArenaEventMark => ({
    id, time: now - age, y: .025, z: 0, hostWidth: 1, events: [],
    event: { id, occurred_at: new Date(now - age).toISOString(), hostname: 'node-01',
      kind: 'query_failure', category: 'queries', severity: 'error', precision: 'exact',
      title: 'Failure', source: 'system.query_log', capability: 'query_log' },
  });
  // Newest first is also the source order that previously put older pins on top.
  const marks = [mark('present', 0), mark('past', 30_000)];
  const pinRefs = { current: new Map<string, HTMLButtonElement>() };
  render(<ArenaEventPins marks={marks} selectedId={null} onSelect={() => {}} pinRefs={pinRefs} />);
  const frames = marks.map(mark => {
    const hook = renderHook(() => ArenaEventMarker({ mark, reducedMotion: true, selected: false,
      cardRef: { current: null }, pinRefs }));
    // Attach the scene root without a WebGL context; run the real frame callback.
    const root = hook.result.current as ReactElement & { ref: RefObject<THREE.Group | null> };
    root.ref.current = new THREE.Group();
    return vi.mocked(useFrame).mock.lastCall![0];
  });
  const frame = () => frames.forEach(callback => callback({} as never, 0));
  const depth = (id: string) => Number(pinRefs.current.get(id)!.style.getPropertyValue('--event-depth'));
  frame();
  expect(pinRefs.current.get('present')!.style.display).not.toBe('none');
  expect(pinRefs.current.get('past')!.style.display).not.toBe('none');
  expect(depth('present')).toBeGreaterThan(depth('past'));
  aim(-9);
  frame();
  expect(depth('past')).toBeGreaterThan(depth('present'));
});
