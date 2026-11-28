import { useRef } from 'react';
import { css } from '@emotion/css';
import { useFrame, useThree } from '@react-three/fiber';
import { Link } from 'react-router-dom';
import { buildQueryLinkUrl, buildMergeLinkUrl } from '@tracehouse/core';
import * as THREE from 'three';
import { EVENT_SEVERITY_COLORS, buildEventsUrl, eventKindName } from '../events/event-model';
import { EVENT_CATEGORY_SYMBOLS } from '../events/event-distribution-model';
import { ARENA_EVENT_HORIZON_SEC, type ArenaEventMark } from './arena-events';
import { Ts, deckBaseY } from './arena-types';

const noRay = () => {};
const vector = new THREE.Vector3();
export function ArenaEventMarker({ mark, arrival, reducedMotion, selected, cardRef, pinRefs }: {
  mark: ArenaEventMark;
  arrival?: number;
  reducedMotion: boolean;
  selected: boolean;
  cardRef: React.RefObject<HTMLDivElement>;
  pinRefs: React.RefObject<Map<string, HTMLButtonElement>>;
}) {
  const group = useRef<THREE.Group>(null);
  const pulse = useRef<THREE.Mesh>(null);
  const pulseMaterial = useRef<THREE.MeshBasicMaterial>(null);
  const band = useRef<THREE.Mesh>(null);
  const { camera, size } = useThree();
  const color = EVENT_SEVERITY_COLORS[mark.event.severity];
  const floorY = mark.entry ? deckBaseY(mark.entry.deck) + .02 : .025;
  const stemHeight = mark.entry ? 0 : .65;

  useFrame(() => {
    if (!group.current) return;
    const now = Date.now(), start = Math.max(mark.time, now - ARENA_EVENT_HORIZON_SEC * 1000);
    const end = mark.open ? now : mark.end ?? mark.time;
    const visible = end >= now - ARENA_EVENT_HORIZON_SEC * 1000 && mark.time <= now;
    group.current.visible = visible;
    const x = Math.min(0, (start - now) / 1000 * Ts);
    group.current.position.set(x, mark.y, mark.z);
    if (band.current) {
      const width = Math.max(.04, (Math.min(end, now) - start) / 1000 * Ts);
      band.current.scale.x = width;
      band.current.position.x = width / 2;
    }
    if (pulse.current && pulseMaterial.current) {
      const age = arrival === undefined ? Infinity : (now - arrival) / 1000;
      pulse.current.visible = !reducedMotion && age >= 0 && age < 2;
      pulse.current.scale.setScalar(.2 + Math.min(2, age) * .35);
      pulseMaterial.current.opacity = Math.max(0, .7 * (1 - age / 2));
    }
    // Matched events sit on their operation. Standalone server observations
    // retain a world-space stem above the floor.
    vector.set(x, mark.y + stemHeight, mark.z).project(camera);
    const px = (vector.x / 2 + .5) * size.width;
    const py = (-vector.y / 2 + .5) * size.height;
    const pin = pinRefs.current?.get(mark.id);
    if (pin) {
      // DOM hit targets keep their pixel size and are not occluded by blocks.
      pin.style.display = visible && vector.z > -1 && vector.z < 1 && px >= 0 && px <= size.width && py >= 76 && py <= size.height ? '' : 'none';
      pin.style.transform = `translate3d(${px - 22}px, ${py - 22}px, 0)`;
      pin.dataset.side = px > size.width / 2 ? 'left' : 'right';
      pin.dataset.arriving = !reducedMotion && arrival !== undefined && now - arrival < 2000 ? 'true' : 'false';
    }
    if (selected && cardRef.current) {
      vector.set(x, mark.y + .18, mark.z).project(camera);
      const card = cardRef.current;
      card.style.visibility = visible ? 'visible' : 'hidden';
      const left = Math.max(8, Math.min(size.width - card.offsetWidth - 8, (vector.x / 2 + .5) * size.width + 14));
      const top = Math.max(44, Math.min(size.height - card.offsetHeight - 36, (-vector.y / 2 + .5) * size.height - card.offsetHeight - 12));
      card.style.transform = `translate(${left}px, ${top}px)`;
    }
  });

  return <group ref={group}>
    {!mark.entry && <group raycast={noRay}>
      <mesh position={[0, stemHeight / 2, 0]}>
        <cylinderGeometry args={[.012, .012, stemHeight, 8]} />
        <meshBasicMaterial color={color} transparent opacity={.8} />
      </mesh>
      <mesh>
        <sphereGeometry args={[.035, 10, 10]} />
        <meshBasicMaterial color={color} />
      </mesh>
    </group>}
    {!mark.entry && <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, floorY - mark.y, 0]} raycast={noRay}>
      <ringGeometry args={[.16, .18, 40]} />
      <meshBasicMaterial color={color} transparent opacity={selected ? .9 : .45} depthWrite={false} side={THREE.DoubleSide} />
    </mesh>}
    <mesh ref={pulse} rotation={[-Math.PI / 2, 0, 0]} position={[0, floorY - mark.y + .004, 0]} raycast={noRay}>
      <ringGeometry args={[.94, 1, 40]} />
      <meshBasicMaterial ref={pulseMaterial} color={color} transparent depthWrite={false} side={THREE.DoubleSide} />
    </mesh>
    {mark.end !== undefined && <mesh ref={band} rotation={[-Math.PI / 2, 0, 0]} position={[0, floorY - mark.y + .008, 0]} raycast={noRay}>
      <planeGeometry args={[1, mark.entry ? .3 : mark.hostWidth]} />
      <meshBasicMaterial color={color} transparent opacity={.16} depthWrite={false} side={THREE.DoubleSide} />
    </mesh>}
    {!mark.entry && <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, floorY - mark.y + .006, 0]} raycast={noRay}>
      <planeGeometry args={[.035, mark.hostWidth]} />
      <meshBasicMaterial color={color} transparent opacity={selected ? .65 : .25} depthWrite={false} side={THREE.DoubleSide} />
    </mesh>}
  </group>;
}

const pinLayer = css`
  position: absolute; inset: 0; pointer-events: none; z-index: 11;
  .arena-event-pin { position: absolute; top: 0; left: 0; width: 44px; height: 44px;
    display: grid; place-items: center; padding: 0; border: 0; background: transparent;
    color: var(--event-color); pointer-events: auto; cursor: pointer; }
  .arena-event-badge { display: grid; place-items: center; width: 28px; height: 28px;
    border: 2px solid currentColor; border-radius: 50%; background: #131321;
    box-shadow: 0 0 0 3px #131321; font: 600 15px/1 monospace; }
  .arena-event-pin:hover, .arena-event-pin:focus-visible, .arena-event-pin[aria-pressed='true'] { z-index: 2; }
  .arena-event-pin:hover .arena-event-badge, .arena-event-pin:focus-visible .arena-event-badge,
  .arena-event-pin[aria-pressed='true'] .arena-event-badge { background: currentColor; }
  .arena-event-pin:hover .arena-event-symbol, .arena-event-pin:focus-visible .arena-event-symbol,
  .arena-event-pin[aria-pressed='true'] .arena-event-symbol { color: #131321; }
  .arena-event-pin[data-arriving='true'] .arena-event-badge { box-shadow: 0 0 0 3px #131321, 0 0 12px currentColor; }
  .arena-event-label { display: none; position: absolute; left: 44px; top: 4px; padding: 6px 8px;
    background: #131321; color: #e2e8f0; border: 1px solid var(--event-color); border-radius: 5px;
    font: 12px/1.4 monospace; text-align: left; width: max-content; max-width: min(230px, 45vw);
    pointer-events: none; overflow-wrap: anywhere; }
  .arena-event-pin[data-side='left'] .arena-event-label { left: auto; right: 44px; }
  .arena-event-pin:hover .arena-event-label, .arena-event-pin:focus-visible .arena-event-label { display: block; }
`;

export function ArenaEventPins({ marks, selectedId, onSelect, pinRefs }: {
  marks: readonly ArenaEventMark[]; selectedId: string | null; onSelect: (id: string) => void;
  pinRefs: React.RefObject<Map<string, HTMLButtonElement>>;
}) {
  return <div className={pinLayer} aria-label="Events in the arena">
    {marks.map(mark => <button key={mark.id} type="button" className="arena-event-pin"
      ref={element => { if (element) pinRefs.current?.set(mark.id, element); else pinRefs.current?.delete(mark.id); }}
      style={{ '--event-color': EVENT_SEVERITY_COLORS[mark.event.severity], display: 'none' } as React.CSSProperties}
      aria-label={`${mark.event.title} · ${mark.event.hostname || 'Unspecified host'}${mark.events.length > 1 ? ` · ${mark.events.length} events` : ''}`}
      aria-pressed={mark.id === selectedId}
      onPointerDown={event => event.stopPropagation()}
      onClick={event => { event.stopPropagation(); onSelect(mark.id); }}>
      <span className="arena-event-badge" aria-hidden="true"><span className="arena-event-symbol">
        {mark.events.length > 1 ? mark.events.length : mark.event.severity === 'error' || mark.event.severity === 'critical' ? '!' : EVENT_CATEGORY_SYMBOLS[mark.event.category]}
      </span></span>
      <span className="arena-event-label" aria-hidden="true">{eventKindName(mark.event.kind)}<br />{mark.event.hostname || 'Unspecified host'}</span>
    </button>)}
  </div>;
}

export function ArenaEventCard({ mark, domRef, onClose }: {
  mark: ArenaEventMark; domRef: React.RefObject<HTMLDivElement>; onClose: () => void;
}) {
  const color = EVENT_SEVERITY_COLORS[mark.event.severity];
  const operation = mark.entry;
  const operationUrl = operation?.queryId ? buildQueryLinkUrl(operation.queryId, operation.endTime)
    : operation?.database && operation.table && operation.partName
      ? buildMergeLinkUrl(operation.database, operation.table, operation.partName, operation.hostname, operation.startTime)
      : null;
  return <div ref={domRef} role="region" aria-label="Selected arena event" style={{
    position: 'absolute', top: 0, left: 0, zIndex: 15, width: 260, maxWidth: 'calc(100% - 16px)',
    padding: 12, border: `1px solid ${color}`, borderRadius: 7, background: 'rgba(15,15,28,.96)',
    color: '#e2e8f0', fontFamily: 'monospace', fontSize: 12, boxSizing: 'border-box',
    overflowWrap: 'anywhere', visibility: 'hidden',
  }}>
    <button type="button" aria-label="Close event details" onClick={onClose} style={{ float: 'right', color: '#cbd5e1', background: 'none', border: 0, cursor: 'pointer' }}>×</button>
    <div style={{ color, fontWeight: 600 }}>{mark.event.title}</div>
    <div style={{ marginTop: 6 }}>{mark.event.hostname || 'Unspecified host'} · {mark.event.severity}</div>
    <div style={{ marginTop: 4 }}>{new Date(mark.event.occurred_at).toLocaleTimeString()} · {mark.event.precision}</div>
    {mark.event.detail && <div style={{ marginTop: 8, maxHeight: 96, overflow: 'auto' }}>{mark.event.detail}</div>}
    <div style={{ marginTop: 8, color: '#94a3b8' }}>{mark.entry ? 'Matched operation on this server' : 'Server observation; no operation match'}</div>
    {mark.events.length > 1 && <div style={{ marginTop: 8, maxHeight: 100, overflow: 'auto' }}>
      {mark.events.map(event => <div key={event.id}><Link style={{ color: '#60a5fa' }} to={buildEventsUrl(event, 1, 'overview')} state={{ from: { path: '/overview', label: 'Overview' } }}>
        {new Date(event.occurred_at).toLocaleTimeString()} · {event.title}
      </Link></div>)}
    </div>}
    <Link to={buildEventsUrl(mark.event, 1, 'overview')} state={{ from: { path: '/overview', label: 'Overview' } }}
      style={{ display: 'inline-block', marginTop: 10, color: '#60a5fa' }}>Open in Events ↗</Link>
    {operationUrl && <Link to={operationUrl} state={{ from: { path: '/overview', label: 'Overview' } }}
      style={{ display: 'inline-block', marginTop: 8, marginLeft: 12, color: '#60a5fa' }}>Open {operation?.isMerge ? 'merge' : 'query'} ↗</Link>}
  </div>;
}
