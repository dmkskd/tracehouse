import { useEffect, useRef, useState } from 'react';
import type { ObjectCard } from '@tracehouse/core/services/object-browser';
import type { ObjectDiagramStyle } from '../../../stores/userPreferenceStore';
import { createTracehousePortal } from '../../../providers/TracehousePortal';
import { ObjectHoverCard } from '../ObjectCard';
import { placeHover, type Box } from '../hoverPlacement';

/** Viewport-anchored content is shared; renderers own the anchor's geometry. */
export function useObjectHover(cards: ReadonlyMap<string, ObjectCard>, diagramStyle: ObjectDiagramStyle = 'cards') {
  const [tip, setTip] = useState<{ key: string; anchor: Box } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const hideTip = () => { clearTimeout(timer.current);setTip(null); };
  const showTip = (key: string, element: HTMLElement) => {
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setTip({ key, anchor: element.getBoundingClientRect() }), 350);
  };
  useEffect(() => () => clearTimeout(timer.current), []);
  useEffect(() => {
    if (!tip) return;
    const hide = () => { clearTimeout(timer.current);setTip(null); };
    window.addEventListener('scroll', hide, true);window.addEventListener('mousedown', hide, true);
    return () => { window.removeEventListener('scroll', hide, true);window.removeEventListener('mousedown', hide, true); };
  }, [tip]);
  const place = (element: HTMLDivElement | null) => {
    if (!element || !tip) return;
    const { left, top } = placeHover(tip.anchor, { width: element.offsetWidth, height: element.offsetHeight }, { width: window.innerWidth, height: window.innerHeight });
    element.style.left = `${left}px`;element.style.top = `${top}px`;element.style.visibility = 'visible';
  };
  return { showTip, hideTip, tooltip: tip && cards.get(tip.key) ? createTracehousePortal(
    <ObjectHoverCard key={tip.key} ref={place} card={cards.get(tip.key)!} diagramStyle={diagramStyle}
      position={{ left: 0, top: 0, visibility: 'hidden' }} />, document.body) : null };
}
