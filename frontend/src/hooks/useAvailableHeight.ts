/**
 * useAvailableHeight — vertical space from the top of an element to the bottom
 * of the viewport that scrolls it, independent of the current scroll position.
 *
 * Measures the nearest scrolling ancestor instead of window.innerHeight, so the
 * same code works standalone (the window scrolls) and inside the Grafana app
 * plugin (a height-constrained content div below Grafana's chrome scrolls).
 *
 * Returns a callback ref to attach to the element, and the measured height
 * (null until the element is mounted).
 */

import { useState, useLayoutEffect, useCallback } from 'react';

function findScrollParent(el: HTMLElement): HTMLElement | null {
  let node = el.parentElement;
  while (node && node !== document.body && node !== document.documentElement) {
    const { overflowY } = getComputedStyle(node);
    if (overflowY === 'auto' || overflowY === 'scroll' || overflowY === 'overlay') return node;
    node = node.parentElement;
  }
  return null;
}

function measure(el: HTMLElement, scroller: HTMLElement | null): number {
  const top = el.getBoundingClientRect().top;
  if (!scroller) return window.innerHeight - (top + window.scrollY);
  const scrollerRect = scroller.getBoundingClientRect();
  return scroller.clientHeight - (top - scrollerRect.top + scroller.scrollTop);
}

export function useAvailableHeight(): [(el: HTMLElement | null) => void, number | null] {
  const [element, setElement] = useState<HTMLElement | null>(null);
  const [height, setHeight] = useState<number | null>(null);
  const ref = useCallback((el: HTMLElement | null) => setElement(el), []);

  useLayoutEffect(() => {
    if (!element) return;
    const scroller = findScrollParent(element);
    const update = () => setHeight(Math.round(measure(element, scroller)));
    update();

    // Scroller resizes cover window/Grafana layout changes; the parent covers
    // content above the element changing height (banners, wrapping headers).
    const observer = new ResizeObserver(update);
    if (scroller) observer.observe(scroller);
    if (element.parentElement) observer.observe(element.parentElement);
    window.addEventListener('resize', update);
    return () => {
      observer.disconnect();
      window.removeEventListener('resize', update);
    };
  }, [element]);

  return [ref, height];
}
