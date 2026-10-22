import { useEffect, useRef, type RefObject } from 'react';

/** Keep expanded focus panels and the map in sync without scrolling back on
 * scroll-driven selection changes. Explicit navigation still scrolls to a panel. */
export function useDashboardScrollFocus(
  viewport: RefObject<HTMLDivElement>,
  enabled: boolean,
  focusedIndex: number | null,
  onFocusChange: (index: number) => void,
): void {
  const scrollSelection = useRef<number | null>(null);

  useEffect(() => {
    const root = viewport.current;
    if (!enabled || focusedIndex === null || !root) {
      scrollSelection.current = null;
      return;
    }
    let navigating = scrollSelection.current !== focusedIndex;
    scrollSelection.current = null;
    let frame = 0;
    let settleTimer: ReturnType<typeof setTimeout> | undefined;

    const followScroll = () => {
      frame = 0;
      if (navigating) return;
      const bounds = root.getBoundingClientRect();
      if (bounds.height <= 0) return;
      const panels = Array.from(root.querySelectorAll<HTMLElement>('[data-dashboard-panel-index]'))
        .map(element => ({ element, bounds: element.getBoundingClientRect() }))
        .filter(panel => panel.bounds.height > 0);
      if (panels.length === 0) return;
      // Use a reading line near the top; a tall panel stays active while read.
      const readingLine = bounds.top + Math.min(bounds.height / 4, 120);
      const atBottom = root.scrollTop > 0 && root.scrollTop + root.clientHeight >= root.scrollHeight - 2;
      const current = atBottom ? panels[panels.length - 1]
        : panels.find(panel => panel.bounds.top <= readingLine && panel.bounds.bottom > readingLine)
          ?? panels.find(panel => panel.bounds.bottom > readingLine);
      if (!current) return;
      const index = Number(current.element.dataset.dashboardPanelIndex);
      if (Number.isInteger(index) && index !== focusedIndex) {
        scrollSelection.current = index;
        onFocusChange(index);
      }
    };
    const schedule = () => {
      if (!frame) frame = window.requestAnimationFrame(followScroll);
    };
    const settle = () => {
      clearTimeout(settleTimer);
      settleTimer = setTimeout(() => {
        navigating = false;
        schedule();
      }, 160);
    };
    const onScroll = (event: Event) => {
      if (event.target !== root) return;
      // Smooth navigation passes other panels. Keep its destination selected
      // until scrolling stops, or until the user takes over with wheel/touch.
      if (navigating) settle();
      else schedule();
    };
    const takeOver = () => {
      navigating = false;
      clearTimeout(settleTimer);
    };

    root.addEventListener('scroll', onScroll, { passive: true });
    root.addEventListener('wheel', takeOver, { passive: true });
    root.addEventListener('touchstart', takeOver, { passive: true });
    root.addEventListener('pointerdown', takeOver, { passive: true });
    window.addEventListener('resize', schedule);
    if (navigating) {
      frame = window.requestAnimationFrame(() => {
        frame = 0;
        root.querySelector<HTMLElement>(`[data-dashboard-panel-index="${focusedIndex}"]`)
          ?.scrollIntoView({ block: 'start', behavior: 'smooth' });
      });
      settle();
    }
    return () => {
      window.cancelAnimationFrame(frame);
      clearTimeout(settleTimer);
      root.removeEventListener('scroll', onScroll);
      root.removeEventListener('wheel', takeOver);
      root.removeEventListener('touchstart', takeOver);
      root.removeEventListener('pointerdown', takeOver);
      window.removeEventListener('resize', schedule);
    };
  }, [viewport, enabled, focusedIndex, onFocusChange]);
}
