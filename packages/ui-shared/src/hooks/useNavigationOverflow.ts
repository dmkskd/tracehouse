import { useCallback, useLayoutEffect, useRef, useState } from 'react';

import { countVisibleNavigationItems } from '../navigation.js';

export interface NavigationOverflowOptions {
  /** Item keys in display order. Must be referentially stable across renders. */
  itemKeys: readonly string[];
  /** Gap between adjacent items, matching the row's CSS gap. */
  gap: number;
  /**
   * Changing this re-measures. Pass the active route: the overflow trigger is relabelled
   * with the active page, which changes the width it reserves.
   */
  resetKey?: string;
}

/**
 * Fit as many nav items as the row can hold, reporting the rest as overflow.
 *
 * Each item's width is measured once while it is on screen and then cached. A folded item is
 * not in the DOM and has no width to read back, and the labels are static, so one
 * measurement holds. With the widths known the count is computed in a single pass, rather
 * than by removing items until the row stops overflowing — that approach can only answer
 * "drop one", so it has to restart from the full row on every resize and visibly steps down.
 *
 * Measuring in a layout effect settles the count before paint, so no overflowing row is ever
 * shown. Attach `rowRef` to an element whose width comes from the space beside it rather
 * than from its contents (`flex: 1 1 0%`); otherwise folding an item shrinks the row and
 * re-triggers the measurement that folded it.
 */
export function useNavigationOverflow({ itemKeys, gap, resetKey }: NavigationOverflowOptions) {
  const rowRef = useRef<HTMLElement | null>(null);
  const triggerRef = useRef<HTMLElement | null>(null);
  const items = useRef(new Map<string, HTMLElement>());
  const widths = useRef(new Map<string, number>());
  const [visibleCount, setVisibleCount] = useState(itemKeys.length);

  const registerItem = useCallback((key: string, element: HTMLElement | null) => {
    if (element) items.current.set(key, element);
    else items.current.delete(key);
  }, []);

  useLayoutEffect(() => {
    const row = rowRef.current;
    if (!row) return;

    const fit = () => {
      items.current.forEach((element, key) => {
        if (element.offsetWidth > 0) widths.current.set(key, element.offsetWidth);
      });

      const itemWidths = itemKeys.map(key => widths.current.get(key));
      // Nothing to compute until every item has been seen on screen at least once.
      if (itemWidths.some(width => width === undefined)) return;

      setVisibleCount(countVisibleNavigationItems({
        itemWidths: itemWidths as number[],
        gap,
        availableWidth: row.clientWidth,
        overflowTriggerWidth: triggerRef.current?.offsetWidth ?? 0,
      }));
    };

    fit();
    const observer = new ResizeObserver(fit);
    observer.observe(row);
    return () => observer.disconnect();
  }, [itemKeys, gap, resetKey]);

  return { rowRef, triggerRef, registerItem, visibleCount };
}
