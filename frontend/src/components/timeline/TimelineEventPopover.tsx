import { useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { createPortal } from 'react-dom';

/** Escape scroll-container clipping while retaining the enclosing modal's focus scope. */
export function TimelineEventPopover({ getAnchor, width, style, children }: {
  getAnchor: () => HTMLElement | undefined; width: number; style?: CSSProperties; children: ReactNode;
}) {
  const popup = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState<{ anchor?: HTMLElement; left: number; top: number; maxHeight: number }>({ left: 0, top: 0, maxHeight: 250 });
  useLayoutEffect(() => {
    const anchor = getAnchor();
    if (!anchor) return;
    const place = () => {
      const rect = anchor.getBoundingClientRect();
      const actualWidth = Math.min(width, window.innerWidth - 24);
      const below = window.innerHeight - rect.bottom - 18;
      const above = rect.top - 18;
      const useAbove = below < 180 && above > below;
      const maxHeight = Math.max(60, Math.min(250, useAbove ? above : below));
      const height = Math.min(popup.current?.getBoundingClientRect().height || maxHeight, maxHeight);
      const next = {
        anchor,
        left: Math.max(12, Math.min(window.innerWidth - actualWidth - 12, rect.left + rect.width / 2 - actualWidth / 2)),
        top: useAbove ? Math.max(12, rect.top - height - 6) : rect.bottom + 6,
        maxHeight,
      };
      setPosition(previous => previous.anchor === anchor && previous.left === next.left && previous.top === next.top && previous.maxHeight === next.maxHeight ? previous : next);
    };
    place();
    window.addEventListener('resize', place);
    document.addEventListener('scroll', place, true);
    return () => {
      window.removeEventListener('resize', place);
      document.removeEventListener('scroll', place, true);
    };
  }, [getAnchor, width]);
  const { anchor, ...coordinates } = position;
  if (!anchor) return null;
  const target = anchor.closest('[role="dialog"][aria-modal="true"]') ?? document.body;
  return createPortal(<div ref={popup} data-testid="timeline-event-popover" style={{ ...style,
    position: 'fixed', ...coordinates, width: Math.min(width, window.innerWidth - 24), boxSizing: 'border-box', overflow: 'auto',
    zIndex: 100010, pointerEvents: 'auto',
  }}>{children}</div>, target);
}
