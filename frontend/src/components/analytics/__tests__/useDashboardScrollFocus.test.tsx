import { useState } from 'react';
import { act, cleanup, fireEvent, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useDashboardScrollFocus } from '../useDashboardScrollFocus';

describe('expanded dashboard scroll focus', () => {
  let root: HTMLDivElement;
  let panels: HTMLDivElement[];
  let scrollToPanel: ReturnType<typeof vi.fn>[];

  beforeEach(() => {
    vi.useFakeTimers();
    root = document.createElement('div');
    document.body.appendChild(root);
    Object.defineProperties(root, {
      clientHeight: { value: 500 },
      scrollHeight: { value: 1260 },
    });
    vi.spyOn(root, 'getBoundingClientRect').mockImplementation(() => ({ top: 100, bottom: 600, height: 500 } as DOMRect));
    scrollToPanel = [];
    panels = Array.from({ length: 3 }, (_, index) => {
      const panel = document.createElement('div');
      panel.dataset.dashboardPanelIndex = String(index);
      const scroll = vi.fn();
      scrollToPanel.push(scroll);
      panel.scrollIntoView = scroll;
      vi.spyOn(panel, 'getBoundingClientRect').mockImplementation(() => ({
        top: 100 + index * 420 - root.scrollTop,
        bottom: 100 + (index + 1) * 420 - root.scrollTop,
        height: 420,
      } as DOMRect));
      root.appendChild(panel);
      return panel;
    });
  });

  afterEach(() => {
    cleanup();
    root.remove();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  function mount(enabled = true) {
    const viewport = { current: root };
    const hook = renderHook(({ enabled }) => {
      const [selected, select] = useState<number | null>(0);
      useDashboardScrollFocus(viewport, enabled, selected, select);
      return { selected, select };
    }, { initialProps: { enabled } });
    act(() => vi.advanceTimersByTime(200));
    scrollToPanel.forEach(scroll => scroll.mockClear());
    return hook;
  }

  function scrollTo(offset: number) {
    act(() => {
      root.scrollTop = offset;
      fireEvent.scroll(root);
      vi.advanceTimersByTime(20);
    });
  }

  it('follows manual scrolling in both directions without scrolling the viewport back', () => {
    const { result } = mount();
    scrollTo(500);
    expect(result.current.selected).toBe(1);
    scrollTo(760);
    expect(result.current.selected).toBe(2);
    scrollTo(0);
    expect(result.current.selected).toBe(0);
    act(() => vi.advanceTimersByTime(300));
    expect(scrollToPanel.every(scroll => scroll.mock.calls.length === 0)).toBe(true);
  });

  it('retains the navigation destination while smooth scrolling passes other panels', () => {
    const { result } = mount();
    act(() => result.current.select(2));
    act(() => vi.advanceTimersByTime(20));
    expect(scrollToPanel[2]).toHaveBeenCalledWith({ block: 'start', behavior: 'smooth' });
    scrollTo(100);
    expect(result.current.selected).toBe(2);
    scrollTo(500);
    expect(result.current.selected).toBe(2);
    scrollTo(760);
    act(() => vi.advanceTimersByTime(200));
    expect(result.current.selected).toBe(2);
    scrollTo(0);
    expect(result.current.selected).toBe(0);
    expect(scrollToPanel[0]).not.toHaveBeenCalled();
  });

  it('lets user input interrupt navigation and ignores scrolling inside a panel table', () => {
    const { result } = mount();
    act(() => result.current.select(2));
    act(() => vi.advanceTimersByTime(20));
    fireEvent.wheel(root);
    scrollTo(500);
    expect(result.current.selected).toBe(1);
    root.scrollTop = 0;
    fireEvent.scroll(panels[1]);
    act(() => vi.advanceTimersByTime(20));
    expect(result.current.selected).toBe(1);
  });

  it('does not follow hidden panels or run outside expanded focus mode', () => {
    const hook = mount(false);
    scrollTo(500);
    expect(hook.result.current.selected).toBe(0);
    hook.rerender({ enabled: true });
    act(() => vi.advanceTimersByTime(200));
    vi.mocked(panels[1].getBoundingClientRect).mockReturnValue({ top: 0, bottom: 0, height: 0 } as DOMRect);
    fireEvent.wheel(root);
    scrollTo(500);
    expect(hook.result.current.selected).toBe(2);
    hook.unmount();
    scrollTo(0);
    expect(scrollToPanel[2]).not.toHaveBeenCalled();
  });
});
