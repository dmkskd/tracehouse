import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { createRef } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventLayerMenu } from '../EventLayerControls';
import { emptyEventFilter } from '../event-model';

afterEach(() => { cleanup(); vi.restoreAllMocks(); });
describe('explicit event visibility control', () => {
  it('toggles visibility through the switch while keeping filters independent', () => {
    const onVisibilityChange = vi.fn();
    const props = { visible: true, shownCount: 0, totalCount: 0, filterUniverse: [], coverage: [],
      filter: emptyEventFilter(), onFilterChange: vi.fn(), onVisibilityChange };
    const view = render(<EventLayerMenu {...props} showVisibilitySwitch />);
    fireEvent.click(screen.getByRole('button', { name: 'Event filters and source coverage' }));
    expect(onVisibilityChange).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Event filters and source coverage' }).getAttribute('aria-expanded')).toBe('true');
    fireEvent.click(screen.getByText('Events', { selector: 'label' }));
    expect(onVisibilityChange).toHaveBeenLastCalledWith(false);
    view.rerender(<EventLayerMenu {...props} visible={false} showVisibilitySwitch />);
    fireEvent.click(screen.getByRole('switch', { name: 'Show operational events' }));
    expect(onVisibilityChange).toHaveBeenLastCalledWith(true);
  });
  it('preserves the existing single-button behavior on other screens', () => {
    const onVisibilityChange = vi.fn();
    render(<EventLayerMenu visible shownCount={0} totalCount={0} filterUniverse={[]} coverage={[]}
      filter={emptyEventFilter()} onFilterChange={vi.fn()} onVisibilityChange={onVisibilityChange} />);
    expect(screen.queryByRole('switch')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Events 0' }));
    expect(onVisibilityChange).toHaveBeenCalledWith(false);
  });
  it('keeps the scene menu within its clipping boundary and constrains its height', () => {
    const boundaryRef = createRef<HTMLDivElement>();
    vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockImplementation(function (this: HTMLElement) {
      return (this.hasAttribute('data-boundary')
        ? { left: 100, right: 460, top: 100, bottom: 400, width: 360, height: 300 }
        : { left: 420, right: 450, top: 110, bottom: 140, width: 30, height: 30 }) as DOMRect;
    });
    render(<div ref={boundaryRef} data-boundary><EventLayerMenu appearance="dark" boundaryRef={boundaryRef}
      showVisibilitySwitch visible shownCount={0} totalCount={0} filterUniverse={[]} coverage={[]}
      filter={emptyEventFilter()} onFilterChange={vi.fn()} onVisibilityChange={vi.fn()} /></div>);
    const button = screen.getByRole('button', { name: 'Event filters and source coverage' });
    expect(button.hasAttribute('title')).toBe(false);
    fireEvent.click(button);
    const popup = screen.getByRole('dialog', { name: 'Event filters' });
    const left = 420 + parseFloat(popup.style.left), width = parseFloat(popup.style.width);
    expect(left).toBeGreaterThanOrEqual(108);
    expect(left + width).toBeLessThanOrEqual(452);
    expect(140 + 7 + parseFloat(popup.style.maxHeight)).toBeLessThanOrEqual(392);
  });
});
