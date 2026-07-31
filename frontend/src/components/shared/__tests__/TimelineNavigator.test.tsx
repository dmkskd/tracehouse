import { cleanup, render } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { TimelineNavigator } from '../TimelineNavigator';

beforeAll(() => {
  class ResizeObserverMock {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  vi.stubGlobal('ResizeObserver', ResizeObserverMock);
});

afterEach(cleanup);

describe('TimelineNavigator delta rendering', () => {
  it('renders increases above zero in amber and decreases below zero in cyan', () => {
    const { container } = render(
      <TimelineNavigator
        data={[
          { t: '2026-07-31 08:00:00', v: 0 },
          { t: '2026-07-31 08:00:03', v: 4_000_000 },
          { t: '2026-07-31 08:00:06', v: -2_000_000 },
        ]}
        variant="delta"
        metricMode="cpu"
        cpuCores={8}
        height={70}
        rangeStartMs={Date.parse('2026-07-31T08:00:00Z')}
        rangeEndMs={Date.parse('2026-07-31T08:01:00Z')}
        viewportStartMs={Date.parse('2026-07-31T08:00:00Z')}
        viewportEndMs={Date.parse('2026-07-31T08:00:30Z')}
        onViewportChange={() => undefined}
      />,
    );

    const amberBars = container.querySelectorAll('rect[fill="#d29922"]');
    const cyanBars = container.querySelectorAll('rect[fill="#38bdf8"]');
    const zeroLine = container.querySelector('line[stroke="var(--border-primary)"]');

    expect(amberBars).toHaveLength(2);
    expect(cyanBars).toHaveLength(1);
    expect(Number(amberBars[1].getAttribute('y'))).toBeLessThan(25);
    expect(Number(cyanBars[0].getAttribute('y'))).toBe(25);
    expect(zeroLine?.getAttribute('y1')).toBe('25');
    expect(zeroLine?.getAttribute('y2')).toBe('25');
    expect(container.querySelector('[data-testid="navigator-scale-max"]')?.textContent).toBe('50%');
    expect(container.querySelector('[data-testid="navigator-scale-min"]')?.textContent).toBe('−50%');
  });

  it('renders touching step buckets while leaving missing buckets empty', () => {
    const { container } = render(
      <TimelineNavigator
        data={[
          { t: '2026-07-31 08:00:00', v: 2_000_000 },
          { t: '2026-07-31 08:00:03', v: 4_000_000 },
          { t: '2026-07-31 08:00:12', v: 3_000_000 },
        ]}
        bucketMs={3_000}
        metricMode="cpu"
        cpuCores={8}
        rangeStartMs={Date.parse('2026-07-31T08:00:00Z')}
        rangeEndMs={Date.parse('2026-07-31T08:01:00Z')}
        viewportStartMs={Date.parse('2026-07-31T08:00:00Z')}
        viewportEndMs={Date.parse('2026-07-31T08:00:30Z')}
        onViewportChange={() => undefined}
      />,
    );

    const areas = container.querySelectorAll('svg path');
    const stops = container.querySelectorAll('linearGradient stop');
    expect(areas).toHaveLength(2);
    expect(areas[0].getAttribute('stroke-width')).toBe('2');
    expect(areas[0].getAttribute('vector-effect')).toBe('non-scaling-stroke');
    expect(areas[0].getAttribute('d')).toContain('L5,');
    expect(areas[1].getAttribute('d')).toContain('M20,');
    expect(stops[0].getAttribute('stop-opacity')).toBe('0.4');
    expect(stops[1].getAttribute('stop-opacity')).toBe('0.05');
    expect(container.querySelector('[data-testid="navigator-scale-max"]')?.textContent).toBe('50%');
    expect(container.querySelector('[data-testid="navigator-scale-min"]')?.textContent).toBe('0%');
  });

});
