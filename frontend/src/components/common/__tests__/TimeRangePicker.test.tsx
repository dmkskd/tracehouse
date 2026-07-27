import { fireEvent, render, screen } from '@testing-library/react';
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { TimeRangePicker } from '../TimeRangePicker';

const ORIGINAL_TZ = process.env.TZ;

describe.sequential('TimeRangePicker UTC contract', () => {
  beforeEach(() => {
    process.env.TZ = 'Europe/London';
  });

  afterAll(() => {
    process.env.TZ = ORIGINAL_TZ;
  });

  it('emits browser-local input as a canonical UTC range', () => {
    const onChange = vi.fn();
    render(<TimeRangePicker value="1 HOUR" onChange={onChange} />);

    fireEvent.click(screen.getByRole('button', { name: 'Custom' }));
    fireEvent.change(screen.getByLabelText('From'), {
      target: { value: '2026-07-27T14:13' },
    });
    fireEvent.change(screen.getByLabelText('To'), {
      target: { value: '2026-07-27T15:05' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));

    expect(onChange).toHaveBeenCalledWith(
      'CUSTOM:2026-07-27T13:13:00.000Z,2026-07-27T14:05:00.000Z',
    );
  });

  it('restores canonical UTC values into browser-local inputs', () => {
    render(
      <TimeRangePicker
        value="CUSTOM:2026-07-27T13:13:00.000Z,2026-07-27T14:05:00.000Z"
        onChange={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Custom' }));

    expect(screen.getByLabelText('From')).toHaveValue('2026-07-27T14:13');
    expect(screen.getByLabelText('To')).toHaveValue('2026-07-27T15:05');
  });
});
