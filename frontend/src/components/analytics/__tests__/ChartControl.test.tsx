import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ChartControl } from '../QueryExplorer';

describe('ChartControl', () => {
  it('keeps explicit host-safe select dimensions and remains accessible', () => {
    const onChange = vi.fn();

    render(
      <ChartControl
        label="Group By"
        value="table"
        options={[
          ['database', 'database'],
          ['table', 'table'],
        ]}
        onChange={onChange}
      />,
    );

    const select = screen.getByRole('combobox', { name: 'Group By' });
    expect(select).toHaveClass('tracehouse-chart-control-select');
    expect(select).toHaveStyle({
      height: '24px',
      minHeight: '24px',
      lineHeight: '18px',
      boxSizing: 'border-box',
      flexShrink: '0',
    });

    fireEvent.change(select, { target: { value: 'database' } });
    expect(onChange).toHaveBeenCalledWith('database');
  });
});
