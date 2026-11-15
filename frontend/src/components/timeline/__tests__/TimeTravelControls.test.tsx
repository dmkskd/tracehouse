import { useState } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TimeTravelControls } from '../TimeTravelControls';

const hosts = ['server-a', 'server-b', 'server-c', 'server-d', 'server-e'];
afterEach(cleanup);

describe('TimeTravelControls', () => {
  it.each([
    { count: 4, split: false }, { count: 4, split: true },
    { count: 5, split: false }, { count: 5, split: true },
  ])('preserves split=$split when selecting servers from a $count-server cluster', ({ count, split }) => {
    const onLayoutChange = vi.fn();
    function Controls() {
      const [selection, setSelection] = useState<string[]>(['server-a']);
      return <TimeTravelControls hosts={hosts.slice(0, count)} selectedHosts={selection}
        onSelectedHostsChange={setSelection} perServerView={split}
        onPerServerViewChange={onLayoutChange} splitAvailable />;
    }
    render(<Controls />);
    if (count === 4) {
      fireEvent.click(screen.getByRole('button', { name: 'server-b' }), { ctrlKey: true });
      expect(screen.getByRole('button', { name: 'server-b' })).toHaveAttribute('aria-pressed', 'true');
      fireEvent.click(screen.getByRole('button', { name: 'server-c' }));
      expect(screen.getByRole('button', { name: 'server-c' })).toHaveAttribute('aria-pressed', 'true');
    } else {
      fireEvent.click(screen.getByRole('button', { name: 'server-a' }));
      fireEvent.click(screen.getByRole('checkbox', { name: 'server-b' }));
      expect(screen.getByRole('button', { name: '2 servers' })).toBeInTheDocument();
      fireEvent.click(screen.getByRole('button', { name: 'Select only server-c' }));
      expect(screen.getByRole('button', { name: 'server-c' })).toBeInTheDocument();
    }
    expect(onLayoutChange).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'By Server' })).toHaveAttribute('aria-pressed', String(split));
    expect(screen.getByRole('button', { name: 'Aggregated' })).toHaveAttribute('aria-pressed', String(!split));
  });

  it('changes layout only through the layout buttons', () => {
    const onSelectionChange = vi.fn();
    function Controls() {
      const [split, setSplit] = useState(false);
      return <TimeTravelControls hosts={hosts} selectedHosts={[]} onSelectedHostsChange={onSelectionChange}
        perServerView={split} onPerServerViewChange={setSplit} splitAvailable />;
    }
    render(<Controls />);
    fireEvent.click(screen.getByRole('button', { name: 'By Server' }));
    expect(screen.getByRole('button', { name: 'By Server' })).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(screen.getByRole('button', { name: 'Aggregated' }));
    expect(screen.getByRole('button', { name: 'Aggregated' })).toHaveAttribute('aria-pressed', 'true');
    expect(onSelectionChange).not.toHaveBeenCalled();
  });

  it('keeps aggregation when split is unavailable and inline servers are modifier-selected', () => {
    const onLayoutChange = vi.fn();
    const onSelectionChange = vi.fn();
    render(<TimeTravelControls hosts={hosts.slice(0, 4)} selectedHosts={['server-a']}
      onSelectedHostsChange={onSelectionChange} perServerView={false}
      onPerServerViewChange={onLayoutChange} splitAvailable={false} />);
    fireEvent.click(screen.getByRole('button', { name: 'server-b' }), { metaKey: true });
    expect(onSelectionChange).toHaveBeenCalledWith(['server-a', 'server-b']);
    expect(screen.getByRole('button', { name: 'By Server' })).toBeDisabled();
    fireEvent.click(screen.getByRole('button', { name: 'By Server' }));
    expect(onLayoutChange).not.toHaveBeenCalled();
  });
});
