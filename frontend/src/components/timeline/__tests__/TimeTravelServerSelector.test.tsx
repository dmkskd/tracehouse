import { useState } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TimeTravelServerSelector } from '../TimeTravelServerSelector';

const hosts = ['server-a', 'server-b', 'server-c', 'server-d', 'server-e'];
function Selector() {
  const [selected, setSelected] = useState<string[]>([]);
  return <TimeTravelServerSelector hosts={hosts} selectedHosts={selected} onChange={setSelected} />;
}

afterEach(cleanup);

describe('TimeTravelServerSelector', () => {
  it('uses inline buttons for four servers and emits selection through onChange', () => {
    const onChange = vi.fn();
    render(<TimeTravelServerSelector hosts={hosts.slice(0, 4)} selectedHosts={['server-a']} onChange={onChange} />);
    expect(screen.queryByRole('button', { expanded: false })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'server-b' }), { shiftKey: true });
    expect(onChange).toHaveBeenLastCalledWith(['server-a', 'server-b']);
    fireEvent.click(screen.getByRole('button', { name: 'server-c' }));
    expect(onChange).toHaveBeenLastCalledWith(['server-c']);
    fireEvent.click(screen.getByRole('button', { name: 'All servers' }));
    expect(onChange).toHaveBeenLastCalledWith([]);
  });

  it('filters a larger cluster and supports single, multiple, and all-server selection', () => {
    render(<Selector />);
    fireEvent.click(screen.getByRole('button', { name: 'All servers' }));
    const search = screen.getByRole('searchbox', { name: 'Search servers' });
    expect(search).toHaveFocus();
    fireEvent.change(search, { target: { value: 'SERVER-B' } });
    expect(screen.getAllByRole('checkbox')).toHaveLength(1);
    fireEvent.click(screen.getByRole('button', { name: 'Select only server-b' }));
    expect(screen.getByRole('button', { name: 'server-b' })).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('checkbox')).toBeDisabled();
    fireEvent.change(search, { target: { value: '' } });
    fireEvent.click(screen.getByRole('checkbox', { name: 'server-c' }));
    expect(screen.getByRole('button', { name: '2 servers' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'All servers (5)' }));
    expect(screen.getAllByRole('checkbox').every(box => (box as HTMLInputElement).checked)).toBe(true);
    fireEvent.click(screen.getByRole('checkbox', { name: 'server-a' }));
    expect(screen.getByRole('button', { name: '4 servers' })).toBeInTheDocument();
  });

  it('shows no matches and closes on Escape or an outside pointer', () => {
    render(<Selector />);
    const trigger = screen.getByRole('button', { name: 'All servers' });
    fireEvent.click(trigger);
    const search = screen.getByRole('searchbox');
    fireEvent.change(search, { target: { value: 'unknown' } });
    expect(screen.getByText('No matching servers')).toBeInTheDocument();
    fireEvent.keyDown(search, { key: 'Escape' });
    expect(trigger).toHaveFocus();
    expect(screen.queryByRole('searchbox')).not.toBeInTheDocument();
    fireEvent.click(trigger);
    expect(screen.getByRole('searchbox')).toHaveValue('');
    fireEvent.pointerDown(document.body);
    expect(screen.queryByRole('searchbox')).not.toBeInTheDocument();
  });

  it.each(['checkbox', 'Only', 'All servers'])('processes %s clicks when pointer interaction blurs search without a focus target', action => {
    render(<Selector />);
    fireEvent.click(screen.getByRole('button', { name: 'All servers' }));
    if (action === 'All servers') {
      fireEvent.click(screen.getByRole('button', { name: 'Select only server-b' }));
    }
    const target = action === 'checkbox'
      ? screen.getByRole('checkbox', { name: 'server-a' })
      : screen.getByRole('button', { name: action === 'Only' ? 'Select only server-b' : 'All servers (5)' });
    fireEvent.pointerDown(target);
    fireEvent.mouseDown(target);
    // macOS browsers may blur the input without focusing the clicked control.
    fireEvent.blur(screen.getByRole('searchbox'), { relatedTarget: null });
    expect(target).toBeInTheDocument();
    fireEvent.mouseUp(target);
    fireEvent.click(target);
    expect(screen.getByRole('button', {
      name: action === 'checkbox' ? '4 servers' : action === 'Only' ? 'server-b' : 'All servers',
    })).toHaveAttribute('aria-expanded', 'true');
  });

  it('closes when keyboard focus moves outside the selector', () => {
    render(<><Selector /><button type="button">Outside</button></>);
    fireEvent.click(screen.getByRole('button', { name: 'All servers' }));
    fireEvent.focusIn(screen.getByRole('button', { name: 'Select only server-a' }));
    expect(screen.getByRole('searchbox')).toBeInTheDocument();
    fireEvent.focusIn(screen.getByRole('button', { name: 'Outside' }));
    expect(screen.queryByRole('searchbox')).not.toBeInTheDocument();
  });
});
