import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { createPortal } from 'react-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ThemeProvider, useTheme } from '../ThemeProvider';
import { useThemeDetection } from '../../hooks/useThemeDetection';

function PortalTheme() {
  const theme = useThemeDetection();
  return createPortal(<output>{theme}</output>, document.body);
}

function Controls() {
  const { toggleTheme } = useTheme();
  return <button onClick={toggleTheme}>Toggle theme</button>;
}

beforeEach(() => {
  const values = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, value); },
    removeItem: (key: string) => { values.delete(key); },
  });
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  document.documentElement.removeAttribute('data-theme');
});

describe('standalone theme provider', () => {
  it('migrates the saved preference and updates context consumers in portals', () => {
    localStorage.setItem('tracehouse-theme', 'light');
    render(<ThemeProvider><Controls /><PortalTheme /></ThemeProvider>);
    expect(screen.getByText('light')).toBeTruthy();
    expect(localStorage.getItem('tracehouse-theme')).toBe('light');
    expect(localStorage.getItem('dmkskd-tracehouse-app:tracehouse-theme')).toBe('light');
    fireEvent.click(screen.getByRole('button', { name: 'Toggle theme' }));
    expect(screen.getByText('dark')).toBeTruthy();
    expect(localStorage.getItem('dmkskd-tracehouse-app:tracehouse-theme')).toBe('dark');
  });
});
