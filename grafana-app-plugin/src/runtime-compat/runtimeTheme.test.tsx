import { TracehouseStyleContext } from '@frontend/providers/TracehouseStyleContext';
// @vitest-environment jsdom
import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TracehouseThemeContext } from '@frontend/providers/TracehouseThemeContext';
import { createRuntimeComponent } from './runtimeComponent';

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe('runtime bundle theme hand-off', () => {
  it.each(['18.3.1', '19.1.0'])(
    'passes theme updates into an independently loaded React %s bundle',
    async (version) => {
      // Simulate a separate webpack build: it owns a different context object.
      vi.resetModules();
      const { TracehouseThemeContext: localContext } =
        await import('@frontend/providers/TracehouseThemeContext');
      const { useThemeDetection: useLocalTheme } = await import('@frontend/hooks/useThemeDetection');
      const { withRuntimeTheme } = await import('./runtimeTheme');
      const { createTracehousePortal } = await import('@frontend/providers/TracehousePortal');
      expect(localContext).not.toBe(TracehouseThemeContext);
      function LocalConsumer({ label }: { label: string }) {
        const theme = useLocalTheme();
        return createTracehousePortal(
          <output>
            {label}:{theme}
          </output>,
          document.body,
        );
      }
      const load = vi.fn().mockResolvedValue({ ThemeProbe: withRuntimeTheme(LocalConsumer) });
      vi.stubGlobal('System', { import: load });
      const previousVersion = React.version;
      Object.defineProperty(React, 'version', { value: version, configurable: true });
      // A conflicting page theme must not affect context consumers.
      document.documentElement.setAttribute('data-theme', 'dark');
      try {
        const Component = createRuntimeComponent<{ label: string }>('probe', 'ThemeProbe', 'Loading');
        const { rerender } = render(
          <TracehouseStyleContext.Provider value="runtime-scope">
            <TracehouseThemeContext.Provider value="light">
              <Component label="runtime" />
            </TracehouseThemeContext.Provider>
          </TracehouseStyleContext.Provider>,
        );
        expect(await screen.findByText('runtime:light')).toBeTruthy();
        expect(document.querySelector('[data-tracehouse-portal]')?.className).toBe('runtime-scope');
        expect(document.querySelector('[data-tracehouse-portal]')?.getAttribute('data-theme')).toBe('light');
        expect(load).toHaveBeenCalledWith(
          expect.stringContaining(`probe-react${version.startsWith('19') ? '19' : '18'}.js`),
        );
        rerender(
          <TracehouseStyleContext.Provider value="runtime-scope">
            <TracehouseThemeContext.Provider value="dark">
              <Component label="runtime" />
            </TracehouseThemeContext.Provider>
          </TracehouseStyleContext.Provider>,
        );
        expect(await screen.findByText('runtime:dark')).toBeTruthy();
        expect(document.querySelector('[data-tracehouse-portal]')?.getAttribute('data-theme')).toBe('dark');
        expect(load).toHaveBeenCalledTimes(1);
      } finally {
        Object.defineProperty(React, 'version', { value: previousVersion, configurable: true });
        document.documentElement.removeAttribute('data-theme');
      }
    },
  );
});
