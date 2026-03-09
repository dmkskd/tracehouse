/**
 * useThemeDetection — observes the `data-theme` attribute on <html>
 * and returns the current theme ('dark' | 'light').
 *
 * Useful for portals and 3D canvases that don't inherit CSS variables
 * from the React tree.
 */

import { useState, useEffect } from 'react';

export function useThemeDetection(): 'dark' | 'light' {
  const [theme, setTheme] = useState<'dark' | 'light'>(() => {
    if (typeof document !== 'undefined') {
      return (document.documentElement.getAttribute('data-theme') as 'dark' | 'light') || 'dark';
    }
    return 'dark';
  });

  useEffect(() => {
    const observer = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        if (mutation.attributeName === 'data-theme') {
          const newTheme = (document.documentElement.getAttribute('data-theme') as 'dark' | 'light') || 'dark';
          setTheme(newTheme);
        }
      }
    });

    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => observer.disconnect();
  }, []);

  return theme;
}
