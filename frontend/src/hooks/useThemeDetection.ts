import { useContext } from 'react';
import { TracehouseThemeContext, type TracehouseTheme } from '../providers/TracehouseThemeContext';

/** Read the theme supplied by the standalone entry point or Grafana runtime bridge. */
export function useThemeDetection(): TracehouseTheme {
  return useContext(TracehouseThemeContext);
}
