import { TracehouseStyleContext } from '@frontend/providers/TracehouseStyleContext';
import React from 'react';
import { TracehouseThemeContext, type TracehouseTheme } from '@frontend/providers/TracehouseThemeContext';

export interface RuntimeThemeProps {
  tracehouseTheme: TracehouseTheme;
  tracehouseStyleClassName: string | null;
}

/** Re-provide the theme using THIS bundle's context, not the main bundle's context. */
export function withRuntimeTheme<P extends object>(
  Component: React.ComponentType<P>,
): React.FC<P & RuntimeThemeProps> {
  const ThemedRuntime: React.FC<P & RuntimeThemeProps> = ({
    tracehouseTheme,
    tracehouseStyleClassName,
    ...props
  }) => (
    <TracehouseStyleContext.Provider value={tracehouseStyleClassName}>
      <TracehouseThemeContext.Provider value={tracehouseTheme}>
        <Component {...(props as P)} />
      </TracehouseThemeContext.Provider>
    </TracehouseStyleContext.Provider>
  );
  ThemedRuntime.displayName = `ThemedRuntime(${Component.displayName ?? Component.name ?? 'Component'})`;
  return ThemedRuntime;
}
