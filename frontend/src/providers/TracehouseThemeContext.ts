import { createContext } from 'react';

export type TracehouseTheme = 'dark' | 'light';

/** Each independently compiled runtime bundle has its own instance of this context. */
export const TracehouseThemeContext = createContext<TracehouseTheme>('dark');
