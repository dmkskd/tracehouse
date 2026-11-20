import { createContext } from 'react';

/** The Grafana entry point supplies its CSS module root; standalone owns its page styles. */
export const TracehouseStyleContext = createContext<string | null>(null);
