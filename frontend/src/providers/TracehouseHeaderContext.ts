import { createContext, type RefObject } from 'react';

/** The owning app supplies its header; standalone dashboards use the 48px fallback. */
export const TracehouseHeaderContext = createContext<RefObject<HTMLElement> | null>(null);
