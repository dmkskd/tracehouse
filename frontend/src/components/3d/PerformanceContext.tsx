/**
 * Performance context for 3D scene components
 * 
 * Provides performance mode state to child components for optimizing
 * rendering of large datasets.
 */

import { createContext, useContext } from 'react';
import type { PerformanceContextValue } from './types';

/**
 * Default performance context value
 */
const defaultPerformanceContext: PerformanceContextValue = {
  performanceMode: false,
  enableAnimations: true,
  maxElements: 10000,
  lodFactor: 1,
};

/**
 * Performance context for child components to access performance mode state
 */
export const PerformanceContext = createContext<PerformanceContextValue>(defaultPerformanceContext);

/**
 * Hook to access performance context in child components
 * 
 * @example
 * ```tsx
 * const MyComponent = () => {
 *   const { performanceMode, maxElements } = usePerformanceMode();
 *   
 *   // Limit rendered elements in performance mode
 *   const elementsToRender = performanceMode 
 *     ? data.slice(0, maxElements) 
 *     : data;
 *   
 *   return <>{elementsToRender.map(...)}</>;
 * };
 * ```
 */
export const usePerformanceMode = (): PerformanceContextValue => {
  return useContext(PerformanceContext);
};
