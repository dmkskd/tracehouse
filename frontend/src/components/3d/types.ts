/**
 * Type definitions for 3D visualization components
 */

import type { ReactNode } from 'react';

/**
 * Scene configuration interface matching design specification
 * Uses number for backgroundColor (Three.js hex color format)
 */
export interface SceneConfig {
  /** Background color as hex number (e.g., 0x1a1a2e) */
  backgroundColor: number;
  /** Camera position as [x, y, z] tuple */
  cameraPosition: [number, number, number];
  /** Enable orbit controls for rotation, zoom, pan */
  enableOrbitControls: boolean;
  /** Enable smooth animations for transitions */
  enableAnimations: boolean;
  /** Performance mode for large datasets */
  performanceMode: boolean;
}

/**
 * Props for Scene3D component
 */
export interface Scene3DProps {
  /** Scene configuration */
  config: SceneConfig;
  /** Child 3D elements to render */
  children: ReactNode;
  /** Optional CSS class name for the container */
  className?: string;
  /** Show performance stats (FPS, memory) - useful for debugging */
  showStats?: boolean;
  /** Callback when performance mode changes */
  onPerformanceModeChange?: (enabled: boolean) => void;
}

/**
 * Performance context value for child components
 */
export interface PerformanceContextValue {
  /** Whether performance mode is enabled */
  performanceMode: boolean;
  /** Whether animations are enabled */
  enableAnimations: boolean;
  /** Maximum number of elements to render in performance mode */
  maxElements: number;
  /** Level of detail reduction factor (1 = full, 0.5 = half) */
  lodFactor: number;
}
