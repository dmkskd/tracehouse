/**
 * Configuration utilities for 3D scene components
 */

import type { SceneConfig } from './types';

/**
 * Default scene configuration
 */
export const defaultSceneConfig: SceneConfig = {
  backgroundColor: 0x1a1a2e,
  cameraPosition: [5, 5, 5],
  enableOrbitControls: true,
  enableAnimations: true,
  performanceMode: false,
};

/**
 * Helper function to create a SceneConfig with partial overrides
 */
export const createSceneConfig = (overrides: Partial<SceneConfig> = {}): SceneConfig => ({
  ...defaultSceneConfig,
  ...overrides,
});
