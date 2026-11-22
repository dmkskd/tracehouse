import { afterEach, describe, expect, it, vi } from 'vitest';
import { tracehouseBuildDefines } from '../../../vite.buildDefines';
afterEach(() => vi.unstubAllEnvs());
describe('standalone object browser build availability', () => {
  it('is disabled for absent or non-true values', () => {
    for (const value of [undefined, '', 'false', '1', 'TRUE']) {
      vi.stubEnv('TH_OBJECT_BROWSER', value);
      expect(tracehouseBuildDefines().__TH_OBJECT_BROWSER__).toBe('false');
    }
  });
  it('is enabled only by explicit true', () => {
    vi.stubEnv('TH_OBJECT_BROWSER', 'true');
    expect(tracehouseBuildDefines().__TH_OBJECT_BROWSER__).toBe('true');
  });
});
