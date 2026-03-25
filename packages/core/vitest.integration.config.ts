import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  resolve: {
    alias: {
      // Allow integration tests to import from the frontend queries directory
      '@frontend-queries': resolve(__dirname, '..', '..', 'frontend', 'src', 'components', 'analytics', 'queries'),
      '@frontend-analytics': resolve(__dirname, '..', '..', 'frontend', 'src', 'components', 'analytics'),
    },
  },
  test: {
    globals: true,
    include: ['src/__tests__/integration/**/*.integration.test.ts'],
    pool: 'forks',
    poolOptions: {
      forks: { maxForks: 20 },
    },
    // Generous timeout for container startup + queries
    testTimeout: 60_000,
    hookTimeout: 120_000,
  },
});
