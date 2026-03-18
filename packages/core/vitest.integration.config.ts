import { defineConfig } from 'vitest/config';

export default defineConfig({
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
