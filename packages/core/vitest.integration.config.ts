import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['src/__tests__/integration/**/*.integration.test.ts'],
    // Integration tests are slower (container startup), run sequentially
    pool: 'forks',
    poolOptions: {
      forks: { singleFork: true },
    },
    // Generous timeout for container startup + queries
    testTimeout: 60_000,
    hookTimeout: 120_000,
  },
});
