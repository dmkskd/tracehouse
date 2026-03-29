import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['src/__tests__/**/*.integration.test.ts'],
    pool: 'forks',
    maxWorkers: 1,
    testTimeout: 60_000,
    hookTimeout: 120_000,
    reporters: ['default', 'json'],
    outputFile: {
      json: './test-reports/results.json',
    },
    tags: [
      { name: 'connectivity' },
    ],
  },
});
