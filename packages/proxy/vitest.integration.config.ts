import { defineConfig } from 'vitest/config';

const compactOutput = process.env.TRACEHOUSE_COMPACT_TEST_OUTPUT === '1';

export default defineConfig({
  test: {
    globals: true,
    include: ['src/__tests__/**/*.integration.test.ts'],
    pool: 'forks',
    maxWorkers: 1,
    testTimeout: 60_000,
    hookTimeout: 120_000,
    reporters: [compactOutput ? 'dot' : 'default', 'json'],
    silent: compactOutput ? 'passed-only' : false,
    outputFile: {
      json: './test-reports/results.json',
    },
    tags: [
      { name: 'connectivity' },
    ],
  },
});
