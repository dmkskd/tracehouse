import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

const compactOutput = process.env.TRACEHOUSE_COMPACT_TEST_OUTPUT === '1';

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
    maxWorkers: 10,
    // Generous timeout for container startup + queries
    testTimeout: 60_000,
    hookTimeout: 120_000,
    reporters: [compactOutput ? 'dot' : 'default', 'html', 'json'],
    silent: compactOutput ? 'passed-only' : false,
    outputFile: {
      html: './test-reports/integration-html/index.html',
      json: './test-reports/integration-results.json',
    },
    tags: [
      { name: 'security' },
      { name: 'merge-engine' },
      { name: 'query-analysis' },
      { name: 'analytics' },
      { name: 'storage' },
      { name: 'observability' },
      { name: 'connectivity' },
      { name: 'cluster' },
      { name: 'setup' },
      { name: 'perf' },
    ],
  },
});
