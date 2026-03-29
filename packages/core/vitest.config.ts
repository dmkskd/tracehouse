import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['src/**/__tests__/**/*.test.ts', 'src/**/__tests__/**/*.property.test.ts'],
    exclude: ['src/__tests__/integration/**'],
    reporters: ['default', 'html', 'json'],
    outputFile: {
      html: './test-reports/html/index.html',
      json: './test-reports/results.json',
    },
    tags: [
      { name: 'connectivity' },
      { name: 'query-analysis' },
      { name: 'observability' },
      { name: 'storage' },
      { name: 'merge-engine' },
      { name: 'cluster' },
      { name: 'security' },
    ],
  },
});
