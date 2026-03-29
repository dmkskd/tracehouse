import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import { tracehouseBuildDefines } from './vite.buildDefines';

export default defineConfig({
  define: tracehouseBuildDefines(),
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/__tests__/setup.ts'],
    include: ['src/**/*.{test,spec}.{ts,tsx}'],
    exclude: ['node_modules', 'dist'],
    server: {
      deps: {
        inline: ['@testing-library/jest-dom'],
      },
    },
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
      { name: 'analytics' },
      { name: 'visualization' },
    ],
  },
});
