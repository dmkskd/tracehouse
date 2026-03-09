import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['src/**/__tests__/**/*.test.ts', 'src/**/__tests__/**/*.property.test.ts'],
    exclude: ['src/__tests__/integration/**'],
  },
});
