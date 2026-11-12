import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';

export default defineConfig({
  resolve: {
    alias: {
      '@frontend': fileURLToPath(new URL('../frontend/src', import.meta.url)),
      '@tracehouse/core': fileURLToPath(new URL('../packages/core/dist', import.meta.url)),
    },
  },
});
