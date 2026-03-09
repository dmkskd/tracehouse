import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

// Resolve the single hoisted React copy so all deps (including react-router-dom,
// @react-three/*, etc.) share the exact same instance at runtime.
const reactPath = path.resolve(__dirname, '..', 'node_modules', 'react')
const reactDomPath = path.resolve(__dirname, '..', 'node_modules', 'react-dom')

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    fs: {
      // Allow serving files from the monorepo root so that workspace symlinks
      // (packages/core, packages/ui-shared) resolve correctly.
      allow: [path.resolve(__dirname, '..')],
    },
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true,
      },
    },
  },
  resolve: {
    alias: {
      // Redirect Node.js-only @clickhouse/client to the browser version
      '@clickhouse/client': '@clickhouse/client-web',
      // Pin React to the single hoisted copy — prevents "Invalid hook call"
      // when react-router-dom or workspace packages resolve a different instance
      'react': reactPath,
      'react-dom': reactDomPath,
    },
    // Ensure a single copy of React in the monorepo — workspace packages
    // (core, ui-shared) excluded from optimizeDeps can otherwise resolve
    // their own React, causing the "Invalid hook call" runtime error.
    dedupe: ['react', 'react-dom', 'react-router-dom'],
  },
  optimizeDeps: {
    // Exclude sql.js from pre-bundling as it uses WASM
    // Exclude workspace packages so changes are picked up immediately
    exclude: ['sql.js', '@tracehouse/core', '@tracehouse/ui-shared'],
  },
  build: {
    rollupOptions: {
      output: {
        // Keep WASM files as separate assets
        assetFileNames: (assetInfo) => {
          if (assetInfo.name?.endsWith('.wasm')) {
            return 'assets/[name]-[hash][extname]'
          }
          return 'assets/[name]-[hash][extname]'
        },
      },
    },
  },
})
