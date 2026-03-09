import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'
import fs from 'fs'

const reactPath = path.resolve(__dirname, '..', 'node_modules', 'react')
const reactDomPath = path.resolve(__dirname, '..', 'node_modules', 'react-dom')

/**
 * Rollup plugin that writes per-module rendered sizes to a JSON file.
 * The post-build script reads this to produce a library size breakdown.
 */
function moduleSizePlugin(): Plugin {
  return {
    name: 'module-size-report',
    generateBundle(_options, bundle) {
      const modules: Record<string, number> = {}
      for (const chunk of Object.values(bundle)) {
        if (chunk.type === 'chunk' && chunk.modules) {
          for (const [id, info] of Object.entries(chunk.modules)) {
            modules[id] = (modules[id] || 0) + info.renderedLength
          }
        }
      }
      const outPath = path.resolve(__dirname, 'dist/single/module-sizes.json')
      fs.mkdirSync(path.dirname(outPath), { recursive: true })
      fs.writeFileSync(outPath, JSON.stringify(modules))
    },
  }
}

/**
 * Vite config for building the full app as a single HTML file.
 *
 * Produces one JS bundle + one CSS file in dist-single/assets/.
 * The post-build script (scripts/build-single-html.js) then inlines
 * everything into a self-contained HTML that works from file://.
 */
export default defineConfig({
  plugins: [react(), tailwindcss(), moduleSizePlugin()],
  base: './',
  resolve: {
    alias: {
      '@clickhouse/client': '@clickhouse/client-web',
      'react': reactPath,
      'react-dom': reactDomPath,
    },
    dedupe: ['react', 'react-dom', 'react-router-dom'],
  },
  optimizeDeps: {
    exclude: ['@tracehouse/core', '@tracehouse/ui-shared'],
  },
  build: {
    outDir: 'dist/single',
    sourcemap: false,
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        manualChunks: undefined,
        inlineDynamicImports: true,
      },
    },
  },
  esbuild: { legalComments: 'none' },
})
