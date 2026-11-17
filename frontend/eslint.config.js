import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import { defineConfig, globalIgnores } from 'eslint/config'
import grafanaStyles from './eslint.grafana-styles.config.js'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
  },
  {
    files: ['src/**/*.{ts,tsx}'],
    ignores: ['src/components/timeline/resource-sankey/**'],
    rules: {
      'no-restricted-syntax': ['error', {
        selector: 'ImportDeclaration[source.value=/resource-sankey.+/]',
        message: 'Import the resource Sankey views through its public directory index only.',
      }],
    },
  },
  ...grafanaStyles,
])
