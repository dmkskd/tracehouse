import tseslint from 'typescript-eslint'
import reactHooks from 'eslint-plugin-react-hooks'

export default [{
  files: ['src/**/*.{ts,tsx}'],
  ignores: ['src/main.tsx', 'src/App.tsx'],
  languageOptions: { parser: tseslint.parser },
  plugins: { '@typescript-eslint': tseslint.plugin, 'react-hooks': reactHooks },
  rules: {
    'no-restricted-imports': ['error', {
      patterns: [{
        group: ['**/*.css', '**/*.scss', '**/*.sass', '**/*.less', '**/*.css?*', '**/*.scss?*', '**/*.sass?*', '**/*.less?*'],
        message: 'Shared frontend code is bundled into Grafana. Use scoped css from @emotion/css or inline styles; direct stylesheet imports fail plugin validation.',
      }],
      paths: [{
        name: '@emotion/react',
        importNames: ['Global'],
        message: 'Grafana prohibits Emotion Global. Use scoped css from @emotion/css.',
      }],
    }],
  },
}]
