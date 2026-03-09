import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'TraceHouse',
  tagline: 'Real-time monitoring, 3D visualization, and deep observability for ClickHouse',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
  },

  url: 'https://tracehouse.dev',
  baseUrl: '/',

  organizationName: 'dmkskd',
  projectName: 'tracehouse',

  onBrokenLinks: 'throw',

  customFields: {
//     assetsBaseUrl: 'https://github.com/dmkskd/tracehouse/releases/download/docs-assets-v1',
    assetsBaseUrl: '',

  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
        },
        blog: false, // disable blog for now
        theme: {
          // Switch theme by changing the import below:
          //   theme-gold.css    — dark GitHub-style, ClickHouse yellow accent
          //   theme-electric.css — deep navy, bright cyan/teal accent
          //   theme-ember.css   — warm charcoal, orange/red accent
          //   theme-frost.css   — clean slate, indigo/violet accent
          customCss: './src/css/theme-gold.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'TraceHouse',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/dmkskd/tracehouse',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Getting Started', to: '/docs/getting-started'},
            {label: 'Deployment', to: '/docs/guides/deployment'},
            {label: 'Architecture', to: '/docs/architecture'},
          ],
        },
        {
          title: 'Guides',
          items: [
            {label: 'Connecting to ClickHouse', to: '/docs/guides/connecting'},
            {label: 'Loading Test Data', to: '/docs/guides/test-data'},
            {label: 'Deployment', to: '/docs/guides/deployment'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'Metrics & Formulas', to: '/docs/reference/metrics'},
            {label: 'Polling & Performance', to: '/docs/reference/polling'},
            {label: 'ClickHouse Internals', to: '/docs/reference/clickhouse-internals'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} TraceHouse Contributors. Licensed under the Apache License 2.0.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'sql', 'yaml', 'docker', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
