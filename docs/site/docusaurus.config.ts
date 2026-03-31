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
  baseUrl: '/tracehouse/',

  organizationName: 'dmkskd',
  projectName: 'tracehouse',

  onBrokenLinks: 'throw',

  customFields: {
    assetsBaseUrl: 'https://dmkskd.github.io/tracehouse-assets',
    // Live demo instance — set to '' to hide the demo link/button entirely
    demoUrl: 'http://178.104.103.140',
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  clientModules: ['./src/demoStatus.ts'],

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
      respectPrefersColorScheme: false,
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
          type: 'html',
          position: 'right',
          value: '<a id="navbar-demo-link" class="navbar-demo-link navbar__item navbar__link" target="_blank" rel="noopener noreferrer">Live Demo<span id="navbar-demo-dot" class="demo-status-dot" hidden></span></a>',
        },
        {
          href: 'https://github.com/dmkskd/tracehouse',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
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
        {
          title: 'Community',
          items: [
            {
              html: '<a href="https://github.com/dmkskd/tracehouse" target="_blank" rel="noopener noreferrer" class="footer__link-item"><svg width="16" height="16" viewBox="0 0 24 24" style="vertical-align:middle;margin-right:6px"><path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12" fill="currentColor"/></svg>GitHub</a>',
            },
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
