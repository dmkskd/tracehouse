import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    'getting-started',
    'architecture',
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/connecting',
        'guides/test-data',
        'guides/deployment',
      ],
    },
    {
      type: 'category',
      label: 'Features',
      items: [
        'features/overview',
        'features/engine-internals',
        'features/cluster',
        'features/database-explorer',
        'features/time-travel',
        'features/query-monitor',
        {
          type: 'doc',
          id: 'features/query-xray',
          label: 'Query X-Ray ᵉˣᵖ',
          className: 'sidebar-experimental',
        },
        'features/merge-tracker',
        'features/analytics',
        'features/analytics-query-language',
        'features/system-map',
        'features/grafana',
      ],
    },
    {
      type: 'category',
      label: 'Development',
      items: [
        'development/project-structure',
        'development/building',
        'development/grafana-plugin',
        'development/testing',
        'development/security-scanning',
        'development/documentation',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        'reference/metrics',
        'reference/polling',
        'reference/clickhouse-internals',
        'reference/project-structure',
        'reference/clickstack',
      ],
    },
  ],
};

export default sidebars;
