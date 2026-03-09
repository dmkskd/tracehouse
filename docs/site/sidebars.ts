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
        'features/cluster-overview',
        'features/observability-map',
        'features/database-explorer',
        'features/merge-tracker',
        'features/query-monitor',
        'features/analytics-query-language',
        'features/engine-internals',
        'features/grafana',
      ],
    },
    {
      type: 'category',
      label: 'Development',
      items: [
        'development/project-structure',
        'development/building',
        'development/testing',
        'development/security-scanning',
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
