import React from 'react';
import Link from '@docusaurus/Link';
import timeTravel from '@site/../../grafana-app-plugin/src/img/screenshots/time-travel.png';
import analytics from '@site/../../grafana-app-plugin/src/img/screenshots/analytics.png';
import databaseExplorer from '@site/../../grafana-app-plugin/src/img/screenshots/database-explorer.png';
import styles from './IntroFeatures.module.css';

const previews = [
  { title: 'Time Travel', route: 'time-travel', image: timeTravel,
    alt: 'Time Travel CPU timeline with query and merge activity in Grafana',
    description: 'Correlate historical resource usage with the queries, merges, mutations, and events active at a selected time.' },
  { title: 'Analytics', route: 'analytics', image: analytics,
    alt: 'TraceHouse Analytics charts and query results in Grafana',
    description: 'Analyze ordering-key efficiency, run diagnostic SQL, build dashboards, and compare trends and correlations.' },
  { title: 'Database Explorer', route: 'database-explorer', image: databaseExplorer,
    alt: 'Database Explorer showing data parts and merge levels in a 3D lineage view',
    description: 'Browse databases, tables, parts, and columns. Inspect merge history, part lineage, and storage efficiency.' },
];

const sections = [
  { title: 'Overview', route: 'overview', description: 'See live CPU, memory, disk I/O, and network usage attributed to queries, merges, and background work.' },
  { title: 'Queries', route: 'query-monitor', description: 'Inspect live and historical queries, resource usage, scan efficiency, and distributed execution.' },
  { title: 'Merges and Mutations', route: 'merge-tracker', description: 'Track active and historical work with progress, ETA, throughput, classification, and dependencies.' },
  { title: 'Events', route: 'events', description: 'Investigate query failures, DDL, server, replication, Keeper, and storage events with their source evidence.' },
  { title: 'Cluster', route: 'cluster', description: 'Explore cluster topology and node health.' },
  { title: 'Replication', route: 'replication', description: 'Inspect replica state, queue depth, and replication lag.' },
  { title: 'Engine Internals', route: 'engine-internals', description: 'Inspect thread pools, memory allocation, CPU sampling, primary-key indexes, and dictionaries.' },
];

export default function IntroFeatures() {
  return (
    <>
      <div className={styles.previews}>
        {previews.map(feature => (
          <Link key={feature.route} to={`/docs/features/${feature.route}`} className={styles.preview}>
            <img src={feature.image} alt={feature.alt} loading="lazy" width="1400" height="1090" />
            <div className={styles.description}>
              <span className={styles.title}>{feature.title} <span aria-hidden="true">→</span></span>
              <p>{feature.description}</p>
            </div>
          </Link>
        ))}
      </div>
      <p className={styles.caption}>Screenshots show TraceHouse running as a Grafana app plugin.</p>
      <div className={styles.sections}>
        {sections.map(section => (
          <div key={section.route} className={styles.section}>
            <Link to={`/docs/features/${section.route}`} className={styles.title}>{section.title} <span aria-hidden="true">→</span></Link>
            <p>{section.description}</p>
          </div>
        ))}
      </div>
    </>
  );
}
