import React, { useEffect, useState } from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './InteractiveFeatures.module.css';

type Feature = {
  title: string;
  doc: string;
  description: string | React.ReactNode;
  imageUrl: string;
  videoFile?: string; // filename only - resolved against assetsBaseUrl
};

const features: Feature[] = [
  {
    title: 'Time Travel',
    doc: 'time-travel',
    description: 'Correlate historical resource metrics with recorded queries, merges, and mutations.',
    imageUrl: '/img/bg.png',
    videoFile: 'time-travel.mp4',
  },
  {
    title: 'Real-time View',
    doc: 'overview',
    description: 'Monitor live CPU, memory, disk I/O, and network usage. Select a resource attribution to inspect the associated queries, merges, or parts.',
    imageUrl: '/img/bg.png',
    videoFile: 'overview.mp4',
  },
  {
    title: 'Database Explorer',
    doc: 'database-explorer',
    description: 'Browse databases, tables, parts, and columns. Inspect part lineage, merge history, and storage efficiency.',
    imageUrl: '/img/bg.png',
    videoFile: 'database-explorer.mp4',
  },
  {
    title: 'Merge Tracker',
    doc: 'merge-tracker',
    description: 'Live merge monitoring with dependency diagrams, timeline views, and throughput analysis.',
    imageUrl: '/img/bg.png',
    videoFile: 'merge-tracker.mp4',
  },
  {
    title: 'Query Monitor',
    doc: 'query-monitor',
    description: 'Running queries with per-query resource attribution, query anatomy breakdowns, and historical analysis from query_log.',
    imageUrl: '/img/bg.png',
    videoFile: 'history.mp4',
  },
  {
    title: 'Engine Internals',
    doc: 'engine-internals',
    description: <>Visualizations of ClickHouse engine internals, with reference to <a href="https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf" target="_blank" rel="noopener noreferrer"><em>ClickHouse – Lightning Fast Analytics for Everyone</em></a> (VLDB 2024). Thread pools, memory allocators, CPU sampling, PK index efficiency, and dictionaries.</>,
    imageUrl: '/img/bg.png',
    videoFile: 'engine-internals.mp4',
  },
  {
    title: 'System Map',
    doc: 'system-map',
    description: <>An interactive map of ClickHouse system tables, metrics, and diagnostics organized by subsystem. Inspired by <a href="https://www.brendangregg.com/linuxperf.html" target="_blank" rel="noopener noreferrer">Brendan Gregg's Linux Performance Tools</a> and <a href="https://presentations.clickhouse.com/meetup_wroclaw_2025/" target="_blank" rel="noopener noreferrer">Azat Khuzhin's "Know Your ClickHouse"</a>.</>,
    imageUrl: '/img/bg.png',
    videoFile: 'system-map.mp4',
  },
  {
    title: 'Analytics',
    doc: 'analytics',
    description: 'A query editor and dashboard builder for creating reusable views of cluster metrics. Queries can be saved, shared, and pinned to dashboards.',
    imageUrl: '/img/bg.png',
    videoFile: 'analytics.mp4',
  },
  {
    title: 'App Internal Monitor',
    doc: 'monitoring',
    description: 'Tracks the monitoring tool\'s own CPU, memory, and network usage per request to measure its overhead on the cluster.',
    imageUrl: '/img/bg.png',
    videoFile: 'app-monitor.mp4',
  },
  {
    title: 'Grafana Plugin',
    doc: 'grafana',
    description: 'Deploys as a Grafana app plugin. Uses a configured Grafana ClickHouse data source and the current Grafana session for query execution.',
    imageUrl: '/img/bg.png',
    videoFile: 'grafana.mp4',
  },
];

const InteractiveFeatures = () => {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const [expandedSrc, setExpandedSrc] = useState<string | null>(null);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setExpandedSrc(null);
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  return (
    <section className={styles.featuresSection}>
      <div className="container">
        <div className={styles.stickyContainer}>
          {features.map((feature, index) => (
            <div
              key={index}
              className={`${styles.featureItem} ${index % 2 === 0 ? styles.textLeft : styles.textRight}`}
            >
              <div className={styles.textContainer}>
                <h2><Link to={`/docs/features/${feature.doc}`}>{feature.title}</Link></h2>
                <p>{feature.description}</p>
              </div>
              <div className={styles.imageContainer}>
                {feature.videoFile ? (
                  <video
                    src={`${assetsBaseUrl}/${feature.videoFile}`}
                    autoPlay
                    loop
                    muted
                    playsInline
                    className={styles.media}
                    onClick={() => setExpandedSrc(`${assetsBaseUrl}/${feature.videoFile}`)}
                  />
                ) : (
                  <img
                    src={feature.imageUrl}
                    alt={feature.title}
                    className={styles.media}
                    onClick={() => setExpandedSrc(feature.imageUrl)}
                  />
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
      {expandedSrc && (
        <div className={styles.overlay} onClick={() => setExpandedSrc(null)}>
          <video
            src={expandedSrc}
            autoPlay
            loop
            muted
            playsInline
            className={styles.overlayMedia}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
    </section>
  );
};

export default InteractiveFeatures;
