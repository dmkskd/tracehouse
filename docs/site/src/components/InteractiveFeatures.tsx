import React, { useEffect, useRef, useState } from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './InteractiveFeatures.module.css';

type Feature = {
  title: string;
  description: string | React.ReactNode;
  imageUrl: string;
  videoFile?: string; // filename only — resolved against assetsBaseUrl
};

const features: Feature[] = [
  {
    title: 'Time Travel',
    description: 'Inspect queries, merges, and mutations at any point in time. See what the cluster was doing and when.',
    imageUrl: '/img/bg.png',
    videoFile: 'time-travel.mp4',
  },
  {
    title: 'Real-time View',
    description: 'Watch your cluster workload as it happens. CPU, memory, disk I/O, and network attributed in real time. From here you can deep dive into queries, merges, or parts from any metric.',
    imageUrl: '/img/bg.png',
    videoFile: 'overview.mp4',
  },
  {
    title: 'Database Explorer',
    description: 'Browse databases, tables, parts, and columns. Inspect part lineage, merge history, and storage efficiency.',
    imageUrl: '/img/bg.png',
    videoFile: 'database-explorer.mp4',
  },
  {
    title: 'Merge Tracker',
    description: 'Live merge monitoring with dependency diagrams, timeline views, and throughput analysis.',
    imageUrl: '/img/bg.png',
    videoFile: 'merge-tracker.mp4',
  },
  {
    title: 'Query Monitor',
    description: 'Running queries with per-query resource attribution, query anatomy breakdowns, and historical analysis from query_log.',
    imageUrl: '/img/bg.png',
    videoFile: 'history.mp4',
  },
  {
    title: 'Engine Internals',
    description: <>An attempt to visualise the internals of the ClickHouse engine as described in <a href="https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf" target="_blank" rel="noopener noreferrer"><em>ClickHouse – Lightning Fast Analytics for Everyone</em></a> (VLDB 2024). Thread pools, memory allocators, CPU sampling, PK index efficiency, and dictionaries.</>,
    imageUrl: '/img/bg.png',
    videoFile: 'engine-internals.mp4',
  },
  {
    title: 'System Map',
    description: <>An interactive map of ClickHouse system tables, metrics, and diagnostics organized by subsystem. Inspired by <a href="https://www.brendangregg.com/linuxperf.html" target="_blank" rel="noopener noreferrer">Brendan Gregg's Linux Performance Tools</a> and <a href="https://presentations.clickhouse.com/meetup_wroclaw_2025/" target="_blank" rel="noopener noreferrer">Azat Khuzhin's "Know Your ClickHouse"</a>.</>,
    imageUrl: '/img/bg.png',
    videoFile: 'system-map.mp4',
  },
  {
    title: 'Analytics',
    description: 'A query editor and dashboard builder for creating reusable views of cluster metrics. Queries can be saved, shared, and pinned to dashboards.',
    imageUrl: '/img/bg.png',
    videoFile: 'analytics.mp4',
  },
  {
    title: 'App Internal Monitor',
    description: 'Tracks the monitoring tool\'s own CPU, memory, and network usage per request to measure its overhead on the cluster.',
    imageUrl: '/img/bg.png',
    videoFile: 'app-monitor.mp4',
  },
  {
    title: 'Grafana Plugin',
    description: 'Deploys as a Grafana app plugin. Reuses the existing Grafana ClickHouse datasource and Grafana permissions, no separate configuration needed.',
    imageUrl: '/img/bg.png',
    videoFile: 'grafana.mp4',
  },
];

const InteractiveFeatures = () => {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const featureRefs = useRef<(HTMLDivElement | null)[]>([]);
  const videoRefs = useRef<(HTMLVideoElement | null)[]>([]);
  const [expandedSrc, setExpandedSrc] = useState<string | null>(null);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setExpandedSrc(null);
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add(styles.visible);
            const video = (entry.target as HTMLElement).querySelector('video');
            if (video) {
              video.currentTime = 0;
              video.play().catch(() => { });
            }
          } else {
            entry.target.classList.remove(styles.visible);
            const video = (entry.target as HTMLElement).querySelector('video');
            if (video) {
              video.pause();
            }
          }
        });
      },
      {
        threshold: 0.5, // Start playing when 50% of the item is visible
      }
    );

    featureRefs.current.forEach((ref) => {
      if (ref) {
        observer.observe(ref);
      }
    });

    // Fade out → reset → fade in on each loop restart
    const handleEnded = (e: Event) => {
      const video = e.target as HTMLVideoElement;
      video.classList.add(styles.videoFadedOut);
      setTimeout(() => {
        video.currentTime = 0;
        video.classList.remove(styles.videoFadedOut);
        video.play().catch(() => { });
      }, 400);
    };
    const videos = videoRefs.current.filter(Boolean) as HTMLVideoElement[];
    videos.forEach((v) => v.addEventListener('ended', handleEnded));

    return () => {
      featureRefs.current.forEach((ref) => {
        if (ref) {
          observer.unobserve(ref);
        }
      });
      videos.forEach((v) => v.removeEventListener('ended', handleEnded));
    };
  }, []);

  return (
    <section className={styles.featuresSection}>
      <div className="container">
        <div className={styles.stickyContainer}>
          {features.map((feature, index) => (
            <div
              key={index}
              className={`${styles.featureItem} ${index % 2 === 0 ? styles.textLeft : styles.textRight}`}
              ref={(el) => (featureRefs.current[index] = el)}
            >
              <div className={styles.textContainer}>
                <h2>{feature.title}</h2>
                <p>{feature.description}</p>
              </div>
              <div className={styles.imageContainer}>
                {feature.videoFile ? (
                  <video
                    ref={(el) => (videoRefs.current[index] = el)}
                    src={`${assetsBaseUrl}/${feature.videoFile}`}
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
