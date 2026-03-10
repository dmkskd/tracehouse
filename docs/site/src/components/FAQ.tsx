import React, { useEffect, useRef, useState } from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './InteractiveFeatures.module.css';

type FAQItem = {
  title: string;
  description: string | React.ReactNode;
  videoFile?: string;
};

const faqItems: FAQItem[] = [
  {
    title: 'Why 3D?',
    description:
      'Some views use 3D to show relationships between parts, merges, and resources that are hard to represent in flat charts. If you prefer a simpler view, every screen has a 2D mode.',
    videoFile: '2d.mp4',
  },
  {
    title: 'Does TraceHouse require any changes to my ClickHouse configuration?',
    description:
      <>TraceHouse connects as a read-only client and queries system tables. On connect it probes the server for available capabilities (log tables, introspection functions, profiler settings, cluster topology, Keeper, Cloud mode) and gracefully disables features that aren't available. Some features (e.g. query profiling, trace_log) may require enabling specific settings on your server. See <a href="/docs/guides/connecting">Connecting to ClickHouse</a> for details.</>,
  },
  {
    title: 'Which ClickHouse versions are supported?',
    description:
      'TraceHouse is developed against recent ClickHouse releases. Older versions may work but some features that depend on newer system tables will be unavailable. The capability detection on connect will tell you what\'s missing.',
  },
  {
    title: 'Can I monitor multiple clusters at once?',
    description:
      'Yes. Connections are stateless (HTTP-based), so you can configure multiple ClickHouse connections and switch between them in the UI. Each cluster gets its own overview dashboard, merge tracker, and query monitor.',
  },
  {
    title: 'Is there a performance impact on my ClickHouse cluster?',
    description:
      'TraceHouse polls system tables at configurable intervals (default 5 s). We are actively working on optimising query overhead. You can check the actual impact yourself via the built-in App Internal Monitor, which tracks the tool\'s own CPU, memory, and network usage per request.',
  },
  {
    title: 'Can I use TraceHouse alongside Grafana?',
    description:
      'TraceHouse is a client-side application. It runs entirely in the browser and does not install anything on your ClickHouse server or Grafana backend. It can be deployed as a Grafana app plugin, reusing your existing Grafana ClickHouse datasource and permissions, or run independently as a standalone web app.',
  },
  {
    title: 'Is TraceHouse suitable for production use?',
    description:
      <>TraceHouse only performs read-only queries over the HTTP interface, but we strongly recommend using a dedicated <strong>read-only</strong> ClickHouse account. We strongly suggest testing it against a staging or development cluster first. Pay attention to query load, memory usage, and network overhead that matter for your environment. That said, we haven't tested it at every scale. If you run into issues with large clusters or high-cardinality workloads, we'd love to hear about it.</>,
  },
];

const FAQ = () => {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const itemRefs = useRef<(HTMLDivElement | null)[]>([]);
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
            if (video) video.play().catch(() => {});
          } else {
            const video = (entry.target as HTMLElement).querySelector('video');
            if (video) video.pause();
          }
        });
      },
      { threshold: 0.3 }
    );

    itemRefs.current.forEach((ref) => {
      if (ref) observer.observe(ref);
    });

    return () => {
      itemRefs.current.forEach((ref) => {
        if (ref) observer.unobserve(ref);
      });
    };
  }, []);

  return (
    <section className={styles.featuresSection}>
      <div className="container">
        <h2 style={{ textAlign: 'center', fontSize: '2.5rem', marginBottom: '4rem' }}>
          Frequently Asked Questions
        </h2>
        <div className={styles.stickyContainer}>
          {faqItems.map((item, index) => (
            <div
              key={index}
              className={`${styles.featureItem} ${index % 2 === 0 ? styles.textLeft : styles.textRight}`}
              ref={(el) => (itemRefs.current[index] = el)}
            >
              <div className={styles.textContainer}>
                <h2>{item.title}</h2>
                <p>{item.description}</p>
              </div>
              {item.videoFile && (
                <div className={styles.imageContainer}>
                  <video
                    src={`${assetsBaseUrl}/${item.videoFile}`}
                    loop
                    muted
                    playsInline
                    className={styles.media}
                    onClick={() => setExpandedSrc(`${assetsBaseUrl}/${item.videoFile}`)}
                  />
                </div>
              )}
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

export default FAQ;
