import React, { useEffect, useState } from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

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

const accordionStyles: Record<string, React.CSSProperties> = {
  section: {
    padding: '4rem 0',
  },
  list: {
    maxWidth: 960,
    margin: '0 auto',
    listStyle: 'none',
    padding: 0,
  },
  item: {
    borderBottom: '1px solid var(--ifm-color-emphasis-200)',
  },
  button: {
    width: '100%',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '1rem 0',
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    textAlign: 'left',
    color: 'var(--ifm-font-color-base)',
    fontSize: '1rem',
    fontWeight: 600,
    fontFamily: 'inherit',
    lineHeight: 1.4,
  },
  chevron: {
    flexShrink: 0,
    marginLeft: '1rem',
    transition: 'transform 0.2s ease',
    fontSize: '0.8rem',
    color: 'var(--ifm-color-emphasis-500)',
  },
  body: {
    overflow: 'hidden',
    transition: 'max-height 0.3s ease, opacity 0.3s ease',
  },
  bodyInner: {
    paddingBottom: '1rem',
    fontSize: '1.05rem',
    lineHeight: 1.7,
    color: 'var(--ifm-color-emphasis-700)',
  },
};

const FAQ = () => {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const [openIndex, setOpenIndex] = useState<number | null>(null);
  const [expandedSrc, setExpandedSrc] = useState<string | null>(null);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setExpandedSrc(null);
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  return (
    <section style={accordionStyles.section}>
      <div className="container">
        <h2 style={{ textAlign: 'center', fontSize: '1.8rem', marginBottom: '2.5rem' }}>
          Frequently Asked Questions
        </h2>
        <ul style={accordionStyles.list}>
          {faqItems.map((item, index) => {
            const isOpen = openIndex === index;
            return (
              <li key={index} style={accordionStyles.item}>
                <button
                  style={accordionStyles.button}
                  onClick={() => setOpenIndex(isOpen ? null : index)}
                  aria-expanded={isOpen}
                >
                  {item.title}
                  <span style={{
                    ...accordionStyles.chevron,
                    transform: isOpen ? 'rotate(180deg)' : 'rotate(0deg)',
                  }}>
                    &#9660;
                  </span>
                </button>
                <div style={{
                  ...accordionStyles.body,
                  maxHeight: isOpen ? 1200 : 0,
                  opacity: isOpen ? 1 : 0,
                }}>
                  <div style={accordionStyles.bodyInner}>
                    <p style={{ margin: 0 }}>{item.description}</p>
                    {item.videoFile && (
                      <video
                        ref={(el) => {
                          if (el && isOpen) el.play().catch(() => {});
                          if (el && !isOpen) el.pause();
                        }}
                        src={`${assetsBaseUrl}/${item.videoFile}`}
                        loop
                        muted
                        playsInline
                        style={{
                          width: '100%',
                          borderRadius: 8,
                          marginTop: '1rem',
                          cursor: 'pointer',
                        }}
                        onClick={() => setExpandedSrc(`${assetsBaseUrl}/${item.videoFile}`)}
                      />
                    )}
                  </div>
                </div>
              </li>
            );
          })}
        </ul>
      </div>
      {expandedSrc && (
        <div
          style={{
            position: 'fixed', top: 0, left: 0, width: '100vw', height: '100vh',
            background: 'rgba(0,0,0,0.85)', display: 'flex', justifyContent: 'center',
            alignItems: 'center', zIndex: 9999, cursor: 'pointer',
          }}
          onClick={() => setExpandedSrc(null)}
        >
          <video
            src={expandedSrc}
            autoPlay
            loop
            muted
            controls
            playsInline
            style={{ maxWidth: '90vw', maxHeight: '90vh', borderRadius: 8 }}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
    </section>
  );
};

export default FAQ;
