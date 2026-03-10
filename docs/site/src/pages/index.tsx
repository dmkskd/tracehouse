import React, { useCallback, useRef, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import InteractiveFeatures from '../components/InteractiveFeatures';
import FAQ from '../components/FAQ';

function HeroSection(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const videoRef = useRef<HTMLVideoElement>(null);
  const fadeTime = 2; // seconds for fade in/out

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    const pauseTime = 1; // seconds to hold black between loops
    let rafId: number;
    const tick = () => {
      if (video.duration) {
        const timeLeft = video.duration - video.currentTime;
        let t: number;
        if (video.currentTime < pauseTime) {
          // Hold black for pauseTime seconds
          t = 0;
        } else if (video.currentTime < pauseTime + fadeTime) {
          // Fade in
          t = (video.currentTime - pauseTime) / fadeTime;
        } else if (timeLeft < fadeTime) {
          // Fade out
          t = timeLeft / fadeTime;
        } else {
          t = 1;
        }
        video.style.opacity = String(0.7 * t);
        const scale = 1 + 0.15 * (1 - t);
        video.style.transform = `translate(-50%, -50%) scale(${scale})`;
      }
      rafId = requestAnimationFrame(tick);
    };
    rafId = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafId);
  }, []);

  return (
    <header className="hero-clickhouse">
      <video
        ref={videoRef}
        className="hero-video-bg"
        autoPlay
        muted
        loop
        playsInline
        preload="auto"
      >
        <source src={`${assetsBaseUrl}/hero.mp4`} type="video/mp4" />
      </video>
      <div className="container">
        <h1 className="hero-title">
          <span>TraceHouse</span>
          <br />
          ClickHouse monitoring under one roof
        </h1>
        <p>
          An open-source tool to visually explore and monitor ClickHouse.
          <br />
          Resource attribution, merge activity, query performance, and engine internals in one place.
        </p>
        <div className="hero-buttons">
          <Link className="button button--primary button--lg" to="/docs/getting-started">
            Get Started
          </Link>
          <Link className="button button--secondary button--lg" to="https://github.com/dmkskd/tracehouse">
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}


function QuickStartSection(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;

  return (
    <section className="quickstart-section">
      <div className="container">
        <h2>Quickstart</h2>
        <div className="quickstart-code">
          <div><span className="comment"># Clone the repo</span></div>
          <div><span className="command">git clone https://github.com/dmkskd/tracehouse.git && cd tracehouse</span></div>
          <div><span className="comment"># Start the app and a local ClickHouse instance</span></div>
          <div><span className="command">cd infra/quickstart && docker compose up</span></div>
          <br />
          <div><span className="comment"># Open http://localhost:8990</span></div>
        </div>
        <div className="quickstart-or">
          <span>or</span>
        </div>
        <div style={{ textAlign: 'center' }}>
          <Link
            className="button button--primary button--lg"
            to={`${assetsBaseUrl}/tracehouse.html`}
          >
            Try Online
          </Link>
          <p className="quickstart-hint">
            Single-file HTML that runs entirely in your browser with any{' '}
            <Link to="/docs/guides/connecting#cors-proxy">CORS-enabled</Link> ClickHouse instance.
          </p>
          <p className="quickstart-hint">
            The online version will ask for your ClickHouse credentials.
            If you prefer not to enter them on a third-party page,
            download it from the{' '}
            <Link to="https://github.com/dmkskd/tracehouse/releases/latest">releases page</Link>{' '}
            or <Link to="/docs/guides/deployment">build it yourself</Link>.
          </p>
        </div>
      </div>
    </section>
  );
}

function ContactSection(): ReactNode {
  const [revealed, setRevealed] = useState(false);
  const handleReveal = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setRevealed(true);
  }, []);

  const email = revealed
    ? ['ddmmkk', 'proton', 'me'].join('\u0040').replace('proton\u0040', 'proton.')
    : null;

  return (
    <section style={{ textAlign: 'center', padding: '2rem 0 3rem' }}>
      <p style={{ color: 'var(--ifm-color-emphasis-600)', fontSize: '0.95rem' }}>
        Questions or feedback?{' '}
        {email ? (
          <span
            style={{ fontFamily: 'monospace', userSelect: 'all', cursor: 'text' }}
          >
            {email}
          </span>
        ) : (
          <a href="#contact" onClick={handleReveal} style={{ cursor: 'pointer' }}>
            Click to reveal email
          </a>
        )}
      </p>
    </section>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Real-time ClickHouse Monitoring"
      description="Visually explore and monitor ClickHouse clusters">
      <HeroSection />
      <InteractiveFeatures />
      <FAQ />
      <QuickStartSection />
      <ContactSection />
    </Layout>
  );
}
