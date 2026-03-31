import React, { useCallback, useRef, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import type { ReactNode } from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import InteractiveFeatures from '../components/InteractiveFeatures';
import FAQ from '../components/FAQ';

function DemoButton({ demoUrl }: { demoUrl: string }): ReactNode {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number; width: number } | null>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as Node;
      const inWrapper = wrapperRef.current?.contains(target);
      const inDropdown = dropdownRef.current?.contains(target);
      if (!inWrapper && !inDropdown) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const handleChevronClick = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (!open && wrapperRef.current) {
      const r = wrapperRef.current.getBoundingClientRect();
      setPos({ top: r.bottom + 6, left: r.left, width: r.width });
    }
    setOpen(o => !o);
  }, [open]);

  return (
    <div ref={wrapperRef} className="demo-button-wrapper">
      <Link className="button button--primary button--lg hero-demo-button" to={demoUrl}>
        Live Demo
        <span className="hero-demo-dot demo-status-dot" hidden aria-hidden="true" />
        <span
          className="demo-chevron"
          role="button"
          tabIndex={0}
          onClick={handleChevronClick}
          onKeyDown={(e) => { if (e.key === 'Enter') handleChevronClick(e as unknown as React.MouseEvent); }}
          aria-label="More demo options"
        >
          <svg width="10" height="6" viewBox="0 0 10 6">
            <path d="M1 1l4 4 4-4" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </span>
      </Link>
      {open && pos && createPortal(
        <div
          ref={dropdownRef}
          className="demo-dropdown"
          style={{ position: 'fixed', top: pos.top, left: pos.left, width: pos.width }}
        >
          <Link className="demo-dropdown-option" to={demoUrl} onClick={() => setOpen(false)}>
            TraceHouse
          </Link>
          <Link className="demo-dropdown-option" to={`${demoUrl}:3000`} onClick={() => setOpen(false)}>
            Grafana Plugin
          </Link>
        </div>,
        document.body,
      )}
    </div>
  );
}

function HeroSection(): ReactNode {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const demoUrl = siteConfig.customFields?.demoUrl as string;
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
          {demoUrl && (
            <DemoButton demoUrl={demoUrl} />
          )}
          <Link className="button button--secondary button--lg" to="https://github.com/dmkskd/tracehouse">
            <svg width="20" height="20" viewBox="0 0 24 24" style={{marginRight: 8, verticalAlign: 'middle'}}><path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12" fill="currentColor"/></svg>
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
      <QuickStartSection />
      <FAQ />
      <ContactSection />
    </Layout>
  );
}
