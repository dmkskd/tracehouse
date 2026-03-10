import React, { useEffect, useState } from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';

const styles: Record<string, React.CSSProperties> = {
  video: {
    width: '100%',
    borderRadius: 8,
    boxShadow: '0 4px 8px rgba(0,0,0,0.1)',
    cursor: 'pointer',
    marginBottom: '1.5rem',
  },
  overlay: {
    position: 'fixed',
    top: 0,
    left: 0,
    width: '100vw',
    height: '100vh',
    background: 'rgba(0, 0, 0, 0.85)',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    zIndex: 9999,
    cursor: 'pointer',
  },
  overlayVideo: {
    maxWidth: '90vw',
    maxHeight: '90vh',
    borderRadius: 8,
    boxShadow: '0 16px 48px rgba(0,0,0,0.5)',
  },
};

/**
 * Embeds a video from the external assets CDN.
 *
 * Usage in MDX:
 *   import FeatureVideo from '@site/src/components/FeatureVideo';
 *   <FeatureVideo src="system-map.mp4" />
 */
export default function FeatureVideo({ src }: { src: string }) {
  const { siteConfig } = useDocusaurusContext();
  const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
  const localUrl = useBaseUrl(src);
  const videoUrl = src.startsWith('http')
    ? src
    : src.startsWith('/')
      ? localUrl
      : `${assetsBaseUrl}/${src}`;

  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    if (!expanded) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setExpanded(false);
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [expanded]);

  return (
    <>
      <video
        src={videoUrl}
        autoPlay
        loop
        muted
        playsInline
        style={styles.video}
        onClick={() => setExpanded(true)}
      />
      {expanded && (
        <div style={styles.overlay} onClick={() => setExpanded(false)}>
          <video
            src={videoUrl}
            autoPlay
            loop
            muted
            playsInline
            style={styles.overlayVideo}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
    </>
  );
}
