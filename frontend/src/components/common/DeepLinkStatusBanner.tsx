/**
 * DeepLinkStatusBanner - explains why a query / merge deep link has not
 * opened its detail view yet (still running, log not flushed, not found).
 * Floats at the bottom of the viewport so pages can render it anywhere.
 */

import React from 'react';
import { describeLinkStatus } from '@tracehouse/core';
import type { DeepLinkBannerState } from '../../hooks/useDeepLinkResolution';

interface DeepLinkStatusBannerProps {
  banner: DeepLinkBannerState | null;
  onDismiss: () => void;
}

export const DeepLinkStatusBanner: React.FC<DeepLinkStatusBannerProps> = ({ banner, onDismiss }) => {
  if (!banner || (!banner.status && !banner.error)) return null;

  const waiting = banner.status?.state === 'running' || banner.status?.state === 'pending_flush';
  const text = banner.error
    ? `Failed to look up ${banner.kind} ${banner.subject}: ${banner.error}`
    : describeLinkStatus(banner.kind, banner.subject, banner.status!);
  const accent = banner.error ? '#ef4444' : waiting ? '#3b82f6' : '#f59e0b';

  return (
    <div
      role="status"
      style={{
        position: 'fixed',
        bottom: 24,
        left: '50%',
        transform: 'translateX(-50%)',
        zIndex: 1000,
        width: 'min(640px, calc(100vw - 32px))',
        boxShadow: '0 4px 16px rgba(0,0,0,0.25)',
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        padding: '10px 14px',
        fontSize: 13,
        lineHeight: 1.45,
        color: 'var(--text-primary)',
        // Opaque: --bg-card is near-transparent and lets the page show through.
        background: 'var(--bg-secondary)',
        border: '1px solid var(--border-primary)',
        borderRadius: 8,
      }}
    >
      <span aria-hidden="true" style={{ width: 8, height: 8, borderRadius: '50%', background: accent, flexShrink: 0 }} />
      <span style={{ flex: 1, minWidth: 0, overflowWrap: 'anywhere' }}>{text}</span>
      <button
        onClick={onDismiss}
        aria-label="Dismiss"
        style={{
          padding: '0 4px',
          fontSize: 14,
          color: 'var(--text-muted)',
          background: 'transparent',
          border: 'none',
          cursor: 'pointer',
          fontFamily: 'inherit',
        }}
      >
        ×
      </button>
    </div>
  );
};
