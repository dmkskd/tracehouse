/**
 * PermissionGate — Renders a clean full-page or inline message when a query
 * fails due to insufficient privileges or missing system tables.
 *
 * Replaces raw SQL error banners with a consistent, user-friendly display.
 */

import React from 'react';
import { formatClickHouseError } from '../../utils/errorFormatters';

export interface PermissionGateProps {
  /** Raw error string (from catch block) */
  error: string;
  /** Page/feature title shown in the message */
  title: string;
  /** 'page' = full centered message, 'banner' = inline dismissable strip */
  variant?: 'page' | 'banner';
  /** Optional dismiss callback (only for banner variant) */
  onDismiss?: () => void;
}

/**
 * Displays a permission/access error in a clean, consistent style.
 * For non-permission errors, falls back to a standard error card.
 */
export const PermissionGate: React.FC<PermissionGateProps> = ({
  error,
  title,
  variant = 'banner',
  onDismiss,
}) => {
  const formatted = formatClickHouseError(error);

  if (variant === 'page') {
    return (
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '64px 24px',
        textAlign: 'center',
      }}>
        <div style={{
          fontSize: 14,
          fontWeight: 600,
          color: 'var(--text-primary)',
          marginBottom: 12,
        }}>
          {title}
        </div>
        <div style={{
          fontSize: 12,
          color: 'var(--text-muted)',
          maxWidth: 480,
          lineHeight: 1.6,
        }}>
          {formatted.message}
        </div>
      </div>
    );
  }

  // Banner variant — inline strip
  if (formatted.isPermissionError) {
    return (
      <div style={{
        padding: '10px 14px',
        borderRadius: 8,
        fontSize: 12,
        background: 'rgba(210,153,34,0.08)',
        color: '#d29922',
        border: '1px solid rgba(210,153,34,0.2)',
        display: 'flex',
        alignItems: 'center',
        gap: 8,
      }}>
        <span style={{ flex: 1 }}>{formatted.message}</span>
        {onDismiss && (
          <button onClick={onDismiss} style={{ color: '#d29922', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14 }}>×</button>
        )}
      </div>
    );
  }

  // Non-permission error — standard red error card
  return (
    <div style={{
      padding: '10px 14px',
      borderRadius: 8,
      fontSize: 12,
      background: 'rgba(248,81,73,0.08)',
      color: '#f85149',
      border: '1px solid rgba(248,81,73,0.2)',
      display: 'flex',
      alignItems: 'center',
      gap: 8,
    }}>
      <span style={{ flex: 1 }}>{formatted.message}</span>
      {onDismiss && (
        <button onClick={onDismiss} style={{ color: '#f85149', background: 'none', border: 'none', cursor: 'pointer', fontSize: 14 }}>×</button>
      )}
    </div>
  );
};

export default PermissionGate;
