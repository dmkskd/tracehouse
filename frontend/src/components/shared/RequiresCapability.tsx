/**
 * RequiresCapability - Standard wrapper that checks monitoring capabilities
 * before rendering children. Shows a consistent message when required
 * system tables or features are unavailable.
 *
 * Usage:
 *   <RequiresCapability requires={['query_log']} label="Query History">
 *     <QueryHistoryTable />
 *   </RequiresCapability>
 *
 *   <RequiresCapability requires={['trace_log', 'text_log']} label="Query Tracer" mode="any">
 *     <TracerContent />
 *   </RequiresCapability>
 */

import React from 'react';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';

export interface RequiresCapabilityProps {
  /** Capability IDs to check (e.g. 'query_log', 'trace_log', 'metric_log') */
  requires: string[];
  /** Human-readable label for the feature (shown in the unavailable message) */
  label: string;
  /** 'all' = every capability must be available (default). 'any' = at least one. */
  mode?: 'all' | 'any';
  /** Render style: 'card' wraps in a card box, 'inline' renders a compact message, 'banner' renders a warning strip */
  variant?: 'card' | 'inline' | 'banner';
  /** Optional extra class */
  className?: string;
  children: React.ReactNode;
}

/**
 * Standalone hook for checking capabilities in components that need
 * more control than the wrapper provides.
 */
export function useCapabilityCheck(
  requires: string[],
  mode: 'all' | 'any' = 'all'
): { available: boolean; missing: string[]; probing: boolean } {
  const { capabilities, probeStatus } = useMonitoringCapabilitiesStore();

  if (probeStatus !== 'done' || !capabilities) {
    return { available: false, missing: requires, probing: true };
  }

  const missing = requires.filter(id => {
    const cap = capabilities.capabilities.find(c => c.id === id);
    return !cap?.available;
  });

  const available = mode === 'all' ? missing.length === 0 : missing.length < requires.length;
  return { available, missing, probing: false };
}

/** Format capability IDs to human-readable names */
function formatCapName(id: string): string {
  return `system.${id}`;
}

export function RequiresCapability({
  requires,
  label,
  mode = 'all',
  variant = 'card',
  className = '',
  children,
}: RequiresCapabilityProps) {
  const { probeStatus } = useMonitoringCapabilitiesStore();
  const { available, missing } = useCapabilityCheck(requires, mode);

  // Still probing — don't block, just render children (they'll show loading states)
  if (probeStatus !== 'done') {
    return <>{children}</>;
  }

  if (available) {
    return <>{children}</>;
  }

  const missingNames = missing.map(formatCapName).join(', ');
  const message = missing.length === 1
    ? `${label} requires ${missingNames} (not available on this server)`
    : `${label} requires ${missingNames} (not available on this server)`;

  if (variant === 'inline') {
    return (
      <div className={className} style={{ fontSize: 11, color: 'var(--text-muted)', padding: '8px 0' }}>
        {message}
      </div>
    );
  }

  if (variant === 'banner') {
    return (
      <div
        className={className}
        style={{
          padding: '8px 12px',
          fontSize: 11,
          color: 'var(--text-muted)',
          background: 'var(--bg-tertiary)',
          borderRadius: 6,
          border: '1px solid var(--border-secondary)',
        }}
      >
        {message}
      </div>
    );
  }

  // Default: card variant
  return (
    <div
      className={`rounded-lg border ${className}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>{label}</h3>
      </div>
      <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
        {message}
      </div>
    </div>
  );
}

export default RequiresCapability;
