/**
 * TimeTravelLoading — modal shown on the first Time Travel load, before any
 * data is on screen. Same look as StartupProgress, at a higher level: what is
 * being read, plus the elapsed time. The reads run in parallel, so every row
 * stays in progress until the page renders and closes it.
 */

import React, { useEffect, useState } from 'react';
import { ModalWrapper } from '../shared/ModalWrapper';
import { ElapsedSeconds } from '../common/RefreshingOverlay';
import { LoadingSpinner } from '../shared/LoadingSpinner';

const noop = () => {};

const READS: { label: string; detail: string }[] = [
  { label: 'Server resources', detail: 'metric_log, asynchronous_metric_log' },
  { label: 'Queries', detail: 'query_log, processes' },
  { label: 'Merges and mutations', detail: 'part_log, merges' },
  { label: 'Events', detail: 'Failures, restarts, DDL, backups' },
];

/** Loads faster than this never show the modal. */
const SHOW_AFTER_MS = 1000;

interface TimeTravelLoadingProps {
  title?: string;
  description?: string;
  reads?: readonly { label: string; detail: string }[];
  delayMs?: number;
  onClose?: () => void;
}

export const TimeTravelLoading: React.FC<TimeTravelLoadingProps> = ({
  title = 'Loading Time Travel',
  description = 'Rebuilding the selected window from ClickHouse system tables.',
  reads = READS,
  delayMs = SHOW_AFTER_MS,
  onClose,
}) => {
  const [startedAt] = useState(() => Date.now());
  const [visible, setVisible] = useState(delayMs === 0);
  useEffect(() => {
    const timer = setTimeout(() => setVisible(true), delayMs);
    return () => clearTimeout(timer);
  }, [delayMs]);
  if (!visible) return null;

  return (
    <ModalWrapper
      isOpen
      onClose={onClose ?? noop}
      maxWidth={480}
      height="auto"
      backdropBackground="rgba(0, 0, 0, 0.45)"
      backdropBlur={2}
      ariaLabel={title}
    >
      <div style={{ padding: '20px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
          <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>{title}</div>
          {onClose && <button type="button" aria-label={`Close ${title.toLowerCase()}`} onClick={onClose}
            style={{ border: 0, background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 18 }}>×</button>}
        </div>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 4, marginBottom: 16 }}>
          {description}
        </div>
        <ol style={{ listStyle: 'none', margin: 0, padding: 0 }}>
          {reads.map(read => (
            <li
              key={read.label}
              style={{
                display: 'flex',
                alignItems: 'baseline',
                gap: 10,
                padding: '6px 0',
                borderTop: '1px solid var(--border-secondary)',
              }}
            >
              <LoadingSpinner />
              <span style={{ fontSize: 12, color: 'var(--text-primary)' }}>{read.label}</span>
              <span style={{ fontSize: 11, color: 'var(--text-tertiary)' }}>{read.detail}</span>
            </li>
          ))}
        </ol>
        <div role="status" style={{ marginTop: 12, fontSize: 11, color: 'var(--text-tertiary)', fontFamily: 'monospace', textAlign: 'right' }}>
          <ElapsedSeconds since={startedAt} />
        </div>
      </div>
    </ModalWrapper>
  );
};
