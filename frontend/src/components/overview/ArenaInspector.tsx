/**
 * ArenaInspector — docked right-hand panel shown when a block is clicked in
 * ResourceArena3D ("Essential" design): status line, identity, one hero
 * (progress, or the headline numbers when there is none), a quiet list of the
 * non-zero values, the latest log line (expandable to the full tail) and a
 * link to the Queries / Merges page. One accent color, used only for the
 * status dot and the progress bar. Enabled via USE_ARENA_INSPECTOR in
 * ResourceArena3D.tsx.
 */

import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import type { TraceLog } from '@tracehouse/core';
import { buildQueryLinkUrl, buildMergeLinkUrl } from '@tracehouse/core';
import { formatElapsed } from '../../utils/formatters';
import type { BlockEntry } from './arena-types';
import { panelFont, withAlpha } from './arena-hud-format';
import { buildInspectorModel, type InspectorValue } from './arena-inspector-model';

/** Height of the arena header bar the panel sits under. */
const HEADER_H = 34;
const PAD = 20;

const sansFont = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif";
const C = {
  ground: '#10101a',
  hairline: '#23232f',
  text: '#ececf1',
  secondary: '#a3a3b2',
  muted: '#8b8b9a',
  log: '#c9c9d4',
  raised: '#171724',
};
const mono: React.CSSProperties = { fontFamily: panelFont, fontVariantNumeric: 'tabular-nums' };

const LOG_ALERT_COLORS: Record<string, string> = {
  Fatal: '#f87171', Critical: '#f87171', Error: '#f87171', Warning: '#fbbf24',
};

function CloseIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" aria-hidden="true">
      <path d="M3 3l8 8M11 3l-8 8" />
    </svg>
  );
}

function CopyIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" aria-hidden="true">
      <rect x="4.5" y="4.5" width="7" height="7" rx="1.5" />
      <path d="M9.5 4.5v-1A1.5 1.5 0 0 0 8 2H3.5A1.5 1.5 0 0 0 2 3.5V8a1.5 1.5 0 0 0 1.5 1.5h1" />
    </svg>
  );
}

/** A titled two-column list; renders nothing when it has no rows. */
function ValueGroup({ title, values, children }: { title: string; values: InspectorValue[]; children?: React.ReactNode }) {
  if (values.length === 0 && !children) return null;
  return (
    <div style={{ marginTop: 20 }}>
      <div style={{ fontSize: 11, fontWeight: 500, color: C.muted }}>{title}</div>
      <dl style={{ margin: '8px 0 0', display: 'grid', gridTemplateColumns: '88px minmax(0, 1fr)', rowGap: 8, fontSize: 13 }}>
        {values.map(v => (
          <React.Fragment key={v.label}>
            <dt style={{ color: C.muted }}>{v.label}</dt>
            <dd style={{ ...mono, margin: 0, overflowWrap: 'anywhere' }}>{v.value}</dd>
          </React.Fragment>
        ))}
        {children}
      </dl>
    </div>
  );
}

function logTime(log: TraceLog): string {
  return (log.event_time_microseconds || log.event_time || '').split(' ').pop()?.substring(0, 12) ?? '';
}

/** Full log tail, newest at the bottom; follows new lines unless scrolled up. */
function LogList({ logs }: { logs: TraceLog[] }) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);

  useEffect(() => {
    const el = scrollRef.current;
    if (el && stickToBottom.current) el.scrollTop = el.scrollHeight;
  }, [logs.length]);

  return (
    <div
      ref={scrollRef}
      onScroll={e => {
        const el = e.currentTarget;
        stickToBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 8;
      }}
      style={{
        flex: 1, minHeight: 0, overflowY: 'auto', marginTop: 12,
        display: 'flex', flexDirection: 'column', gap: 12,
      }}
    >
      {logs.map((log, i) => (
        <div key={i} style={{ ...mono, fontSize: 12, lineHeight: 1.55 }}>
          <div style={{ fontSize: 11, color: LOG_ALERT_COLORS[log.level] ?? C.muted }}>
            {logTime(log)} · {log.level}
          </div>
          <div style={{ color: C.log, overflowWrap: 'anywhere' }}>{log.message}</div>
        </div>
      ))}
    </div>
  );
}

export function ArenaInspector({ entry, onClose, logs, compact }: {
  entry: BlockEntry;
  onClose: () => void;
  logs: TraceLog[];
  compact?: boolean;
}) {
  const width = compact ? 320 : 380;
  const running = entry.endTime === null;
  // Ticks every 100ms while running so the elapsed time counts up live.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setNow(Date.now()), 100);
    return () => clearInterval(id);
  }, [running]);
  const elapsed = running ? Math.max(0, (now - entry.startTime) / 1000) : entry.elapsed;

  const [copied, setCopied] = useState(false);
  const [logsOpen, setLogsOpen] = useState(false);
  // A different block starts collapsed.
  const [logsFor, setLogsFor] = useState(entry.id);
  if (logsFor !== entry.id) {
    setLogsFor(entry.id);
    setLogsOpen(false);
  }

  const accent = entry.color;
  const kindLabel = entry.kind.charAt(0) + entry.kind.slice(1).toLowerCase();
  const title = entry.tableHint !== 'unknown' ? entry.tableHint
    : (entry.database && entry.table ? `${entry.database}.${entry.table}` : kindLabel);
  const model = buildInspectorModel(entry);
  const detailUrl = entry.isMerge
    ? (entry.database && entry.table && entry.partName ? buildMergeLinkUrl(entry.database, entry.table, entry.partName, entry.hostname, entry.startTime) : null)
    : (entry.queryId ? buildQueryLinkUrl(entry.queryId, entry.endTime) : null);
  const latestLog = logs.length > 0 ? logs[logs.length - 1] : null;

  return (
    <div
      onClick={e => e.stopPropagation()}
      style={{
        position: 'absolute', top: HEADER_H, right: 0, bottom: 0, width, zIndex: 20,
        boxSizing: 'border-box', padding: PAD,
        display: 'flex', flexDirection: 'column',
        background: C.ground, borderLeft: `1px solid ${C.hairline}`,
        color: C.text, fontFamily: sansFont,
        animation: 'arena-fade-in 0.2s ease both',
      }}
    >
      {/* Status line */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
        <span aria-hidden="true" style={{
          width: 8, height: 8, borderRadius: '50%', background: accent,
          boxShadow: running ? `0 0 0 3px ${withAlpha(accent, 0.18)}` : undefined,
          animation: running ? 'arena-glow-pulse 2s ease infinite' : undefined,
        }} />
        <span style={{ fontSize: 12, fontWeight: 500, color: C.secondary }}>
          {kindLabel} · {running ? 'running' : 'finished'}
        </span>
        <span style={{ ...mono, marginLeft: 'auto', fontSize: 13 }}>{model.hero ? '' : formatElapsed(elapsed)}</span>
        <button
          onClick={onClose}
          aria-label="Close"
          title="Close (Esc)"
          style={{ background: 'none', border: 'none', padding: 4, marginLeft: 4, color: C.muted, cursor: 'pointer', display: 'flex' }}
        >
          <CloseIcon />
        </button>
      </div>

      {/* Identity */}
      <h2 style={{ margin: '14px 0 0', fontSize: 17, fontWeight: 600, letterSpacing: '-0.01em', overflowWrap: 'anywhere', flexShrink: 0 }}>
        {title}
      </h2>
      {entry.isMerge && entry.partName ? (
        <div style={{ ...mono, marginTop: 4, fontSize: 12, color: C.muted, overflowWrap: 'anywhere', flexShrink: 0 }}>
          &rarr; {entry.partName}
        </div>
      ) : !entry.isMerge && (
        <div title={entry.label} style={{
          ...mono, marginTop: 6, fontSize: 12, lineHeight: 1.55, color: C.muted, flexShrink: 0,
          display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' as const,
          overflow: 'hidden', overflowWrap: 'anywhere',
        }}>{entry.label}</div>
      )}

      {logsOpen ? (
        /* Expanded logs take the space of the metrics */
        <div style={{ marginTop: 20, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: C.muted }}>
            <span>Logs · {logs.length}</span>
            <button
              onClick={() => setLogsOpen(false)}
              style={{ background: 'none', border: 'none', padding: 0, fontSize: 12, color: C.secondary, cursor: 'pointer', fontFamily: 'inherit' }}
            >Back to details</button>
          </div>
          <LogList logs={logs} />
        </div>
      ) : (
        <div style={{ flex: 1, minHeight: 0, overflowY: 'auto' }}>
          {/* Hero: progress while running, final totals once finished */}
          {model.progress && (
            <div style={{ marginTop: 20 }}>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                <span style={{ ...mono, fontSize: 26, fontWeight: 500, letterSpacing: '-0.02em' }}>{model.progress.pct.toFixed(1)}%</span>
                <span style={{ fontSize: 12, color: C.muted }}>{model.progress.caption}</span>
              </div>
              <div style={{ marginTop: 10, height: 4, borderRadius: 2, background: C.hairline, overflow: 'hidden' }}>
                <div style={{ width: `${model.progress.pct}%`, height: '100%', background: accent, borderRadius: 2 }} />
              </div>
            </div>
          )}
          {model.hero && (
            <div style={{
              marginTop: 16, padding: 14, borderRadius: 10, background: C.raised,
              display: 'grid', gridTemplateColumns: `repeat(${model.hero.length}, minmax(0, 1fr))`, gap: 8,
            }}>
              {model.hero.map(h => (
                <div key={h.label} style={{ minWidth: 0 }}>
                  <div style={{ ...mono, fontSize: 20, fontWeight: 500, letterSpacing: '-0.02em', whiteSpace: 'nowrap' }}>{h.value}</div>
                  <div style={{ marginTop: 4, fontSize: 11, color: C.muted }}>{h.label}</div>
                </div>
              ))}
            </div>
          )}
          {model.note && (
            <p style={{ margin: '16px 0 0', fontSize: 12, lineHeight: 1.5, color: C.muted }}>{model.note}</p>
          )}

          <ValueGroup title="Resources" values={model.resources} />
          <ValueGroup title="Context" values={model.context}>
            {entry.queryId && (
              <>
                <dt style={{ color: C.muted }}>Query ID</dt>
                <dd style={{ margin: 0, display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
                  <span title={entry.queryId} style={{ ...mono, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{entry.queryId}</span>
                  <button
                    onClick={() => {
                      navigator.clipboard.writeText(entry.queryId!).then(() => {
                        setCopied(true);
                        setTimeout(() => setCopied(false), 1200);
                      }, (err: unknown) => console.error('[ArenaInspector] Copy failed:', err));
                    }}
                    aria-label="Copy query ID"
                    title={copied ? 'Copied' : 'Copy query ID'}
                    style={{ background: 'none', border: 'none', padding: 2, flexShrink: 0, display: 'flex', cursor: 'pointer', color: copied ? '#4ade80' : C.muted }}
                  >
                    <CopyIcon />
                  </button>
                </dd>
              </>
            )}
          </ValueGroup>

          {/* Latest log */}
          <div style={{ marginTop: 20 }}>
            <div style={{ fontSize: 12, color: C.muted }}>Latest log</div>
            {latestLog ? (
              <>
                <p style={{
                  ...mono, margin: '8px 0 0', fontSize: 12, lineHeight: 1.55,
                  color: LOG_ALERT_COLORS[latestLog.level] ?? C.log,
                  display: '-webkit-box', WebkitLineClamp: 3, WebkitBoxOrient: 'vertical' as const,
                  overflow: 'hidden', overflowWrap: 'anywhere',
                }}>{latestLog.message}</p>
                <button
                  onClick={() => setLogsOpen(true)}
                  style={{ marginTop: 8, background: 'none', border: 'none', padding: 0, fontSize: 12, color: C.secondary, cursor: 'pointer', fontFamily: 'inherit' }}
                >View all {logs.length} {logs.length === 1 ? 'line' : 'lines'}</button>
              </>
            ) : (
              <p style={{ margin: '8px 0 0', fontSize: 12, color: C.muted }}>Waiting for logs…</p>
            )}
          </div>
        </div>
      )}

      {/* Action */}
      <div style={{
        marginTop: 16, paddingTop: 14, borderTop: `1px solid ${C.hairline}`, flexShrink: 0,
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      }}>
        {detailUrl ? (
          <Link
            to={detailUrl}
            state={{ from: { path: '/overview', label: 'Overview' } }}
            style={{ fontSize: 13, fontWeight: 500, color: C.text, textDecoration: 'none' }}
          >
            {entry.isMerge ? 'Open in Merges' : 'Open in Queries'} &rarr;
          </Link>
        ) : <span />}
        <span style={{ fontSize: 11, color: C.muted }}>Esc to close</span>
      </div>
    </div>
  );
}
