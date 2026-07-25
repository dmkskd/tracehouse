import React, { useState } from 'react';
import type { QueryExecutionAnalysisResult } from '@tracehouse/core';
import { formatBytes, formatDurationMs, formatMicroseconds } from '../../utils/formatters';
import type { PreviousExecutionMetrics } from './executionAnalysisModel';
import { ExecutionAnalysisVisual } from './ExecutionAnalysisVisual';

interface AnalysisRunButtonProps {
  label: string;
  runningLabel: string;
  isRunning: boolean;
  onClick: () => void;
  disabled?: boolean;
  title?: string;
}

/** Shared primary action for analysis views. */
export const AnalysisRunButton: React.FC<AnalysisRunButtonProps> = ({
  label,
  runningLabel,
  isRunning,
  onClick,
  disabled = false,
  title,
}) => {
  const isDisabled = disabled || isRunning;

  return (
    <button
      type="button"
      className="btn btn-primary"
      onClick={onClick}
      disabled={isDisabled}
      title={title}
      style={{
        flexShrink: 0,
        padding: '8px 18px',
        borderRadius: 6,
        fontSize: 11,
        cursor: isDisabled ? 'not-allowed' : 'pointer',
        opacity: isDisabled ? 0.45 : 1,
      }}
    >
      {isRunning ? runningLabel : label}
    </button>
  );
};

interface ExecutionAnalysisDialogProps {
  onConfirm: () => void;
  onCancel: () => void;
  title?: string;
  message?: string;
  confirmLabel?: string;
}

function formatPreviousMetric(
  value: number | undefined,
  formatter: (metric: number) => string,
): string {
  return value !== undefined && Number.isFinite(value) && value >= 0
    ? formatter(value)
    : 'Unavailable';
}

interface PreviousExecutionSummaryProps {
  metrics: PreviousExecutionMetrics;
}

/** Historical resource usage shown before the user starts a new execution. */
export const PreviousExecutionSummary: React.FC<PreviousExecutionSummaryProps> = ({
  metrics,
}) => (
  <div>
    <div style={{
      display: 'flex',
      alignItems: 'baseline',
      justifyContent: 'space-between',
      gap: 12,
      marginBottom: 8,
    }}>
      <div style={{ color: 'var(--text-tertiary)', fontSize: 10, fontWeight: 600, letterSpacing: '0.5px', textTransform: 'uppercase' }}>
        Previous execution
      </div>
      <div style={{ color: 'var(--text-muted)', fontSize: 9 }}>
        Reference only. The next run may differ.
      </div>
    </div>
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
      border: '1px solid var(--border-primary)',
      borderRadius: 6,
      overflow: 'hidden',
      background: 'var(--bg-code, #0d1117)',
    }}>
      {[
        {
          label: 'Elapsed',
          value: formatPreviousMetric(metrics.elapsedMs, formatDurationMs),
        },
        {
          label: 'CPU time',
          value: formatPreviousMetric(metrics.cpuTimeUs, formatMicroseconds),
        },
        {
          label: 'Peak memory',
          value: formatPreviousMetric(metrics.peakMemoryBytes, formatBytes),
        },
      ].map((metric, index) => (
        <div
          key={metric.label}
          style={{
            minWidth: 0,
            padding: '10px 12px',
            borderLeft: index > 0 ? '1px solid var(--border-primary)' : 'none',
          }}
        >
          <div style={{ color: 'var(--text-muted)', fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.4px' }}>
            {metric.label}
          </div>
          <div style={{
            marginTop: 4,
            overflow: 'hidden',
            color: 'var(--text-primary)',
            fontFamily: "'Share Tech Mono','Fira Code',monospace",
            fontSize: 13,
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}>
            {metric.value}
          </div>
        </div>
      ))}
    </div>
  </div>
);

interface ResolvedQueryPreviewProps {
  query: string;
}

/** SQL review shown on the setup page, before the confirmation boundary. */
export const ResolvedQueryPreview: React.FC<ResolvedQueryPreviewProps> = ({
  query,
}) => (
  <div>
    <div style={{ color: 'var(--text-tertiary)', fontSize: 10, fontWeight: 600, letterSpacing: '0.5px', textTransform: 'uppercase' }}>
      Resolved query
    </div>
    <pre style={{
      maxHeight: 170,
      margin: '8px 0 0',
      padding: 12,
      overflow: 'auto',
      border: '1px solid var(--border-primary)',
      borderRadius: 6,
      background: 'var(--bg-code, #0d1117)',
      color: 'var(--text-secondary)',
      fontFamily: "'Share Tech Mono','Fira Code',monospace",
      fontSize: 11,
      lineHeight: 1.5,
      whiteSpace: 'pre-wrap',
      wordBreak: 'break-word',
    }}>
      {query}
    </pre>
  </div>
);

/**
 * Minimal safety boundary for EXPLAIN ANALYZE. Configuration and feature
 * documentation belong on the invoking page; this dialog only obtains consent.
 */
export const ExecutionAnalysisDialog: React.FC<ExecutionAnalysisDialogProps> = ({
  onConfirm,
  onCancel,
  title = 'Confirm query execution?',
  message = 'ClickHouse will execute this SELECT.',
  confirmLabel = 'Confirm',
}) => (
  <div
    role="presentation"
    onMouseDown={(event) => {
      if (event.target === event.currentTarget) onCancel();
    }}
    style={{
      position: 'fixed',
      inset: 0,
      zIndex: 100000,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: 24,
      background: 'var(--backdrop-overlay)',
    }}
  >
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="execution-analysis-title"
      style={{
        width: 'min(480px, 100%)',
        border: '1px solid var(--border-primary)',
        borderRadius: 10,
        background: 'var(--bg-secondary)',
        boxShadow: 'var(--shadow-modal)',
        overflow: 'hidden',
      }}
    >
      <div style={{ padding: '18px 20px' }}>
        <div id="execution-analysis-title" style={{ color: 'var(--text-primary)', fontSize: 15, fontWeight: 650 }}>
          {title}
        </div>
        <div style={{ marginTop: 8, color: 'var(--color-warning)', fontSize: 12, lineHeight: 1.55 }}>
          {message}
        </div>
      </div>

      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, padding: '12px 20px', borderTop: '1px solid var(--border-primary)' }}>
        <button
          onClick={onCancel}
          style={{ padding: '6px 14px', border: '1px solid var(--border-primary)', borderRadius: 5, background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer' }}
        >
          Cancel
        </button>
        <button
          className="btn btn-primary"
          onClick={onConfirm}
          style={{ padding: '6px 14px', borderRadius: 5, cursor: 'pointer' }}
        >
          {confirmLabel}
        </button>
      </div>
    </div>
  </div>
);

interface ProcessorTimingOptionProps {
  checked: boolean;
  onChange: (enabled: boolean) => void;
  compact?: boolean;
}

/** Reusable EXPLAIN ANALYZE configuration control, shown before confirmation. */
export const ProcessorTimingOption: React.FC<ProcessorTimingOptionProps> = ({
  checked,
  onChange,
  compact = false,
}) => (
  <label style={{
    display: 'flex',
    alignItems: compact ? 'center' : 'flex-start',
    gap: compact ? 6 : 10,
    color: 'var(--text-secondary)',
    fontSize: compact ? 10 : 12,
    cursor: 'pointer',
  }}>
    <input
      type="checkbox"
      checked={checked}
      onChange={(event) => onChange(event.target.checked)}
      style={{ marginTop: compact ? 0 : 2 }}
    />
    <span>
      Include processor-level timing stats
      {!compact && (
        <span style={{ display: 'block', marginTop: 3, color: 'var(--text-muted)', fontSize: 10, lineHeight: 1.4 }}>
          Adds detailed timing per execution-plan processor to help identify slow or unevenly loaded stages.
        </span>
      )}
    </span>
  </label>
);

interface ExecutionAnalysisPanelProps {
  result: QueryExecutionAnalysisResult;
  requestDurationMs: number;
  onAnalyzeAgain?: () => void;
  onClose?: () => void;
}

/** Visual-first runtime plan with the authoritative ClickHouse text as its final tab. */
export const ExecutionAnalysisPanel: React.FC<ExecutionAnalysisPanelProps> = ({
  result,
  requestDurationMs,
  onAnalyzeAgain,
  onClose,
}) => {
  const [copied, setCopied] = useState(false);
  const [activeView, setActiveView] = useState<'visual' | 'raw'>('visual');

  const copy = async () => {
    await navigator.clipboard.writeText(result.output);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div style={{
      width: '100%',
      height: '100%',
      minWidth: 0,
      minHeight: 0,
      flex: 1,
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
    }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 12,
        padding: '7px 16px',
        flexShrink: 0,
        borderBottom: '1px solid var(--border-primary)',
        background: 'var(--bg-secondary)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0, overflow: 'hidden' }}>
          <span style={{ color: 'var(--text-primary)', fontSize: 12, fontWeight: 600 }}>Execution analysis</span>
          <span style={{ color: 'var(--accent-green)', fontFamily: "'Share Tech Mono',monospace", fontSize: 10 }}>
            EXPLAIN ANALYZE
          </span>
          {result.processors && (
            <span style={{ padding: '1px 6px', borderRadius: 4, background: 'rgba(var(--color-info-rgb), 0.1)', color: 'var(--color-info)', fontSize: 9 }}>
              PROCESSORS
            </span>
          )}
          <span style={{ color: 'var(--text-muted)', fontFamily: "'Share Tech Mono',monospace", fontSize: 10 }}>
            request {requestDurationMs.toFixed(1)}ms
          </span>
          {result.queryId && (
            <span
              title={`Query ID: ${result.queryId}`}
              style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', color: 'var(--text-muted)', fontFamily: "'Share Tech Mono',monospace", fontSize: 9, whiteSpace: 'nowrap' }}
            >
              {result.queryId}
            </span>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {onAnalyzeAgain && (
            <button
              onClick={onAnalyzeAgain}
              style={{ padding: '3px 9px', border: '1px solid rgba(var(--color-warning-rgb), 0.35)', borderRadius: 4, background: 'rgba(var(--color-warning-rgb), 0.08)', color: 'var(--color-warning)', fontSize: 10, cursor: 'pointer' }}
            >
              Run again
            </button>
          )}
          <button
            onClick={copy}
            title="Copy execution plan"
            style={{ padding: '3px 9px', border: '1px solid var(--border-primary)', borderRadius: 4, background: 'transparent', color: copied ? 'var(--accent-green)' : 'var(--text-muted)', fontSize: 10, cursor: 'pointer' }}
          >
            {copied ? '✓ Copied' : 'Copy plan'}
          </button>
          {onClose && (
            <button
              onClick={onClose}
              aria-label="Close execution analysis result"
              title="Close execution analysis result"
              style={{ padding: '3px 8px', border: '1px solid var(--border-primary)', borderRadius: 4, background: 'transparent', color: 'var(--text-muted)', fontSize: 10, cursor: 'pointer' }}
            >
              ×
            </button>
          )}
        </div>
      </div>
      <div
        role="tablist"
        aria-label="Execution analysis views"
        style={{
          display: 'flex',
          gap: 22,
          padding: '0 18px',
          flexShrink: 0,
          borderBottom: '1px solid var(--border-primary)',
          background: 'var(--bg-secondary)',
        }}
      >
        {([
          { key: 'visual' as const, label: 'Visual plan' },
          { key: 'raw' as const, label: 'Raw plan' },
        ]).map(view => {
          const active = activeView === view.key;
          return (
            <button
              key={view.key}
              type="button"
              role="tab"
              aria-selected={active}
              onClick={() => setActiveView(view.key)}
              style={{
                padding: '9px 2px 8px',
                border: 0,
                borderBottom: active ? '2px solid var(--color-info)' : '2px solid transparent',
                background: 'transparent',
                color: active ? 'var(--text-primary)' : 'var(--text-muted)',
                fontSize: 10,
                fontWeight: active ? 650 : 500,
                cursor: 'pointer',
              }}
            >
              {view.label}
            </button>
          );
        })}
      </div>
      <div
        role="tabpanel"
        aria-label={activeView === 'visual' ? 'Visual plan' : 'Raw plan'}
        style={{
          flex: 1,
          width: '100%',
          minHeight: 0,
          minWidth: 0,
          overflow: 'auto',
          background: activeView === 'raw'
            ? 'var(--bg-code, #0d1117)'
            : 'var(--bg-primary)',
        }}
      >
        {activeView === 'visual' ? (
          <ExecutionAnalysisVisual output={result.output} />
        ) : (
          <pre style={{
            width: '100%',
            minHeight: '100%',
            minWidth: 0,
            boxSizing: 'border-box',
            margin: 0,
            padding: '16px 20px 28px',
            overflow: 'auto',
            color: 'var(--text-secondary)',
            fontFamily: "'Share Tech Mono','Fira Code',monospace",
            fontSize: 11,
            lineHeight: 1.65,
            whiteSpace: 'pre',
          }}>
            {result.output}
          </pre>
        )}
      </div>
    </div>
  );
};
