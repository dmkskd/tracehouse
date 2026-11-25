import { useEffect, useRef, useState } from 'react';
import { describeTimelineFilters, explainListedOperations, formatBytesThreshold, formatDurationThreshold, MIN_DURATION_CHOICES, MIN_MEMORY_CHOICES } from '@tracehouse/core';
import { useTimelineFilters } from './timeline-filters-preference';
import { showMenu as styles } from '../common/showMenuStyles';


const TOP_CHOICES = [25, 50, 100, 250, 500];
const METRIC_NAMES: Record<OperationsMetric, string> = { cpu: 'CPU', memory: 'memory', network: 'network', disk: 'disk I/O' };
export type OperationsMetric = 'cpu' | 'memory' | 'network' | 'disk';

/**
 * Which operations Time Travel lists: the top N queries, merges and mutations by the chart's
 * metric, among those over the memory or duration threshold, plus optional kinds of query.
 * The button label is the summary.
 */
export function OperationsMenu({ activityLimit, onActivityLimit, includeRunning, onIncludeRunning, isLive, metric }: {
  activityLimit: number; onActivityLimit: (limit: number) => void;
  includeRunning: boolean; onIncludeRunning: (include: boolean) => void; isLive: boolean; metric: OperationsMetric;
}) {
  const { filters, defaults, changed, fromLink, choose, reset } = useTimelineFilters();
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', close);document.addEventListener('keydown', escape);
    return () => { document.removeEventListener('mousedown', close);document.removeEventListener('keydown', escape); };
  }, [open]);
  const metricName = METRIC_NAMES[metric];
  const summary = `top ${activityLimit} by ${metricName} · ${describeTimelineFilters(filters)}`;
  return <div className={styles} ref={root}>
    <button type="button" className="filters-button" aria-expanded={open} aria-haspopup="dialog" onClick={() => setOpen(!open)}
      aria-label="Listed operations"
      title={explainListedOperations(activityLimit, metricName, filters)}>
      <span className="label">Show</span><span className="value">{summary}</span>
      <span className="caret" aria-hidden="true">▾</span>
    </button>
    {open && <div className="menu-panel" role="dialog" aria-label="Listed operations">
      <div className="rows">
        <span className="phrase">Show the top</span>
        <span className="choices">{TOP_CHOICES.map(n => <button key={n} type="button" aria-pressed={activityLimit === n}
          onClick={() => onActivityLimit(n)}>{n}</button>)}</span>
        <span />
        <span className="note">queries, merges and mutations each, by {metricName}</span>
        <span className="phrase">that used more than</span>
        <span className="choices">{MIN_MEMORY_CHOICES.map(bytes => <button key={bytes} type="button" aria-pressed={filters.minMemoryBytes === bytes}
          onClick={() => choose({ minMemoryBytes: bytes })}>{bytes === 0 ? 'Off' : formatBytesThreshold(bytes)}</button>)}</span>
        <span className="phrase">or ran longer than</span>
        <span className="choices">{MIN_DURATION_CHOICES.map(ms => <button key={ms} type="button" aria-pressed={filters.minDurationMs === ms}
          onClick={() => choose({ minDurationMs: ms })}>{ms === 0 ? 'Off' : formatDurationThreshold(ms)}</button>)}</span>
        <span className="phrase">Also show</span>
        <span className="choices">
          <button type="button" aria-pressed={includeRunning && isLive} disabled={!isLive} title={isLive ? 'Queries and merges still running' : 'Live mode only'}
            onClick={() => onIncludeRunning(!includeRunning)}>{includeRunning && isLive && <span aria-hidden="true">✓ </span>}in-flight</button>
          <button type="button" aria-pressed={filters.includeFailedBeforeStart} title="Queries that failed before they started (ExceptionBeforeStart), e.g. access denied or unknown table"
            onClick={() => choose({ includeFailedBeforeStart: !filters.includeFailedBeforeStart })}>{filters.includeFailedBeforeStart && <span aria-hidden="true">✓ </span>}failed before start</button>
          <button type="button" aria-pressed={filters.includeTraceHouse} title="Queries TraceHouse runs to draw its pages"
            onClick={() => choose({ includeTraceHouse: !filters.includeTraceHouse })}>{filters.includeTraceHouse && <span aria-hidden="true">✓ </span>}TraceHouse's own</button>
        </span>
      </div>
      <div className="footer">
        <span>{fromLink ? 'Thresholds set by the link' : `Default: ${describeTimelineFilters(defaults)}`}</span>
        {changed && <button type="button" onClick={reset}>Reset to default</button>}
      </div>
    </div>}
  </div>;
}
