import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { css } from '@emotion/css';
import type { MemoryTimeline, OperationKind, AttributionSelection, OperationalEvent } from '@tracehouse/core';
import { attributeTimeline, getTimelineRamCapacity, parseOperationTimestamp } from '@tracehouse/core';
import { formatBytes } from '../../../utils/formatters';
import { ModalWrapper } from '../../shared/ModalWrapper';
import { TimelineNavigator } from '../../shared/TimelineNavigator';
import { METRIC_CONFIG, type MetricMode } from '../timeline-constants';
import { TimeTravelServerSelector } from '../TimeTravelServerSelector';
import { TimelineEventOverlay } from '../TimelineEventOverlay';
import { segmentsStyle } from '../TimeTravelSegments.styles';
import { controlsStyle } from '../TimeTravelControls.styles';
import { ResourceInfoPopover } from './ResourceInfoPopover';
import { ZeroResourceActivity } from './ZeroResourceActivity';
import { useResourceData } from './useResourceData';
import { ResourceAttributionFlow, type ResourceFlowSelection } from './ResourceAttributionFlow';
import { explorationServerSeries, attributionConsumers, resourceConsumers, groupResourceFlow, type ResourceDataLoader, type ResourceOperationOpen } from './resource-exploration-model';

const styles = css`
  padding: 16px; overflow: auto; color: var(--text-primary); font-size: 12px;
  .resource-action, .resource-select { font: inherit; color: var(--text-primary); background: var(--bg-tertiary); border: 1px solid var(--border-primary); border-radius: 4px; padding: 6px 10px; }
  .resource-action { cursor: pointer; }
  button:focus-visible, select:focus-visible, input:focus-visible, [role=button]:focus-visible { outline: 2px solid #58a6ff; outline-offset: 2px; }
  .heading, .controls { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; }
  .heading { justify-content: space-between; }
  h2 { font-size: 16px; margin: 0; }
  .modal-heading { gap: 10px; }
  .origin { flex: 1; min-width: 180px; }
  .metrics-line { display: grid; grid-template-columns: 390px minmax(0, 1fr); align-items: center; gap: 12px; min-width: 0; }
  .muted { color: var(--text-muted); font-size: 11px; line-height: 1.6; }
  .controls { display: grid; grid-template-columns: 80px 160px minmax(0, 1fr); gap: 8px; margin: 10px 0 6px; padding-bottom: 8px; border-bottom: 1px solid var(--border-primary); }
  label { display: flex; align-items: center; gap: 6px; min-width: 0; }
  label select { width: 100%; min-width: 0; }
  .context { margin: 8px 0 0; }
  .comparison { width: 390px; max-width: 100%; display: grid; grid-template-columns: 148px minmax(40px, 1fr) 78px; align-items: center; gap: 5px 10px; font-size: 11px; }
  .comparison strong { font-size: 12px; font-weight: 500; text-align: right; font-variant-numeric: tabular-nums; }
  .comparison-track { position: relative; height: 8px; background: var(--bg-tertiary); border-radius: 2px; }
  .comparison-fill { height: 100%; border-radius: 2px; }
  .comparison-capacity { position: absolute; top: -2px; bottom: -2px; border-left: 1px solid var(--text-muted); }
  .comparison .operation-fill { background: repeating-linear-gradient(135deg, #7b9eed 0px, #7b9eed 3px, #a4bdf2 3px, #a4bdf2 6px); }
  .coverage { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 12px; min-width: 0; margin-top: 0; }
  .leaf-limit { display: flex; align-items: center; gap: 6px; white-space: nowrap; }
  .leaf-select { position: relative; }
  .leaf-select select { appearance: none; width: 60px; height: 26px; padding: 0 22px 0 10px; border: 0; border-radius: 5px; background: var(--bg-primary); color: var(--text-primary); box-shadow: 0 1px 3px rgba(0,0,0,.1); font: inherit; cursor: pointer; }
  .leaf-select svg { position: absolute; right: 7px; top: 9px; pointer-events: none; }
  .time-navigation { position: relative; border-radius: 8px; }
  .time-selection-keyboard { position: absolute; inset: 0; pointer-events: none; border-radius: 8px; }
  .time-selection-keyboard:focus-visible { outline: 2px solid #58a6ff; outline-offset: 3px; }
  .notice { margin: 12px 0; padding: 10px; background: var(--bg-tertiary); line-height: 1.6; }
  .flow-viewport { overflow-x: auto; overscroll-behavior-x: contain; scrollbar-gutter: stable; }
  .flow-navigation { margin: 12px 0 8px; }
  .flow svg { height: auto; display: block; max-width: none; }
  .flow text { fill: var(--text-primary); font-size: 12px; }
  .flow .value { fill: var(--text-muted); font-size: 11px; }
  .flow g[role=button] { cursor: pointer; }
  .flow g[role=button]:hover path { opacity: .5; }
  .flow .operation-hint { fill: var(--accent-blue); }
  .flow g[role=button]:hover .operation-link, .flow g[role=button]:focus-visible .operation-link { fill: var(--accent-blue); text-decoration: underline; }
  @media (max-width: 1100px) { .metrics-line { grid-column: 1 / -1; } }
  @media (max-width: 700px) { padding: 12px; .metrics-line { grid-template-columns: minmax(0, 1fr); } .comparison { width: 100%; } }
`;

function instantFormat(value: number, mode: MetricMode) {
  return mode === 'cpu' ? `${(value / 1e6).toLocaleString(undefined, { maximumFractionDigits: 2 })} cores`
    : `${formatBytes(value)}${mode === 'memory' ? '' : '/s'}`;
}

export function ResourceExplorationModal({ data, atMs, initialMode, hiddenCategories, hostLabel, availableHosts, initialHosts, events, onViewEventDetails, onLoadResource, onOpenOperation, onClose, initialDataLoaded = false, initialSamplingWarning }: {
  data: MemoryTimeline; atMs: number; initialMode: MetricMode; hiddenCategories?: ReadonlySet<OperationKind>; hostLabel?: string; availableHosts?: readonly string[]; initialHosts?: readonly string[]; events?: OperationalEvent[]; onViewEventDetails?: (event: OperationalEvent) => void; onLoadResource?: ResourceDataLoader; onOpenOperation?: ResourceOperationOpen; initialDataLoaded?: boolean; initialSamplingWarning?: string; onClose: () => void;
}) {
  const [mode, setMode] = useState(initialMode);
  const [groupBy, setGroupBy] = useState<'activity' | 'table'>('activity');
  const [operationLimit, setOperationLimit] = useState(5);
  const [eventsVisible, setEventsVisible] = useState(true);
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const accountingId = useId();
  const [originalData] = useState(data);
  const [time, setTime] = useState(atMs);
  const [selectionMode, setSelectionMode] = useState<'instant' | 'range'>('instant');
  const [rangeSpan, setRangeSpan] = useState(60_000);
  const [originHosts] = useState<string[]>(() => hostLabel ? [hostLabel] : [...(initialHosts ?? [])]);
  const [selectedHosts, setSelectedHosts] = useState<string[]>(originHosts);
  // An explicit empty scope overrides the originating page's hostname filter.
  const hostScope = useMemo(() => selectedHosts.length ? selectedHosts : hostLabel || initialHosts ? [] : undefined, [selectedHosts, hostLabel, initialHosts]);
  const scopedEvents = useMemo(() => (events ?? []).filter(event =>
    !selectedHosts.length || !event.hostname || selectedHosts.includes(event.hostname)), [events, selectedHosts]);
  const originStart = Math.min(atMs, parseOperationTimestamp(originalData.window_start));
  const originEnd = Math.max(atMs, parseOperationTimestamp(originalData.window_end), originStart + 1);
  const min = selectionMode === 'range' ? Math.min(originStart, atMs - rangeSpan / 2) : originStart;
  const max = selectionMode === 'range' ? Math.max(originEnd, atMs + rangeSpan / 2) : originEnd;
  const selectionSpan = selectionMode === 'range' ? Math.min(rangeSpan, max - min) : Math.min(1000, max - min);
  const rangeStart = Math.max(min, Math.min(max - selectionSpan, time - selectionSpan / 2));
  const selection: AttributionSelection = selectionMode === 'instant' ? { kind: 'instant', atMs: time }
    : { kind: 'range', startMs: rangeStart, endMs: rangeStart + selectionSpan };
  const { source, loading, loadError, samplingWarning, contextSeries, retry } = useResourceData(originalData, mode, time, hostScope, onLoadResource, selection, initialDataLoaded ? { samplingWarning: initialSamplingWarning } : undefined);

  const [selected, setSelected] = useState<ResourceFlowSelection | null>(null);
  const closeButton = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    closeButton.current?.focus();
  }, []);
  const host = onLoadResource && !loading && !loadError ? undefined : hostScope;
  const attribution = useMemo(() => attributeTimeline(source, { selection: selectionMode === 'instant' ? { kind: 'instant', atMs: time }
    : { kind: 'range', startMs: rangeStart, endMs: rangeStart + selectionSpan }, mode, hostname: host, hiddenKinds: hiddenCategories }),
  [source, selectionMode, time, rangeStart, selectionSpan, mode, host, hiddenCategories]);
  const { consumers, missingMemory } = useMemo(() => attributionConsumers(attribution), [attribution]);
  const series = useMemo(() => contextSeries ?? explorationServerSeries(source, mode, host ?? ''), [source, mode, host, contextSeries]);
  const measured = attribution.measured;
  const total = attribution.total;
  const percent = (value: number, denominator: number) => {
    const share = denominator > 0 ? value / denominator * 100 : 0;
    return share > 0 && share < .1 ? '<0.1%' : `${share.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  };
  const fmt = (value: number, metric: MetricMode) => selectionMode === 'range' ? percent(value, total) : instantFormat(value, metric);
  const flowFormat = (value: number, metric: MetricMode) => selectionMode === 'range'
    ? percent(value, total) : `${instantFormat(value, metric)} · ${percent(value, total)}`;
  const formatComparison = (value: number) => selectionMode === 'range' ? percent(value, attribution.capacity ?? total) : instantFormat(value, mode);

  const sampled = attribution.coverage.sampled;
  const hosts = availableHosts ? [...availableHosts] : [...new Set([...Object.keys(originalData.per_host_cpu ?? {}), ...originalData.queries.map(q => q.hostname), ...originalData.merges.map(m => m.hostname), ...(originalData.mutations ?? []).map(m => m.hostname)].filter((h): h is string => !!h))].sort();
  const duration = selectionMode === 'range' ? selectionSpan / 1000 : 1;
  const capacity = mode === 'cpu' ? (attribution.capacity ?? 0) / 1e6 / duration : 0;
  const comparisonCapacity = attribution.capacity ?? 0;
  // Independent values on one scale: estimates may exceed the server measurement or capacity.
  const comparisonMax = Math.max(comparisonCapacity, measured.value ?? 0, total, 1);
  const operationsUnavailable = mode === 'memory' && missingMemory > 0 && consumers.length === 0;
  const resourceLabel = METRIC_CONFIG[mode].label;
  // Instant values use a fixed resource reference while scrubbing; range shares normalize to 100%.
  const flowScaleReference = useMemo(() => {
    if (selectionMode === 'range') return Math.max(total, 1);
    if ((mode === 'cpu' || mode === 'memory') && attribution.capacity) return attribution.capacity;
    const ram = !host || source.host_count === 1 ? getTimelineRamCapacity(source) : 0;
    if (mode === 'memory' && ram > 0) return ram;
    const pinTotal = resourceConsumers(source, atMs, mode, host, hiddenCategories).consumers.reduce((sum, c) => sum + c.value, 0);
    return series.reduce((peak, point) => Math.max(peak, point.v), Math.max(pinTotal, 1));
  }, [source, atMs, mode, host, hiddenCategories, series, attribution.capacity, selectionMode, total]);
  const tableGroups = useMemo(() => groupResourceFlow(consumers, groupBy), [consumers, groupBy]);
  // A narrow native navigator window supplies the same grip/drag behavior as
  // Time Travel while selecting one instant, not aggregating an interval.
  const change = () => { setSelected(null); setSelectedEventId(null); };
  const selectFlow = (selection: ResourceFlowSelection | null) => {
    const consumer = selection?.consumerId ? consumers.find(c => c.id === selection.consumerId) : undefined;
    setSelected(selection);
    if (consumer && onOpenOperation) onOpenOperation(consumer.kind, consumer.source);
  };
  return <ModalWrapper isOpen onClose={onClose} maxWidth={1100} height="85vh" ariaLabel="Resource usage at the selected time">
    <div className={styles}>
      <div className="heading modal-heading"><h2>Resource usage</h2>
        <div className="muted origin">Pinned {new Date(atMs).toLocaleString()} · {hostLabel || `${originalData.host_count} server${originalData.host_count === 1 ? '' : 's'}`}</div>
        <button className="resource-action" ref={closeButton} onClick={onClose} aria-label="Close resource exploration">×</button>
      </div>
      <div className="controls">
        <label><select className="resource-select" aria-label="Resource" value={mode} onChange={e => { setMode(e.target.value as MetricMode); change(); }}>{Object.entries(METRIC_CONFIG).map(([key, cfg]) => <option key={key} value={key}>{cfg.label}</option>)}</select></label>
        <div className={controlsStyle}><TimeTravelServerSelector display="dropdown" hosts={hosts} selectedHosts={selectedHosts} onChange={next => { setSelectedHosts(next); change(); }} /></div>
        <div className="metrics-line">
          <div className="comparison" role="group" aria-label={`${resourceLabel} comparison`}>
            <span>{selectionMode === 'range' ? attribution.capacity ? 'Server / capacity' : 'Server / attributed' : `Server ${resourceLabel} usage`}</span>
            <div className="comparison-track" aria-hidden="true" title={comparisonCapacity > 0 ? selectionMode === 'range' ? '100% of available capacity over this interval' : `${instantFormat(comparisonCapacity, mode)} capacity` : undefined}>
              {!loading && measured.value !== null && <div className="comparison-fill" style={{ width: `${measured.value / comparisonMax * 100}%`, background: '#3fbb50' }} />}
              {comparisonCapacity > 0 && <span className="comparison-capacity" style={{ left: `${comparisonCapacity / comparisonMax * 100}%` }} />}
            </div>
            <strong>{loading ? 'Loading…' : measured.value === null ? 'Unavailable' : formatComparison(measured.value)}</strong>
            <span>{selectionMode === 'range' ? attribution.capacity ? 'Operations / capacity' : 'Attributed total' : `Operations ${resourceLabel}`}</span>
            <div className="comparison-track" aria-hidden="true" title={mode === 'cpu' ? 'Interval CPU samples and lifetime-average CPU estimates; not a reconciled allocation of server usage' : 'Available samples for active queries, merges, and mutations; not a reconciled allocation of server usage'}>
              {!loading && !operationsUnavailable && <div className="comparison-fill operation-fill" style={{ width: `${total / comparisonMax * 100}%` }} />}
              {comparisonCapacity > 0 && <span className="comparison-capacity" style={{ left: `${comparisonCapacity / comparisonMax * 100}%` }} />}
            </div>
            <strong>{loading ? 'Loading…' : operationsUnavailable ? 'Unavailable' : formatComparison(total)}</strong>
          </div>
      <div className="coverage">
        <span className="muted">{sampled} sampled · {attribution.coverage.estimated} estimated{missingMemory > 0 ? ` · ${missingMemory} missing memory samples` : ''}{selectionMode === 'instant' && capacity > 0 ? ` · ${capacity} cores capacity` : ''}</span>
        <ResourceInfoPopover id={accountingId} label="Measurement details" title="How resource usage is calculated">
        <strong>How to read these values</strong>
        <ul>
          <li><strong>Server usage:</strong> {mode === 'cpu' ? 'Total CPU usage' : mode === 'memory' ? 'Memory tracked by ClickHouse' : mode === 'disk' ? 'Total disk read and write rate' : 'Total network send and receive rate'} across the selected servers.</li>
          <li><strong>Operation usage:</strong> Total usage attributed to queries, merges, and mutations in the current results. Operations excluded by filters or query result limits are omitted from this total.</li>
          {mode === 'memory' ? <li><strong>Sampled memory:</strong> Uses concurrent memory samples. Missing samples are excluded; peak memory is not substituted.</li> : <>
            <li><strong>Sampled:</strong> {selectionMode === 'instant' ? 'Uses a measurement recorded near the selected time.' : 'Uses measurements recorded during the selected range.'}</li>
            <li><strong>Estimated:</strong> {selectionMode === 'instant' ? 'No measurement is available near the selected time, so we use the operation’s average usage over its full execution. This cannot show whether it was busy or idle at that time.' : 'Where measurements are missing, we use the operation’s average usage over its full execution. This cannot show whether it was busy or idle during those gaps.'}</li>
          </>}
          {selectionMode === 'range' && <li><strong>Range percentages:</strong> Each branch’s share of accumulated {mode === 'cpu' ? 'CPU time' : mode === 'memory' ? 'memory occupancy (bytes × time)' : mode === 'disk' ? 'disk I/O bytes' : 'network I/O bytes'}. The Sankey total is 100%.</li>}
          {selectionMode === 'instant' && <li><strong>Sankey percentages:</strong> Each branch’s share of total operation usage at the selected time. The operation total is 100%.</li>}
          <li><strong>Multiple tables:</strong> Each operation’s cost is counted once under its table set. No per-table cost split is available.</li>
        </ul>
        </ResourceInfoPopover>
      </div>
        </div>
      </div>
      {loading && <div className="notice" role="status">Loading {METRIC_CONFIG[mode].label.toLowerCase()} history and operation samples…</div>}
      {loadError && <div className="notice" role="alert">Could not load this resource: {loadError} <button className="resource-action" onClick={retry}>Retry</button></div>}
      {samplingWarning && <div className="notice">{samplingWarning}</div>}
      <div className="heading" style={{ marginTop: 8, marginBottom: 6 }}>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
          <div className={controlsStyle}>
            <div className={segmentsStyle} role="group" aria-label="Attribution time selection">
              <button type="button" aria-pressed={selectionMode === 'instant'} onClick={() => { setSelectionMode('instant'); change(); }}>Selected instant</button>
              <button type="button" aria-pressed={selectionMode === 'range'} onClick={() => { setSelectionMode('range'); change(); }}>Time range</button>
            </div>
          </div>
          {selectionMode === 'range' && <div className={controlsStyle}><div className={segmentsStyle} role="group" aria-label="Attribution range">
            {[{ label: '1m', ms: 60_000 }, { label: '5m', ms: 300_000 }, { label: '15m', ms: 900_000 }, { label: '1h', ms: 3600_000 }, ...([60_000, 300_000, 900_000, 3600_000].includes(originEnd - originStart) ? [] : [{ label: 'Current view', ms: originEnd - originStart }])].map(preset =>
              <button type="button" key={preset.label} aria-pressed={rangeSpan === preset.ms} onClick={() => { setRangeSpan(preset.ms); change(); }}>{preset.label}</button>)}
          </div></div>}
          <span className={controlsStyle} style={{ padding: 0, border: 0, background: 'transparent' }} aria-hidden="true"><span className="tt-controls-divider" /></span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span className="muted">Group by</span>
            <div className={controlsStyle}><div className={segmentsStyle} role="group" aria-label="Sankey grouping">
              <button type="button" aria-pressed={groupBy === 'activity'} onClick={() => { setGroupBy('activity'); change(); }}>Activity</button>
              <button type="button" aria-pressed={groupBy === 'table'} onClick={() => { setGroupBy('table'); change(); }}>Table</button>
            </div></div>
          </div>
          <span className={controlsStyle} style={{ padding: 0, border: 0, background: 'transparent' }} aria-hidden="true"><span className="tt-controls-divider" /></span>
          <div className="leaf-limit">
            <span className="muted">Operations</span>
            <div className={controlsStyle}><div className="leaf-select">
            <select aria-label="Operations per type" aria-describedby={`${accountingId}-leaves`} value={Number.isFinite(operationLimit) ? operationLimit : 'all'}
              onChange={event => setOperationLimit(event.target.value === 'all' ? Infinity : Number(event.target.value))}>
              {[5, 10, 20, 50].map(limit => <option key={limit} value={limit}>{limit}</option>)}
              <option value="all">All</option>
            </select>
            <svg width="8" height="8" viewBox="0 0 8 8" aria-hidden="true"><path d="m1 2 3 3 3-3" fill="none" stroke="currentColor" /></svg>
            </div></div>
            <ResourceInfoPopover id={`${accountingId}-leaves`} label="Leaf limit information" title="Maximum operations per branch">
              Shows the largest individual operations per expanded activity branch. Remaining operations are combined into an Other operations branch; totals are unchanged.
            </ResourceInfoPopover>
          </div>
          <span className={controlsStyle} style={{ padding: 0, border: 0, background: 'transparent' }} aria-hidden="true"><span className="tt-controls-divider" /></span>
          <div className={controlsStyle}><div className={segmentsStyle}>
            <button type="button" aria-pressed={eventsVisible} title="Show events already loaded by Time Travel" onClick={() => { setEventsVisible(value => !value); setSelectedEventId(null); }}>Events</button>
          </div></div>
        </div>
        {selectionMode === 'range' && <span className="muted">Shares of accumulated {mode === 'cpu' ? 'CPU time' : mode === 'memory' ? 'sampled memory occupancy' : mode === 'disk' ? 'disk I/O' : 'network I/O'} · {total > 0 ? 'Attributed total = 100%' : 'No attributed resource use'}</span>}
        <button type="button" className="resource-action" title="Restore the pinned time, resource, and server selection" onClick={() => { if (loadError) retry(); setSelectionMode('instant'); setTime(atMs); setMode(initialMode); setSelectedHosts(originHosts); change(); }} style={{ marginLeft: 'auto' }}>Reset to pin</button>
      </div>
      <div className="context">
        <div className="time-navigation">
        <div className="time-selection-keyboard" role="slider" tabIndex={0} aria-label="Exploration time" aria-valuemin={min} aria-valuemax={max} aria-valuenow={time} aria-valuetext={new Date(time).toLocaleString()}
          onKeyDown={event => {
            const step = event.shiftKey ? 10000 : 1000;
            const next = event.key === 'Home' ? min : event.key === 'End' ? max
              : event.key === 'ArrowLeft' || event.key === 'ArrowDown' ? time - step
              : event.key === 'ArrowRight' || event.key === 'ArrowUp' ? time + step : null;
            if (next !== null) { event.preventDefault(); setTime(Math.max(min, Math.min(max, next))); change(); }
          }} />
          <TimelineNavigator data={series} metricMode={mode} height={70} isLoading={loading}
            rangeStartMs={selectionMode === 'range' ? min : min - selectionSpan / 2} rangeEndMs={selectionMode === 'range' ? max : max + selectionSpan / 2}
            viewportStartMs={selectionMode === 'range' ? rangeStart : time - selectionSpan / 2} viewportEndMs={selectionMode === 'range' ? rangeStart + selectionSpan : time + selectionSpan / 2}
            selectionLabel={selectionMode === 'instant' ? new Date(time).toLocaleTimeString() : undefined}
            cpuCores={capacity} totalRam={host && source.host_count > 1 ? 0 : getTimelineRamCapacity(source)}
            onViewportChange={endMs => { setTime(Math.max(min, Math.min(max, endMs - selectionSpan / 2))); change(); }} />
          {eventsVisible && <div style={{ position: 'absolute', inset: '0 0 20px', pointerEvents: 'none' }}>
            <TimelineEventOverlay events={scopedEvents}
              rangeStartMs={selectionMode === 'range' ? min : min - selectionSpan / 2}
              rangeEndMs={selectionMode === 'range' ? max : max + selectionSpan / 2}
              selectedEventId={selectedEventId} onSelectEvent={event => setSelectedEventId(event.id)}
              onClearEventSelection={() => setSelectedEventId(null)} onViewEventDetails={onViewEventDetails}
              consumeEscape />
          </div>}
        </div>
      </div>
      {selectionMode === 'range' && measured.value !== null && measured.coverageMs < selectionSpan * .95 && <div className="muted">Server measurement coverage: {((measured.coverageMs / selectionSpan) * 100).toFixed(1)}% of the selected time.</div>}
      {selectionMode === 'range' && missingMemory > 0 && <div className="muted" role="status">Partial memory coverage: {missingMemory} operations have unsampled intervals. Occupancy and peak include observed samples only.</div>}
      {!loading && !loadError && <ZeroResourceActivity attribution={attribution} formattedZero={fmt(0, mode)} />}
      {!loading && !loadError && (consumers.length > 0 ? <>
        <ResourceAttributionFlow groupBy={groupBy} operationLimit={operationLimit} selectionKind={selectionMode} scaleReference={flowScaleReference} groups={tableGroups} mode={mode} selection={selected} onSelect={selectFlow} format={flowFormat} />
      </> : <div className="notice">{mode === 'memory' && missingMemory > 0 ? 'Concurrent memory attribution is unavailable at this time. Choose a time with operation samples.' : 'No contributing loaded operations at this time.'}</div>)}
    </div>
  </ModalWrapper>;
}
