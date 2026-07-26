import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type {
  MonitoringCapability,
  TimelineEvent,
  TimelineEventCategory,
  TimelineEventKind,
  TimelineEventSeverity,
  TimelineEventSourceCoverage,
} from '@tracehouse/core';
import { clampToAllowed, useRefreshConfig } from '@tracehouse/ui-shared';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import {
  useGlobalLastUpdatedStore,
  useRefreshSettingsStore,
} from '../../stores/refreshSettingsStore';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_KIND_LABELS,
  EVENT_SEVERITY_COLORS,
  TIMELINE_EVENT_CATEGORIES,
} from '../timeline/timeline-event-model';
import { EventDistribution } from './EventDistribution';
import { TimeRangePicker } from './TimeRangePicker';
import { DocsLink } from '../common/DocsLink';
import {
  MetricStrip,
  MetricStripDivider,
  MetricStripItem,
} from '../common/MetricStrip';
import { EventContextView } from './EventContextView';
import {
  buildEventMarkerSelection,
  countEventSeverities,
  SUPPORTED_EVENT_TYPES,
  eventDetailLabel,
  eventDetailSections,
  eventKindLabel,
  eventSourceExplanation,
  eventSourceStatusDetail,
  formatEventClusterRange,
  formatEventDateTime,
  observedEventKinds,
  selectTimelineEvent,
  sortAndFilterEvents,
  sortTimelineEvents,
  supportedEventCoverage,
  supportedEventGroups,
  toClickHouseEventTime,
  type EventMarkerSelection,
  type EventDetailSection,
  type SupportedEventAvailability,
} from './events-dashboard-model';

interface EventsDashboardProps {
  selectedEventId?: string;
  selectedEventTime?: string;
  rangeCenterTime?: string;
  rangeHours: number;
  timeRangeValue: string;
  onTimeRangeChange: (value: string | null) => void;
  onRangeSelect?: (startMs: number, endMs: number) => void;
  onSelectEvent: (event: TimelineEvent) => void;
  onOpenQueryDetails?: (event: TimelineEvent) => void;
  onInvestigateEvent?: (event: TimelineEvent) => void;
  onBackToTimeTravel?: () => void;
}

const EVENTS_MIN_AUTO_REFRESH_SECONDS = 10;

export const EventsDashboard: React.FC<EventsDashboardProps> = ({
  selectedEventId,
  selectedEventTime,
  rangeCenterTime,
  rangeHours,
  timeRangeValue,
  onTimeRangeChange,
  onRangeSelect,
  onSelectEvent,
  onOpenQueryDetails,
  onInvestigateEvent,
  onBackToTimeTravel,
}) => {
  const services = useClickHouseServices();
  const refreshConfig = useRefreshConfig();
  const refreshRateSeconds = useRefreshSettingsStore(state => state.refreshRateSeconds);
  const manualRefreshTick = useGlobalLastUpdatedStore(state => state.manualRefreshTick);
  const touchGlobalRefresh = useGlobalLastUpdatedStore(state => state.touch);
  const setGlobalRefreshStatus = useGlobalLastUpdatedStore(state => state.setStatus);
  const monitoringCapabilities = useMonitoringCapabilitiesStore(state => state.capabilities);
  const capabilityProbeStatus = useMonitoringCapabilitiesStore(state => state.probeStatus);
  const refreshMonitoringCapabilities = useMonitoringCapabilitiesStore(
    state => state.refresh,
  );
  const availableCapabilities = useMemo(
    () => monitoringCapabilities?.capabilities
      .filter(capability => capability.available)
      .map(capability => capability.id),
    [monitoringCapabilities],
  );
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [coverage, setCoverage] = useState<TimelineEventSourceCoverage[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [severity, setSeverity] = useState<'all' | TimelineEventSeverity>('all');
  const [category, setCategory] = useState<'all' | TimelineEventCategory>('all');
  const [kind, setKind] = useState<'all' | TimelineEventKind>('all');
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [eventHelpView, setEventHelpView] = useState<'events' | 'sources' | null>(null);
  const eventHelpRef = useRef<HTMLDivElement>(null);
  const fetchInFlightRef = useRef(false);
  const [liveRangeEndMs, setLiveRangeEndMs] = useState(() => Date.now());
  const [clusterSelection, setClusterSelection] = useState<EventMarkerSelection | null>(
    null,
  );
  const [selectedPanel, setSelectedPanel] = useState<'details' | 'context'>('details');

  const parsedRangeCenterMs = rangeCenterTime ? Date.parse(rangeCenterTime) : Number.NaN;
  const hasAnchoredRange = Number.isFinite(parsedRangeCenterMs);
  const anchorMs = useMemo(() => {
    return hasAnchoredRange ? parsedRangeCenterMs : 0;
  }, [hasAnchoredRange, parsedRangeCenterMs]);

  const timeRange = useMemo(() => {
    const rangeMs = rangeHours * 60 * 60 * 1000;
    const endMs = hasAnchoredRange ? anchorMs + rangeMs / 2 : liveRangeEndMs;
    return {
      startMs: hasAnchoredRange ? anchorMs - rangeMs / 2 : endMs - rangeMs,
      endMs,
      hasAnchor: hasAnchoredRange,
    };
  }, [anchorMs, hasAnchoredRange, liveRangeEndMs, rangeHours]);

  const fetchEvents = useCallback(async () => {
    if (
      !services
      || availableCapabilities === undefined
      || fetchInFlightRef.current
    ) return;
    fetchInFlightRef.current = true;
    setLoading(true);
    setError(null);
    const rangeMs = rangeHours * 60 * 60 * 1000;
    const requestEndMs = hasAnchoredRange
      ? anchorMs + rangeMs / 2
      : Date.now();
    const requestStartMs = hasAnchoredRange
      ? anchorMs - rangeMs / 2
      : requestEndMs - rangeMs;
    try {
      const result = await services.timelineService.getEvents({
        startTime: toClickHouseEventTime(new Date(requestStartMs)),
        endTime: toClickHouseEventTime(new Date(requestEndMs)),
        availableCapabilities,
        limit: 5000,
      });
      setEvents(result.events);
      setCoverage(result.coverage);
      if (!hasAnchoredRange) setLiveRangeEndMs(requestEndMs);
      touchGlobalRefresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Failed to load events');
      setGlobalRefreshStatus('error');
    } finally {
      fetchInFlightRef.current = false;
      setLoading(false);
    }
  }, [
    services,
    availableCapabilities,
    anchorMs,
    hasAnchoredRange,
    rangeHours,
    setGlobalRefreshStatus,
    touchGlobalRefresh,
  ]);

  useEffect(() => {
    void fetchEvents();
  }, [fetchEvents, manualRefreshTick]);

  useEffect(() => {
    if (
      !autoRefresh
      || refreshRateSeconds <= 0
      || hasAnchoredRange
      || !services
      || availableCapabilities === undefined
    ) return;
    const intervalSeconds = Math.max(
      EVENTS_MIN_AUTO_REFRESH_SECONDS,
      clampToAllowed(refreshRateSeconds, refreshConfig),
    );
    if (intervalSeconds <= 0) return;
    const intervalId = window.setInterval(() => {
      void fetchEvents();
    }, intervalSeconds * 1000);
    return () => window.clearInterval(intervalId);
  }, [
    autoRefresh,
    availableCapabilities,
    fetchEvents,
    hasAnchoredRange,
    refreshConfig,
    refreshRateSeconds,
    services,
  ]);

  const effectiveAutoRefreshSeconds = refreshRateSeconds > 0
    ? Math.max(
        EVENTS_MIN_AUTO_REFRESH_SECONDS,
        clampToAllowed(refreshRateSeconds, refreshConfig),
      )
    : 0;
  const refreshing = loading || capabilityProbeStatus === 'probing';

  const refreshEventsAndSources = useCallback(async () => {
    if (!services || refreshing) return;
    try {
      // Updating the capability store changes availableCapabilities and
      // triggers fetchEvents with the fresh source set.
      await refreshMonitoringCapabilities(services.adapter);
    } catch {
      setGlobalRefreshStatus('error');
    }
  }, [
    refreshMonitoringCapabilities,
    refreshing,
    services,
    setGlobalRefreshStatus,
  ]);

  useEffect(() => {
    if (!eventHelpView) return;
    const onMouseDown = (event: MouseEvent) => {
      if (
        eventHelpRef.current
        && !eventHelpRef.current.contains(event.target as Node)
      ) {
        setEventHelpView(null);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setEventHelpView(null);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [eventHelpView]);

  const sortedEvents = useMemo(
    () => sortTimelineEvents(events),
    [events],
  );
  const filteredEvents = useMemo(
    () => sortAndFilterEvents(events, { search, severity, category, kind }),
    [events, search, severity, category, kind],
  );

  const observedKinds = useMemo(
    () => observedEventKinds(events),
    [events],
  );

  const displayedEvents = useMemo(() => {
    if (!clusterSelection) return filteredEvents;
    return filteredEvents.filter(event => clusterSelection.eventIds.has(event.id));
  }, [clusterSelection, filteredEvents]);

  useEffect(() => {
    if (!clusterSelection) return;
    const visibleIds = new Set(filteredEvents.map(event => event.id));
    if ([...clusterSelection.eventIds].some(id => !visibleIds.has(id))) {
      setClusterSelection(null);
    }
  }, [clusterSelection, filteredEvents]);

  const selectedEvent = useMemo(() => {
    return selectTimelineEvent(
      displayedEvents,
      filteredEvents.length > 0 ? filteredEvents : sortedEvents,
      selectedEventId,
      selectedEventTime,
    );
  }, [
    displayedEvents,
    filteredEvents,
    selectedEventId,
    selectedEventTime,
    sortedEvents,
  ]);
  const severityCounts = useMemo(() => countEventSeverities(events), [events]);
  const coverageProblems = coverage.filter(item => item.status === 'failed' || item.truncated);
  const loadedSources = coverage.filter(item => item.status === 'loaded').length;

  return (
    <div style={{
      height: '100%',
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
      background: 'var(--bg-primary)',
    }}>
      <MetricStrip
        ariaLabel="Event severity summary"
        style={{ flexShrink: 0, margin: '12px 20px' }}
      >
        <MetricStripItem label="all events" value={events.length} color="#58a6ff" />
        <MetricStripDivider />
        <MetricStripItem
          label="critical"
          value={severityCounts.critical}
          color={severityCounts.critical > 0 ? '#f85149' : undefined}
          indicatorColor="#f85149"
        />
        <MetricStripItem
          label="errors"
          value={severityCounts.error}
          color={severityCounts.error > 0 ? '#f0883e' : undefined}
          indicatorColor="#f0883e"
        />
        <MetricStripItem
          label="warnings"
          value={severityCounts.warning}
          color={severityCounts.warning > 0 ? '#d29922' : undefined}
          indicatorColor="#d29922"
        />
        <MetricStripItem
          label="information"
          value={severityCounts.info}
          color={severityCounts.info > 0 ? '#58a6ff' : undefined}
          indicatorColor="#58a6ff"
        />
      </MetricStrip>

      <div style={{
        flexShrink: 0,
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        padding: '10px 20px',
        borderBottom: '1px solid var(--border-primary)',
        background: 'var(--bg-secondary)',
      }}>
        {onBackToTimeTravel && (
          <button onClick={onBackToTimeTravel} style={secondaryButtonStyle}>
            ← Time Travel
          </button>
        )}
        <span style={{ color: 'var(--text-secondary)', fontSize: 11 }}>
          Time range
        </span>
        <TimeRangePicker
          value={timeRangeValue}
          onChange={onTimeRangeChange}
          popoverAlign="left"
        />
        <div
          ref={eventHelpRef}
          style={{
            position: 'relative',
            marginLeft: 'auto',
            display: 'flex',
            alignItems: 'center',
            gap: 5,
          }}
        >
          <button
            onClick={() => setEventHelpView(value => (
              value === 'events' ? null : 'events'
            ))}
            aria-expanded={eventHelpView === 'events'}
            title="TraceHouse shows only the event types in this catalog"
            style={{
              ...secondaryButtonStyle,
              color: eventHelpView === 'events' ? '#58a6ff' : 'var(--text-secondary)',
              borderColor: eventHelpView === 'events'
                ? 'rgba(88,166,255,0.45)'
                : 'var(--border-primary)',
              background: eventHelpView === 'events'
                ? 'rgba(88,166,255,0.08)'
                : 'var(--bg-card)',
            }}
          >
            Coverage · {SUPPORTED_EVENT_TYPES.length} supported event types
          </button>
          <span style={{
            color: coverageProblems.length > 0 ? '#d29922' : 'var(--text-muted)',
            fontSize: 10,
          }}>
            {loadedSources}/{coverage.length} sources available
          </span>
          {eventHelpView && (
            <EventCoverageHelp
              coverage={coverage}
              capabilities={monitoringCapabilities?.capabilities ?? []}
              view={eventHelpView}
              onViewChange={setEventHelpView}
            />
          )}
        </div>
        <button
          onClick={() => setAutoRefresh(value => !value)}
          disabled={hasAnchoredRange || refreshRateSeconds <= 0}
          title={
            hasAnchoredRange
              ? 'Auto-refresh is available for live ranges only'
              : refreshRateSeconds <= 0
                ? 'Enable a global refresh rate in Settings first'
                : autoRefresh
                  ? `Auto-refresh every ${effectiveAutoRefreshSeconds}s — click to pause`
                  : `Enable auto-refresh (minimum ${EVENTS_MIN_AUTO_REFRESH_SECONDS}s for Events)`
          }
          style={{
            ...secondaryButtonStyle,
            display: 'inline-flex',
            alignItems: 'center',
            gap: 5,
            background: autoRefresh ? 'rgba(63,185,80,0.1)' : secondaryButtonStyle.background,
            border: autoRefresh
              ? '1px solid rgba(63,185,80,0.3)'
              : secondaryButtonStyle.border,
            color: autoRefresh ? '#3fb950' : secondaryButtonStyle.color,
            cursor: hasAnchoredRange || refreshRateSeconds <= 0 ? 'not-allowed' : 'pointer',
            opacity: hasAnchoredRange || refreshRateSeconds <= 0 ? 0.5 : 1,
          }}
        >
          {autoRefresh && (
            <span style={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              background: '#3fb950',
            }} />
          )}
          Auto{autoRefresh ? ` · ${effectiveAutoRefreshSeconds}s` : ''}
        </button>
        <button
          onClick={() => { void refreshEventsAndSources(); }}
          disabled={refreshing}
          style={secondaryButtonStyle}
        >
          {refreshing ? 'Loading…' : '↻ Refresh'}
        </button>
      </div>

      <div style={{ flexShrink: 0, padding: '10px 20px 0' }}>
        <EventDistribution
          events={filteredEvents}
          rangeStartMs={timeRange.startMs}
          rangeEndMs={timeRange.endMs}
          selectedEventId={selectedEvent?.id}
          onSelectEvent={event => {
            setClusterSelection(buildEventMarkerSelection([event], timeRange.endMs));
            onSelectEvent(event);
          }}
          onSelectCluster={(clusterEvents, primaryEvent) => {
            setClusterSelection(
              buildEventMarkerSelection(clusterEvents, timeRange.endMs),
            );
            onSelectEvent(primaryEvent);
          }}
          onRangeSelect={onRangeSelect}
        />
      </div>

      <div style={{
        flexShrink: 0,
        display: 'flex',
        gap: 8,
        padding: '0 20px 10px',
      }}>
        <input
          value={search}
          onChange={event => setSearch(event.target.value)}
          placeholder="Search title, host, query ID, table, exception…"
          style={{ ...inputStyle, flex: 1 }}
        />
        <select
          value={severity}
          onChange={event => setSeverity(event.target.value as typeof severity)}
          style={inputStyle}
        >
          <option value="all">All severities</option>
          <option value="critical">Critical</option>
          <option value="error">Error</option>
          <option value="warning">Warning</option>
          <option value="info">Info</option>
        </select>
        <select
          value={category}
          onChange={event => setCategory(event.target.value as typeof category)}
          style={inputStyle}
        >
          <option value="all">All categories</option>
          {TIMELINE_EVENT_CATEGORIES.map(value => (
            <option key={value} value={value}>{EVENT_CATEGORY_LABELS[value]}</option>
          ))}
        </select>
        <select
          value={kind}
          onChange={event => setKind(event.target.value as typeof kind)}
          style={inputStyle}
        >
          <option value="all">All event types</option>
          {observedKinds.map(value => (
            <option key={value} value={value}>
              {EVENT_KIND_LABELS[value] ?? value.replaceAll('_', ' ')}
            </option>
          ))}
        </select>
      </div>

      {error && (
        <div style={{
          margin: '0 20px 10px',
          padding: '8px 10px',
          borderRadius: 6,
          border: '1px solid rgba(248,81,73,0.35)',
          background: 'rgba(248,81,73,0.08)',
          color: '#f85149',
          fontSize: 11,
        }}>
          {error}
        </div>
      )}

      <div style={{
        minHeight: 0,
        flex: 1,
        display: 'grid',
        gridTemplateColumns: 'minmax(340px, 0.8fr) minmax(480px, 1.2fr)',
        gap: 12,
        padding: '0 20px 16px',
      }}>
        <div style={panelStyle}>
          <div style={panelHeaderStyle}>
            <strong>
              {displayedEvents.length}
              {clusterSelection
                ? ` ${displayedEvents.length === 1 ? 'event' : 'events'} under this marker`
                : ` ${displayedEvents.length === 1 ? 'event' : 'events'}`}
            </strong>
            {clusterSelection ? (
              <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ color: 'var(--text-muted)' }}>
                  {clusterSelection.eventIds.size === 1
                    ? formatEventDateTime(new Date(clusterSelection.startMs).toISOString())
                    : formatEventClusterRange(clusterSelection.startMs, clusterSelection.endMs)}
                </span>
                <button
                  onClick={() => setClusterSelection(null)}
                  style={secondaryButtonStyle}
                >
                  Clear event filter
                </button>
              </span>
            ) : (
              <span style={{ color: 'var(--text-muted)' }}>newest first</span>
            )}
          </div>
          <div style={{ minHeight: 0, flex: 1, overflow: 'auto', padding: 6 }}>
            {!loading && displayedEvents.length === 0 && (
              <div style={emptyStyle}>No events match this range and filter.</div>
            )}
            {displayedEvents.map(event => {
              const selected = event.id === selectedEvent?.id;
              return (
                <button
                  key={event.id}
                  onClick={() => onSelectEvent(event)}
                  style={{
                    width: '100%',
                    display: 'grid',
                    gridTemplateColumns: '8px minmax(0, 1fr) auto',
                    gap: 8,
                    alignItems: 'start',
                    padding: '8px 9px',
                    border: selected
                      ? '1px solid rgba(88,166,255,0.4)'
                      : '1px solid transparent',
                    borderRadius: 6,
                    background: selected ? 'rgba(88,166,255,0.08)' : 'transparent',
                    color: 'var(--text-primary)',
                    textAlign: 'left',
                    cursor: 'pointer',
                  }}
                >
                  <span style={{
                    width: 7,
                    height: 7,
                    marginTop: 4,
                    borderRadius: '50%',
                    background: EVENT_SEVERITY_COLORS[event.severity],
                  }} />
                  <span style={{ minWidth: 0 }}>
                    <strong style={{
                      display: 'block',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      fontSize: 11,
                    }}>
                      {event.title}
                    </strong>
                    <span style={{
                      display: 'block',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      color: 'var(--text-muted)',
                      fontSize: 9,
                      marginTop: 2,
                    }}>
                      {eventKindLabel(event)} · {event.hostname ?? 'cluster'}
                      {event.query_id ? ` · ${event.query_id}` : ''}
                    </span>
                  </span>
                  <span style={{ color: 'var(--text-muted)', fontSize: 9, whiteSpace: 'nowrap' }}>
                    {formatEventDateTime(event.occurred_at)}
                  </span>
                </button>
              );
            })}
          </div>
        </div>

        <div style={panelStyle}>
          <div style={panelHeaderStyle}>
            <span style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <strong>Selected event</strong>
              {selectedEvent && (
                <span style={{ display: 'inline-flex', gap: 2 }}>
                  {(['details', 'context'] as const).map(panel => (
                    <button
                      key={panel}
                      onClick={() => setSelectedPanel(panel)}
                      style={{
                        ...panelTabStyle,
                        color: selectedPanel === panel ? '#58a6ff' : 'var(--text-muted)',
                        borderColor: selectedPanel === panel
                          ? 'rgba(88,166,255,0.4)'
                          : 'transparent',
                        background: selectedPanel === panel
                          ? 'rgba(88,166,255,0.08)'
                          : 'transparent',
                      }}
                    >
                      {panel === 'details' ? 'Details' : 'Context'}
                    </button>
                  ))}
                </span>
              )}
            </span>
            <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {selectedEvent && onInvestigateEvent && (
                <button
                  onClick={() => onInvestigateEvent(selectedEvent)}
                  style={eventNavigationButtonStyle}
                >
                  Open in Time Travel ↗
                </button>
              )}
            </span>
          </div>
          {!selectedEvent ? (
            <div style={emptyStyle}>Select an event to inspect its context and source details.</div>
          ) : selectedPanel === 'context' ? (
            <EventContextView
              event={selectedEvent}
              events={events}
              availableCapabilities={availableCapabilities ?? []}
            />
          ) : (
            <div style={{ overflow: 'auto', padding: 14 }}>
              <div style={{
                display: 'flex',
                alignItems: 'center',
                flexWrap: 'wrap',
                gap: 9,
                marginBottom: 4,
              }}>
                <h3 style={{ margin: 0, color: 'var(--text-primary)', fontSize: 16 }}>
                  {selectedEvent.title}
                </h3>
                <EventSeverity severity={selectedEvent.severity} />
              </div>
              <div style={{ color: 'var(--text-muted)', fontSize: 10, marginBottom: 14 }}>
                {selectedEvent.precision === 'inferred' ? 'Estimated at ' : ''}
                {formatEventDateTime(selectedEvent.occurred_at)}
                {selectedEvent.observed_at
                  && selectedEvent.observed_at !== selectedEvent.occurred_at
                  ? ` · observed ${formatEventDateTime(selectedEvent.observed_at)}`
                  : ''}
              </div>

              <div style={{
                display: 'flex',
                flexDirection: 'column',
                gap: 18,
                marginBottom: 14,
              }}>
                {eventDetailSections(selectedEvent).map(section => (
                  <EventDetailSectionView
                    key={section.id}
                    section={section}
                    action={section.id === 'identifiers'
                      && selectedEvent.query_id
                      && onOpenQueryDetails
                      ? (
                        <button
                          onClick={() => onOpenQueryDetails(selectedEvent)}
                          style={eventNavigationButtonStyle}
                        >
                          View Query Details ↗
                        </button>
                      )
                      : undefined}
                  />
                ))}
              </div>

              {selectedEvent.detail && (
                <DetailBlock
                  label={eventDetailLabel(selectedEvent)}
                  value={selectedEvent.detail}
                />
              )}
              {selectedEvent.query && (
                <DetailBlock label="Query" value={selectedEvent.query} code />
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const SOURCE_STATUS_COLORS: Record<TimelineEventSourceCoverage['status'], string> = {
  loaded: '#3fb950',
  failed: '#f85149',
  unavailable: '#8b949e',
  not_requested: '#8b949e',
};

const EventCoverageHelp: React.FC<{
  coverage: readonly TimelineEventSourceCoverage[];
  capabilities: readonly MonitoringCapability[];
  view: 'events' | 'sources';
  onViewChange: (view: 'events' | 'sources') => void;
}> = ({ coverage, capabilities, view, onViewChange }) => (
  <div style={{
    position: 'absolute',
    top: 'calc(100% + 7px)',
    right: 0,
    zIndex: 100,
    width: 560,
    maxHeight: 'min(650px, calc(100vh - 120px))',
    overflow: 'auto',
    padding: 12,
    borderRadius: 8,
    border: '1px solid var(--border-primary)',
    background: 'var(--bg-secondary)',
    boxShadow: '0 10px 30px rgba(0,0,0,0.3)',
  }}>
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 8,
      marginBottom: 5,
    }}>
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3 }}>
        <button
          onClick={() => onViewChange('events')}
          style={{
            ...helpTabStyle,
            color: view === 'events' ? '#58a6ff' : 'var(--text-muted)',
            borderColor: view === 'events'
              ? 'rgba(88,166,255,0.4)'
              : 'transparent',
            background: view === 'events'
              ? 'rgba(88,166,255,0.08)'
              : 'transparent',
          }}
        >
          Supported events
        </button>
        <button
          onClick={() => onViewChange('sources')}
          style={{
            ...helpTabStyle,
            color: view === 'sources' ? '#58a6ff' : 'var(--text-muted)',
            borderColor: view === 'sources'
              ? 'rgba(88,166,255,0.4)'
              : 'transparent',
            background: view === 'sources'
              ? 'rgba(88,166,255,0.08)'
              : 'transparent',
          }}
        >
          Source status
        </button>
      </span>
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
        <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>
          Full reference
        </span>
        <DocsLink path="/features/events" />
      </span>
    </div>
    {view === 'events'
      ? <SupportedEventsCatalog coverage={coverage} />
      : (
        <EventSourcesCatalog
          coverage={coverage}
          capabilities={capabilities}
        />
      )}
  </div>
);

const SupportedEventsCatalog: React.FC<{
  coverage: readonly TimelineEventSourceCoverage[];
}> = ({ coverage }) => (
  <>
    <div style={{
      margin: '8px 0 11px',
      color: 'var(--text-secondary)',
      fontSize: 9,
      lineHeight: 1.5,
    }}>
      TraceHouse shows only the event types listed here. Loaded system tables
      are filtered by these detectors; other records in those tables are not
      emitted as Events.
    </div>
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
      gap: '12px 18px',
    }}>
      {supportedEventGroups().map(group => (
        <section key={group.category} style={{ minWidth: 0 }}>
          <div style={{
            marginBottom: 4,
            paddingBottom: 4,
            borderBottom: '1px solid var(--border-primary)',
            color: 'var(--text-muted)',
            fontSize: 8,
            fontWeight: 700,
            letterSpacing: '0.45px',
            textTransform: 'uppercase',
          }}>
            {group.label} · {group.events.length}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            {group.events.map(eventType => {
              const sourceCoverage = supportedEventCoverage(eventType, coverage);
              const { availability } = sourceCoverage;
              const availabilityColor = SUPPORTED_AVAILABILITY_COLORS[availability];
              return (
                <div
                  key={eventType.kind}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: 'minmax(0, 1fr) auto',
                    gap: 8,
                    alignItems: 'start',
                    padding: '6px 1px',
                    borderBottom: '1px solid var(--border-primary)',
                  }}
                >
                  <span style={{ minWidth: 0 }}>
                    <strong style={{
                      display: 'block',
                      color: 'var(--text-primary)',
                      fontSize: 9.5,
                    }}>
                      {eventType.label}
                    </strong>
                    <span style={{
                      display: 'block',
                      marginTop: 2,
                      color: 'var(--text-muted)',
                      fontSize: 8,
                      lineHeight: 1.35,
                    }}>
                      {eventType.description}
                    </span>
                    <span style={{
                      display: 'block',
                      marginTop: 3,
                      color: 'var(--text-muted)',
                      fontSize: 7.5,
                      fontFamily: "'Share Tech Mono', monospace",
                      overflowWrap: 'anywhere',
                    }}>
                      {eventType.sources.join(' · ')}
                    </span>
                  </span>
                  <span style={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'flex-end',
                    gap: 3,
                    paddingTop: 1,
                    whiteSpace: 'nowrap',
                  }}>
                    <span style={{
                      color: EVENT_SEVERITY_COLORS[eventType.severity],
                      fontSize: 7,
                      fontWeight: 700,
                      textTransform: 'uppercase',
                    }}>
                      {eventType.severity}
                    </span>
                    <span style={{
                      color: availabilityColor,
                      fontSize: 7,
                      fontWeight: 700,
                      textTransform: 'uppercase',
                    }}>
                      {sourceCoverage.label}
                    </span>
                    {sourceCoverage.warning && (
                      <span style={{
                        color: availabilityColor,
                        fontSize: 7,
                        fontWeight: 700,
                        textTransform: 'uppercase',
                      }}>
                        {sourceCoverage.warning}
                      </span>
                    )}
                  </span>
                </div>
              );
            })}
          </div>
        </section>
      ))}
    </div>
  </>
);

const EventSourcesCatalog: React.FC<{
  coverage: readonly TimelineEventSourceCoverage[];
  capabilities: readonly MonitoringCapability[];
}> = ({ coverage, capabilities }) => (
  <>
    <div style={{
      margin: '8px 0 11px',
      color: 'var(--text-secondary)',
      fontSize: 9,
      lineHeight: 1.5,
    }}>
      A loaded source means its detector query completed. It does not mean that
      every row or error in the underlying system table is shown as an event.
    </div>
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      {coverage.map(item => {
        const explanation = eventSourceExplanation(item.capability);
        const statusDetail = eventSourceStatusDetail(item, capabilities);
        const statusLabel = item.truncated ? 'truncated' : item.status.replace('_', ' ');
        const statusColor = item.truncated
          ? '#d29922'
          : SOURCE_STATUS_COLORS[item.status];
        return (
          <div
            key={`${item.capability}:${item.source}`}
            style={{
              display: 'grid',
              gridTemplateColumns: '8px minmax(0, 1fr) auto',
              gap: 7,
              alignItems: 'start',
              padding: '7px 8px',
              borderRadius: 6,
              background: 'var(--bg-tertiary)',
              border: '1px solid var(--border-primary)',
            }}
          >
            <span style={{
              width: 7,
              height: 7,
              marginTop: 3,
              borderRadius: '50%',
              background: statusColor,
            }} />
            <span style={{ minWidth: 0 }}>
              <strong style={{
                display: 'block',
                color: 'var(--text-secondary)',
                fontSize: 10,
                fontFamily: "'Share Tech Mono', monospace",
                marginBottom: 2,
              }}>
                {explanation?.label ?? item.source}
              </strong>
              <span style={{
                display: 'block',
                color: 'var(--text-muted)',
                fontSize: 8.5,
                lineHeight: 1.4,
              }}>
                {explanation?.description ?? item.source}
              </span>
              {statusDetail && (
                <span style={{
                  display: 'block',
                  marginTop: 3,
                  color: statusColor,
                  fontSize: 8,
                  overflowWrap: 'anywhere',
                }}>
                  {statusDetail}
                </span>
              )}
            </span>
            <span style={{
              color: statusColor,
              fontSize: 8,
              fontWeight: 700,
              textTransform: 'uppercase',
              whiteSpace: 'nowrap',
            }}>
              {statusLabel}
              {item.status === 'loaded' ? ` · ${item.event_count}` : ''}
            </span>
          </div>
        );
      })}
    </div>
    <div style={{
      marginTop: 9,
      paddingTop: 8,
      borderTop: '1px solid var(--border-primary)',
      color: 'var(--text-muted)',
      fontSize: 8.5,
      lineHeight: 1.45,
    }}>
      “Unavailable” means the capability probe could not use the required
      table or columns. The source entry above shows the reported reason.
      Sources are queried independently.
    </div>
  </>
);

const SUPPORTED_AVAILABILITY_COLORS: Record<SupportedEventAvailability, string> = {
  available: '#3fb950',
  partial: '#d29922',
  unavailable: '#8b949e',
  failed: '#f85149',
};

const helpTabStyle: React.CSSProperties = {
  padding: '4px 8px',
  borderRadius: 5,
  border: '1px solid transparent',
  background: 'transparent',
  color: 'var(--text-muted)',
  fontSize: 9,
  fontWeight: 700,
  cursor: 'pointer',
};

const secondaryButtonStyle: React.CSSProperties = {
  padding: '4px 9px',
  borderRadius: 5,
  border: '1px solid var(--border-primary)',
  background: 'var(--bg-card)',
  color: 'var(--text-muted)',
  fontSize: 10,
  cursor: 'pointer',
};

const eventNavigationButtonStyle: React.CSSProperties = {
  ...secondaryButtonStyle,
  padding: '4px 10px',
  fontSize: 9,
  fontWeight: 600,
  lineHeight: 1.3,
  color: '#58a6ff',
  background: 'rgba(88,166,255,0.1)',
  border: '1px solid rgba(88,166,255,0.45)',
  textTransform: 'none',
  letterSpacing: 'normal',
};

const panelTabStyle: React.CSSProperties = {
  padding: '3px 7px',
  borderRadius: 4,
  border: '1px solid transparent',
  color: 'var(--text-muted)',
  fontSize: 9,
  cursor: 'pointer',
};

const inputStyle: React.CSSProperties = {
  padding: '6px 9px',
  borderRadius: 5,
  border: '1px solid var(--border-primary)',
  background: 'var(--bg-card)',
  color: 'var(--text-primary)',
  fontSize: 10,
  outline: 'none',
};

const panelStyle: React.CSSProperties = {
  minHeight: 0,
  display: 'flex',
  flexDirection: 'column',
  overflow: 'hidden',
  borderRadius: 8,
  border: '1px solid var(--border-primary)',
  background: 'var(--bg-secondary)',
};

const panelHeaderStyle: React.CSSProperties = {
  flexShrink: 0,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: '8px 11px',
  borderBottom: '1px solid var(--border-primary)',
  color: 'var(--text-secondary)',
  fontSize: 10,
};

const emptyStyle: React.CSSProperties = {
  flex: 1,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  minHeight: 120,
  color: 'var(--text-muted)',
  fontSize: 11,
};

const EventSeverity: React.FC<{
  severity: TimelineEventSeverity;
}> = ({ severity }) => {
  const color = EVENT_SEVERITY_COLORS[severity];
  return (
    <span
      aria-label={`Event severity: ${severity}`}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 5,
        color,
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: '0.45px',
        textTransform: 'uppercase',
      }}
    >
      <span style={{
        width: 5,
        height: 5,
        borderRadius: '50%',
        background: color,
      }} />
      {severity}
    </span>
  );
};

const EventDetailSectionView: React.FC<{
  section: EventDetailSection;
  action?: React.ReactNode;
}> = ({ section, action }) => (
  <section style={{
    minWidth: 0,
  }}>
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'flex-start',
      gap: 10,
      paddingBottom: 5,
      marginBottom: 3,
      borderBottom: '1px solid var(--border-primary)',
      color: 'var(--text-muted)',
      fontSize: 9,
      fontWeight: 700,
      textTransform: 'uppercase',
      letterSpacing: '0.55px',
    }}>
      <span>{section.label}</span>
      {action}
    </div>
    <dl style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
      columnGap: 24,
      rowGap: 10,
      margin: 0,
      paddingTop: 4,
    }}>
      {section.rows.map(row => (
        <div
          key={row.label}
          style={{
            minWidth: 0,
          }}
        >
          <dt style={{
            margin: 0,
            color: 'var(--text-muted)',
            fontSize: 9,
            lineHeight: 1.3,
          }}>
            {row.label}
          </dt>
          <dd style={{
            minWidth: 0,
            margin: '3px 0 0',
            color: 'var(--text-secondary)',
            fontSize: 11,
            lineHeight: 1.4,
            fontFamily: row.monospace ? "'Share Tech Mono', monospace" : 'inherit',
            overflowWrap: 'anywhere',
          }}>
            {row.value}
          </dd>
        </div>
      ))}
    </dl>
  </section>
);

const DetailBlock: React.FC<{ label: string; value: string; code?: boolean }> = ({
  label,
  value,
  code,
}) => (
  <div style={{ marginTop: 10 }}>
    <div style={{
      color: 'var(--text-muted)',
      fontSize: 8,
      fontWeight: 700,
      textTransform: 'uppercase',
      letterSpacing: '0.5px',
      marginBottom: 4,
    }}>
      {label}
    </div>
    <pre style={{
      margin: 0,
      padding: '9px 10px',
      borderRadius: 6,
      border: '1px solid var(--border-primary)',
      background: 'var(--bg-tertiary)',
      color: 'var(--text-secondary)',
      fontFamily: code ? "'Share Tech Mono', monospace" : 'inherit',
      fontSize: 10,
      lineHeight: 1.45,
      whiteSpace: 'pre-wrap',
      overflowWrap: 'anywhere',
    }}>
      {value}
    </pre>
  </div>
);
