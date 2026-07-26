import React, { useCallback, useEffect, useRef, useState } from 'react';
import type { QuerySeries } from '@tracehouse/core';
import { EventsDashboard } from '../components/analytics/EventsDashboard';
import { DocsLink } from '../components/common/DocsLink';
import { QueryDetailModal } from '../components/query/modal/QueryDetailModal';
import { useNavigate } from '../hooks/useAppLocation';
import { useQueryDeepLink } from '../hooks/useQueryDeepLink';
import { useEventsUrlState } from '../hooks/useUrlState';
import { buildTimeTravelEventUrl } from '../components/timeline/timeline-event-model';
import {
  EVENT_HOURS_INTERVAL,
  EVENT_INTERVAL_HOURS,
  eventToQuerySeries,
  legacyEventsRangeCenter,
  toLocalEventDateTime,
} from './events-page-model';

export const Events: React.FC = () => {
  const navigate = useNavigate();
  const { state, update } = useEventsUrlState();
  const [selectedQuery, setSelectedQuery] = useState<QuerySeries | null>(null);
  const { query: modalQuery, onClose: closeQueryDetails } = useQueryDeepLink(
    selectedQuery,
    () => setSelectedQuery(null),
  );
  const fromTimeTravel = state.from === 'timetravel';
  const [legacyRangeCenter] = useState(() => legacyEventsRangeCenter(
    state.from,
    state.range_center,
    state.event_time,
  ));
  const migratedLegacyRangeRef = useRef(false);
  const effectiveRangeCenter = state.range_center
    ?? (!migratedLegacyRangeRef.current ? legacyRangeCenter : undefined);

  useEffect(() => {
    if (!legacyRangeCenter || migratedLegacyRangeRef.current) return;
    migratedLegacyRangeRef.current = true;
    update({ range_center: legacyRangeCenter });
  }, [legacyRangeCenter, update]);

  const rangeHours = state.event_range ?? 24;
  const rangeCenterMs = effectiveRangeCenter
    ? Date.parse(effectiveRangeCenter)
    : Number.NaN;
  const rangeMs = rangeHours * 3_600_000;
  const timeRangeValue = Number.isFinite(rangeCenterMs)
    ? `CUSTOM:${toLocalEventDateTime(rangeCenterMs - rangeMs / 2)},${toLocalEventDateTime(
      rangeCenterMs + rangeMs / 2,
    )}`
    : EVENT_HOURS_INTERVAL.get(rangeHours) ?? '1 DAY';

  const handleRangeSelect = useCallback((startMs: number, endMs: number) => {
    const center = new Date(startMs + (endMs - startMs) / 2).toISOString();
    const hours = Math.max(1 / 60, (endMs - startMs) / 3_600_000);
    update({
      range_center: center,
      event_range: hours,
      event_id: undefined,
      event_time: undefined,
    }, { push: true });
  }, [update]);

  return (
    <div style={{
      height: '100%',
      minHeight: 0,
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden',
      background: 'var(--bg-primary)',
    }}>
      <div style={{
        flexShrink: 0,
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        padding: '12px 20px',
        borderBottom: '1px solid var(--border-primary)',
        background: 'var(--bg-secondary)',
      }}>
        <h2 style={{
          margin: 0,
          color: 'var(--text-primary)',
          fontSize: 18,
          fontWeight: 600,
        }}>
          Events
        </h2>
        <DocsLink path="/features/events" />
        <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>
          Historical operational changes and failures
        </span>
      </div>

      <div style={{ flex: 1, minHeight: 0 }}>
        <EventsDashboard
          selectedEventId={state.event_id}
          selectedEventTime={state.event_time}
          rangeCenterTime={effectiveRangeCenter}
          rangeHours={rangeHours}
          timeRangeValue={timeRangeValue}
          onTimeRangeChange={value => {
            if (value?.startsWith('CUSTOM:')) {
              const [start, end] = value.slice(7).split(',');
              const startMs = Date.parse(start);
              const endMs = Date.parse(end);
              if (Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs) {
                handleRangeSelect(startMs, endMs);
              }
              return;
            }
            update({
              range_center: undefined,
              event_range: value ? (EVENT_INTERVAL_HOURS[value] ?? 24) : 24,
              event_id: undefined,
              event_time: undefined,
            }, { push: true });
          }}
          onRangeSelect={handleRangeSelect}
          onSelectEvent={event => update({
            event_id: event.id,
            event_time: event.occurred_at,
          })}
          onOpenQueryDetails={event => {
            if (event.query_id) setSelectedQuery(eventToQuerySeries(event));
          }}
          onInvestigateEvent={event => navigate(buildTimeTravelEventUrl(event))}
          onBackToTimeTravel={fromTimeTravel ? () => navigate(-1) : undefined}
        />
      </div>
      <QueryDetailModal query={modalQuery} onClose={closeQueryDetails} />
    </div>
  );
};

export default Events;
