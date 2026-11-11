import React, { useCallback, useEffect, useRef, useState } from 'react';
import type { QuerySeries } from '@tracehouse/core';
import { EventsDashboard } from '../components/events/EventsDashboard';
import { DocsLink } from '../components/common/DocsLink';
import { QueryDetailModal } from '../components/query/modal/QueryDetailModal';
import { useNavigate } from '../hooks/useAppLocation';
import { useQueryDeepLink } from '../hooks/useQueryDeepLink';
import { DeepLinkStatusBanner } from '../components/common/DeepLinkStatusBanner';
import { useUrlState } from '../hooks/useUrlState';
import { defineShareSchema } from '../share/shareSchema';
import { buildTimeTravelEventUrl } from '../components/events/event-model';
import { useReturnToOrigin } from '../hooks/useReturnToOrigin';
import {
  buildMergeDetailsUrl,
  EVENT_HOURS_INTERVAL,
  EVENT_INTERVAL_HOURS,
  eventToQuerySeries,
  legacyEventsRangeCenter,
  linkTimeToEventsRange,
} from './events-page-model';
import { LINK_TIME_PARAMS, hasLinkTimeParams, readLinkTime } from '../utils/linkCoordinates';

/** State listed here must survive copying and reopening an Events URL. */
export const EVENTS_SHARE_SCHEMA = defineShareSchema({
  event_id: { type: 'string' },
  event_time: { type: 'string' },
  range_center: { type: 'string' },
  event_range: { type: 'number', default: 1 },
  from: { type: 'string' },
  event_search: { type: 'string' },
  event_severity: { type: 'string', default: 'all' },
  event_category: { type: 'string', default: 'all' },
  event_kind: { type: 'string', default: 'all' },
  event_auto: { type: 'boolean', default: false },
  event_group: { type: 'boolean', default: true },
  event_panel: { type: 'string', default: 'details' },
});

export const Events: React.FC = () => {
  const navigate = useNavigate();
  const { state, update, searchParams } = useUrlState(EVENTS_SHARE_SCHEMA);
  const [selectedQuery, setSelectedQuery] = useState<QuerySeries | null>(null);
  const { query: modalQuery, onClose: closeQueryDetails, linkBanner, dismissLinkStatus } = useQueryDeepLink(
    selectedQuery,
    () => setSelectedQuery(null),
  );
  const fromTimeTravel = state.from === 'timetravel';
  // Analytics opens events with a navigation-state origin; the URL updates below would drop it.
  const { from: linkOrigin } = useReturnToOrigin();
  const [legacyRangeCenter] = useState(() => legacyEventsRangeCenter(
    state.from,
    state.range_center,
    state.event_time,
  ));
  const migratedLegacyRangeRef = useRef(false);

  // Link time inputs (th_at, th_from, th_to) win over the page's range. They are used
  // from the first render and then replaced in the URL by range_center / event_range.
  const [hasLinkTime] = useState(() => hasLinkTimeParams(searchParams));
  const [linkRange] = useState(() => {
    const time = readLinkTime(searchParams);
    return time ? linkTimeToEventsRange(time) : undefined;
  });
  const consumedLinkTimeRef = useRef(false);
  const pendingLinkRange = consumedLinkTimeRef.current ? undefined : linkRange;

  const effectiveRangeCenter = pendingLinkRange?.range_center
    ?? state.range_center
    ?? (!migratedLegacyRangeRef.current ? legacyRangeCenter : undefined);

  useEffect(() => {
    if (!hasLinkTime || consumedLinkTimeRef.current) return;
    consumedLinkTimeRef.current = true;
    update(linkRange ?? {}, { remove: LINK_TIME_PARAMS });
  }, [hasLinkTime, linkRange, update]);

  useEffect(() => {
    if (!legacyRangeCenter || migratedLegacyRangeRef.current) return;
    migratedLegacyRangeRef.current = true;
    update({ range_center: legacyRangeCenter });
  }, [legacyRangeCenter, update]);

  const rangeHours = pendingLinkRange?.event_range ?? state.event_range ?? 1;
  const rangeCenterMs = effectiveRangeCenter
    ? Date.parse(effectiveRangeCenter)
    : Number.NaN;
  const rangeMs = rangeHours * 3_600_000;
  const timeRangeValue = Number.isFinite(rangeCenterMs)
    ? `CUSTOM:${new Date(rangeCenterMs - rangeMs / 2).toISOString()},${new Date(
      rangeCenterMs + rangeMs / 2,
    ).toISOString()}`
    : EVENT_HOURS_INTERVAL.get(rangeHours) ?? '1 HOUR';

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
          search={state.event_search ?? ''}
          onSearchChange={event_search => update({ event_search: event_search || undefined })}
          severity={(state.event_severity ?? 'all') as React.ComponentProps<typeof EventsDashboard>['severity']}
          onSeverityChange={event_severity => update({ event_severity })}
          category={(state.event_category ?? 'all') as React.ComponentProps<typeof EventsDashboard>['category']}
          onCategoryChange={event_category => update({ event_category })}
          kind={(state.event_kind ?? 'all') as React.ComponentProps<typeof EventsDashboard>['kind']}
          onKindChange={event_kind => update({ event_kind })}
          autoRefresh={state.event_auto ?? false}
          onAutoRefreshChange={event_auto => update({ event_auto })}
          groupSimilarEvents={state.event_group ?? true}
          onGroupSimilarEventsChange={event_group => update({ event_group })}
          selectedPanel={(state.event_panel ?? 'details') as 'details' | 'context'}
          onSelectedPanelChange={event_panel => update({ event_panel })}
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
              event_range: value ? (EVENT_INTERVAL_HOURS[value] ?? 1) : 1,
              event_id: undefined,
              event_time: undefined,
            }, { push: true });
          }}
          onRangeSelect={handleRangeSelect}
          onSelectEvent={event => update({
            event_id: event.id,
            event_time: event.occurred_at,
          })}
          onClearSelection={() => update({ event_id: undefined, event_time: undefined })}
          onOpenQueryDetails={event => {
            if (event.query_id) setSelectedQuery(eventToQuerySeries(event));
          }}
          onOpenMergeDetails={event => {
            const url = buildMergeDetailsUrl(event);
            if (url) navigate(url);
          }}
          onOpenAnalyticsDashboard={() => navigate(
            '/analytics?tab=dashboards&fromDashboard=operational-events-explorer',
          )}
          onInvestigateEvent={event => navigate(buildTimeTravelEventUrl(event))}
          onBackToTimeTravel={fromTimeTravel ? () => navigate(-1) : undefined}
          linkOrigin={linkOrigin}
        />
      </div>
      <QueryDetailModal query={modalQuery} onClose={closeQueryDetails} />
      <DeepLinkStatusBanner banner={linkBanner} onDismiss={dismissLinkStatus} />
    </div>
  );
};

export default Events;
