import { createContext, useCallback, useContext, useMemo } from 'react';
import { readTimelineFilters, resolveTimelineFilters, TIMELINE_FILTER_PARAMS, type TimelineFilters } from '@tracehouse/core';
import { useSearchParams } from '../../hooks/useAppLocation';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';

/** Admin default (Grafana app settings); runtime configuration, never copied into user storage. */
export const TimelineFiltersDefaultContext = createContext<Partial<TimelineFilters>>({});

/**
 * Time Travel filters: built-in defaults, then the admin default, the personal setting and
 * the URL (a shared link). Choosing a value sets the personal setting and drops that one
 * value from the URL, so the choice applies.
 */
export function useTimelineFilters() {
  const adminDefault = useContext(TimelineFiltersDefaultContext);
  const personal = useUserPreferenceStore(s => s.timeTravelFilters);
  const setPersonal = useUserPreferenceStore(s => s.setTimeTravelFilters);
  const [searchParams, setSearchParams] = useSearchParams();
  const fromUrl = useMemo(() => readTimelineFilters(name => searchParams.get(name)), [searchParams]);
  // Keyed by value: the page rewrites its URL on every zoom or pin, which must not refetch.
  const filtersKey = JSON.stringify(resolveTimelineFilters(adminDefault, personal, fromUrl));
  const filters = useMemo(() => JSON.parse(filtersKey) as TimelineFilters, [filtersKey]);
  const defaults = useMemo(() => resolveTimelineFilters(adminDefault), [adminDefault]);
  const dropUrl = useCallback((keys: (keyof TimelineFilters)[]) => setSearchParams(prev => {
    const next = new URLSearchParams(prev);
    for (const key of keys) next.delete(TIMELINE_FILTER_PARAMS[key]);
    return next;
  }, { replace: true }), [setSearchParams]);
  // A choice is saved as the personal setting and replaces only the link value it changes:
  // other values a link set stay with that link and are never saved.
  const choose = useCallback((change: Partial<TimelineFilters>) => {
    setPersonal({ ...personal, ...change });dropUrl(Object.keys(change) as (keyof TimelineFilters)[]);
  }, [personal, setPersonal, dropUrl]);
  const reset = useCallback(() => {
    setPersonal({});dropUrl(Object.keys(TIMELINE_FILTER_PARAMS) as (keyof TimelineFilters)[]);
  }, [setPersonal, dropUrl]);
  const changed = (Object.keys(filters) as (keyof TimelineFilters)[]).some(key => filters[key] !== defaults[key]);
  return { filters, defaults, changed, fromLink: Object.keys(fromUrl).length > 0, choose, reset };
}
