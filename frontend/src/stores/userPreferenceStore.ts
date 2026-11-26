import type { TimelineFilters } from '@tracehouse/core';
import { tracehouseLocalStorage } from '@tracehouse/core';
/**
 * User Preference Store - Persisted UI preferences
 *
 * Stores user preferences (view mode, feature toggles, etc.) across sessions.
 */

import { create } from 'zustand';
import { persist, createJSONStorage } from 'zustand/middleware';
import type { QueryXRaySourcePreference } from '@tracehouse/core';
import { buildConfig } from '../buildConfig';

export type ViewMode = '3d' | '2d';

export type NavigatorShapeMode = 'trend' | 'peaks' | 'change';

/** Object browser diagram style, under trial: quiet cards, Sankey bars, grouped columns, metro lines. */
export type ObjectDiagramStyle = 'cards' | 'bars' | 'columns' | 'metro';
/** Object browser line options: route lines between cards, dim lines outside the hovered card's lineage,
 * and essential lines only (broad privileges end at the data band). */
export interface ObjectDiagramLines { route: boolean; highlight: boolean; essential: boolean }
export const OBJECT_DIAGRAM_LINE_DEFAULTS: ObjectDiagramLines = { route: false, highlight: true, essential: false };

interface UserPreferenceState {
  preferredViewMode: ViewMode;
  setPreferredViewMode: (mode: ViewMode) => void;
  toggleViewMode: () => void;
  /** Allow killing queries from the Active Queries view. Off by default. */
  killQueriesEnabled: boolean;
  setKillQueriesEnabled: (enabled: boolean) => void;
  /** Enable experimental features. Off by default. */
  experimentalEnabled: boolean;
  setExperimentalEnabled: (enabled: boolean) => void;
  /** Hide replica merges (same merge on multiple replicas). Off by default — replica merges are shown. */
  hideReplicaMerges: boolean;
  setHideReplicaMerges: (enabled: boolean) => void;
  /** Show event annotations on the Time Travel 2D chart and navigator. */
  timeTravelEventsVisible: boolean;
  setTimeTravelEventsVisible: (visible: boolean) => void;
  /** Show operational events on Analytics dashboards. Off by default; the last choice is kept. */
  analyticsEventsVisible: boolean;
  setAnalyticsEventsVisible: (visible: boolean) => void;
  /** Time Travel navigator rollup shape. */
  timeTravelNavigatorShape: NavigatorShapeMode;
  setTimeTravelNavigatorShape: (shape: NavigatorShapeMode) => void;
  /** Show a live preview of a dashboard when hovering its card in the Analytics list. Defaults to the TH_DASHBOARD_PREVIEW build flag. */
  dashboardPreviewEnabled: boolean;
  setDashboardPreviewEnabled: (enabled: boolean) => void;
  /**
   * Which table the Query X-Ray reads. 'auto' lets selectQueryXRaySource() decide
   * from what the connection has and whether the query is still running.
   */
  queryXraySource: Record<string, QueryXRaySourcePreference>;
  setQueryXraySource: (connectionId: string, source: QueryXRaySourcePreference | undefined) => void;
  /** Personal Time Travel filters; unset fields follow the admin default. */
  timeTravelFilters: Partial<TimelineFilters>;
  setTimeTravelFilters: (filters: Partial<TimelineFilters>) => void;
  objectDiagramStyle: ObjectDiagramStyle;
  setObjectDiagramStyle: (style: ObjectDiagramStyle) => void;
  objectDatabaseColumn: boolean;
  toggleObjectDatabaseColumn: () => void;
  objectDiagramLines: ObjectDiagramLines;
  toggleObjectDiagramLine: (name: keyof ObjectDiagramLines) => void;
}

export const useUserPreferenceStore = create<UserPreferenceState>()(
  persist(
    (set, get) => ({
      preferredViewMode: '3d',
      setPreferredViewMode: (mode) => set({ preferredViewMode: mode }),
      toggleViewMode: () => set({ preferredViewMode: get().preferredViewMode === '3d' ? '2d' : '3d' }),
      killQueriesEnabled: false,
      setKillQueriesEnabled: (enabled) => set({ killQueriesEnabled: enabled }),
      experimentalEnabled: true,
      setExperimentalEnabled: (enabled) => set({ experimentalEnabled: enabled }),
      hideReplicaMerges: false,
      setHideReplicaMerges: (enabled: boolean) => set({ hideReplicaMerges: enabled }),
      timeTravelEventsVisible: true,
      setTimeTravelEventsVisible: (visible: boolean) => set({ timeTravelEventsVisible: visible }),
      analyticsEventsVisible: false,
      setAnalyticsEventsVisible: (visible: boolean) => set({ analyticsEventsVisible: visible }),
      timeTravelNavigatorShape: 'peaks',
      setTimeTravelNavigatorShape: (shape: NavigatorShapeMode) => set({ timeTravelNavigatorShape: shape }),
      dashboardPreviewEnabled: buildConfig.dashboardPreview,
      setDashboardPreviewEnabled: (enabled: boolean) => set({ dashboardPreviewEnabled: enabled }),
      queryXraySource: {},
      setQueryXraySource: (connectionId, source) => set(state => {
        const queryXraySource = { ...state.queryXraySource };
        if (source === undefined) delete queryXraySource[connectionId];
        else queryXraySource[connectionId] = source;
        return { queryXraySource };
      }),
      timeTravelFilters: {},
      setTimeTravelFilters: (filters) => set({ timeTravelFilters: filters }),
      objectDiagramStyle: 'cards',
      setObjectDiagramStyle: (style) => set({ objectDiagramStyle: style }),
      objectDatabaseColumn: false,
      toggleObjectDatabaseColumn: () => set(state => ({ objectDatabaseColumn: !state.objectDatabaseColumn })),
      objectDiagramLines: OBJECT_DIAGRAM_LINE_DEFAULTS,
      toggleObjectDiagramLine: (name) => set(state => ({ objectDiagramLines: { ...state.objectDiagramLines, [name]: !state.objectDiagramLines[name] } })),
    }),
    {
      name: 'tracehouse-view-preference',
      storage: createJSONStorage(() => tracehouseLocalStorage),
      version: 1,
      // The old preference was one browser-wide string with no connection
      // identity, so there is nothing faithful to convert it into. Drop it and
      // start everyone on automatic, which now prefers the sampler.
      migrate: (persisted) => {
        const { xraySource: _legacy, ...state } = persisted as Record<string, unknown>;
        return { ...state, queryXraySource: {} };
      },
      // Line options saved before an option existed take that option's default.
      merge: (persisted, current) => {
        const saved = (persisted ?? {}) as Partial<UserPreferenceState>;
        return { ...current, ...saved, objectDiagramLines: { ...OBJECT_DIAGRAM_LINE_DEFAULTS, ...saved.objectDiagramLines } };
      },
    }
  )
);
