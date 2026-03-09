// @tracehouse/ui-shared barrel export

// 3D visualization components
export {
  // Size calculations
  calculatePartSizes,
  verifyProportionality,
  calculateExpectedProportion,
  defaultSizeConfig,
  type PartSizeData,
  type PartVisualSize,
  type SizeCalculationConfig,
  // Components
  GlassBox,
  type GlassBoxProps,
  MergeLanes,
  type MergeLanesProps,
  FlowLine,
  type FlowLineProps,
  ResultPartGhost,
  type ResultPartGhostProps,
  PartsScene,
  type PartsSceneProps,
} from './3d/index.js';

// Headless hooks
export {
  // ClickHouse services hook (shared between frontend and Grafana)
  useClickHouseServices,
  useRequiredClickHouseServices,
  ClickHouseContext,
  type ClickHouseServices,
  // Part inspector
  usePartInspector,
  type InspectorTab,
  type SortField,
  type SortDir,
  type UsePartInspectorResult,
  useMergeTracking,
  type MergeTrackingOptions,
  type CompletedMerge,
  type MergeStatistics,
  type UseMergeTrackingResult,
  useLineageTree,
  type UseLineageTreeResult,
  usePartsLayout,
  type PartPosition,
  type LevelMetadata,
  type UsePartsLayoutResult,
  type PartsLayoutOptions,
  // Refresh config (admin-configurable polling rates)
  useRefreshConfig,
  filterAllowedOptions,
  getEffectiveDefault,
  clampToAllowed,
  RefreshConfigContext,
  ALL_REFRESH_RATE_OPTIONS,
  DEFAULT_REFRESH_CONFIG,
  type RefreshConfig,
  type RefreshRateOption,
} from './hooks/index.js';

// Style-agnostic 2D components
export {
  DonutChart,
  type DonutChartProps,
  type DonutSegment,
  LineageNodeCard,
  type LineageNodeCardProps,
  MergeTimeline,
  type MergeTimelineProps,
  MergeCard,
  type MergeCardProps,
} from './components/index.js';
