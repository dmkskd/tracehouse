export { Layout } from './Layout';

// Error boundary components
export { ErrorBoundary } from './shared/ErrorBoundary';
export type { ErrorBoundaryProps } from './shared/ErrorBoundary';

// Loading skeleton components
export {
  Skeleton,
  SkeletonText,
  SkeletonCard,
  SkeletonTable,
  Skeleton3D,
  SkeletonMetricCard,
  SkeletonList,
  SkeletonChart,
  SkeletonTree,
  SkeletonPage,
} from './shared/LoadingSkeletons';

// 3D visualization components
export { 
  Scene3D, 
  defaultSceneConfig, 
  createSceneConfig,
  usePerformanceMode,
  PerformanceContext,
  // Error boundary for 3D
  ErrorBoundary3D,
  isWebGLSupported,
  isWebGL2Supported,
  // 2D Fallback components
  PartsFallback2D,
  PipelineFallback2D,
  MergeFallback2D,
} from './3d';
export type { 
  SceneConfig, 
  Scene3DProps, 
  PerformanceContextValue,
  ErrorBoundary3DProps,
  PartsFallback2DProps,
  PipelineFallback2DProps,
  MergeFallback2DProps,
} from './3d';

// Connection management components
export { ConnectionForm } from './connection/ConnectionForm';
export { ConnectionSelector } from './connection/ConnectionSelector';
export { ConnectionStatusIndicator } from './connection/ConnectionStatusIndicator';

// Metrics components
export { MetricCard } from './metrics/MetricCard';
export type { MetricCardProps, MetricType } from './metrics/MetricCard';

// Metric warning components
export { MetricWarning, WarningsSummary, calculateSeverity } from './metrics/MetricWarning';
export type { MetricWarningProps, WarningsSummaryProps, WarningSeverity } from './metrics/MetricWarning';

// Time-series chart components
export { 
  TimeSeriesChart, 
  MetricSelector, 
  ViewModeToggle 
} from './metrics/TimeSeriesChart';
export type { 
  TimeSeriesChartProps, 
  MetricSelectorProps, 
  ViewModeToggleProps 
} from './metrics/TimeSeriesChart';

// Query monitoring components
export { QueryRunningTable } from './query/QueryRunningTable';
export { QueryHistoryTable } from './query/QueryHistoryTable';
export { QueryDetail } from './query/QueryDetail';

// Database explorer components
export { DatabaseTree } from './database/DatabaseTree';
export { TableDetail } from './database/TableDetail';
export { PartsTable } from './database/PartsTable';

// Merge tracking components
export { MergeTrackerView } from './merge/MergeTracker';
export { ActiveMergeList } from './merge/ActiveMergeList';
export { MergeHistoryTable } from './merge/MergeHistoryTable';

// Query tracing components
export { TraceLogViewer } from './tracing/TraceLogViewer';
export { ExplainViewer } from './tracing/ExplainViewer';

// Timeline navigation components
export { TimelineNavigator } from './shared/TimelineNavigator';

// Query detail modal components
export {
  QueryDetailModal,
} from './query/modal/QueryDetailModal';
export type {
  TimelineQueryModalProps,
} from './query/modal/QueryDetailModal';

// Part inspector modal component (with reusable tab components)
export { 
  PartInspector,
  OverviewTab,
  ColumnsTab,
  DataTab,
  LineageTab,
  COLUMN_COLORS,
} from './database/PartInspector';
export type { PartInspectorProps } from './database/PartInspector';

// Monitoring capability check components
export { RequiresCapability, useCapabilityCheck } from './shared/RequiresCapability';
export type { RequiresCapabilityProps } from './shared/RequiresCapability';

