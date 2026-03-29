export { extractLiterals, diffLiterals, extractQueryParameters, formatLiteral } from './query-literals.js';
export type { QueryLiteral, LiteralDiff } from './query-literals.js';
export {
  classifyActiveMerge,
  classifyMergeHistory,
  getMergeCategoryInfo,
  refineCategoryWithRowDiff,
  classifyMutationCommand,
  isPatchPart,
  isDeduplicatingEngine,
  markReplicaMerges,
  markReplicaMergeHistory,
  MERGE_CATEGORIES,
  ALL_MERGE_CATEGORIES,
  MUTATION_SUBTYPES,
  isCategoryClientSideOnly,
} from './merge-classification.js';
export type { MergeCategory, MergeCategoryInfo, MutationSubtype, MutationSubtypeInfo } from './merge-classification.js';
export { parseTimeValue } from './time.js';
export { parseTTL, formatTTLDuration } from './ttl-parser.js';
export { parsePartName, getLevelFromName, isMergedPart, isMutatedPart, stripMutationVersion, getPartLevelGroupKey, MUTATION_GROUP_KEY } from './part-name-parser.js';
export { parseVerticalMergeProgress } from './vertical-merge-progress.js';
export type { VerticalMergeProgress, VerticalMergeSegment } from './vertical-merge-progress.js';
export { computeMergeEta, pickThroughputEstimate } from './merge-eta.js';
export type { MergeEtaInfo } from './merge-eta.js';
export { deriveHealth, mergeThroughputHealth, worstHealth, isMergeStuck } from './merge-health.js';
export type { Health, HealthNode, ThroughputMap } from './merge-health.js';
export { deriveQueryHealth, isQueryStuck } from './query-health.js';
export { detectTimestamp, timestampToDate, formatCell } from './format-cell.js';
export type { TimestampUnit } from './format-cell.js';
export { pearson, spearman, crossCorrelation, crossCorrelationDetail, rollingCorrelation, minMaxNormalize, normalizePanelData, correlateToFocused, correlationToOpacity, correlationStrength, computeInsightsAndLags, interpretCorrelation, CORRELATION_ALGORITHMS, CORRELATION_THRESHOLDS, ROLLING_WINDOW_TRIGGER, ROLLING_WINDOW_THRESHOLD } from './correlation.js';
export type { CorrelationFn, CorrelationAlgorithm, CorrelationInsight, CorrelationStrength, CrossCorrelationResult, NormalizedSeries, CorrelationResult, CorrelatedWindow } from './correlation.js';
export { processLanesData, aggregateLanes, rankLanes, computeLaneBreakdowns, formatLaneLabels, formatTimeLabels, buildMergeTotalsMap, buildCombinedSystemTotals, buildChannelGrid, STRESS_COMPONENTS, MERGE_CHANNEL_MAP } from './resource-lanes-processor.js';
export type { ResourceChannel, ViewMode, StressScale, LaneResourceBreakdown, ProcessedLanes } from './resource-lanes-processor.js';
