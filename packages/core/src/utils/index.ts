export { extractLiterals, diffLiterals, extractQueryParameters, formatLiteral } from './query-literals.js';
export type { QueryLiteral, LiteralDiff } from './query-literals.js';
export {
  classifyActiveMerge,
  classifyMergeHistory,
  getMergeCategoryInfo,
  refineCategoryWithRowDiff,
  classifyMutationCommand,
  isPatchPart,
  MERGE_CATEGORIES,
  ALL_MERGE_CATEGORIES,
  MUTATION_SUBTYPES,
} from './merge-classification.js';
export type { MergeCategory, MergeCategoryInfo, MutationSubtype, MutationSubtypeInfo } from './merge-classification.js';
export { parseTimeValue } from './time.js';
export { parseTTL, formatTTLDuration } from './ttl-parser.js';
export { parsePartName, getLevelFromName, isMergedPart, isMutatedPart, stripMutationVersion, getPartLevelGroupKey, MUTATION_GROUP_KEY } from './part-name-parser.js';
export { parseVerticalMergeProgress } from './vertical-merge-progress.js';
export type { VerticalMergeProgress, VerticalMergeSegment } from './vertical-merge-progress.js';
