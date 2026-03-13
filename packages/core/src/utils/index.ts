export { extractLiterals, diffLiterals, extractQueryParameters, formatLiteral } from './query-literals.js';
export type { QueryLiteral, LiteralDiff } from './query-literals.js';
export {
  classifyActiveMerge,
  classifyMergeHistory,
  getMergeCategoryInfo,
  refineCategoryWithRowDiff,
  MERGE_CATEGORIES,
  ALL_MERGE_CATEGORIES,
} from './merge-classification.js';
export type { MergeCategory, MergeCategoryInfo } from './merge-classification.js';
export { parseTimeValue } from './time.js';
export { parseTTL, formatTTLDuration } from './ttl-parser.js';
