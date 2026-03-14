/**
 * Preset query loading — maps RAW_QUERIES to typed Query objects.
 */

import { parseQueryMetadata } from './metaLanguage';
import { RAW_QUERIES } from './queries';
import { type Query } from './types';

export { RAW_QUERIES };

export const PRESET_QUERIES: Query[] = RAW_QUERIES
  .map(sql => parseQueryMetadata(sql, 'preset'))
  .filter((q): q is Query => q !== null);
