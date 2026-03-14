import type { ParsedDirectives, QueryGroup } from './metaLanguage';

export type QueryType = 'preset' | 'custom';

export interface Query {
  name: string;
  description: string;
  sql: string;
  group: QueryGroup;
  type: QueryType;
  directives: ParsedDirectives;
}
