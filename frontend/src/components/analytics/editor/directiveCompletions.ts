/**
 * CodeMirror autocomplete source for TraceHouse directive comments.
 *
 * Provides completions for:
 *   1. Directive names:      -- @  →  @meta, @chart, @rag, @drill, @link
 *   2. Directive parameters:  -- @chart: type=  →  bar, line, pie …
 *   3. Column-based params:   -- @chart: labels=  →  columns from last query result
 *   4. Template variables:    {{  →  time_range, drill:col, cluster_aware:…
 */

import type { CompletionContext, CompletionResult, Completion } from '@codemirror/autocomplete';
import { QUERY_GROUPS } from '../metaLanguage';

/* ─── directive definitions ─── */

type ParamValues = string[] | 'column' | 'query' | null; // 'column' = result columns, 'query' = query names, null = free-text

interface DirectiveDef {
  label: string;
  detail: string;
  template: string;
  params: Record<string, ParamValues>;
  /** Human-readable description for each parameter (shown as info tooltip). */
  paramInfo: Record<string, string>;
}

const DIRECTIVES: DirectiveDef[] = [
  {
    label: '@meta',
    detail: 'Query metadata (title, group, description)',
    template: "-- @meta: title='$1' group='$2'",
    params: {
      title: null,
      group: Object.keys(QUERY_GROUPS),
      description: null,
      interval: ['15 MINUTE', '1 HOUR', '6 HOUR', '1 DAY', '2 DAY', '7 DAY', '30 DAY'],
    },
    paramInfo: {
      title: 'Display name shown in the sidebar and dashboard tiles',
      group: 'Category for grouping queries in the sidebar (e.g. Overview, Performance)',
      description: 'Longer explanation shown as a tooltip on the query card',
      interval: 'Default time window for {{time_range}} — user can override via the picker',
    },
  },
  {
    label: '@chart',
    detail: 'Chart visualization config',
    template: '-- @chart: type=$1 labels=$2 values=$3',
    params: {
      type: ['bar', 'line', 'pie', 'area', 'grouped_bar', 'stacked_bar', 'grouped_line'],
      labels: 'column',
      values: 'column',
      group: 'column',
      style: ['2d', '3d'],
      orientation: ['horizontal', 'vertical'],
      unit: ['ms', 'MB', 'GB', '%', 's', 'rows', 'bytes'],
    },
    paramInfo: {
      type: 'Chart type — determines how data points are rendered',
      labels: 'Column used for the X-axis / category labels (e.g. database, table)',
      values: 'Column used for the Y-axis / numeric values (e.g. total_bytes, count)',
      group: 'Column to split series by — creates one line/bar per unique value',
      style: '2d renders an SVG chart, 3d renders an interactive Three.js scene',
      orientation: 'Flip axes — horizontal puts labels on the Y-axis (useful for long names)',
      unit: 'Format suffix for values — controls axis labels and tooltips',
    },
  },
  {
    label: '@rag',
    detail: 'Red/amber/green threshold rule',
    template: '-- @rag: column=$1 green<$2 amber<$3',
    params: {
      column: 'column',
      'green<': null, 'green>': null,
      'amber<': null, 'amber>': null,
    },
    paramInfo: {
      column: 'Column to evaluate — cells are color-coded based on their value',
      'green<': 'Values below this threshold are green (good)',
      'green>': 'Values above this threshold are green (good)',
      'amber<': 'Values below this threshold are amber (warning) — above is red',
      'amber>': 'Values above this threshold are amber (warning) — below is red',
    },
  },
  {
    label: '@drill',
    detail: 'Click-through navigation to another query',
    template: "-- @drill: on=$1 into='$2'",
    params: {
      on: 'column',
      into: 'query',
    },
    paramInfo: {
      on: 'Column that becomes clickable — the clicked cell value is passed as a drill parameter',
      into: "Target query name (title) — receives the clicked value via {{drill:column}} template",
    },
  },
  {
    label: '@link',
    detail: 'Modal popup navigation to another query',
    template: "-- @link: on=$1 into='$2'",
    params: {
      on: 'column',
      into: 'query',
    },
    paramInfo: {
      on: 'Column that becomes clickable — opens target query in a modal popup',
      into: "Target query name (title) — receives the clicked value via {{drill:column}} template",
    },
  },
];

/* ─── template variable completions ─── */

const TEMPLATE_VARS: Completion[] = [
  { label: '{{time_range}}', detail: 'Time filter (resolved from @meta interval)', type: 'variable' },
  { label: '{{drill:column | fallback}}', detail: 'Drill-down filter expression', type: 'variable' },
  { label: '{{drill_value:column | fallback}}', detail: 'Drill-down quoted value', type: 'variable' },
  { label: '{{cluster_aware:system.table}}', detail: 'Cluster-aware table (clusterAllReplicas)', type: 'variable' },
  { label: '{{cluster_metadata:system.table}}', detail: 'Cluster metadata table reference', type: 'variable' },
];

/* ─── completion source factory ─── */

export interface QueryNameEntry {
  name: string;
  group: string;
}

interface DirectiveCompletionRefs {
  columns: { current: string[] };
  queries: { current: QueryNameEntry[] };
}

/**
 * Build a directive completion source with access to result columns and query names.
 * Refs are read at completion time so they always reflect the latest state.
 */
export function createDirectiveCompletionSource(refs: DirectiveCompletionRefs) {
  return function directiveCompletionSource(context: CompletionContext): CompletionResult | null {
    const line = context.state.doc.lineAt(context.pos);
    const textBefore = line.text.slice(0, context.pos - line.from);

    // 1. Template variable: user typed {{ anywhere
    const templateMatch = textBefore.match(/(\{\{[\w_:]*?)$/);
    if (templateMatch) {
      return {
        from: context.pos - templateMatch[1].length,
        options: TEMPLATE_VARS,
        validFor: /^[\w{}:_| ]*/,
      };
    }

    // Only process directives inside comment lines
    if (!textBefore.match(/^\s*--\s*/)) return null;

    // 2. Directive name: user typed "-- @" or "-- @ch..."
    const directiveNameMatch = textBefore.match(/--\s*(@\w*)$/);
    if (directiveNameMatch) {
      return {
        from: context.pos - directiveNameMatch[1].length,
        options: DIRECTIVES.map(d => ({
          label: d.label,
          detail: d.detail,
          type: 'keyword' as const,
          apply: d.template.replace(/^--\s*/, '').replace(/\$\d/g, ''),
          boost: 1,
        })),
        validFor: /^@\w*/,
      };
    }

    // 3. Parameter value: user typed e.g. "type=b" or "into='Biggest T"
    //    Quoted form: into='partial text   Unquoted form: type=partial
    const quotedValueMatch = textBefore.match(/--\s*@(\w+):\s*.*?([\w<>]+)='([^']*)$/);
    const unquotedValueMatch = !quotedValueMatch && textBefore.match(/--\s*@(\w+):\s*.*?([\w<>]+)=([^'= ]*)$/);
    const paramValueMatch = quotedValueMatch ?? unquotedValueMatch;
    if (paramValueMatch) {
      const [, directiveName, paramName, typed] = paramValueMatch;
      const directive = DIRECTIVES.find(d => d.label === `@${directiveName}`);
      if (directive) {
        const paramDef = directive.params[paramName];
        if (paramDef === 'query') {
          const queries = refs.queries.current;
          if (queries.length > 0) {
            // If user already typed opening quote, replace from after it; otherwise include quotes
            const isQuoted = !!quotedValueMatch;
            return {
              from: context.pos - typed.length,
              options: queries.map(q => ({
                label: q.name,
                detail: q.group,
                type: 'text' as const,
                // Auto-wrap in quotes, closing the quote on apply
                apply: isQuoted ? `${q.name}'` : `'${q.name}'`,
              })),
              validFor: /^[^']*$/,
            };
          }
        }
        const values = paramDef === 'column' ? refs.columns.current : paramDef;
        if (values && typeof values !== 'string' && values.length > 0) {
          return {
            from: context.pos - typed.length,
            options: values.map(v => ({
              label: v,
              type: paramDef === 'column' ? 'variable' as const : 'enum' as const,
            })),
            validFor: quotedValueMatch ? /^[^']*$/ : /^[\w]*$/,
          };
        }
      }
    }

    // 4. Parameter key: user typed "-- @chart: type=bar " or "-- @chart: type=bar la"
    //    Match both after a space (ready for next param) and mid-word (typing param name)
    const paramKeyMatch = textBefore.match(/--\s*@(\w+):\s*(.*\s)(\w*)$/);
    if (paramKeyMatch) {
      const [, directiveName, existing, partial] = paramKeyMatch;
      const directive = DIRECTIVES.find(d => d.label === `@${directiveName}`);
      if (directive) {
        const usedParams = [...existing.matchAll(/([\w<>]+)=/g)].map(m => m[1]);
        const remaining = Object.keys(directive.params).filter(p => !usedParams.includes(p));
        if (remaining.length > 0) {
          return {
            from: context.pos - partial.length,
            options: remaining.map(p => {
              const paramDef = directive.params[p];
              let detail: string;
              if (paramDef === 'column') {
                const cols = refs.columns.current;
                detail = cols.length > 0 ? `(${cols.slice(0, 3).join(', ')}…)` : '(result column)';
              } else if (paramDef === 'query') {
                const queries = refs.queries.current;
                detail = queries.length > 0 ? `(${queries.slice(0, 2).map(q => q.name).join(', ')}…)` : '(query name)';
              } else if (paramDef) {
                detail = `(${paramDef.slice(0, 3).join(', ')}…)`;
              } else {
                detail = '(free text)';
              }
              return {
                label: `${p}=`,
                detail,
                type: 'property' as const,
                info: directive.paramInfo[p] || undefined,
              };
            }),
            validFor: /^[\w<>]*=?$/,
          };
        }
      }
    }

    return null;
  };
}

/** Backward-compatible standalone source (no dynamic data). */
const emptyRefs: DirectiveCompletionRefs = { columns: { current: [] }, queries: { current: [] } };
export const directiveCompletionSource = createDirectiveCompletionSource(emptyRefs);
