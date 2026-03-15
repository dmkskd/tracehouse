/**
 * CodeMirror 6 SQL editor with ClickHouse dialect, directive autocomplete,
 * and template variable highlighting.
 *
 * Drop-in replacement for the textarea+pre overlay in QueryExplorer.
 */

import React, { useRef, useEffect } from 'react';
import { Compartment, EditorState } from '@codemirror/state';
import { EditorView, keymap, placeholder as cmPlaceholder, ViewUpdate, Decoration, MatchDecorator, ViewPlugin } from '@codemirror/view';
import type { RangeSet } from '@codemirror/state';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { sql } from '@codemirror/lang-sql';
import { autocompletion, closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete';
import type { CompletionContext, CompletionResult, Completion } from '@codemirror/autocomplete';
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search';
import { bracketMatching, indentOnInput, syntaxHighlighting, HighlightStyle } from '@codemirror/language';
import { tags as t } from '@lezer/highlight';
import type { ChFunction } from '@tracehouse/core';
import { clickhouseDialect, buildClickHouseDialect } from './clickhouseDialect';
import { createDirectiveCompletionSource, type QueryNameEntry } from './directiveCompletions';

/* ─── theme: match existing dark editor look ─── */

const editorTheme = EditorView.theme({
  '&': {
    height: '100%',
    fontSize: '12px',
    fontFamily: "'Share Tech Mono','Fira Code',monospace",
    background: 'transparent',
  },
  '.cm-content': {
    padding: '16px',
    caretColor: 'var(--text-primary)',
    lineHeight: '1.6',
    letterSpacing: 'normal',
  },
  '.cm-gutters': { display: 'none' },
  '.cm-cursor': { borderLeftColor: 'var(--text-primary)' },
  '.cm-activeLine': { background: 'rgba(255,255,255,0.03)' },
  '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
    background: 'rgba(88,166,255,0.15) !important',
  },
  '.cm-matchingBracket': {
    background: 'rgba(88,166,255,0.25)',
    outline: '1px solid rgba(88,166,255,0.4)',
  },
  // Autocomplete popup
  '.cm-tooltip-autocomplete': {
    background: 'var(--bg-secondary, #161b22)',
    border: '1px solid var(--border-primary, #30363d)',
    borderRadius: '6px',
    boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
    fontFamily: "'Share Tech Mono','Fira Code',monospace",
    fontSize: '12px',
  },
  '.cm-tooltip-autocomplete ul li': {
    padding: '3px 8px',
    color: 'var(--text-secondary, #c9d1d9)',
  },
  '.cm-tooltip-autocomplete ul li[aria-selected]': {
    background: 'rgba(88,166,255,0.15)',
    color: 'var(--text-primary, #f0f6fc)',
  },
  '.cm-completionLabel': { fontFamily: 'inherit' },
  '.cm-completionDetail': {
    fontStyle: 'italic',
    color: 'var(--text-muted, #8b949e)',
    marginLeft: '8px',
  },
  // Info tooltip (shown beside the selected completion)
  '.cm-completionInfo': {
    background: 'var(--bg-secondary, #161b22)',
    border: '1px solid var(--border-primary, #30363d)',
    borderRadius: '6px',
    padding: '6px 10px',
    color: 'var(--text-secondary, #c9d1d9)',
    fontFamily: "'Share Tech Mono','Fira Code',monospace",
    fontSize: '11px',
    lineHeight: '1.5',
    maxWidth: '320px',
  },
  // Scrollbar
  '.cm-scroller': { overflow: 'auto' },
  '.cm-scroller::-webkit-scrollbar': { width: '6px', height: '6px' },
  '.cm-scroller::-webkit-scrollbar-thumb': {
    background: 'rgba(255,255,255,0.15)',
    borderRadius: '3px',
  },
  // Search panel
  '.cm-panels': {
    background: 'var(--bg-secondary, #161b22)',
    borderBottom: '1px solid var(--border-primary, #30363d)',
    color: 'var(--text-secondary, #c9d1d9)',
  },
  '.cm-panel input': {
    background: 'var(--bg-code, #0d1117)',
    color: 'var(--text-primary, #f0f6fc)',
    border: '1px solid var(--border-primary, #30363d)',
    borderRadius: '4px',
    padding: '2px 6px',
    fontFamily: 'inherit',
    fontSize: 'inherit',
  },
  '.cm-panel button': {
    background: 'var(--bg-tertiary, #21262d)',
    color: 'var(--text-secondary, #c9d1d9)',
    border: '1px solid var(--border-primary, #30363d)',
    borderRadius: '4px',
    padding: '2px 8px',
    cursor: 'pointer',
  },
}, { dark: true });

/* ─── syntax colours: match existing highlighting ─── */

const syntaxColors = HighlightStyle.define([
  { tag: t.keyword,           color: '#c678dd', fontWeight: '500' },
  { tag: t.operatorKeyword,   color: '#c678dd' },
  { tag: [t.standard(t.name), t.function(t.variableName)], color: '#61afef' },
  { tag: t.string,            color: '#98c379' },
  { tag: t.number,            color: '#d19a66' },
  { tag: t.comment,           color: 'var(--text-muted)', fontStyle: 'italic' },
  { tag: t.operator,          color: '#c9d1d9' },
  { tag: t.paren,             color: '#c9d1d9' },
  { tag: t.typeName,          color: '#e5c07b' },
  { tag: t.bool,              color: '#d19a66' },
  { tag: t.null,              color: '#d19a66' },
]);

/* ─── template variable decorations ─── */

const templateMark = Decoration.mark({
  class: 'cm-template-var',
  attributes: {
    style: 'color:#d19a66;background:rgba(209,154,102,0.1);border-radius:2px;padding:0 1px',
  },
});

const templateDecorator = new MatchDecorator({
  regexp: /\{\{[^}]+\}\}/g,
  decoration: () => templateMark,
});

const templatePlugin = ViewPlugin.fromClass(
  class {
    decorations: RangeSet<Decoration>;
    constructor(view: EditorView) {
      this.decorations = templateDecorator.createDeco(view);
    }
    update(update: ViewUpdate) {
      this.decorations = templateDecorator.updateDeco(update, this.decorations);
    }
  },
  { decorations: v => v.decorations },
);

/* ─── compartments for reconfigurable extensions ─── */

const readOnlyCompartment = new Compartment();
const placeholderCompartment = new Compartment();
const sqlLanguageCompartment = new Compartment();

/* ─── component ─── */

export interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
  placeholder?: string;
  readOnly?: boolean;
  /** Dynamic functions from system.functions. Updates dialect + autocomplete when provided. */
  functions?: ChFunction[];
  /** Column names from the last query result. Used for directive param autocomplete. */
  columns?: string[];
  /** All queries (presets + custom). Used for @drill/@link into= autocomplete. */
  queries?: QueryNameEntry[];
}

export const SqlEditor: React.FC<SqlEditorProps> = ({
  value,
  onChange,
  onRun,
  placeholder = 'Enter SQL query…',
  readOnly = false,
  functions,
  columns,
  queries,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onRunRef = useRef(onRun);
  const fnCompletionsRef = useRef<Completion[]>([]);
  const columnsRef = useRef<string[]>([]);
  const queriesRef = useRef<QueryNameEntry[]>([]);
  onChangeRef.current = onChange;
  onRunRef.current = onRun;

  // Keep refs in sync
  columnsRef.current = columns ?? [];
  queriesRef.current = queries ?? [];

  // Directive completion source (reads refs at completion time)
  const directiveSource = useRef(createDirectiveCompletionSource({ columns: columnsRef, queries: queriesRef }));

  // Completion source for ClickHouse functions (reads from ref, updated dynamically)
  // Skips directive comment lines (-- @...) to avoid mixing with directive completions.
  const fnCompletionSource = useRef((context: CompletionContext): CompletionResult | null => {
    const options = fnCompletionsRef.current;
    if (options.length === 0) return null;
    // Don't offer function completions inside directive comments
    const line = context.state.doc.lineAt(context.pos);
    if (/^\s*--\s*@/.test(line.text)) return null;
    const word = context.matchBefore(/\w+/);
    if (!word || (word.from === word.to && !context.explicit)) return null;
    return { from: word.from, options, validFor: /^\w*$/ };
  });

  // Create editor on mount
  useEffect(() => {
    if (!containerRef.current) return;

    const state = EditorState.create({
      doc: value,
      extensions: [
        editorTheme,
        syntaxHighlighting(syntaxColors),
        sqlLanguageCompartment.of(sql({ dialect: clickhouseDialect })),
        autocompletion({
          override: [directiveSource.current, fnCompletionSource.current],
          activateOnTyping: true,
          defaultKeymap: true,
        }),
        templatePlugin,
        history(),
        bracketMatching(),
        closeBrackets(),
        indentOnInput(),
        highlightSelectionMatches(),
        placeholderCompartment.of(cmPlaceholder(placeholder)),
        EditorView.updateListener.of((update: ViewUpdate) => {
          if (update.docChanged) {
            onChangeRef.current(update.state.doc.toString());
          }
        }),
        keymap.of([
          {
            key: 'Mod-Enter',
            run: () => { onRunRef.current?.(); return true; },
          },
          ...closeBracketsKeymap,
          ...defaultKeymap,
          ...historyKeymap,
          ...searchKeymap,
        ]),
        readOnlyCompartment.of(EditorState.readOnly.of(readOnly)),
        EditorView.lineWrapping,
      ],
    });
    const view = new EditorView({ state, parent: containerRef.current });
    viewRef.current = view;

    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Sync external value changes (e.g. preset selection)
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const currentDoc = view.state.doc.toString();
    if (value !== currentDoc) {
      view.dispatch({
        changes: { from: 0, to: currentDoc.length, insert: value },
      });
    }
  }, [value]);

  // Sync readOnly
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    view.dispatch({ effects: readOnlyCompartment.reconfigure(EditorState.readOnly.of(readOnly)) });
  }, [readOnly]);

  // Sync placeholder
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    view.dispatch({ effects: placeholderCompartment.reconfigure(cmPlaceholder(placeholder)) });
  }, [placeholder]);

  // Swap SQL dialect + function completions when dynamic functions arrive
  useEffect(() => {
    const view = viewRef.current;
    if (!view || !functions || functions.length === 0) return;
    // Update dialect for syntax highlighting
    const dialect = buildClickHouseDialect(functions.map(f => f.name));
    view.dispatch({ effects: sqlLanguageCompartment.reconfigure(sql({ dialect })) });
    // Update completion options with descriptions
    fnCompletionsRef.current = functions.map(f => ({
      label: f.name,
      type: 'function' as const,
      detail: [
        f.is_aggregate ? 'agg' : '',
        f.alias_to ? `→ ${f.alias_to}` : '',
        f.description,
      ].filter(Boolean).join(' · ') || undefined,
      boost: -1,
    }));
  }, [functions]);

  return (
    <div
      ref={containerRef}
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: 'transparent',
      }}
    />
  );
};

export default SqlEditor;
