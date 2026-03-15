/**
 * Read-only SQL syntax highlighter using CodeMirror's ClickHouse dialect.
 * Renders a minimal EditorView — visually consistent with the SQL editor,
 * zero extra bundle cost since CodeMirror is already loaded.
 */

import React, { useRef, useEffect } from 'react';
import { EditorState } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import { syntaxHighlighting, HighlightStyle } from '@codemirror/language';
import { sql } from '@codemirror/lang-sql';
import { tags as t } from '@lezer/highlight';
import { clickhouseDialect } from '../analytics/editor/clickhouseDialect';

/* Reuse the same syntax colours as the main editor */
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

const readOnlyTheme = EditorView.theme({
  '&': {
    background: 'transparent',
    fontSize: '12px',
    fontFamily: "'Share Tech Mono','Fira Code',monospace",
  },
  '.cm-content': {
    padding: '0',
    caretColor: 'transparent',
    lineHeight: '1.6',
  },
  '.cm-gutters': { display: 'none' },
  '.cm-cursor': { display: 'none' },
  '.cm-activeLine': { background: 'transparent' },
  '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': {
    background: 'rgba(88,166,255,0.15) !important',
  },
  '.cm-scroller': { overflow: 'visible' },
  '&.cm-focused': { outline: 'none' },
}, { dark: true });

interface SqlHighlightProps {
  children: string;
  style?: React.CSSProperties;
}

export const SqlHighlight: React.FC<SqlHighlightProps> = ({ children, style }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const state = EditorState.create({
      doc: children,
      extensions: [
        readOnlyTheme,
        syntaxHighlighting(syntaxColors),
        sql({ dialect: clickhouseDialect }),
        EditorState.readOnly.of(true),
        EditorView.editable.of(false),
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

  // Sync content changes
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const current = view.state.doc.toString();
    if (children !== current) {
      view.dispatch({ changes: { from: 0, to: current.length, insert: children } });
    }
  }, [children]);

  return <div ref={containerRef} style={style} />;
};
