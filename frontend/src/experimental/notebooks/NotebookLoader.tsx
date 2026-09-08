import React, { useCallback, useRef, useState } from 'react';
import { NotebookView } from './NotebookView';
import { NotebookErrorBoundary } from './NotebookErrorBoundary';
import { parseNotebook } from './validate';
import { notebookToMarkdown } from './markdown';
import { memoryLimitNotebook } from './example';
import type { NotebookDocument } from './model';

const buttonStyle: React.CSSProperties = {
  padding: '8px 12px',
  border: '1px solid var(--border-primary)',
  borderRadius: 6,
  color: 'var(--text-secondary)',
  background: 'var(--bg-secondary)',
  fontSize: 12,
  cursor: 'pointer',
};

/**
 * Gets a notebook into the app.
 *
 * Notebooks are composed elsewhere — by an agent running the compose skill, or
 * by hand against notebook.schema.json — and arrive as a JSON file. Opening one
 * is the smallest path that makes this a feature rather than a fixture viewer,
 * and it needs no backend, so it does not prejudge where notebooks eventually
 * live.
 *
 * Everything is validated before it reaches the renderer. A bad notebook shows
 * its errors; it never renders half a document.
 */
/**
 * The notebook format is mid-change: evidence is moving from frozen rows to
 * queries. Documents saved today may not load later, so say so wherever a
 * notebook is opened rather than only in the nav.
 */
const ExperimentalNotice: React.FC = () => (
  <div style={{
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '7px 16px', borderBottom: '1px solid var(--border-primary)',
    background: 'rgba(210,153,34,0.08)', color: 'var(--text-secondary)', fontSize: 11,
  }}>
    <span style={{
      padding: '1px 4px', borderRadius: 3,
      border: '1px solid var(--accent-yellow)', color: 'var(--accent-yellow)',
      fontFamily: 'monospace', fontSize: 8, fontWeight: 700,
    }}>EXP</span>
    Experimental — the notebook format is still changing. Documents saved now may not load in a later version.
  </div>
);

export function NotebookLoader() {
  const [document, setDocument] = useState<NotebookDocument | null>(null);
  const [errors, setErrors] = useState<string[] | null>(null);
  const [sourceName, setSourceName] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const [pasted, setPasted] = useState('');
  const [showSource, setShowSource] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  /** Single entry point, so every source is validated the same way. */
  const loadText = useCallback((text: string, name: string) => {
    const result = parseNotebook(text);
    setSourceName(name);
    if (result.ok) {
      setDocument(result.document);
      setErrors(null);
    } else {
      setDocument(null);
      setErrors(result.errors);
    }
  }, []);

  const load = useCallback(async (file: File) => {
    loadText(await file.text(), file.name);
  }, [loadText]);

  const reset = useCallback(() => {
    setDocument(null);
    setErrors(null);
    setSourceName(null);
    setShowSource(false);
    setPasted('');
    if (inputRef.current) inputRef.current.value = '';
  }, []);

  const onDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    setDragging(false);
    const file = event.dataTransfer.files?.[0];
    if (file) void load(file);
  }, [load]);

  if (document) {
    return (
      <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
        <ExperimentalNotice />
        <div style={{
          display: 'flex', alignItems: 'center', gap: 10,
          padding: '8px 16px', borderBottom: '1px solid var(--border-primary)',
        }}>
          <span style={{ color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: 10 }}>
            {sourceName ?? 'example'}
          </span>
          <button
            onClick={() => setShowSource(value => !value)}
            style={{
              ...buttonStyle, marginLeft: 'auto', padding: '4px 9px',
              background: showSource ? 'var(--bg-card-hover)' : 'var(--bg-secondary)',
              color: showSource ? 'var(--text-primary)' : 'var(--text-secondary)',
            }}
          >
            {showSource ? 'Hide full source' : 'Full source'}
          </button>
          <button onClick={reset} style={{ ...buttonStyle, padding: '4px 9px' }}>
            Close
          </button>
        </div>
        <div style={{ flex: 1, minHeight: 0, display: 'flex', overflow: 'hidden' }}>
          <div style={{ flex: 1, minWidth: 0, overflow: 'auto' }}>
            <NotebookErrorBoundary title={sourceName ?? undefined} onReset={reset}>
              <NotebookView document={document} />
            </NotebookErrorBoundary>
          </div>
          {showSource && (
            <aside
              aria-label="Notebook source"
              style={{
                width: 'min(520px, 45%)', borderLeft: '1px solid var(--border-primary)',
                background: 'var(--bg-primary)', overflow: 'auto', padding: 14,
              }}
            >
              {/* Markdown, not the raw JSON: evidence rows repeat their column
                  names on every object, so the shape of the data is unreadable
                  until it is a table. */}
              <pre style={{
                margin: 0, color: 'var(--text-secondary)',
                fontFamily: 'monospace', fontSize: 11, lineHeight: 1.5,
                whiteSpace: 'pre-wrap', wordBreak: 'break-word',
              }}>
                {notebookToMarkdown(document)}
              </pre>
            </aside>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="page-layout" style={{ height: '100%', overflow: 'auto', padding: 0 }}>
      <ExperimentalNotice />
      <div style={{ width: 'min(760px, 100%)', margin: '40px auto' }}>
        <h1 style={{ margin: '0 0 6px', color: 'var(--text-primary)', fontSize: 24 }}>Notebooks</h1>
        <p style={{ margin: '0 0 22px', color: 'var(--text-secondary)', fontSize: 14 }}>
          Open an investigation, runbook, or report composed against{' '}
          <code style={{ fontFamily: 'monospace', fontSize: 12 }}>notebook.schema.json</code>.
          TraceHouse renders it and links each claim back to live evidence.
        </p>

        <div
          onDragOver={event => { event.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
          style={{
            padding: '38px 20px',
            border: `1px dashed ${dragging ? 'var(--accent-yellow)' : 'var(--border-primary)'}`,
            borderRadius: 8,
            background: dragging ? 'rgba(210,153,34,0.06)' : 'var(--bg-card)',
            textAlign: 'center',
          }}
        >
          <div style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
            Drop a notebook <code style={{ fontFamily: 'monospace' }}>.json</code> here
          </div>
          <input
            ref={inputRef}
            type="file"
            accept="application/json,.json"
            onChange={event => {
              const file = event.target.files?.[0];
              if (file) void load(file);
            }}
            style={{ display: 'none' }}
          />
          <button onClick={() => inputRef.current?.click()} style={buttonStyle}>
            Choose file
          </button>
          <button
            onClick={() => {
              setDocument(memoryLimitNotebook);
              setErrors(null);
              setSourceName('example notebook');
            }}
            style={{ ...buttonStyle, marginLeft: 8 }}
          >
            View example
          </button>
        </div>

        {/* Paste is the path that needs no file at all: an agent prints a
            notebook, you copy it straight out of the chat. */}
        <div style={{ marginTop: 18 }}>
          <label
            htmlFor="notebook-paste"
            style={{ display: 'block', marginBottom: 6, color: 'var(--text-muted)', fontSize: 12 }}
          >
            or paste a notebook
          </label>
          <textarea
            id="notebook-paste"
            value={pasted}
            onChange={event => setPasted(event.target.value)}
            placeholder='{ "schemaVersion": "0.1", "title": … }'
            spellCheck={false}
            rows={6}
            style={{
              width: '100%', padding: 10, resize: 'vertical',
              border: '1px solid var(--border-primary)', borderRadius: 6,
              background: 'var(--bg-primary)', color: 'var(--text-primary)',
              fontFamily: 'monospace', fontSize: 11,
            }}
          />
          <button
            onClick={() => loadText(pasted, 'pasted notebook')}
            disabled={!pasted.trim()}
            style={{ ...buttonStyle, marginTop: 8, opacity: pasted.trim() ? 1 : 0.5 }}
          >
            Load pasted notebook
          </button>
        </div>

        {errors && (
          <div style={{
            marginTop: 18, padding: 16, borderRadius: 8,
            border: '1px solid var(--border-primary)',
            borderLeft: '2px solid var(--accent-red, #ff6b6b)',
            background: 'var(--bg-card)',
          }}>
            <h2 style={{ margin: '0 0 4px', fontSize: 14, color: 'var(--text-primary)' }}>
              {sourceName} is not a valid notebook
            </h2>
            <p style={{ margin: '0 0 10px', color: 'var(--text-muted)', fontSize: 12 }}>
              {errors.length} problem{errors.length === 1 ? '' : 's'} found. Nothing was rendered.
            </p>
            <ul style={{ margin: 0, paddingLeft: 18, color: 'var(--text-secondary)', fontSize: 12 }}>
              {errors.map(message => (
                <li key={message} style={{ margin: '4px 0', fontFamily: 'monospace' }}>{message}</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}
