import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { buildChartData, ChartRenderer } from '../../components/analytics/charts';
import { NotebookTable } from './NotebookTable';
import type { NotebookDocument, NotebookEvidence, NotebookCell } from './model';
import { evidenceTarget, factValueFontSize, notebookKindLabel, rowMatchesKey } from './model';
import { cellToMarkdown } from './markdown';

const buttonStyle: React.CSSProperties = {
  padding: '7px 10px',
  border: '1px solid var(--border-primary)',
  borderRadius: 6,
  color: 'var(--text-secondary)',
  background: 'var(--bg-secondary)',
  fontSize: 11,
  cursor: 'pointer',
  textDecoration: 'none',
};

/**
 * Panel actions share one style, matching the analytics dashboard toolbar:
 * small, muted, square, and grouped in the header rather than scattered
 * between header and footer.
 */
const panelActionStyle: React.CSSProperties = {
  display: 'grid', placeItems: 'center',
  height: 20, padding: '0 7px',
  border: '1px solid transparent', borderRadius: 4,
  background: 'transparent', color: 'var(--text-muted)',
  fontSize: 11, lineHeight: 1, cursor: 'pointer', textDecoration: 'none',
};

const panelActionActiveStyle: React.CSSProperties = {
  ...panelActionStyle,
  color: 'var(--accent-primary, #6366f1)',
  background: 'rgba(99,102,241,0.10)',
  border: '1px solid rgba(99,102,241,0.24)',
};

function EvidenceAction({ evidence }: { evidence: NotebookEvidence }) {
  const target = evidenceTarget(evidence);
  if (!target) return null;
  if (target.kind === 'route') {
    return (
      <Link to={target.value} style={panelActionStyle} title="Open this evidence in TraceHouse">
        Evidence ↗
      </Link>
    );
  }
  return (
    <a
      href={target.value}
      target="_blank"
      rel="noopener noreferrer"
      style={panelActionStyle}
      title="Open this evidence in a new tab"
    >
      Evidence ↗
    </a>
  );
}

function CellVisual({ cell, evidence }: { cell: NotebookCell; evidence: NotebookEvidence }) {
  const rows = evidence.rows;

  if (cell.block === 'timeseries.annotated') {
    const x = cell.encoding.x;
    const y = Array.isArray(cell.encoding.y) ? cell.encoding.y[0] : cell.encoding.y;
    const data = buildChartData(rows, evidence.columns, x, y);
    return (
      <div style={{ height: 320, minHeight: 240 }}>
        <ChartRenderer
          chartType="line"
          data={data}
          groupedData={[]}
          unit={y ? evidence.units?.[y] : undefined}
          fullHeight
        />
      </div>
    );
  }

  if (cell.block === 'table.ranked') return <NotebookTable cell={cell} evidence={evidence} />;

  const label = cell.encoding.label ?? evidence.columns[0];
  const value = cell.encoding.value ?? evidence.columns[1];
  const factRowKey = cell.highlight?.rowKey;
  if (rows.some(row => String(row[value] ?? '').length > 100)) {
    return <NotebookTable cell={{ ...cell, columns: cell.columns ?? [{ field: label, label: 'Record' }, { field: value, label: 'Details' }] }} evidence={evidence} />;
  }
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 10 }}>
      {rows.map((row, index) => {
        // Same guard: without it every tile renders highlighted, which
        // highlights nothing.
        const highlighted = factRowKey ? rowMatchesKey(row, factRowKey) : false;
        const text = String(row[value] ?? '—');
        return (
          // `stat-card` is the app's own tile: one background, one border, one
          // radius, and a light-theme override that already exists.
          <div key={index} className="stat-card" style={{
            display: 'flex', flexDirection: 'column', minWidth: 0, padding: 14,
            border: `1px solid ${highlighted ? 'var(--accent-yellow)' : 'var(--border-primary)'}`,
            background: highlighted ? 'rgba(210,153,34,0.09)' : undefined,
          }}>
            <div style={{ color: 'var(--text-muted)', fontSize: 11, lineHeight: 1.35 }}>{String(row[label] ?? '—')}</div>
            {/* Values are read across the row, so they sit on a common bottom
                edge: a label that wraps to two lines must not push its value
                out of line with its neighbours'. `anywhere` is the backstop for
                a token with no space to wrap at. */}
            <div style={{
              marginTop: 'auto', paddingTop: 8, color: 'var(--text-primary)',
              fontSize: factValueFontSize(text), fontFamily: 'monospace', fontWeight: 650,
              lineHeight: 1.3, overflowWrap: 'anywhere',
            }}>
              {text}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function CellCard({
  cell,
  evidence,
  index,
  focused,
  onFocus,
}: {
  cell: NotebookCell;
  evidence: NotebookEvidence;
  index: number;
  focused: boolean;
  onFocus: () => void;
}) {
  const [showSource, setShowSource] = useState(false);
  return (
    <section id={`notebook-cell-${cell.id}`} style={{
      border: focused ? '1px solid rgba(99,102,241,0.62)' : '1px solid var(--border-primary)',
      borderRadius: 8, background: 'var(--bg-card)', overflow: 'hidden',
      boxShadow: focused ? '0 0 0 2px rgba(99,102,241,0.10)' : undefined,
    }}>
      <header style={{ padding: '17px 18px', borderBottom: '1px solid var(--border-primary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ color: 'var(--accent-yellow)', fontFamily: 'monospace', fontSize: 10, fontWeight: 700 }}>
            CELL {String(index + 1).padStart(2, '0')}
          </span>
          {/* Every panel action lives here: one row, one style, nothing in a
              footer. */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 2, marginLeft: 'auto' }}>
            <button
              onClick={() => setShowSource(value => !value)}
              style={showSource ? panelActionActiveStyle : panelActionStyle}
              title="Show this cell as Markdown"
            >
              Markdown
            </button>
            <EvidenceAction evidence={evidence} />
            <button
              onClick={onFocus}
              style={focused ? panelActionActiveStyle : panelActionStyle}
              title="Focus this cell"
            >
              Focus
            </button>
          </div>
        </div>
        <h2 style={{ margin: '9px 0 7px', fontSize: 20, color: 'var(--text-primary)' }}>{cell.headline}</h2>
        <p style={{ margin: 0, color: 'var(--text-secondary)', fontSize: 13, lineHeight: 1.55 }}>{cell.takeaway}</p>
      </header>
      <div style={{ padding: 18 }}>
        <div style={{ marginBottom: 12, color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: 10 }}>
          {evidence.title} · {evidence.mode === 'live-link' ? 'Linked evidence' : 'Captured evidence'}
        </div>
        <CellVisual cell={cell} evidence={evidence} />
        {showSource && (
          <pre
            aria-label={`Source for cell ${index + 1}`}
            style={{
              margin: '14px 0 0', padding: 12, overflow: 'auto',
              border: '1px solid var(--border-primary)', borderRadius: 6,
              background: 'var(--bg-primary)', color: 'var(--text-secondary)',
              fontFamily: 'monospace', fontSize: 11, lineHeight: 1.5, whiteSpace: 'pre-wrap',
            }}
          >
            {cellToMarkdown(cell, evidence, index).join('\n')}
          </pre>
        )}
      </div>
    </section>
  );
}

export function NotebookView({ document, focusIndex: controlledFocus, onFocusChange }: {
  document: NotebookDocument;
  focusIndex?: number | null;
  onFocusChange?: (index: number | null) => void;
}) {
  const [localFocus, setLocalFocus] = useState<number | null>(null);
  const focusIndex = controlledFocus === undefined ? localFocus : controlledFocus;
  const setFocusIndex = onFocusChange ?? setLocalFocus;
  const focusActive = focusIndex !== null;

  useEffect(() => {
    if (!focusActive) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setFocusIndex(null);
      if (event.key === 'ArrowDown' || event.key === 'ArrowRight') {
        event.preventDefault();
        setFocusIndex(Math.min(document.cells.length - 1, (focusIndex ?? -1) + 1));
      }
      if (event.key === 'ArrowUp' || event.key === 'ArrowLeft') {
        event.preventDefault();
        setFocusIndex(Math.max(0, (focusIndex ?? 1) - 1));
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [document.cells.length, focusActive, focusIndex, setFocusIndex]);

  const visibleCells = useMemo(() => focusIndex === null
    ? document.cells.map((cell, index) => ({ cell, index }))
    : [{ cell: document.cells[focusIndex], index: focusIndex }], [document.cells, focusIndex]);

  return (
    <div className="page-layout" style={{ height: '100%', overflow: 'auto', gap: 0 }}>
      <div style={{ width: 'min(1180px, 100%)', margin: '0 auto' }}>
        <header style={{ padding: '20px 0 22px', borderBottom: '1px solid var(--border-primary)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ color: 'var(--accent-yellow)', fontFamily: 'monospace', fontSize: 10, fontWeight: 700, letterSpacing: '0.1em' }}>
              TRACEHOUSE NOTEBOOK
            </span>
            <span className="badge">{notebookKindLabel(document)}</span>
            {document.scope.sourceLabel && <span className="badge">{document.scope.sourceLabel}</span>}
            {controlledFocus === undefined && <button onClick={() => setFocusIndex(focusActive ? null : 0)} style={{ ...buttonStyle, marginLeft: 'auto' }}>
              {focusActive ? 'Exit focus' : 'Focus'}
            </button>}
          </div>
          <h1 style={{ margin: '10px 0 5px', color: 'var(--text-primary)', fontSize: 27 }}>{document.title}</h1>
          <p style={{ margin: 0, color: 'var(--text-secondary)', fontSize: 15 }}>{document.question}</p>
          <div style={{ marginTop: 11, color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: 10 }}>
            {document.scope.from} → {document.scope.to}
            {document.scope.hosts?.length ? ` · hosts: ${document.scope.hosts.join(', ')}` : ''}
          </div>
        </header>

        <div style={{ display: 'grid', gridTemplateColumns: focusActive ? '220px minmax(0, 1fr)' : '1fr', gap: 18, padding: '22px 0 40px' }}>
          {focusActive && (
            <aside style={{ position: 'sticky', top: 0, alignSelf: 'start', display: 'grid', gap: 6 }}>
              {document.cells.map((cell, index) => (
                <button key={cell.id} onClick={() => setFocusIndex(index)} style={{
                  padding: '9px 10px', border: '1px solid var(--border-primary)', borderRadius: 6,
                  background: index === focusIndex ? 'var(--bg-card-hover)' : 'var(--bg-card)',
                  color: index === focusIndex ? 'var(--text-primary)' : 'var(--text-muted)',
                  textAlign: 'left', fontSize: 11, cursor: 'pointer',
                }}>
                  <span style={{ color: 'var(--accent-yellow)', fontFamily: 'monospace', marginRight: 7 }}>{String(index + 1).padStart(2, '0')}</span>
                  {cell.headline}
                </button>
              ))}
              <div style={{ color: 'var(--text-muted)', fontSize: 9, fontFamily: 'monospace', padding: '6px 2px' }}>
                ↑ ↓ cells · Esc exit focus
              </div>
            </aside>
          )}
          <main style={{ display: 'grid', gap: 18, minWidth: 0 }}>
            {visibleCells.map(({ cell, index }) => (
              <CellCard
                key={cell.id}
                cell={cell}
                evidence={document.evidence[cell.evidence]}
                index={index}
                focused={focusIndex === index}
                onFocus={() => setFocusIndex(index)}
              />
            ))}
            {/* Ternary, not &&: an empty array makes `length` a falsy 0, which React renders. */}
            {!focusActive && document.limitations?.length ? (
              <section className="card" style={{ padding: 18 }}>
                <h2 style={{ margin: '0 0 8px', fontSize: 14 }}>Limitations</h2>
                <ul style={{ margin: 0, paddingLeft: 20, color: 'var(--text-muted)', fontSize: 12 }}>
                  {document.limitations.map(item => <li key={item} style={{ margin: '5px 0' }}>{item}</li>)}
                </ul>
              </section>
            ) : null}
          </main>
        </div>
      </div>
    </div>
  );
}
