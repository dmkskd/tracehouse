import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { buildChartData, ChartRenderer, sortRows } from '../../components/analytics/charts';
import { ResultsTable } from '../../components/analytics/ResultsTable';
import type { NotebookDocument, NotebookEvidence, NotebookStage } from './model';
import { evidenceTarget, notebookKindLabel, rowMatchesKey } from './model';
import { stageToMarkdown } from './markdown';

const claimColors = {
  observed: 'var(--accent-blue)',
  derived: 'var(--accent-yellow)',
  inferred: '#f59e0b',
  recommended: 'var(--accent-green)',
} as const;

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

function StageVisual({ stage, evidence }: { stage: NotebookStage; evidence: NotebookEvidence }) {
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const rows = evidence.rows as Record<string, unknown>[];
  const sortedRows = sortColumn ? sortRows(rows, sortColumn, sortDirection) : rows;

  if (stage.block === 'timeseries.annotated') {
    const x = stage.encoding.x;
    const y = Array.isArray(stage.encoding.y) ? stage.encoding.y[0] : stage.encoding.y;
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

  if (stage.block === 'table.ranked') {
    const rankBy = stage.encoding.rankBy;
    const rankedRows = rankBy ? sortRows(rows, rankBy, 'desc') : sortedRows;
    // Guarded: rowMatchesKey is vacuously true for an absent key, so a stage
    // with no rowKey would otherwise report its first row as the highlight.
    const rowKey = stage.highlight?.rowKey;
    const highlighted = rowKey ? rankedRows.find(row => rowMatchesKey(row, rowKey)) : undefined;
    return (
      <div>
        {highlighted && rankBy && (
          <div style={{
            marginBottom: 10, padding: '8px 10px', borderLeft: '2px solid var(--accent-yellow)',
            background: 'rgba(210,153,34,0.09)', color: 'var(--text-secondary)', fontSize: 12,
          }}>
            Highlighted: {String(highlighted[stage.encoding.label ?? evidence.columns[0]])}
            {' · '}{String(highlighted[rankBy])}{evidence.units?.[rankBy] ? ` ${evidence.units[rankBy]}` : ''}
          </div>
        )}
        <ResultsTable
          columns={evidence.columns}
          rows={rankedRows}
          sortColumn={sortColumn}
          sortDirection={sortDirection}
          onSort={column => {
            if (sortColumn === column) setSortDirection(value => value === 'asc' ? 'desc' : 'asc');
            else { setSortColumn(column); setSortDirection('desc'); }
          }}
          compact
          enableRowDetails
        />
      </div>
    );
  }

  const label = stage.encoding.label ?? evidence.columns[0];
  const value = stage.encoding.value ?? evidence.columns[1];
  const factRowKey = stage.highlight?.rowKey;
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 10 }}>
      {rows.map((row, index) => {
        // Same guard: without it every tile renders highlighted, which
        // highlights nothing.
        const highlighted = factRowKey ? rowMatchesKey(row, factRowKey) : false;
        return (
          <div key={index} style={{
            padding: 14, border: `1px solid ${highlighted ? 'var(--accent-yellow)' : 'var(--border-primary)'}`,
            borderRadius: 7, background: highlighted ? 'rgba(210,153,34,0.09)' : 'var(--bg-primary)',
          }}>
            <div style={{ color: 'var(--text-muted)', fontSize: 11 }}>{String(row[label] ?? '—')}</div>
            <div style={{ marginTop: 5, color: 'var(--text-primary)', fontSize: 17, fontFamily: 'monospace', fontWeight: 650 }}>
              {String(row[value] ?? '—')}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function StageCard({
  stage,
  evidence,
  index,
  focused,
  onFocus,
}: {
  stage: NotebookStage;
  evidence: NotebookEvidence;
  index: number;
  focused: boolean;
  onFocus: () => void;
}) {
  const [showSource, setShowSource] = useState(false);
  return (
    <section id={`notebook-stage-${stage.id}`} style={{
      border: focused ? '1px solid rgba(99,102,241,0.62)' : '1px solid var(--border-primary)',
      borderRadius: 8, background: 'var(--bg-card)', overflow: 'hidden',
      boxShadow: focused ? '0 0 0 2px rgba(99,102,241,0.10)' : undefined,
    }}>
      <header style={{ padding: '17px 18px', borderBottom: '1px solid var(--border-primary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ color: 'var(--accent-yellow)', fontFamily: 'monospace', fontSize: 10, fontWeight: 700 }}>
            STEP {String(index + 1).padStart(2, '0')}
          </span>
          <span style={{
            padding: '2px 6px', border: `1px solid ${claimColors[stage.claimType]}`,
            borderRadius: 4, color: claimColors[stage.claimType], fontFamily: 'monospace',
            fontSize: 9, fontWeight: 700, textTransform: 'uppercase',
          }}>
            {stage.claimType}
          </span>
          {/* Every panel action lives here: one row, one style, nothing in a
              footer. */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 2, marginLeft: 'auto' }}>
            <button
              onClick={() => setShowSource(value => !value)}
              style={showSource ? panelActionActiveStyle : panelActionStyle}
              title="Show this step as Markdown"
            >
              Source
            </button>
            <EvidenceAction evidence={evidence} />
            <button
              onClick={onFocus}
              style={focused ? panelActionActiveStyle : panelActionStyle}
              title="Focus this step"
            >
              Focus
            </button>
          </div>
        </div>
        <h2 style={{ margin: '9px 0 7px', fontSize: 20, color: 'var(--text-primary)' }}>{stage.headline}</h2>
        <p style={{ margin: 0, color: 'var(--text-secondary)', fontSize: 13, lineHeight: 1.55 }}>{stage.takeaway}</p>
        {stage.caveat && (
          <div style={{ marginTop: 11, padding: '8px 10px', borderLeft: '2px solid #f59e0b', background: 'rgba(245,158,11,0.08)', color: '#d99a30', fontSize: 11 }}>
            Inference boundary: {stage.caveat}
          </div>
        )}
      </header>
      <div style={{ padding: 18 }}>
        <div style={{ marginBottom: 12, color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: 10 }}>
          {evidence.title} · {evidence.mode}
        </div>
        <StageVisual stage={stage} evidence={evidence} />
        {showSource && (
          <pre
            aria-label={`Source for step ${index + 1}`}
            style={{
              margin: '14px 0 0', padding: 12, overflow: 'auto',
              border: '1px solid var(--border-primary)', borderRadius: 6,
              background: 'var(--bg-primary)', color: 'var(--text-secondary)',
              fontFamily: 'monospace', fontSize: 11, lineHeight: 1.5, whiteSpace: 'pre-wrap',
            }}
          >
            {stageToMarkdown(stage, evidence, index).join('\n')}
          </pre>
        )}
      </div>
    </section>
  );
}

export function NotebookView({ document }: { document: NotebookDocument }) {
  const [focusIndex, setFocusIndex] = useState<number | null>(null);
  const focusActive = focusIndex !== null;

  useEffect(() => {
    if (!focusActive) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setFocusIndex(null);
      if (event.key === 'ArrowDown' || event.key === 'ArrowRight') {
        event.preventDefault();
        setFocusIndex(value => value === null ? 0 : Math.min(document.stages.length - 1, value + 1));
      }
      if (event.key === 'ArrowUp' || event.key === 'ArrowLeft') {
        event.preventDefault();
        setFocusIndex(value => value === null ? 0 : Math.max(0, value - 1));
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [document.stages.length, focusActive]);

  const visibleStages = useMemo(() => focusIndex === null
    ? document.stages.map((stage, index) => ({ stage, index }))
    : [{ stage: document.stages[focusIndex], index: focusIndex }], [document.stages, focusIndex]);

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
            <button onClick={() => setFocusIndex(focusActive ? null : 0)} style={{ ...buttonStyle, marginLeft: 'auto' }}>
              {focusActive ? 'Exit focus' : 'Present notebook'}
            </button>
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
              {document.stages.map((stage, index) => (
                <button key={stage.id} onClick={() => setFocusIndex(index)} style={{
                  padding: '9px 10px', border: '1px solid var(--border-primary)', borderRadius: 6,
                  background: index === focusIndex ? 'var(--bg-card-hover)' : 'var(--bg-card)',
                  color: index === focusIndex ? 'var(--text-primary)' : 'var(--text-muted)',
                  textAlign: 'left', fontSize: 11, cursor: 'pointer',
                }}>
                  <span style={{ color: 'var(--accent-yellow)', fontFamily: 'monospace', marginRight: 7 }}>{String(index + 1).padStart(2, '0')}</span>
                  {stage.headline}
                </button>
              ))}
              <div style={{ color: 'var(--text-muted)', fontSize: 9, fontFamily: 'monospace', padding: '6px 2px' }}>
                ↑ ↓ steps · Esc workbook
              </div>
            </aside>
          )}
          <main style={{ display: 'grid', gap: 18, minWidth: 0 }}>
            {visibleStages.map(({ stage, index }) => (
              <StageCard
                key={stage.id}
                stage={stage}
                evidence={document.evidence[stage.evidence]}
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
