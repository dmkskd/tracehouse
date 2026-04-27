/**
 * DetailSidebar — right-hand panel showing system table details and query runner
 */

import React, { useState } from 'react';
import { useNavigate } from '../../hooks/useAppLocation';
import { encodeSql } from '../../hooks/useUrlState';
import { SqlHighlight } from '../common/SqlHighlight';
import type { SunburstNodeData, DiagnosticQuery, ObservabilityData, ColumnCommentMap } from './data';
import { OBSERVABILITY_DATA } from './data';

// ─── Inline styles (converted from ObservabilitySunburst.css) ─

const sidebarBase: React.CSSProperties = {
  width: 440,
  background: 'rgba(10, 10, 26, 0.55)',
  backdropFilter: 'blur(16px)',
  WebkitBackdropFilter: 'blur(16px)',
  borderLeft: '1px solid rgba(255, 255, 255, 0.08)',
  boxShadow: '-6px 0 24px rgba(0, 0, 0, 0.25)',
  overflowY: 'auto',
  padding: 20,
  flexShrink: 0,
  height: '100%',
  color: 'var(--text-primary)',
  position: 'relative',
  zIndex: 1,
  transition: 'width 0.25s ease',
};

const sidebarStyle = (expanded: boolean | undefined): React.CSSProperties => ({
  ...sidebarBase,
  ...(expanded ? { width: 880 } : {}),
});

const DS = {
  expandBtn: {
    background: 'none',
    border: '1px solid var(--border-secondary)',
    borderRadius: 3,
    color: 'var(--text-muted)',
    fontSize: 10,
    width: 20,
    height: 20,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    cursor: 'pointer',
    flexShrink: 0,
  } as React.CSSProperties,
  header: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 11,
    color: 'var(--text-secondary)',
    marginBottom: 14,
    textTransform: 'uppercase',
    letterSpacing: 2,
    fontWeight: 600,
  } as React.CSSProperties,
  breadcrumb: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 11,
    color: 'var(--text-muted)',
    marginBottom: 14,
    paddingBottom: 10,
    borderBottom: '1px solid var(--border-secondary)',
  } as React.CSSProperties,
  tableName: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 16,
    fontWeight: 600,
    color: 'var(--text-primary)',
    marginBottom: 5,
  } as React.CSSProperties,
  docsLink: {
    fontSize: 14,
    color: 'var(--text-muted)',
    textDecoration: 'none',
    opacity: 0.5,
    flexShrink: 0,
  } as React.CSSProperties,
  desc: {
    fontSize: 13,
    color: 'var(--text-secondary)',
    lineHeight: 1.5,
    marginBottom: 8,
  } as React.CSSProperties,
  sectionTitle: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 9,
    color: 'var(--text-muted)',
    marginBottom: 8,
    textTransform: 'uppercase',
    letterSpacing: 1.5,
    fontWeight: 600,
  } as React.CSSProperties,
  colsList: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 4,
  } as React.CSSProperties,
  colTag: {
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: 10.5,
    background: 'rgba(255, 255, 255, 0.06)',
    color: 'var(--text-secondary)',
    padding: '3px 10px',
    borderRadius: 4,
    border: 'none',
  } as React.CSSProperties,
  colTagWithTitle: {
    cursor: 'help',
    textDecoration: 'underline dotted var(--border-secondary)',
    textUnderlineOffset: '3px',
  } as React.CSSProperties,
};

// ─── Types ───────────────────────────────────────────────────

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  executionTime: number;
  error?: string;
}

export interface DetailSidebarProps {
  selectedNode: SunburstNodeData | null;
  queryResult: QueryResult | null;
  runQueryIndex: number | null;
  isQueryRunning: boolean;
  onRunQuery: (sql: string, queryIndex: number) => void;
  enrichedData?: ObservabilityData;
  columnComments?: ColumnCommentMap;
  expanded?: boolean;
  onToggleExpand?: () => void;
}

// ─── Helpers ─────────────────────────────────────────────────

function findTableData(tableName: string, data: ObservabilityData) {
  for (const cat of data.children) {
    for (const table of cat.children) {
      if (table.name === tableName) {
        return { category: cat.name, color: cat.color, table };
      }
    }
  }
  return null;
}

function getDocsUrl(tableName: string): string | null {
  if (tableName.startsWith('system.')) {
    const name = tableName.replace('system.', '');
    return `https://clickhouse.com/docs/en/operations/system-tables/${name}`;
  }
  if (tableName.toLowerCase().includes('explain')) {
    return 'https://clickhouse.com/docs/en/sql-reference/statements/explain';
  }
  return null;
}

function hasPlaceholder(sql: string): boolean {
  return sql.includes("'...'") || sql.includes('SELECT ...') || sql.includes('my_table') || sql.includes('mydb') || sql.includes('mytable');
}

function formatCellValue(val: unknown): string {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'number') {
    if (Number.isInteger(val)) return val.toLocaleString();
    return val.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  return String(val);
}

// ─── Component ───────────────────────────────────────────────

export const DetailSidebar: React.FC<DetailSidebarProps> = ({
  selectedNode,
  queryResult,
  runQueryIndex,
  isQueryRunning,
  onRunQuery,
  enrichedData,
  columnComments,
  expanded,
  onToggleExpand,
}) => {
  const [editingSql, setEditingSql] = useState<string | null>(null);
  const sourceData = enrichedData || OBSERVABILITY_DATA;

  // Resolve the table-level data
  let tableInfo: ReturnType<typeof findTableData> = null;
  let selectedColumn: string | null = null;

  if (selectedNode?.meta?.type === 'table') {
    tableInfo = findTableData(selectedNode.name, sourceData);
  } else if (selectedNode?.meta?.type === 'column' && selectedNode.meta.category) {
    // Walk up: find parent table
    const cat = sourceData.children.find(c => c.name === selectedNode.meta!.category);
    if (cat) {
      for (const table of cat.children) {
        if (table.children.some(c => c.name === selectedNode.name)) {
          tableInfo = { category: cat.name, color: cat.color, table };
          selectedColumn = selectedNode.name;
          break;
        }
      }
    }
  } else if (selectedNode?.meta?.type === 'category') {
    const cat = sourceData.children.find(c => c.name === selectedNode.name);
    if (cat) {
      return (
        <div style={sidebarStyle(expanded)}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
            {onToggleExpand && (
              <button onClick={onToggleExpand} style={DS.expandBtn} title={expanded ? 'Collapse panel' : 'Expand panel'}>
                {expanded ? '→' : '←'}
              </button>
            )}
            <h2 style={{ ...DS.header, marginBottom: 0 }}>Details</h2>
          </div>
          <div style={DS.breadcrumb}>
            <span style={{ color: 'var(--text-secondary)' }}>{cat.name}</span>
          </div>
          <div style={{ marginBottom: 16 }}>
            <div style={DS.tableName}>{cat.name}</div>
            <div style={DS.desc}>
              {cat.children.length} system tables in this category. Click a segment to explore.
            </div>
          </div>
          <div>
            <h3 style={DS.sectionTitle}>Tables</h3>
            <div style={DS.colsList}>
              {cat.children.map(t => (
                <span key={t.name} style={DS.colTag}>{t.name}</span>
              ))}
            </div>
          </div>
        </div>
      );
    }
  }

  if (!tableInfo) {
    return (
      <div style={sidebarStyle(expanded)}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
          {onToggleExpand && (
            <button onClick={onToggleExpand} style={DS.expandBtn} title={expanded ? 'Collapse panel' : 'Expand panel'}>
              {expanded ? '→' : '←'}
            </button>
          )}
          <h2 style={{ ...DS.header, marginBottom: 0 }}>Details</h2>
        </div>
        <p style={{ color: 'var(--text-muted)', fontSize: 13 }}>
          Hover or click a segment to see system table details, key columns, and example diagnostic queries.
        </p>
      </div>
    );
  }

  const { category, table } = tableInfo;

  return (
    <div style={sidebarStyle(expanded)}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        {onToggleExpand && (
          <button onClick={onToggleExpand} style={DS.expandBtn} title={expanded ? 'Collapse panel' : 'Expand panel'}>
            {expanded ? '→' : '←'}
          </button>
        )}
        <h2 style={{ ...DS.header, marginBottom: 0 }}>Details</h2>
      </div>

      {/* Breadcrumb */}
      <div style={DS.breadcrumb}>
        <span style={{ color: 'var(--text-secondary)' }}>{category}</span>
        <span style={{ color: 'var(--text-muted)' }}> → </span>
        {table.name}
      </div>

      {/* Table info */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{ ...DS.tableName, marginBottom: 0 }}>{table.name}</div>
          {getDocsUrl(table.name) && (
            <a
              href={getDocsUrl(table.name)!}
              target="_blank"
              rel="noopener noreferrer"
              style={DS.docsLink}
            >
              ↗
            </a>
          )}
        </div>
        <div style={DS.desc}>{table.desc}</div>
        {/* Metadata badges */}
        <div style={{ display: 'flex', gap: 6, marginTop: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          {table.available === false && (
            <span title="This table does not exist on the connected server version" style={badgeStyle('#ef4444')}>Not available</span>
          )}
          {table.available === true && (
            <span title="Table exists on this server version — some tables may need to be enabled in server config before they contain data" style={badgeStyle('#16a34a')}>Available</span>
          )}
          {table.since && (
            <span style={badgeStyle('#64748b')}>Since {table.since}</span>
          )}
          {table.cloudOnly && (
            <span style={badgeStyle('#8b5cf6')}>Cloud only</span>
          )}
        </div>
      </div>

      {/* Sorting / Primary key */}
      {(table.sortingKey || table.primaryKey) && (
        <div style={{ marginBottom: 16 }}>
          {table.sortingKey && (
            <div style={{ marginBottom: table.primaryKey && table.primaryKey !== table.sortingKey ? 8 : 0 }}>
              <h3 style={DS.sectionTitle}>Sorting Key</h3>
              <code style={{ fontSize: 11, color: 'var(--text-secondary)', fontFamily: "'JetBrains Mono', monospace", wordBreak: 'break-all' }}>
                {table.sortingKey}
              </code>
            </div>
          )}
          {table.primaryKey && table.primaryKey !== table.sortingKey && (
            <div>
              <h3 style={DS.sectionTitle}>Primary Key</h3>
              <code style={{ fontSize: 11, color: 'var(--text-secondary)', fontFamily: "'JetBrains Mono', monospace", wordBreak: 'break-all' }}>
                {table.primaryKey}
              </code>
            </div>
          )}
        </div>
      )}

      {/* Key columns */}
      {table.cols.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <h3 style={DS.sectionTitle}>Key Columns</h3>
          <div style={DS.colsList}>
            {table.cols.map(c => {
              const comment = columnComments?.get(`${table.name}.${c}`);
              return (
                <span
                  key={c}
                  title={comment || undefined}
                  style={{
                    ...DS.colTag,
                    ...(comment ? DS.colTagWithTitle : {}),
                    ...(selectedColumn === c ? { color: 'var(--text-primary)', fontWeight: 700 } : {}),
                  }}
                >
                  {c}
                </span>
              );
            })}
          </div>
        </div>
      )}

      {/* Selected column highlight */}
      {selectedColumn && (
        <div style={{ marginBottom: 16 }}>
          <h3 style={DS.sectionTitle}>Selected</h3>
          <div style={{ ...DS.tableName, fontSize: 13, color: 'var(--text-primary)' }}>{selectedColumn}</div>
          <div style={DS.desc}>{columnComments?.get(`${table.name}.${selectedColumn}`) || selectedNode?.meta?.desc || ''}</div>
        </div>
      )}

      {/* Diagnostic queries */}
      {table.queries.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <h3 style={DS.sectionTitle}>Diagnostic Queries</h3>
          {table.queries.map((q, i) => {
            const isResultForThis = queryResult && runQueryIndex === i;
            return (
              <React.Fragment key={i}>
                <QueryBlock
                  query={q}
                  queryIndex={i}
                  isRunning={isQueryRunning}
                  editingSql={editingSql}
                  onEdit={setEditingSql}
                  onRun={onRunQuery}
                />
                {isResultForThis && (
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 4 }}>
                      {queryResult.error
                        ? 'Error'
                        : `${queryResult.rows.length} rows · ${(queryResult.executionTime / 1000).toFixed(2)}s`}
                    </div>
                    {queryResult.error ? (
                      <div style={{ ...queryBlockStyle, borderColor: 'var(--accent-red)', color: 'var(--accent-red)' }}>
                        {queryResult.error}
                      </div>
                    ) : queryResult.rows.length === 0 ? (
                      <div style={{ ...queryBlockStyle, color: 'var(--text-muted)' }}>No rows returned</div>
                    ) : (
                      <div style={{ overflowX: 'auto', borderRadius: 6, border: '1px solid var(--border-secondary)' }}>
                        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, fontFamily: "'JetBrains Mono', monospace" }}>
                          <thead>
                            <tr>
                              {queryResult.columns.map(col => (
                                <th key={col} style={thStyle}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {queryResult.rows.slice(0, 50).map((row, ri) => (
                              <tr key={ri} style={{ borderTop: '1px solid var(--border-secondary)' }}>
                                {queryResult.columns.map(col => (
                                  <td key={col} style={tdStyle}>{formatCellValue(row[col])}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {queryResult.rows.length > 50 && (
                          <div style={{ padding: '6px 10px', fontSize: 10, color: 'var(--text-muted)', textAlign: 'center' }}>
                            Showing 50 of {queryResult.rows.length} rows
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </React.Fragment>
            );
          })}
        </div>
      )}
    </div>
  );
};

// ─── QueryBlock sub-component ────────────────────────────────

const queryButtonStyle: React.CSSProperties = {
  fontSize: 10,
  padding: '3px 12px',
  borderRadius: 4,
  border: '1px solid var(--border-primary)',
  background: 'var(--bg-card)',
  cursor: 'pointer',
  fontFamily: "'JetBrains Mono', monospace",
  color: 'var(--text-secondary)',
};

const QueryBlock: React.FC<{
  query: DiagnosticQuery;
  queryIndex: number;
  isRunning: boolean;
  editingSql: string | null;
  onEdit: (sql: string | null) => void;
  onRun: (sql: string, queryIndex: number) => void;
}> = ({ query, queryIndex, isRunning, editingSql, onEdit, onRun }) => {
  const navigate = useNavigate();
  const isEditing = editingSql !== null && editingSql === query.sql;
  const needsEdit = hasPlaceholder(query.sql);
  const [localSql, setLocalSql] = useState(query.sql);

  const handleRun = () => {
    if (isEditing) {
      onRun(localSql, queryIndex);
      onEdit(null);
    } else if (needsEdit) {
      setLocalSql(query.sql);
      onEdit(query.sql);
    } else {
      onRun(query.sql, queryIndex);
    }
  };

  const handleOpenInExplorer = () => {
    const sql = isEditing ? localSql : query.sql;
    const params = new URLSearchParams({ tab: 'misc', sql: encodeSql(sql), from: 'obsmap' });
    navigate(`/analytics?${params.toString()}`);
  };

  return (
    <div style={queryBlockStyle}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <span style={queryLabelStyle}>{query.label}</span>
        <div style={{ display: 'flex', gap: 4 }}>
          <button
            onClick={handleRun}
            disabled={isRunning}
            style={{
              ...queryButtonStyle,
              background: isRunning ? 'transparent' : 'var(--bg-card)',
              color: isRunning ? 'var(--text-muted)' : 'var(--text-primary)',
              cursor: isRunning ? 'wait' : 'pointer',
            }}
          >
            {isRunning ? '...' : isEditing ? 'Execute' : needsEdit ? 'Edit & Run' : 'Run'}
          </button>
          <button
            onClick={handleOpenInExplorer}
            title="Open in Query Explorer"
            style={{ ...queryButtonStyle }}
          >
            Open ↗
          </button>
        </div>
      </div>
      {isEditing ? (
        <textarea
          value={localSql}
          onChange={e => setLocalSql(e.target.value)}
          style={{
            width: '100%',
            minHeight: 80,
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 10.5,
            color: 'var(--accent-blue)',
            background: 'transparent',
            border: '1px solid var(--border-primary)',
            borderRadius: 4,
            padding: 8,
            resize: 'vertical',
            lineHeight: 1.6,
            outline: 'none',
          }}
        />
      ) : (
        <SqlHighlight style={{
          background: 'transparent',
          padding: 0,
          fontSize: 10.5,
          lineHeight: 1.6,
        }}>
          {query.sql}
        </SqlHighlight>
      )}
    </div>
  );
};

// ─── Inline styles for query block sub-components ────────────

const queryBlockStyle: React.CSSProperties = {
  background: 'var(--bg-tertiary)',
  border: '1px solid var(--border-primary)',
  borderRadius: 6,
  padding: '10px 12px',
  marginBottom: 8,
  fontFamily: "'JetBrains Mono', monospace",
};

const queryLabelStyle: React.CSSProperties = {
  fontSize: 9,
  color: 'var(--text-muted)',
  textTransform: 'uppercase',
  letterSpacing: 1,
  fontWeight: 500,
};

const thStyle: React.CSSProperties = {
  padding: '6px 8px',
  textAlign: 'left',
  background: 'var(--bg-tertiary)',
  color: 'var(--text-muted)',
  fontWeight: 600,
  fontSize: 10,
  whiteSpace: 'nowrap',
  position: 'sticky',
  top: 0,
};

const tdStyle: React.CSSProperties = {
  padding: '4px 8px',
  color: 'var(--text-secondary)',
  whiteSpace: 'nowrap',
  maxWidth: 200,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
};

const badgeStyle = (color: string): React.CSSProperties => ({
  fontSize: 9,
  padding: '2px 8px',
  borderRadius: 4,
  border: `1px solid ${color}40`,
  background: `${color}15`,
  color,
  fontFamily: "'JetBrains Mono', monospace",
  fontWeight: 500,
  letterSpacing: 0.3,
});
