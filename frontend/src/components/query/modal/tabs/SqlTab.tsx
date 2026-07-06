import React, { useMemo, useState } from 'react';
import type { QueryDetail as QueryDetailType, QuerySeries } from '@tracehouse/core';
import { formatBytes } from '../../../../stores/databaseStore';
import { formatDurationMs, formatMicroseconds } from '../../../../utils/formatters';
import { querySqlText, type SqlDisplayMode } from '../../../../utils/querySqlText';
import { SqlHighlight } from '../../../common/SqlHighlight';

interface SqlTabProps {
  q: QuerySeries;
  queryDetail: QueryDetailType | null;
  isSelectQuery: boolean;
  onNavigateToQuery: (queryId: string) => void;
}

const PANEL: React.CSSProperties = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-secondary)',
  borderRadius: 8,
};

const LABEL: React.CSSProperties = {
  fontSize: 9,
  color: 'var(--text-muted)',
  textTransform: 'uppercase',
  letterSpacing: '1px',
};

function shortHash(value: string | undefined): string {
  return value ? String(value).slice(0, 12) : '-';
}

function openInQueryExplorer(sql: string): void {
  const encoded = btoa(unescape(encodeURIComponent(sql)));
  window.location.hash = `#/analytics?tab=misc&sql=b64:${encoded}&noAutoExecute=1&from=queries`;
}

const ToolbarIconButton: React.FC<{
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  title: string;
  ariaLabel: string;
  active?: boolean;
}> = ({ children, onClick, disabled, title, ariaLabel, active }) => (
  <button
    type="button"
    onClick={disabled ? undefined : onClick}
    disabled={disabled}
    title={title}
    aria-label={ariaLabel}
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      width: 28,
      height: 26,
      borderRadius: 5,
      border: '1px solid transparent',
      background: 'transparent',
      color: active ? 'var(--color-success)' : 'var(--text-muted)',
      cursor: disabled ? 'not-allowed' : 'pointer',
      opacity: disabled ? 0.45 : 1,
      padding: 0,
      fontFamily: 'monospace',
      fontSize: 14,
      lineHeight: 1,
    }}
    onMouseEnter={(event) => {
      if (!disabled && !active) event.currentTarget.style.color = 'var(--text-primary)';
      if (!disabled) event.currentTarget.style.borderColor = 'var(--border-primary)';
    }}
    onMouseLeave={(event) => {
      if (!active) event.currentTarget.style.color = 'var(--text-muted)';
      event.currentTarget.style.borderColor = 'transparent';
    }}
  >
    {children}
  </button>
);

const SqlModeToggle: React.FC<{
  mode: SqlDisplayMode;
  onChange: (mode: SqlDisplayMode) => void;
}> = ({ mode, onChange }) => (
  <div
    role="group"
    aria-label="SQL display mode"
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      border: '1px solid var(--border-primary)',
      borderRadius: 6,
      overflow: 'hidden',
      background: 'var(--bg-card)',
      height: 28,
    }}
  >
    <ModeButton
      active={mode === 'formatted'}
      title="Show formatted SQL"
      ariaLabel="Show formatted SQL"
      onClick={() => onChange('formatted')}
    >
      <FormattedIcon />
    </ModeButton>
    <ModeButton
      active={mode === 'raw'}
      title="Show raw SQL"
      ariaLabel="Show raw SQL"
      onClick={() => onChange('raw')}
    >
      src
    </ModeButton>
  </div>
);

const ModeButton: React.FC<{
  active: boolean;
  title: string;
  ariaLabel: string;
  onClick: () => void;
  children: React.ReactNode;
}> = ({ active, title, ariaLabel, onClick, children }) => (
  <button
    type="button"
    aria-label={ariaLabel}
    aria-pressed={active}
    title={title}
    onClick={onClick}
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      minWidth: 34,
      height: '100%',
      border: 'none',
      borderRight: '1px solid var(--border-primary)',
      background: active ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
      color: active ? 'var(--text-primary)' : 'var(--text-muted)',
      cursor: 'pointer',
      padding: '0 8px',
      fontFamily: 'monospace',
      fontSize: 10,
      lineHeight: 1,
    }}
  >
    {children}
  </button>
);

const iconProps = {
  width: 14,
  height: 14,
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 2,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  'aria-hidden': true,
};

const CopyIcon = () => (
  <svg {...iconProps}>
    <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
  </svg>
);

const OpenIcon = () => (
  <svg {...iconProps}>
    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
    <path d="M15 3h6v6" />
    <path d="M10 14 21 3" />
  </svg>
);

const FormattedIcon = () => (
  <svg {...iconProps}>
    <path d="M4 7h16" />
    <path d="M4 12h10" />
    <path d="M4 17h16" />
  </svg>
);

const FactRow: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div style={{
    display: 'grid',
    gridTemplateColumns: '140px minmax(0, 1fr)',
    gap: 14,
    alignItems: 'baseline',
    padding: '8px 0',
    borderBottom: '1px solid var(--border-secondary)',
  }}>
    <div style={LABEL}>{label}</div>
    <div style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-secondary)', minWidth: 0 }}>
      {value}
    </div>
  </div>
);

const QueryLink: React.FC<{
  queryId: string;
  label?: string;
  onNavigateToQuery: (queryId: string) => void;
}> = ({ queryId, label, onNavigateToQuery }) => (
  <button
    type="button"
    onClick={() => onNavigateToQuery(queryId)}
    title={`Open query ${queryId}`}
    style={{
      fontFamily: 'monospace',
      fontSize: 12,
      color: '#58a6ff',
      background: 'transparent',
      border: 'none',
      cursor: 'pointer',
      padding: 0,
      textAlign: 'left',
      textDecoration: 'underline',
      textDecorationStyle: 'dotted',
      wordBreak: 'break-all',
    }}
  >
    {label ?? queryId}
  </button>
);

const ChipList: React.FC<{ values: string[]; empty?: string; color?: string; limit?: number }> = ({
  values,
  empty = '-',
  color = '#58a6ff',
  limit = 28,
}) => {
  const shown = values.slice(0, limit);
  const remaining = values.length - shown.length;
  if (values.length === 0) {
    return <span style={{ color: 'var(--text-muted)' }}>{empty}</span>;
  }
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
      {shown.map((value) => (
        <span
          key={value}
          title={value}
          style={{
            maxWidth: 220,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            padding: '3px 7px',
            borderRadius: 5,
            border: `1px solid ${color}55`,
            background: `${color}16`,
            color,
            fontSize: 11,
          }}
        >
          {value}
        </span>
      ))}
      {remaining > 0 && (
        <span style={{
          padding: '3px 7px',
          borderRadius: 5,
          border: '1px solid var(--border-primary)',
          color: 'var(--text-muted)',
          fontSize: 11,
        }}>
          +{remaining}
        </span>
      )}
    </div>
  );
};

const Section: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <div style={PANEL}>
    <div style={{ padding: '12px 14px', borderBottom: '1px solid var(--border-secondary)' }}>
      <div style={{ ...LABEL, color: 'var(--text-tertiary)', fontWeight: 700 }}>{title}</div>
    </div>
    <div style={{ padding: '6px 14px 8px' }}>
      {children}
    </div>
  </div>
);

export const SqlTab: React.FC<SqlTabProps> = ({ q, queryDetail, isSelectQuery, onNavigateToQuery }) => {
  const [mode, setMode] = useState<SqlDisplayMode>('formatted');
  const [copied, setCopied] = useState(false);
  const sql = querySqlText(q, queryDetail, mode);
  const settingsCount = Object.keys(queryDetail?.Settings ?? {}).length;
  const profileEventsCount = Object.keys(queryDetail?.ProfileEvents ?? {}).length;
  const role = queryDetail?.is_initial_query === 0 ? 'Node sub-query' : 'Coordinator';
  const loggedDatabases = queryDetail?.databases ?? [];
  const displayedDatabase = loggedDatabases.length > 0
    ? loggedDatabases.join(', ')
    : queryDetail?.current_database || 'default';
  const metadataGroups = [
    { label: 'Databases', values: queryDetail?.databases ?? [], color: '#58a6ff' },
    { label: 'Tables', values: queryDetail?.tables ?? [], color: '#3fb950' },
    { label: 'Columns', values: queryDetail?.columns ?? [], color: '#d29922', limit: 36 },
    { label: 'Functions', values: queryDetail?.used_functions ?? [], color: '#58a6ff' },
    { label: 'Aggregates', values: queryDetail?.used_aggregate_functions ?? [], color: '#a371f7' },
    { label: 'Table funcs', values: queryDetail?.used_table_functions ?? [], color: '#3fb950' },
    { label: 'Formats', values: queryDetail?.used_formats ?? [], color: '#f0883e' },
    { label: 'Storages', values: queryDetail?.used_storages ?? [], color: '#8b949e' },
  ];
  const visibleMetadataGroups = metadataGroups.filter(group => group.values.length > 0);

  const summary = useMemo(() => {
    return [
      queryDetail?.query_kind || q.query_kind || 'Query',
      displayedDatabase,
      role,
      queryDetail?.hostname,
    ].filter(Boolean).join(' / ');
  }, [displayedDatabase, q.query_kind, queryDetail, role]);

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 16, marginBottom: 16 }}>
        <div>
          <div style={{ fontSize: 18, color: 'var(--text-primary)', fontFamily: 'monospace', marginBottom: 4 }}>
            SQL
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'monospace' }}>
            {summary}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', justifyContent: 'flex-end', alignItems: 'center' }}>
          <SqlModeToggle mode={mode} onChange={setMode} />
          <ToolbarIconButton
            onClick={() => {
              navigator.clipboard.writeText(sql);
              setCopied(true);
              window.setTimeout(() => setCopied(false), 1200);
            }}
            title="Copy SQL"
            ariaLabel="Copy SQL"
            active={copied}
          >
            {copied ? '✓' : <CopyIcon />}
          </ToolbarIconButton>
          <ToolbarIconButton
            onClick={() => openInQueryExplorer(sql)}
            disabled={!isSelectQuery}
            title={isSelectQuery ? 'Open in Query Explorer without executing' : 'Only SELECT queries can be opened in Query Explorer'}
            ariaLabel="Open in Query Explorer"
          >
            <OpenIcon />
          </ToolbarIconButton>
        </div>
      </div>

      <div style={{ ...PANEL, marginBottom: 16, overflow: 'hidden' }}>
        <SqlHighlight maxHeight={460} style={{
          padding: 16,
          fontSize: 12,
          lineHeight: 1.6,
          color: 'var(--text-secondary)',
          background: 'var(--bg-code)',
        }}>
          {sql}
        </SqlHighlight>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(260px, 0.8fr) minmax(320px, 1fr) minmax(320px, 1.15fr)', gap: 16 }}>
        <Section title="Statement">
          <FactRow label="Kind" value={queryDetail?.query_kind || q.query_kind || '-'} />
          <FactRow
            label={loggedDatabases.length > 0 ? 'Databases' : 'Current DB'}
            value={loggedDatabases.length > 0 ? <ChipList values={loggedDatabases} color="#58a6ff" limit={8} /> : displayedDatabase}
          />
          <FactRow label="User" value={queryDetail?.user || q.user || '-'} />
          <FactRow label="Role" value={role} />
          {queryDetail?.is_initial_query === 0 && queryDetail.initial_query_id && (
            <FactRow
              label="Parent Query"
              value={<QueryLink queryId={queryDetail.initial_query_id} label={`${queryDetail.initial_query_id.slice(0, 16)}...`} onNavigateToQuery={onNavigateToQuery} />}
            />
          )}
          {queryDetail?.log_comment && <FactRow label="Comment" value={queryDetail.log_comment} />}
        </Section>

        <Section title="Identity">
          <FactRow label="Query ID" value={<QueryLink queryId={q.query_id} onNavigateToQuery={onNavigateToQuery} />} />
          <FactRow label="Hash" value={`${shortHash(queryDetail?.query_hash)} / normalized ${shortHash(queryDetail?.normalized_query_hash)}`} />
          <FactRow label="Server" value={queryDetail?.hostname || q.hostname || '-'} />
          <FactRow label="Client" value={queryDetail?.client_hostname || queryDetail?.client_name || '-'} />
          <FactRow label="Settings" value={settingsCount.toLocaleString()} />
          <FactRow label="Events" value={profileEventsCount.toLocaleString()} />
        </Section>

        <Section title="Logged Metadata">
          {visibleMetadataGroups.length > 0 ? (
            visibleMetadataGroups.map(group => (
              <FactRow
                key={group.label}
                label={group.label}
                value={<ChipList values={group.values} color={group.color} limit={group.limit} />}
              />
            ))
          ) : (
            <div style={{
              padding: '12px 0',
              fontSize: 12,
              lineHeight: 1.5,
              color: 'var(--text-muted)',
            }}>
              ClickHouse did not log object, column, or function metadata for this query row.
            </div>
          )}
        </Section>
      </div>

      <div style={{ ...PANEL, marginTop: 16, padding: 14 }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, minmax(0, 1fr))', gap: 12 }}>
          <Metric label="Duration" value={formatDurationMs(q.duration_ms)} />
          <Metric label="CPU" value={formatMicroseconds(q.cpu_us)} />
          <Metric label="Memory" value={formatBytes(q.peak_memory)} />
          <Metric label="Read" value={formatBytes(queryDetail?.read_bytes ?? q.disk_read)} />
          <Metric label="Network" value={formatBytes(q.net_recv + q.net_send)} />
          <Metric label="Cache" value={queryDetail?.query_cache_usage || '-'} />
        </div>
      </div>
    </div>
  );
};

const Metric: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
  <div style={{ minWidth: 0 }}>
    <div style={{ ...LABEL, marginBottom: 4 }}>{label}</div>
    <div style={{
      fontFamily: 'monospace',
      fontSize: 13,
      color: 'var(--text-primary)',
      overflow: 'hidden',
      textOverflow: 'ellipsis',
      whiteSpace: 'nowrap',
    }}>
      {value}
    </div>
  </div>
);
