/**
 * QueryScanEfficiency - Visual breakdown of a SELECT query's logical data flow.
 *
 * Each stage (columns, partitions, parts, granules, rows) is shown as an
 * independent card with a block grid: filled blocks = selected, empty = pruned.
 * SVG taper connectors link the stages to hint at the flow.
 */

import React, { useMemo, useEffect, useState, useCallback } from 'react';
import { calculatePruning, formatPruningDetail, type QueryDetail } from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

const fmtBytes = (b: number): string => {
  if (b === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log2(b) / 10), units.length - 1);
  return `${(b / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
};

const fmtNum = (n: number): string => {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
};

import { formatMicroseconds as fmtUs } from '../../utils/formatters';

/** Color for the % that passes through: lower = greener (more was filtered) */
const scanColor = (pct: number): string =>
  pct <= 5 ? '#3fb950' : pct <= 15 ? '#7ee787' : pct <= 40 ? '#d29922' : pct <= 70 ? '#f0883e' : '#f85149';

/* ------------------------------------------------------------------ */
/*  ScanEfficiency data model                                                 */
/* ------------------------------------------------------------------ */

export interface ScanEfficiencyData {
  tables: string[];
  columns: string[];
  allColumns: string[];
  partitions: string[];
  selectedParts: number; totalParts: number;
  selectedMarks: number; totalMarks: number;
  selectedRanges: number;
  readRows: number; readBytes: number;
  resultRows: number; resultBytes: number;
  aggregateFunctions: string[];
  functions: string[];
  pkFilterUs: number | null;
  skipIndexUs: number | null;
  markCacheHitRate: number | null;
  verdict: { label: string; color: string; detail: string; icon: string };
}

export function buildScanEfficiency(d: QueryDetail, allTableColumns: Record<string, string[]>): ScanEfficiencyData {
  const pe = d.ProfileEvents || {};
  const sp = pe['SelectedParts'] || 0;
  const spt = pe['SelectedPartsTotal'] || 0;
  const sm = pe['SelectedMarks'] || 0;
  const smt = pe['SelectedMarksTotal'] || 0;
  const sr = pe['SelectedRanges'] || 0;
  const mch = pe['MarkCacheHits'] || 0;
  const mcm = pe['MarkCacheMisses'] || 0;
  const mct = mch + mcm;

  const pruningInput = { selectedParts: sp, totalParts: spt, selectedMarks: sm, totalMarks: smt };
  const pruning = calculatePruning(pruningInput);
  const detail = formatPruningDetail(pruningInput, pruning);

  const VERDICT_MAP: Record<string, Omit<ScanEfficiencyData['verdict'], 'detail'>> = {
    excellent: { label: 'Excellent', color: '#3fb950', icon: '✓' },
    good:      { label: 'Good',      color: '#7ee787', icon: '○' },
    fair:      { label: 'Fair',      color: '#d29922', icon: '△' },
    poor:      { label: 'Poor',      color: '#f85149', icon: '✗' },
    none:      { label: 'N/A',       color: '#8b949e', icon: '?' },
  };
  const v = VERDICT_MAP[pruning.severity] ?? VERDICT_MAP.none;
  const verdict: ScanEfficiencyData['verdict'] = { ...v, detail };

  const allColsSet = new Set<string>();
  for (const cols of Object.values(allTableColumns)) {
    for (const c of cols) allColsSet.add(c);
  }
  const allColumns = allColsSet.size > 0 ? Array.from(allColsSet) : [];

  return {
    tables: d.tables || [],
    columns: d.columns || [],
    allColumns,
    partitions: d.partitions || [],
    selectedParts: sp, totalParts: spt,
    selectedMarks: sm, totalMarks: smt,
    selectedRanges: sr,
    readRows: d.read_rows, readBytes: d.read_bytes,
    resultRows: d.result_rows, resultBytes: d.result_bytes,
    aggregateFunctions: d.used_aggregate_functions || [],
    functions: d.used_functions || [],
    pkFilterUs: pe['FilteringMarksWithPrimaryKeyMicroseconds'] || null,
    skipIndexUs: pe['FilteringMarksWithSecondaryKeysMicroseconds'] || null,
    markCacheHitRate: mct > 0 ? (mch / mct) * 100 : null,
    verdict,
  };
}

/* ------------------------------------------------------------------ */
/*  Shared styles                                                      */
/* ------------------------------------------------------------------ */

const CARD: React.CSSProperties = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-secondary)',
  borderRadius: 10,
  padding: '14px 18px',
  marginBottom: 0,
};

const LABEL: React.CSSProperties = {
  fontSize: 10,
  fontWeight: 700,
  textTransform: 'uppercase',
  letterSpacing: '1.5px',
  color: 'var(--text-muted)',
  marginBottom: 6,
};

/* ------------------------------------------------------------------ */
/*  TaperConnector                                                     */
/* ------------------------------------------------------------------ */

const TaperConnector: React.FC<{ topWidth: number; bottomWidth: number; color: string; label?: string }> = ({
  topWidth, bottomWidth, color, label,
}) => {
  const tw = Math.max(4, Math.min(100, topWidth));
  const bw = Math.max(4, Math.min(100, bottomWidth));
  const tl = (100 - tw) / 2;
  const tr = tl + tw;
  const bl = (100 - bw) / 2;
  const br = bl + bw;
  const passPct = tw > 0 ? (bw / tw) * 100 : 100;
  const barW = Math.max(0.5, (passPct / 100) * 20);
  return (
    <div style={{ position: 'relative', height: 32, margin: '0 16px' }}>
      <svg width="100%" height="32" viewBox="0 0 100 32" preserveAspectRatio="none" style={{ display: 'block' }}>
        <polygon
          points={`${tl},0 ${tr},0 ${br},32 ${bl},32`}
          fill={color}
          opacity={0.18}
        />
        <rect x={50 - barW / 2} y={0} width={barW} height={32} fill={color} opacity={0.45} rx={0.3} />
      </svg>
      {label && (
        <div style={{
          position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)',
          fontSize: 9, color: 'var(--text-muted)', whiteSpace: 'nowrap',
          background: 'var(--bg-primary, #0d1117)', padding: '1px 8px', borderRadius: 3,
          letterSpacing: '0.5px', textTransform: 'uppercase',
        }}>
          {label}
        </div>
      )}
    </div>
  );
};

/** Column cell — a small labeled block, highlighted or dimmed */
const ColumnCell: React.FC<{ name: string; active: boolean; color: string }> = ({ name, active, color }) => (
  <div
    title={name}
    style={{
      padding: '4px 8px',
      borderRadius: 4,
      fontSize: 10,
      fontFamily: 'monospace',
      whiteSpace: 'nowrap',
      overflow: 'hidden',
      textOverflow: 'ellipsis',
      maxWidth: 140,
      background: active ? `${color}22` : 'var(--bg-tertiary)',
      border: `1px solid ${active ? `${color}55` : 'var(--border-primary)'}`,
      color: active ? color : 'var(--text-muted)',
      opacity: active ? 1 : 0.35,
      transition: 'all 0.3s ease',
    }}
  >
    {name}
  </div>
);

/**
 * BlockGrid — renders a grid of small squares where `selected` are lit and the rest are dimmed.
 * For large counts, it caps the visual blocks and shows a summary.
 */
const BlockGrid: React.FC<{
  selected: number;
  total: number;
  activeColor: string;
  maxBlocks?: number;
  blockSize?: number;
}> = ({ selected, total, activeColor, maxBlocks = 200, blockSize = 10 }) => {
  if (total === 0) return null;

  // If total is small enough, render 1:1 blocks
  // Otherwise, scale down so each block represents N items
  const scale = total <= maxBlocks ? 1 : total / maxBlocks;
  const visualTotal = Math.min(total, maxBlocks);
  const visualSelected = Math.round(selected / scale);
  const gap = blockSize <= 6 ? 1 : 2;

  return (
    <div>
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap,
        padding: 4,
        background: 'var(--bg-tertiary)',
        borderRadius: 6,
        border: '1px solid var(--border-primary)',
      }}>
        {Array.from({ length: visualTotal }, (_, i) => {
          const isActive = i < visualSelected;
          return (
            <div
              key={i}
              style={{
                width: blockSize,
                height: blockSize,
                borderRadius: blockSize <= 6 ? 1 : 2,
                background: activeColor,
                opacity: isActive ? 0.85 : 0.15,
                transition: 'opacity 0.2s, background 0.2s',
              }}
            />
          );
        })}
      </div>
      {scale > 1 && (
        <div style={{ fontSize: 9, color: 'var(--text-muted)', marginTop: 3, textAlign: 'right' }}>
          each block ≈ {fmtNum(Math.round(scale))} {scale >= 2 ? 'items' : 'item'}
        </div>
      )}
    </div>
  );
};

/** Data volume bar — shows bytes/rows as a proportional filled bar */
const VolumeBar: React.FC<{
  value: number;
  maxValue: number;
  label: string;
  sublabel: string;
  color: string;
}> = ({ value, maxValue, label, sublabel, color }) => {
  const pct = maxValue > 0 ? Math.max(2, (value / maxValue) * 100) : 0;
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
      <div style={{ width: 80, fontSize: 11, fontWeight: 600, color: 'var(--text-secondary)', textAlign: 'right', flexShrink: 0 }}>
        {label}
      </div>
      <div style={{ flex: 1, position: 'relative', height: 28, background: 'var(--bg-tertiary)', borderRadius: 4, overflow: 'hidden' }}>
        <div style={{
          width: `${pct}%`,
          height: '100%',
          background: color,
          opacity: 0.6,
          borderRadius: 4,
          transition: 'width 0.5s ease',
        }} />
        <div style={{
          position: 'absolute', top: 0, left: 8, right: 0, bottom: 0,
          display: 'flex', alignItems: 'center',
          fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)',
        }}>
          {sublabel}
        </div>
      </div>
    </div>
  );
};

/** Stat pill for the bottom metrics row */
const StatPill: React.FC<{ label: string; value: string; color?: string }> = ({ label, value, color }) => (
  <div style={{
    ...CARD,
    padding: '8px 12px',
    display: 'flex',
    flexDirection: 'column',
    gap: 2,
    minWidth: 0,
  }}>
    <div style={{ ...LABEL, marginBottom: 0 }}>{label}</div>
    <div style={{ fontSize: 15, fontWeight: 600, fontFamily: 'monospace', color: color || 'var(--text-primary)' }}>
      {value}
    </div>
  </div>
);

/* ------------------------------------------------------------------ */
/*  Stage sections                                                     */
/* ------------------------------------------------------------------ */

/** Stage header with count badge */
const StageHeader: React.FC<{
  title: string;
  selected: number;
  total: number;
  color: string;
  prunedLabel?: string;
}> = ({ title, selected, total, color, prunedLabel }) => {
  const pct = total > 0 ? (selected / total) * 100 : 0;
  const prunedPct = total > 0 ? 100 - pct : 0;
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ ...LABEL, marginBottom: 0 }}>{title}</span>
        <span style={{
          fontSize: 11, fontFamily: 'monospace', fontWeight: 600,
          color, padding: '1px 6px', borderRadius: 4,
          background: `${color}18`,
        }}>
          {fmtNum(selected)} / {fmtNum(total)}
        </span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{
          fontSize: 11, fontWeight: 700, fontFamily: 'monospace',
          color: scanColor(pct),
        }}>
          {prunedPct.toFixed(1)}% pruned
        </span>
        {prunedLabel && (
          <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>
            {prunedLabel}
          </span>
        )}
      </div>
    </div>
  );
};

/** Columns stage — shows each column as a named cell, selected highlighted, others dimmed */
const ColumnsStage: React.FC<{ columns: string[]; allColumns: string[] }> = ({ columns, allColumns }) => {
  if (columns.length === 0 && allColumns.length === 0) return null;

  // Build a set of selected column names (strip "table." prefix for matching)
  const selectedSet = new Set(columns.map(c => {
    const dot = c.lastIndexOf('.');
    return dot > 0 ? c.substring(dot + 1) : c;
  }));
  // Also keep full names for matching
  for (const c of columns) selectedSet.add(c);

  const hasTotal = allColumns.length > 0;
  const displayColumns = hasTotal ? allColumns : columns;
  const totalCount = hasTotal ? allColumns.length : columns.length;
  const selectedCount = hasTotal
    ? allColumns.filter(c => selectedSet.has(c)).length
    : columns.length;
  // If we couldn't match any (naming mismatch), fall back to showing query columns count
  const effectiveSelected = selectedCount > 0 ? selectedCount : columns.length;
  const skippedCount = hasTotal ? totalCount - effectiveSelected : 0;
  const skippedPct = hasTotal && totalCount > 0 ? (skippedCount / totalCount) * 100 : 0;

  return (
    <div style={CARD}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <span style={LABEL}>Columns</span>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{
            fontSize: 11, fontFamily: 'monospace', fontWeight: 600,
            color: '#d29922', padding: '1px 6px', borderRadius: 4,
            background: '#d2992218',
          }}>
            {effectiveSelected} / {totalCount}
          </span>
          {hasTotal && skippedPct > 0 && (
            <span style={{
              fontSize: 11, fontWeight: 700, fontFamily: 'monospace',
              color: scanColor(100 - skippedPct),
            }}>
              {skippedPct.toFixed(0)}% skipped
            </span>
          )}
        </div>
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
        {displayColumns.slice(0, 60).map((c, i) => {
          const isSelected = hasTotal ? selectedSet.has(c) : true;
          return <ColumnCell key={i} name={c} active={isSelected} color="#d29922" />;
        })}
        {displayColumns.length > 60 && (
          <div style={{
            padding: '4px 8px', borderRadius: 4, fontSize: 10,
            color: 'var(--text-muted)', background: 'var(--bg-tertiary)',
            border: '1px solid var(--border-primary)',
            display: 'flex', alignItems: 'center',
          }}>
            +{displayColumns.length - 60} more
          </div>
        )}
      </div>
    </div>
  );
};

/** Parts stage — block grid of parts */
const PartsStage: React.FC<{ selected: number; total: number }> = ({ selected, total }) => {
  if (total === 0) return null;
  const pct = (selected / total) * 100;
  return (
    <div style={CARD}>
      <StageHeader
        title="Parts"
        selected={selected}
        total={total}
        color={scanColor(pct)}
        prunedLabel="by partition key"
      />
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 6, fontFamily: 'monospace' }}>
        Selected {fmtNum(selected)}/{fmtNum(total)} parts by partition key
      </div>
      <BlockGrid
        selected={selected}
        total={total}
        activeColor={scanColor(pct)}
        maxBlocks={150}
        blockSize={12}
      />
    </div>
  );
}

/** Granules stage — dense block grid */
const GranulesStage: React.FC<{ selected: number; total: number; ranges: number }> = ({ selected, total, ranges }) => {
  if (total === 0) return null;
  const pct = (selected / total) * 100;
  return (
    <div style={CARD}>
      <StageHeader
        title="Marks"
        selected={selected}
        total={total}
        color={scanColor(pct)}
        prunedLabel="by primary key"
      />
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 6, fontFamily: 'monospace' }}>
        Selected {fmtNum(selected)}/{fmtNum(total)} marks by primary key
        {ranges > 0 && `, ${fmtNum(selected)} marks to read from ${fmtNum(ranges)} range${ranges !== 1 ? 's' : ''}`}
      </div>
      <BlockGrid
        selected={selected}
        total={total}
        activeColor={scanColor(pct)}
        maxBlocks={400}
        blockSize={6}
      />
    </div>
  );
}

/** Data I/O stage — rows read vs result with volume bars */
const DataStage: React.FC<{
  readRows: number; readBytes: number;
  resultRows: number; resultBytes: number;
}> = ({ readRows, readBytes, resultRows, resultBytes }) => {
  if (readRows === 0 && resultRows === 0) return null;
  const maxRows = Math.max(readRows, resultRows, 1);
  const scanRatio = resultRows > 0 ? readRows / resultRows : null;
  return (
    <div style={CARD}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
        <span style={LABEL}>Data Flow</span>
        {scanRatio !== null && scanRatio > 1 && (
          <span style={{
            fontSize: 11, fontFamily: 'monospace', fontWeight: 600,
            color: scanRatio > 100 ? '#f85149' : scanRatio > 10 ? '#d29922' : '#7ee787',
            padding: '1px 6px', borderRadius: 4,
            background: scanRatio > 100 ? '#f8514918' : scanRatio > 10 ? '#d2992218' : '#7ee78718',
          }}>
            {scanRatio.toFixed(1)}:1 scan ratio
          </span>
        )}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <VolumeBar value={readRows} maxValue={maxRows} label="Read" sublabel={`${fmtNum(readRows)} rows · ${fmtBytes(readBytes)}`} color="#58a6ff" />
        <VolumeBar value={resultRows} maxValue={maxRows} label="Result" sublabel={`${fmtNum(resultRows)} rows · ${fmtBytes(resultBytes)}`} color="#bc8cff" />
      </div>
    </div>
  );
};

/** Processing stage — functions used */
const ProcessingStage: React.FC<{ aggregates: string[]; functions: string[] }> = ({ aggregates, functions }) => {
  if (aggregates.length === 0 && functions.length === 0) return null;
  return (
    <div style={CARD}>
      <span style={{ ...LABEL, display: 'block', marginBottom: 8 }}>Processing</span>
      {aggregates.length > 0 && (
        <div style={{ marginBottom: functions.length > 0 ? 8 : 0 }}>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 4 }}>
            Aggregations ({aggregates.length})
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {aggregates.map((f, i) => <ColumnCell key={i} name={f} active color="#f0883e" />)}
          </div>
        </div>
      )}
      {functions.length > 0 && (
        <div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 4 }}>
            Functions ({functions.length})
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {functions.slice(0, 20).map((f, i) => <ColumnCell key={i} name={f} active color="#58a6ff" />)}
            {functions.length > 20 && (
              <div style={{ padding: '4px 8px', fontSize: 10, color: 'var(--text-muted)' }}>
                +{functions.length - 20} more
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

/* ------------------------------------------------------------------ */
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

interface QueryScanEfficiencyProps {
  queryDetail: QueryDetail | null;
  isLoading: boolean;
}

export const QueryScanEfficiency: React.FC<QueryScanEfficiencyProps> = ({ queryDetail, isLoading }) => {
  const services = useClickHouseServices();
  const [allTableColumns, setAllTableColumns] = useState<Record<string, string[]>>({});

  // Fetch total columns for the tables referenced by this query
  const fetchTableColumns = useCallback(async () => {
    if (!services || !queryDetail?.tables?.length) return;
    try {
      const result = await services.queryAnalyzer.getTableColumns(queryDetail.tables);
      setAllTableColumns(result);
    } catch (err) {
      console.warn('[QueryScanEfficiency] Failed to fetch table columns:', err);
    }
  }, [services, queryDetail?.tables]);

  useEffect(() => {
    setAllTableColumns({});
    fetchTableColumns();
  }, [fetchTableColumns]);

  const a = useMemo(
    () => (queryDetail ? buildScanEfficiency(queryDetail, allTableColumns) : null),
    [queryDetail, allTableColumns],
  );

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: 24, height: 24, borderWidth: 2, borderStyle: 'solid',
            borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)',
            borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px',
          }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Analyzing query…</span>
        </div>
      </div>
    );
  }

  if (!a || !queryDetail) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <div style={{ fontSize: 14, color: 'var(--text-tertiary)', marginBottom: 8 }}>No data available</div>
        <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 300, margin: '0 auto' }}>
          Query detail data is required for the scan_efficiency view.
        </div>
      </div>
    );
  }

  // Compute taper widths for connectors (percentage of "data remaining")
  const colsPct = a.allColumns.length > 0
    ? (a.columns.length / a.allColumns.length) * 100
    : 80;
  const partsPct = a.totalParts > 0 ? (a.selectedParts / a.totalParts) * 100 : 100;
  const marksPct = a.totalMarks > 0 ? (a.selectedMarks / a.totalMarks) * 100 : partsPct;
  // Result narrowing relative to read
  const resultPct = a.readRows > 0 ? Math.max(3, (a.resultRows / a.readRows) * 100) : marksPct;

  return (
    <div style={{ padding: 24, overflow: 'auto', height: '100%' }}>
      {/* ── Verdict banner ── */}
      <div style={{
        background: `${a.verdict.color}12`,
        border: `1px solid ${a.verdict.color}33`,
        borderRadius: 8,
        padding: '12px 16px',
        marginBottom: 20,
        display: 'flex', alignItems: 'center', gap: 12,
      }}>
        <div style={{
          width: 36, height: 36, borderRadius: '50%',
          background: `${a.verdict.color}22`,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: 14, fontWeight: 700, fontFamily: 'monospace',
          color: a.verdict.color, flexShrink: 0,
        }}>
          {a.verdict.icon}
        </div>
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: a.verdict.color, marginBottom: 2, display: 'flex', alignItems: 'center', gap: 8 }}>
            Index Effectiveness: {a.verdict.label}
            <span style={{
              fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '1.5px',
              padding: '2px 7px', borderRadius: 4,
              background: 'rgba(210, 153, 34, 0.2)', color: '#d29922',
            }}>Experimental</span>
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.4 }}>
            {a.verdict.detail}
          </div>
        </div>
      </div>

      {/* ── Tables ── */}
      {a.tables.length > 0 && (
        <div style={{ ...CARD, marginBottom: 0 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <span style={LABEL}>Tables</span>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {a.tables.map((t, i) => <ColumnCell key={i} name={t} active color="#3fb950" />)}
          </div>
        </div>
      )}

      {/* ── Columns ── */}
      <ColumnsStage columns={a.columns} allColumns={a.allColumns} />
      <TaperConnector topWidth={100} bottomWidth={Math.max(8, 100 - colsPct)} color={scanColor(colsPct)} label="column selection" />

      {/* ── Parts ── */}
      {a.totalParts > 0 && (
        <>
          <PartsStage selected={a.selectedParts} total={a.totalParts} />
          <TaperConnector topWidth={100} bottomWidth={Math.max(8, partsPct)} color={scanColor(partsPct)} label="partition pruning" />
        </>
      )}

      {/* ── Marks ── */}
      {a.totalMarks > 0 && (
        <>
          <GranulesStage selected={a.selectedMarks} total={a.totalMarks} ranges={a.selectedRanges} />
          <TaperConnector topWidth={100} bottomWidth={Math.max(5, marksPct)} color={scanColor(marksPct)} label="primary key pruning" />
        </>
      )}

      {/* ── Data Flow ── */}
      {(a.readRows > 0 || a.resultRows > 0) && (
        <>
          <DataStage
            readRows={a.readRows} readBytes={a.readBytes}
            resultRows={a.resultRows} resultBytes={a.resultBytes}
          />
          <TaperConnector topWidth={100} bottomWidth={Math.max(3, resultPct)} color="#58a6ff" label="read & aggregate" />
        </>
      )}

      {/* ── Processing ── */}
      {(a.aggregateFunctions.length > 0 || a.functions.length > 0) && (
        <ProcessingStage aggregates={a.aggregateFunctions} functions={a.functions} />
      )}

      {/* ── Bottom metrics ── */}
      {(a.pkFilterUs !== null || a.skipIndexUs !== null || a.markCacheHitRate !== null) && (
        <div style={{
          marginTop: 20,
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
          gap: 8,
        }}>
          {a.pkFilterUs !== null && (
            <StatPill label="PK Filter Time" value={fmtUs(a.pkFilterUs)} />
          )}
          {a.skipIndexUs !== null && (
            <StatPill label="Skip Index Time" value={fmtUs(a.skipIndexUs)} />
          )}
          {a.markCacheHitRate !== null && (
            <StatPill
              label="Mark Cache Hit"
              value={`${a.markCacheHitRate.toFixed(1)}%`}
              color={a.markCacheHitRate >= 80 ? '#3fb950' : a.markCacheHitRate >= 50 ? '#d29922' : '#f85149'}
            />
          )}
          {a.readRows > 0 && a.resultRows > 0 && (
            <StatPill
              label="Scan Ratio"
              value={`${(a.readRows / a.resultRows).toFixed(1)}:1`}
              color={a.readRows / a.resultRows > 100 ? '#f85149' : a.readRows / a.resultRows > 10 ? '#d29922' : '#3fb950'}
            />
          )}
        </div>
      )}
    </div>
  );
};

export default QueryScanEfficiency;
