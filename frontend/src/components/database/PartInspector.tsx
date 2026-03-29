/**
 * PartInspector - Reusable modal component for viewing part details
 * 
 * Extracted from DatabaseExplorer for reuse in:
 * - DatabaseExplorer (original location)
 * - TimeTravelPage (for viewing merge/mutation part details)
 * 
 * Features:
 * - Overview tab: Part stats, storage by column donut chart
 * - Columns tab: Detailed column information with compression ratios
 * - Data tab: Sample data from the part
 * - Lineage tab: Merge history visualization
 */

import React, { useState, useEffect, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { databaseApi, formatBytes } from '../../stores/databaseStore';
import type { PartDetailInfo, PartLineageInfo } from '../../stores/databaseStore';
import type { PartDataResponse } from '@tracehouse/core';
import { useThemeDetection } from '../../hooks/useThemeDetection';
import { LineageVisualization } from './LineageVisualization';

// Light theme CSS variables - applied as inline styles for portals
// This ensures CSS variables work correctly even when portal is outside main React tree
const LIGHT_THEME_VARS: React.CSSProperties = {
  '--bg-primary': '#f8fafc',
  '--bg-secondary': '#ffffff',
  '--bg-tertiary': '#f1f5f9',
  '--bg-card': 'rgba(0, 0, 0, 0.02)',
  '--bg-card-hover': 'rgba(0, 0, 0, 0.04)',
  '--bg-input': 'rgba(0, 0, 0, 0.03)',
  '--bg-modal': 'linear-gradient(180deg, #ffffff 0%, #f8fafc 100%)',
  '--bg-code': '#f6f8fa',
  '--bg-hover': 'rgba(0, 0, 0, 0.04)',
  '--border-primary': 'rgba(0, 0, 0, 0.12)',
  '--border-secondary': 'rgba(0, 0, 0, 0.06)',
  '--border-accent': 'rgba(37, 99, 235, 0.3)',
  '--border-input': 'rgba(0, 0, 0, 0.15)',
  '--text-primary': '#1e293b',
  '--text-secondary': '#475569',
  '--text-tertiary': '#64748b',
  '--text-muted': '#94a3b8',
  '--text-disabled': '#cbd5e1',
  '--accent-primary': '#7c3aed',
  '--accent-primary-rgb': '124, 58, 237',
  '--accent-secondary': '#8b5cf6',
  '--accent-secondary-rgb': '139, 92, 246',
  '--shadow-modal': '0 25px 50px -12px rgba(0, 0, 0, 0.25), 0 0 0 1px rgba(0, 0, 0, 0.05)',
  '--backdrop-overlay': 'rgba(0, 0, 0, 0.5)',
} as React.CSSProperties;

// ============================================================================
// CONSTANTS (Exported for reuse)
// ============================================================================

// Palette: violet → indigo → blue → teal → slate
export const COLUMN_COLORS = [
  '#8b5cf6', '#7c6be0', '#6d73c8', '#5e7db5', '#5085a5',
  '#478b98', '#3f8e88', '#4a8a78', '#6366b8', '#5a5e9e',
  '#4f5584', '#7e6dcc', '#6f65b2', '#5080a0', '#478890',
  '#3d8680', '#507e9a', '#5c6eaa', '#4a6080', '#3c4e68',
];

// ============================================================================
// LINEAGE VISUALIZATION & MERGE TIMELINE — now in separate files
// ============================================================================

// ============================================================================
// MAIN PART INSPECTOR COMPONENT
// ============================================================================

export interface PartInspectorProps {
  partDetail: PartDetailInfo | null;
  isLoading: boolean;
  onClose: () => void;
  breadcrumbPath?: string[];
  database?: string;
  table?: string;
  /** Base z-index for the portal (default 99998). Backdrop = base+1, modal = base+2. */
  zIndex?: number;
}

export const PartInspector: React.FC<PartInspectorProps> = ({
  partDetail,
  isLoading,
  onClose,
  breadcrumbPath = [],
  database,
  table,
  zIndex: baseZIndex = 99998,
}) => {
  const services = useClickHouseServices();
  const [activeTab, setActiveTab] = useState<'overview' | 'columns' | 'data' | 'lineage'>('overview');
  const [partData, setPartData] = useState<PartDataResponse | null>(null);
  const [isLoadingData, setIsLoadingData] = useState(false);
  const [dataError, setDataError] = useState<string | null>(null);
  
  const [lineageData, setLineageData] = useState<PartLineageInfo | null>(null);
  const [isLoadingLineage, setIsLoadingLineage] = useState(false);
  const [lineageError, setLineageError] = useState<string | null>(null);

  // Min/max column stats — fetched lazily on columns tab
  const [columnMinMax, setColumnMinMax] = useState<Map<string, { min: string; max: string }> | null>(null);
  const [isLoadingMinMax, setIsLoadingMinMax] = useState(false);
  
  // Fetch data when Data tab is selected
  useEffect(() => {
    if (activeTab === 'data' && partDetail && database && table && services && !partData && !isLoadingData) {
      setIsLoadingData(true);
      setDataError(null);
      databaseApi.fetchPartData(services.databaseExplorer, database, table, partDetail.name, 100)
        .then(data => {
          setPartData(data);
          setIsLoadingData(false);
        })
        .catch(err => {
          setDataError(err.message || 'Failed to fetch part data');
          setIsLoadingData(false);
        });
    }
  }, [activeTab, partDetail, database, table, services, partData, isLoadingData]);
  
  // Fetch min/max on demand (scans actual part data)
  const loadColumnMinMax = () => {
    if (!partDetail?.columns || !database || !table || !services || isLoadingMinMax) return;
    setIsLoadingMinMax(true);
    databaseApi.fetchPartColumnMinMax(
      services.databaseExplorer, database, table, partDetail.name,
      partDetail.columns.map(c => ({ column_name: c.column_name, type: c.type })),
    )
      .then(data => { setColumnMinMax(data); setIsLoadingMinMax(false); })
      .catch(() => { setIsLoadingMinMax(false); });
  };

  // Reset data when part changes
  useEffect(() => {
    setPartData(null);
    setDataError(null);
    setLineageData(null);
    setLineageError(null);
    setColumnMinMax(null);
  }, [partDetail?.name]);
  
  // Fetch lineage when Lineage tab is selected
  useEffect(() => {
    if (activeTab === 'lineage' && partDetail && database && table && services && !lineageData && !isLoadingLineage && !lineageError) {
      setIsLoadingLineage(true);
      setLineageError(null);
      databaseApi.fetchPartLineage(services.databaseExplorer, database, table, partDetail.name)
        .then(data => {
          setLineageData(data);
          setIsLoadingLineage(false);
        })
        .catch(err => {
          setLineageError(err.message || 'Failed to fetch lineage');
          setIsLoadingLineage(false);
        });
    }
  }, [activeTab, partDetail, database, table, services, lineageData, isLoadingLineage, lineageError]);
  
  // ESC key to close
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    
    if (partDetail || isLoading) {
      window.addEventListener('keydown', handleKeyDown);
      return () => window.removeEventListener('keydown', handleKeyDown);
    }
  }, [partDetail, isLoading, onClose]);
  
  // Prepare donut chart data
  const chartData = useMemo(() => {
    if (!partDetail) return [];
    return partDetail.columns
      .filter(c => c.compressed_bytes > 0)
      .sort((a, b) => b.compressed_bytes - a.compressed_bytes)
      .slice(0, 15)
      .map((col, i) => ({
        name: col.column_name,
        value: col.compressed_bytes,
        color: COLUMN_COLORS[i % COLUMN_COLORS.length],
      }));
  }, [partDetail]);
  
  const othersSize = useMemo(() => {
    if (!partDetail) return 0;
    const top15Total = chartData.reduce((sum, d) => sum + d.value, 0);
    return partDetail.data_compressed_bytes - top15Total;
  }, [partDetail, chartData]);
  
  const finalChartData = useMemo(() => {
    if (othersSize > 0) {
      return [...chartData, { name: 'Others', value: othersSize, color: '#374151' }];
    }
    return chartData;
  }, [chartData, othersSize]);
  
  // Get current theme for portal - portals need explicit theme context
  // MUST be called before any early returns to satisfy React's rules of hooks
  const currentTheme = useThemeDetection();
  
  if (!partDetail && !isLoading) return null;
  
  // Apply light theme CSS variables as inline styles for portal
  // This ensures variables work correctly even when portal is outside main React tree
  const portalStyle: React.CSSProperties = {
    position: 'fixed',
    inset: 0,
    zIndex: baseZIndex,
    ...(currentTheme === 'light' ? LIGHT_THEME_VARS : {}),
  };
  
  return createPortal(
    <div 
      data-theme={currentTheme} 
      className={currentTheme === 'light' ? 'theme-light' : 'theme-dark'}
      style={portalStyle}
    >
      {/* Inject theme styles for portal - ensures CSS variables work correctly */}
      {currentTheme === 'light' && (
        <style>{`
          .theme-light, .theme-light * {
            --bg-primary: #f8fafc;
            --bg-secondary: #ffffff;
            --bg-tertiary: #f1f5f9;
            --bg-card: rgba(0, 0, 0, 0.02);
            --bg-card-hover: rgba(0, 0, 0, 0.04);
            --bg-input: rgba(0, 0, 0, 0.03);
            --bg-modal: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
            --bg-code: #f6f8fa;
            --bg-hover: rgba(0, 0, 0, 0.04);
            --border-primary: rgba(0, 0, 0, 0.12);
            --border-secondary: rgba(0, 0, 0, 0.06);
            --border-accent: rgba(37, 99, 235, 0.3);
            --border-input: rgba(0, 0, 0, 0.15);
            --text-primary: #1e293b;
            --text-secondary: #475569;
            --text-tertiary: #64748b;
            --text-muted: #94a3b8;
            --text-disabled: #cbd5e1;
            --accent-primary: #7c3aed;
            --accent-primary-rgb: 124, 58, 237;
            --accent-secondary: #8b5cf6;
            --accent-secondary-rgb: 139, 92, 246;
            --shadow-modal: 0 25px 50px -12px rgba(0, 0, 0, 0.25), 0 0 0 1px rgba(0, 0, 0, 0.05);
            --backdrop-overlay: rgba(0, 0, 0, 0.5);
          }
        `}</style>
      )}
      {/* Breadcrumb */}
      {breadcrumbPath.length > 0 && (
        <div
          className="fixed left-6 pointer-events-auto"
          style={{ zIndex: 100001, top: '72px' }}
        >
          <div style={{
            fontFamily: 'monospace',
            display: 'flex',
            alignItems: 'baseline',
            whiteSpace: 'nowrap',
            background: 'var(--bg-tertiary)',
            padding: '6px 14px',
            borderRadius: '6px',
            border: '1px solid var(--border-primary)',
            backdropFilter: 'blur(8px)',
          }}>
            {breadcrumbPath.map((item, i) => {
              const isLast = i === breadcrumbPath.length - 1;
              return (
                <span key={i} style={{ display: 'inline-flex', alignItems: 'baseline' }}>
                  {i > 0 && (
                    <span style={{ color: 'var(--text-muted)', margin: '0 8px', fontSize: '12px' }}>›</span>
                  )}
                  <span style={{
                    color: isLast ? 'var(--text-primary)' : 'var(--text-tertiary)',
                    fontSize: isLast ? '13px' : '11px',
                    fontWeight: isLast ? 600 : 400,
                  }}>
                    {item}
                  </span>
                </span>
              );
            })}
          </div>
        </div>
      )}
      
      {/* Backdrop */}
      <div 
        className="fixed inset-0"
        style={{ zIndex: baseZIndex + 1, background: 'var(--backdrop-overlay)' }}
        onClick={onClose}
      />
      
      {/* Modal */}
      <div 
        className="fixed inset-0 flex items-center justify-center pointer-events-none"
        style={{ zIndex: baseZIndex + 2, padding: '16px' }}
      >
        <div
          className="rounded-xl w-[980px] h-[75vh] flex flex-col overflow-hidden pointer-events-auto"
          style={{
            background: 'var(--bg-secondary)',
            border: '1px solid var(--border-primary)',
            boxShadow: '0 25px 50px rgba(0,0,0,0.5)',
            marginTop: '60px',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          {/* Header */}
          <div style={{ padding: '16px 20px 12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-primary)' }}>
            <div>
              <h2 style={{ fontSize: '15px', fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
                {partDetail?.name || 'Loading...'}
              </h2>
            </div>
            <button onClick={onClose} style={{ color: 'var(--text-muted)', background: 'none', border: 'none', cursor: 'pointer', fontSize: 18, padding: '4px 8px' }}>✕</button>
          </div>

          {/* Tabs */}
          <div style={{ display: 'flex', borderBottom: '1px solid var(--border-primary)', padding: '0 20px' }}>
            {(['overview', 'lineage', 'columns', 'data'] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                style={{
                  padding: '10px 14px',
                  fontSize: '12px',
                  fontWeight: activeTab === tab ? 600 : 400,
                  color: activeTab === tab ? 'var(--accent-primary)' : 'var(--text-secondary)',
                  background: 'none',
                  border: 'none',
                  borderBottom: activeTab === tab ? '2px solid var(--accent-primary)' : '2px solid transparent',
                  cursor: 'pointer',
                }}
              >
                {tab === 'overview' ? 'Overview' : tab === 'columns' ? 'Columns' : tab === 'data' ? 'Data' : 'Lineage'}
              </button>
            ))}
          </div>
          
          {/* Content */}
          <div style={{ flex: 1, overflow: 'auto', padding: 20 }}>
            {isLoading ? (
              <div className="h-full flex items-center justify-center">
                <div className="animate-spin w-8 h-8 border-2 border-purple-500/30 border-t-purple-500 rounded-full" />
              </div>
            ) : partDetail && activeTab === 'overview' ? (
              <OverviewTab partDetail={partDetail} chartData={finalChartData} />
            ) : partDetail && activeTab === 'columns' ? (
              <ColumnsTab partDetail={partDetail} columnMinMax={columnMinMax} isLoadingMinMax={isLoadingMinMax} onLoadMinMax={loadColumnMinMax} />
            ) : partDetail && activeTab === 'data' ? (
              <DataTab 
                partData={partData} 
                isLoading={isLoadingData} 
                error={dataError} 
              />
            ) : partDetail && activeTab === 'lineage' ? (
              <LineageTab 
                lineageData={lineageData}
                isLoading={isLoadingLineage}
                error={lineageError}
                onRetry={() => {
                  setLineageError(null);
                  setLineageData(null);
                }}
              />
            ) : null}
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
};


// ============================================================================
// TAB COMPONENTS (Exported for reuse in TimelinePartModal)
// ============================================================================

export const OverviewTab: React.FC<{ 
  partDetail: PartDetailInfo; 
  chartData: { name: string; value: number; color: string }[];
}> = ({ partDetail, chartData }) => {
  const [hoveredCol, setHoveredCol] = useState<number | null>(null);
  const total = chartData.reduce((sum, d) => sum + d.value, 0);

  return (
    <div>
      {/* Stats Row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '12px', marginBottom: '28px' }}>
        {[
          { label: 'Rows', value: partDetail.rows.toLocaleString() },
          { label: 'Compressed', value: formatBytes(partDetail.data_compressed_bytes) },
          { label: 'Uncompressed', value: formatBytes(partDetail.data_uncompressed_bytes) },
          { label: 'Ratio', value: `${partDetail.compression_ratio.toFixed(1)}x` },
          { label: 'Format', value: partDetail.part_type || 'Wide' },
        ].map(({ label, value }) => (
          <div key={label} style={{ borderRadius: 8, padding: 12, background: 'var(--bg-tertiary)' }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{label}</div>
            <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 13 }}>{value}</div>
          </div>
        ))}
      </div>

      {/* Two Column Layout */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '40px' }}>
        {/* Left: Info Sections */}
        <div>
          {partDetail.partition_key && (
            <div style={{ paddingBottom: '20px', marginBottom: '20px', borderBottom: '1px solid var(--border-secondary)' }}>
              <h3 style={{ fontSize: '9px', color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '2px', marginBottom: '12px' }}>Table Schema</h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {partDetail.partition_key && (
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: '12px' }}>
                    <span style={{ color: 'var(--text-muted)', fontSize: '11px', flexShrink: 0 }}>Partition</span>
                    <code style={{ color: 'var(--text-secondary)', fontFamily: 'monospace', fontSize: '11px' }}>{partDetail.partition_key}</code>
                  </div>
                )}
                {partDetail.sorting_key && (
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: '12px' }}>
                    <span style={{ color: 'var(--text-muted)', fontSize: '11px', flexShrink: 0 }}>Order By</span>
                    <code style={{ color: 'var(--text-secondary)', fontFamily: 'monospace', fontSize: '11px' }}>{partDetail.sorting_key}</code>
                  </div>
                )}
              </div>
            </div>
          )}

          <div style={{ paddingBottom: '20px', marginBottom: '20px', borderBottom: '1px solid var(--border-secondary)' }}>
            <h3 style={{ fontSize: '9px', color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '2px', marginBottom: '12px' }}>Date Range</h3>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', fontSize: '12px' }}>
              <span style={{ color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{partDetail.min_date}</span>
              <span style={{ color: 'var(--text-muted)' }}>→</span>
              <span style={{ color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{partDetail.max_date}</span>
            </div>
          </div>

          <div style={{ paddingBottom: '20px', marginBottom: '20px', borderBottom: '1px solid var(--border-secondary)' }}>
            <h3 style={{ fontSize: '9px', color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '2px', marginBottom: '12px' }}>Part Info</h3>
            <div style={{ display: 'flex', gap: '40px' }}>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px' }}>
                <span style={{ fontSize: '18px', color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{partDetail.marks_count.toLocaleString()}</span>
                <span style={{ fontSize: '10px', color: 'var(--text-muted)' }}>marks</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px' }}>
                <span style={{ fontSize: '18px', color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{partDetail.columns.length}</span>
                <span style={{ fontSize: '10px', color: 'var(--text-muted)' }}>columns</span>
              </div>
            </div>
          </div>

          <div>
            <h3 style={{ fontSize: '9px', color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '2px', marginBottom: '12px' }}>Storage Location</h3>
            <div style={{ fontSize: '10px', color: 'var(--text-muted)', fontFamily: 'monospace', wordBreak: 'break-all', lineHeight: '1.5' }}>
              <span style={{ color: 'var(--text-disabled)' }}>{partDetail.disk_name}:</span> {partDetail.path}
            </div>
          </div>
        </div>

        {/* Right: Storage Breakdown */}
        <div>
          <h3 style={{ fontSize: '9px', color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: '2px', marginBottom: '14px' }}>Storage by Column</h3>
          
          {/* Stacked bar */}
          <div style={{ height: '18px', borderRadius: '4px', overflow: 'hidden', display: 'flex', marginBottom: '14px', background: 'var(--bg-card)', border: '1px solid var(--border-secondary)' }}>
            {chartData.map((d, i) => {
              const pct = total > 0 ? (d.value / total) * 100 : 0;
              if (pct < 0.5) return null;
              return (
                <div
                  key={i}
                  style={{
                    width: `${pct}%`,
                    background: d.color,
                    opacity: hoveredCol === null || hoveredCol === i ? 1 : 0.3,
                    transition: 'opacity 0.15s ease',
                    cursor: 'pointer',
                  }}
                  onMouseEnter={() => setHoveredCol(i)}
                  onMouseLeave={() => setHoveredCol(null)}
                  title={`${d.name}: ${formatBytes(d.value)} (${pct.toFixed(1)}%)`}
                />
              );
            })}
          </div>

          {/* Column list */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1px', maxHeight: '260px', overflowY: 'auto' }}>
            {chartData.map((d, i) => {
              const pct = total > 0 ? (d.value / total) * 100 : 0;
              return (
                <div
                  key={i}
                  style={{
                    display: 'grid',
                    gridTemplateColumns: '8px 1fr auto auto',
                    gap: '8px',
                    alignItems: 'center',
                    padding: '5px 8px',
                    borderRadius: '4px',
                    background: hoveredCol === i ? 'var(--bg-hover)' : 'transparent',
                    cursor: 'pointer',
                    transition: 'background 0.15s ease',
                  }}
                  onMouseEnter={() => setHoveredCol(i)}
                  onMouseLeave={() => setHoveredCol(null)}
                >
                  <div style={{ width: 8, height: 8, borderRadius: 2, background: d.color }} />
                  <span style={{ fontSize: '11px', fontFamily: 'monospace', color: hoveredCol === i ? 'var(--text-primary)' : 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {d.name}
                  </span>
                  <span style={{ fontSize: '10px', fontFamily: 'monospace', color: 'var(--text-muted)', textAlign: 'right' }}>
                    {formatBytes(d.value)}
                  </span>
                  <span style={{ fontSize: '10px', fontFamily: 'monospace', color: 'var(--text-muted)', width: '36px', textAlign: 'right' }}>
                    {pct.toFixed(0)}%
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
};


type SortField = 'default' | 'column_name' | 'type' | 'raw' | 'compressed' | 'ratio';
type SortDir = 'asc' | 'desc';

export const ColumnsTab: React.FC<{
  partDetail: PartDetailInfo;
  columnMinMax?: Map<string, { min: string; max: string }> | null;
  isLoadingMinMax?: boolean;
  onLoadMinMax?: () => void;
}> = ({ partDetail, columnMinMax, isLoadingMinMax, onLoadMinMax }) => {
  const [sortField, setSortField] = useState<SortField>('default');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const handleSort = (field: SortField) => {
    if (field === sortField) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setSortField(field);
      setSortDir(field === 'column_name' || field === 'type' ? 'asc' : 'desc');
    }
  };

  const sortIndicator = (field: SortField) =>
    sortField === field ? (sortDir === 'desc' ? ' ↓' : ' ↑') : '';

  // Uniform muted style for all type badges
  const typeStyle = { color: 'var(--text-secondary)', background: 'var(--bg-tertiary)' };

  const simplifyType = (type: string) => {
    return type.replace('LowCardinality(', 'LC(').replace('Nullable(', 'N(');
  };

  const sortedColumns = useMemo(() => {
    const cols = [...partDetail.columns];
    if (sortField === 'default') {
      return cols.sort((a, b) => {
        const aScore = (a.is_in_partition_key ? 1000 : 0) + (a.is_in_sorting_key ? 100 : 0) + (a.is_in_primary_key ? 10 : 0);
        const bScore = (b.is_in_partition_key ? 1000 : 0) + (b.is_in_sorting_key ? 100 : 0) + (b.is_in_primary_key ? 10 : 0);
        if (aScore !== bScore) return bScore - aScore;
        return b.compressed_bytes - a.compressed_bytes;
      });
    }
    const dir = sortDir === 'asc' ? 1 : -1;
    return cols.sort((a, b) => {
      switch (sortField) {
        case 'column_name': return dir * a.column_name.localeCompare(b.column_name);
        case 'type': return dir * a.type.localeCompare(b.type);
        case 'raw': return dir * (a.uncompressed_bytes - b.uncompressed_bytes);
        case 'compressed': return dir * (a.compressed_bytes - b.compressed_bytes);
        case 'ratio': return dir * (a.compression_ratio - b.compression_ratio);
        default: return 0;
      }
    });
  }, [partDetail.columns, sortField, sortDir]);
  
  const showMinMax = !!(columnMinMax || isLoadingMinMax);
    const gridCols = showMinMax
      ? 'grid-cols-[minmax(100px,180px)_100px_55px_75px_75px_50px_45px_minmax(80px,1fr)_minmax(60px,120px)_minmax(60px,120px)]'
      : 'grid-cols-[minmax(100px,180px)_100px_55px_75px_75px_50px_45px_minmax(80px,1fr)]';

    return (<>
    <div>
      <div className="rounded-t-lg" style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderBottom: 'none' }}>
        <div className={`grid ${gridCols} gap-2 px-4 py-3 text-[10px] uppercase tracking-wider font-mono select-none`} style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border-secondary)' }}>
          <div style={{ cursor: 'pointer' }} onClick={() => handleSort('column_name')}>Column{sortIndicator('column_name')}</div>
          <div style={{ cursor: 'pointer' }} onClick={() => handleSort('type')}>Type{sortIndicator('type')}</div>
          <div>Codec</div>
          <div className="text-right" style={{ cursor: 'pointer' }} onClick={() => handleSort('raw')}>Raw{sortIndicator('raw')}</div>
          <div className="text-right" style={{ cursor: 'pointer' }} onClick={() => handleSort('compressed')}>Compressed{sortIndicator('compressed')}</div>
          <div className="text-right" style={{ cursor: 'pointer' }} onClick={() => handleSort('ratio')}>Ratio{sortIndicator('ratio')}</div>
          <div>Keys</div>
          <div>Size</div>
          {showMinMax && <div className="text-right">Min</div>}
          {showMinMax && <div className="text-right">Max</div>}
        </div>
      </div>
    
      <div className="rounded-b-lg overflow-hidden" style={{ border: '1px solid var(--border-secondary)', borderTop: 'none' }}>
        {sortedColumns.map((col, i) => {
          const pct = partDetail.data_compressed_bytes > 0
            ? (col.compressed_bytes / partDetail.data_compressed_bytes) * 100
            : 0;
        
          return (
            <div
              key={i}
              className={`grid ${gridCols} gap-2 px-4 py-2.5 items-center transition-colors`}
              style={{ 
                background: i % 2 === 0 ? 'var(--bg-card)' : 'transparent',
              }}
              onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-card-hover)'}
              onMouseLeave={(e) => e.currentTarget.style.background = i % 2 === 0 ? 'var(--bg-card)' : 'transparent'}
            >
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-mono text-xs truncate" style={{ color: 'var(--text-primary)' }} title={col.column_name}>
                  {col.column_name}
                </span>
              </div>
              
              <div className="min-w-0">
                <span
                  className="text-[10px] font-mono px-1.5 py-0.5 rounded truncate block"
                  style={typeStyle}
                  title={col.type}
                >
                  {simplifyType(col.type)}
                </span>
              </div>
              
              <div>
                <span className="text-[10px] font-mono px-1.5 py-0.5 rounded" style={{ color: 'var(--text-muted)', background: 'var(--bg-tertiary)' }}>
                  {col.codec ? col.codec.replace('CODEC(', '').replace(')', '') : 'LZ4'}
                </span>
              </div>
              
              <div className="text-right">
                <span className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>{formatBytes(col.uncompressed_bytes)}</span>
              </div>
              
              <div className="text-right">
                <span className="text-xs font-mono" style={{ color: 'var(--text-secondary)' }}>{formatBytes(col.compressed_bytes)}</span>
              </div>
              
              <div className="text-right">
                <span className="text-[10px] font-mono" style={{ color: 'var(--text-secondary)' }}>
                  {col.compression_ratio.toFixed(1)}x
                </span>
              </div>
              
              <div className="flex items-center gap-0.5">
                {col.is_in_partition_key && (
                  <span className="w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold" style={{ background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }} title="Partition Key">P</span>
                )}
                {col.is_in_sorting_key && (
                  <span className="w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold" style={{ background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }} title="Sort Key">S</span>
                )}
                {col.is_in_primary_key && (
                  <span className="w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold" style={{ background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }} title="Primary Key">K</span>
                )}
                {!col.is_in_partition_key && !col.is_in_sorting_key && !col.is_in_primary_key && (
                  <span className="text-[10px]" style={{ color: 'var(--text-disabled)' }}>—</span>
                )}
              </div>

              <div className="relative h-5 rounded overflow-hidden" style={{ background: 'var(--bg-tertiary)' }}>
                <div
                  className="absolute inset-y-0 left-0 rounded"
                  style={{
                    width: `${Math.max(pct, 2)}%`,
                    background: 'var(--accent-primary)',
                    opacity: 0.15 + Math.min(pct / 100, 1) * 0.55,
                  }}
                />
                <span className="absolute inset-0 flex items-center justify-end pr-2 text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
                  {pct.toFixed(1)}%
                </span>
              </div>

              {/* Min/Max values — only rendered when loaded or loading */}
              {showMinMax && (() => {
                const mm = columnMinMax?.get(col.column_name);
                if (isLoadingMinMax) return (
                  <>
                    <div className="text-right"><span className="text-[10px] font-mono" style={{ color: 'var(--text-disabled)' }}>...</span></div>
                    <div className="text-right"><span className="text-[10px] font-mono" style={{ color: 'var(--text-disabled)' }}>...</span></div>
                  </>
                );
                return (
                  <>
                    <div className="text-right truncate" title={mm?.min}>
                      <span className="text-[10px] font-mono" style={{ color: 'var(--text-secondary)' }}>{mm?.min ?? '—'}</span>
                    </div>
                    <div className="text-right truncate" title={mm?.max}>
                      <span className="text-[10px] font-mono" style={{ color: 'var(--text-secondary)' }}>{mm?.max ?? '—'}</span>
                    </div>
                  </>
                );
              })()}
            </div>
          );
        })}
      </div>
      
      <div className="mt-4 flex items-center justify-end gap-6 text-xs" style={{ color: 'var(--text-muted)' }}>
        <span className="flex items-center gap-1.5">
          <span className="w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold" style={{ background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }}>P</span>
          Partition
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold" style={{ background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }}>S</span>
          Sort Key
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold" style={{ background: 'var(--bg-tertiary)', color: 'var(--text-secondary)' }}>K</span>
          Primary
        </span>
      </div>
      {!showMinMax && onLoadMinMax && (
        <div style={{
          marginTop: 16, display: 'flex', alignItems: 'center', gap: 10,
          padding: '10px 14px', borderRadius: 8,
          background: 'var(--bg-tertiary)',
          border: '1px solid var(--border-primary)',
        }}>
          <button
            onClick={onLoadMinMax}
            style={{
              background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
              borderRadius: 6, padding: '6px 12px', cursor: 'pointer',
              color: 'var(--text-secondary)', fontSize: 11, fontWeight: 500,
              whiteSpace: 'nowrap', flexShrink: 0,
            }}
          >
            Load Min/Max
          </button>
          <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
            Reads actual part data to compute per-column min/max values. May be slow on large parts.
          </span>
        </div>
      )}
    </div>
  </>);
};


export const DataTab: React.FC<{ 
  partData: PartDataResponse | null; 
  isLoading: boolean; 
  error: string | null;
}> = ({ partData, isLoading, error }) => (
  <div style={{ height: '100%', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
    {isLoading ? (
      <div className="flex-1 flex items-center justify-center">
        <div className="animate-spin w-8 h-8 border-2 border-purple-500/30 border-t-purple-500 rounded-full" />
      </div>
    ) : error ? (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-400 text-sm mb-2">Failed to load data</div>
          <div className="text-xs" style={{ color: 'var(--text-muted)' }}>{error}</div>
        </div>
      </div>
    ) : partData ? (
      <>
        <div style={{ marginBottom: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
            Showing <span style={{ color: '#a78bfa', fontWeight: 500 }}>{partData.returned_rows}</span> of <span style={{ color: 'var(--text-secondary)' }}>{partData.total_rows_in_part.toLocaleString()}</span> rows
          </div>
          <div style={{ fontSize: '10px', color: 'var(--text-muted)' }}>
            {partData.columns.length} columns
          </div>
        </div>
        
        <div style={{ flex: 1, overflow: 'auto', borderRadius: '8px', border: '1px solid var(--border-secondary)' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
            <thead>
              <tr style={{ position: 'sticky', top: 0, zIndex: 10 }}>
                {partData.columns.map((col, i) => (
                  <th 
                    key={i}
                    style={{
                      padding: '10px 12px',
                      textAlign: 'left',
                      color: 'var(--text-tertiary)',
                      fontWeight: 500,
                      fontSize: '10px',
                      textTransform: 'uppercase',
                      letterSpacing: '1px',
                      borderBottom: '1px solid var(--border-primary)',
                      whiteSpace: 'nowrap',
                      background: 'var(--bg-tertiary)',
                    }}
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {partData.rows.map((row, rowIdx) => (
                <tr 
                  key={rowIdx}
                  style={{ 
                    background: rowIdx % 2 === 0 ? 'transparent' : 'var(--bg-card)',
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = 'rgba(124, 58, 237, 0.1)'}
                  onMouseLeave={(e) => e.currentTarget.style.background = rowIdx % 2 === 0 ? 'transparent' : 'var(--bg-card)'}
                >
                  {row.map((cell, cellIdx) => (
                    <td 
                      key={cellIdx}
                      style={{
                        padding: '8px 12px',
                        color: cell === null ? 'var(--text-disabled)' : 'var(--text-secondary)',
                        fontFamily: 'monospace',
                        fontSize: '11px',
                        borderBottom: '1px solid var(--border-secondary)',
                        maxWidth: '200px',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                      title={cell !== null ? String(cell) : 'NULL'}
                    >
                      {cell === null ? 'NULL' : String(cell)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </>
    ) : (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-sm" style={{ color: 'var(--text-muted)' }}>No data available</div>
      </div>
    )}
  </div>
);

export const LineageTab: React.FC<{ 
  lineageData: PartLineageInfo | null;
  isLoading: boolean;
  error: string | null;
  onRetry: () => void;
}> = ({ lineageData, isLoading, error, onRetry }) => (
  <div style={{ padding: '16px 24px 32px 24px', overflowX: 'hidden' }}>
    {isLoading ? (
      <div style={{ height: '400px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div className="animate-spin w-8 h-8 border-2 border-purple-500/30 border-t-purple-500 rounded-full" />
      </div>
    ) : error ? (
      <div style={{ height: '400px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div className="text-center">
          <div className="text-red-400 text-sm mb-2">Failed to load lineage</div>
          <div className="text-xs mb-4" style={{ color: 'var(--text-muted)' }}>{error}</div>
          <button
            onClick={onRetry}
            className="px-4 py-2 bg-purple-500/20 hover:bg-purple-500/30 border border-purple-500/40 rounded text-purple-300 text-sm transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    ) : lineageData ? (
      <LineageVisualization lineage={lineageData} />
    ) : (
      <div style={{ height: '400px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div className="text-sm" style={{ color: 'var(--text-muted)' }}>No lineage data available</div>
      </div>
    )}
  </div>
);

export default PartInspector;
