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

// Vibrant color palette for donut chart
export const COLUMN_COLORS = [
  '#8b5cf6', '#6366f1', '#3b82f6', '#0ea5e9', '#06b6d4',
  '#14b8a6', '#10b981', '#22c55e', '#84cc16', '#eab308',
  '#f97316', '#ef4444', '#ec4899', '#d946ef', '#a855f7',
  '#64748b', '#475569', '#334155', '#1e293b', '#0f172a',
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
}

export const PartInspector: React.FC<PartInspectorProps> = ({ 
  partDetail, 
  isLoading, 
  onClose, 
  breadcrumbPath = [], 
  database, 
  table, 
}) => {
  const services = useClickHouseServices();
  const [activeTab, setActiveTab] = useState<'overview' | 'columns' | 'data' | 'lineage'>('overview');
  const [partData, setPartData] = useState<PartDataResponse | null>(null);
  const [isLoadingData, setIsLoadingData] = useState(false);
  const [dataError, setDataError] = useState<string | null>(null);
  
  const [lineageData, setLineageData] = useState<PartLineageInfo | null>(null);
  const [isLoadingLineage, setIsLoadingLineage] = useState(false);
  const [lineageError, setLineageError] = useState<string | null>(null);
  
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
  
  // Reset data when part changes
  useEffect(() => {
    setPartData(null);
    setDataError(null);
    setLineageData(null);
    setLineageError(null);
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
    zIndex: 99998,
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
            fontFamily: "'Orbitron', 'Rajdhani', 'Share Tech Mono', monospace",
            textTransform: 'uppercase',
            letterSpacing: '3px',
            display: 'flex',
            alignItems: 'baseline',
            whiteSpace: 'nowrap',
            background: 'var(--bg-tertiary)',
            padding: '8px 16px',
            borderRadius: '8px',
            border: '1px solid var(--border-primary)',
            backdropFilter: 'blur(8px)',
          }}>
            {breadcrumbPath.map((item, i) => {
              const isLast = i === breadcrumbPath.length - 1;
              const glowColor = isLast ? '#fbcfe8' : '#c4b5fd';
              const mainColor = isLast ? '#ec4899' : '#7c3aed';
              return (
                <span key={i} style={{ display: 'inline-flex', alignItems: 'baseline' }}>
                  {i > 0 && (
                    <span style={{
                      color: 'var(--text-muted)',
                      margin: '0 10px',
                      fontSize: '18px',
                    }}>
                      ›
                    </span>
                  )}
                  <span 
                    style={{
                      color: isLast ? glowColor : 'var(--text-tertiary)',
                      fontSize: isLast ? '24px' : '16px',
                      fontWeight: isLast ? 900 : 500,
                      textShadow: isLast 
                        ? `0 0 30px ${glowColor}, 0 0 60px ${mainColor}, 0 0 90px ${mainColor}40`
                        : 'none',
                      padding: '4px 8px',
                      borderRadius: '4px',
                    }}
                  >
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
        style={{ zIndex: 99999, background: 'var(--backdrop-overlay)' }}
        onClick={onClose}
      />
      
      {/* Modal */}
      <div 
        className="fixed inset-0 flex items-center justify-center pointer-events-none"
        style={{ zIndex: 100000, padding: '16px' }}
      >
        <div 
          className="rounded-2xl w-[980px] h-[75vh] flex flex-col overflow-hidden pointer-events-auto"
          style={{
            background: 'var(--bg-modal)',
            border: '1px solid var(--accent-primary)',
            boxShadow: 'var(--shadow-modal)',
            marginTop: '60px',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div 
            className="flex-shrink-0"
            style={{
              padding: '16px 32px 12px 32px',
              borderBottom: '1px solid var(--border-accent)',
              background: 'var(--bg-card)',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <h2 style={{ fontSize: '18px', fontWeight: 500, color: 'var(--text-primary)', letterSpacing: '0.5px', fontFamily: 'monospace' }}>
                {partDetail?.name || 'Loading...'}
              </h2>
              <button 
                onClick={onClose}
                style={{
                  padding: '6px',
                  borderRadius: '8px',
                  background: 'transparent',
                  border: 'none',
                  cursor: 'pointer',
                  color: 'var(--text-muted)',
                  transition: 'all 0.2s ease',
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = 'var(--bg-hover)';
                  e.currentTarget.style.color = 'var(--text-primary)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = 'transparent';
                  e.currentTarget.style.color = 'var(--text-muted)';
                }}
              >
                <svg style={{ width: '16px', height: '16px' }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            
            {/* Tabs */}
            <div style={{ display: 'flex', gap: '2px' }}>
              {(['overview', 'lineage', 'columns', 'data'] as const).map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  style={{
                    fontFamily: "'Orbitron', 'Rajdhani', monospace",
                    padding: '8px 16px',
                    fontSize: '10px',
                    textTransform: 'uppercase',
                    letterSpacing: '1.5px',
                    borderRadius: '6px 6px 0 0',
                    border: activeTab === tab ? '1px solid var(--border-accent)' : '1px solid transparent',
                    borderBottom: 'none',
                    background: activeTab === tab ? 'rgba(var(--accent-primary-rgb), 0.15)' : 'transparent',
                    color: activeTab === tab ? 'var(--text-primary)' : 'var(--text-tertiary)',
                    cursor: 'pointer',
                    transition: 'all 0.2s ease',
                    position: 'relative',
                    marginBottom: '-1px',
                  }}
                  onMouseEnter={(e) => {
                    if (activeTab !== tab) {
                      e.currentTarget.style.color = 'var(--text-primary)';
                      e.currentTarget.style.background = 'var(--bg-hover)';
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (activeTab !== tab) {
                      e.currentTarget.style.color = 'var(--text-tertiary)';
                      e.currentTarget.style.background = 'transparent';
                    }
                  }}
                >
                  {tab === 'overview' ? 'Overview' : tab === 'columns' ? 'Columns' : tab === 'data' ? 'Data' : 'Lineage'}
                  {activeTab === tab && (
                    <span style={{
                      position: 'absolute',
                      bottom: '0',
                      left: '0',
                      right: '0',
                      height: '2px',
                      background: 'linear-gradient(90deg, transparent, var(--accent-secondary), transparent)',
                    }} />
                  )}
                </button>
              ))}
            </div>
          </div>
          
          {/* Content */}
          <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, background: 'var(--bg-secondary)' }}>
            {isLoading ? (
              <div className="h-full flex items-center justify-center">
                <div className="animate-spin w-8 h-8 border-2 border-purple-500/30 border-t-purple-500 rounded-full" />
              </div>
            ) : partDetail && activeTab === 'overview' ? (
              <OverviewTab partDetail={partDetail} chartData={finalChartData} />
            ) : partDetail && activeTab === 'columns' ? (
              <ColumnsTab partDetail={partDetail} />
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
    <div style={{ padding: '32px 40px' }}>
      {/* Stats Row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: '12px', marginBottom: '28px' }}>
        <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: '6px', padding: '14px 16px' }}>
          <div style={{ fontSize: '9px', color: 'var(--text-muted)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>Rows</div>
          <div style={{ fontSize: '16px', color: 'var(--text-primary)', fontFamily: 'monospace' }}>{partDetail.rows.toLocaleString()}</div>
        </div>
        <div style={{ background: 'rgba(6, 182, 212, 0.05)', border: '1px solid rgba(6, 182, 212, 0.15)', borderRadius: '6px', padding: '14px 16px' }}>
          <div style={{ fontSize: '9px', color: 'rgba(6, 182, 212, 0.6)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>Compressed</div>
          <div style={{ fontSize: '16px', color: '#22d3ee', fontFamily: 'monospace' }}>{formatBytes(partDetail.data_compressed_bytes)}</div>
        </div>
        <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: '6px', padding: '14px 16px' }}>
          <div style={{ fontSize: '9px', color: 'var(--text-muted)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>Uncompressed</div>
          <div style={{ fontSize: '16px', color: 'var(--text-tertiary)', fontFamily: 'monospace' }}>{formatBytes(partDetail.data_uncompressed_bytes)}</div>
        </div>
        <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: '6px', padding: '14px 16px' }}>
          <div style={{ fontSize: '9px', color: 'var(--text-muted)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>Ratio</div>
          <div style={{ fontSize: '16px', fontFamily: 'monospace', color: partDetail.compression_ratio >= 5 ? '#34d399' : partDetail.compression_ratio >= 2 ? '#22d3ee' : '#fbbf24' }}>{partDetail.compression_ratio.toFixed(1)}x</div>
        </div>
        <div style={{ background: 'rgba(124, 58, 237, 0.05)', border: '1px solid rgba(124, 58, 237, 0.15)', borderRadius: '6px', padding: '14px 16px' }}>
          <div style={{ fontSize: '9px', color: 'rgba(167, 139, 250, 0.6)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '1px' }}>Format</div>
          <div style={{ fontSize: '16px', color: '#a78bfa', fontFamily: 'monospace' }}>{partDetail.part_type || 'Wide'}</div>
        </div>
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
                  <div style={{ width: 8, height: 8, borderRadius: 2, background: d.color, boxShadow: hoveredCol === i ? `0 0 6px ${d.color}` : 'none' }} />
                  <span style={{ fontSize: '11px', fontFamily: 'monospace', color: hoveredCol === i ? 'var(--text-primary)' : 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {d.name}
                  </span>
                  <span style={{ fontSize: '10px', fontFamily: 'monospace', color: 'var(--text-muted)', textAlign: 'right' }}>
                    {formatBytes(d.value)}
                  </span>
                  <span style={{ fontSize: '10px', fontFamily: 'monospace', color: d.color, fontWeight: 600, width: '36px', textAlign: 'right' }}>
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


export const ColumnsTab: React.FC<{ partDetail: PartDetailInfo }> = ({ partDetail }) => {
  const theme = useThemeDetection();
  const isLight = theme === 'light';

  const getTypeColor = (type: string) => {
    const t = type.toLowerCase();
    if (isLight) {
      if (t.includes('int') || t.includes('float') || t.includes('decimal')) return 'text-blue-700 bg-blue-100';
      if (t.includes('string') || t.includes('fixedstring')) return 'text-green-700 bg-green-100';
      if (t.includes('date') || t.includes('time')) return 'text-amber-700 bg-amber-100';
      if (t.includes('uuid')) return 'text-purple-700 bg-purple-100';
      if (t.includes('array') || t.includes('tuple') || t.includes('map')) return 'text-pink-700 bg-pink-100';
      if (t.includes('nullable')) return 'text-cyan-700 bg-cyan-100';
      if (t.includes('lowcardinality')) return 'text-emerald-700 bg-emerald-100';
    } else {
      if (t.includes('int') || t.includes('float') || t.includes('decimal')) return 'text-blue-400 bg-blue-500/10';
      if (t.includes('string') || t.includes('fixedstring')) return 'text-green-400 bg-green-500/10';
      if (t.includes('date') || t.includes('time')) return 'text-amber-400 bg-amber-500/10';
      if (t.includes('uuid')) return 'text-purple-400 bg-purple-500/10';
      if (t.includes('array') || t.includes('tuple') || t.includes('map')) return 'text-pink-400 bg-pink-500/10';
      if (t.includes('nullable')) return 'text-cyan-400 bg-cyan-500/10';
      if (t.includes('lowcardinality')) return 'text-emerald-400 bg-emerald-500/10';
    }
    return '';
  };
  
  // Default style for unknown types
  const defaultTypeStyle = { color: 'var(--text-tertiary)', background: 'var(--bg-hover)' };
  
  const simplifyType = (type: string) => {
    return type.replace('LowCardinality(', 'LC(').replace('Nullable(', 'N(');
  };
  
  const sortedColumns = [...partDetail.columns].sort((a, b) => {
    const aScore = (a.is_in_partition_key ? 1000 : 0) + (a.is_in_sorting_key ? 100 : 0) + (a.is_in_primary_key ? 10 : 0);
    const bScore = (b.is_in_partition_key ? 1000 : 0) + (b.is_in_sorting_key ? 100 : 0) + (b.is_in_primary_key ? 10 : 0);
    if (aScore !== bScore) return bScore - aScore;
    return b.compressed_bytes - a.compressed_bytes;
  });
  
  return (
    <div style={{ padding: '40px' }}>
      <div className="rounded-t-lg" style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderBottom: 'none' }}>
        <div className="grid grid-cols-[minmax(100px,180px)_100px_55px_75px_75px_50px_45px_minmax(80px,1fr)] gap-2 px-4 py-3 text-xs uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
          <div>Column</div>
          <div>Type</div>
          <div>Codec</div>
          <div className="text-right">Raw</div>
          <div className="text-right">Compressed</div>
          <div className="text-right">Ratio</div>
          <div>Keys</div>
          <div>Size</div>
        </div>
      </div>
    
      <div className="rounded-b-lg overflow-hidden" style={{ border: '1px solid var(--border-secondary)', borderTop: 'none' }}>
        {sortedColumns.map((col, i) => {
          const pct = partDetail.data_compressed_bytes > 0 
            ? (col.compressed_bytes / partDetail.data_compressed_bytes) * 100 
            : 0;
          const color = COLUMN_COLORS[i % COLUMN_COLORS.length];
        
          return (
            <div 
              key={i} 
              className="grid grid-cols-[minmax(100px,180px)_100px_55px_75px_75px_50px_45px_minmax(80px,1fr)] gap-2 px-4 py-2.5 items-center transition-colors"
              style={{ 
                background: i % 2 === 0 ? 'var(--bg-card)' : 'transparent',
              }}
              onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-card-hover)'}
              onMouseLeave={(e) => e.currentTarget.style.background = i % 2 === 0 ? 'var(--bg-card)' : 'transparent'}
            >
              <div className="flex items-center gap-2 min-w-0">
                <div className="w-1.5 h-1.5 rounded-sm flex-shrink-0" style={{ backgroundColor: color }} />
                <span className="font-mono text-xs truncate" style={{ color: 'var(--text-primary)' }} title={col.column_name}>
                  {col.column_name}
                </span>
              </div>
              
              <div className="min-w-0">
                <span 
                  className={`text-[10px] font-mono px-1.5 py-0.5 rounded truncate block ${getTypeColor(col.type)}`}
                  style={!getTypeColor(col.type) ? defaultTypeStyle : undefined}
                  title={col.type}
                >
                  {simplifyType(col.type)}
                </span>
              </div>
              
              <div>
                <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${isLight ? 'bg-purple-100 text-purple-700' : 'bg-purple-500/15 text-purple-300'}`}>
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
                <span className={`text-[10px] font-mono font-bold ${
                  col.compression_ratio > 5 ? (isLight ? 'text-green-700' : 'text-green-400') : 
                  col.compression_ratio > 2 ? (isLight ? 'text-cyan-700' : 'text-cyan-400') : 
                  col.compression_ratio > 1 ? (isLight ? 'text-amber-700' : 'text-amber-400') : (isLight ? 'text-red-700' : 'text-red-400')
                }`}>
                  {col.compression_ratio.toFixed(1)}x
                </span>
              </div>
              
              <div className="flex items-center gap-0.5">
                {col.is_in_partition_key && (
                  <span className={`w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold ${isLight ? 'bg-cyan-100 text-cyan-700' : 'bg-cyan-500/20 text-cyan-400'}`} title="Partition Key">P</span>
                )}
                {col.is_in_sorting_key && (
                  <span className={`w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold ${isLight ? 'bg-green-100 text-green-700' : 'bg-green-500/20 text-green-400'}`} title="Sort Key">S</span>
                )}
                {col.is_in_primary_key && (
                  <span className={`w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold ${isLight ? 'bg-amber-100 text-amber-700' : 'bg-amber-500/20 text-amber-400'}`} title="Primary Key">K</span>
                )}
                {!col.is_in_partition_key && !col.is_in_sorting_key && !col.is_in_primary_key && (
                  <span className="text-[10px]" style={{ color: 'var(--text-disabled)' }}>—</span>
                )}
              </div>
              
              <div className="relative h-5 rounded overflow-hidden" style={{ background: 'var(--bg-hover)' }}>
                <div 
                  className="absolute inset-y-0 left-0 rounded"
                  style={{ 
                    width: `${Math.max(pct, 2)}%`,
                    backgroundColor: color,
                    opacity: 0.6
                  }}
                />
                <span className="absolute inset-0 flex items-center justify-end pr-2 text-[10px] font-mono" style={{ color: 'var(--text-secondary)' }}>
                  {pct.toFixed(1)}%
                </span>
              </div>
            </div>
          );
        })}
      </div>
      
      <div className="mt-4 flex items-center justify-end gap-6 text-xs" style={{ color: 'var(--text-muted)' }}>
        <span className="flex items-center gap-1.5">
          <span className={`w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold ${isLight ? 'bg-cyan-100 text-cyan-700' : 'bg-cyan-500/20 text-cyan-400'}`}>P</span>
          Partition
        </span>
        <span className="flex items-center gap-1.5">
          <span className={`w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold ${isLight ? 'bg-green-100 text-green-700' : 'bg-green-500/20 text-green-400'}`}>S</span>
          Sort Key
        </span>
        <span className="flex items-center gap-1.5">
          <span className={`w-3.5 h-3.5 rounded flex items-center justify-center text-[8px] font-bold ${isLight ? 'bg-amber-100 text-amber-700' : 'bg-amber-500/20 text-amber-400'}`}>K</span>
          Primary
        </span>
      </div>
    </div>
  );
};


export const DataTab: React.FC<{ 
  partData: PartDataResponse | null; 
  isLoading: boolean; 
  error: string | null;
}> = ({ partData, isLoading, error }) => (
  <div style={{ padding: '40px', height: '100%', display: 'flex', flexDirection: 'column', minHeight: 0 }}>
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
