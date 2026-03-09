/**
 * Flamegraph component using d3-flame-graph for CPU/Memory profiling visualization
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { flamegraph } from 'd3-flame-graph';
import 'd3-flame-graph/dist/d3-flamegraph.css';
import type { FlamegraphNode } from '@tracehouse/core';
import { formatBytes } from '../../stores/databaseStore';

export type { FlamegraphNode } from '@tracehouse/core';

export type FlamegraphType = 'CPU' | 'Real' | 'Memory';

interface FlamegraphProps {
  data: FlamegraphNode | null;
  isLoading: boolean;
  error: string | null;
  onRefresh: (type: FlamegraphType) => void;
  profileType?: FlamegraphType;
  onTypeChange?: (type: FlamegraphType) => void;
}

// Header with profile type toggle - always visible
const FlamegraphHeader: React.FC<{
  profileType: FlamegraphType;
  onTypeChange?: (type: FlamegraphType) => void;
  onRefresh: (type: FlamegraphType) => void;
  data: FlamegraphNode | null;
  isInverted: boolean;
  setIsInverted: (v: boolean) => void;
  onReset: () => void;
  showControls: boolean;
}> = ({ profileType, onTypeChange, onRefresh, data, isInverted, setIsInverted, onReset, showControls }) => (
  <div style={{ 
    padding: '12px 16px',
    borderBottom: '1px solid var(--border-primary)',
    background: 'var(--bg-secondary)',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
  }}>
    <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
      {/* Profile type toggle */}
      {onTypeChange && (
        <div style={{ display: 'flex', gap: 1, background: 'var(--bg-tertiary)', borderRadius: 6, padding: 3 }}>
          <button
            onClick={() => onTypeChange('CPU')}
            style={{
              padding: '6px 14px',
              fontSize: 12,
              fontWeight: 500,
              borderRadius: 4,
              border: 'none',
              background: profileType === 'CPU' ? 'var(--bg-primary)' : 'transparent',
              color: profileType === 'CPU' ? 'var(--text-primary)' : 'var(--text-tertiary)',
              cursor: 'pointer',
              boxShadow: profileType === 'CPU' ? '0 1px 2px rgba(0,0,0,0.1)' : 'none',
            }}
          >
            CPU
          </button>
          <button
            onClick={() => onTypeChange('Real')}
            style={{
              padding: '6px 14px',
              fontSize: 12,
              fontWeight: 500,
              borderRadius: 4,
              border: 'none',
              background: profileType === 'Real' ? 'var(--bg-primary)' : 'transparent',
              color: profileType === 'Real' ? 'var(--text-primary)' : 'var(--text-tertiary)',
              cursor: 'pointer',
              boxShadow: profileType === 'Real' ? '0 1px 2px rgba(0,0,0,0.1)' : 'none',
            }}
            title="Wall-clock time (includes I/O waits)"
          >
            Real
          </button>
          <button
            onClick={() => onTypeChange('Memory')}
            style={{
              padding: '6px 14px',
              fontSize: 12,
              fontWeight: 500,
              borderRadius: 4,
              border: 'none',
              background: profileType === 'Memory' ? 'var(--bg-primary)' : 'transparent',
              color: profileType === 'Memory' ? 'var(--text-primary)' : 'var(--text-tertiary)',
              cursor: 'pointer',
              boxShadow: profileType === 'Memory' ? '0 1px 2px rgba(0,0,0,0.1)' : 'none',
            }}
          >
            Memory
          </button>
        </div>
      )}
      {showControls && data && data.value > 0 && (
        <>
          <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
            {profileType === 'Memory' 
              ? formatBytes(data.value)
              : `${data.value.toLocaleString()} samples`}
          </span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button
              onClick={() => setIsInverted(!isInverted)}
              style={{
                padding: '6px 12px',
                fontSize: 11,
                fontWeight: 500,
                borderRadius: 4,
                border: '1px solid var(--border-primary)',
                background: isInverted ? 'rgba(88, 166, 255, 0.2)' : 'var(--bg-tertiary)',
                color: isInverted ? '#2563eb' : 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              {isInverted ? '⇅ Icicle' : '⇅ Flame'}
            </button>
            <button
              onClick={onReset}
              style={{
                padding: '6px 12px',
                fontSize: 11,
                fontWeight: 500,
                borderRadius: 4,
                border: '1px solid var(--border-primary)',
                background: 'var(--bg-tertiary)',
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              ↻ Reset Zoom
            </button>
          </div>
        </>
      )}
    </div>
    <button
      onClick={() => onRefresh(profileType)}
      style={{
        padding: '6px 12px',
        fontSize: 11,
        borderRadius: 4,
        border: '1px solid var(--border-primary)',
        background: 'var(--bg-tertiary)',
        color: 'var(--text-secondary)',
        cursor: 'pointer',
      }}
    >
      Refresh
    </button>
  </div>
);

// Empty state content
const EmptyState: React.FC<{ profileType: FlamegraphType; onTypeChange?: (type: FlamegraphType) => void }> = ({ profileType, onTypeChange }) => {
  const isMemory = profileType === 'Memory';
  const isReal = profileType === 'Real';
  
  return (
    <div style={{ padding: 48, textAlign: 'center' }}>
      <div style={{ fontSize: 15, color: 'var(--text-tertiary)', marginBottom: 12 }}>
        No {profileType} profile data available
      </div>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 500, margin: '0 auto', lineHeight: 1.7 }}>
        {isMemory ? (
          <>
            Memory profiling requires <code style={{ background: 'var(--bg-tertiary)', padding: '2px 6px', borderRadius: 4 }}>memory_profiler_sample_probability</code> to be set.
            <pre style={{ 
              margin: '16px 0', 
              padding: 16, 
              background: 'var(--bg-tertiary)', 
              borderRadius: 8,
              textAlign: 'left',
              fontSize: 11,
              color: '#58a6ff',
              overflow: 'auto',
            }}>
{`-- Enable memory profiling
SET memory_profiler_sample_probability = 1;
SET max_untracked_memory = 1;

-- Then run your query
SELECT ...`}
            </pre>
          </>
        ) : isReal ? (
          <>
            Real (wall-clock) profiling shows where time is spent including I/O waits. It's useful for I/O-bound queries.
            <pre style={{ 
              margin: '16px 0', 
              padding: 16, 
              background: 'var(--bg-tertiary)', 
              borderRadius: 8,
              textAlign: 'left',
              fontSize: 11,
              color: '#58a6ff',
              overflow: 'auto',
            }}>
{`-- Enable real-time profiling (default: 1 sample/sec)
SET query_profiler_real_time_period_ns = 100000000;

-- Then run your query
SELECT ...`}
            </pre>
          </>
        ) : (
          <>
            CPU profiling is enabled by default (1 sample/sec). If no data appears, the query may have been too fast to capture samples.
            <pre style={{ 
              margin: '16px 0', 
              padding: 16, 
              background: 'var(--bg-tertiary)', 
              borderRadius: 8,
              textAlign: 'left',
              fontSize: 11,
              color: '#58a6ff',
              overflow: 'auto',
            }}>
{`-- For more granular CPU profiling (10ms intervals)
SET query_profiler_cpu_time_period_ns = 10000000;

-- Then run your query
SELECT ...`}
            </pre>
          </>
        )}
      </div>
      {onTypeChange && (
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', marginTop: 16 }}>
          {profileType !== 'CPU' && (
            <button
              onClick={() => onTypeChange('CPU')}
              style={{
                padding: '8px 20px',
                fontSize: 13,
                borderRadius: 6,
                border: '1px solid var(--border-primary)',
                background: 'var(--bg-tertiary)',
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              Try CPU
            </button>
          )}
          {profileType !== 'Real' && (
            <button
              onClick={() => onTypeChange('Real')}
              style={{
                padding: '8px 20px',
                fontSize: 13,
                borderRadius: 6,
                border: '1px solid var(--border-primary)',
                background: 'var(--bg-tertiary)',
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              Try Real
            </button>
          )}
          {profileType !== 'Memory' && (
            <button
              onClick={() => onTypeChange('Memory')}
              style={{
                padding: '8px 20px',
                fontSize: 13,
                borderRadius: 6,
                border: '1px solid var(--border-primary)',
                background: 'var(--bg-tertiary)',
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              Try Memory
            </button>
          )}
        </div>
      )}
    </div>
  );
};

export const Flamegraph: React.FC<FlamegraphProps> = ({ 
  data, 
  isLoading, 
  error, 
  onRefresh,
  profileType = 'CPU',
  onTypeChange,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof flamegraph> | null>(null);
  const [isInverted, setIsInverted] = useState(false);

  const renderChart = useCallback(() => {
    if (!containerRef.current || !data || data.value === 0) return;

    // Clear previous chart
    d3.select(containerRef.current).selectAll('*').remove();

    const width = containerRef.current.clientWidth;
    
    // Create flamegraph
    const chart = flamegraph()
      .width(width)
      .cellHeight(20)
      .inverted(isInverted)
      .transitionDuration(200)
      .transitionEase(d3.easeCubic)
      .minFrameSize(2)
      .selfValue(false)
      .label((d: { data: FlamegraphNode }) => {
        const percent = data.value > 0 ? ((d.data.value / data.value) * 100).toFixed(1) : '0';
        if (profileType === 'Memory') {
          return `${d.data.name}\n${formatBytes(d.data.value)} (${percent}%)`;
        }
        const unit = d.data.value === 1 ? 'sample' : 'samples';
        return `${d.data.name}\n${d.data.value.toLocaleString()} ${unit} (${percent}%)`;
      });

    // Store reference for cleanup
    chartRef.current = chart;

    // Render
    d3.select(containerRef.current)
      .datum(data)
      .call(chart as unknown as (selection: d3.Selection<HTMLDivElement, FlamegraphNode, null, undefined>) => void);

    // Style adjustments for dark theme
    setTimeout(() => {
      if (!containerRef.current) return;
      containerRef.current.querySelectorAll('text').forEach(text => {
        text.setAttribute('fill', '#fff');
        text.setAttribute('font-size', '11px');
      });
    }, 100);
  }, [data, isInverted, profileType]);

  useEffect(() => {
    renderChart();
    
    // Handle resize
    const handleResize = () => renderChart();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [renderChart]);

  const handleReset = () => {
    if (chartRef.current) {
      chartRef.current.resetZoom();
    }
  };

  const hasData = data && data.value > 0;

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--bg-primary)' }}>
      {/* Header with tabs - always visible */}
      <FlamegraphHeader
        profileType={profileType}
        onTypeChange={onTypeChange}
        onRefresh={onRefresh}
        data={data}
        isInverted={isInverted}
        setIsInverted={setIsInverted}
        onReset={handleReset}
        showControls={!!hasData && !isLoading && !error}
      />

      {/* Content area */}
      <div style={{ flex: 1, overflow: 'auto', minHeight: 300 }}>
        {isLoading ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', minHeight: 300 }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: 32,
                height: 32,
                borderWidth: 3,
                borderStyle: 'solid',
                borderColor: 'var(--border-primary)',
                borderTopColor: 'var(--accent-primary)',
                borderRadius: '50%',
                animation: 'spin 1s linear infinite',
                margin: '0 auto 12px',
              }} />
              <span style={{ fontSize: 13, color: 'var(--text-muted)' }}>Loading {profileType} profile...</span>
              <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
            </div>
          </div>
        ) : error ? (
          <div style={{ padding: 24 }}>
            <div style={{ 
              padding: 20, 
              borderRadius: 8, 
              background: 'rgba(248, 81, 73, 0.1)', 
              border: '1px solid rgba(248, 81, 73, 0.3)',
            }}>
              <div style={{ fontWeight: 600, color: '#f85149', marginBottom: 8 }}>Error loading flamegraph</div>
              <div style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>{error}</div>
            </div>
          </div>
        ) : data?.unavailableReason ? (
          <div style={{ padding: 48, textAlign: 'center' }}>
            <div style={{ fontSize: 15, color: 'var(--text-tertiary)', marginBottom: 12 }}>
              Flamegraph unavailable
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 500, margin: '0 auto', lineHeight: 1.7 }}>
              {data.unavailableReason}
            </div>
            <pre style={{
              margin: '16px auto',
              padding: 16,
              background: 'var(--bg-tertiary)',
              borderRadius: 8,
              textAlign: 'left',
              fontSize: 11,
              color: '#58a6ff',
              overflow: 'auto',
              maxWidth: 500,
            }}>
{`SET allow_introspection_functions = 1`}
            </pre>
          </div>
        ) : !hasData ? (
          <EmptyState profileType={profileType} onTypeChange={onTypeChange} />
        ) : (
          <div 
            ref={containerRef}
            style={{ 
              padding: '16px',
              minHeight: 300,
            }}
          />
        )}
      </div>

      {/* Override d3-flame-graph tooltip styles for faster appearance */}
      <style>{`
        .d3-flame-graph-tip {
          transition: opacity 0.1s ease-in-out !important;
          opacity: 0;
        }
        .d3-flame-graph-tip.show {
          opacity: 1 !important;
        }
        .d3-flame-graph rect:hover {
          stroke: #fff !important;
          stroke-width: 1px !important;
        }
      `}</style>
    </div>
  );
};

export default Flamegraph;
