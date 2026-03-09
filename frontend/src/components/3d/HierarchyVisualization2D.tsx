/**
 * HierarchyVisualization2D - Clean 2D grid view for databases, tables, and partitions
 * 
 * A 2D alternative to the 3D HierarchyVisualization, providing a consistent
 * 2D/3D toggle experience across all levels of the database explorer.
 */

import React, { useState, useMemo, useCallback, useRef } from 'react';
import type { HierarchyItem, HierarchyLevel } from './HierarchyVisualization';
import { formatBytes } from '../../utils/formatters';

// Health status colors
const HEALTH_COLORS: Record<string, { bg: string; border: string; text: string; glow: string }> = {
  good:     { bg: 'rgba(34,197,94,0.08)',  border: '#22c55e', text: '#4ade80', glow: '0 0 12px rgba(34,197,94,0.15)' },
  warning:  { bg: 'rgba(234,179,8,0.08)',   border: '#eab308', text: '#facc15', glow: '0 0 12px rgba(234,179,8,0.15)' },
  critical: { bg: 'rgba(239,68,68,0.08)',   border: '#ef4444', text: '#f87171', glow: '0 0 12px rgba(239,68,68,0.15)' },
  merging:  { bg: 'rgba(6,182,212,0.08)',   border: '#06b6d4', text: '#22d3ee', glow: '0 0 12px rgba(6,182,212,0.15)' },
};

interface HierarchyVisualization2DProps {
  items: HierarchyItem[];
  level: HierarchyLevel;
  onItemClick: (item: HierarchyItem) => void;
  onItemHover: (item: HierarchyItem | null) => void;
  highlightedMergeId?: string | null;
  mergeColorMap?: Map<string, number>;
}

/** Single item card */
const ItemCard: React.FC<{
  item: HierarchyItem;
  level: HierarchyLevel;
  isHovered: boolean;
  isDimmed: boolean;
  onClick: () => void;
  onMouseEnter: () => void;
  onMouseLeave: () => void;
}> = ({ item, level, isHovered, isDimmed, onClick, onMouseEnter, onMouseLeave }) => {
  const colors = HEALTH_COLORS[item.health] || HEALTH_COLORS.good;
  
  // Size indicator (relative bar)
  const levelLabel = level === 'databases' ? 'DB' : level === 'tables' ? 'TB' : level === 'partitions' ? 'PT' : '';
  
  return (
    <div
      onClick={onClick}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      className={item.merging ? 'card-pulse-merging' : ''}
      style={{
        background: isHovered ? colors.bg : 'var(--bg-card)',
        border: `1px solid ${isHovered ? colors.border : item.merging ? colors.border : 'var(--border-primary)'}`,
        borderRadius: 10,
        padding: '14px 16px',
        cursor: 'pointer',
        transition: 'all 0.15s ease',
        opacity: isDimmed ? 0.3 : 1,
        boxShadow: isHovered ? colors.glow : item.merging ? `0 0 8px rgba(6,182,212,0.2)` : 'none',
        minWidth: 160,
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      {/* Merging pulse indicator */}
      {item.merging && (
        <div style={{
          position: 'absolute', top: 6, right: 6,
          width: 8, height: 8, borderRadius: '50%',
          background: '#06b6d4',
          animation: 'pulse 1.5s ease-in-out infinite',
          boxShadow: '0 0 8px rgba(6,182,212,0.6)',
        }} />
      )}
      
      {/* Header row */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{
          fontSize: 9, fontWeight: 700, color: colors.text,
          background: colors.bg, border: `1px solid ${colors.border}40`,
          padding: '2px 6px', borderRadius: 4,
          fontFamily: "'Share Tech Mono', monospace",
        }}>
          {levelLabel}
        </span>
        <span style={{
          fontSize: 13, fontWeight: 600, color: 'var(--text-primary)',
          fontFamily: "'Orbitron', 'Rajdhani', monospace",
          letterSpacing: '0.5px',
          overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          flex: 1,
        }}>
          {item.name}
        </span>
      </div>
      
      {/* Metrics */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
        {Object.entries(item.metrics).slice(0, 4).map(([key, value]) => (
          <div key={key} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
              {key}
            </span>
            <span style={{ fontSize: 11, color: 'var(--text-secondary)', fontFamily: "'Share Tech Mono', monospace" }}>
              {value}
            </span>
          </div>
        ))}
      </div>
      
      {/* Size bar */}
      {item.size > 0 && (
        <div style={{ marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
          {formatBytes(item.size)}
        </div>
      )}
      
      {/* Health score bar */}
      <div style={{
        marginTop: 6, height: 3, borderRadius: 2,
        background: 'var(--bg-secondary)',
        overflow: 'hidden',
      }}>
        <div style={{
          height: '100%', borderRadius: 2,
          width: `${item.healthScore}%`,
          background: colors.border,
          transition: 'width 0.3s ease',
        }} />
      </div>
      
      {/* Child count badge */}
      {item.childCount !== undefined && (
        <div style={{
          marginTop: 6, fontSize: 10, color: 'var(--text-muted)',
          display: 'flex', alignItems: 'center', gap: 4,
        }}>
          <span>→</span>
          <span>{item.childCount} {level === 'databases' ? 'tables' : level === 'tables' ? 'partitions' : 'parts'}</span>
        </div>
      )}
      
      {/* Issues */}
      {item.issues && item.issues.length > 0 && (
        <div style={{ marginTop: 6, fontSize: 10, color: '#f87171' }}>
          ⚠ {item.issues[0]}
        </div>
      )}
    </div>
  );
};

export const HierarchyVisualization2D: React.FC<HierarchyVisualization2DProps> = ({
  items,
  level,
  onItemClick,
  onItemHover,
  highlightedMergeId,
}) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [zoom, setZoom] = useState(1);
  const containerRef = useRef<HTMLDivElement>(null);

  // Inject pulse keyframes once
  const styleInjected = useRef(false);
  if (!styleInjected.current && typeof document !== 'undefined') {
    styleInjected.current = true;
    const id = 'card-pulse-merging-style';
    if (!document.getElementById(id)) {
      const style = document.createElement('style');
      style.id = id;
      style.textContent = `
        @keyframes cardPulseMerging {
          0%, 100% { box-shadow: 0 0 6px rgba(6,182,212,0.15); }
          50% { box-shadow: 0 0 18px rgba(6,182,212,0.35); }
        }
        .card-pulse-merging {
          animation: cardPulseMerging 2s ease-in-out infinite;
        }
      `;
      document.head.appendChild(style);
    }
  }

  const handleZoomIn = useCallback(() => setZoom(z => Math.min(z + 0.25, 2)), []);
  const handleZoomOut = useCallback(() => setZoom(z => Math.max(z - 0.25, 0.5)), []);
  const handleZoomReset = useCallback(() => setZoom(1), []);

  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.1 : 0.1;
      setZoom(z => Math.max(0.5, Math.min(2, z + delta)));
    }
  }, []);

  const handleHover = useCallback((item: HierarchyItem | null) => {
    setHoveredId(item?.id || null);
    onItemHover(item);
  }, [onItemHover]);

  // Sort items: by size descending
  const sortedItems = useMemo(() => {
    return [...items].sort((a, b) => b.size - a.size);
  }, [items]);

  if (items.length === 0) {
    return (
      <div style={{
        height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center',
        color: 'var(--text-muted)', fontSize: 16,
      }}>
        No {level} found
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      onWheel={handleWheel}
      style={{
        height: '100%', overflow: 'auto',
        position: 'relative',
      }}
    >
      {/* Zoom controls - bottom left to avoid overlap with auto-refresh */}
      <div style={{
        position: 'absolute', bottom: 90, right: 16,
        display: 'flex', gap: 4, zIndex: 1100,
        background: 'var(--bg-secondary)', borderRadius: 8,
        border: '1px solid var(--border-primary)',
        padding: 4,
        boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
        backdropFilter: 'blur(12px)',
      }}>
        <button onClick={handleZoomOut} style={zoomBtnStyle}>−</button>
        <button onClick={handleZoomReset} style={{ ...zoomBtnStyle, fontSize: 9, minWidth: 36 }}>
          {Math.round(zoom * 100)}%
        </button>
        <button onClick={handleZoomIn} style={zoomBtnStyle}>+</button>
      </div>

      <div style={{
        padding: '260px 24px 80px 24px',
        transform: `scale(${zoom})`,
        transformOrigin: 'top left',
      }}>
        {/* Flat grid for all levels */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
          gap: 14,
        }}>
          {sortedItems.map(item => (
            <ItemCard
              key={item.id}
              item={item}
              level={level}
              isHovered={hoveredId === item.id}
              isDimmed={!!highlightedMergeId && item.mergeId !== highlightedMergeId}
              onClick={() => onItemClick(item)}
              onMouseEnter={() => handleHover(item)}
              onMouseLeave={() => handleHover(null)}
            />
          ))}
        </div>
      </div>
    </div>
  );
};


const zoomBtnStyle: React.CSSProperties = {
  background: 'transparent', border: 'none', cursor: 'pointer',
  color: 'var(--text-secondary)', fontSize: 14, fontWeight: 600,
  width: 28, height: 28, borderRadius: 6,
  display: 'flex', alignItems: 'center', justifyContent: 'center',
};
