/**
 * MemoryWaterfall - Horizontal stacked bar for memory breakdown with detail table
 */

import type { MemorySubsystem } from '@tracehouse/core';
import { formatBytes } from '../../utils/formatters';

interface MemoryWaterfallProps {
  subsystems: MemorySubsystem[];
  totalRSS: number;
  className?: string;
}

function formatPercent(pct: number): string {
  return `${pct.toFixed(1)}%`;
}

export function MemoryWaterfall({ subsystems, totalRSS, className = '' }: MemoryWaterfallProps) {
  // Filter out zero-value subsystems for the bar
  const visibleSubsystems = subsystems.filter(s => s.bytes > 0);
  
  return (
    <div className={className}>
      {/* Stacked Bar */}
      <div
        className="w-full h-8 rounded-lg overflow-hidden flex"
        style={{ backgroundColor: 'var(--bg-tertiary)' }}
      >
        {visibleSubsystems.map((subsystem) => {
          const widthPct = totalRSS > 0 ? (subsystem.bytes / totalRSS) * 100 : 0;
          if (widthPct < 0.5) return null; // Hide tiny segments
          
          return (
            <div
              key={subsystem.id}
              className="h-full transition-all duration-300 relative group"
              style={{
                width: `${widthPct}%`,
                backgroundColor: subsystem.color,
              }}
            >
              {/* Tooltip */}
              <div 
                className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-2 py-1 rounded text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10"
                style={{ background: 'var(--bg-secondary)', color: 'var(--text-primary)', border: '1px solid var(--border-primary)' }}
              >
                {subsystem.label}: {formatBytes(subsystem.bytes)} ({formatPercent(widthPct)})
              </div>
            </div>
          );
        })}
      </div>

      {/* Detail Table */}
      <div className="mt-4 space-y-2">
        {subsystems.map((subsystem) => {
          const pct = totalRSS > 0 ? (subsystem.bytes / totalRSS) * 100 : 0;
          
          return (
            <div
              key={subsystem.id}
              className="flex items-center gap-3 py-2 px-3 rounded-lg transition-colors"
              style={{ background: 'transparent' }}
            >
              {/* Color indicator */}
              <div
                className="w-3 h-3 rounded-sm shrink-0"
                style={{ backgroundColor: subsystem.color }}
              />
              
              {/* Label and detail */}
              <div className="flex-1 min-w-0">
                <div className="text-sm" style={{ color: 'var(--text-primary)' }}>{subsystem.label}</div>
                <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>{subsystem.detail}</div>
              </div>
              
              {/* Size */}
              <div className="text-right shrink-0">
                <div className="text-sm font-mono" style={{ color: 'var(--text-primary)' }}>{formatBytes(subsystem.bytes)}</div>
                <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>{formatPercent(pct)}</div>
              </div>
              
              {/* Sub-details (cache stats) */}
              {subsystem.sub && (
                <div className="text-right text-xs shrink-0 w-24" style={{ color: 'var(--text-tertiary)' }}>
                  {subsystem.sub.hitRate !== undefined && (
                    <div>Hit: {subsystem.sub.hitRate.toFixed(1)}%</div>
                  )}
                  {subsystem.sub.files !== undefined && (
                    <div>{subsystem.sub.files} files</div>
                  )}
                  {subsystem.sub.cells !== undefined && (
                    <div>{subsystem.sub.cells} cells</div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default MemoryWaterfall;
