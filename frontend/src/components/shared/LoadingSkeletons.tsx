/**
 * LoadingSkeletons - Skeleton screen components for loading states
 * 
 * These components provide visual placeholders while data is being loaded,
 * improving perceived performance and user experience.
 */

import React from 'react';

/**
 * Base skeleton component with animation
 */
interface SkeletonProps {
  className?: string;
  animate?: boolean;
  style?: React.CSSProperties;
}

export const Skeleton: React.FC<SkeletonProps> = ({ 
  className = '', 
  animate = true,
  style,
}) => (
  <div
    className={`
      bg-gray-200 dark:bg-gray-700 rounded
      ${animate ? 'animate-pulse' : ''}
      ${className}
    `}
    style={style}
  />
);

/**
 * Skeleton for text lines
 */
interface SkeletonTextProps {
  lines?: number;
  className?: string;
}

export const SkeletonText: React.FC<SkeletonTextProps> = ({ 
  lines = 3, 
  className = '' 
}) => (
  <div className={`space-y-2 ${className}`}>
    {Array.from({ length: lines }).map((_, i) => (
      <Skeleton
        key={i}
        className={`h-4 ${i === lines - 1 ? 'w-3/4' : 'w-full'}`}
      />
    ))}
  </div>
);

/**
 * Skeleton for a card component
 */
interface SkeletonCardProps {
  className?: string;
  showHeader?: boolean;
  showFooter?: boolean;
}

export const SkeletonCard: React.FC<SkeletonCardProps> = ({
  className = '',
  showHeader = true,
  showFooter = false,
}) => (
  <div
    className={`
      bg-white dark:bg-gray-800 rounded-lg shadow p-4
      ${className}
    `}
  >
    {showHeader && (
      <div className="flex items-center justify-between mb-4">
        <Skeleton className="h-6 w-1/3" />
        <Skeleton className="h-6 w-16" />
      </div>
    )}
    <SkeletonText lines={3} />
    {showFooter && (
      <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
        <div className="flex justify-end gap-2">
          <Skeleton className="h-8 w-20" />
          <Skeleton className="h-8 w-20" />
        </div>
      </div>
    )}
  </div>
);

/**
 * Skeleton for table rows
 */
interface SkeletonTableProps {
  rows?: number;
  columns?: number;
  className?: string;
  showHeader?: boolean;
}

export const SkeletonTable: React.FC<SkeletonTableProps> = ({
  rows = 5,
  columns = 4,
  className = '',
  showHeader = true,
}) => (
  <div className={`overflow-hidden ${className}`}>
    <table className="w-full">
      {showHeader && (
        <thead>
          <tr className="border-b border-gray-200 dark:border-gray-700">
            {Array.from({ length: columns }).map((_, i) => (
              <th key={i} className="px-4 py-3 text-left">
                <Skeleton className="h-4 w-20" />
              </th>
            ))}
          </tr>
        </thead>
      )}
      <tbody>
        {Array.from({ length: rows }).map((_, rowIndex) => (
          <tr
            key={rowIndex}
            className="border-b border-gray-100 dark:border-gray-700/50"
          >
            {Array.from({ length: columns }).map((_, colIndex) => (
              <td key={colIndex} className="px-4 py-3">
                <Skeleton
                  className={`h-4 ${
                    colIndex === 0 ? 'w-32' : colIndex === columns - 1 ? 'w-16' : 'w-24'
                  }`}
                />
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

/**
 * Skeleton for 3D visualization area
 */
interface Skeleton3DProps {
  className?: string;
  message?: string;
}

export const Skeleton3D: React.FC<Skeleton3DProps> = ({
  className = '',
  message = 'Loading 3D visualization...',
}) => (
  <div
    className={`
      relative h-full min-h-[400px] 
      bg-gray-100 dark:bg-gray-800 rounded-lg
      flex items-center justify-center
      ${className}
    `}
  >
    {/* Animated background pattern */}
    <div className="absolute inset-0 overflow-hidden rounded-lg">
      <div className="absolute inset-0 bg-gradient-to-br from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-800 animate-pulse" />
      
      {/* Fake 3D grid lines */}
      <svg
        className="absolute inset-0 w-full h-full opacity-20"
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          <pattern
            id="grid"
            width="40"
            height="40"
            patternUnits="userSpaceOnUse"
          >
            <path
              d="M 40 0 L 0 0 0 40"
              fill="none"
              stroke="currentColor"
              strokeWidth="1"
              className="text-gray-400 dark:text-gray-600"
            />
          </pattern>
        </defs>
        <rect width="100%" height="100%" fill="url(#grid)" />
      </svg>
      
      {/* Fake 3D cubes */}
      <div className="absolute inset-0 flex items-center justify-center gap-4">
        {[1, 2, 3].map((i) => (
          <div
            key={i}
            className="w-16 h-16 bg-gray-300 dark:bg-gray-600 rounded animate-pulse"
            style={{
              animationDelay: `${i * 200}ms`,
              transform: `translateY(${(i - 2) * 10}px)`,
            }}
          />
        ))}
      </div>
    </div>
    
    {/* Loading message */}
    <div className="relative z-10 text-center">
      <div className="animate-spin inline-block w-8 h-8 border-3 border-gray-400 border-t-gray-600 rounded-full mb-3"></div>
      <p className="text-sm text-gray-500 dark:text-gray-400 font-medium">
        {message}
      </p>
    </div>
  </div>
);

/**
 * Skeleton for metric cards
 */
interface SkeletonMetricCardProps {
  className?: string;
}

export const SkeletonMetricCard: React.FC<SkeletonMetricCardProps> = ({
  className = '',
}) => (
  <div
    className={`
      bg-white dark:bg-gray-800 rounded-lg shadow p-4
      ${className}
    `}
  >
    <div className="flex items-center justify-between mb-2">
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-6 w-6 rounded-full" />
    </div>
    <Skeleton className="h-8 w-20 mb-2" />
    <Skeleton className="h-3 w-16" />
  </div>
);

/**
 * Skeleton for a list of items
 */
interface SkeletonListProps {
  items?: number;
  className?: string;
  showIcon?: boolean;
}

export const SkeletonList: React.FC<SkeletonListProps> = ({
  items = 5,
  className = '',
  showIcon = true,
}) => (
  <div className={`space-y-3 ${className}`}>
    {Array.from({ length: items }).map((_, i) => (
      <div key={i} className="flex items-center gap-3">
        {showIcon && <Skeleton className="h-8 w-8 rounded-full flex-shrink-0" />}
        <div className="flex-1 space-y-2">
          <Skeleton className="h-4 w-3/4" />
          <Skeleton className="h-3 w-1/2" />
        </div>
      </div>
    ))}
  </div>
);

/**
 * Skeleton for chart/graph area
 */
interface SkeletonChartProps {
  className?: string;
  type?: 'bar' | 'line' | 'pie';
}

export const SkeletonChart: React.FC<SkeletonChartProps> = ({
  className = '',
  type = 'bar',
}) => (
  <div
    className={`
      bg-white dark:bg-gray-800 rounded-lg p-4
      ${className}
    `}
  >
    {/* Chart header */}
    <div className="flex items-center justify-between mb-4">
      <Skeleton className="h-5 w-32" />
      <div className="flex gap-2">
        <Skeleton className="h-6 w-16" />
        <Skeleton className="h-6 w-16" />
      </div>
    </div>
    
    {/* Chart area */}
    <div className="h-64 flex items-end justify-around gap-2">
      {type === 'bar' && (
        <>
          {[40, 65, 45, 80, 55, 70, 50].map((height, i) => (
            <Skeleton
              key={i}
              className="w-8 rounded-t animate-pulse"
              style={{
                height: `${height}%`,
                animationDelay: `${i * 100}ms`,
              }}
            />
          ))}
        </>
      )}
      
      {type === 'line' && (
        <div className="w-full h-full relative">
          <Skeleton className="absolute bottom-0 left-0 right-0 h-1/2 rounded" />
          <div className="absolute inset-0 flex items-center justify-around">
            {[1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} className="w-3 h-3 rounded-full" />
            ))}
          </div>
        </div>
      )}
      
      {type === 'pie' && (
        <div className="flex items-center justify-center w-full">
          <Skeleton className="w-48 h-48 rounded-full" />
        </div>
      )}
    </div>
    
    {/* Legend */}
    <div className="mt-4 flex justify-center gap-4">
      {[1, 2, 3].map((i) => (
        <div key={i} className="flex items-center gap-2">
          <Skeleton className="w-3 h-3 rounded" />
          <Skeleton className="h-3 w-16" />
        </div>
      ))}
    </div>
  </div>
);

/**
 * Skeleton for tree/hierarchy view
 */
interface SkeletonTreeProps {
  depth?: number;
  itemsPerLevel?: number;
  className?: string;
}

export const SkeletonTree: React.FC<SkeletonTreeProps> = ({
  depth = 3,
  itemsPerLevel = 3,
  className = '',
}) => {
  const renderLevel = (currentDepth: number): React.ReactNode => {
    if (currentDepth >= depth) return null;
    
    return (
      <div className="space-y-2">
        {Array.from({ length: itemsPerLevel }).map((_, i) => (
          <div key={i} style={{ paddingLeft: `${currentDepth * 16}px` }}>
            <div className="flex items-center gap-2 py-1">
              <Skeleton className="w-4 h-4 rounded" />
              <Skeleton className={`h-4 w-${24 - currentDepth * 4}`} />
            </div>
            {i === 0 && currentDepth < depth - 1 && renderLevel(currentDepth + 1)}
          </div>
        ))}
      </div>
    );
  };
  
  return <div className={className}>{renderLevel(0)}</div>;
};

/**
 * Full page loading skeleton
 */
interface SkeletonPageProps {
  showSidebar?: boolean;
  className?: string;
}

export const SkeletonPage: React.FC<SkeletonPageProps> = ({
  showSidebar = true,
  className = '',
}) => (
  <div className={`flex h-full ${className}`}>
    {showSidebar && (
      <div className="w-64 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 p-4">
        <Skeleton className="h-8 w-32 mb-6" />
        <SkeletonList items={6} showIcon={false} />
      </div>
    )}
    <div className="flex-1 p-6">
      <div className="flex items-center justify-between mb-6">
        <Skeleton className="h-8 w-48" />
        <div className="flex gap-2">
          <Skeleton className="h-10 w-24" />
          <Skeleton className="h-10 w-24" />
        </div>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <SkeletonMetricCard />
        <SkeletonMetricCard />
        <SkeletonMetricCard />
      </div>
      <SkeletonCard className="mb-6" />
      <SkeletonTable rows={5} columns={5} />
    </div>
  </div>
);

export default {
  Skeleton,
  SkeletonText,
  SkeletonCard,
  SkeletonTable,
  Skeleton3D,
  SkeletonMetricCard,
  SkeletonList,
  SkeletonChart,
  SkeletonTree,
  SkeletonPage,
};
