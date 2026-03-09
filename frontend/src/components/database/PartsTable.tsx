/**
 * PartsTable - Component for displaying table parts information
 * 
 * Shows parts information for MergeTree tables with sorting capabilities.
 * Displays partition, size, rows, and merge level information.
 * 
 */

import React, { useCallback } from 'react';
import type { PartInfo, PartSort, PartSortField, SortDirection } from '../../stores/databaseStore';
import { formatBytes, formatNumber, sortParts } from '../../stores/databaseStore';
import { CopyTableButton } from '../common/CopyTableButton';

interface PartsTableProps {
  parts: PartInfo[];
  sort: PartSort;
  onSortChange: (sort: PartSort) => void;
  isLoading: boolean;
  isMergeTree: boolean;
}

/**
 * Sortable column header component
 */
const SortableHeader: React.FC<{
  field: PartSortField;
  label: string;
  currentSort: PartSort;
  onSort: (field: PartSortField) => void;
}> = ({ field, label, currentSort, onSort }) => {
  const isActive = currentSort.field === field;
  
  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700"
      onClick={() => onSort(field)}
    >
      <div className="flex items-center space-x-1">
        <span>{label}</span>
        <span className={`text-xs ${isActive ? 'text-blue-500' : 'text-gray-400'}`}>
          {isActive ? (currentSort.direction === 'asc' ? '▲' : '▼') : '⇅'}
        </span>
      </div>
    </th>
  );
};

/**
 * Part level badge component
 */
const LevelBadge: React.FC<{ level: number }> = ({ level }) => {
  const getColor = () => {
    if (level === 0) return 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400';
    if (level <= 2) return 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400';
    if (level <= 5) return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400';
    return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
  };

  return (
    <span className={`px-2 py-0.5 text-xs font-medium rounded-full ${getColor()}`}>
      L{level}
    </span>
  );
};

/**
 * Single part row component
 */
const PartRow: React.FC<{
  part: PartInfo;
  index: number;
}> = ({ part, index }) => {
  // Format modification time
  const formatTime = (timestamp: string): string => {
    return new Date(timestamp).toLocaleString();
  };

  return (
    <tr className={index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800/50'}>
      {/* Part name */}
      <td className="px-4 py-3 whitespace-nowrap">
        <code className="text-sm text-gray-800 dark:text-gray-200 font-mono">
          {part.name}
        </code>
      </td>

      {/* Partition ID */}
      <td className="px-4 py-3 whitespace-nowrap">
        <span className="text-sm text-gray-600 dark:text-gray-400">
          {part.partition_id}
        </span>
      </td>

      {/* Rows */}
      <td className="px-4 py-3 whitespace-nowrap">
        <span className="text-sm text-gray-700 dark:text-gray-300">
          {formatNumber(part.rows)}
        </span>
      </td>

      {/* Size on disk */}
      <td className="px-4 py-3 whitespace-nowrap">
        <span className="text-sm text-gray-700 dark:text-gray-300">
          {formatBytes(part.bytes_on_disk)}
        </span>
      </td>

      {/* Modification time */}
      <td className="px-4 py-3 whitespace-nowrap">
        <span className="text-xs text-gray-500 dark:text-gray-400">
          {formatTime(part.modification_time)}
        </span>
      </td>

      {/* Level */}
      <td className="px-4 py-3 whitespace-nowrap">
        <LevelBadge level={part.level} />
      </td>

      {/* Primary key memory */}
      <td className="px-4 py-3 whitespace-nowrap">
        <span className="text-sm text-gray-600 dark:text-gray-400">
          {formatBytes(part.primary_key_bytes_in_memory)}
        </span>
      </td>
    </tr>
  );
};

/**
 * Parts summary component
 */
const PartsSummary: React.FC<{ parts: PartInfo[] }> = ({ parts }) => {
  const totalRows = parts.reduce((sum, p) => sum + p.rows, 0);
  const totalBytes = parts.reduce((sum, p) => sum + p.bytes_on_disk, 0);
  const partitions = new Set(parts.map(p => p.partition_id)).size;

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Total Parts
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200">
          {formatNumber(parts.length)}
        </div>
      </div>

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Partitions
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200">
          {formatNumber(partitions)}
        </div>
      </div>

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Total Rows
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200">
          {formatNumber(totalRows)}
        </div>
      </div>

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Total Size
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200">
          {formatBytes(totalBytes)}
        </div>
      </div>
    </div>
  );
};

/**
 * Loading state component
 */
const LoadingState: React.FC = () => (
  <div className="flex flex-col items-center justify-center py-8">
    <div className="animate-spin inline-block w-8 h-8 border-3 border-gray-400 border-t-gray-600 rounded-full mb-2"></div>
    <p className="text-gray-500 dark:text-gray-400 text-sm">
      Loading parts information...
    </p>
  </div>
);

/**
 * Empty state when no parts
 */
const EmptyState: React.FC = () => (
  <div className="text-center py-8 text-gray-500 dark:text-gray-400">
    <div className="text-2xl mb-2 font-light">--</div>
    <p>No parts found for this table</p>
  </div>
);

/**
 * Not MergeTree message
 */
const NotMergeTreeMessage: React.FC = () => (
  <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
    <div className="flex items-center">
      <span className="text-yellow-500 text-xl mr-3 font-bold">i</span>
      <p className="text-sm text-yellow-700 dark:text-yellow-300">
        Parts information is only available for MergeTree family tables.
        This table uses a different engine.
      </p>
    </div>
  </div>
);

/**
 * Main PartsTable component
 */
export const PartsTable: React.FC<PartsTableProps> = ({
  parts,
  sort,
  onSortChange,
  isLoading,
  isMergeTree,
}) => {
  // Handle sort change
  const handleSort = useCallback((field: PartSortField) => {
    const newDirection: SortDirection = 
      sort.field === field && sort.direction === 'desc' ? 'asc' : 'desc';
    onSortChange({ field, direction: newDirection });
  }, [sort, onSortChange]);

  // Sort the parts
  const sortedParts = sortParts(parts, sort);

  // Show message if not MergeTree
  if (!isMergeTree) {
    return <NotMergeTreeMessage />;
  }

  if (isLoading) {
    return <LoadingState />;
  }

  if (parts.length === 0) {
    return <EmptyState />;
  }

  return (
    <div>
      {/* Summary */}
      <PartsSummary parts={parts} />

      {/* Parts table */}
      <div className="overflow-x-auto">
        <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '4px 8px' }}>
          <CopyTableButton
            headers={['Part Name', 'Partition', 'Rows', 'Size', 'Modified', 'Level', 'PK Memory']}
            rows={sortedParts.map(p => [
              p.name, p.partition_id, formatNumber(p.rows), formatBytes(p.bytes_on_disk),
              new Date(p.modification_time).toLocaleString(), `L${p.level}`,
              formatBytes(p.primary_key_bytes_in_memory),
            ])}
          />
        </div>
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead className="bg-gray-50 dark:bg-gray-800">
            <tr>
              <SortableHeader
                field="name"
                label="Part Name"
                currentSort={sort}
                onSort={handleSort}
              />
              <SortableHeader
                field="partition_id"
                label="Partition"
                currentSort={sort}
                onSort={handleSort}
              />
              <SortableHeader
                field="rows"
                label="Rows"
                currentSort={sort}
                onSort={handleSort}
              />
              <SortableHeader
                field="bytes_on_disk"
                label="Size"
                currentSort={sort}
                onSort={handleSort}
              />
              <SortableHeader
                field="modification_time"
                label="Modified"
                currentSort={sort}
                onSort={handleSort}
              />
              <SortableHeader
                field="level"
                label="Level"
                currentSort={sort}
                onSort={handleSort}
              />
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                PK Memory
              </th>
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
            {sortedParts.map((part, index) => (
              <PartRow key={part.name} part={part} index={index} />
            ))}
          </tbody>
        </table>
      </div>

      {/* Parts count info */}
      <div className="mt-4 text-sm text-gray-500 dark:text-gray-400">
        Showing {sortedParts.length} active parts
      </div>
    </div>
  );
};

export default PartsTable;
