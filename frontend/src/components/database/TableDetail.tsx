/**
 * TableDetail - Component for displaying table schema and metadata
 * 
 * Shows detailed information about a selected table including columns,
 * data types, keys, and table properties.
 * 
 */

import React from 'react';
import type { TableInfo, ColumnSchema } from '../../stores/databaseStore';
import { formatBytes, formatNumber } from '../../stores/databaseStore';

interface TableDetailProps {
  table: TableInfo | null;
  schema: ColumnSchema[];
  isLoading: boolean;
}

/**
 * Table metadata section
 */
const TableMetadata: React.FC<{ table: TableInfo }> = ({ table }) => {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
      {/* Engine */}
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Engine
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200 text-sm">
          {table.engine}
        </div>
      </div>

      {/* Total Rows */}
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Total Rows
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200 text-sm">
          {formatNumber(table.total_rows)}
        </div>
      </div>

      {/* Total Size */}
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Total Size
        </div>
        <div className="font-medium text-gray-800 dark:text-gray-200 text-sm">
          {formatBytes(table.total_bytes)}
        </div>
      </div>

      {/* MergeTree indicator */}
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3">
        <div className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
          Type
        </div>
        <div className="font-medium text-sm">
          {table.is_merge_tree ? (
            <span className="text-green-600 dark:text-green-400">MergeTree</span>
          ) : (
            <span className="text-gray-600 dark:text-gray-400">Other</span>
          )}
        </div>
      </div>
    </div>
  );
};

/**
 * Key information section
 */
const KeyInfo: React.FC<{ table: TableInfo }> = ({ table }) => {
  if (!table.partition_key && !table.sorting_key) {
    return null;
  }

  return (
    <div className="mb-6 space-y-3">
      {/* Partition Key */}
      {table.partition_key && (
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-3">
          <div className="flex items-center mb-1">
            <span className="text-blue-500 mr-2 font-bold text-xs">PK</span>
            <span className="text-xs text-blue-600 dark:text-blue-400 uppercase tracking-wide font-medium">
              Partition Key
            </span>
          </div>
          <code className="text-sm text-blue-800 dark:text-blue-300 font-mono">
            {table.partition_key}
          </code>
        </div>
      )}

      {/* Sorting Key */}
      {table.sorting_key && (
        <div className="bg-purple-50 dark:bg-purple-900/20 border border-purple-200 dark:border-purple-800 rounded-lg p-3">
          <div className="flex items-center mb-1">
            <span className="text-purple-500 mr-2 font-bold text-xs">SK</span>
            <span className="text-xs text-purple-600 dark:text-purple-400 uppercase tracking-wide font-medium">
              Sorting Key
            </span>
          </div>
          <code className="text-sm text-purple-800 dark:text-purple-300 font-mono">
            {table.sorting_key}
          </code>
        </div>
      )}
    </div>
  );
};

/**
 * Column key badge component
 */
const KeyBadge: React.FC<{ type: string; color: string }> = ({ type, color }) => (
  <span className={`px-1.5 py-0.5 text-xs rounded ${color}`}>
    {type}
  </span>
);

/**
 * Schema table component
 */
const SchemaTable: React.FC<{ schema: ColumnSchema[] }> = ({ schema }) => {
  if (schema.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500 dark:text-gray-400">
        No columns found
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead className="bg-gray-50 dark:bg-gray-800">
          <tr>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Column
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Type
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Keys
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Default
            </th>
            <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Comment
            </th>
          </tr>
        </thead>
        <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
          {schema.map((column, index) => (
            <tr 
              key={column.name}
              className={index % 2 === 0 ? 'bg-white dark:bg-gray-900' : 'bg-gray-50 dark:bg-gray-800/50'}
            >
              {/* Column name */}
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="font-mono text-sm text-gray-800 dark:text-gray-200">
                  {column.name}
                </span>
              </td>

              {/* Data type */}
              <td className="px-4 py-3 whitespace-nowrap">
                <code className="text-sm text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/30 px-2 py-0.5 rounded">
                  {column.type}
                </code>
              </td>

              {/* Key indicators */}
              <td className="px-4 py-3 whitespace-nowrap">
                <div className="flex flex-wrap gap-1">
                  {column.is_in_primary_key && (
                    <KeyBadge type="PK" color="bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400" />
                  )}
                  {column.is_in_partition_key && (
                    <KeyBadge type="PART" color="bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400" />
                  )}
                  {column.is_in_sorting_key && (
                    <KeyBadge type="SORT" color="bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400" />
                  )}
                  {column.is_in_sampling_key && (
                    <KeyBadge type="SAMPLE" color="bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400" />
                  )}
                </div>
              </td>

              {/* Default value */}
              <td className="px-4 py-3">
                {column.default_kind && column.default_expression ? (
                  <div className="text-sm">
                    <span className="text-gray-500 dark:text-gray-400 text-xs uppercase">
                      {column.default_kind}:
                    </span>
                    <code className="ml-1 text-gray-700 dark:text-gray-300 font-mono text-xs">
                      {column.default_expression.length > 30 
                        ? column.default_expression.substring(0, 30) + '...'
                        : column.default_expression
                      }
                    </code>
                  </div>
                ) : (
                  <span className="text-gray-400 dark:text-gray-500">-</span>
                )}
              </td>

              {/* Comment */}
              <td className="px-4 py-3">
                {column.comment ? (
                  <span className="text-sm text-gray-600 dark:text-gray-400 italic">
                    {column.comment.length > 50 
                      ? column.comment.substring(0, 50) + '...'
                      : column.comment
                    }
                  </span>
                ) : (
                  <span className="text-gray-400 dark:text-gray-500">-</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

/**
 * Loading state component
 */
const LoadingState: React.FC = () => (
  <div className="flex flex-col items-center justify-center py-12">
    <div className="animate-spin inline-block w-8 h-8 border-3 border-gray-400 border-t-gray-600 rounded-full mb-2"></div>
    <p className="text-gray-500 dark:text-gray-400">
      Loading table schema...
    </p>
  </div>
);

/**
 * Empty state when no table selected
 */
const EmptyState: React.FC = () => (
  <div className="flex flex-col items-center justify-center py-12 text-center">
    <div className="text-4xl mb-4 font-bold text-gray-400">TB</div>
    <h3 className="text-lg font-semibold text-gray-700 dark:text-gray-300 mb-2">
      No Table Selected
    </h3>
    <p className="text-gray-500 dark:text-gray-400 max-w-md">
      Select a table from the database tree to view its schema, columns, and metadata.
    </p>
  </div>
);

/**
 * Main TableDetail component
 */
export const TableDetail: React.FC<TableDetailProps> = ({
  table,
  schema,
  isLoading,
}) => {
  if (!table) {
    return <EmptyState />;
  }

  if (isLoading) {
    return <LoadingState />;
  }

  return (
    <div>
      {/* Table header */}
      <div className="mb-4">
        <div className="flex items-center">
          <span className="text-xl mr-3 font-bold text-gray-500">TB</span>
          <div>
            <h3 className="text-lg font-semibold text-gray-800 dark:text-white">
              {table.name}
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {table.database}
            </p>
          </div>
        </div>
      </div>

      {/* Table metadata */}
      <TableMetadata table={table} />

      {/* Key information */}
      <KeyInfo table={table} />

      {/* Schema section */}
      <div>
        <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wide mb-3">
          Columns ({schema.length})
        </h4>
        <SchemaTable schema={schema} />
      </div>
    </div>
  );
};

export default TableDetail;
