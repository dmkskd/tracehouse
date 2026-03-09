/**
 * DatabaseTree - Tree navigation component for database explorer
 * 
 * Displays databases and tables in a hierarchical tree view.
 * Users can expand databases to see tables and click tables to view details.
 */

import React, { useCallback } from 'react';
import type { DatabaseInfo, TableInfo } from '../../stores/databaseStore';
import { formatNumber } from '../../stores/databaseStore';

interface DatabaseTreeProps {
  databases: DatabaseInfo[];
  tables: TableInfo[];
  expandedDatabases: Set<string>;
  selectedDatabase: string | null;
  selectedTable: TableInfo | null;
  isLoadingDatabases: boolean;
  isLoadingTables: boolean;
  onToggleDatabase: (database: string) => void;
  onSelectDatabase: (database: string) => void;
  onSelectTable: (table: TableInfo) => void;
}

/**
 * Database node in the tree
 */
const DatabaseNode: React.FC<{
  database: DatabaseInfo;
  isExpanded: boolean;
  isSelected: boolean;
  isLoadingTables: boolean;
  tables: TableInfo[];
  selectedTable: TableInfo | null;
  onToggle: () => void;
  onSelect: () => void;
  onSelectTable: (table: TableInfo) => void;
}> = ({
  database,
  isExpanded,
  isSelected,
  isLoadingTables,
  tables,
  selectedTable,
  onToggle,
  onSelect,
  onSelectTable,
}) => {
  const handleClick = useCallback(() => {
    onSelect();
    if (!isExpanded) {
      onToggle();
    }
  }, [onSelect, onToggle, isExpanded]);

  const handleToggle = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    onToggle();
  }, [onToggle]);

  return (
    <div className="select-none">
      {/* Database row */}
      <div
        className={`
          flex items-center px-2 py-1.5 cursor-pointer rounded-md transition-colors
          ${isSelected 
            ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' 
            : 'hover:bg-gray-100 dark:hover:bg-gray-700/50 text-gray-700 dark:text-gray-300'
          }
        `}
        onClick={handleClick}
      >
        {/* Expand/collapse button */}
        <button
          onClick={handleToggle}
          className="w-5 h-5 flex items-center justify-center text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
        >
          <span className={`text-xs transition-transform ${isExpanded ? 'rotate-90' : ''}`}>
            ▶
          </span>
        </button>
        
        {/* Database icon */}
        <span className="mr-2 text-sm font-bold text-gray-500">DB</span>
        
        {/* Database name */}
        <span className="flex-1 font-medium text-sm truncate">
          {database.name}
        </span>
        
        {/* Table count badge */}
        <span className="ml-2 px-2 py-0.5 text-xs bg-gray-200 dark:bg-gray-600 rounded-full text-gray-600 dark:text-gray-300">
          {database.table_count}
        </span>
      </div>

      {/* Tables list (when expanded) */}
      {isExpanded && (
        <div className="ml-4 border-l border-gray-200 dark:border-gray-700">
          {isLoadingTables ? (
            <div className="flex items-center px-4 py-2 text-sm text-gray-500 dark:text-gray-400">
              <span className="animate-spin inline-block w-4 h-4 border-2 border-gray-400 border-t-gray-600 rounded-full mr-2"></span>
              Loading tables...
            </div>
          ) : tables.length === 0 ? (
            <div className="px-4 py-2 text-sm text-gray-400 dark:text-gray-500 italic">
              No tables found
            </div>
          ) : (
            tables.map((table) => (
              <TableNode
                key={`${table.database}.${table.name}`}
                table={table}
                isSelected={selectedTable?.name === table.name && selectedTable?.database === table.database}
                onSelect={() => onSelectTable(table)}
              />
            ))
          )}
        </div>
      )}
    </div>
  );
};

/**
 * Table node in the tree
 */
const TableNode: React.FC<{
  table: TableInfo;
  isSelected: boolean;
  onSelect: () => void;
}> = ({ table, isSelected, onSelect }) => {
  // Determine table icon based on engine
  const getTableIcon = () => {
    if (table.is_merge_tree) return 'MT';
    if (table.engine.includes('Log')) return 'LG';
    if (table.engine.includes('View')) return 'VW';
    if (table.engine.includes('Dictionary')) return 'DC';
    if (table.engine.includes('Memory')) return 'MM';
    return 'TB';
  };

  return (
    <div
      className={`
        flex items-center px-4 py-1.5 cursor-pointer rounded-md transition-colors ml-2
        ${isSelected 
          ? 'bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400' 
          : 'hover:bg-gray-50 dark:hover:bg-gray-700/30 text-gray-600 dark:text-gray-400'
        }
      `}
      onClick={onSelect}
    >
      {/* Table icon */}
      <span className="mr-2 text-xs font-bold text-gray-400">{getTableIcon()}</span>
      
      {/* Table name */}
      <span className="flex-1 text-sm truncate">
        {table.name}
      </span>
      
      {/* Row count */}
      <span className="ml-2 text-xs text-gray-400 dark:text-gray-500">
        {formatNumber(table.total_rows)} rows
      </span>
    </div>
  );
};

/**
 * Empty state when no databases
 */
const EmptyState: React.FC<{ isLoading: boolean }> = ({ isLoading }) => (
  <div className="flex flex-col items-center justify-center py-8 text-center">
    {isLoading ? (
      <>
        <div className="animate-spin inline-block w-8 h-8 border-3 border-gray-400 border-t-gray-600 rounded-full mb-2"></div>
        <p className="text-gray-500 dark:text-gray-400 text-sm">
          Loading databases...
        </p>
      </>
    ) : (
      <>
        <div className="text-2xl mb-2 font-bold text-gray-400">DB</div>
        <p className="text-gray-500 dark:text-gray-400 text-sm">
          No databases found
        </p>
      </>
    )}
  </div>
);

/**
 * Main DatabaseTree component
 */
export const DatabaseTree: React.FC<DatabaseTreeProps> = ({
  databases,
  tables,
  expandedDatabases,
  selectedDatabase,
  selectedTable,
  isLoadingDatabases,
  isLoadingTables,
  onToggleDatabase,
  onSelectDatabase,
  onSelectTable,
}) => {
  if (databases.length === 0) {
    return <EmptyState isLoading={isLoadingDatabases} />;
  }

  return (
    <div className="space-y-1">
      {databases.map((database) => {
        const isExpanded = expandedDatabases.has(database.name);
        const isSelected = selectedDatabase === database.name;
        // Show tables for this database if it's expanded AND selected (tables were fetched for it)
        const showTables = isExpanded && isSelected;
        
        return (
          <DatabaseNode
            key={database.name}
            database={database}
            isExpanded={isExpanded}
            isSelected={isSelected}
            isLoadingTables={isLoadingTables && isSelected}
            tables={showTables ? tables : []}
            selectedTable={selectedTable}
            onToggle={() => onToggleDatabase(database.name)}
            onSelect={() => onSelectDatabase(database.name)}
            onSelectTable={onSelectTable}
          />
        );
      })}
    </div>
  );
};

export default DatabaseTree;
