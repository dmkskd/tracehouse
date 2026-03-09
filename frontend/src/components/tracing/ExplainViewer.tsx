/**
 * ExplainViewer - Component for displaying EXPLAIN query output
 * 
 * Shows EXPLAIN PLAN and EXPLAIN PIPELINE results with syntax highlighting
 * and type selection.
 */

import React, { useState } from 'react';
import type { ExplainResult, ExplainType } from '../../stores/traceStore';

interface ExplainViewerProps {
  result: ExplainResult | null;
  selectedType: ExplainType;
  isLoading: boolean;
  error: string | null;
  query: string | null;
  onTypeChange: (type: ExplainType) => void;
  onExecute: (query: string, type: ExplainType) => void;
}

// Available EXPLAIN types
const EXPLAIN_TYPES: { value: ExplainType; label: string; description: string }[] = [
  { value: 'PLAN', label: 'PLAN', description: 'Query execution plan' },
  { value: 'PIPELINE', label: 'PIPELINE', description: 'Query pipeline stages' },
  { value: 'AST', label: 'AST', description: 'Abstract syntax tree' },
  { value: 'SYNTAX', label: 'SYNTAX', description: 'Formatted query syntax' },
  { value: 'QUERY TREE', label: 'QUERY TREE', description: 'Query tree structure' },
];

/**
 * EXPLAIN type selector tabs
 */
const ExplainTypeSelector: React.FC<{
  selectedType: ExplainType;
  onTypeChange: (type: ExplainType) => void;
  disabled: boolean;
}> = ({ selectedType, onTypeChange, disabled }) => (
  <div className="flex flex-wrap gap-1">
    {EXPLAIN_TYPES.map(({ value, label, description }) => (
      <button
        key={value}
        onClick={() => onTypeChange(value)}
        disabled={disabled}
        title={description}
        className={`
          px-3 py-1.5 text-sm font-medium rounded-md transition-colors
          ${selectedType === value
            ? 'bg-blue-600 text-white'
            : 'bg-gray-100 text-gray-600 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600'
          }
          ${disabled ? 'opacity-50 cursor-not-allowed' : ''}
        `}
      >
        {label}
      </button>
    ))}
  </div>
);

/**
 * Query input for EXPLAIN
 */
const QueryInput: React.FC<{
  query: string;
  onChange: (query: string) => void;
  onExecute: () => void;
  isLoading: boolean;
}> = ({ query, onChange, onExecute, isLoading }) => {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      onExecute();
    }
  };
  
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
          Query to Explain
        </label>
        <span className="text-xs text-gray-500 dark:text-gray-400">
          Ctrl+Enter to execute
        </span>
      </div>
      <textarea
        value={query}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Enter SQL query to explain..."
        rows={4}
        className="w-full px-3 py-2 text-sm font-mono border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white resize-y"
      />
      <button
        onClick={onExecute}
        disabled={isLoading || !query.trim()}
        className={`
          w-full px-4 py-2 text-sm font-medium rounded-md transition-colors
          ${isLoading || !query.trim()
            ? 'bg-gray-300 text-gray-500 cursor-not-allowed dark:bg-gray-600'
            : 'bg-blue-600 text-white hover:bg-blue-700'
          }
        `}
      >
        {isLoading ? 'Executing...' : 'Execute EXPLAIN'}
      </button>
    </div>
  );
};

/**
 * EXPLAIN output display with syntax highlighting
 */
const ExplainOutput: React.FC<{
  output: string;
  explainType: string;
}> = ({ output, explainType }) => {
  const [copied, setCopied] = useState(false);
  
  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(output);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };
  
  // Simple syntax highlighting for EXPLAIN output
  const highlightOutput = (text: string): React.ReactNode => {
    // Split into lines and highlight keywords
    const lines = text.split('\n');
    
    return lines.map((line, index) => {
      // Highlight different parts based on EXPLAIN type
      let highlightedLine = line;
      
      // Common patterns to highlight
      const patterns = [
        // Stage names (e.g., "ReadFromMergeTree", "Expression", "Filter")
        { regex: /^(\s*)([\w]+)(\s|$)/g, replacement: '$1<span class="text-blue-600 dark:text-blue-400 font-semibold">$2</span>$3' },
        // Numbers
        { regex: /\b(\d+(?:\.\d+)?)\b/g, replacement: '<span class="text-green-600 dark:text-green-400">$1</span>' },
        // Arrows and operators
        { regex: /(→|->|=>|<-|←)/g, replacement: '<span class="text-purple-600 dark:text-purple-400">$1</span>' },
        // Column names in brackets
        { regex: /\[([^\]]+)\]/g, replacement: '[<span class="text-orange-600 dark:text-orange-400">$1</span>]' },
        // Table names after FROM
        { regex: /(FROM\s+)(\w+)/gi, replacement: '$1<span class="text-yellow-600 dark:text-yellow-400">$2</span>' },
      ];
      
      patterns.forEach(({ regex, replacement }) => {
        highlightedLine = highlightedLine.replace(regex, replacement);
      });
      
      return (
        <div
          key={index}
          className="hover:bg-gray-100 dark:hover:bg-gray-700 px-2 -mx-2"
          dangerouslySetInnerHTML={{ __html: highlightedLine || '&nbsp;' }}
        />
      );
    });
  };
  
  return (
    <div className="relative">
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
          EXPLAIN {explainType} Output
        </span>
        <button
          onClick={handleCopy}
          className="px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-300"
        >
          {copied ? 'Copied!' : 'Copy'}
        </button>
      </div>
      
      {/* Output */}
      <div className="bg-gray-50 dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <pre className="p-4 text-sm font-mono text-gray-700 dark:text-gray-300 overflow-x-auto whitespace-pre">
          {highlightOutput(output)}
        </pre>
      </div>
    </div>
  );
};

/**
 * Parsed tree visualization (if available)
 */
const ParsedTreeView: React.FC<{
  tree: Record<string, unknown>;
}> = ({ tree }) => {
  const [expanded, setExpanded] = useState(true);
  
  const renderNode = (node: unknown, depth: number = 0): React.ReactNode => {
    if (node === null || node === undefined) {
      return <span className="text-gray-400">null</span>;
    }
    
    if (typeof node === 'string' || typeof node === 'number' || typeof node === 'boolean') {
      return (
        <span className={
          typeof node === 'string' ? 'text-green-600 dark:text-green-400' :
          typeof node === 'number' ? 'text-blue-600 dark:text-blue-400' :
          'text-purple-600 dark:text-purple-400'
        }>
          {typeof node === 'string' ? `"${node}"` : String(node)}
        </span>
      );
    }
    
    if (Array.isArray(node)) {
      if (node.length === 0) {
        return <span className="text-gray-400">[]</span>;
      }
      return (
        <div className="ml-4">
          {node.map((item, index) => (
            <div key={index} className="flex">
              <span className="text-gray-400 mr-2">{index}:</span>
              {renderNode(item, depth + 1)}
            </div>
          ))}
        </div>
      );
    }
    
    if (typeof node === 'object') {
      const entries = Object.entries(node as Record<string, unknown>);
      if (entries.length === 0) {
        return <span className="text-gray-400">{'{}'}</span>;
      }
      return (
        <div className="ml-4">
          {entries.map(([key, value]) => (
            <div key={key} className="flex">
              <span className="text-orange-600 dark:text-orange-400 mr-2">{key}:</span>
              {renderNode(value, depth + 1)}
            </div>
          ))}
        </div>
      );
    }
    
    return <span className="text-gray-400">{String(node)}</span>;
  };
  
  return (
    <div className="mt-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300 mb-2"
      >
        <span className={`transform transition-transform mr-2 ${expanded ? 'rotate-90' : ''}`}>
          ▶
        </span>
        Parsed Tree Structure
      </button>
      
      {expanded && (
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4 overflow-x-auto">
          <div className="font-mono text-sm">
            {renderNode(tree)}
          </div>
        </div>
      )}
    </div>
  );
};

/**
 * Main ExplainViewer component
 */
export const ExplainViewer: React.FC<ExplainViewerProps> = ({
  result,
  selectedType,
  isLoading,
  error,
  query,
  onTypeChange,
  onExecute,
}) => {
  const [localQuery, setLocalQuery] = useState(query || '');
  
  // Update local query when prop changes
  React.useEffect(() => {
    if (query && query !== localQuery) {
      setLocalQuery(query);
    }
  }, [query]);
  
  const handleExecute = () => {
    if (localQuery.trim()) {
      onExecute(localQuery, selectedType);
    }
  };
  
  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="p-4 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-gray-800 dark:text-white">
            EXPLAIN Plan
          </h3>
        </div>
        
        {/* Type selector */}
        <ExplainTypeSelector
          selectedType={selectedType}
          onTypeChange={onTypeChange}
          disabled={isLoading}
        />
      </div>
      
      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Query input */}
        <QueryInput
          query={localQuery}
          onChange={setLocalQuery}
          onExecute={handleExecute}
          isLoading={isLoading}
        />
        
        {/* Loading state */}
        {isLoading && (
          <div className="flex items-center justify-center h-32">
            <div className="text-center">
              <div className="animate-spin inline-block w-6 h-6 border-2 border-gray-400 border-t-gray-600 rounded-full mb-2"></div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                Executing EXPLAIN {selectedType}...
              </div>
            </div>
          </div>
        )}
        
        {/* Error state */}
        {error && !isLoading && (
          <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
            <div className="flex items-start">
              <span className="text-red-500 mr-2 font-bold">!</span>
              <div>
                <div className="font-medium text-red-700 dark:text-red-400">
                  Error executing EXPLAIN
                </div>
                <div className="text-sm text-red-600 dark:text-red-300 mt-1">
                  {error}
                </div>
              </div>
            </div>
          </div>
        )}
        
        {/* Result display */}
        {result && !isLoading && !error && (
          <div className="space-y-4">
            {/* Output */}
            <ExplainOutput
              output={result.output}
              explainType={result.explain_type}
            />
            
            {/* Parsed tree (if available) */}
            {result.parsed_tree && Object.keys(result.parsed_tree).length > 0 && (
              <ParsedTreeView tree={result.parsed_tree} />
            )}
          </div>
        )}
        
        {/* Empty state */}
        {!result && !isLoading && !error && (
          <div className="flex items-center justify-center h-32">
            <div className="text-center text-gray-500 dark:text-gray-400">
              <div className="text-2xl mb-2 font-light">--</div>
              <div className="text-sm">
                Enter a query and click Execute to see the execution plan
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ExplainViewer;
