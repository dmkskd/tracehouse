/**
 * Headless hook for lineage tree functionality.
 * Fetches lineage data and manages expand/collapse state for tree nodes.
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import type { IClickHouseAdapter, PartLineage, LineageNode } from '@tracehouse/core';
import { DatabaseExplorer } from '@tracehouse/core';

export interface UseLineageTreeResult {
  lineage: PartLineage | null;
  isLoading: boolean;
  error: string | null;
  expandedNodes: Set<string>;
  toggleNode: (partName: string) => void;
  expandAll: () => void;
  collapseAll: () => void;
  isExpanded: (partName: string) => boolean;
}

function collectAllNodeNames(node: LineageNode): string[] {
  const names: string[] = [node.part_name];
  for (const child of node.children) {
    names.push(...collectAllNodeNames(child));
  }
  return names;
}

function collectNonLeafNodeNames(node: LineageNode): string[] {
  const names: string[] = [];
  if (node.children.length > 0) {
    names.push(node.part_name);
    for (const child of node.children) {
      names.push(...collectNonLeafNodeNames(child));
    }
  }
  return names;
}

export function useLineageTree(
  adapter: IClickHouseAdapter,
  database: string,
  table: string,
  partName: string | null
): UseLineageTreeResult {
  const [lineage, setLineage] = useState<PartLineage | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());

  const cancelledRef = useRef(false);

  useEffect(() => {
    if (!partName || !database || !table) {
      setLineage(null);
      setError(null);
      setExpandedNodes(new Set());
      return;
    }

    cancelledRef.current = false;
    setIsLoading(true);
    setError(null);

    const explorer = new DatabaseExplorer(adapter);
    explorer
      .getPartLineage(database, table, partName)
      .then(result => {
        if (!cancelledRef.current) {
          setLineage(result);
          // Auto-expand the root node
          setExpandedNodes(new Set([result.root.part_name]));
          setIsLoading(false);
        }
      })
      .catch(err => {
        if (!cancelledRef.current) {
          setError(err instanceof Error ? err.message : String(err));
          setIsLoading(false);
        }
      });

    return () => {
      cancelledRef.current = true;
    };
  }, [adapter, database, table, partName]);

  const toggleNode = useCallback((nodeName: string) => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(nodeName)) {
        next.delete(nodeName);
      } else {
        next.add(nodeName);
      }
      return next;
    });
  }, []);

  const expandAll = useCallback(() => {
    if (!lineage) return;
    const allNames = collectNonLeafNodeNames(lineage.root);
    setExpandedNodes(new Set(allNames));
  }, [lineage]);

  const collapseAll = useCallback(() => {
    setExpandedNodes(new Set());
  }, []);

  const isExpanded = useCallback(
    (nodeName: string) => expandedNodes.has(nodeName),
    [expandedNodes]
  );

  return {
    lineage,
    isLoading,
    error,
    expandedNodes,
    toggleNode,
    expandAll,
    collapseAll,
    isExpanded,
  };
}
