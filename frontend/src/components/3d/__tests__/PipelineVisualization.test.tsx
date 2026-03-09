/**
 * Tests for PipelineVisualization component
 * 
 * Tests the parsing of EXPLAIN PIPELINE output into a graph structure
 * and the correlation of nodes with trace logs.
 * 
 */

import { describe, it, expect } from 'vitest';
import { parsePipelineOutput } from '../PipelineVisualization';

describe('PipelineVisualization', () => {
  describe('parsePipelineOutput', () => {
    it('should parse empty output', () => {
      const result = parsePipelineOutput('');
      expect(result).toEqual([]);
    });

    it('should parse single node', () => {
      const output = 'ExpressionTransform';
      const result = parsePipelineOutput(output);
      
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('ExpressionTransform');
      expect(result[0].depth).toBe(0);
      expect(result[0].parentId).toBeNull();
      expect(result[0].childIds).toEqual([]);
    });

    it('should parse parenthesized stage names', () => {
      const output = '(Expression)';
      const result = parsePipelineOutput(output);
      
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Expression');
    });

    it('should parse nodes with parameters', () => {
      const output = 'MergeTreeSelect(pool: ReadPool, algorithm: Thread)';
      const result = parsePipelineOutput(output);
      
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('MergeTreeSelect');
      expect(result[0].metadata).toEqual({
        pool: 'ReadPool',
        algorithm: 'Thread',
      });
    });

    it('should parse hierarchical pipeline structure', () => {
      const output = `(Expression)
ExpressionTransform
  (ReadFromMergeTree)
  MergeTreeSelect(pool: ReadPool)`;
      
      const result = parsePipelineOutput(output);
      
      expect(result).toHaveLength(4);
      
      // Root node
      expect(result[0].name).toBe('Expression');
      expect(result[0].depth).toBe(0);
      expect(result[0].parentId).toBeNull();
      
      // Second level
      expect(result[1].name).toBe('ExpressionTransform');
      expect(result[1].depth).toBe(0);
      expect(result[1].parentId).toBeNull();
      
      // Third level (children of ExpressionTransform)
      expect(result[2].name).toBe('ReadFromMergeTree');
      expect(result[2].depth).toBe(1);
      expect(result[2].parentId).toBe(result[1].id);
      
      expect(result[3].name).toBe('MergeTreeSelect');
      expect(result[3].depth).toBe(1);
      expect(result[3].parentId).toBe(result[1].id);
    });

    it('should establish parent-child relationships', () => {
      const output = `Root
  Child1
    GrandChild1
  Child2`;
      
      const result = parsePipelineOutput(output);
      
      // Find nodes by name
      const root = result.find(n => n.name === 'Root');
      const child1 = result.find(n => n.name === 'Child1');
      const grandChild1 = result.find(n => n.name === 'GrandChild1');
      const child2 = result.find(n => n.name === 'Child2');
      
      expect(root).toBeDefined();
      expect(child1).toBeDefined();
      expect(grandChild1).toBeDefined();
      expect(child2).toBeDefined();
      
      // Check parent-child relationships
      expect(root!.childIds).toContain(child1!.id);
      expect(root!.childIds).toContain(child2!.id);
      expect(child1!.childIds).toContain(grandChild1!.id);
      expect(child1!.parentId).toBe(root!.id);
      expect(grandChild1!.parentId).toBe(child1!.id);
      expect(child2!.parentId).toBe(root!.id);
    });

    it('should handle complex real-world pipeline output', () => {
      const output = `(Expression)
ExpressionTransform × 4
  (Sorting)
  MergingSortedTransform 4 → 1
    MergeSortingTransform × 4
      (Expression)
      ExpressionTransform × 4
        (ReadFromMergeTree)
        MergeTreeSelect(pool: ReadPool, algorithm: Thread) × 4 0 → 1`;
      
      const result = parsePipelineOutput(output);
      
      // Should parse all nodes
      expect(result.length).toBeGreaterThan(0);
      
      // Check that all nodes have valid structure
      for (const node of result) {
        expect(node.id).toBeDefined();
        expect(node.name).toBeDefined();
        expect(node.name.length).toBeGreaterThan(0);
        expect(node.depth).toBeGreaterThanOrEqual(0);
        expect(Array.isArray(node.childIds)).toBe(true);
        expect(node.hasCorrelatedLogs).toBe(false);
        expect(node.correlatedLogCount).toBe(0);
      }
    });

    it('should preserve full text for each node', () => {
      const output = 'MergeTreeSelect(pool: ReadPool, algorithm: Thread) × 4 0 → 1';
      const result = parsePipelineOutput(output);
      
      expect(result).toHaveLength(1);
      expect(result[0].fullText).toBe(output);
    });

    it('should handle whitespace-only lines', () => {
      const output = `Root
  
  Child`;
      
      const result = parsePipelineOutput(output);
      
      // Should skip empty lines
      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('Root');
      expect(result[1].name).toBe('Child');
    });
  });

  describe('node structure', () => {
    it('should have all required fields', () => {
      const output = 'TestNode';
      const result = parsePipelineOutput(output);
      
      const node = result[0];
      
      // Check all required fields exist
      expect(node).toHaveProperty('id');
      expect(node).toHaveProperty('name');
      expect(node).toHaveProperty('fullText');
      expect(node).toHaveProperty('depth');
      expect(node).toHaveProperty('parentId');
      expect(node).toHaveProperty('childIds');
      expect(node).toHaveProperty('hasCorrelatedLogs');
      expect(node).toHaveProperty('correlatedLogCount');
      expect(node).toHaveProperty('metadata');
    });

    it('should generate unique IDs for each node', () => {
      const output = `Node1
Node2
Node3`;
      
      const result = parsePipelineOutput(output);
      const ids = result.map(n => n.id);
      const uniqueIds = new Set(ids);
      
      expect(uniqueIds.size).toBe(ids.length);
    });
  });
});
