import { describe, it, expect } from 'vitest';
import { ClusterService } from '../services/cluster-service.js';

describe('ClusterService.resolveTableRefs', () => {
  const cluster = 'default';

  it('resolves {{cluster_aware:system.query_log}} with cluster', () => {
    const sql = 'SELECT * FROM {{cluster_aware:system.query_log}} WHERE type = 1';
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toBe("SELECT * FROM clusterAllReplicas('default', system.query_log) WHERE type = 1");
  });

  it('resolves multiple placeholders', () => {
    const sql = `SELECT * FROM {{cluster_aware:system.query_log}} q
JOIN {{cluster_aware:system.query_thread_log}} t ON q.query_id = t.query_id`;
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
    expect(result).toContain("JOIN clusterAllReplicas('default', system.query_thread_log)");
  });

  it('strips placeholders when no cluster (single node)', () => {
    const sql = 'SELECT * FROM {{cluster_aware:system.query_log}} WHERE type = 1';
    const result = ClusterService.resolveTableRefs(sql, null);
    expect(result).toBe('SELECT * FROM system.query_log WHERE type = 1');
  });

  it('leaves bare system.X references untouched', () => {
    const sql = 'SELECT * FROM system.processes WHERE is_cancelled = 0';
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toBe(sql);
  });

  it('does not touch system.X inside string literals', () => {
    const sql = `SELECT * FROM {{cluster_aware:system.query_log}}
WHERE query NOT LIKE '%source:Monitor:%'`;
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
    // The LIKE string should be untouched
    expect(result).toContain("'%source:Monitor:%'");
  });

  it('handles multiline SQL', () => {
    const sql = `SELECT count()
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'NewPart'`;
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toContain("FROM clusterAllReplicas('default', system.part_log)");
  });

  it('rejects malicious cluster names', () => {
    const sql = 'SELECT * FROM {{cluster_aware:system.query_log}}';
    // Should strip placeholder — name fails validation
    expect(ClusterService.resolveTableRefs(sql, "'); DROP TABLE --")).toBe('SELECT * FROM system.query_log');
    expect(ClusterService.resolveTableRefs(sql, "foo' OR '1'='1")).toBe('SELECT * FROM system.query_log');
    // Valid name should work
    expect(ClusterService.resolveTableRefs(sql, 'valid_name-1.0')).toContain('clusterAllReplicas');
  });

  it('handles mixed placeholders and bare references', () => {
    const sql = `SELECT * FROM {{cluster_aware:system.query_log}} q
JOIN system.tables t ON t.name = 'foo'`;
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
    expect(result).toContain('JOIN system.tables');
  });

  it('works with all log table types', () => {
    const tables = [
      'system.query_log',
      'system.query_thread_log',
      'system.part_log',
      'system.text_log',
      'system.trace_log',
      'system.metric_log',
      'system.asynchronous_metric_log',
      'system.opentelemetry_span_log',
      'system.processors_profile_log',
    ];
    for (const table of tables) {
      const sql = `SELECT * FROM {{cluster_aware:${table}}}`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe(`SELECT * FROM clusterAllReplicas('default', ${table})`);
    }
  });

  it('works with metadata tables (parts, tables, columns, etc.)', () => {
    const tables = [
      'system.tables',
      'system.columns',
      'system.databases',
      'system.parts',
      'system.parts_columns',
      'system.replicas',
      'system.dictionaries',
      'system.disks',
      'system.mutations',
    ];
    for (const table of tables) {
      const sql = `SELECT * FROM {{cluster_metadata:${table}}}`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe(`SELECT * FROM clusterAllReplicas('default', ${table})`);
    }
  });

  it('strips cluster_metadata placeholders when no cluster (single node)', () => {
    const sql = 'SELECT * FROM {{cluster_metadata:system.parts}} WHERE active = 1';
    const result = ClusterService.resolveTableRefs(sql, null);
    expect(result).toBe('SELECT * FROM system.parts WHERE active = 1');
  });

  it('handles mixed cluster_aware and cluster_metadata in same query', () => {
    const sql = `SELECT * FROM {{cluster_aware:system.query_log}} q
JOIN {{cluster_metadata:system.tables}} t ON t.name = 'foo'`;
    const result = ClusterService.resolveTableRefs(sql, cluster);
    expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
    expect(result).toContain("JOIN clusterAllReplicas('default', system.tables)");
  });

  it('rejects malicious cluster names for cluster_metadata too', () => {
    const sql = 'SELECT * FROM {{cluster_metadata:system.parts}}';
    expect(ClusterService.resolveTableRefs(sql, "'); DROP TABLE --")).toBe('SELECT * FROM system.parts');
  });
});
