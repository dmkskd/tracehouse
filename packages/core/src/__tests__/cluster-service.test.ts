import { describe, it, expect } from 'vitest';
import { ClusterService } from '../services/cluster-service.js';

describe('ClusterService.resolveTableRefs', { tags: ['cluster'] }, () => {
  const cluster = 'default';

  // =====================================================================
  // cluster_aware — system tables (existing functionality)
  // =====================================================================

  describe('cluster_aware with system tables', () => {
    it('resolves single system table with cluster', () => {
      const sql = 'SELECT * FROM {{cluster_aware:system.query_log}} WHERE type = 1';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe("SELECT * FROM clusterAllReplicas('default', system.query_log) WHERE type = 1");
    });

    it('resolves multiple system table placeholders', () => {
      const sql = `SELECT * FROM {{cluster_aware:system.query_log}} q
JOIN {{cluster_aware:system.query_thread_log}} t ON q.query_id = t.query_id`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
      expect(result).toContain("JOIN clusterAllReplicas('default', system.query_thread_log)");
    });

    it('strips system table placeholders on single node', () => {
      const sql = 'SELECT * FROM {{cluster_aware:system.query_log}} WHERE type = 1';
      const result = ClusterService.resolveTableRefs(sql, null);
      expect(result).toBe('SELECT * FROM system.query_log WHERE type = 1');
    });

    it('works with all common log table types', () => {
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
  });

  // =====================================================================
  // cluster_aware — tracehouse tables (new functionality)
  // =====================================================================

  describe('cluster_aware with tracehouse tables', () => {
    it('resolves tracehouse.processes_history with cluster', () => {
      const sql = 'SELECT * FROM {{cluster_aware:tracehouse.processes_history}} WHERE query_id = \'abc\'';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe("SELECT * FROM clusterAllReplicas('default', tracehouse.processes_history) WHERE query_id = 'abc'");
    });

    it('resolves tracehouse.merges_history with cluster', () => {
      const sql = 'SELECT * FROM {{cluster_aware:tracehouse.merges_history}} WHERE elapsed > 10';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe("SELECT * FROM clusterAllReplicas('default', tracehouse.merges_history) WHERE elapsed > 10");
    });

    it('strips tracehouse placeholders on single node', () => {
      const sql = 'SELECT * FROM {{cluster_aware:tracehouse.processes_history}} WHERE query_id = \'abc\'';
      const result = ClusterService.resolveTableRefs(sql, null);
      expect(result).toBe("SELECT * FROM tracehouse.processes_history WHERE query_id = 'abc'");
    });

    it('resolves tracehouse table in nested subquery', () => {
      const sql = `SELECT t, memory_mb FROM (
    SELECT sample_time, memory_usage / (1024*1024) AS memory_mb
    FROM {{cluster_aware:tracehouse.processes_history}}
    WHERE query_id = 'test'
)`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', tracehouse.processes_history)");
    });

    it('works with any database-qualified table', () => {
      const sql = 'SELECT * FROM {{cluster_aware:mydb.my_table}}';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe("SELECT * FROM clusterAllReplicas('default', mydb.my_table)");
    });
  });

  // =====================================================================
  // Mixed system + tracehouse in same query
  // =====================================================================

  describe('mixed system and tracehouse placeholders', () => {
    it('resolves both system and tracehouse tables in same query', () => {
      const sql = `SELECT ql.query_id, ph.memory_mb
FROM {{cluster_aware:system.query_log}} ql
JOIN {{cluster_aware:tracehouse.processes_history}} ph ON ql.query_id = ph.query_id`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
      expect(result).toContain("JOIN clusterAllReplicas('default', tracehouse.processes_history)");
    });

    it('strips both on single node', () => {
      const sql = `SELECT * FROM {{cluster_aware:system.query_log}} ql
JOIN {{cluster_aware:tracehouse.processes_history}} ph ON 1=1`;
      const result = ClusterService.resolveTableRefs(sql, null);
      expect(result).toContain('FROM system.query_log');
      expect(result).toContain('JOIN tracehouse.processes_history');
    });

    it('resolves cluster_aware + cluster_name together', () => {
      const sql = `SELECT * FROM {{cluster_aware:tracehouse.processes_history}}
WHERE hostname IN (SELECT host_name FROM {{cluster_aware:system.clusters}} WHERE cluster = {{cluster_name}})`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', tracehouse.processes_history)");
      expect(result).toContain("FROM clusterAllReplicas('default', system.clusters)");
      expect(result).toContain("cluster = 'default'");
    });

    it('resolves metadata tables with cluster', () => {
      const tables = [
        'system.tables', 'system.columns', 'system.databases',
        'system.parts', 'system.parts_columns', 'system.replicas',
        'system.dictionaries', 'system.disks', 'system.mutations',
      ];
      for (const table of tables) {
        const sql = `SELECT * FROM {{cluster_aware:${table}}}`;
        const result = ClusterService.resolveTableRefs(sql, cluster);
        expect(result).toBe(`SELECT * FROM clusterAllReplicas('default', ${table})`);
      }
    });

    it('strips metadata placeholders on single node', () => {
      const sql = 'SELECT * FROM {{cluster_aware:system.parts}} WHERE active = 1';
      const result = ClusterService.resolveTableRefs(sql, null);
      expect(result).toBe('SELECT * FROM system.parts WHERE active = 1');
    });
  });

  // =====================================================================
  // cluster_name
  // =====================================================================

  describe('cluster_name placeholder', () => {
    it('resolves to quoted cluster name', () => {
      const sql = "SELECT * FROM system.clusters WHERE cluster = {{cluster_name}}";
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe("SELECT * FROM system.clusters WHERE cluster = 'default'");
    });

    it('resolves to empty string when no cluster', () => {
      const sql = "SELECT * FROM system.clusters WHERE cluster = {{cluster_name}}";
      const result = ClusterService.resolveTableRefs(sql, null);
      expect(result).toBe("SELECT * FROM system.clusters WHERE cluster = ''");
    });

    it('resolves cluster name with dots and hyphens', () => {
      const sql = "SELECT cluster = {{cluster_name}}";
      expect(ClusterService.resolveTableRefs(sql, 'my-cluster.v2')).toBe("SELECT cluster = 'my-cluster.v2'");
    });
  });

  // =====================================================================
  // Bare references — should NOT be touched
  // =====================================================================

  describe('bare references are untouched', () => {
    it('does not modify bare system.X references', () => {
      const sql = 'SELECT * FROM system.processes WHERE is_cancelled = 0';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe(sql);
    });

    it('does not modify bare tracehouse.X references', () => {
      const sql = 'SELECT * FROM tracehouse.processes_history WHERE query_id = \'abc\'';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toBe(sql);
    });

    it('does not touch table refs inside string literals', () => {
      const sql = `SELECT * FROM {{cluster_aware:system.query_log}}
WHERE query NOT LIKE '%source:TraceHouse:%'`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', system.query_log)");
      expect(result).toContain("'%source:TraceHouse:%'");
    });
  });

  // =====================================================================
  // SQL injection protection
  // =====================================================================

  describe('SQL injection protection', () => {
    it('rejects cluster names with SQL injection', () => {
      const sql = 'SELECT * FROM {{cluster_aware:system.query_log}}';
      expect(ClusterService.resolveTableRefs(sql, "'); DROP TABLE --")).toBe('SELECT * FROM system.query_log');
      expect(ClusterService.resolveTableRefs(sql, "foo' OR '1'='1")).toBe('SELECT * FROM system.query_log');
    });

    it('rejects cluster names with SQL injection for tracehouse tables', () => {
      const sql = 'SELECT * FROM {{cluster_aware:tracehouse.processes_history}}';
      expect(ClusterService.resolveTableRefs(sql, "'; DROP --")).toBe('SELECT * FROM tracehouse.processes_history');
    });

    it('rejects cluster names with SQL injection for cluster_name', () => {
      const sql = "SELECT * FROM system.clusters WHERE cluster = {{cluster_name}}";
      expect(ClusterService.resolveTableRefs(sql, "'; DROP TABLE --")).toBe(
        "SELECT * FROM system.clusters WHERE cluster = ''"
      );
    });

    it('accepts valid cluster names', () => {
      const sql = 'SELECT * FROM {{cluster_aware:system.query_log}}';
      expect(ClusterService.resolveTableRefs(sql, 'valid_name-1.0')).toContain('clusterAllReplicas');
    });
  });

  // =====================================================================
  // Multiline and formatting edge cases
  // =====================================================================

  describe('formatting edge cases', () => {
    it('handles multiline SQL', () => {
      const sql = `SELECT count()
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'NewPart'`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', system.part_log)");
    });

    it('handles multiple placeholders on the same line', () => {
      const sql = '{{cluster_aware:system.query_log}} {{cluster_aware:system.text_log}}';
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("clusterAllReplicas('default', system.query_log)");
      expect(result).toContain("clusterAllReplicas('default', system.text_log)");
    });

    it('preserves SQL around placeholders', () => {
      const sql = `SELECT ql.query_id, ql.type
FROM {{cluster_aware:tracehouse.processes_history}} ph
WHERE ph.sample_time >= now() - INTERVAL 1 HOUR
  AND ph.query_id IN (SELECT query_id FROM {{cluster_aware:system.query_log}} WHERE type = 2)
ORDER BY ph.sample_time DESC
LIMIT 100`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', tracehouse.processes_history) ph");
      expect(result).toContain("FROM clusterAllReplicas('default', system.query_log) WHERE type = 2");
      expect(result).toContain('ORDER BY ph.sample_time DESC');
      expect(result).toContain('LIMIT 100');
    });

    it('returns unchanged SQL when no placeholders present', () => {
      const sql = 'SELECT 1';
      expect(ClusterService.resolveTableRefs(sql, cluster)).toBe(sql);
      expect(ClusterService.resolveTableRefs(sql, null)).toBe(sql);
    });
  });

  // =====================================================================
  // Analytics query resolution — real-world queries from selfMonitoring
  // =====================================================================

  describe('analytics query resolution', () => {
    it('resolves Sampler Sample Gaps query (cluster_aware inside subquery)', () => {
      const sql = `SELECT
    hostname,
    prev_time,
    sample_time AS gap_start,
    round(gap_seconds, 1) AS gap_seconds
FROM (
    SELECT
        hostname,
        sample_time,
        lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time) AS prev_time,
        date_diff('millisecond', lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time), sample_time) / 1000.0 AS gap_seconds
    FROM {{cluster_aware:tracehouse.processes_history}}
    WHERE sample_time > now() - INTERVAL 6 HOUR
)
WHERE gap_seconds > 5
  AND prev_time > toDateTime64('1970-01-01 00:00:01', 3)
ORDER BY gap_seconds DESC
LIMIT 50`;
      const result = ClusterService.resolveTableRefs(sql, cluster);
      expect(result).toContain("FROM clusterAllReplicas('default', tracehouse.processes_history)");
      expect(result).not.toContain('{{cluster_aware');
    });

    it('resolves Sampler Sample Gaps query on single node', () => {
      const sql = `SELECT hostname FROM (
    SELECT hostname, sample_time
    FROM {{cluster_aware:tracehouse.processes_history}}
    WHERE sample_time > now() - INTERVAL 6 HOUR
)`;
      const result = ClusterService.resolveTableRefs(sql, null);
      expect(result).toContain('FROM tracehouse.processes_history');
      expect(result).not.toContain('{{cluster_aware');
    });

    it('resolves cluster_aware after time_range is already resolved (dashboard execution path)', () => {
      // Simulates what DashboardViewer does:
      // 1. resolveTimeRange replaces {{time_range}}
      // 2. adapter.executeQuery → ClusterService.resolveTableRefs replaces {{cluster_aware:...}}
      const rawSql = `SELECT
    hostname,
    prev_time,
    sample_time AS gap_start,
    round(gap_seconds, 1) AS gap_seconds
FROM (
    SELECT
        hostname,
        sample_time,
        lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time) AS prev_time,
        date_diff('millisecond', lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time), sample_time) / 1000.0 AS gap_seconds
    FROM {{cluster_aware:tracehouse.processes_history}}
    WHERE sample_time > {{time_range}}
)
WHERE gap_seconds > 5
  AND prev_time > toDateTime64('1970-01-01 00:00:01', 3)
ORDER BY gap_seconds DESC
LIMIT 50`;

      // Step 1: simulate resolveTimeRange (replaces {{time_range}} only)
      const afterTimeRange = rawSql.replaceAll('{{time_range}}', 'now() - INTERVAL 6 HOUR');
      expect(afterTimeRange).toContain('{{cluster_aware:tracehouse.processes_history}}');

      // Step 2: simulate ClusterAwareAdapter calling resolveTableRefs
      const afterCluster = ClusterService.resolveTableRefs(afterTimeRange, cluster);
      expect(afterCluster).toContain("clusterAllReplicas('default', tracehouse.processes_history)");
      expect(afterCluster).not.toContain('{{');
      expect(afterCluster).not.toContain('}}');

      // Step 2b: same but single-node
      const afterClusterNull = ClusterService.resolveTableRefs(afterTimeRange, null);
      expect(afterClusterNull).toContain('FROM tracehouse.processes_history');
      expect(afterClusterNull).not.toContain('{{');
      expect(afterClusterNull).not.toContain('}}');
    });
  });

  // =====================================================================
  // Regex boundary — things that should NOT match
  // =====================================================================

  describe('regex boundaries', () => {
    it('does not match unqualified table names', () => {
      const sql = 'SELECT * FROM {{cluster_aware:query_log}}';
      // No dot → regex won't match → placeholder stays as-is
      expect(ClusterService.resolveTableRefs(sql, cluster)).toBe(sql);
    });

    it('does not match three-part names', () => {
      const sql = 'SELECT * FROM {{cluster_aware:a.b.c}}';
      // a.b.c won't match \w+\.\w+ as a whole
      expect(ClusterService.resolveTableRefs(sql, cluster)).toBe(sql);
    });

    it('does not match empty placeholders', () => {
      const sql = 'SELECT * FROM {{cluster_aware:}}';
      expect(ClusterService.resolveTableRefs(sql, cluster)).toBe(sql);
    });
  });
});
