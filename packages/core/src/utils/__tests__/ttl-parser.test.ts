import { describe, it, expect } from 'vitest';
import { parseTTL, formatTTLDuration } from '../ttl-parser.js';

describe('parseTTL', () => {
  describe('toIntervalDay syntax', () => {
    it('parses TTL event_date + toIntervalDay(30)', () => {
      const ddl = `CREATE TABLE system.query_log (...) ENGINE = MergeTree ORDER BY event_date TTL event_date + toIntervalDay(30) DELETE SETTINGS ...`;
      expect(parseTTL(ddl)).toBe('30 days');
    });

    it('parses TTL with 1 day singular', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalDay(1) DELETE`;
      expect(parseTTL(ddl)).toBe('1 day');
    });

    it('parses toIntervalMonth', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalMonth(3)`;
      expect(parseTTL(ddl)).toBe('3 months');
    });

    it('parses toIntervalHour', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalHour(24) DELETE`;
      expect(parseTTL(ddl)).toBe('24 hours');
    });

    it('parses toIntervalWeek', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalWeek(2) DELETE`;
      expect(parseTTL(ddl)).toBe('2 weeks');
    });

    it('parses toIntervalYear', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalYear(1) DELETE`;
      expect(parseTTL(ddl)).toBe('1 year');
    });

    it('parses toIntervalMinute', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_time + toIntervalMinute(30) DELETE`;
      expect(parseTTL(ddl)).toBe('30 minutes');
    });

    it('parses toIntervalSecond', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_time + toIntervalSecond(3600) DELETE`;
      expect(parseTTL(ddl)).toBe('3600 seconds');
    });
  });

  describe('INTERVAL N UNIT syntax', () => {
    it('parses TTL event_date + INTERVAL 30 DAY', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + INTERVAL 30 DAY DELETE`;
      expect(parseTTL(ddl)).toBe('30 days');
    });

    it('parses INTERVAL 7 DAY', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + INTERVAL 7 DAY`;
      expect(parseTTL(ddl)).toBe('7 days');
    });

    it('parses INTERVAL 1 MONTH', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + INTERVAL 1 MONTH DELETE`;
      expect(parseTTL(ddl)).toBe('1 month');
    });

    it('parses INTERVAL 6 HOUR', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_time + INTERVAL 6 HOUR DELETE`;
      expect(parseTTL(ddl)).toBe('6 hours');
    });
  });

  describe('TTL with SETTINGS after DELETE', () => {
    it('stops at SETTINGS keyword', () => {
      const ddl = `CREATE TABLE system.query_log (event_date Date, ...) ENGINE = MergeTree ORDER BY event_date TTL event_date + toIntervalDay(30) SETTINGS index_granularity = 8192`;
      expect(parseTTL(ddl)).toBe('30 days');
    });
  });

  describe('TTL with TO DISK / TO VOLUME', () => {
    it('stops at TO DISK', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalDay(7) TO DISK 'cold'`;
      expect(parseTTL(ddl)).toBe('7 days');
    });

    it('stops at TO VOLUME', () => {
      const ddl = `CREATE TABLE t (...) ENGINE = MergeTree ORDER BY d TTL event_date + toIntervalDay(14) TO VOLUME 'archive'`;
      expect(parseTTL(ddl)).toBe('14 days');
    });
  });

  describe('no TTL', () => {
    it('returns null for DDL without TTL', () => {
      const ddl = `CREATE TABLE system.query_log (event_date Date, ...) ENGINE = MergeTree ORDER BY event_date SETTINGS index_granularity = 8192`;
      expect(parseTTL(ddl)).toBeNull();
    });

    it('returns null for empty string', () => {
      expect(parseTTL('')).toBeNull();
    });

    it('returns null for undefined-like input', () => {
      expect(parseTTL(null as unknown as string)).toBeNull();
    });
  });

  describe('realistic system table DDLs', () => {
    it('parses a full system.query_log DDL', () => {
      const ddl = `CREATE TABLE system.query_log (\`hostname\` LowCardinality(String), \`type\` Enum8('QueryStart' = 1, 'QueryFinish' = 2, 'ExceptionBeforeStart' = 3, 'ExceptionWhileProcessing' = 4), \`event_date\` Date, \`event_time\` DateTime, \`event_time_microseconds\` DateTime64(6), \`query_start_time\` DateTime, \`query_start_time_microseconds\` DateTime64(6), \`query_duration_ms\` UInt64, \`read_rows\` UInt64, \`read_bytes\` UInt64, \`written_rows\` UInt64, \`written_bytes\` UInt64, \`result_rows\` UInt64, \`result_bytes\` UInt64, \`memory_usage\` Int64, \`current_database\` String, \`query\` String, \`formatted_query\` String, \`normalized_query_hash\` UInt64, \`query_kind\` LowCardinality(String), \`databases\` Array(LowCardinality(String)), \`tables\` Array(LowCardinality(String)), \`columns\` Array(LowCardinality(String)), \`partitions\` Array(LowCardinality(String)), \`projections\` Array(LowCardinality(String)), \`views\` Array(LowCardinality(String)), \`exception_code\` Int32, \`exception\` String, \`stack_trace\` String, \`is_initial_query\` UInt8, \`user\` String, \`query_id\` String, \`address\` IPv6, \`port\` UInt16, \`initial_user\` String, \`initial_query_id\` String, \`initial_address\` IPv6, \`initial_port\` UInt16, \`initial_query_start_time\` DateTime, \`initial_query_start_time_microseconds\` DateTime64(6), \`interface\` UInt8, \`is_secure\` UInt8, \`os_user\` String, \`client_hostname\` String, \`client_name\` String, \`client_revision\` UInt32, \`client_version_major\` UInt32, \`client_version_minor\` UInt32, \`client_version_patch\` UInt32, \`http_method\` UInt8, \`http_user_agent\` String, \`http_referer\` String, \`forwarded_for\` String, \`quota_key\` String, \`revision\` UInt32, \`log_comment\` String, \`thread_ids\` Array(UInt64), \`ProfileEvents\` Map(String, UInt64), \`Settings\` Map(String, String), \`used_aggregate_functions\` Array(String), \`used_aggregate_function_combinators\` Array(String), \`used_database_engines\` Array(String), \`used_data_type_families\` Array(String), \`used_dictionaries\` Array(String), \`used_formats\` Array(String), \`used_functions\` Array(String), \`used_storages\` Array(String), \`used_table_functions\` Array(String), \`used_row_policies\` Array(String), \`transaction_id\` Tuple(UInt64, UInt64, UUID), \`query_cache_usage\` Enum8('Unknown' = 0, 'None' = 1, 'Write' = 2, 'Read' = 3), \`asynchronous_read_counters\` Map(String, UInt64)) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) TTL event_date + toIntervalDay(30) SETTINGS index_granularity = 8192`;
      expect(parseTTL(ddl)).toBe('30 days');
    });

    it('parses system.metric_log with no TTL', () => {
      const ddl = `CREATE TABLE system.metric_log (\`hostname\` LowCardinality(String), \`event_date\` Date, \`event_time\` DateTime, ...) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 8192`;
      expect(parseTTL(ddl)).toBeNull();
    });
  });
});

describe('formatTTLDuration', () => {
  it('singular day', () => expect(formatTTLDuration(1, 'day')).toBe('1 day'));
  it('plural days', () => expect(formatTTLDuration(7, 'day')).toBe('7 days'));
  it('singular month', () => expect(formatTTLDuration(1, 'month')).toBe('1 month'));
  it('plural months', () => expect(formatTTLDuration(6, 'month')).toBe('6 months'));
  it('singular hour', () => expect(formatTTLDuration(1, 'hour')).toBe('1 hour'));
  it('plural hours', () => expect(formatTTLDuration(24, 'hour')).toBe('24 hours'));
  it('handles unit with trailing s', () => expect(formatTTLDuration(3, 'days')).toBe('3 days'));
  it('unknown unit passes through', () => expect(formatTTLDuration(5, 'quarter')).toBe('5 quarter'));
});
