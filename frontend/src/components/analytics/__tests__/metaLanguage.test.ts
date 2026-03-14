import { describe, test, expect } from 'vitest';
import {
  parseDirectives,
  parseRagRules,
  getRagColor,
  parseChartDirective,
  resolveQueryRef,
  type RagRule,
} from '../metaLanguage';
import {
  resolveTimeRange,
  resolveDrillParams,
} from '../templateResolution';

/* ═══════════════════════════════════════════════════════════════════════════
 * 1. parseDirectives — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('parseDirectives', () => {
  const cases: {
    name: string;
    sql: string;
    expected: Record<string, unknown> | null;
  }[] = [
    /* ── basics ── */
    {
      name: 'minimal: title + group only',
      sql: `-- @meta: title='CPU Usage' group='Overview'\nSELECT 1`,
      expected: {
        meta: { title: 'CPU Usage', group: 'Overview', description: undefined, interval: undefined },
        rag: [],
      },
    },
    {
      name: 'with description and interval',
      sql: `-- @meta: title='Insert Rate' group='Inserts' description='Inserts per minute' interval='2 HOUR'\nSELECT 1`,
      expected: {
        meta: { title: 'Insert Rate', group: 'Inserts', description: 'Inserts per minute', interval: '2 HOUR' },
      },
    },

    /* ── @chart combos ── */
    {
      name: 'chart: line + 3d',
      sql: `-- @meta: title='Parts' group='Parts'\n-- @chart: type=line labels=minute values=new_parts style=3d\nSELECT 1`,
      expected: {
        chart: { type: 'line', style: '3d', labelColumn: 'minute', valueColumn: 'new_parts', groupColumn: undefined },
      },
    },
    {
      name: 'chart: pie + 3d',
      sql: `-- @meta: title='DB Sizes' group='Overview'\n-- @chart: type=pie labels=database values=total_bytes style=3d\nSELECT 1`,
      expected: {
        chart: { type: 'pie', style: '3d', labelColumn: 'database', valueColumn: 'total_bytes', groupColumn: undefined },
      },
    },
    {
      name: 'chart: stacked_bar with group column',
      sql: `-- @meta: title='Duration' group='Self-Monitoring'\n-- @chart: type=stacked_bar labels=component values=value_ms group=metric unit=ms style=2d\nSELECT 1`,
      expected: {
        chart: { type: 'stacked_bar', style: '2d', labelColumn: 'component', valueColumn: 'value_ms', groupColumn: 'metric' },
      },
    },
    {
      name: 'chart: area + 2d',
      sql: `-- @meta: title='QPS' group='Advanced Dashboard'\n-- @chart: type=area labels=t values=qps style=2d\nSELECT 1`,
      expected: {
        chart: { type: 'area', style: '2d', labelColumn: 't', valueColumn: 'qps', groupColumn: undefined },
      },
    },

    /* ── @drill ── */
    {
      name: 'with drill directive',
      sql: `-- @meta: title='Table Sizes' group='Overview'\n-- @drill: on=database into='Table Sizes'\nSELECT 1`,
      expected: {
        drill: { on: 'database', into: 'Table Sizes' },
      },
    },

    /* ── @link ── */
    {
      name: 'with link directive',
      sql: `-- @meta: title='Query Cost' group='Self-Monitoring'\n-- @link: on=query_hash into='App Query Executions'\nSELECT 1`,
      expected: {
        link: { on: 'query_hash', into: 'App Query Executions' },
      },
    },

    /* ── @rag ── */
    {
      name: 'with ascending rag rule',
      sql: `-- @meta: title='Q' group='Overview'\n-- @rag: column=avg_memory_mb green<20 amber<50\nSELECT 1`,
      expected: {
        rag: [{ column: 'avg_memory_mb', direction: 'asc', greenThreshold: 20, amberThreshold: 50 }],
      },
    },
    {
      name: 'with descending rag rule',
      sql: `-- @meta: title='Q' group='Overview'\n-- @rag: column=hit_rate green>90 amber>70\nSELECT 1`,
      expected: {
        rag: [{ column: 'hit_rate', direction: 'desc', greenThreshold: 90, amberThreshold: 70 }],
      },
    },
    {
      name: 'with multiple rag rules',
      sql: `-- @meta: title='Q' group='Overview'\n-- @rag: column=avg_result_bytes green<10000 amber<100000\n-- @rag: column=avg_memory_mb green<20 amber<50\nSELECT 1`,
      expected: {
        rag: [
          { column: 'avg_result_bytes', direction: 'asc', greenThreshold: 10000, amberThreshold: 100000 },
          { column: 'avg_memory_mb', direction: 'asc', greenThreshold: 20, amberThreshold: 50 },
        ],
      },
    },

    /* ── source ── */
    {
      name: 'with source URL',
      sql: `-- @meta: title='Q' group='Overview'\n-- Source: https://clickhouse.com/blog/example\nSELECT 1`,
      expected: {
        source: 'https://clickhouse.com/blog/example',
      },
    },

    /* ── full combo ── */
    {
      name: 'all directives combined',
      sql: [
        `-- @meta: title='Full Query' group='Selects' description='Everything' interval='1 DAY'`,
        `-- @chart: type=bar labels=col1 values=col2 group=col3 style=2d`,
        `-- @drill: on=col1 into='Target Query'`,
        `-- @link: on=col4 into='Link Target'`,
        `-- @rag: column=col2 green<100 amber<500`,
        `-- Source: https://example.com/docs`,
        `SELECT 1`,
      ].join('\n'),
      expected: {
        meta: { title: 'Full Query', group: 'Selects', description: 'Everything', interval: '1 DAY' },
        chart: { type: 'bar', style: '2d', labelColumn: 'col1', valueColumn: 'col2', groupColumn: 'col3' },
        drill: { on: 'col1', into: 'Target Query' },
        link: { on: 'col4', into: 'Link Target' },
        rag: [{ column: 'col2', direction: 'asc', greenThreshold: 100, amberThreshold: 500 }],
        source: 'https://example.com/docs',
      },
    },

    /* ── null / invalid cases ── */
    {
      name: 'no @meta returns null',
      sql: `SELECT 1`,
      expected: null,
    },
    {
      name: '@meta missing title returns null',
      sql: `-- @meta: group='Overview'\nSELECT 1`,
      expected: null,
    },
    {
      name: '@meta missing group returns null',
      sql: `-- @meta: title='No Group'\nSELECT 1`,
      expected: null,
    },
  ];

  test.each(cases)('$name', ({ sql, expected }) => {
    const result = parseDirectives(sql);
    if (expected === null) {
      expect(result).toBeNull();
    } else {
      expect(result).not.toBeNull();
      for (const [key, value] of Object.entries(expected)) {
        expect(result).toHaveProperty(key, value);
      }
    }
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * 2. parseChartDirective — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('parseChartDirective', () => {
  const cases: {
    name: string;
    sql: string;
    expected: Record<string, unknown> | null;
  }[] = [
    {
      name: 'single value column',
      sql: `-- @chart: type=bar labels=minute values=count style=2d\nSELECT 1`,
      expected: { type: 'bar', labelColumn: 'minute', valueColumn: 'count', visualization: '2d' },
    },
    {
      name: 'multi-column values',
      sql: `-- @chart: type=grouped_line labels=t values=TCP_Connections,MySQL_Connections,HTTP_Connections style=2d\nSELECT 1`,
      expected: {
        type: 'grouped_line',
        labelColumn: 't',
        valueColumn: 'TCP_Connections',
        valueColumns: ['TCP_Connections', 'MySQL_Connections', 'HTTP_Connections'],
      },
    },
    {
      name: 'orientation vertical',
      sql: `-- @chart: type=bar labels=x values=y orientation=vertical\nSELECT 1`,
      expected: { orientation: 'vertical' },
    },
    {
      name: 'orientation horizontal',
      sql: `-- @chart: type=bar labels=x values=y orientation=horizontal\nSELECT 1`,
      expected: { orientation: 'horizontal' },
    },
    {
      name: 'orientation shorthand v',
      sql: `-- @chart: type=bar labels=x values=y orientation=v\nSELECT 1`,
      expected: { orientation: 'vertical' },
    },
    {
      name: 'unit parsed',
      sql: `-- @chart: type=stacked_bar labels=x values=y unit=ms\nSELECT 1`,
      expected: { unit: 'ms' },
    },
    {
      name: 'style 3d',
      sql: `-- @chart: type=line labels=x values=y style=3d\nSELECT 1`,
      expected: { visualization: '3d' },
    },
    {
      name: 'extracts title and description from @meta',
      sql: `-- @meta: title='My Chart' description='A description'\n-- @chart: type=pie labels=x values=y\nSELECT 1`,
      expected: { title: 'My Chart', description: 'A description' },
    },
    {
      name: 'group column',
      sql: `-- @chart: type=stacked_bar labels=x values=y group=category\nSELECT 1`,
      expected: { groupColumn: 'category' },
    },
    {
      name: 'no @chart returns null',
      sql: `-- @meta: title='Q' group='Overview'\nSELECT 1`,
      expected: null,
    },
  ];

  test.each(cases)('$name', ({ sql, expected }) => {
    const result = parseChartDirective(sql);
    if (expected === null) {
      expect(result).toBeNull();
    } else {
      expect(result).not.toBeNull();
      for (const [key, value] of Object.entries(expected)) {
        expect(result).toHaveProperty(key, value);
      }
    }
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * 3. parseRagRules — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('parseRagRules', () => {
  const cases: {
    name: string;
    sql: string;
    expected: { column: string; direction: 'asc' | 'desc'; greenThreshold: number; amberThreshold: number }[];
  }[] = [
    {
      name: 'ascending rule (lower is better)',
      sql: `-- @rag: column=latency green<50 amber<200`,
      expected: [{ column: 'latency', direction: 'asc', greenThreshold: 50, amberThreshold: 200 }],
    },
    {
      name: 'descending rule (higher is better)',
      sql: `-- @rag: column=cache_hit green>95 amber>80`,
      expected: [{ column: 'cache_hit', direction: 'desc', greenThreshold: 95, amberThreshold: 80 }],
    },
    {
      name: 'multiple rules',
      sql: `-- @rag: column=memory green<100 amber<500\n-- @rag: column=hit_rate green>90 amber>70`,
      expected: [
        { column: 'memory', direction: 'asc', greenThreshold: 100, amberThreshold: 500 },
        { column: 'hit_rate', direction: 'desc', greenThreshold: 90, amberThreshold: 70 },
      ],
    },
    {
      name: 'decimal thresholds',
      sql: `-- @rag: column=ratio green<0.5 amber<0.8`,
      expected: [{ column: 'ratio', direction: 'asc', greenThreshold: 0.5, amberThreshold: 0.8 }],
    },
    {
      name: 'no @rag returns empty array',
      sql: `SELECT 1`,
      expected: [],
    },
  ];

  test.each(cases)('$name', ({ sql, expected }) => {
    expect(parseRagRules(sql)).toEqual(expected);
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * 4. getRagColor — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('getRagColor', () => {
  const ascRule: RagRule[] = [
    { column: 'latency', direction: 'asc', greenThreshold: 50, amberThreshold: 200 },
  ];
  const descRule: RagRule[] = [
    { column: 'hit_rate', direction: 'desc', greenThreshold: 90, amberThreshold: 70 },
  ];

  const cases: { name: string; column: string; value: unknown; rules?: RagRule[]; expected: string | undefined }[] = [
    // ascending: lower is better
    { name: 'asc: green (below threshold)',   column: 'latency', value: 30,  rules: ascRule, expected: '#22c55e' },
    { name: 'asc: amber (between)',           column: 'latency', value: 100, rules: ascRule, expected: '#f59e0b' },
    { name: 'asc: red (above amber)',         column: 'latency', value: 300, rules: ascRule, expected: '#ef4444' },
    { name: 'asc: boundary at green',         column: 'latency', value: 50,  rules: ascRule, expected: '#f59e0b' },
    { name: 'asc: boundary at amber',         column: 'latency', value: 200, rules: ascRule, expected: '#ef4444' },
    // descending: higher is better
    { name: 'desc: green (above threshold)',  column: 'hit_rate', value: 95,  rules: descRule, expected: '#22c55e' },
    { name: 'desc: amber (between)',          column: 'hit_rate', value: 80,  rules: descRule, expected: '#f59e0b' },
    { name: 'desc: red (below amber)',        column: 'hit_rate', value: 50,  rules: descRule, expected: '#ef4444' },
    { name: 'desc: boundary at green',        column: 'hit_rate', value: 90,  rules: descRule, expected: '#f59e0b' },
    { name: 'desc: boundary at amber',        column: 'hit_rate', value: 70,  rules: descRule, expected: '#ef4444' },
    // edge cases
    { name: 'no rules → undefined',           column: 'latency', value: 10,  rules: undefined, expected: undefined },
    { name: 'column not in rules → undefined', column: 'other',  value: 10,  rules: ascRule,   expected: undefined },
    { name: 'non-numeric → undefined',         column: 'latency', value: 'abc', rules: ascRule, expected: undefined },
    { name: 'string number coerced',           column: 'latency', value: '30',  rules: ascRule, expected: '#22c55e' },
  ];

  test.each(cases)('$name', ({ column, value, rules, expected }) => {
    expect(getRagColor(column, value, rules)).toBe(expected);
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * 5. resolveTimeRange — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('resolveTimeRange', () => {
  const cases: {
    name: string;
    sql: string;
    defaultInterval?: string;
    userInterval?: string | null;
    expected: string;
  }[] = [
    {
      name: 'standard interval replaces placeholder',
      sql: `WHERE event_time > {{time_range}}`,
      defaultInterval: '1 DAY',
      expected: `WHERE event_time > now() - INTERVAL 1 DAY`,
    },
    {
      name: 'user interval overrides default',
      sql: `WHERE event_time > {{time_range}}`,
      defaultInterval: '1 DAY',
      userInterval: '6 HOUR',
      expected: `WHERE event_time > now() - INTERVAL 6 HOUR`,
    },
    {
      name: 'no placeholder → passthrough',
      sql: `SELECT 1`,
      defaultInterval: '1 DAY',
      expected: `SELECT 1`,
    },
    {
      name: 'no interval → passthrough',
      sql: `WHERE event_time > {{time_range}}`,
      expected: `WHERE event_time > {{time_range}}`,
    },
    {
      name: 'multiple placeholders replaced',
      sql: `WHERE a > {{time_range}} AND b > {{time_range}}`,
      defaultInterval: '2 HOUR',
      expected: `WHERE a > now() - INTERVAL 2 HOUR AND b > now() - INTERVAL 2 HOUR`,
    },
    {
      name: 'custom range: event_time pattern',
      sql: `WHERE event_time > {{time_range}}`,
      userInterval: 'CUSTOM:2025-01-01T10:00,2025-01-01T12:00',
      expected: `WHERE event_time > toDateTime('2025-01-01 10:00:00') AND event_time < toDateTime('2025-01-01 12:00:00')`,
    },
    {
      name: 'custom range: event_date toDate pattern',
      sql: `WHERE event_date >= toDate({{time_range}})`,
      userInterval: 'CUSTOM:2025-01-01T10:00,2025-01-01T12:00',
      expected: `WHERE event_date >= toDate('2025-01-01 10:00:00') AND event_date <= toDate('2025-01-01 12:00:00')`,
    },
    {
      name: 'custom range: fallback for remaining placeholders',
      sql: `WHERE x = {{time_range}}`,
      userInterval: 'CUSTOM:2025-06-01T08:00,2025-06-01T18:00',
      expected: `WHERE x = toDateTime('2025-06-01 08:00:00')`,
    },
    {
      name: 'custom range: normalises datetime without seconds',
      sql: `WHERE event_time > {{time_range}}`,
      userInterval: 'CUSTOM:2025-03-15T09:30,2025-03-15T17:45',
      expected: `WHERE event_time > toDateTime('2025-03-15 09:30:00') AND event_time < toDateTime('2025-03-15 17:45:00')`,
    },
  ];

  test.each(cases)('$name', ({ sql, defaultInterval, userInterval, expected }) => {
    expect(resolveTimeRange(sql, defaultInterval, userInterval)).toBe(expected);
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * 6. resolveDrillParams — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('resolveDrillParams', () => {
  const cases: {
    name: string;
    sql: string;
    params: Record<string, string>;
    expected: string;
  }[] = [
    {
      name: 'drill with value → equality condition',
      sql: `WHERE {{drill:database | 1=1}}`,
      params: { database: 'nyc_taxi' },
      expected: `WHERE database = 'nyc_taxi'`,
    },
    {
      name: 'drill without value → fallback',
      sql: `WHERE {{drill:database | 1=1}}`,
      params: {},
      expected: `WHERE 1=1`,
    },
    {
      name: 'drill_value with value → quoted value',
      sql: `WHERE query_hash = {{drill_value:query_hash | ''}}`,
      params: { query_hash: 'abc123' },
      expected: `WHERE query_hash = 'abc123'`,
    },
    {
      name: 'drill_value without value → fallback',
      sql: `WHERE query_hash = {{drill_value:query_hash | ''}}`,
      params: {},
      expected: `WHERE query_hash = ''`,
    },
    {
      name: 'multiple drill placeholders',
      sql: `WHERE {{drill:database | 1=1}} AND {{drill:table | 1=1}}`,
      params: { database: 'default', table: 'hits' },
      expected: `WHERE database = 'default' AND table = 'hits'`,
    },
    {
      name: 'mixed drill and drill_value',
      sql: `WHERE {{drill:component | 1=1}} AND t > {{drill_value:t | now()}}`,
      params: { component: 'api', t: '2025-01-01 00:00:00' },
      expected: `WHERE component = 'api' AND t > '2025-01-01 00:00:00'`,
    },
    {
      name: 'value with single quotes is escaped',
      sql: `WHERE {{drill:name | 1=1}}`,
      params: { name: "O'Reilly" },
      expected: `WHERE name = 'O''Reilly'`,
    },
    {
      name: 'drill_value with single quotes is escaped',
      sql: `WHERE x = {{drill_value:name | ''}}`,
      params: { name: "it's" },
      expected: `WHERE x = 'it''s'`,
    },
    {
      name: 'partial params — some resolved, some fallback',
      sql: `WHERE {{drill:database | 1=1}} AND {{drill:table | 1=1}}`,
      params: { database: 'system' },
      expected: `WHERE database = 'system' AND 1=1`,
    },
  ];

  test.each(cases)('$name', ({ sql, params, expected }) => {
    expect(resolveDrillParams(sql, params)).toBe(expected);
  });
});

/* ═══════════════════════════════════════════════════════════════════════════
 * 7. resolveQueryRef — table-driven
 * ═══════════════════════════════════════════════════════════════════════════ */

describe('resolveQueryRef', () => {
  const queries = [
    { name: 'Active Merges', group: 'Merges' },
    { name: 'Merge Errors', group: 'Merges' },
    { name: 'CPU Usage', group: 'Overview' },
    { name: 'CPU Usage', group: 'Advanced Dashboard' },
    { name: 'Table Sizes', group: 'Overview' },
  ];

  const cases: {
    name: string;
    ref: string;
    sourceGroup: string | undefined;
    expectedName: string | undefined;
    expectedGroup: string | undefined;
  }[] = [
    {
      name: 'namespaced ref — exact group + name',
      ref: 'Merges#Active Merges',
      sourceGroup: undefined,
      expectedName: 'Active Merges',
      expectedGroup: 'Merges',
    },
    {
      name: 'namespaced ref — cross-group drill',
      ref: 'Advanced Dashboard#CPU Usage',
      sourceGroup: 'Merges',
      expectedName: 'CPU Usage',
      expectedGroup: 'Advanced Dashboard',
    },
    {
      name: 'namespaced ref — wrong group returns undefined',
      ref: 'Resources#CPU Usage',
      sourceGroup: undefined,
      expectedName: undefined,
      expectedGroup: undefined,
    },
    {
      name: 'bare ref — prefers same group',
      ref: 'CPU Usage',
      sourceGroup: 'Overview',
      expectedName: 'CPU Usage',
      expectedGroup: 'Overview',
    },
    {
      name: 'bare ref — prefers same group (Advanced Dashboard)',
      ref: 'CPU Usage',
      sourceGroup: 'Advanced Dashboard',
      expectedName: 'CPU Usage',
      expectedGroup: 'Advanced Dashboard',
    },
    {
      name: 'bare ref — no sourceGroup falls back to first match',
      ref: 'CPU Usage',
      sourceGroup: undefined,
      expectedName: 'CPU Usage',
      expectedGroup: 'Overview',
    },
    {
      name: 'bare ref — sourceGroup has no match, falls back to any',
      ref: 'Table Sizes',
      sourceGroup: 'Merges',
      expectedName: 'Table Sizes',
      expectedGroup: 'Overview',
    },
    {
      name: 'bare ref — no match anywhere returns undefined',
      ref: 'Nonexistent Query',
      sourceGroup: 'Merges',
      expectedName: undefined,
      expectedGroup: undefined,
    },
    {
      name: 'namespaced ref — no match returns undefined',
      ref: 'Merges#Nonexistent',
      sourceGroup: undefined,
      expectedName: undefined,
      expectedGroup: undefined,
    },
  ];

  test.each(cases)('$name', ({ ref, sourceGroup, expectedName, expectedGroup }) => {
    const result = resolveQueryRef(ref, sourceGroup, queries);
    if (expectedName === undefined) {
      expect(result).toBeUndefined();
    } else {
      expect(result).toBeDefined();
      expect(result!.name).toBe(expectedName);
      expect(result!.group).toBe(expectedGroup);
    }
  });
});
