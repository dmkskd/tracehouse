import { describe, it, expect } from 'vitest';
import {
  parseExplainIndexesJson,
  parseIndexEntry,
  parseRatio,
} from '../explain-parser.js';

// ─── parseRatio ─────────────────────────────────────────────────────────────

describe('parseRatio', () => {
  it('parses "1/1083"', () => {
    expect(parseRatio('1/1083')).toEqual({ selected: 1, total: 1083 });
  });

  it('parses "2/5"', () => {
    expect(parseRatio('2/5')).toEqual({ selected: 2, total: 5 });
  });

  it('parses with spaces "3 / 10"', () => {
    expect(parseRatio('3 / 10')).toEqual({ selected: 3, total: 10 });
  });

  it('returns null for null/undefined', () => {
    expect(parseRatio(null)).toBeNull();
    expect(parseRatio(undefined)).toBeNull();
  });

  it('returns null for invalid format', () => {
    expect(parseRatio('abc')).toBeNull();
    expect(parseRatio('1-2')).toBeNull();
  });
});

// ─── parseIndexEntry ────────────────────────────────────────────────────────

describe('parseIndexEntry', () => {
  it('parses a PrimaryKey entry', () => {
    const entry = parseIndexEntry({
      Type: 'PrimaryKey',
      Keys: ['UserID'],
      Condition: '(UserID in [749927693, 749927693])',
      Parts: '1/1',
      Granules: '1/1083',
    });
    expect(entry.type).toBe('PrimaryKey');
    expect(entry.keys).toEqual(['UserID']);
    expect(entry.condition).toBe('(UserID in [749927693, 749927693])');
    expect(entry.parts).toEqual({ selected: 1, total: 1 });
    expect(entry.granules).toEqual({ selected: 1, total: 1083 });
    expect(entry.name).toBeUndefined();
  });

  it('parses a MinMax entry', () => {
    const entry = parseIndexEntry({
      Type: 'MinMax',
      Keys: ['y'],
      Condition: '(y in [1, +inf))',
      Parts: '4/5',
      Granules: '11/12',
    });
    expect(entry.type).toBe('MinMax');
    expect(entry.keys).toEqual(['y']);
    expect(entry.parts).toEqual({ selected: 4, total: 5 });
    expect(entry.granules).toEqual({ selected: 11, total: 12 });
  });

  it('parses a Skip index entry with Name', () => {
    const entry = parseIndexEntry({
      Type: 'Skip',
      Name: 'idx_country',
      Keys: ['country_code'],
      Condition: '(country_code = \'US\')',
      Description: 'minmax GRANULARITY 4',
      Parts: '2/5',
      Granules: '8/100',
    });
    expect(entry.type).toBe('Skip');
    expect(entry.name).toBe('idx_country');
    expect(entry.description).toBe('minmax GRANULARITY 4');
    expect(entry.keys).toEqual(['country_code']);
  });

  it('handles missing optional fields', () => {
    const entry = parseIndexEntry({ Type: 'PrimaryKey' });
    expect(entry.type).toBe('PrimaryKey');
    expect(entry.keys).toEqual([]);
    expect(entry.condition).toBe('');
    expect(entry.parts).toBeNull();
    expect(entry.granules).toBeNull();
    expect(entry.name).toBeUndefined();
  });
});

// ─── parseExplainIndexesJson ────────────────────────────────────────────────

describe('parseExplainIndexesJson', () => {
  it('returns error for empty input', () => {
    const result = parseExplainIndexesJson('');
    expect(result.success).toBe(false);
    expect(result.error).toContain('Empty');
  });

  it('returns error for invalid JSON', () => {
    const result = parseExplainIndexesJson('not json at all');
    expect(result.success).toBe(false);
    expect(result.error).toContain('parse');
  });

  it('parses a simple plan with PrimaryKey index', () => {
    // Realistic EXPLAIN json = 1, indexes = 1 output
    const json = JSON.stringify([
      {
        Plan: {
          'Node Type': 'Expression',
          Plans: [
            {
              'Node Type': 'ReadFromMergeTree',
              Indexes: [
                {
                  Type: 'PrimaryKey',
                  Keys: ['event_date', 'user_id'],
                  Condition: '(event_date in [19750, 19750]) AND (user_id in [42, 42])',
                  Parts: '1/3',
                  Granules: '2/1083',
                },
              ],
            },
          ],
        },
      },
    ]);

    const result = parseExplainIndexesJson(json);
    expect(result.success).toBe(true);
    expect(result.indexes).toHaveLength(1);
    expect(result.primaryKey).not.toBeNull();
    expect(result.primaryKey!.type).toBe('PrimaryKey');
    expect(result.primaryKey!.keys).toEqual(['event_date', 'user_id']);
    expect(result.primaryKey!.granules).toEqual({ selected: 2, total: 1083 });
    expect(result.primaryKey!.parts).toEqual({ selected: 1, total: 3 });
    expect(result.skipIndexes).toHaveLength(0);
  });

  it('parses multiple indexes (MinMax + Partition + PrimaryKey)', () => {
    const json = JSON.stringify([
      {
        Plan: {
          'Node Type': 'Expression',
          Plans: [
            {
              'Node Type': 'ReadFromMergeTree',
              Indexes: [
                {
                  Type: 'MinMax',
                  Keys: ['y'],
                  Condition: '(y in [1, +inf))',
                  Parts: '4/5',
                  Granules: '11/12',
                },
                {
                  Type: 'Partition',
                  Keys: ['y', 'bitAnd(z, 3)'],
                  Condition: 'and((bitAnd(z, 3) not in [1, 1]), ...)',
                  Parts: '3/4',
                  Granules: '10/11',
                },
                {
                  Type: 'PrimaryKey',
                  Keys: ['x', 'y'],
                  Condition: 'and((x in [11, +inf)), (y in [1, +inf)))',
                  Parts: '2/3',
                  Granules: '6/10',
                },
              ],
            },
          ],
        },
      },
    ]);

    const result = parseExplainIndexesJson(json);
    expect(result.success).toBe(true);
    expect(result.indexes).toHaveLength(3);
    expect(result.primaryKey!.keys).toEqual(['x', 'y']);
    expect(result.primaryKey!.granules).toEqual({ selected: 6, total: 10 });
    expect(result.skipIndexes).toHaveLength(0);
  });

  it('parses Skip indexes', () => {
    const json = JSON.stringify([
      {
        Plan: {
          'Node Type': 'ReadFromMergeTree',
          Indexes: [
            {
              Type: 'PrimaryKey',
              Keys: ['event_date'],
              Condition: '(event_date in [19750, 19750])',
              Parts: '1/3',
              Granules: '100/1083',
            },
            {
              Type: 'Skip',
              Name: 'idx_country',
              Keys: ['country_code'],
              Condition: "(country_code = 'US')",
              Description: 'minmax GRANULARITY 4',
              Parts: '1/1',
              Granules: '25/100',
            },
          ],
        },
      },
    ]);

    const result = parseExplainIndexesJson(json);
    expect(result.success).toBe(true);
    expect(result.indexes).toHaveLength(2);
    expect(result.primaryKey!.keys).toEqual(['event_date']);
    expect(result.skipIndexes).toHaveLength(1);
    expect(result.skipIndexes[0].name).toBe('idx_country');
    expect(result.skipIndexes[0].granules).toEqual({ selected: 25, total: 100 });
  });

  it('handles plan with no Indexes (e.g. non-MergeTree)', () => {
    const json = JSON.stringify([
      {
        Plan: {
          'Node Type': 'Expression',
          Plans: [
            {
              'Node Type': 'ReadFromStorage',
            },
          ],
        },
      },
    ]);

    const result = parseExplainIndexesJson(json);
    expect(result.success).toBe(true);
    expect(result.indexes).toHaveLength(0);
    expect(result.primaryKey).toBeNull();
  });

  it('handles deeply nested plan tree', () => {
    const json = JSON.stringify([
      {
        Plan: {
          'Node Type': 'Expression',
          Plans: [
            {
              'Node Type': 'Sorting',
              Plans: [
                {
                  'Node Type': 'Aggregating',
                  Plans: [
                    {
                      'Node Type': 'Filter',
                      Plans: [
                        {
                          'Node Type': 'ReadFromMergeTree',
                          Indexes: [
                            {
                              Type: 'PrimaryKey',
                              Keys: ['UserID'],
                              Condition: '(UserID in [749927693, 749927693])',
                              Parts: '1/1',
                              Granules: '1/1083',
                            },
                          ],
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
      },
    ]);

    const result = parseExplainIndexesJson(json);
    expect(result.success).toBe(true);
    expect(result.primaryKey!.keys).toEqual(['UserID']);
    expect(result.primaryKey!.granules).toEqual({ selected: 1, total: 1083 });
  });

  it('handles no WHERE clause (full scan, no PrimaryKey index)', () => {
    // When there's no WHERE, ClickHouse may not report a PrimaryKey index at all
    const json = JSON.stringify([
      {
        Plan: {
          'Node Type': 'Expression',
          Plans: [
            {
              'Node Type': 'ReadFromMergeTree',
              // No Indexes key at all
            },
          ],
        },
      },
    ]);

    const result = parseExplainIndexesJson(json);
    expect(result.success).toBe(true);
    expect(result.indexes).toHaveLength(0);
    expect(result.primaryKey).toBeNull();
  });
});

// ─── diagnoseOrderingKeyUsage with explainKeys ──────────────────────────────

import { diagnoseOrderingKeyUsage } from '../ordering-key-diagnostics.js';

describe('diagnoseOrderingKeyUsage with explainKeys from EXPLAIN', () => {
  const sortingKey = 'event_date, user_id, event_time';

  it('uses EXPLAIN keys instead of WHERE heuristic when provided', () => {
    // EXPLAIN says only event_date was used, even though WHERE has user_id too
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM t WHERE event_date = today() AND user_id = 42`,
      80,
      ['event_date'], // EXPLAIN says only event_date was used as key
    );
    expect(result.label).toBe('Partial key (1/3)');
    expect(result.matchedColumns).toEqual(['event_date']);
    expect(result.prefixLength).toBe(1);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('EXPLAIN keys showing full key match', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM t WHERE event_date = today() AND user_id = 42 AND event_time > now()`,
      95,
      ['event_date', 'user_id', 'event_time'],
    );
    expect(result.label).toBe('Full key match');
    expect(result.severity).toBe('good');
    expect(result.matchedColumns).toEqual(['event_date', 'user_id', 'event_time']);
    expect(result.prefixLength).toBe(3);
  });

  it('EXPLAIN keys showing no keys used (empty array) falls back to WHERE heuristic', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM t WHERE event_date = today()`,
      70,
      [], // empty EXPLAIN keys → fallback to heuristic
    );
    // Should fall back to WHERE parsing which finds event_date
    expect(result.label).toBe('Partial key (1/3)');
    expect(result.matchedColumns).toEqual(['event_date']);
  });

  it('EXPLAIN keys are case-insensitive', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM t WHERE event_date = today()`,
      90,
      ['Event_Date', 'User_ID'],
    );
    expect(result.matchedColumns).toEqual(['event_date', 'user_id']);
    expect(result.prefixLength).toBe(2);
  });

  it('without explainKeys, uses WHERE heuristic as before', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM t WHERE event_date = today() AND user_id = 42`,
      80,
    );
    expect(result.matchedColumns).toEqual(['event_date', 'user_id']);
    expect(result.prefixLength).toBe(2);
  });
});
