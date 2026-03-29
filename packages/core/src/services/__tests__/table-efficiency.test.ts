import { describe, it, expect } from 'vitest';
import {
  parseSortingKey,
  extractWhereColumns,
  diagnoseOrderingKeyUsage,
} from '../ordering-key-diagnostics.js';

describe('Table Efficiency', { tags: ['storage'] }, () => {

// ─── parseSortingKey ────────────────────────────────────────────────────────

describe('parseSortingKey', () => {
  it('parses simple comma-separated columns', () => {
    expect(parseSortingKey('event_date, user_id, event_time')).toEqual([
      'event_date',
      'user_id',
      'event_time',
    ]);
  });

  it('extracts column from function expressions', () => {
    expect(parseSortingKey('toDate(event_time), user_id')).toEqual([
      'event_time',
      'user_id',
    ]);
  });

  it('handles cityHash64 and other functions', () => {
    expect(parseSortingKey('cityHash64(url), timestamp')).toEqual([
      'url',
      'timestamp',
    ]);
  });

  it('returns empty array for empty/null input', () => {
    expect(parseSortingKey('')).toEqual([]);
    expect(parseSortingKey('  ')).toEqual([]);
  });

  it('handles single column', () => {
    expect(parseSortingKey('id')).toEqual(['id']);
  });
});

// ─── extractWhereColumns ────────────────────────────────────────────────────

describe('extractWhereColumns', () => {
  it('extracts columns from simple equality', () => {
    const cols = extractWhereColumns(
      "SELECT * FROM t WHERE event_date = '2025-01-01'"
    );
    expect(cols).toContain('event_date');
  });

  it('extracts columns from multiple AND conditions', () => {
    const cols = extractWhereColumns(
      "SELECT * FROM t WHERE event_date = '2025-01-01' AND user_id = 42 AND event_time >= now()"
    );
    expect(cols).toContain('event_date');
    expect(cols).toContain('user_id');
    expect(cols).toContain('event_time');
  });

  it('extracts columns from IN clause', () => {
    const cols = extractWhereColumns(
      'SELECT * FROM t WHERE user_id IN (1, 2, 3)'
    );
    expect(cols).toContain('user_id');
  });

  it('extracts columns from BETWEEN', () => {
    const cols = extractWhereColumns(
      "SELECT * FROM t WHERE event_date BETWEEN '2025-01-01' AND '2025-02-01'"
    );
    expect(cols).toContain('event_date');
  });

  it('extracts columns from function calls like toDate(col)', () => {
    const cols = extractWhereColumns(
      "SELECT * FROM t WHERE toDate(event_time) = '2025-01-01'"
    );
    expect(cols).toContain('event_time');
  });

  it('extracts columns from comparison operators (>, <, >=, <=, !=)', () => {
    const cols = extractWhereColumns(
      'SELECT * FROM t WHERE revenue > 0 AND duration_ms <= 5000'
    );
    expect(cols).toContain('revenue');
    expect(cols).toContain('duration_ms');
  });

  it('returns empty array when no WHERE clause', () => {
    const cols = extractWhereColumns(
      'SELECT count() FROM t GROUP BY event_type'
    );
    expect(cols).toEqual([]);
  });

  it('returns empty array for empty input', () => {
    expect(extractWhereColumns('')).toEqual([]);
  });

  it('ignores SQL keywords that look like column names', () => {
    const cols = extractWhereColumns(
      "SELECT * FROM t WHERE event_date = '2025-01-01' AND NOT is_deleted"
    );
    // 'not' should be filtered out as a keyword
    expect(cols).not.toContain('not');
  });

  it('handles SETTINGS clause termination', () => {
    const cols = extractWhereColumns(
      "SELECT * FROM t WHERE event_date = today() SETTINGS use_query_cache = 0"
    );
    expect(cols).toContain('event_date');
    // 'use_query_cache' should NOT be extracted (it's after SETTINGS)
    expect(cols).not.toContain('use_query_cache');
  });
});


// ─── diagnoseOrderingKeyUsage — synthetic_data.events ────────────────────────────────
// ORDER BY (event_date, user_id, event_time)

describe('diagnoseOrderingKeyUsage — synthetic_data.events ORDER BY (event_date, user_id, event_time)', () => {
  const sortingKey = 'event_date, user_id, event_time';

  it('Full key match: WHERE on all 3 ORDER BY columns → binary search', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events
       WHERE event_date = today() - 1
         AND user_id = 42
         AND event_time >= now() - INTERVAL 2 DAY`,
      95,
    );
    expect(result.label).toBe('Full key match');
    expect(result.severity).toBe('good');
    expect(result.usesLeftmostKey).toBe(true);
    expect(result.matchedColumns).toEqual(['event_date', 'user_id', 'event_time']);
    expect(result.prefixLength).toBe(3);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Partial key (2/3): WHERE on event_date + user_id → binary search', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events
       WHERE event_date BETWEEN today() - 14 AND today()
         AND user_id IN (100, 200, 300)`,
      80,
    );
    expect(result.label).toBe('Partial key (2/3)');
    expect(result.severity).toBe('warning');
    expect(result.usesLeftmostKey).toBe(true);
    expect(result.matchedColumns).toEqual(['event_date', 'user_id']);
    expect(result.prefixLength).toBe(2);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Partial key (1/3): WHERE on event_date only → binary search', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT event_type, count() FROM synthetic_data.events
       WHERE event_date = today() - 7
       GROUP BY event_type`,
      70,
    );
    expect(result.label).toBe('Partial key (1/3)');
    expect(result.severity).toBe('warning');
    expect(result.usesLeftmostKey).toBe(true);
    expect(result.matchedColumns).toEqual(['event_date']);
    expect(result.prefixLength).toBe(1);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Partial key with gap: WHERE on event_date + event_time (skips user_id) → binary search + generic exclusion', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events
       WHERE event_date = today() - 1
         AND event_time >= now() - INTERVAL 1 HOUR`,
      60,
    );
    // Effective prefix is 1 (event_date only), even though 2 key cols matched
    expect(result.label).toBe('Partial key (1/3)');
    expect(result.severity).toBe('warning');
    expect(result.usesLeftmostKey).toBe(true);
    expect(result.prefixLength).toBe(1);
    expect(result.indexAlgorithm).toBe('binary_search');
    // matchedColumns still has both
    expect(result.matchedColumns).toContain('event_date');
    expect(result.matchedColumns).toContain('event_time');
    // Reason should explain the gap and generic exclusion search
    expect(result.reason).toContain('skips');
    expect(result.reason).toContain('user_id');
    expect(result.reason).toContain('generic exclusion search');
  });

  it('Skips leftmost key: WHERE on user_id only → generic exclusion search', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT event_date, count() FROM synthetic_data.events
       WHERE user_id = 12345
       GROUP BY event_date`,
      5,
    );
    expect(result.label).toBe('Skips leftmost key');
    expect(result.severity).toBe('poor');
    expect(result.usesLeftmostKey).toBe(false);
    expect(result.matchedColumns).toEqual(['user_id']);
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
    expect(result.reason).toContain('generic exclusion search');
  });

  it('Skips leftmost key: WHERE on event_time only (3rd column) → generic exclusion', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events
       WHERE event_time >= now() - INTERVAL 1 HOUR`,
      2,
    );
    expect(result.label).toBe('Skips leftmost key');
    expect(result.severity).toBe('poor');
    expect(result.usesLeftmostKey).toBe(false);
    expect(result.matchedColumns).toEqual(['event_time']);
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
  });

  it('Skips leftmost key: WHERE on user_id + event_time (2nd + 3rd, no leftmost)', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events
       WHERE user_id = 42 AND event_time >= now() - INTERVAL 1 DAY`,
      3,
    );
    expect(result.label).toBe('Skips leftmost key');
    expect(result.severity).toBe('poor');
    expect(result.usesLeftmostKey).toBe(false);
    expect(result.matchedColumns).toEqual(['user_id', 'event_time']);
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
  });

  it('Skips leftmost key BUT actual pruning is decent → severity is warning not poor', () => {
    // This can happen when the leftmost column has low cardinality,
    // so generic exclusion search is still effective
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events
       WHERE user_id = 42`,
      65, // actual pruning is decent despite skipping leftmost
    );
    expect(result.label).toBe('Skips leftmost key');
    expect(result.severity).toBe('warning'); // not 'poor' because actual pruning >= 50%
    expect(result.indexAlgorithm).toBe('generic_exclusion');
    expect(result.reason).toContain('However, actual pruning is 65%');
    expect(result.reason).toContain('low cardinality');
  });

  it('No key match: WHERE on non-key columns → no index algorithm', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT event_type, count() FROM synthetic_data.events
       WHERE country_code = 'US' AND device_type = 'mobile'
       GROUP BY event_type`,
      1,
    );
    expect(result.label).toBe('No key match');
    expect(result.severity).toBe('poor');
    expect(result.usesLeftmostKey).toBe(false);
    expect(result.matchedColumns).toEqual([]);
    expect(result.whereColumns).toContain('country_code');
    expect(result.whereColumns).toContain('device_type');
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('none');
  });

  it('No WHERE clause: full table scan', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count(), avg(duration_ms) FROM synthetic_data.events`,
      0,
    );
    expect(result.label).toBe('No WHERE clause');
    expect(result.severity).toBe('poor'); // avgPruningPct < 50
    expect(result.whereColumns).toEqual([]);
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('none');
  });

  it('No WHERE clause with null pruning is warning not poor', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM synthetic_data.events`,
      null, // no pruning data
    );
    expect(result.label).toBe('No WHERE clause');
    expect(result.severity).toBe('warning');
  });
});


// ─── diagnoseOrderingKeyUsage — edge cases ───────────────────────────────────────────

describe('diagnoseOrderingKeyUsage — edge cases', () => {
  it('No ORDER BY defined on table', () => {
    const result = diagnoseOrderingKeyUsage(
      '',
      "SELECT * FROM t WHERE id = 1",
      null,
    );
    expect(result.label).toBe('No ORDER BY');
    expect(result.severity).toBe('warning');
    expect(result.orderByColumns).toEqual([]);
    expect(result.prefixLength).toBe(0);
  });

  it('null sorting key', () => {
    const result = diagnoseOrderingKeyUsage(
      null,
      "SELECT * FROM t WHERE id = 1",
      null,
    );
    expect(result.label).toBe('No ORDER BY');
  });

  it('Partial key with high pruning is marked good', () => {
    const result = diagnoseOrderingKeyUsage(
      'event_date, user_id, event_time',
      `SELECT count() FROM t WHERE event_date = today()`,
      95, // high pruning despite partial key
    );
    expect(result.label).toBe('Partial key (1/3)');
    expect(result.severity).toBe('good'); // >= 90% pruning
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('handles function-wrapped ORDER BY columns', () => {
    const result = diagnoseOrderingKeyUsage(
      'toDate(event_time), user_id',
      `SELECT count() FROM t WHERE event_time = now() AND user_id = 1`,
      90,
    );
    expect(result.label).toBe('Full key match');
    expect(result.severity).toBe('good');
    expect(result.orderByColumns).toEqual(['event_time', 'user_id']);
    expect(result.prefixLength).toBe(2);
    expect(result.indexAlgorithm).toBe('binary_search');
  });
});

// ─── diagnoseOrderingKeyUsage — nyc_taxi.trips ───────────────────────────────────────
// ORDER BY (pickup_date, pickup_location_id, pickup_datetime)

describe('diagnoseOrderingKeyUsage — nyc_taxi.trips ORDER BY (pickup_date, pickup_location_id, pickup_datetime)', () => {
  const sortingKey = 'pickup_date, pickup_location_id, pickup_datetime';

  it('Full key match → binary search, prefixLength 3', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM nyc_taxi.trips
       WHERE pickup_date = today() - 3
         AND pickup_location_id = 132
         AND pickup_datetime >= now() - INTERVAL 4 DAY`,
      98,
    );
    expect(result.label).toBe('Full key match');
    expect(result.severity).toBe('good');
    expect(result.prefixLength).toBe(3);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Partial key (2/3): pickup_date + pickup_location_id', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM nyc_taxi.trips
       WHERE pickup_date BETWEEN today() - 7 AND today()
         AND pickup_location_id IN (132, 138, 161)`,
      85,
    );
    expect(result.label).toBe('Partial key (2/3)');
    expect(result.prefixLength).toBe(2);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Skips leftmost: WHERE on pickup_location_id only → generic exclusion', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT pickup_date, count() FROM nyc_taxi.trips
       WHERE pickup_location_id = 161
       GROUP BY pickup_date`,
      5,
    );
    expect(result.label).toBe('Skips leftmost key');
    expect(result.severity).toBe('poor');
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
  });

  it('No key match: WHERE on vendor_name + payment_type', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM nyc_taxi.trips
       WHERE vendor_name = 'Yellow Cab' AND payment_type = 'Credit card'`,
      2,
    );
    expect(result.label).toBe('No key match');
    expect(result.severity).toBe('poor');
    expect(result.indexAlgorithm).toBe('none');
  });
});

// ─── diagnoseOrderingKeyUsage — uk_price_paid ────────────────────────────────────────
// ORDER BY (postcode1, postcode2, date)

describe('diagnoseOrderingKeyUsage — uk_price_paid ORDER BY (postcode1, postcode2, date)', () => {
  const sortingKey = 'postcode1, postcode2, date';

  it('Full key match', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM uk_price_paid.uk_price_paid
       WHERE postcode1 = 'SW1' AND postcode2 = '1AA' AND date >= '2024-01-01'`,
      97,
    );
    expect(result.label).toBe('Full key match');
    expect(result.severity).toBe('good');
    expect(result.prefixLength).toBe(3);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Partial key with gap: postcode1 + date (skips postcode2) → binary + generic exclusion', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT count() FROM uk_price_paid.uk_price_paid
       WHERE postcode1 = 'SW1' AND date >= '2024-01-01'`,
      60,
    );
    // Effective prefix is 1 (postcode1 only), even though 2 key cols matched
    expect(result.label).toBe('Partial key (1/3)');
    expect(result.reason).toContain('skips');
    expect(result.reason).toContain('postcode2');
    expect(result.reason).toContain('generic exclusion search');
    expect(result.prefixLength).toBe(1);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('Skips leftmost: WHERE on date only (3rd column) → generic exclusion', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT postcode1, count() FROM uk_price_paid.uk_price_paid
       WHERE date >= '2025-01-01' AND date < '2025-07-01'
       GROUP BY postcode1`,
      3,
    );
    expect(result.label).toBe('Skips leftmost key');
    expect(result.severity).toBe('poor');
    expect(result.prefixLength).toBe(0);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
  });

  it('No key match: WHERE on town + county', () => {
    const result = diagnoseOrderingKeyUsage(
      sortingKey,
      `SELECT street, count() FROM uk_price_paid.uk_price_paid
       WHERE town = 'LONDON' AND county = 'GREATER LONDON'
       GROUP BY street`,
      1,
    );
    expect(result.label).toBe('No key match');
    expect(result.severity).toBe('poor');
    expect(result.indexAlgorithm).toBe('none');
  });
});

// ─── Index algorithm correctness ────────────────────────────────────────────

describe('diagnoseOrderingKeyUsage — index algorithm detection', () => {
  const sortingKey = 'a, b, c';

  it('binary_search when leftmost key is in WHERE', () => {
    const result = diagnoseOrderingKeyUsage(sortingKey, 'SELECT * FROM t WHERE a = 1', 80);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('binary_search when all keys are in WHERE', () => {
    const result = diagnoseOrderingKeyUsage(sortingKey, 'SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3', 99);
    expect(result.indexAlgorithm).toBe('binary_search');
  });

  it('generic_exclusion when only non-leftmost key is in WHERE', () => {
    const result = diagnoseOrderingKeyUsage(sortingKey, 'SELECT * FROM t WHERE b = 2', 10);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
  });

  it('generic_exclusion when 2nd + 3rd keys but not 1st', () => {
    const result = diagnoseOrderingKeyUsage(sortingKey, 'SELECT * FROM t WHERE b = 2 AND c = 3', 5);
    expect(result.indexAlgorithm).toBe('generic_exclusion');
  });

  it('none when no key columns in WHERE', () => {
    const result = diagnoseOrderingKeyUsage(sortingKey, 'SELECT * FROM t WHERE x = 1', 0);
    expect(result.indexAlgorithm).toBe('none');
  });

  it('none when no WHERE clause', () => {
    const result = diagnoseOrderingKeyUsage(sortingKey, 'SELECT * FROM t', null);
    expect(result.indexAlgorithm).toBe('none');
  });
});

}); // Table Efficiency
