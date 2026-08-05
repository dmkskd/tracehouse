/**
 * Integration test for column-comment lookup escaping against real ClickHouse.
 *
 * The unit test pins the generated SQL string. This one proves the behaviour
 * ClickHouse actually exhibits: a column name containing a backslash before a
 * quote must match only itself, never every column in the database.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { QueryAnalyzer } from '../../services/query-analyzer.js';

const CONTAINER_TIMEOUT = 120_000;

/** A column name any user with CREATE TABLE rights can choose. */
const CRAFTED_COLUMN = String.raw`x\' OR 1=1 --`;
const CRAFTED_COMMENT = 'crafted column comment';

describe('column comment lookup escaping', { tags: ['security'] }, () => {
  let ctx: TestClickHouseContext;
  let analyzer: QueryAnalyzer;

  beforeAll(async () => {
    ctx = await startClickHouse();
    analyzer = new QueryAnalyzer(ctx.adapter);

    await ctx.client.command({ query: 'CREATE DATABASE IF NOT EXISTS escape_test' });
    await ctx.client.command({ query: 'DROP TABLE IF EXISTS escape_test.crafted' });
    // Backticked identifiers unescape \\ to one backslash, so this creates a
    // column literally named:  x\' OR 1=1 --
    await ctx.client.command({
      query: [
        'CREATE TABLE escape_test.crafted (',
        "  `x\\\\' OR 1=1 --` String COMMENT '" + CRAFTED_COMMENT + "',",
        "  plain String COMMENT 'plain column comment'",
        ') ENGINE = MergeTree() ORDER BY tuple()',
      ].join('\n'),
    });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, CONTAINER_TIMEOUT);

  it('creates the crafted column name verbatim', async () => {
    const rows = await ctx.client
      .query({
        query: "SELECT name FROM system.columns WHERE database = 'escape_test' AND table = 'crafted' ORDER BY name",
        format: 'JSONEachRow',
      })
      .then(r => r.json<{ name: string }>());

    expect(rows.map(r => r.name)).toContain(CRAFTED_COLUMN);
  });

  it('returns only the crafted column, not every column', async () => {
    const comments = await analyzer.getColumnComments([
      `escape_test.crafted.${CRAFTED_COLUMN}`,
    ]);

    // With the pre-fix escaping the literal terminated early and the trailing
    // OR 1=1 matched every row, so this returned unrelated columns too.
    expect(comments).toEqual({
      [`escape_test.crafted.${CRAFTED_COLUMN}`]: CRAFTED_COMMENT,
    });
  });
});
