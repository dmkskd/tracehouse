import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { ChartSummary } from '../ChartSummary';
import { parseChartDirective, parseDirectives, parseSummaryDirectives } from '../metaLanguage';
import { buildResultSummary } from '../resultSummary';

afterEach(cleanup);

const sql = `-- @meta: title='Revenue' group='Custom'
-- @chart: type=bar group_by=region value=revenue style=2d
-- @summary: value=total label='Total duration' unit=s
-- @summary: value=orders label='Orders'
SELECT ...`;
const config = parseSummaryDirectives(sql)!;
const rows = [{ region: 'West', revenue: 125, total: 5648.305, orders: '454' }];

describe('summary directive grammar', () => {
  it('parses repeated standalone metrics in declaration order, without requiring chart or meta', () => {
    expect(config).toEqual({ metrics: [
      { valueColumn: 'total', label: 'Total duration', unit: 's' },
      { valueColumn: 'orders', label: 'Orders' },
    ], diagnostics: [] });
    expect(parseDirectives(sql)?.summaries).toEqual(config);
    expect(parseSummaryDirectives("-- @summary: value=x label='X' unit=%\nSELECT 1 AS x")?.metrics)
      .toEqual([{ valueColumn: 'x', label: 'X', unit: '%' }]);
    expect(parseSummaryDirectives("-- @summary: value=x label='User\\'s total'")?.metrics[0].label).toBe("User's total");
  });

  it.each([
    "value=x", "value=x label=''", "label='X'", "value=x label=X",
    "value=x label='X' reduce=sum", "value=x value=y label='X'", "value=x label='X' garbage",
  ])('reports invalid syntax without accepting a partial directive: %s', text => {
    const parsed = parseSummaryDirectives(`-- @summary: ${text}`)!;
    expect(parsed.metrics).toEqual([]);
    expect(parsed.diagnostics).toHaveLength(1);
  });

  it('does not infer metrics or retain the discarded @chart summary= prototype', () => {
    expect(parseSummaryDirectives('SELECT 1')).toBeUndefined();
    expect(parseChartDirective('-- @chart: type=bar summary=overview')).not.toHaveProperty('summaryColumn');
    render(<ChartSummary rows={[{ total_elapsed_seconds: 25, total_merge_count: 100 }]} />);
    expect(screen.queryByLabelText('Result summary')).not.toBeInTheDocument();
  });

  it('keeps later valid declarations when an earlier line is empty', () => {
    const parsed = parseSummaryDirectives("-- @summary:\n-- @summary: value=x label='X'")!;
    expect(parsed.diagnostics).toHaveLength(1);
    expect(parsed.metrics).toEqual([{ valueColumn: 'x', label: 'X' }]);
  });
});

describe('result-wide summary validation and formatting', () => {
  it('uses the axis formatter while preserving raw SQL values in accessible labels', () => {
    render(<ChartSummary rows={rows} config={config} />);
    expect(screen.getByLabelText('Result summary')).toHaveTextContent('Total duration 1.6 h');
    expect(screen.getByLabelText('Total duration: 5648.305 s')).toHaveTextContent('1.6 h');
    expect(screen.getByLabelText('Orders: 454')).toHaveTextContent('454');
    expect(screen.getByLabelText('Total duration: 5648.305 s')).toHaveAttribute('title', 'Total duration: 5648.305 s');
    expect(buildResultSummary(rows, { metrics: [{ valueColumn: 'total', label: 'Total', unit: 'sec' }], diagnostics: [] }).values[0].display).toBe('5.6K sec');
  });

  it.each(['9007199254740991', '9007199254740992', '9007199254740993', '18446744073709551615', '-9223372036854775808'])('preserves exact integer identity and raw labels: %s', raw => {
    const result = buildResultSummary([{ orders: raw }], { metrics: [config.metrics[1]], diagnostics: [] });
    expect(result.diagnostics).toEqual([]);
    expect(result.values[0].accessibleLabel).toBe(`Orders: ${raw}`);
    if (!Number.isSafeInteger(Number(raw))) expect(result.values[0].display).toBe(raw);
  });

  it('validates all rows before sampling and compares numeric strings without losing precision', () => {
    const data = Array.from({ length: 51 }, () => ({ orders: '9007199254740992' }));
    data[50].orders = '9007199254740993';
    const result = buildResultSummary(data, { metrics: [config.metrics[1]], diagnostics: [] });
    expect(result.values).toEqual([]);
    expect(result.diagnostics[0]).toContain('values differ');
    expect(buildResultSummary([{ orders: '454' }, { orders: 454 }, { orders: '4.540e2' }], { metrics: [config.metrics[1]], diagnostics: [] }).diagnostics).toEqual([]);
  });

  it.each(['', ' ', 'Infinity', '-Infinity', 'NaN', '1e999', '2 GiB', '0x10', '1_000', Infinity, NaN, undefined, true, [], {}])('rejects unsupported values: %s', value => {
    const result = buildResultSummary([{ orders: value }], { metrics: [config.metrics[1]], diagnostics: [] });
    expect(result.values).toEqual([]);
    expect(result.diagnostics[0]).toContain('unsupported value');
  });

  it('supports null, quoted zero and negative values, and does not invent totals for empty results', () => {
    const input = { metrics: [config.metrics[1]], diagnostics: [] };
    expect(buildResultSummary([], input).values).toEqual([]);
    expect(buildResultSummary([{ orders: null }], input).values[0]).toMatchObject({ display: '—', accessibleLabel: 'Orders: unavailable' });
    expect(buildResultSummary([{ orders: '0' }], input).values[0].display).toBe('0');
    expect(buildResultSummary([{ orders: '-454' }], input).values[0].display).toBe('-454');
    expect(buildResultSummary([{ orders: null }, { orders: '0' }], input).diagnostics[0]).toContain('values differ');
  });

  it('reports invalid metrics without hiding valid siblings or breaking the chart', () => {
    render(<ChartSummary rows={[{ orders: '454' }]} config={config} />);
    expect(screen.getByLabelText('Orders: 454')).toBeInTheDocument();
    expect(screen.getByText('Summary unavailable')).toBeInTheDocument();
    expect(screen.getByText(/column is missing/)).toBeInTheDocument();
  });

  it('renders explicit deferred-surface feedback without showing summary values', () => {
    render(<ChartSummary rows={rows} config={config} unavailableReason="Summaries are available in 2D or table view." />);
    expect(screen.queryByLabelText('Result summary')).not.toBeInTheDocument();
    expect(screen.getByText('Summaries are available in 2D or table view.')).toBeInTheDocument();
  });
});
