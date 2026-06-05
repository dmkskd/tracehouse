import type { GrafanaExportInput } from './types.js';
import { convertTracehouseTimeRangeMacros, quoteIdent, valueColumns } from './utils.js';

function quoteStringLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function pivotSeriesBarSql(input: GrafanaExportInput, sql: string): string | undefined {
  const chart = input.chart;
  if (!chart?.seriesColumn || !chart.seriesValues?.length) return undefined;
  if (chart.type !== 'grouped_bar' && chart.type !== 'stacked_bar') return undefined;

  const seriesFields = chart.seriesValues.map(seriesValue => (
    `sumIf(${quoteIdent(chart.valueColumn)}, ${quoteIdent(chart.seriesColumn!)} = ${quoteStringLiteral(seriesValue)}) AS ${quoteIdent(seriesValue)}`
  ));
  const totalExpression = seriesFields
    .map((_, index) => quoteIdent(chart.seriesValues![index]))
    .join(' + ');
  const limit = chart.maxRows && chart.maxRows > 0 ? `\nLIMIT ${Math.round(chart.maxRows)}` : '';

  return `SELECT ${quoteIdent(chart.groupByColumn)}, ${seriesFields.join(', ')}
FROM (
${sql}
)
GROUP BY ${quoteIdent(chart.groupByColumn)}
ORDER BY ${totalExpression} DESC${limit}`;
}

export function grafanaSql(input: GrafanaExportInput, cleanSql: string, panelType: string): string {
  const chart = input.chart;
  const sql = convertTracehouseTimeRangeMacros(cleanSql);
  if (!chart) return sql;

  const pivotSql = pivotSeriesBarSql(input, sql);
  if (pivotSql) return pivotSql;

  const fields = [
    chart.groupByColumn,
    chart.seriesColumn,
    ...valueColumns(chart),
  ].filter((field, index, all): field is string => Boolean(field) && all.indexOf(field) === index);

  if ((panelType === 'timeseries' || panelType === 'barchart' || panelType === 'piechart') && fields.length >= 2) {
    const limit = chart.maxRows && chart.maxRows > 0 ? `\nLIMIT ${Math.round(chart.maxRows)}` : '';
    return `SELECT ${fields.map(quoteIdent).join(', ')}\nFROM (\n${sql}\n)${limit}`;
  }

  return sql;
}
