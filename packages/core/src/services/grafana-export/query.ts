import type { GrafanaExportInput } from './types.js';
import { convertTracehouseTimeRangeMacros, quoteIdent, resolveResultColumn, sparklineImageColumn, valueColumns } from './utils.js';

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

function tableSparklineSql(input: GrafanaExportInput, sql: string): string | undefined {
  const sparklineStyles = (input.cellStyles ?? []).filter(style => style.type === 'sparkline');
  if (!sparklineStyles.length) return undefined;

  const helperFields = sparklineStyles
    .map(style => {
      const column = resolveResultColumn(style.column, input.resultColumns) ?? style.column;
      return `${sparklineImageExpression(column, style.color, style.fill)} AS ${quoteIdent(sparklineImageColumn(column))}`;
    });

  return `SELECT *, ${helperFields.join(', ')}
FROM (
${sql}
)`;
}

function sparklineImageExpression(column: string, color = '#6366f1', fill = false): string {
  const source = quoteIdent(column);
  const values = `arraySlice(arrayMap(x -> toFloat64(x), ${source}), greatest(1, length(${source}) - 59), 60)`;
  const safeValues = `if(length(${values}) = 0, [0.0], if(length(${values}) = 1, arrayConcat(${values}, ${values}), ${values}))`;
  const minValue = `arrayMin(${safeValues})`;
  const maxValue = `arrayMax(${safeValues})`;
  const y = `if(${maxValue} = ${minValue}, 12, 22 - ((v - ${minValue}) * 20 / (${maxValue} - ${minValue})))`;
  const x = `if(length(${safeValues}) = 1, 40, (i - 1) * 80 / (length(${safeValues}) - 1))`;
  const points = `arrayStringConcat(arrayMap((v, i) -> concat(toString(round(${x}, 1)), ',', toString(round(${y}, 1))), ${safeValues}, arrayEnumerate(${safeValues})), ' ')`;
  const polygonEnd = quoteStringLiteral(` 80,24" fill="${fill ? color : 'none'}" fill-opacity="${fill ? '0.16' : '0'}"/>`);
  const polylineEnd = quoteStringLiteral(`" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>`);

  return `concat('data:image/svg+xml;utf8,', encodeURLComponent(concat('<svg xmlns="http://www.w3.org/2000/svg" width="80" height="24" viewBox="0 0 80 24">', if(length(${values}) = 0, '', concat('<polygon points="0,24 ', ${points}, ${polygonEnd}, '<polyline points="', ${points}, ${polylineEnd})), '</svg>')))`;
}

export function grafanaSql(input: GrafanaExportInput, cleanSql: string, panelType: string): string {
  const chart = input.chart;
  const sql = convertTracehouseTimeRangeMacros(cleanSql);
  if (!chart) return tableSparklineSql(input, sql) ?? sql;

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
