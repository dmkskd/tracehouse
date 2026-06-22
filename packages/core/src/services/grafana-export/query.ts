import type { GrafanaExportInput } from './types.js';
import type { GrafanaRadarCellStyle, GrafanaRadarAxisRange } from './types.js';
import { convertTracehouseTimeRangeMacros, quoteIdent, radarImageColumn, resolveResultColumn, sparklineImageColumn, valueColumns } from './utils.js';

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

function tableRadarSql(input: GrafanaExportInput, sql: string): string | undefined {
  const radarStyles = (input.cellStyles ?? []).filter((style): style is GrafanaRadarCellStyle => style.type === 'radar');
  if (!radarStyles.length) return undefined;

  const helperFields = radarStyles
    .map(style => {
      const displayColumn = radarDisplayColumn(style);
      if (!displayColumn) return undefined;
      const values = radarValuesExpression(style, input);
      if (!values) return undefined;
      return `${radarImageExpression(values)} AS ${quoteIdent(radarImageColumn(displayColumn))}`;
    })
    .filter((field): field is string => Boolean(field));

  if (!helperFields.length) return undefined;
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

function radarDisplayColumn(style: GrafanaRadarCellStyle): string | undefined {
  return style.radarColumn ?? style.column;
}

function parseRangeNumber(raw: string): number {
  const match = raw.trim().match(/^(-?\d+(?:\.\d+)?)([a-zA-Z]+)?$/);
  if (!match) return Number(raw);
  const n = Number(match[1]);
  switch (match[2]?.toLowerCase()) {
    case 'k': return n * 1_000;
    case 'm': return n * 1_000_000;
    case 'b': return n * 1_000_000_000;
    case 'ki': return n * 1024;
    case 'mi': return n * 1024 * 1024;
    case 'gi': return n * 1024 * 1024 * 1024;
    case 'ti': return n * 1024 * 1024 * 1024 * 1024;
    default: return n;
  }
}

function profileTransform(profile: string | undefined, axis: string): string {
  if (profile === 'query_pressure') return axis === 'scan' ? 'linear' : 'log';
  return 'linear';
}

function normalizeExpression(column: string, range: GrafanaRadarAxisRange | undefined, transform: string): string {
  const source = `toFloat64(${quoteIdent(column)})`;
  if (!range) return `greatest(0., least(1., ${source}))`;
  const low = parseRangeNumber(range.low);
  const high = parseRangeNumber(range.high);
  if (!Number.isFinite(low) || !Number.isFinite(high) || high <= low) return '0.';
  if (transform === 'log') {
    const safeLow = Math.max(1, low);
    const safeHigh = Math.max(safeLow + 1, high);
    return `if(${source} <= 0, 0., greatest(0., least(1., (log10(greatest(${source}, ${safeLow})) - log10(${safeLow})) / nullIf(log10(${safeHigh}) - log10(${safeLow}), 0))))`;
  }
  return `greatest(0., least(1., (${source} - ${low}) / nullIf(${high} - ${low}, 0)))`;
}

function radarValuesExpression(style: GrafanaRadarCellStyle, input: GrafanaExportInput): string | undefined {
  if (style.column) {
    const column = resolveResultColumn(style.column, input.resultColumns) ?? style.column;
    return `arrayMap(x -> greatest(0., least(1., toFloat64(x))), ${quoteIdent(column)})`;
  }

  if (!style.axes || !style.ranges) return undefined;
  const orderedAxes = [
    ...['time', 'memory', 'cpu', 'io', 'scan'].filter(axis => style.axes?.[axis]),
    ...Object.keys(style.axes).filter(axis => !['time', 'memory', 'cpu', 'io', 'scan'].includes(axis)),
  ];
  if (!orderedAxes.length) return undefined;

  const values = orderedAxes.map(axis => {
    const column = resolveResultColumn(style.axes![axis], input.resultColumns) ?? style.axes![axis];
    const transform = style.transforms?.[axis] ?? profileTransform(style.profile, axis);
    return normalizeExpression(column, style.ranges?.[axis], transform);
  });
  return `[${values.join(', ')}]`;
}

function radarImageExpression(valuesExpression: string): string {
  const values = `if(length(${valuesExpression}) = 0, [0., 0., 0.], ${valuesExpression})`;
  const score = `arrayMax(${values})`;
  const color = `multiIf(${score} >= 0.85, '#f85149', ${score} >= 0.40, '#d29922', ${score} >= 0.15, '#3fb950', '#8b949e')`;
  const angle = `(-pi() / 2) + ((i - 1) * 2 * pi() / length(${values}))`;
  const radius = `(5 + greatest(0., least(1., v)) * 17)`;
  const x = `21 + cos(${angle}) * ${radius}`;
  const y = `21 + sin(${angle}) * ${radius}`;
  const points = `arrayStringConcat(arrayMap((v, i) -> concat(toString(round(${x}, 1)), ',', toString(round(${y}, 1))), ${values}, arrayEnumerate(${values})), ' ')`;
  return `concat('data:image/svg+xml;utf8,', encodeURLComponent(concat('<svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 42 42"><circle cx="21" cy="21" r="18" fill="none" stroke="#94a3b8" stroke-opacity="0.55"/><polygon points="', ${points}, '" fill="', ${color}, '38" stroke="', ${color}, '" stroke-width="2.5"/></svg>')))`;
}

export function grafanaSql(input: GrafanaExportInput, cleanSql: string, panelType: string): string {
  const chart = input.chart;
  const sql = convertTracehouseTimeRangeMacros(cleanSql);
  if (!chart) {
    const sparklineSql = tableSparklineSql(input, sql) ?? sql;
    return tableRadarSql(input, sparklineSql) ?? sparklineSql;
  }

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
