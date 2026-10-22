import { compactFormatter, isNumericValue } from './charts';
import type { SummaryDirectives, SummaryMetric } from './metaLanguage';

interface NumericScalar {
  key: string;
  raw: string;
  displayNumber: number;
  exactDisplay: boolean;
}

/** Canonical decimal identity avoids comparing 64-bit integers through Number. */
function numericScalar(value: unknown): NumericScalar | null {
  if ((typeof value !== 'number' && typeof value !== 'string') || !isNumericValue(value)) return null;
  const raw = String(value);
  const match = raw.trim().match(/^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))(?:e([+-]?\d+))?$/i);
  const displayNumber = Number(value);
  if (!match || !Number.isFinite(displayNumber)) return null;
  const fraction = match[3] ?? match[4] ?? '';
  let digits = ((match[2] ?? '') + fraction).replace(/^0+/, '');
  if (!digits) return { key: '0', raw, displayNumber: 0, exactDisplay: false };
  const trailing = digits.match(/0+$/)?.[0].length ?? 0;
  digits = digits.slice(0, digits.length - trailing);
  const exponent = BigInt(match[5] ?? '0') - BigInt(fraction.length) + BigInt(trailing);
  const key = `${match[1] === '-' ? '-' : ''}${digits}e${exponent}`;
  return { key, raw, displayNumber, exactDisplay: exponent >= 0n && !Number.isSafeInteger(displayNumber) };
}

export interface SummaryValue extends SummaryMetric {
  display: string;
  accessibleLabel: string;
}

/** Validate the full response, before any chart sampling, sorting, or pivoting. */
export function buildResultSummary(rows: Record<string, unknown>[], config?: SummaryDirectives): {
  values: SummaryValue[];
  diagnostics: string[];
} {
  const result: { values: SummaryValue[]; diagnostics: string[] } = { values: [], diagnostics: [...(config?.diagnostics ?? [])] };
  if (!rows.length || !config) return result;
  for (const metric of config.metrics) {
    let error: string | undefined;
    const first = rows[0][metric.valueColumn];
    const scalar = first === null ? null : numericScalar(first);
    for (const row of rows) {
      if (!Object.hasOwn(row, metric.valueColumn)) { error = 'column is missing'; break; }
      const value = row[metric.valueColumn];
      const current = value === null ? null : numericScalar(value);
      if (value !== null && !current) { error = 'unsupported value'; break; }
      if ((first === null) !== (value === null) || (value !== null && current?.key !== scalar?.key)) {
        error = 'values differ between result rows'; break;
      }
    }
    if (error) {
      result.diagnostics.push(`${metric.label} (${metric.valueColumn}): ${error}.`);
      continue;
    }
    const suffix = metric.unit ? ` ${metric.unit}` : '';
    result.values.push({
      ...metric,
      display: scalar ? scalar.exactDisplay ? `${scalar.raw}${suffix}` : compactFormatter(metric.unit)(scalar.displayNumber) : '—',
      accessibleLabel: `${metric.label}: ${scalar ? scalar.raw + suffix : 'unavailable'}`,
    });
  }
  return result;
}
