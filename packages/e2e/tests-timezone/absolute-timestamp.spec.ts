import { readFileSync } from 'node:fs';
import { test, expect } from '@playwright/test';
import { absoluteTimestampCases, invalidAbsoluteTimestamps } from '../../core/src/utils/__tests__/fixtures/absolute-timestamps.js';

// `just test-timezone-browser` builds core first. Load the actual emitted module,
// rather than copying the parser or depending on a running dev server/cache.
const parserSource = readFileSync(new URL('../../core/dist/utils/absolute-timestamp.js', import.meta.url), 'utf8');

test('valid strings preserve their absolute instants in this engine and timezone', async ({ page }, testInfo) => {
  const result = await page.evaluate(async ({ source, inputs }) => {
    const url = URL.createObjectURL(new Blob([source], { type: 'text/javascript' }));
    try {
      const { parseAbsoluteTimestamp } = await import(url);
      return {
        zone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        epochs: inputs.map(input => parseAbsoluteTimestamp(input)),
      };
    } finally { URL.revokeObjectURL(url); }
  }, { source: parserSource, inputs: absoluteTimestampCases.map(([input]) => input) });
  expect(result.zone).toBe(testInfo.project.use.timezoneId);
  expect(result.epochs).toEqual(absoluteTimestampCases.map(([, expected]) => Date.parse(expected)));
});

test('invalid strings are rejected without engine-specific fallback', async ({ page }) => {
  const rejected = await page.evaluate(async ({ source, inputs }) => {
    const url = URL.createObjectURL(new Blob([source], { type: 'text/javascript' }));
    try {
      const { parseAbsoluteTimestamp } = await import(url);
      return inputs.map(input => Number.isNaN(parseAbsoluteTimestamp(input)));
    } finally { URL.revokeObjectURL(url); }
  }, { source: parserSource, inputs: [...invalidAbsoluteTimestamps] });
  expect(rejected).toEqual(invalidAbsoluteTimestamps.map(() => true));
});
