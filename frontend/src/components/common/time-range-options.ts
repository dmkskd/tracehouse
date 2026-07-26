/** Shared time-range presets used across Events, Analytics, Queries, and Merges. */
export const TIME_RANGE_OPTIONS: {
  label: string;
  interval: string | null;
}[] = [
  { label: '15m', interval: '15 MINUTE' },
  { label: '1h', interval: '1 HOUR' },
  { label: '6h', interval: '6 HOUR' },
  { label: '1d', interval: '1 DAY' },
  { label: '2d', interval: '2 DAY' },
  { label: '7d', interval: '7 DAY' },
  { label: '30d', interval: '30 DAY' },
];

/** Keep Events presets bounded; longer investigations remain available through Custom. */
export const EVENT_TIME_RANGE_OPTIONS = TIME_RANGE_OPTIONS.slice(0, 4).map(option => ({
  label: option.label,
  interval: option.interval!,
}));
