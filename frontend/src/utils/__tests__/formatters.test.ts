import { describe, it, expect } from 'vitest';
import { formatDuration, formatDurationMs, formatMicroseconds, formatElapsed } from '../formatters';

describe('formatDuration (seconds input)', { tags: ['storage'] }, () => {
  it('formats sub-second as milliseconds', () => {
    expect(formatDuration(0)).toBe('0ms');
    expect(formatDuration(0.25)).toBe('250ms');
    expect(formatDuration(0.999)).toBe('999ms');
  });

  it('formats seconds with 2 decimal places', () => {
    expect(formatDuration(1)).toBe('1.00s');
    expect(formatDuration(3.14)).toBe('3.14s');
    expect(formatDuration(59.99)).toBe('59.99s');
  });

  it('formats minutes and seconds', () => {
    expect(formatDuration(60)).toBe('1m 0s');
    expect(formatDuration(90)).toBe('1m 30s');
    expect(formatDuration(3599)).toBe('59m 59s');
  });

  it('formats hours and minutes', () => {
    expect(formatDuration(3600)).toBe('1h 0m');
    expect(formatDuration(7260)).toBe('2h 1m');
    expect(formatDuration(86399)).toBe('23h 59m');
  });

  it('formats days and hours', () => {
    expect(formatDuration(86400)).toBe('1d 0h');
    expect(formatDuration(172800)).toBe('2d 0h');
    expect(formatDuration(90000)).toBe('1d 1h');
    expect(formatDuration(259200 + 10800)).toBe('3d 3h');
  });
});

describe('formatDurationMs (milliseconds input)', { tags: ['storage'] }, () => {
  it('formats sub-second with precision, trimming trailing zeros', () => {
    expect(formatDurationMs(0)).toBe('0ms');
    expect(formatDurationMs(1)).toBe('1ms');
    expect(formatDurationMs(3.14159)).toBe('3.14ms');
    expect(formatDurationMs(100)).toBe('100ms');
    expect(formatDurationMs(999)).toBe('999ms');
    // trailing zeros removed via Number()
    expect(formatDurationMs(10.0)).toBe('10ms');
  });

  it('formats seconds with 2 decimal places', () => {
    expect(formatDurationMs(1000)).toBe('1.00s');
    expect(formatDurationMs(1500)).toBe('1.50s');
    expect(formatDurationMs(59999)).toBe('60.00s');
  });

  it('formats minutes:seconds with colon notation', () => {
    expect(formatDurationMs(60000)).toBe('1:00m');
    expect(formatDurationMs(90000)).toBe('1:30m');
    expect(formatDurationMs(150000)).toBe('2:30m');
    expect(formatDurationMs(1350000)).toBe('22:30m');
  });

  it('formats hours:minutes:seconds with colon notation', () => {
    expect(formatDurationMs(3600000)).toBe('1:00:00h');
    expect(formatDurationMs(5400000)).toBe('1:30:00h');
    expect(formatDurationMs(3723000)).toBe('1:02:03h');
  });
});

describe('formatMicroseconds (microseconds input)', { tags: ['storage'] }, () => {
  it('formats sub-millisecond as microseconds', () => {
    expect(formatMicroseconds(0)).toBe('0µs');
    expect(formatMicroseconds(500)).toBe('500µs');
    expect(formatMicroseconds(999)).toBe('999µs');
  });

  it('formats milliseconds with 1 decimal', () => {
    expect(formatMicroseconds(1000)).toBe('1.0ms');
    expect(formatMicroseconds(1500)).toBe('1.5ms');
    expect(formatMicroseconds(999999)).toBe('1000.0ms');
  });

  it('formats seconds with 2 decimals', () => {
    expect(formatMicroseconds(1_000_000)).toBe('1.00s');
    expect(formatMicroseconds(1_500_000)).toBe('1.50s');
    expect(formatMicroseconds(59_000_000)).toBe('59.00s');
  });

  it('formats minutes:seconds with colon notation', () => {
    expect(formatMicroseconds(60_000_000)).toBe('1:00m');
    expect(formatMicroseconds(90_000_000)).toBe('1:30m');
  });

  it('formats hours:minutes:seconds with colon notation', () => {
    expect(formatMicroseconds(3_600_000_000)).toBe('1:00:00h');
    expect(formatMicroseconds(5_400_000_000)).toBe('1:30:00h');
  });
});

describe('formatElapsed (seconds input, 1 decimal)', { tags: ['storage'] }, () => {
  it('formats seconds', () => {
    expect(formatElapsed(3.2)).toBe('3.2s');
    expect(formatElapsed(59.9)).toBe('59.9s');
  });

  it('formats minutes and seconds', () => {
    expect(formatElapsed(120)).toBe('2m 0s');
    expect(formatElapsed(135)).toBe('2m 15s');
  });

  it('formats hours and minutes', () => {
    expect(formatElapsed(3600)).toBe('1h 0m');
    expect(formatElapsed(3900)).toBe('1h 5m');
  });
});
