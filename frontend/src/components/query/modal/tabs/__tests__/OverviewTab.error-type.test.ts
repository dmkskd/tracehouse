import { describe, expect, it } from 'vitest';
import {
  clickHouseExceptionPhase,
  clickHouseExceptionType,
} from '../OverviewTab';

describe('OverviewTab ClickHouse error types', () => {
  it('keeps ExceptionBeforeStart verbatim and explains its phase', () => {
    const type = clickHouseExceptionType('ExceptionBeforeStart');

    expect(type).toBe('ExceptionBeforeStart');
    expect(clickHouseExceptionPhase(type)).toBe('before execution');
  });

  it('keeps ExceptionWhileProcessing verbatim and explains its phase', () => {
    const type = clickHouseExceptionType('ExceptionWhileProcessing');

    expect(type).toBe('ExceptionWhileProcessing');
    expect(clickHouseExceptionPhase(type)).toBe('during execution');
  });

  it('does not invent an exact ClickHouse type for generic errors', () => {
    expect(clickHouseExceptionType('error')).toBeNull();
    expect(clickHouseExceptionPhase(null)).toBeNull();
  });
});
