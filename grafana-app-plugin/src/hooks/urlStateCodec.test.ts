import { describe, expect, it } from 'vitest';
import {
  parseSearchParam,
  serializeParam,
  type UrlParamDef,
} from './urlStateCodec';

const stringList = { type: 'string[]' } satisfies UrlParamDef;

describe('Grafana URL-state codec', () => {
  it('parses repeated multi-value parameters as an array', () => {
    const params = new URLSearchParams(
      'status=Running&status=OK&status=%20Error%20',
    );

    expect(parseSearchParam(params, 'status', stringList)).toEqual([
      'Running',
      'OK',
      'Error',
    ]);
  });

  it('serializes multi-value parameters for Grafana locationService', () => {
    expect(serializeParam(['Running', ' OK ', ''], stringList)).toEqual([
      'Running',
      'OK',
    ]);
  });

  it('normalizes a legacy scalar multi-value parameter', () => {
    expect(serializeParam('Running', stringList)).toEqual(['Running']);
  });

  it('uses the configured default when no multi-value parameter is present', () => {
    const params = new URLSearchParams();
    const definition = {
      type: 'string[]',
      default: ['Running'],
    } satisfies UrlParamDef<string[]>;

    expect(parseSearchParam(params, 'status', definition)).toEqual(['Running']);
  });
});
