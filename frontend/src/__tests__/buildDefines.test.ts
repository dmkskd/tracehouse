import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { tracehouseBuildDefines } from '../../vite.buildDefines';

describe('tracehouseBuildDefines', () => {
  const ORIG = { ...process.env };

  beforeEach(() => {
    // clear all TH_ vars
    for (const k of Object.keys(process.env)) {
      if (k.startsWith('TH_')) delete process.env[k];
    }
  });

  afterEach(() => {
    // restore original env
    for (const k of Object.keys(process.env)) {
      if (k.startsWith('TH_')) delete process.env[k];
    }
    Object.assign(process.env, ORIG);
  });

  it('returns all expected keys', () => {
    const defines = tracehouseBuildDefines();
    expect(Object.keys(defines)).toEqual([
      '__TH_DEFAULT_CH_HOST__',
      '__TH_DEFAULT_CH_PORT__',
      '__TH_DEFAULT_CH_USER__',
      '__TH_DEFAULT_CH_PASSWORD__',
      '__TH_DEFAULT_CH_DATABASE__',
      '__TH_DEFAULT_CH_SECURE__',
      '__TH_DEFAULT_CH_CLUSTER__',
      '__TH_AUTO_CONNECT__',
      '__TH_BUNDLED_PROXY__',
      '__TH_DASHBOARD_PREVIEW__',
    ]);
  });

  it('returns undefined for unset string vars', () => {
    const defines = tracehouseBuildDefines();
    expect(defines['__TH_DEFAULT_CH_HOST__']).toBe('undefined');
    expect(defines['__TH_DEFAULT_CH_CLUSTER__']).toBe('undefined');
  });

  it('returns false for unset boolean vars', () => {
    const defines = tracehouseBuildDefines();
    expect(defines['__TH_BUNDLED_PROXY__']).toBe('false');
    expect(defines['__TH_AUTO_CONNECT__']).toBe('false');
    expect(defines['__TH_DASHBOARD_PREVIEW__']).toBe('false');
  });

  it('injects string values as JSON-quoted strings', () => {
    process.env.TH_DEFAULT_CH_HOST = 'my-cluster.example.com';
    process.env.TH_DEFAULT_CH_PORT = '9440';

    const defines = tracehouseBuildDefines();
    expect(defines['__TH_DEFAULT_CH_HOST__']).toBe('"my-cluster.example.com"');
    expect(defines['__TH_DEFAULT_CH_PORT__']).toBe('"9440"');
  });

  it('injects boolean true when env var is "true"', () => {
    process.env.TH_BUNDLED_PROXY = 'true';
    process.env.TH_AUTO_CONNECT = 'true';
    process.env.TH_DASHBOARD_PREVIEW = 'true';

    const defines = tracehouseBuildDefines();
    expect(defines['__TH_BUNDLED_PROXY__']).toBe('true');
    expect(defines['__TH_AUTO_CONNECT__']).toBe('true');
    expect(defines['__TH_DASHBOARD_PREVIEW__']).toBe('true');
  });

  it('treats non-"true" boolean values as false', () => {
    process.env.TH_BUNDLED_PROXY = 'yes';
    process.env.TH_AUTO_CONNECT = '1';
    process.env.TH_DEFAULT_CH_SECURE = '';

    const defines = tracehouseBuildDefines();
    expect(defines['__TH_BUNDLED_PROXY__']).toBe('false');
    expect(defines['__TH_AUTO_CONNECT__']).toBe('false');
    expect(defines['__TH_DEFAULT_CH_SECURE__']).toBe('false');
  });
});
