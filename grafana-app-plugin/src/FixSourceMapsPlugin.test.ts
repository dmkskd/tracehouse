import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type webpack from 'webpack';
import { decode, encode } from '@jridgewell/sourcemap-codec';
import { expect, it, vi } from 'vitest';
import { FixSourceMapsPlugin } from '../.config/webpack/FixSourceMapsPlugin';

it('removes dependency sources while preserving application locations and unmapped boundaries', () => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'tracehouse-map-'));
  try {
    const outputPath = path.join(root, 'dist');
    mkdirSync(outputPath);
    const mapPath = path.join(outputPath, 'module.js.map');
    writeFileSync(mapPath, JSON.stringify({
      version: 3,
      sources: [
        'webpack:///node_modules/dependency.js',
        'webpack:///ServiceProvider.tsx',
        'webpack:///webpack/runtime/bootstrap',
        'webpack:///App.tsx',
      ],
      sourcesContent: ['dependency', 'provider', 'runtime', 'app'],
      names: ['start'],
      mappings: encode([
        [[0, 0, 0, 0], [10, 1, 6, 2, 0], [20, 2, 0, 0], [30, 3, 8, 4], [40]],
        [[0, 3, 9, 0], [10, 0, 1, 0], [20, 1, 7, 1]],
      ]),
    }));
    const tapAsync = vi.fn();
    new FixSourceMapsPlugin(root).apply({
      outputPath, hooks: { afterEmit: { tapAsync } },
    } as unknown as webpack.Compiler);
    const done = vi.fn();
    tapAsync.mock.calls[0][1]({}, done);
    expect(done).toHaveBeenCalledOnce();

    const result = JSON.parse(readFileSync(mapPath, 'utf8'));
    expect(result.sources).toEqual(['webpack:///ServiceProvider.tsx', 'webpack:///App.tsx']);
    expect(result.sourcesContent).toEqual(['provider', 'app']);
    expect(result.names).toEqual(['start']);
    expect(decode(result.mappings)).toEqual([
      [[0], [10, 0, 6, 2, 0], [20], [30, 1, 8, 4], [40]],
      [[0, 1, 9, 0], [10], [20, 0, 7, 1]],
    ]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
