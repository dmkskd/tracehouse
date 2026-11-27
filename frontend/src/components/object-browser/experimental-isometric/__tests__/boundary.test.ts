import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

describe('removable spatial renderer boundary', () => {
  it('has exactly one production integration module and no shared/core dependency on the experiment', () => {
    const root = join(dirname(fileURLToPath(import.meta.url)), '../../../..');
    const imports: string[] = [];
    const scan = (dir: string) => {
      for (const item of readdirSync(dir, { withFileTypes: true })) {
        const path = join(dir, item.name);
        if (item.isDirectory()) { if (item.name !== 'experimental-isometric' && item.name !== '__tests__') scan(path); }
        else if (/\.[jt]sx?$/.test(item.name) && /(?:from\s*|import\s*\()['"][^'"]*experimental-isometric/.test(readFileSync(path, 'utf8'))) imports.push(relative(root, path));
      }
    };
    scan(root);
    expect(imports).toEqual(['components/object-browser/ObjectCatalogView.tsx']);
    const core = join(root, '../../packages/core/src');
    scan(core);
    expect(imports).toHaveLength(1);
  });
});
