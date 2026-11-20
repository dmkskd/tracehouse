import { createRequire } from 'node:module';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postcss from 'postcss';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const scope = require('./scope-styles.cjs');
const tailwind = require('@tailwindcss/postcss');

async function transform(css: string) {
  return (await postcss([scope()]).process(css, { from: undefined })).root;
}

describe('Grafana stylesheet isolation', () => {
  it('anchors generic classes, root variables, themes, scrollbars, and nested rules', async () => {
    const root =
      await transform(`:root { --bg: black; } [data-theme="light"] .card, [data-theme="light"] .btn { color: white; }
      ::-webkit-scrollbar { width: 6px; } *, ::before, ::after { box-sizing: border-box; } .btn { &:hover { color: red; } }`);
    const css = root.toString();
    expect(css).not.toMatch(/:root\s*\{/);
    expect(css).toContain(':where(:local(.root))[data-theme="light"] .card');
    expect(css).toContain(':where(:local(.root))[data-theme="light"] .btn');
    expect(css).toContain(
      ':where(:local(.root)) :not(:where([data-tracehouse-grafana], [data-tracehouse-grafana] *))::-webkit-scrollbar',
    );
    expect(css).toContain('&:hover:not(:where([data-tracehouse-grafana], [data-tracehouse-grafana] *))');
    expect(css).not.toMatch(/\.card\s*\{/);
    expect(css).toContain(':not(:where([data-tracehouse-grafana], [data-tracehouse-grafana] *))::before');
    expect(css).not.toContain(')) ::before');
  });

  it('preserves explicit local-root scopes without requiring a second root ancestor', async () => {
    const root = await transform(`:where(:local(.root)) { --bg: black; }
      :where(:local(.root)) .card, :where(:local(.root)) .btn:hover { color: white; }
      :where(:local(.root))[data-theme="light"] .card { color: black; }
      @media (width > 800px) { :where(:local(.root)) .card { padding: 4px; } }
      .p-4 { padding: 1rem; }`);
    root.walkRules((rule) => {
      for (const selector of rule.selectors) {
        expect(selector.match(/:where\(:local\(\.root\)\)/g)).toHaveLength(1);
        expect(selector.trim()).toMatch(/^:where\(:local\(\.root\)\)/);
        expect(selector).toContain('[data-tracehouse-grafana]');
      }
    });
    expect(root.toString()).toContain(':where(:local(.root))[data-theme="light"] .card');
  });

  it('requires explicit scopes on handwritten plugin rules', async () => {
    for (const file of ['src/styles.module.css', 'src/reset.css']) {
      const root = postcss.parse(await readFile(path.resolve(file), 'utf8'));
      root.walkRules((rule) => {
        for (const selector of rule.selectors) {
          expect(selector.trim(), `${file}: ${selector}`).toMatch(/^:where\(:local\(\.root\)\)/);
        }
      });
    }
  });

  it('preserves and namespaces layers and animations without scoping keyframe stops', async () => {
    const root =
      await transform(`@layer base, utilities; @layer utilities { .animate-spin { animation: spin 1s; } }
      @keyframes spin { to { transform: rotate(360deg); } }
      @property --tw-ring-color { syntax: "<color>"; inherits: false; initial-value: black; }
      .ring { --tw-ring-color: red; color: var(--tw-ring-color); }`);
    const css = root.toString();
    expect(css).toContain('@layer dmkskd-tracehouse-app-base, dmkskd-tracehouse-app-utilities');
    expect(css).toContain('animation: dmkskd-tracehouse-app-spin 1s');
    expect(css).toContain('@keyframes dmkskd-tracehouse-app-spin { to {');
    expect(css).not.toMatch(/(?<![\w-])--tw-/);
    expect(css).toContain('@property --dmkskd-tracehouse-app-tw-ring-color');
  });

  it('compiles the actual Tailwind entry with all selector rules scoped and full Preflight excluded', async () => {
    const from = path.resolve('src/styles.module.css');
    const result = await postcss([tailwind({ base: process.cwd() }), scope()]).process(
      await readFile(from, 'utf8'),
      { from },
    );
    result.root.walkRules((rule) => {
      let parent = rule.parent;
      while (parent && parent.type !== 'root') {
        if (parent.type === 'rule' || (parent.type === 'atrule' && /keyframes$/.test(parent.name))) return;
        parent = parent.parent;
      }
      expect(rule.selector, rule.toString()).toContain(':where(:local(.root))');
      expect(rule.selector, rule.toString()).toContain('[data-tracehouse-grafana]');
    });
    expect(result.css).not.toContain('-webkit-text-size-adjust');
    expect(result.css).toContain('border: 0 solid');
    expect(result.css).toContain('dmkskd-tracehouse-app-utilities');
    expect(result.css).toContain(' .p-4:');
    expect(result.css).toContain(' .border:');
  });
});
