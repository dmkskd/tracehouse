import { createRequire } from 'node:module';
import { writeFile, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
const pluginDir = fileURLToPath(new URL('..', import.meta.url));
const repo = path.dirname(pluginDir);
const require = createRequire(repo + '/package.json');
const webpack = require('webpack');
const { chromium } = require('playwright');
process.chdir(repo + '/grafana-app-plugin');
const { default: createConfig } = await import(
  pathToFileURL(repo + '/grafana-app-plugin/.config/webpack/webpack.config.ts').href
);
process.chdir(repo + '/grafana-app-plugin');
const base = (await createConfig({ development: true }))[0];
const out = await mkdtemp(path.join(tmpdir(), 'tracehouse-style-qa-'));
await writeFile(
  out + '/fixture.tsx',
  `
import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Select } from '@grafana/ui';
import { createTheme, ThemeContext } from '@grafana/data';
import { css } from '@emotion/css';
import { GrafanaControl } from '${repo}/grafana-app-plugin/src/components/GrafanaControl';
import { TracehouseThemeContext } from '${repo}/frontend/src/providers/TracehouseThemeContext';
import { TracehouseStyleContext } from '${repo}/frontend/src/providers/TracehouseStyleContext';
import { createTracehousePortal } from '${repo}/frontend/src/providers/TracehousePortal';
const emotionClass = css\`padding: 7px; background: rgb(8, 90, 80);\`;
const options = [{label:'ClickHouse',value:'clickhouse'},{label:'Test cluster',value:'cluster'}];
function Portal() { return createTracehousePortal(<div id="plugin-popover" className="card" style={{position:'fixed',left:340,top:350,zIndex:100010,padding:16}}>Scoped fixed popover</div>,document.body); }
function Fixture() {
 const [mounted,setMounted]=useState(false),[mode,setMode]=useState<'dark'|'light'>('dark');
 useEffect(()=>{window.installPluginStyles=()=>{window.scopeClass=require('${repo}/grafana-app-plugin/src/styles.module.css').default.root;setMounted(true);};window.setMode=setMode;window.removePlugin=()=>setMounted(false);window.ready=true;},[]);
 const theme=createTheme({colors:{mode}});
 useEffect(()=>{document.body.style.fontFamily=theme.typography.fontFamily;},[theme.typography.fontFamily]);
 return <ThemeContext.Provider value={theme}>
 <section id="outside" style={{padding:20}}><h2>Host page</h2><div id="host-card" className="card host-card">Host card</div><button id="host-button" className="btn host-button">Host button</button><div id="host-select" style={{width:260}}><Select options={options} value="clickhouse" onChange={()=>{}} /></div></section>
 {mounted && <TracehouseThemeContext.Provider value={mode}><TracehouseStyleContext.Provider value={window.scopeClass}>
 <section id="plugin" className={window.scopeClass} data-theme={mode} style={{margin:20,padding:20,border:'1px solid #64748b',minHeight:350}}>
 <h2>TraceHouse scope</h2><div className="card" id="plugin-card" style={{padding:12}}>Plugin card</div>
 <div id="mixed" className={'p-4 '+emotionClass}>Emotion padding: 7px</div><div id="utility-border" className="border" style={{marginTop:8,padding:10}}>Tailwind border</div>
 <h3>Grafana control inside exclusion marker</h3><GrafanaControl><div id="plugin-select" style={{width:260}}><Select options={options} value="clickhouse" onChange={()=>{}} /></div></GrafanaControl>
 <Portal />
 </section></TracehouseStyleContext.Provider></TracehouseThemeContext.Provider>}
 </ThemeContext.Provider>;
}
createRoot(document.getElementById('fixture')!).render(<Fixture />);
`,
);
await writeFile(
  out + '/grafana-ui.mjs',
  `export {Select} from '${repo}/node_modules/@grafana/ui/dist/esm/components/Select/Select.mjs'; export {useTheme2} from '${repo}/node_modules/@grafana/ui/dist/esm/themes/ThemeContext.mjs';`,
);
await new Promise<void>((resolve, reject) =>
  webpack(
    {
      mode: 'development',
      devtool: false,
      context: repo,
      entry: out + '/fixture.tsx',
      output: { path: out, filename: 'fixture.js', publicPath: '', clean: false },
      module: { rules: [...base.module.rules, { test: /\.m?js$/, resolve: { fullySpecified: false } }] },
      resolve: {
        ...base.resolve,
        modules: [repo + '/node_modules'],
        alias: {
          ...base.resolve.alias,
          '@grafana/ui$': out + '/grafana-ui.mjs',
          react$: require.resolve('react'),
          'react-dom$': require.resolve('react-dom'),
          'react/jsx-runtime$': require.resolve('react/jsx-runtime'),
          'react-dom/client$': require.resolve('react-dom/client'),
        },
      },
      resolveLoader: { modules: [repo + '/node_modules'] },
      optimization: { minimize: false },
      performance: { hints: false },
    },
    (error, stats) => {
      if (error || stats?.hasErrors())
        reject(error ?? new Error(stats.toString({ all: false, errors: true })));
      else resolve();
    },
  ),
);
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 1000, height: 800 } });
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.setContent(
    '<html><head><style>body{margin:0;font:14px system-ui;background:#111827;color:#e5e7eb}.host-card{padding:13px;border:3px dashed #8b5cf6;border-radius:3px;background:#374151}.host-button{padding:9px;border:3px dotted pink;border-radius:13px;background:#14532d;color:#fff}</style></head><body><div id="fixture"></div></body></html>',
  );
  await page.addScriptTag({ content: 'window.__name = (fn) => fn;' });
  await page.addScriptTag({ path: out + '/fixture.js' });
  await page.waitForFunction(() => window.ready);
  const sample = () =>
    page.evaluate(() =>
      Object.fromEntries(
        ['host-card', 'host-button', 'host-select'].map((id) => {
          const el = document.getElementById(id)!;
          const s = getComputedStyle(el);
          return [
            id,
            {
              font: s.font,
              color: s.color,
              background: s.backgroundColor,
              padding: s.padding,
              border: s.border,
              borderRadius: s.borderRadius,
              height: el.getBoundingClientRect().height,
            },
          ];
        }),
      ),
    );
  const before = await sample();
  await page.evaluate(() => window.installPluginStyles());
  await page.waitForSelector('#plugin');
  const after = await sample();
  if (JSON.stringify(before) !== JSON.stringify(after))
    throw new Error('Host styling changed: ' + JSON.stringify({ before, after }));
  const measurements = await page.evaluate(() => {
    const style = (id: string) => getComputedStyle(document.getElementById(id)!);
    const marker = document.querySelector('[data-tracehouse-portal]')!;
    const m = getComputedStyle(marker);
    const rect = document.getElementById('plugin-popover')!.getBoundingClientRect();
    return {
      padding: style('mixed').padding,
      borderWidth: style('utility-border').borderTopWidth,
      borderStyle: style('utility-border').borderTopStyle,
      theme: marker.getAttribute('data-theme'),
      portal: {
        display: m.display,
        transform: m.transform,
        filter: m.filter,
        contain: m.contain,
        position: m.position,
        zIndex: m.zIndex,
      },
      popover: { left: rect.left, top: rect.top },
      htmlTheme: document.documentElement.getAttribute('data-theme'),
      hostBackgroundVar: style('outside').getPropertyValue('--bg-primary'),
    };
  });
  if (
    measurements.padding !== '7px' ||
    measurements.borderWidth !== '1px' ||
    measurements.borderStyle !== 'solid'
  )
    throw new Error('Cascade/reset failed: ' + JSON.stringify(measurements));
  if (
    measurements.popover.left !== 340 ||
    measurements.popover.top !== 350 ||
    measurements.portal.transform !== 'none' ||
    measurements.portal.contain !== 'none' ||
    measurements.portal.zIndex !== 'auto' ||
    measurements.htmlTheme !== null ||
    measurements.hostBackgroundVar !== ''
  )
    throw new Error('Portal/host isolation failed: ' + JSON.stringify(measurements));
  const control = async (id: string) =>
    page.locator('#' + id).evaluate((el: HTMLElement) => {
      const input = el.querySelector('input')!;
      const s = getComputedStyle(input);
      const r = el.getBoundingClientRect();
      return { inputHeight: s.height, font: s.font, border: s.border, padding: s.padding, height: r.height };
    });
  const outsideControl = await control('host-select'),
    insideControl = await control('plugin-select');
  if (JSON.stringify(outsideControl) !== JSON.stringify(insideControl))
    throw new Error(
      'Grafana Select changed inside scope: ' + JSON.stringify({ outsideControl, insideControl }),
    );
  await page.screenshot({ path: out + '/dark.png', fullPage: true });
  await page.evaluate(() => window.setMode('light'));
  await page.waitForFunction(
    () => document.querySelector('[data-tracehouse-portal]')?.getAttribute('data-theme') === 'light',
  );
  const light = await page
    .locator('#plugin-card')
    .evaluate((el: HTMLElement) => ({
      background: getComputedStyle(el).backgroundColor,
      text: getComputedStyle(el).color,
    }));
  if (light.background !== 'rgb(255, 255, 255)')
    throw new Error('Light theme did not reach plugin: ' + JSON.stringify(light));
  const lightControlA = await control('host-select'),
    lightControlB = await control('plugin-select');
  if (JSON.stringify(lightControlA) !== JSON.stringify(lightControlB))
    throw new Error(
      'Light Grafana Select changed inside scope: ' + JSON.stringify({ lightControlA, lightControlB }),
    );
  await page.screenshot({ path: out + '/light.png', fullPage: true });
  const beforeUnmount = await sample();
  await page.evaluate(() => window.removePlugin());
  await page.waitForFunction(() => !document.querySelector('[data-tracehouse-portal]'));
  if (JSON.stringify(beforeUnmount) !== JSON.stringify(await sample()))
    throw new Error('Host changed after plugin unmount');
  if (errors.length) throw new Error('Browser errors: ' + errors.join('\n'));
  console.log(
    JSON.stringify(
      {
        hostUnchanged: true,
        measurements,
        outsideControl,
        insideControl,
        light,
        screenshots: [out + '/dark.png', out + '/light.png'],
      },
      null,
      2,
    ),
  );
} finally {
  await browser.close();
}
