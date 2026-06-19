import webpack from 'webpack';
import fs from 'fs';
import path from 'path';

const PLUGIN_NAME = 'FixSourceMapsPlugin';

export class FixSourceMapsPlugin {
  private repoRoot: string;

  constructor(repoRoot: string) {
    this.repoRoot = repoRoot;
  }

  apply(compiler: webpack.Compiler) {
    compiler.hooks.afterEmit.tapAsync(PLUGIN_NAME, (_compilation, callback) => {
      const outputPath = compiler.outputPath;

      for (const mapFile of fs.readdirSync(outputPath).filter((f) => f.endsWith('.map'))) {
        const mapPath = path.join(outputPath, mapFile);
        const m = JSON.parse(fs.readFileSync(mapPath, 'utf8'));

        for (let i = 0; i < m.sources.length; i++) {
          let src = m.sources[i];
          src = src.replace(/^webpack:\/\/\//, '');
          src = src.replace(/^webpack:\/\/[^/]+\//, '');

          const repoRelativeSrc = src.replace(/^\.\.\//, '');
          const corePrefix = 'packages/core/src/';
          if (repoRelativeSrc.startsWith(corePrefix)) {
            const coreAbs = path.resolve(this.repoRoot, repoRelativeSrc);
            const uiSharedRelativeSrc = repoRelativeSrc.replace(corePrefix, 'packages/ui-shared/src/');
            const uiSharedAbs = path.resolve(this.repoRoot, uiSharedRelativeSrc);
            if (!fs.existsSync(coreAbs) && fs.existsSync(uiSharedAbs)) {
              m.sources[i] = m.sources[i].replace(corePrefix, 'packages/ui-shared/src/');
              src = src.replace(corePrefix, 'packages/ui-shared/src/');
            }
          }

          if (m.sourcesContent[i] !== null) continue;

          let abs = path.resolve(this.repoRoot, src.replace(/^\.\.\//, ''));
          if (!fs.existsSync(abs) && src.replace(/^\.\.\//, '').startsWith(corePrefix)) {
            abs = path.resolve(this.repoRoot, src.replace(/^\.\.\//, '').replace(corePrefix, 'packages/ui-shared/src/'));
          }
          if (fs.existsSync(abs)) {
            m.sourcesContent[i] = fs.readFileSync(abs, 'utf8');
          }
        }

        const keep: number[] = [];
        for (let i = 0; i < m.sources.length; i++) {
          const src = m.sources[i];
          if (src.includes('node_modules/') || src.includes('webpack/runtime')) continue;
          keep.push(i);
        }

        if (keep.length < m.sources.length) {
          m.sources = keep.map((i) => m.sources[i]);
          m.sourcesContent = keep.map((i) => m.sourcesContent[i]);
        }

        fs.writeFileSync(mapPath, JSON.stringify(m));
      }

      callback();
    });
  }
}
