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
          if (m.sourcesContent[i] !== null) continue;

          let src = m.sources[i];
          src = src.replace(/^webpack:\/\/\//, '');
          src = src.replace(/^webpack:\/\/[^/]+\//, '');

          let abs = path.resolve(this.repoRoot, src);
          if (!fs.existsSync(abs) && src.startsWith('packages/core/src/')) {
            abs = path.resolve(this.repoRoot, src.replace('packages/core/src/', 'packages/ui-shared/src/'));
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
