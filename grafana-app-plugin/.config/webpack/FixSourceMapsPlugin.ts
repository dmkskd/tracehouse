import webpack from 'webpack';
import fs from 'fs';
import path from 'path';
import { decode, encode, type SourceMapSegment } from '@jridgewell/sourcemap-codec';

const PLUGIN_NAME = 'FixSourceMapsPlugin';
const SOURCE_ROOTS = [
  'grafana-app-plugin/src',
  'frontend/src',
  'packages/core/src',
  'packages/ui-shared/src',
];

type SourceIndex = Map<string, string[]>;

function normalizeContent(content: string): string {
  return content.replace(/\r\n/g, '\n');
}

function walkSourceFiles(root: string, files: string[] = []): string[] {
  if (!fs.existsSync(root)) return files;
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const entryPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      walkSourceFiles(entryPath, files);
    } else if (/\.[cm]?[jt]sx?$/.test(entry.name)) {
      files.push(entryPath);
    }
  }
  return files;
}

function buildSourceIndex(repoRoot: string): SourceIndex {
  const index: SourceIndex = new Map();
  for (const sourceRoot of SOURCE_ROOTS) {
    const absRoot = path.resolve(repoRoot, sourceRoot);
    for (const file of walkSourceFiles(absRoot)) {
      const content = normalizeContent(fs.readFileSync(file, 'utf8'));
      const repoRelative = path.relative(repoRoot, file).replace(/\\/g, '/');
      const matches = index.get(content);
      if (matches) {
        matches.push(repoRelative);
      } else {
        index.set(content, [repoRelative]);
      }
    }
  }
  return index;
}

function stripWebpackPrefix(source: string): string {
  return source
    .replace(/^webpack:\/\/\//, '')
    .replace(/^webpack:\/\/[^/]+\//, '');
}

function toRepoRelative(source: string): string {
  return stripWebpackPrefix(source).replace(/^\.\.\//, '');
}

function toSourceMapPath(repoRelative: string): string {
  return repoRelative.startsWith('grafana-app-plugin/src/')
    ? repoRelative.substring('grafana-app-plugin/src/'.length)
    : repoRelative;
}

function toWebpackSource(original: string, repoRelative: string): string {
  const prefix = original.match(/^webpack:\/\/[^/]*\//)?.[0] ?? 'webpack:///';
  return `${prefix}${toSourceMapPath(repoRelative)}`;
}

export class FixSourceMapsPlugin {
  private repoRoot: string;

  constructor(repoRoot: string) {
    this.repoRoot = repoRoot;
  }

  apply(compiler: webpack.Compiler) {
    compiler.hooks.afterEmit.tapAsync(PLUGIN_NAME, (_compilation, callback) => {
      const outputPath = compiler.outputPath;
      const sourceIndex = buildSourceIndex(this.repoRoot);

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

          const sourceContent = m.sourcesContent[i];
          if (typeof sourceContent === 'string') {
            const repoRelative = toRepoRelative(m.sources[i]);
            const abs = path.resolve(this.repoRoot, repoRelative);
            const matches = sourceIndex.get(normalizeContent(sourceContent));
            if (
              matches?.length &&
              (!fs.existsSync(abs) || normalizeContent(fs.readFileSync(abs, 'utf8')) !== normalizeContent(sourceContent))
            ) {
              const basename = path.basename(repoRelative);
              const bestMatch = matches.find((match) => path.basename(match) === basename) ?? matches[0];
              m.sources[i] = toWebpackSource(m.sources[i], bestMatch);
              src = stripWebpackPrefix(m.sources[i]);
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
        const sourceIndices = m.sources.map((src: string, index: number) => {
          if (src.includes('node_modules/') || src.includes('webpack/runtime')) return -1;
          keep.push(index);
          return keep.length - 1;
        });
        if (keep.length < m.sources.length) {
          m.mappings = encode(decode(m.mappings).map(line => line.map((segment): SourceMapSegment => {
            if (segment.length === 1) return segment;
            const sourceIndex = sourceIndices[segment[1]];
            if (sourceIndex === -1) return [segment[0]];
            return segment.length === 5
              ? [segment[0], sourceIndex, segment[2], segment[3], segment[4]]
              : [segment[0], sourceIndex, segment[2], segment[3]];
          })));
          m.sources = keep.map(index => m.sources[index]);
          m.sourcesContent = keep.map(index => m.sourcesContent[index]);
        }

        fs.writeFileSync(mapPath, JSON.stringify(m));
      }

      callback();
    });
  }
}
