// Post-process source maps:
// 1. Populate sourcesContent for monorepo files outside webpack's context dir
// 2. Remove node_modules entries (validator can't match them against source archive)
const fs = require('fs');
const path = require('path');

const distDir = path.resolve(__dirname, '../dist');
const repoRoot = path.resolve(__dirname, '../..');

for (const mapFile of fs.readdirSync(distDir).filter(f => f.endsWith('.map'))) {
  const mapPath = path.join(distDir, mapFile);
  const m = JSON.parse(fs.readFileSync(mapPath, 'utf8'));

  // Fill missing sourcesContent from disk
  for (let i = 0; i < m.sources.length; i++) {
    if (m.sourcesContent[i] !== null) continue;

    let src = m.sources[i];
    src = src.replace(/^webpack:\/\/\//, '');
    src = src.replace(/^webpack:\/\/[^/]+\//, '');

    let abs = path.resolve(repoRoot, src);
    if (!fs.existsSync(abs) && src.startsWith('packages/core/src/')) {
      abs = path.resolve(repoRoot, src.replace('packages/core/src/', 'packages/ui-shared/src/'));
    }
    if (fs.existsSync(abs)) {
      m.sourcesContent[i] = fs.readFileSync(abs, 'utf8');
    }
  }

  // Remove node_modules and virtual module entries
  const keep = [];
  for (let i = 0; i < m.sources.length; i++) {
    const src = m.sources[i];
    if (src.includes('node_modules/') || src.includes('webpack/runtime')) continue;
    keep.push(i);
  }

  if (keep.length < m.sources.length) {
    m.sources = keep.map(i => m.sources[i]);
    m.sourcesContent = keep.map(i => m.sourcesContent[i]);
  }

  fs.writeFileSync(mapPath, JSON.stringify(m));
}
