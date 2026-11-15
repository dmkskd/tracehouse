import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

function check(config, workers) {
  const bin = mkdtempSync(join(tmpdir(), 'tracehouse-topology-'));
  try {
    writeFileSync(join(bin, 'kind'), '#!/bin/bash\nprintf "%s\\n" "$MOCK_NODES"\n', { mode: 0o755 });
    const nodes = ['tracehouse-dev-control-plane', ...Array.from({ length: workers }, (_, i) => `tracehouse-dev-worker${i + 1}`)];
    return spawnSync('/bin/bash', [
      fileURLToPath(new URL('./check-kind-topology.sh', import.meta.url)), 'tracehouse-dev',
      fileURLToPath(new URL(config, import.meta.url)),
    ], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${bin}:/usr/bin:/bin`, MOCK_NODES: nodes.join('\n') },
    });
  } finally {
    rmSync(bin, { recursive: true, force: true });
  }
}

test('accepts an existing cluster matching the regular or large configuration', () => {
  assert.equal(check('./kind-config.yaml', 4).status, 0);
  assert.equal(check('./altinity-large/kind-config.yaml', 8).status, 0);
});

test('rejects reusing a four-worker cluster for eight servers with a data-loss explanation', () => {
  const result = check('./altinity-large/kind-config.yaml', 4);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /has 5 nodes.*requires 9/);
  assert.match(result.stderr, /No manifests were applied/);
  assert.match(result.stderr, /Recreating it deletes its local data/);
});

test('also rejects applying the regular topology to an existing large cluster', () => {
  const result = check('./kind-config.yaml', 8);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /has 9 nodes.*requires 5/);
});
