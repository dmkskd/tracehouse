import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, copyFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { parseAllDocuments, parse } from 'yaml';

function runDeployment(failure = '') {
  const root = mkdtempSync(join(tmpdir(), 'tracehouse-grafana-'));
  try {
    const scripts = join(root, 'infra/k8s');
    const dist = join(root, 'grafana-app-plugin/dist');
    const bin = join(root, 'bin');
    for (const dir of [scripts, dist, bin]) mkdirSync(dir, { recursive: true });
    copyFileSync(new URL('./deploy-grafana.sh', import.meta.url), join(scripts, 'deploy-grafana.sh'));
    for (const file of ['plugin.json', 'module.js']) writeFileSync(join(dist, file), '{}');
    const log = join(root, 'commands');
    writeFileSync(log, '');
    for (const command of ['docker', 'kind', 'kubectl']) {
      writeFileSync(join(bin, command), `#!/bin/bash
printf '%s\\n' '${command} '"$*" >> "$MOCK_LOG"
if [[ '${command} '"$*" == *"$MOCK_FAIL"* && -n "$MOCK_FAIL" ]]; then exit 42; fi
`, { mode: 0o755 });
    }
    const result = spawnSync('/bin/bash', [join(scripts, 'deploy-grafana.sh')], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${bin}:/usr/bin:/bin`, MOCK_LOG: log, MOCK_FAIL: failure },
    });
    return { ...result, commands: readFileSync(log, 'utf8').trim().split('\n') };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

test('loads the image before deploying and waits for the restarted Grafana', () => {
  const result = runDeployment();
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(result.commands.map(line => line.split(' ')[0]), [
    'kubectl', 'docker', 'kind', 'kubectl', 'kubectl', 'kubectl', 'kubectl',
  ]);
  for (const line of result.commands.filter(line => line.startsWith('kubectl '))) {
    assert.match(line, /--context kind-tracehouse-dev/);
  }
  assert.match(result.commands.at(-2), /rollout restart deployment\/grafana/);
  assert.match(result.commands.at(-1), /rollout status deployment\/grafana/);
  assert.match(result.stdout, /http:\/\/localhost:3001\/a\/dmkskd-tracehouse-app/);
});

test('does not change Kubernetes resources if loading the image fails', () => {
  const result = runDeployment('kind load');
  assert.equal(result.status, 42);
  assert.equal(result.commands.length, 3);
  assert.doesNotMatch(result.stdout, /TraceHouse in Grafana:/);
});

test('propagates rollout failures instead of reporting Grafana ready', () => {
  const result = runDeployment('rollout status');
  assert.equal(result.status, 42);
  assert.doesNotMatch(result.stdout, /TraceHouse in Grafana:/);
});

test('provisions the app and mounts every referenced ConfigMap', () => {
  const docs = parseAllDocuments(readFileSync(new URL('./grafana.yaml', import.meta.url), 'utf8'));
  for (const doc of docs) assert.deepEqual(doc.errors, []);
  const objects = docs.map(doc => doc.toJSON());
  const deployment = objects.find(object => object.kind === 'Deployment');
  const pod = deployment.spec.template.spec;
  const container = pod.containers[0];
  const configMaps = new Map(objects.filter(object => object.kind === 'ConfigMap').map(object => [object.metadata.name, object]));
  const dashboardDocs = parseAllDocuments(readFileSync(new URL('./grafana-dashboards-cm.yaml', import.meta.url), 'utf8'));
  for (const doc of dashboardDocs) {
    const object = doc.toJSON();
    if (object?.kind === 'ConfigMap') configMaps.set(object.metadata.name, object);
  }
  for (const mount of container.volumeMounts) {
    const volume = pod.volumes.find(volume => volume.name === mount.name);
    assert.ok(volume, `Missing volume ${mount.name}`);
    assert.ok(configMaps.has(volume.configMap.name), `Missing ConfigMap ${volume.configMap.name}`);
  }
  const apps = parse(configMaps.get('grafana-apps').data['tracehouse.yaml']);
  assert.deepEqual(apps.apps, [{ type: 'dmkskd-tracehouse-app', org_id: 1, disabled: false }]);
  assert.equal(container.imagePullPolicy, 'Never');
  const datasource = parse(configMaps.get('grafana-datasources').data['clickhouse.yaml']);
  assert.equal(datasource.datasources[0].jsonData.host, 'clickhouse-external');
});
