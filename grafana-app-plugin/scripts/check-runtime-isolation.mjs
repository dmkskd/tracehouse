import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const modulePath = resolve('dist/module.js');
const moduleSource = readFileSync(modulePath, 'utf8');

const forbiddenPatterns = [
  'ReactCurrentBatchConfig',
  'react-reconciler',
  '@react-three/fiber',
  '@react-three/drei',
];

const matches = forbiddenPatterns.filter((pattern) => moduleSource.includes(pattern));

if (matches.length > 0) {
  console.error('React Three Fiber runtime code leaked into dist/module.js.');
  console.error('The Grafana entry bundle must stay React 18/19 neutral; move 3D imports behind runtime-compat chunks.');
  console.error(`Matched: ${matches.join(', ')}`);
  process.exit(1);
}

console.log('Runtime isolation check passed.');
