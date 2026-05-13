/**
 * Maps plain env vars (no VITE_ prefix) to compile-time globals
 * injected via Vite's `define` option.
 *
 * This is the only place that maps env var names → global names.
 * If you swap the build tool, replace this with DefinePlugin / equivalent.
 */

import { readFileSync } from 'fs'
import { resolve } from 'path'

const str = (v: string | undefined) =>
  v !== undefined ? JSON.stringify(v) : 'undefined'

const bool = (v: string | undefined) =>
  v === 'true' ? 'true' : 'false'

export function tracehouseBuildDefines(): Record<string, string> {
  const e = process.env
  const pkg = JSON.parse(readFileSync(resolve(__dirname, '..', 'package.json'), 'utf-8'))
  return {
    '__TH_VERSION__': JSON.stringify(pkg.version),
    '__TH_DEFAULT_CH_HOST__': str(e.TH_DEFAULT_CH_HOST),
    '__TH_DEFAULT_CH_PORT__': str(e.TH_DEFAULT_CH_PORT),
    '__TH_DEFAULT_CH_USER__': str(e.TH_DEFAULT_CH_USER),
    '__TH_DEFAULT_CH_PASSWORD__': str(e.TH_DEFAULT_CH_PASSWORD),
    '__TH_DEFAULT_CH_DATABASE__': str(e.TH_DEFAULT_CH_DATABASE),
    '__TH_DEFAULT_CH_SECURE__': bool(e.TH_DEFAULT_CH_SECURE),
    '__TH_DEFAULT_CH_CLUSTER__': str(e.TH_DEFAULT_CH_CLUSTER),
    '__TH_AUTO_CONNECT__': bool(e.TH_AUTO_CONNECT),
    '__TH_BUNDLED_PROXY__': bool(e.TH_BUNDLED_PROXY),
    '__TH_DASHBOARD_PREVIEW__': bool(e.TH_DASHBOARD_PREVIEW),
  }
}
