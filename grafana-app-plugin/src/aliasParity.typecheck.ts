/**
 * Swapped-module parity, checked by `tsc -p tsconfig.alias-parity.json` (part of `npm test`).
 *
 * The plugin's webpack config swaps some frontend modules for Grafana versions
 * (see docs/site/docs/development/grafana-plugin.md). Pages are typechecked
 * against the frontend module but run the plugin one, so each plugin module must
 * export every name the frontend one does, and its hooks must be usable wherever
 * the frontend hooks are: plugin type assignable to frontend type.
 *
 * Nothing here runs; a mismatch is a compile error.
 */

import type * as FrontendUrlState from '../../frontend/src/hooks/useUrlState';
import type * as PluginUrlState from './hooks/useUrlState';
import type * as FrontendAppLocation from '../../frontend/src/hooks/useAppLocation';
import type * as PluginAppLocation from './hooks/useAppLocation';
import type * as FrontendUrlParams from '../../frontend/src/utils/urlParams';
import type * as PluginUrlParams from './utils/urlParams';

type Assert<T extends true> = T;
type NoMissingExports<Frontend, Plugin> = Exclude<keyof Frontend, keyof Plugin> extends never ? true : false;
type Substitutes<Plugin, Frontend> = Plugin extends Frontend ? true : false;

const SCHEMA = { th_probe: { type: 'string' } } satisfies FrontendUrlState.UrlSchema;
type Schema = typeof SCHEMA;

export type AliasParity = [
  Assert<NoMissingExports<typeof FrontendUrlState, typeof PluginUrlState>>,
  Assert<Substitutes<
    ReturnType<typeof PluginUrlState.useUrlState<Schema>>,
    ReturnType<typeof FrontendUrlState.useUrlState<Schema>>
  >>,
  Assert<NoMissingExports<typeof FrontendAppLocation, typeof PluginAppLocation>>,
  Assert<Substitutes<typeof PluginAppLocation.useAppLocation, typeof FrontendAppLocation.useAppLocation>>,
  Assert<NoMissingExports<typeof FrontendUrlParams, typeof PluginUrlParams>>,
  Assert<Substitutes<typeof PluginUrlParams.getUrlParam, typeof FrontendUrlParams.getUrlParam>>,
];
