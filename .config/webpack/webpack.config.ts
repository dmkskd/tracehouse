/*
 * Grafana plugin-validator compatibility marker.
 *
 * The real plugin webpack config lives in:
 *   grafana-app-plugin/.config/webpack/webpack.config.ts
 *
 * The official grafana/plugin-actions build-plugin action validates this
 * monorepo with `-sourceCodeUri file://./`, so the validator checks the repo
 * root for create-plugin's standard webpack marker.
 *
 * @grafana/create-plugin
 */
export {};
