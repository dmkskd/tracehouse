export { AdapterError } from './types.js';
export type { IClickHouseAdapter, AdapterErrorCategory } from './types.js';
// BrowserAdapter is NOT exported here because it uses @clickhouse/client-web which
// pulls in @clickhouse/client-common (a large dependency).
// Import it directly via '@tracehouse/core/adapters/browser-adapter' when needed.
export { GrafanaAdapter } from './grafana-adapter.js';
// Note: HttpAdapter is NOT exported here because it uses @clickhouse/client (Node.js only).
// Import it directly via '@tracehouse/core/adapters/http-adapter' for Node.js usage.
export { ClusterAwareAdapter } from './cluster-adapter.js';
export { HostTargetedAdapter } from './host-targeted-adapter.js';
export { ProxyAdapter } from './proxy-adapter.js';
export { applyStickyRouting } from './sticky-routing.js';
