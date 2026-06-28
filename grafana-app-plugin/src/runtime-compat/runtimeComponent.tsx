import React, { Suspense, lazy, useMemo } from 'react';
import pluginJson from '../plugin.json';

declare const __webpack_public_path__: string;

type Runtime = 'react19' | 'react18';

type RuntimeModule<P> = {
  default?: React.ComponentType<P>;
} & Record<string, React.ComponentType<P> | undefined>;

type SystemLoader<P> = {
  import: (url: string) => Promise<RuntimeModule<P>>;
};

const pluginId = (pluginJson as { id?: string }).id ?? 'dmkskd-tracehouse-app';

function getReactRuntime(): Runtime {
  const major = Number.parseInt((React.version ?? '18').split('.')[0] ?? '18', 10);
  return major >= 19 ? 'react19' : 'react18';
}

function getPluginAssetUrl(fileName: string): string {
  const publicPath = typeof __webpack_public_path__ === 'string' && __webpack_public_path__
    ? __webpack_public_path__
    : `public/plugins/${pluginId}/`;
  return `${publicPath.endsWith('/') ? publicPath : `${publicPath}/`}${fileName}`;
}

function getSystemLoader<P>(): SystemLoader<P> {
  const loader = (window as unknown as { System?: SystemLoader<P> }).System;
  if (!loader?.import) {
    throw new Error('Grafana SystemJS loader is not available');
  }
  return loader;
}

export function createRuntimeComponent<P extends object>(
  chunkName: string,
  exportName: string,
  loadingText: string,
  minHeight = 320
): React.FC<P> {
  const lazyByRuntime = new Map<Runtime, React.LazyExoticComponent<React.ComponentType<P>>>();

  function getComponent(runtime: Runtime) {
    const existing = lazyByRuntime.get(runtime);
    if (existing) {
      return existing;
    }

    const component = lazy(async () => {
      const url = getPluginAssetUrl(`${chunkName}-${runtime}.js`);
      const mod = await getSystemLoader<P>().import(url);
      const RuntimeComponent = mod[exportName] ?? mod.default;
      if (!RuntimeComponent) {
        throw new Error(`${exportName} export was not found in ${url}`);
      }
      return { default: RuntimeComponent };
    });

    lazyByRuntime.set(runtime, component);
    return component;
  }

  const RuntimeComponent: React.FC<P> = (props) => {
    const runtime = getReactRuntime();
    const Component = useMemo(() => getComponent(runtime), [runtime]);
    const fallback = minHeight > 0 ? (
      <div style={{
        minHeight,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--text-secondary)',
        fontFamily: "'Share Tech Mono', monospace",
        fontSize: 12,
      }}>
        {loadingText}
      </div>
    ) : null;

    return (
      <Suspense fallback={fallback}>
        <Component {...props} />
      </Suspense>
    );
  };

  RuntimeComponent.displayName = `Runtime${exportName}`;
  return RuntimeComponent;
}
