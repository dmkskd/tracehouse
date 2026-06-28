import React, { Suspense, lazy, useMemo } from 'react';
import type { ActiveMergeInfo, RunningQueryInfo } from '@tracehouse/core';
import pluginJson from '../plugin.json';

export interface ResourceArena3DProps {
  queries: RunningQueryInfo[];
  merges: ActiveMergeInfo[];
  cpuUsage: number;
  memoryPct: number;
  onQueryClick?: (queryId: string) => void;
  compact?: boolean;
  splitAvailable?: boolean;
  splitActive?: boolean;
  onSplitToggle?: () => void;
}

type ArenaModule = {
  ResourceArena3D?: React.ComponentType<ResourceArena3DProps>;
  default?: React.ComponentType<ResourceArena3DProps>;
};

type SystemLoader = {
  import: (url: string) => Promise<ArenaModule>;
};

const pluginId = (pluginJson as { id?: string }).id ?? 'dmkskd-tracehouse-app';
const lazyByRuntime = new Map<string, React.LazyExoticComponent<React.ComponentType<ResourceArena3DProps>>>();

function getReactRuntime(): 'react19' | 'react18' {
  const major = Number.parseInt((React.version ?? '18').split('.')[0] ?? '18', 10);
  return major >= 19 ? 'react19' : 'react18';
}

function getSystemLoader(): SystemLoader {
  const loader = (window as unknown as { System?: SystemLoader }).System;
  if (!loader?.import) {
    throw new Error('Grafana SystemJS loader is not available');
  }
  return loader;
}

function getArenaComponent(runtime: 'react19' | 'react18') {
  const existing = lazyByRuntime.get(runtime);
  if (existing) {
    return existing;
  }

  const component = lazy(async () => {
    const url = `/public/plugins/${pluginId}/resourceArena3D-${runtime}.js`;
    const mod = await getSystemLoader().import(url);
    const ResourceArena3D = mod.ResourceArena3D ?? mod.default;
    if (!ResourceArena3D) {
      throw new Error(`ResourceArena3D export was not found in ${url}`);
    }
    return { default: ResourceArena3D };
  });

  lazyByRuntime.set(runtime, component);
  return component;
}

const fallbackStyle: React.CSSProperties = {
  minHeight: 420,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  border: '1px solid var(--border-primary)',
  borderRadius: 8,
  color: 'var(--text-secondary)',
  background: 'var(--bg-secondary)',
  fontFamily: "'Share Tech Mono', monospace",
  fontSize: 12,
};

export const ResourceArena3D: React.FC<ResourceArena3DProps> = (props) => {
  const runtime = getReactRuntime();
  const Arena = useMemo(() => getArenaComponent(runtime), [runtime]);

  return (
    <Suspense fallback={<div style={fallbackStyle}>Loading 3D arena...</div>}>
      <Arena {...props} />
    </Suspense>
  );
};

export default ResourceArena3D;
