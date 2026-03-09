console.log('[TraceHouse] Module loading...');

import { AppPlugin } from '@grafana/data';
import { App } from './App';
import { AppConfig } from './AppConfig';
import type { AppPluginSettings } from './types';

console.log('[TraceHouse] Imports done, creating plugin...');

export const plugin = new AppPlugin<AppPluginSettings>()
  .setRootPage(App)
  .addConfigPage({
    title: 'Configuration',
    icon: 'cog',
    body: AppConfig,
    id: 'configuration',
  });

console.log('[TraceHouse] Plugin created:', plugin);
