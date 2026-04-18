import { AppPlugin } from '@grafana/data';
import { App } from './App';
import { AppConfig } from './AppConfig';
import type { AppPluginSettings } from './types';

export const plugin = new AppPlugin<AppPluginSettings>()
  .setRootPage(App)
  .addConfigPage({
    title: 'Configuration',
    icon: 'cog',
    body: AppConfig,
    id: 'configuration',
  });
