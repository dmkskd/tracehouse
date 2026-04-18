import { mergeWithRules } from 'webpack-merge';
import path from 'path';
import { fileURLToPath } from 'url';
import webpack, { type Configuration } from 'webpack';
import grafanaConfig, { type Env } from './.config/webpack/webpack.config.ts';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default async (env: Env): Promise<Configuration> => {
  const baseConfig = await grafanaConfig(env);

  // Disable the built-in devtool — we replace it with SourceMapDevToolPlugin
  // so we can control moduleFilenameTemplate and ensure sourcesContent is
  // populated for monorepo files outside grafana-app-plugin/src/.
  baseConfig.devtool = false;

  const customMerge = mergeWithRules({
    module: {
      rules: {
        test: 'match',
        use: 'replace',
      },
    },
  });

  return customMerge(baseConfig, {
    resolve: {
      extensions: ['.ts', '.tsx', '.js', '.jsx', '.css'],
      alias: {
        '@tracehouse/core': path.resolve(__dirname, '../packages/core/dist'),
        '@tracehouse/ui-shared': path.resolve(__dirname, '../packages/ui-shared/dist'),
        '@frontend': path.resolve(__dirname, '../frontend/src'),
        [path.resolve(__dirname, '../frontend/src/stores/connectionStore')]:
          path.resolve(__dirname, 'src/stores/connectionStore'),
        [path.resolve(__dirname, '../frontend/src/providers/ClickHouseProvider')]:
          path.resolve(__dirname, 'src/ServiceProvider'),
        [path.resolve(__dirname, '../frontend/src/hooks/useAppLocation')]:
          path.resolve(__dirname, 'src/hooks/useAppLocation.ts'),
        [path.resolve(__dirname, '../frontend/src/hooks/useUrlState')]:
          path.resolve(__dirname, 'src/hooks/useUrlState.ts'),
        [path.resolve(__dirname, '../frontend/src/utils/urlParams')]:
          path.resolve(__dirname, 'src/utils/urlParams.ts'),
        'react-router-dom': path.resolve(__dirname, 'src/stubs/react-router-dom.tsx'),
      },
      fallback: {
        stream: false,
        zlib: false,
        crypto: false,
        http: false,
        https: false,
        os: false,
        buffer: false,
        url: false,
        util: false,
        path: false,
        fs: false,
        net: false,
        tls: false,
      },
    },
    plugins: [
      new webpack.SourceMapDevToolPlugin({
        filename: '[file].map',
        noSources: false,
        moduleFilenameTemplate: (info: { resourcePath: string; allLoaders?: string; namespace: string }) => {
          let p = info.resourcePath;
          // Context is src/, so plugin-local files are ./X
          // Monorepo files are ../../packages/core/src/X, ../../frontend/src/X
          if (p.startsWith('./')) p = p.substring('./'.length);
          // imports-loader produces paths like grafana-app-plugin/src/module.ts
          if (p.startsWith('grafana-app-plugin/src/')) p = p.substring('grafana-app-plugin/src/'.length);
          if (p.startsWith('../../frontend/')) p = p.substring('../../'.length);
          else if (p.startsWith('../../node_modules/')) p = p.substring('../../'.length);
          else if (p.startsWith('../../src/')) p = 'packages/core/' + p.substring('../../'.length);
          else if (p.startsWith('../../')) p = p.substring('../../'.length);
          const loaders = info.allLoaders ? `?${info.allLoaders}` : '';
          return `webpack://${info.namespace}/${p}${loaders}`;
        },
      }),
      new webpack.NormalModuleReplacementPlugin(
        /^node:/,
        (resource) => {
          resource.request = resource.request.replace(/^node:/, '');
        }
      ),
      new webpack.IgnorePlugin({ resourceRegExp: /jfrview_bg\.wasm$/ }),
      new webpack.DefinePlugin({
        __BUILD_TIME__: JSON.stringify(new Date().toISOString()),
      }),
    ],
    module: {
      rules: [
        {
          test: /\.css$/,
          use: [
            'style-loader',
            'css-loader',
            {
              loader: 'postcss-loader',
              options: {
                postcssOptions: {
                  config: path.resolve(__dirname, 'postcss.config.cjs'),
                },
              },
            },
          ],
        },
      ],
    },
  });
};
