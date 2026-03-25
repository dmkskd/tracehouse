const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const webpack = require('webpack');

module.exports = (env) => {
  const isProduction = !!env.production;

  return {
    mode: isProduction ? 'production' : 'development',
    devtool: isProduction ? 'source-map' : 'eval-source-map',
    entry: './src/module.ts',
    output: {
      path: path.resolve(__dirname, '../../dist'),
      filename: 'module.js',
      chunkFilename: '[id].module.js',
      library: {
        type: 'amd',
      },
      publicPath: '/public/plugins/tracehouse-app/',
      uniqueName: 'tracehouse-app',
    },
    optimization: {
      // Preserve export names for Grafana plugin loading
      moduleIds: 'named',
    },
    externals: [
      'react',
      'react-dom',
      // Note: react-router-dom is NOT external - we shim it via alias (see resolve.alias)
      '@grafana/data',
      '@grafana/runtime',
      '@grafana/ui',
      function ({ request }, callback) {
        if (request && request.startsWith('lodash')) {
          return callback(undefined, request);
        }
        callback();
      },
    ],
    resolve: {
      extensions: ['.ts', '.tsx', '.js', '.jsx', '.css'],
      alias: {
        // Resolve workspace packages to their dist directories for bundling
        '@tracehouse/core': path.resolve(__dirname, '../../../packages/core/dist'),
        '@tracehouse/ui-shared': path.resolve(__dirname, '../../../packages/ui-shared/dist'),
        // Alias frontend src for direct page imports
        '@frontend': path.resolve(__dirname, '../../../frontend/src'),
        // Redirect frontend store/provider imports to our Grafana-compatible shims
        // When frontend pages import '../stores/connectionStore', resolve to our shim
        [path.resolve(__dirname, '../../../frontend/src/stores/connectionStore')]:
          path.resolve(__dirname, '../../src/stores/connectionStore'),
        // Redirect ClickHouseProvider to our ServiceProvider
        [path.resolve(__dirname, '../../../frontend/src/providers/ClickHouseProvider')]:
          path.resolve(__dirname, '../../src/ServiceProvider'),
        // Redirect useAppLocation hook to our Grafana-compatible version
        [path.resolve(__dirname, '../../../frontend/src/hooks/useAppLocation')]:
          path.resolve(__dirname, '../../src/hooks/useAppLocation.ts'),
        // Redirect useUrlState hook to our Grafana-compatible version (no react-router)
        [path.resolve(__dirname, '../../../frontend/src/hooks/useUrlState')]:
          path.resolve(__dirname, '../../src/hooks/useUrlState.ts'),
        // Stub react-router-dom so shared frontend components (Link, NavLink, etc.)
        // don't crash when rendered outside a Router context in Grafana
        'react-router-dom': path.resolve(__dirname, '../../src/stubs/react-router-dom.tsx'),
        // Redirect stores to use frontend stores directly (they work with our ServiceProvider)
        // Note: mergeStore and databaseStore are NOT aliased - they work as-is
      },
      // Stub out Node.js core modules that @clickhouse/client tries to use
      // We use GrafanaAdapter which doesn't need these
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
    // Handle node: URI scheme
    plugins: [
      new webpack.NormalModuleReplacementPlugin(
        /^node:/,
        (resource) => {
          // Replace node:os, node:path, etc. with empty modules
          resource.request = resource.request.replace(/^node:/, '');
        }
      ),
      // The jfrview wasm binary is inlined as base64 in the speedscope-widget bundle,
      // but webpack still tries to resolve the original .wasm import path. Ignore it.
      new webpack.IgnorePlugin({ resourceRegExp: /jfrview_bg\.wasm$/ }),
      new CopyWebpackPlugin({
        patterns: [
          { from: 'src/plugin.json', to: '.' },
          { from: 'README.md', to: '.', noErrorOnMissing: true },
        ],
      }),
      new webpack.DefinePlugin({
        __BUILD_TIME__: JSON.stringify(new Date().toISOString()),
      }),
      ...(isProduction ? [] : []),
    ],
    module: {
      rules: [
        {
          test: /\.tsx?$/,
          exclude: /node_modules/,
          use: {
            loader: 'swc-loader',
            options: {
              jsc: {
                parser: { syntax: 'typescript', tsx: true },
                transform: { react: { runtime: 'automatic' } },
                target: 'es2021',
              },
            },
          },
        },
        {
          test: /\.css$/,
          use: [
            'style-loader',
            'css-loader',
            {
              loader: 'postcss-loader',
              options: {
                postcssOptions: {
                  config: path.resolve(__dirname, '../../postcss.config.cjs'),
                },
              },
            },
          ],
        },
      ],
    },
  };
};
