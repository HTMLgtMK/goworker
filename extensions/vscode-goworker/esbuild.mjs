import { build } from 'esbuild';

const shared = {
  bundle: true,
  sourcemap: true,
  minify: false,
  platform: 'browser',
  target: 'es2022',
  logLevel: 'info',
};

await Promise.all([
  build({
    ...shared,
    platform: 'node',
    external: ['vscode'],
    entryPoints: ['src/extension.ts'],
    outfile: 'dist/extension.js',
    format: 'cjs',
  }),
  build({
    ...shared,
    entryPoints: ['webview/src/main.ts'],
    outfile: 'dist/webview.js',
    format: 'iife',
  }),
  build({
    entryPoints: ['webview/src/styles.css'],
    outfile: 'dist/styles.css',
    bundle: true,
    minify: false,
    logLevel: 'info',
  }),
]);
