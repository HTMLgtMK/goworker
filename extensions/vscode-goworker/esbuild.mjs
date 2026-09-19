import { build, context } from 'esbuild';

const watch = process.argv.includes('--watch');

const shared = {
  bundle: true,
  sourcemap: true,
  minify: false,
  platform: 'browser',
  target: 'es2022',
  logLevel: 'info',
};

const configs = [
  {
    ...shared,
    platform: 'node',
    external: ['vscode'],
    entryPoints: ['src/extension.ts'],
    outfile: 'dist/extension.js',
    format: 'cjs',
  },
  {
    ...shared,
    entryPoints: ['webview/src/main.ts'],
    outfile: 'dist/webview.js',
    format: 'iife',
  },
  {
    entryPoints: ['webview/src/styles.css'],
    outfile: 'dist/styles.css',
    bundle: true,
    minify: false,
    logLevel: 'info',
  },
];

if (watch) {
  const contexts = await Promise.all(configs.map((c) => context(c)));
  await Promise.all(contexts.map((c) => c.watch()));
} else {
  await Promise.all(configs.map((c) => build(c)));
}
