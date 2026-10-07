// Exercise only the installed tarball, outside the repository's module tree.
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync, spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import assert from 'node:assert/strict';

const root = fileURLToPath(new URL('../', import.meta.url));
const temp = mkdtempSync(join(tmpdir(), 'anvilmq-package-'));
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
function run(command, args, cwd = temp) {
  return execFileSync(command === npm ? process.execPath : command, command === npm ? [process.env.npm_execpath, ...args] : args, { cwd, stdio: 'pipe', encoding: 'utf8', timeout: 120000 });
}
async function port() {
  const s = createServer();
  await new Promise(r => s.listen(0, '127.0.0.1', r));
  const p = s.address().port;
  await new Promise(r => s.close(r));
  return p;
}
let broker;
let logs = '';
try {
  const packed = JSON.parse(run(npm, ['pack', '--json', '--pack-destination', temp], root))[0];
  assert(packed.files.some(f => f.path === 'proto/queue.proto'));
  assert(packed.files.some(f => f.path === 'dist/index.d.ts'));
  assert(!packed.files.some(f => /^(src|test|scripts)\//.test(f.path)));
  writeFileSync(join(temp, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
  console.log(run(npm, ['install', '--ignore-scripts', '--no-audit', '--no-fund', `./${packed.filename}`]));
  const installed = JSON.parse(readFileSync(join(temp, 'node_modules/@anvilmq/client/package.json')));
  assert.equal(installed.name, '@anvilmq/client');
  writeFileSync(join(temp, 'types.mts'), `import { Queue, Worker } from '@anvilmq/client';
const q = new Queue<{ value: number }>('types');
q.add({ value: 1 });
new Worker<{ value: number }>('types', async job => { const n: number = job.data.value; });
// @ts-expect-error payload must match the queue's generic
q.add({ value: 'wrong' });
`);
  run(process.execPath, [join(root, 'node_modules/typescript/bin/tsc'), '--noEmit', '--strict', '--skipLibCheck', '--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', 'types.mts']);
  const grpcPort = await port();
  const httpPort = await port();
  broker = spawn(process.env.ANVILMQ_TEST_BINARY ?? resolve(root, '../target/debug', process.platform === 'win32' ? 'rusty-queue.exe' : 'rusty-queue'), [], {
    cwd: temp, env: { ...process.env, ANVILMQ_DB_PATH: join(temp, 'test.db'), ANVILMQ_ADDR: `127.0.0.1:${grpcPort}`, ANVILMQ_HTTP_ADDR: `127.0.0.1:${httpPort}` }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  broker.on('error', e => { logs += String(e); });
  broker.stdout.on('data', x => { logs += x; });
  broker.stderr.on('data', x => { logs += x; });
  let ready = false;
  for (let i = 0; i < 100; i++) {
    try { ready = (await fetch(`http://127.0.0.1:${httpPort}/readyz`)).ok; } catch {}
    if (ready) break;
    await sleep(100);
  }
  assert(ready, `broker failed to become ready: ${logs}`);
  writeFileSync(join(temp, 'consumer.mjs'), `import assert from 'node:assert/strict';
import { Queue, Worker } from '@anvilmq/client';
const address = '127.0.0.1:${grpcPort}';
const name = 'package-' + process.argv[2];
const queue = new Queue(name, { address });
let completed;
const done = new Promise(r => { completed = r; });
const worker = new Worker(name, async job => { assert.equal(job.data.value, 42); }, { address, onCompleted: completed });
const timeout = setTimeout(() => { console.error('completion timeout'); process.exit(1); }, 15000);
try { await queue.add({ value: 42 }); await done; console.log(name + ' completed'); }
finally { clearTimeout(timeout); await worker.close(); queue.close(); }
`);
  // Async children let this process continue servicing the broker's output pipes.
  for (const runtime of [process.execPath, 'bun']) {
    await new Promise((resolve, reject) => {
      const child = spawn(runtime, ['consumer.mjs', runtime === 'bun' ? 'bun' : 'node'], { cwd: temp, stdio: 'inherit' });
      child.on('error', reject);
      child.on('exit', code => code === 0 ? resolve() : reject(new Error(`${runtime} exited ${code}`)));
    });
  }
  console.log('Installed-package types, Node, and Bun checks passed.');
} finally {
  if (broker?.pid && broker.exitCode === null) {
    const exited = new Promise(r => broker.once('exit', r));
    broker.kill();
    await exited;
  }
  rmSync(temp, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
}
