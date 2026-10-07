import { mkdirSync, copyFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
execFileSync(process.execPath, ['node_modules/typescript/bin/tsc', '-p', 'tsconfig.build.json'], { cwd: root, stdio: 'inherit' });
mkdirSync(new URL('../proto/', import.meta.url), { recursive: true });
copyFileSync(new URL('../../proto/queue.proto', import.meta.url), new URL('../proto/queue.proto', import.meta.url));
