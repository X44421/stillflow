import { spawn } from 'node:child_process';
import process from 'node:process';

const isWindows = process.platform === 'win32';
const npmCommand = isWindows ? 'npm.cmd' : 'npm';
const children = new Set();
let shuttingDown = false;

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function startProcess(label, command, args) {
  const child = spawn(command, args, {
    cwd: process.cwd(),
    env: process.env,
    stdio: 'inherit',
    detached: !isWindows,
  });

  children.add(child);
  child.once('error', (error) => {
    console.error(`[${label}] Failed to start: ${error.message}`);
    void shutdown(1);
  });
  child.once('exit', (code, signal) => {
    children.delete(child);
    if (shuttingDown) return;
    const reason = signal ? `signal ${signal}` : `code ${code ?? 1}`;
    console.error(`[${label}] Exited with ${reason}`);
    void shutdown(code === 0 ? 0 : 1);
  });

  return child;
}

async function waitForBackend(backend) {
  const deadline = Date.now() + 120_000;
  while (!shuttingDown && Date.now() < deadline) {
    if (backend.exitCode !== null) {
      throw new Error('Rust backend exited before becoming ready.');
    }
    try {
      const response = await fetch('http://127.0.0.1:8787/api/health', {
        signal: AbortSignal.timeout(1_000),
      });
      if (response.ok) return;
    } catch {
      // Cargo may still be compiling the backend.
    }
    await delay(300);
  }
  throw new Error('Rust backend did not become ready within 120 seconds.');
}

function waitForExit(child, timeout) {
  if (child.exitCode !== null) return Promise.resolve();
  return Promise.race([
    new Promise((resolve) => child.once('exit', resolve)),
    delay(timeout),
  ]);
}

async function stopProcess(child) {
  if (!child.pid || child.exitCode !== null) return;

  if (isWindows) {
    await new Promise((resolve) => {
      const killer = spawn(
        'taskkill',
        ['/pid', String(child.pid), '/T', '/F'],
        { stdio: 'ignore' }
      );
      killer.once('error', resolve);
      killer.once('exit', resolve);
    });
    return;
  }

  try {
    process.kill(-child.pid, 'SIGTERM');
  } catch {
    child.kill('SIGTERM');
  }
  await waitForExit(child, 3_000);
  if (child.exitCode === null) {
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch {
      child.kill('SIGKILL');
    }
  }
}

async function shutdown(exitCode) {
  if (shuttingDown) return;
  shuttingDown = true;
  await Promise.all([...children].map(stopProcess));
  process.exitCode = exitCode;
}

process.once('SIGINT', () => void shutdown(130));
process.once('SIGTERM', () => void shutdown(143));

async function main() {
  const backend = startProcess('backend', 'cargo', [
    'run',
    '--manifest-path',
    'backend/Cargo.toml',
  ]);

  try {
    await waitForBackend(backend);
  } catch (error) {
    if (!shuttingDown) {
      console.error(`[backend] ${error.message}`);
      await shutdown(1);
    }
    return;
  }

  if (shuttingDown) return;
  console.log('[dev] Backend ready. Starting Vite.');
  startProcess('frontend', npmCommand, ['run', 'dev:frontend']);
}

void main();
