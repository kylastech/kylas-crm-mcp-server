const path = require('path');
const fs = require('fs');
const os = require('os');
const { pathToFileURL } = require('url');

// Thin bridge: Claude Desktop (stdio MCP) <-> the hosted Kylas MCP server
// (streamable HTTP) via the bundled `mcp-remote`. It runs on Claude Desktop's own
// bundled Node runtime, so the user installs nothing. On first connect mcp-remote
// drives the OAuth browser login.
//
// mcp-remote is loaded IN-PROCESS via dynamic import() — not as a child process.
// Claude Desktop's runtime is Electron-run-as-Node; re-spawning it as a nested
// Electron-as-node child crashes, and forwarding stdio through an extra hop breaks
// the long-lived MCP pipe. Running in this same process is the reliable path.
//
// mcp-remote and its full dependency tree are pre-bundled by esbuild into a single
// self-contained `proxy.bundled.mjs` next to this launcher (ships ~4 files, not ~660).

const MCP_URL = (process.env.KYLAS_MCP_URL || '').trim() || 'http://localhost:8000/mcp';

const LOG_FILE = path.join(os.homedir(), 'kylas-mcp-launcher.log');
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  try { fs.appendFileSync(LOG_FILE, line); } catch {}
  try { process.stderr.write(`Kylas MCP: ${msg}\n`); } catch {}
}

const mcpRemoteEntry = path.join(__dirname, 'proxy.bundled.mjs');
if (!fs.existsSync(mcpRemoteEntry)) {
  log(`FATAL: bundled mcp-remote not found at ${mcpRemoteEntry}`);
  process.exit(1);
}

// On Linux, mcp-remote opens the OAuth browser via `xdg-open`, which needs DISPLAY
// (and usually XAUTHORITY). Claude Desktop spawns extensions without them, so the
// browser launch silently no-ops. Inject sensible defaults for the current session.
// No-op on macOS/Windows (their `open`/`start` don't use DISPLAY).
function injectGraphicalEnv() {
  if (process.platform !== 'linux') return;
  if (!process.env.DISPLAY) process.env.DISPLAY = ':0';
  if (!process.env.XAUTHORITY) {
    const uid = typeof process.getuid === 'function' ? process.getuid() : 1000;
    for (const p of [`/run/user/${uid}/gdm/Xauthority`, path.join(os.homedir(), '.Xauthority')]) {
      try { if (fs.existsSync(p)) { process.env.XAUTHORITY = p; break; } } catch { /* ignore */ }
    }
  }
}

log('================ launcher start ================');
log(`platform=${process.platform} node=${process.version} pid=${process.pid} server=${MCP_URL}`);
injectGraphicalEnv();

// Keep the event loop alive. Under Claude Desktop's Electron runtime, process.stdin
// can emit a spurious 'end'/'close' right after the handshake; without this the
// process would exit before mcp-remote establishes the MCP transport.
const keepAlive = setInterval(() => {}, 1 << 30);

// Clean shutdown so we never leave a stray process behind: exit on a termination
// signal, or when our parent (Claude Desktop) dies and we're reparented to init
// (ppid becomes 1 on Linux / launchd on macOS).
let shuttingDown = false;
function shutdown(why) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`shutting down: ${why}`);
  clearInterval(keepAlive);
  clearInterval(parentWatch);
  process.exit(0);
}
const initialPpid = (() => { try { return process.ppid; } catch { return 0; } })();
const parentWatch = setInterval(() => {
  try { if (initialPpid > 1 && process.ppid === 1) shutdown('parent exited (orphaned)'); } catch { /* ignore */ }
}, 3000);
if (typeof parentWatch.unref === 'function') parentWatch.unref();
for (const sig of ['SIGTERM', 'SIGINT', 'SIGHUP']) {
  try { process.on(sig, () => shutdown(sig)); } catch { /* signal unsupported */ }
}

// Run mcp-remote in-process. It reads its config from process.argv.slice(2), so set
// argv as if it had been invoked as `node proxy.bundled.mjs <MCP_URL> [--header ...]`.
// If the user configured an API key, forward it as `x-api-key` so the server skips
// OAuth (the server allows api-key requests through). If no key is set, mcp-remote
// runs the normal OAuth browser login on first connect.
const apiKey = (process.env.KYLAS_API_KEY || '').trim();
const proxyArgs = [MCP_URL];
if (apiKey) {
  proxyArgs.push('--header', `x-api-key: ${apiKey}`);
  log('auth mode: x-api-key header (no browser login)');
} else {
  log('auth mode: OAuth (browser login on first connect)');
}
process.argv = [process.execPath, mcpRemoteEntry, ...proxyArgs];
log('loading mcp-remote in-process');
import(pathToFileURL(mcpRemoteEntry).href)
  .then(() => log('mcp-remote running in-process'))
  .catch((err) => {
    log(`FATAL: mcp-remote failed to start: ${err && err.stack ? err.stack : err}`);
    clearInterval(keepAlive);
    process.exit(1);
  });
