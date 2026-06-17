const path = require('path');
const fs = require('fs');
const os = require('os');
const { pathToFileURL } = require('url');

// Thin bridge: connect Claude Desktop (stdio MCP) to the hosted Kylas MCP
// server (streamable HTTP) using the bundled `mcp-remote`. Claude Desktop runs
// this on its own bundled Node runtime, so the client installs nothing — no
// system Node, no uv, no Python. The user's API key comes from the extension
// config and is forwarded as the `x-api-key` header.
//
// IMPORTANT: we load mcp-remote IN-PROCESS via dynamic import() rather than
// spawning a child process. Claude Desktop's bundled runtime is Electron run
// as Node (process.execPath is the Electron binary); spawning it again as a
// nested Electron-as-node child crashes with SIGTRAP after ~8s. Importing the
// proxy into this same process avoids the nested spawn entirely.

const MCP_URL = 'https://sea-turtle-app-x4h8l.ondigitalocean.app/mcp';

const LOG_FILE = path.join(os.homedir(), 'kylas-mcp-launcher.log');
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}\n`;
  try { fs.appendFileSync(LOG_FILE, line); } catch {}
  try { process.stderr.write(`Kylas MCP: ${msg}\n`); } catch {}
}

log('================ launcher start ================');
log(`platform=${process.platform} node=${process.version} pid=${process.pid}`);

const apiKey = (process.env.KYLAS_API_KEY || '').trim();
if (!apiKey) {
  log('FATAL: KYLAS_API_KEY not provided. Set the API Key in the extension config.');
  process.exit(1);
}

// Resolve the bundled mcp-remote entry. mcp-remote and its full dependency
// tree are pre-bundled by esbuild into a single self-contained `proxy.bundled.js`
// next to this launcher, so the extension ships ~4 files instead of ~660. This
// dodges a Claude Desktop on Windows regression where the .mcpb install dialog
// silently closes on bundles with large/deep node_modules trees (mcpb#281).
const mcpRemoteEntry = path.join(__dirname, 'proxy.bundled.mjs');
if (!fs.existsSync(mcpRemoteEntry)) {
  log(`FATAL: bundled mcp-remote not found at ${mcpRemoteEntry}`);
  process.exit(1);
}

// proxy.js reads its config from process.argv.slice(2); set it up as if it had
// been invoked as `node proxy.js <url> --header "x-api-key: <key>"`.
process.argv = [
  process.execPath,
  mcpRemoteEntry,
  MCP_URL,
  '--header',
  `x-api-key: ${apiKey}`,
];

log(`loading mcp-remote in-process: ${MCP_URL} (header x-api-key: ***)`);

// mcp-remote's proxy.js is an ES module that self-executes on import, wiring
// this process's stdin/stdout to the remote server. Use a file:// URL so the
// import resolves on Windows as well as macOS/Linux.
import(pathToFileURL(mcpRemoteEntry).href).catch(err => {
  log(`FATAL: mcp-remote failed to start: ${err && err.stack ? err.stack : err}`);
  process.exit(1);
});
