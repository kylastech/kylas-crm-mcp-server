# Packaging & Distribution Guide

How to build and distribute the Kylas CRM Claude Desktop extension.

This extension is a thin bridge: Claude Desktop talks stdio MCP to `launcher.js`,
which loads a pre-bundled `mcp-remote` in-process and proxies to the hosted Kylas
MCP server over streamable HTTP. The client installs nothing — Claude Desktop
runs it on its own bundled Node runtime.

## TL;DR

```bash
npm install        # first time only — pulls mcp-remote + esbuild (dev)
npm run pack       # builds proxy.bundled.mjs, then packs the .mcpb
cp kylas-crm-mcp-server.mcpb kylas-crm-mcp-server.dxt   # older-Desktop alias
```

Then ship the three artifacts described under [Distribution](#distribution).

## Why we bundle (don't skip this)

`mcp-remote` plus its dependency tree is ~660 files of nested `node_modules`.
A bundle that large triggers a Claude Desktop **on Windows** regression where the
`.mcpb` install dialog silently closes — no error, no log
([mcpb#281](https://github.com/modelcontextprotocol/mcpb/issues/281)). The root
cause is in Desktop's `.mcpb` handler, not in our bundle, so a "clean repack"
does not help.

The fix we control: esbuild collapses `mcp-remote` + all deps into a **single
self-contained `proxy.bundled.mjs`**, dropping the bundle from ~660 files to 4.
`node_modules` is **not** shipped.

| | files | package size |
|---|---|---|
| old (node_modules shipped) | ~663 | 1.5 MB |
| current (bundled) | 4 | ~742 KB |

## Build steps (what `npm run pack` does)

### 1. Build the bundle

```bash
npm run build
```

Runs:

```
esbuild node_modules/mcp-remote/dist/proxy.js \
  --bundle --platform=node --format=esm --target=node18 \
  --outfile=proxy.bundled.mjs \
  --banner:js="import{createRequire as __cr}from'module';const require=__cr(import.meta.url);"
```

Two non-obvious details, both load-bearing:

- **`.mjs` extension** — this package is CommonJS (no `"type":"module"`).
  A `.js` ESM file would be parsed as CJS and fail with
  *"Cannot use import statement outside a module."* `.mjs` forces ESM.
- **`createRequire` banner** — bundled CJS deps (express, body-parser…) call
  `require()` for Node built-ins. ESM output has no `require`, so without the
  banner you get *"Dynamic require of 'path' is not supported."*

### 2. Pack the `.mcpb`

```bash
npx -y @anthropic-ai/mcpb pack
```

`.mcpbignore` excludes `node_modules`, `assets/`, Python sources, and build
artifacts. A correct pack reports exactly **4 files**:

```
icon.png
launcher.js
manifest.json
proxy.bundled.mjs
```

If you see hundreds of files, `node_modules` leaked into the archive — check
`.mcpbignore`.

### 3. Make the `.dxt` alias

```bash
cp kylas-crm-mcp-server.mcpb kylas-crm-mcp-server.dxt
```

Same bytes. The extension format was renamed `.dxt` → `.mcpb`; older Windows
Desktop builds only recognize `.dxt`.

### 4. (Windows) make the unpacked zip

For the **Install Unpacked Extension** fallback:

```bash
rm -rf dist-unpacked kylas-crm-unpacked.zip
mkdir dist-unpacked && unzip -q kylas-crm-mcp-server.mcpb -d dist-unpacked
( cd dist-unpacked && zip -qr ../kylas-crm-unpacked.zip . )
```

## Verify before shipping

Always smoke-test from a **clean extraction** (this is exactly how Desktop runs
it — catches missing-file and module-resolution bugs the working tree hides):

```bash
T=$(mktemp -d); unzip -q kylas-crm-mcp-server.mcpb -d "$T"
KYLAS_API_KEY="<a-valid-key>" \
  { printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"1.0"}}}'; sleep 12; } \
  | node "$T/launcher.js"
rm -rf "$T"
```

Expected: a JSON-RPC result containing
`"serverInfo":{"name":"Kylas CRM","version":"3.2.4"}`.
Use a Node **≥18** runtime (Desktop bundles Node 22). Node 16 will fail.

## Distribution

| Platform | Ship | Install path |
|---|---|---|
| **Windows** | `kylas-crm-unpacked.zip` | Extract, then Settings → Extensions → Advanced → **Install Unpacked Extension** → pick the extracted folder. Avoids the `.mcpb` dialog bug. |
| **macOS / Linux** | `kylas-crm-mcp-server.mcpb` (and `.dxt`) | Settings → Extensions → install the file, or double-click. |

On all platforms the install dialog asks for the **Kylas API Key** (stored in
encrypted extension config, never in the bundle).

> When Anthropic ships the [mcpb#281](https://github.com/modelcontextprotocol/mcpb/issues/281)
> fix, the `.mcpb` will install normally on Windows too and the unpacked zip
> becomes unnecessary — no rebuild required.

## Bumping the version

Keep these two in sync, then re-pack:

- `manifest.json` → `version`
- `package.json` → `version`

## Files reference

| File | Shipped? | Purpose |
|---|---|---|
| `launcher.js` | yes | Entry point; loads the bundle in-process |
| `proxy.bundled.mjs` | yes | esbuild output: mcp-remote + all deps, one file |
| `manifest.json` | yes | Extension metadata + API-key config |
| `icon.png` | yes | Extension icon |
| `node_modules/` | **no** | Build-time only; bundled into `proxy.bundled.mjs` |
| `assets/`, Python sources | **no** | Not part of the Node extension |
