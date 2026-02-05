# Publishing Kylas CRM MCP Server to the Marketplace

This guide covers publishing to the **official MCP Registry** ([registry.modelcontextprotocol.io](https://registry.modelcontextprotocol.io)), so your server appears in Cursor and other MCP clients that use the registry.

---

## Quick steps (checklist)

| # | What to do |
|---|------------|
| 0 | Have a **PyPI account** and a **GitHub account** (with access to the **kylastech** org). |
| 1 | **Publish to PyPI**: `python -m build` → `twine upload dist/*` (use PyPI API token). |
| 2 | **Install mcp-publisher**: e.g. `brew install mcp-publisher` or download from [releases](https://github.com/modelcontextprotocol/registry/releases). |
| 3 | **Log in to the registry**: `mcp-publisher login github` (browser + device code). |
| 4 | **Publish to MCP**: From project root, run `mcp-publisher publish`. |
| 5 | **Verify**: `curl "https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.kylastech/kylas-crm"` or search on [modelcontextprotocol.io](https://modelcontextprotocol.io). |

---

## Prerequisites

- **PyPI account** – [Create one](https://pypi.org/account/register/) if needed. The registry only stores metadata; the actual package is published to PyPI.
- **GitHub account** – Used to authenticate with the MCP Registry. Your server uses the **Kylas Tech organisation** namespace `io.github.kylastech/kylas-crm`; the account you use for `mcp-publisher login github` must have permission to publish on behalf of the [kylastech](https://github.com/kylastech) organisation (e.g. org member with appropriate rights).
- **Repository** – Code in the organisation repo. The default `repository.url` in `server.json` is `https://github.com/kylastech/kylas-crm-mcp-server`; change it if your repo name or path differs.

## What’s Already Done

- **server.json** – Configured with `name`, `packages` (PyPI), env vars, and transport.
- **README** – Contains the required ownership line for PyPI verification:
  ```html
  <!-- mcp-name: io.github.kylastech/kylas-crm -->
  ```
- **pyproject.toml** – Package name `kylas-crm-mcp`, version, entry point.

## Step 1: Publish the package to PyPI

The MCP Registry verifies ownership by checking that your PyPI package’s long description (README) includes `mcp-name: io.github.kylastech/kylas-crm`. So you must publish to PyPI first.

From the project root:

```bash
# Install build tools if needed
uv pip install build twine

# Build the package
python -m build

# Upload to PyPI (use token from https://pypi.org/manage/account/token/)
uv run twine upload dist/*
# Or: python -m twine upload dist/*
```

First time: create an API token at [PyPI → Account settings → API tokens](https://pypi.org/manage/account/token/), then use it when `twine` asks for username/password (username: `__token__`, password: the token).

Confirm the package at [https://pypi.org/project/kylas-crm-mcp/](https://pypi.org/project/kylas-crm-mcp/) and that the project description (from README) contains the `mcp-name` line.

## Step 2: Install the MCP Publisher CLI

**macOS/Linux (Homebrew):**
```bash
brew install mcp-publisher
```

**Or download the binary:**
```bash
curl -L "https://github.com/modelcontextprotocol/registry/releases/latest/download/mcp-publisher_$(uname -s | tr '[:upper:]' '[:lower:]')_$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/').tar.gz" | tar xz
sudo mv mcp-publisher /usr/local/bin/
```

**Verify:**
```bash
mcp-publisher --help
```

## Step 3: Authenticate with the MCP Registry

Your server uses the **organisation** namespace `io.github.kylastech/`, so you must use GitHub authentication:

```bash
mcp-publisher login github
```

Follow the prompts (browser + device code). Log in with a GitHub account that has permission to publish for the **kylastech** organisation (e.g. an org owner or member with publish rights).

## Step 4: Publish to the MCP Registry

From the project root (where `server.json` lives):

```bash
mcp-publisher publish
```

You should see something like:
```text
Publishing to https://registry.modelcontextprotocol.io...
✓ Successfully published
✓ Server io.github.kylastech/kylas-crm version 1.0.0
```

## Step 5: Verify

Search the registry:

```bash
curl "https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.kylastech/kylas-crm"
```

Or open [https://modelcontextprotocol.io](https://modelcontextprotocol.io) and use the registry/search if available.

## Updating a version

1. Bump `version` in `pyproject.toml` and in `server.json` (top-level and inside `packages[].version`).
2. Publish the new version to PyPI (Step 1).
3. Run `mcp-publisher publish` again (Step 4). No need to log in again unless the session expired.

## Troubleshooting

| Issue | What to do |
|-------|------------|
| **Package validation failed** | Ensure the PyPI project’s long description (from README) contains exactly `mcp-name: io.github.kylastech/kylas-crm` (can be in an HTML comment). Re-upload the package if you fixed the README. |
| **Invalid or expired Registry JWT** | Run `mcp-publisher login github` again. |
| **You do not have permission to publish this server** | The server `name` must start with `io.github.kylastech/`. Log in with a GitHub account that has permission to publish for the **kylastech** organisation (e.g. org owner or member with publish rights). |

## Optional: Automate with GitHub Actions

To publish to the MCP Registry on every release, see the official guide:  
[Registry – GitHub Actions](https://modelcontextprotocol.io/registry/github-actions).

## Other places users find MCP servers

- **Cursor** – Users add MCP servers via **Cursor Settings → Features → MCP → Add New MCP Server** (command or URL). Once your server is in the official registry, Cursor may surface it from there depending on their integration.
- **Third-party lists** – Sites like [cursormcp.net](https://cursormcp.net) and [cursormcp.com](https://cursormcp.com) aggregate MCP servers; they may pick up your server from the official registry or you can submit it if they allow.

Publishing to the **official MCP Registry** (steps above) is the standard way to make your server discoverable in the marketplace.
