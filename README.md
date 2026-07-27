# Kylas CRM MCP Server (Lead + Deal)

Model Context Protocol server for **Kylas CRM** lead and deal operations. Use it from Cursor, Claude Desktop, or any MCP client to manage leads and deals, search and filter by criteria, and look up users, products, and pipelines.

<!-- mcp-name: io.github.kylastech/kylas-crm -->

## Features

### Lead Operations
- **get_lead_field_instructions** – Get lead schema (standard + custom fields, picklist IDs)
- **create_lead** – Create a lead with dynamic fields from user context
- **update_lead** – Update an existing lead
- **get_lead** – Retrieve full lead details
- **search_leads** – Search/filter leads by multiple criteria
- **search_leads_by_term** – Multi-field term search across leads
- **search_idle_leads** – Find leads with no activity for N days

### Deal Operations
- **get_deal_field_instructions** – Get deal schema (standard + custom fields, picklist IDs)
- **create_deal** – Create a deal with dynamic fields from user context
- **update_deal** – Update an existing deal
- **get_deal** – Retrieve full deal details
- **search_deals** – Search/filter deals by multiple criteria
- **search_deals_by_term** – Multi-field term search across deals
- **search_idle_deals** – Find deals with no activity for N days

### Shared Utilities
- **lookup_users** – Resolve user names to IDs (for owner, created by, etc.)
- **lookup_products** – Resolve product names to IDs
- **lookup_pipelines** – Resolve pipelines (supports both LEAD and DEAL entity types)
- **get_pipeline_stages** – Get stages for a pipeline
- **get_pipeline_details** – Get pipeline details (stages + closing reasons)
- **get_current_user** – Get current user profile and timezone
- **parse_datetime_to_utc_iso_tool** – Convert local datetime to UTC ISO

## Requirements

- Python 3.10+
- [Kylas](https://kylas.io) account and API key

## Installation

```bash
pip install -e .
# or from PyPI (after publish): pip install kylas-crm-mcp-server
```

## Configuration

Set environment variables (or use a `.env` file):

| Variable         | Required            | Description                          |
|------------------|---------------------|--------------------------------------|
| `KYLAS_API_KEY`  | No                  | Your Kylas API key (fallback / single-user) |
| `KYLAS_BASE_URL` | No                  | API base URL (default: https://api.kylas.io/v1) |
| `KYLAS_CLIENT_ID`| No                  | Kylas Marketplace App Client ID (required for OAuth) |
| `KYLAS_CLIENT_SECRET` | No             | Kylas Marketplace App Client Secret (required for OAuth) |
| `MCP_SERVER_BASE_URL` | No             | Public URL of this MCP server (required for OAuth redirects) |

## OAuth 2.1 Authentication

The server supports standard OAuth 2.1 authorization code flow with PKCE via FastMCP's `OAuthProxy`. This allows multiple users to connect securely, and Kylas will restrict API calls to their own user permissions.

### Prerequisites

1. Register a **Marketplace App** in the Kylas Developer Portal.
2. Set the redirect URI to `<MCP_SERVER_BASE_URL>/auth/callback`.
3. Provide the registered Client ID and Client Secret in `KYLAS_CLIENT_ID` and `KYLAS_CLIENT_SECRET`.
4. Ensure the server is reachable under HTTPS (or HTTP for local dev) by configuring `MCP_SERVER_BASE_URL`.

### Testing the native connector locally (Cloudflare tunnel)

Claude's **native remote connector** requires a public **HTTPS** URL, and the OAuth
discovery metadata must be served *without* a browser interstitial. ngrok's free tier
injects a "You are about to visit…" page for browser-context requests, which makes
Claude receive HTML instead of JSON and fail client registration
(*"Couldn't register with … sign-in service"*). A **Cloudflare quick tunnel** gives a
free HTTPS URL with **no interstitial**.

> Testing only. In production, deploy the server to a real domain (e.g.
> `https://mcp.kylas.io`) and set `MCP_SERVER_BASE_URL` to it — no tunnel needed.

```bash
# One-time: install cloudflared (user-local, no sudo)
mkdir -p ~/.local/bin
curl -fsSL -o ~/.local/bin/cloudflared \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
chmod +x ~/.local/bin/cloudflared

# 1. Start the MCP server
docker compose up -d kylas-mcp

# 2. Start the tunnel (leave running) — note the printed https://<random>.trycloudflare.com URL
~/.local/bin/cloudflared tunnel --url http://localhost:8000 --no-autoupdate

# 3. In a second terminal, point the server at that tunnel URL
export CF="https://bingo-qty-containers-pharmaceutical.trycloudflare.com/"      # paste the URL from step 2
MCP_SERVER_BASE_URL="$CF" docker compose up -d --force-recreate kylas-mcp

# 4. Register the redirect URI in the Kylas OAuth app:  $CF/auth/callback
# 5. Add the connector in Claude (Settings → Connectors → Add custom connector):  $CF/mcp

# Stop everything when done:
docker compose stop kylas-mcp && pkill -x cloudflared
```

The `trycloudflare.com` URL changes on each tunnel restart, so repeat steps 2–4 (re-register
the callback) if you restart it. For a stable URL, use a named Cloudflare tunnel or your own domain.

## Quick Start

### 1. Setup Environment

```bash
# Navigate to the server directory
cd /Users/admin/Documents/kylas_repo/kylas-crm-mcp-server

# Copy environment template
cp .env.example .env

# Edit .env and add your Kylas API key
# KYLAS_API_KEY=your_api_key_here
```

### 2. Activate Virtual Environment

```bash
source venv/bin/activate
```

### 3. Run the Server

```bash
python main.py
```

### 4. Configure in MCP Client

See **[MCP_CONFIG.md](MCP_CONFIG.md)** for detailed setup instructions for:
- Claude Desktop (macOS, Windows, Linux)
- Cursor
- Other MCP clients

## Running the server

The server uses **stdio** transport (default for MCP). Run:

```bash
python main.py  # When developing from repo root (after activating venv)
```

MCP clients (e.g. Cursor, Claude Desktop) typically start this process automatically and communicate via stdin/stdout.

## Docker

```bash
docker build -t kylas-crm-mcp .
docker run -e KYLAS_API_KEY=your_key -i kylas-crm-mcp
```

## Development

```bash
pip install -e ".[dev]"
pytest
```

## Publishing to the MCP Marketplace

To publish this server to the **official MCP Registry** (so it appears in Cursor and other MCP clients):

1. Publish the package to [PyPI](https://pypi.org) (the registry verifies ownership via your README).
2. Install [mcp-publisher](https://modelcontextprotocol.io/registry/quickstart) and run `mcp-publisher login github`, then `mcp-publisher publish`.

See **[PUBLISHING.md](PUBLISHING.md)** for the full step-by-step guide.

## License

See repository for license information.
