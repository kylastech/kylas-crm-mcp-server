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

| Variable         | Required | Description                          |
|------------------|----------|--------------------------------------|
| `KYLAS_API_KEY`  | Yes      | Your Kylas API key                   |
| `KYLAS_BASE_URL` | No       | API base URL (default: https://api.kylas.io/v1) |

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
