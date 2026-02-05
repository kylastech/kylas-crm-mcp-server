# Kylas CRM MCP Server (Lead Only)

Model Context Protocol server for **Kylas CRM** lead operations. Use it from Cursor, Claude Desktop, or any MCP client to create leads, search and filter leads, and look up users, products, and pipelines.

<!-- mcp-name: io.github.kylastech/kylas-crm -->

## Features

- **get_lead_field_instructions** – Get lead schema (standard + custom fields, picklist IDs)
- **create_lead** – Create a lead with dynamic fields from user context
- **search_leads** – Search/filter leads by multiple criteria
- **lookup_users** – Resolve user names to IDs (for owner, created by, etc.)
- **lookup_products** – Resolve product names to IDs
- **lookup_pipelines** / **get_pipeline_stages** – Resolve pipeline and stage for open/closed/won leads
- **search_idle_leads** – Find leads with no activity for N days

## Requirements

- Python 3.10+
- [Kylas](https://kylas.io) account and API key

## Installation

```bash
pip install -e .
# or from PyPI (after publish): pip install kylas-crm-mcp
```

## Configuration

Set environment variables (or use a `.env` file):

| Variable         | Required | Description                          |
|------------------|----------|--------------------------------------|
| `KYLAS_API_KEY`  | Yes      | Your Kylas API key                   |
| `KYLAS_BASE_URL` | No       | API base URL (default: https://api.kylas.io/v1) |

## Running the server

The server uses **stdio** transport (default for MCP). Run:

```bash
python -m kylas_crm_mcp
# or: python main.py  (when developing from repo root)
```

MCP clients (e.g. Cursor) typically start this process and communicate via stdin/stdout.

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
