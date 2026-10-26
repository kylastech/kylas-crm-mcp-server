"""
Kylas CRM MCP Server — Generic Registry Architecture

Model Context Protocol server for Kylas CRM operations (lead, contact,
deal, task, company, meeting, call_log, quotation). There is no
per-entity tool surface any more — every operation is reached through
exactly 5 advertised tools; everything else is internal Python that those
5 tools dispatch to. See SYSTEM_INSTRUCTIONS below (the actual text served
to a connecting MCP client as its `instructions`) for the full, current
contract — this docstring is a short map for a human reading the source,
not duplicated guidance for the client.

STANDALONE TOOLS (2 — deliberately outside the registry flow):
- get_entity_labels() — MANDATORY, call first, every session (this tenant
  may have renamed CRM entities; every other tool only takes the standard
  type, never the tenant's custom display name). Fetched live, per call,
  never cached server-side — see _fetch_entity_labels's docstring.
- get_current_user() — MANDATORY, call first, every session, alongside
  get_entity_labels(). Returns the calling user's IANA timezone and id.
  Every timestamp Kylas returns is UTC; the timezone from this call is what
  turns it into something the user actually recognises. Call it ONCE per
  session and reuse the result — it is not cached server-side.

THE GENERIC REGISTRY FLOW (3 tools):
- list_tool(bucket?, intent?) — find the id of the operation you need.
- build_payload(id, fields?) — get that one id's real method/path/
  usage_notes/schema/example, with this tenant's live custom
  fields/picklist options folded in where relevant.
- execute_request(id, payload) — actually run it, via this repo's
  existing, already-tested *_logic implementation for that id — never a
  reimplemented HTTP call.

Every CRUD operation (get/search/search_by_term/search_idle/create/update)
across every bucket goes through these 3. So does every operation that used
to be its own standalone tool — user/product/pipeline lookups, per-entity
relation lookups (meeting participants, task associations, call logs by
entity), datetime conversion (datetime.parse_to_utc), and the
tasks-with-any-relation search (task.search_any_relation) — each reached as
a registry id, dispatched via _REGISTRY_ID_TO_META_TOOL /
_META_LOOKUP_ROUTERS to the same real Python function that operation
always used, not a new implementation.

Registry source of truth: registry/*.yaml (what each operation is, how to
use it) + this file's runtime schemas (the authoritative request shape) —
see registry/_meta.yaml's header comment for why those two are deliberately
kept separate.
"""

import os

from app.config import logger
from app.server import mcp
import app.entities  # noqa: F401  (registers entity tools on mcp)
import app.tools  # noqa: F401  (registers registry tools and finalizes the tool surface)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def run() -> None:
    """Entry point for console script (e.g. kylas-crm-mcp)."""
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))

    logger.info("Starting Kylas CRM MCP Server (Lead + Contact + Deal + Task + Company + Meeting support)...")

    if transport == "streamable-http":
        logger.info(f"Running Streamable HTTP on {host}:{port}/mcp")
        mcp.run(transport="streamable-http", host=host, port=port, stateless_http=True)

    elif transport == "sse":
        logger.info(f"Running SSE on {host}:{port}/sse")
        mcp.run(transport="sse", host=host, port=port)
    else:
        logger.info("Running stdio transport")
        mcp.run()


if __name__ == "__main__":
    run()
