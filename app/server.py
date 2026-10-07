"""The FastMCP server instance, its lifespan and OAuth wiring."""

from contextlib import asynccontextmanager

from fastmcp.server import FastMCP

from app.config import (
    BASE_URL,
    KYLAS_CLIENT_ID,
    KYLAS_CLIENT_SECRET,
    KYLAS_TOKEN_CACHE_TTL_SECONDS,
    MCP_SERVER_BASE_URL,
    SYSTEM_INSTRUCTIONS,
    logger,
)

# Populated when OAuth is configured; referenced by the lifespan for clean shutdown.
_kylas_token_verifier = None  # type: ignore[var-annotated]
# ---------------------------------------------------------------------------
# MCP Server
#
# All the old per-entity instruction blocks (DEAL_/COMPANY_/MEETING_/
# CALL_LOG_/QUOTATION_SYSTEM_INSTRUCTIONS, and DIAGNOSIS_AND_REPORTING_
# INSTRUCTIONS' entity-specific diagnosis tables) were deleted from here —
# not archived elsewhere as dead code, actually deleted — once their content
# was verified to already live in registry/*.yaml's usage_notes (verbatim,
# real request-shape guidance) or, for the deal/lead/task diagnosis tables
# specifically, moved into deal.yaml/lead.yaml/task.yaml's own .get
# usage_notes. The only pieces that were genuinely generic (not
# entity-specific) — the DEFAULT DATE RANGE rule and the REPORT FORMATTING
# section — were folded into SYSTEM_INSTRUCTIONS above instead, since those
# apply across every bucket, not to one. SYSTEM_INSTRUCTIONS is now the
# server's one and only instructions text — see mcp = FastMCP(...) below.
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _app_lifespan(app: FastMCP):
    """Startup/shutdown. Entity labels are fetched live per-request (see
    _fetch_entity_labels) — nothing to warm or refresh here."""
    try:
        yield {}
    finally:
        # Release the pooled HTTP client held by the token verifier.
        if _kylas_token_verifier is not None:
            try:
                await _kylas_token_verifier.aclose()
            except Exception as _exc:  # shutdown must not raise
                logger.debug("Error closing token verifier: %s", _exc)


# OAuth (multi-user) is wired in app/oauth.py. It returns the provider and the
# verifier (closed by the lifespan); both are None when OAuth isn't configured.
from app.oauth import create_kylas_auth

auth_provider, _kylas_token_verifier = create_kylas_auth(
    base_url=BASE_URL,
    client_id=KYLAS_CLIENT_ID,
    client_secret=KYLAS_CLIENT_SECRET,
    mcp_server_base_url=MCP_SERVER_BASE_URL,
    token_cache_ttl_seconds=KYLAS_TOKEN_CACHE_TTL_SECONDS,
)


mcp = FastMCP("Kylas CRM", instructions=SYSTEM_INSTRUCTIONS, lifespan=_app_lifespan, auth=auth_provider)
