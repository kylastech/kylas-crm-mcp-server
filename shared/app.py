"""
The foundational/root module: environment config + logging (nothing else in
shared/ may depend on anything that depends on this file, else importing it
would cycle), and the FastMCP application built on top of it — its one
instructions text, its lifespan (OAuth token-verifier cleanup on shutdown),
OAuth wiring (kylas_oauth.py), and the `mcp` instance every tool/resource in
this server registers against.

Import `mcp` (and logger/config values) from here wherever a module needs
`@mcp.tool()` / `@mcp.resource()` / `mcp.get_context()` /
`mcp.local_provider`. Everything else in shared/ (including shared/meta.py)
depends on this file directly or transitively — that's why its own content
must stay dependency-free (stdlib + fastmcp + kylas_oauth only), and why it
can't itself absorb shared/meta.py's content the other way around.
"""

import logging
import os
from contextlib import asynccontextmanager
from importlib.metadata import version as _pkg_version, PackageNotFoundError

from dotenv import load_dotenv
from fastmcp.server import FastMCP

# ---------------------------------------------------------------------------
# Configuration & Logging
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("kylas-mcp")

BASE_URL = os.getenv("KYLAS_BASE_URL", "https://api.kylas.io/v1")
API_KEY = os.getenv("KYLAS_API_KEY")
KYLAS_CLIENT_ID = os.getenv("KYLAS_CLIENT_ID")
KYLAS_CLIENT_SECRET = os.getenv("KYLAS_CLIENT_SECRET")
MCP_SERVER_BASE_URL = os.getenv("MCP_SERVER_BASE_URL", "http://localhost:8000")
# Positive-verification cache TTL (seconds). FastMCP verifies the token on every request;
# a short cache avoids calling Kylas /users/me for every call. 0 disables the cache.
KYLAS_TOKEN_CACHE_TTL_SECONDS = float(os.getenv("KYLAS_TOKEN_CACHE_TTL", "30"))

try:
    SERVER_VERSION = _pkg_version("kylas-crm-mcp-server")
except PackageNotFoundError:
    SERVER_VERSION = "unknown"

# ---------------------------------------------------------------------------
# System Instructions: the actual text served to a connecting MCP client as
# its `instructions`. All the old per-entity instruction blocks (DEAL_/
# COMPANY_/MEETING_/CALL_LOG_/QUOTATION_SYSTEM_INSTRUCTIONS, and
# DIAGNOSIS_AND_REPORTING_INSTRUCTIONS' entity-specific diagnosis tables)
# were deleted from here — not archived elsewhere as dead code, actually
# deleted — once their content was verified to already live in
# registry/*.yaml's usage_notes (verbatim, real request-shape guidance) or,
# for the deal/lead/task diagnosis tables specifically, moved into
# deal.yaml/lead.yaml/task.yaml's own .get usage_notes. The only pieces that
# were genuinely generic (not entity-specific) — the DEFAULT DATE RANGE rule
# and the REPORT FORMATTING section — were folded into SYSTEM_INSTRUCTIONS
# below instead, since those apply across every bucket, not to one.
# SYSTEM_INSTRUCTIONS is now the server's one and only instructions text.
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server — Generic Registry Architecture

This server does not expose one dedicated tool per CRM operation. Every
operation (lead, contact, deal, task, company, meeting, call_log, quotation,
plus shared user/product/pipeline/datetime lookups) is reached through
exactly 3 generic tools, plus exactly 2 standalone tools that don't fit the
id-based flow: get_current_user and get_entity_labels (see MANDATORY FIRST
CALL, immediately below — read that before anything else in this document).
There is no per-entity instructions block loaded up front — everything you
need to use any operation correctly is either below (rules that apply
across every operation) or returned by the tools themselves, on demand,
scoped to the one operation you're actually performing.

## MANDATORY FIRST CALL — get_entity_labels()

Call get_entity_labels() FIRST, before any other tool, at the start of
EVERY session — before list_tool, before answering the user's actual
request, before anything else. This is not optional and not a one-time
setup step to skip on a hunch that "this tenant probably hasn't renamed
anything."

Why: this tenant may have renamed standard CRM entities to its own display
names (e.g. "Lid" instead of "Lead", "Deeeel" instead of "Deal"). Every
registry id, every bucket name, every field in a payload still uses the
STANDARD type (lead/contact/deal/task/company/meeting/call_log/quotation)
— never the tenant's custom name. get_entity_labels() returns the mapping
from this tenant's custom names to those standard types. Without calling it
first, a user asking for their "Lids" reads as a request for an entity type
that doesn't exist, when it's actually just "lead" under a different name.

## The 3-step flow

1. list_tool(bucket?, intent?)
   Find the id of the endpoint you need. Returns only a short
   id/bucket/intent/description row per match — never a schema. Call it with
   no arguments first if you don't know what's available yet; narrow with
   bucket/intent once you have a sense of what you're looking for.

2. build_payload(id, fields?)
   Get everything about the ONE endpoint you picked: its real HTTP method and
   path, usage_notes (the actual field-shape rules for that specific
   endpoint — read these, they are load-bearing, not decorative), a
   parameter schema, and a worked example. For endpoints whose real shape
   depends on this tenant's own custom fields/picklist options, this makes a
   genuine live call to fetch them and folds the real data into the schema —
   check the returned "fetched_live" / "live_fetch_error" fields to know
   whether that actually succeeded; if it didn't, the rest of the response
   (method/path/usage_notes/example) is still correct and usable, only the
   tenant-specific enrichment is missing. This tool never sees or validates
   the payload you go on to build from it.

   A few picklist fields on a real tenant are enormous — timezone alone is
   ~435 options. Their option lists are omitted by default and shown as
   "options_count" / "options_omitted" / "uses_internal_name" instead. Every
   other field, including every small picklist and every custom field, always
   comes back complete, so this costs you nothing on a normal request. The
   large ones, with both names each goes by (entities disagree — a lead has
   "companyIndustry", a company has "industry"):

     timezone, country / companyCountry, companyIndustry / industry,
     requirementCurrency, companyBusinessType / businessType

   If the user's request actually mentions one of those — "country India",
   "timezone IST" — pass it in the optional "fields" list to get its real
   options: build_payload("lead.create", fields=["country"]). Otherwise omit
   the parameter. If you only realise later that you need one, just call
   build_payload again naming it; that is always cheaper and always correct
   compared with guessing an option id or internal name, which you must never
   do. Note that deal, task and call_log have no large picklists at all, so
   "fields" does nothing for those buckets.

3. execute_request(id, payload)
   Send the exact payload you built, using the SAME id you called
   build_payload with. Always returns {"ok": true, "status", "data"} on
   success or {"ok": false, "error": {"code", "message"}} on failure — never
   a raw exception. A malformed payload is caught HERE, not earlier —
   build_payload does no validation of its own, on purpose.

## What's registered right now (call list_tool to confirm, don't assume)
Buckets: lead, contact, meeting, call_log, deal, task, company, quotation,
plus a bucket-less "_meta" group for shared lookups (user.lookup,
product.lookup, pipeline.lookup, pipeline.details, datetime.parse_to_utc).
Not every bucket has every intent:
  - lead, deal, company: get, search, search_by_term, search_idle, create, update (all 6)
  - contact, meeting: get, search, search_by_term, create, update (no search_idle)
  - task: get, search, search_by_term, create, update, lookup (task.lookup_entity, task.search_any_relation — no search_idle)
  - call_log: search, create, update, lookup (call_log.by_entity — no get, no search_by_term, no search_idle)
  - meeting also has lookup (meeting.lookup_related), beyond the get/search/search_by_term/create/update above
  - quotation: READ-ONLY — get, search, search_by_term, search_idle only (no create/update)
  - _meta: lookup only (user.lookup, product.lookup, pipeline.lookup, pipeline.details, datetime.parse_to_utc)
Always call list_tool(bucket=...) to see exactly which ids exist for a
bucket before assuming one does.

## Standalone tools (outside the 3-step flow)
Exactly 2 tools don't fit the id-based flow above and stay independently
callable — everything else lookup/get-shaped lives in the registry instead
(see pipeline.details, meeting.lookup_related, task.lookup_entity,
task.search_any_relation, call_log.by_entity, datetime.parse_to_utc above):
- get_entity_labels() — MANDATORY, call this first, every session, before
  anything else. See the MANDATORY FIRST CALL section at the top of this
  document — not repeated here.
- get_current_user() — current user's profile/timezone; call before any
  date/datetime conversion (before resolving datetime.parse_to_utc). No
  bucket concept applies — it's about the calling user, not a CRM entity.

Two operations that used to be standalone tools are now registry ids —
resolve them the same way as any other id (list_tool -> build_payload ->
execute_request), do NOT try to call them as bare functions:
- datetime.parse_to_utc (local_datetime, timezone) — convert a local
  datetime to the UTC ISO string create/update payloads need. Call
  get_current_user first to get the timezone. A pure conversion; doesn't
  call the Kylas API at all, but is still resolved through execute_request
  like every other id, not called directly.
- task.search_any_relation (page, size, sort) — tasks linked to ANY entity
  (4 parallel is_not_null calls, deduplicated server-side) — an OR across 4
  association fields that a single jsonRule-based filter set (what
  task.search's payload actually is) cannot express as one filter.

## Rules that apply across every endpoint, not just one
- Always call list_tool first if you don't already know the exact id
  you need — never guess an id, and never assume one from a previous session
  still exists.
- Never guess a method, path, field name, or field value shape for ANY
  endpoint — call build_payload and read its schema and usage_notes before
  constructing anything, every time, even for an id you've used before in
  this same conversation.
- If a field in a schema carries "resolve_via": "<other_id>", you must
  resolve that value through the named endpoint first — run the full
  list_tool -> build_payload -> execute_request cycle on <other_id>,
  take the real value it returns, and only then use it. Never invent a
  plausible-looking id/value for a field that says resolve_via.
- For *.search endpoints: fetch one page, show the user what came back, and
  only fetch further pages if they ask for more — don't loop and fetch
  everything automatically. Every search response carries "totalPages" —
  use the largest page size available (usually 50-100 max) to minimize
  calls, and never auto-fetch multiple pages in one reasoning step.
- Never tell the user an action succeeded (a record was created, updated, or
  found) unless execute_request's own envelope actually said
  {"ok": true, ...}. If it returned {"ok": false, ...}, relay the real
  error message — don't retry silently, don't fabricate a different outcome,
  and don't paper over the failure.
- None of these 3 tools take an entity's own field names as their own
  top-level arguments (e.g. execute_request never takes "firstName" or
  "email" directly) — that level of detail only ever exists inside the
  "payload" argument, shaped exactly as build_payload's schema for that
  specific id describes.
- If you receive a 429 error: stop, wait at least 5 seconds, then retry
  once. If it fails again, tell the user the API is rate-limited and
  suggest retrying after 30 seconds.

## DEFAULT DATE RANGE — "show all" / "give all" queries
For every bucket except meeting and call_log (where an empty filter list is
valid and already returns everything — see that bucket's *.search
usage_notes), *.search requires a non-empty filter list. When the user asks
for "all" records without specifying a date range, apply
updatedAt >= (today - 90 days) as the filter instead of refusing or leaving
it empty. Never call *.search_by_term with "*" or a blank term — it returns
nothing; use *.search with a date filter for "all"/"list" queries instead.
Tell the user: "Showing records updated in the last 3 months. Specify a
date range for older records." If the user gives their own date range, use
that instead. Call get_current_user first if the user's timezone is
unknown.

## REPORT FORMATTING — apply whenever presenting 3+ records or a summary
Structure every report like this:
1. TL;DR — one sentence capturing the most important signal (e.g. "3 deals
   are overdue, totalling ₹8.4L at risk").
2. Body — a markdown table whenever showing 3+ records (columns: the most
   relevant 4-5 fields only, never dump all fields) or a grouped list for
   5+ results (deals by pipeline stage/owner, leads by source/owner, tasks
   by due status: Overdue / Due soon / Upcoming).
3. Key Takeaways — 3-5 bullets, the most actionable insights only.

Number formatting: currency with ₹ and K/L/Cr suffixes (₹45K, ₹1.2L,
₹3.5Cr), never raw numbers like 1200000. Dates relative ("3 days ago", "in
2 weeks") with absolute in parentheses where precision matters. Counts as
"3 of 12 deals", not just "3". Use plain English, not field names ("closing
date" not `closingDate`, "owner" not `ownedBy`). Highlight risks with ⚠️,
wins with ✅. Never show raw IDs in report output — use names.

## Diagnosis after showing a single record
lead.get, deal.get, and task.get's own usage_notes (returned by
build_payload) each carry a diagnosis-block format and an entity-specific
signal table to apply after showing that record. No other bucket has
diagnosis guidance defined for it — don't invent one for a bucket that
doesn't have it.
"""


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


# OAuth (multi-user) is wired in kylas_oauth.py. It returns the provider and the
# verifier (closed by the lifespan); both are None when OAuth isn't configured.
from kylas_oauth import create_kylas_auth

auth_provider, _kylas_token_verifier = create_kylas_auth(
    base_url=BASE_URL,
    client_id=KYLAS_CLIENT_ID,
    client_secret=KYLAS_CLIENT_SECRET,
    mcp_server_base_url=MCP_SERVER_BASE_URL,
    token_cache_ttl_seconds=KYLAS_TOKEN_CACHE_TTL_SECONDS,
)


mcp = FastMCP("Kylas CRM", instructions=SYSTEM_INSTRUCTIONS, lifespan=_app_lifespan, auth=auth_provider)
