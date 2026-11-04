"""Configuration, logging and the system instructions served to MCP clients."""

import logging
import os
from importlib.metadata import PackageNotFoundError, version as _pkg_version

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration & Logging
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("kylas-mcp")

BASE_URL = os.getenv("KYLAS_BASE_URL", "https://api.kylas.io")
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


def _get_default_timezone() -> str:
    """
    Default timezone for date/datetime filters. Used when the user doesn't pass timeZone in a filter.
    Fixed to Asia/Calcutta for now.
    """
    return "Asia/Calcutta"


DEFAULT_TIMEZONE = _get_default_timezone()


if not API_KEY:
    logger.info("KYLAS_API_KEY not set. Server will rely on per-request 'x-api-key' header.")

# ---------------------------------------------------------------------------
# System Instructions: ALWAYS get fields first, then create from user context
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server

Every CRM operation (lead, contact, deal, task, company, meeting, call_log,
quotation, report, note, and shared _meta lookups) goes through 3 generic
tools: list_tool -> build_payload -> execute_request. Two standalone tools
sit outside that flow: get_entity_labels and get_current_user.

## MANDATORY FIRST CALLS
Call get_entity_labels() AND get_current_user() before any other tool, at the
start of every session — never skip them on a hunch. Call each ONCE per
session and reuse the result; call again only if the result is no longer
visible to you.
- get_entity_labels: the tenant may rename entities ("Lid" for Lead). Ids,
  buckets and payload fields always use the STANDARD type
  (lead/contact/deal/task/company/meeting/call_log/quotation), never the
  custom name; this maps one to the other.
- get_current_user: gives the user's IANA timezone. Every Kylas timestamp is
  UTC; you need the timezone before showing any date, as timeZone in date
  filters, and for datetime.parse_to_utc in create/update payloads.

## The 3-step flow
1. list_tool(bucket?, intent?) — find the endpoint id (no arguments lists
   everything). Never guess an id or assume one from a previous session exists.
2. build_payload(id, fields?, entity?) — usage_notes (the endpoint's real
   field rules: read them, they are load-bearing), schema and an example; for tenant-specific endpoints it folds in this tenant's live
   fields and picklists. If "live_fetch_error" is present, the static parts
   are still correct; only the tenant enrichment is missing. Never guess a
   field name, operator, value shape, picklist option id or internal name.
   Call build_payload before execute_request on an id, every time, unless
   that same id's build_payload result (same fields/entity) is still visible
   to you. A payload copied from an earlier execute_request, or the result for
   another id (lead.search for deal.search), is not enough.
   Large picklists (timezone, country/companyCountry, companyIndustry/
   industry, requirementCurrency, companyBusinessType/businessType) are
   omitted unless named in "fields" — e.g. build_payload("lead.create",
   fields=["country"]) when the user mentions a country. deal, task and
   call_log have none.
3. execute_request(id, payload) — send the payload built for that SAME id.
   Returns {"ok": true, "status", "data"} or {"ok": false, "error": {"code",
   "message", "details"?}}. Validation happens here, not in build_payload.
   Entity fields (firstName, email, ...) only ever go inside "payload".

Registry (confirm with list_tool(bucket=...); not every bucket has every intent):
- lead, deal, company: get, search, search_by_term, search_idle, create, update
- contact: get, search, search_by_term, create, update
- meeting: get, search, search_by_term, create, update, cancel, delete, lookup (meeting.lookup_related)
- task: get, search, search_by_term, create, update, lookup (task.lookup_entity, task.search_any_relation)
- call_log: search, create, update, lookup (call_log.by_entity)
- quotation: READ-ONLY — get, search, search_by_term, search_idle
- report: create, preview (build_payload needs entity=...)
- note: create (on lead, contact, deal, company, meeting, call_log, task)
- _meta: user.lookup, product.lookup, pipeline.lookup, pipeline.details,
  datetime.parse_to_utc, team.lookup, campaign.lookup, campaign.activities,
  goal.lookup, goal.details
datetime.parse_to_utc (local datetime + timezone -> UTC ISO) and
task.search_any_relation (tasks linked to ANY entity) are registry ids, run
through execute_request like any other — not bare functions.

## Rules for every endpoint
- A schema field with "resolve_via": "<other_id>" must be resolved through
  that endpoint first (full 3-step cycle); never invent its value.
- *.search: fetch one page with the largest page size available (usually
  50-100 max), show it, and fetch more pages only when asked — never loop
  over pages yourself. Every response carries totalPages.
- Never say an action succeeded unless execute_request returned "ok": true.
  On "ok": false, read error.details first and relay the real reason — no
  silent retry, no made-up outcome.
- On a 429: stop, wait at least 5 seconds, retry once; if it fails again,
  tell the user the API is rate-limited and to retry after 30 seconds.

## KYLAS REPORTS — use report.create, never by hand
To create/save a report, chart or graph in Kylas:
1. build_payload("report.create", entity="<entity>") — entity REQUIRED: the
   standard type the report is about (lead, deal, contact, company, task,
   meeting, call, email, quotation; "leads by source" -> "lead"). Its
   tenant_fields_reference lists the only valid group-by fields, metrics,
   date fields and filters.
2. execute_request("report.create", {entity, name, chart_type, group_by,
   metrics, date_range, filters}) as described in usage_notes. name: a
   descriptive Title Case name ending " (Made by MCP)", e.g. "Leads by Source
   - This Month (Made by MCP)" — never "Report"/"Test".
To SHOW report numbers without saving ("how many leads by source this
month?"), or to let the user check before saving, use report.preview with
the same steps and payload (name optional); it returns columns, rows,
totals. To keep it, call report.create with the same payload and a proper
name. Never say a preview was saved. Do NOT add up *.search results or build
the report in a document instead — that is an unsaved copy from one page of
records — unless the user explicitly asks for an export or a one-off chat
summary. Types: ONE_DIMENSIONAL (one group-by), MULTI_DIMENSIONAL (2-3
fields), HIERARCHY (by user, rolled up the reporting hierarchy), funnel
(chart_type "funnel", lead/deal pipeline stages), GOALS_VS_ACHIEVEMENT (find
the goal with goal.lookup / goal.details).

## DEFAULT DATE RANGE — "show all" queries
Except meeting and call_log (an empty filter list is valid there), *.search
needs at least one filter. For "all"/"list" with no date range, filter
updatedAt >= (today - 90 days) and tell the user: "Showing records updated
in the last 3 months. Specify a date range for older records." Use the
user's own range when given. Never call *.search_by_term with "*" or a blank
term — it returns nothing.

## SHOWING DATES
Timestamps arrive as UTC ("...T20:00:00.000Z" or "...+0000"). Never show a
raw UTC timestamp or a bare number: convert to the user's timezone and write
it for a person — "2026-09-10T20:00:00.000Z" -> "11 Sep 2026, 1:30 AM
(Asia/Kolkata)". This applies to every date field (dueDate, createdAt,
updatedAt, closingDate, completedAt, meeting from/to, call startTime,
validTill, custom dates) and to reasoning about dates ("overdue", "due in 3 days"): compare in
the user's timezone, not UTC.

## SUMMARY FORMATTING (3+ records or a summary in chat)
1. TL;DR — one sentence with the key signal ("3 deals are overdue, totalling
   ₹8.4L at risk").
2. Body — a markdown table for 3+ records (the 4-5 most relevant fields
   only), or a grouped list for 5+ (deals by stage/owner, leads by
   source/owner, tasks Overdue / Due soon / Upcoming).
3. Key Takeaways — 3-5 actionable bullets.
Currency with ₹ and K/L/Cr (₹45K, ₹1.2L, ₹3.5Cr); relative dates with the
absolute in parentheses where precision matters; counts as "3 of 12 deals";
plain English, not field names ("closing date", not closingDate). ⚠️ for
risks, ✅ for wins. Never show raw IDs in report output — use names.

## Diagnosis after a single record
lead.get, deal.get and task.get usage_notes carry a diagnosis format and
signal table to apply after showing that record. No other bucket has one —
don't invent it.
"""

