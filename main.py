"""
Kylas CRM MCP Server

Model Context Protocol server for Kylas CRM operations:

GENERIC CRUD (use these instead of entity-specific tools):
- create_entity(entity_type, field_values)  — create any entity
- update_entity(entity_type, entity_id, field_values)  — update any entity
  Supported entity_type values: lead, contact, deal, task, company, meeting, call_log

PER-ENTITY FIELD INSTRUCTIONS (call FIRST to get schema before create/update):
- get_lead_field_instructions
- get_contact_field_instructions
- get_task_field_instructions
- get_deal_field_instructions
- get_company_field_instructions

PER-ENTITY GET (fetch full record by ID):
- get_lead, get_contact, get_task, get_deal, get_company, get_meeting, get_call_log

SEARCH:
- search_entity(entity_type, filters) — filter any entity by criteria
- search_entity_by_term(entity_type, term) — full-text search
- search_idle_entities(entity_type, days) — no activity for N days

PIPELINE:
- lookup_pipelines, get_pipeline_stages, get_pipeline_details

SHARED UTILITIES:
- get_current_user (timezone, ID; use for date/datetime handling)
- lookup_users (resolve user names to IDs for ownerId, createdBy, updatedBy)
- lookup_products (find products for field_values)
- parse_datetime_to_utc_iso_tool (convert user timezone to UTC ISO)
"""

import asyncio
import os
import random
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from importlib.metadata import version as _pkg_version, PackageNotFoundError
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo

import httpx
import phonenumbers
import yaml
from dateutil import parser as dateutil_parser
from fastmcp.server import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from dotenv import load_dotenv
import json

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

# Populated when OAuth is configured; referenced by the lifespan for clean shutdown.
_kylas_token_verifier = None  # type: ignore[var-annotated]
try:
    SERVER_VERSION = _pkg_version("kylas-crm-mcp-server")
except PackageNotFoundError:
    SERVER_VERSION = "unknown"


# ---------------------------------------------------------------------------
# Logging Helpers
# ---------------------------------------------------------------------------

def _mask_api_key(api_key: Optional[str]) -> str:
    """Mask API key but keep last 12 characters for context."""
    if not api_key:
        return "None"
    if len(api_key) <= 12:
        return "***"
    return "*" * (len(api_key) - 12) + api_key[-12:]


async def _log_request(request: httpx.Request) -> None:
    """Log request details in a readable format."""
    api_key = request.headers.get("api-key")
    auth_header = request.headers.get("Authorization")

    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        masked_auth = f"Bearer {_mask_api_key(token)}"
    else:
        masked_auth = f"api-key: {_mask_api_key(api_key)}" if api_key else "None"

    payload = "None"
    if request.content:
        try:
            payload = json.dumps(json.loads(request.content), indent=2)
        except Exception:
            payload = request.content.decode("utf-8", errors="replace")

    logger.info(
        f"\n{'='*60}\n"
        f"🚀 API REQUEST\n"
        f"{'-'*60}\n"
        f"Method:   {request.method}\n"
        f"URL:      {request.url}\n"
        f"Auth:     {masked_auth}\n"
        f"Payload:\n{payload}\n"
        f"{'='*60}"
    )


async def _log_response(response: httpx.Response) -> None:
    """Log response details in a readable format."""
    await response.aread()

    body = "None"
    if response.content:
        try:
            body = json.dumps(response.json(), indent=2)
        except Exception:
            body = response.text

    error_info = ""
    if response.status_code >= 400:
        try:
            data = response.json()
            error_code = data.get("errorCode", "N/A")
            details = data.get("details") or data.get("message") or "N/A"
            error_info = f"\nError Code: {error_code}\nDetails:    {details}"
        except Exception:
            pass

    logger.info(
        f"\n{'='*60}\n"
        f"✅ API RESPONSE\n"
        f"{'-'*60}\n"
        f"Status:   {response.status_code}{error_info}\n"
        f"Body:\n{body}\n"
        f"{'='*60}"
    )


def _get_default_timezone() -> str:
    """
    Default timezone for date/datetime filters. Used when the user doesn't pass timeZone in a filter.
    Fixed to Asia/Calcutta for now.
    """
    return "Asia/Calcutta"


DEFAULT_TIMEZONE = _get_default_timezone()

# Entity label mapping (tenant-specific display names)
_ENTITY_LABELS: Dict[str, Dict[str, str]] = {}


def _threshold_iso_days_ago(days: int, time_zone: str) -> str:
    """Return (now - days) in the given timezone as ISO string (UTC with Z)."""
    try:
        tz = ZoneInfo(time_zone)
    except Exception:
        tz = ZoneInfo("UTC")
    now = datetime.now(tz)
    threshold = now - timedelta(days=days)
    return threshold.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _convert_date_value_to_utc(value: Any, timezone_str: str) -> Any:
    """
    Convert date/datetime filter value(s) from user's local timezone to UTC.
    Handles single ISO string, list of ISO strings (for between operator), or None.
    Strips trailing 'Z' before parsing so the value is treated as local time.
    """
    if value is None:
        return value

    try:
        tz = ZoneInfo(timezone_str)
    except Exception:
        tz = ZoneInfo("UTC")
    utc = ZoneInfo("UTC")

    def _convert_single(v: str) -> str:
        if not isinstance(v, str):
            return v
        # Strip trailing Z so we treat it as naive local time
        clean = v.rstrip("Z").rstrip("z")
        try:
            # Try parsing with milliseconds first
            try:
                dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
            # Attach the user's timezone, then convert to UTC
            local_dt = dt.replace(tzinfo=tz)
            utc_dt = local_dt.astimezone(utc)
            return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc_dt.microsecond // 1000:03d}Z"
        except Exception:
            return v  # Return as-is if parsing fails

    if isinstance(value, list):
        return [_convert_single(v) for v in value]
    return _convert_single(value)


def _format_entity_labels_for_instructions(labels: Dict[str, Dict[str, str]]) -> str:
    """
    Format entity labels as action-oriented routing rules for system instructions.
    Returns empty string if no labels.
    """
    if not labels:
        return ""

    lines = [
        "## ENTITY NAME ROUTING — CALL get_entity_labels() FIRST, THEN USE THESE RULES",
        "",
        "This tenant has renamed CRM entities. When the user mentions any of the names below,",
        "call get_entity_labels() to confirm, then use the standard type for all tool calls:",
        "",
    ]

    for entity_type in sorted(labels.keys()):
        label_data = labels[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(
            f'- If user says "{display_name}" or "{display_plural}" → call get_entity_labels(), '
            f'then use standard type "{std_type}" in all tool calls'
        )

    lines.append("")
    lines.append('DO NOT tell the user an entity "doesn\'t exist" before calling get_entity_labels().')

    return "\n".join(lines)


async def _load_entity_labels() -> Dict[str, Dict[str, str]]:
    """
    Fetch entity labels from /v1/entities/label endpoint.
    Returns mapping like: {"LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"}, ...}
    Returns empty dict if fetch fails.
    """
    global _ENTITY_LABELS
    try:
        async with get_client() as client:
            resp = await client.get(f"{BASE_URL}/entities/label")
            _ENTITY_LABELS = resp.json()
            # Log with display names in clear format
            summary = "\n".join(
                f"  • {etype:8} => {data.get('displayName', etype)} / {data.get('displayNamePlural', etype)}"
                for etype, data in sorted(_ENTITY_LABELS.items())
            )
            logger.info(f"\n📦 Loaded Entity Labels:\n{summary}")
            return _ENTITY_LABELS
    except Exception as e:
        logger.warning(f"Failed to load entity labels: {e}")
        _ENTITY_LABELS = {}
        return {}


async def _label_refresh_loop(interval_seconds: int = 1800) -> None:
    """
    Background task: periodically refresh entity labels.
    Default interval: 1800 seconds (30 minutes).
    Runs forever until cancelled. Updates _ENTITY_LABELS in-place and
    rebuilds MCP server instructions so new labels are picked up immediately.
    """
    global _ENTITY_LABELS
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            logger.debug("Refreshing entity labels...")
            async with get_client() as client:
                resp = await client.get(f"{BASE_URL}/entities/label")
                new_labels = resp.json()
                _ENTITY_LABELS.clear()
                _ENTITY_LABELS.update(new_labels)
                logger.info(f"🔄 Refreshed entity labels: {list(_ENTITY_LABELS.keys())}")
                mcp._mcp_server.instructions = _build_instructions()
                _update_tool_description(mcp)
                _patch_entity_tool_descriptions(mcp)
        except asyncio.CancelledError:
            logger.info("Label refresh loop cancelled")
            break
        except Exception as e:
            logger.warning(f"Label refresh failed: {e}. Keeping cached labels.")


if not API_KEY:
    logger.info("KYLAS_API_KEY not set. Server will rely on per-request 'x-api-key' header.")

# ---------------------------------------------------------------------------
# System Instructions: ALWAYS get fields first, then create from user context
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Lead, Contact & Task Support

## ⚠️ MANDATORY SESSION RULES — READ FIRST

### Rule 0 — Call first, every session
Call `get_entity_labels()` IMMEDIATELY at the start of every session before anything else.
This tenant uses custom names for CRM entities. Without this, you will misidentify entity requests.

### Rule 1 — Entity field schemas (once per session)
Call each entity’s field instructions tool the FIRST time you interact with that entity. Do NOT call it again for subsequent operations on the same entity in the same session.

### Rule 2 — NEVER simulate CRM actions
**CRITICAL: You MUST call the appropriate tool for every create / update / delete / search operation. Never respond as if an action succeeded without actually calling the tool.**
- ❌ Do NOT write an artifact, table, or summary showing what "would be" created and then stop.
- ❌ Do NOT say "Lead created" or "I’ve created the lead" without a successful tool call response.
- ❌ Do NOT ask for confirmation before calling a create/update tool — just call it (unless a required field is missing).
- ✅ Call the tool → show the result. That’s the only valid flow.

| Entity    | Tool (call once per session)       |
|-----------|------------------------------------|
| Lead      | `get_lead_field_instructions`      |
| Contact   | `get_contact_field_instructions`   |
| Task      | `get_task_field_instructions`      |
| Deal      | `get_deal_field_instructions`      |
| Company   | `get_company_field_instructions`   |
| Meeting   | `get_meeting_field_instructions`   |
| Call Log  | `get_call_log_field_instructions`  |

---

## 🚨 ENTITY LABEL MAPPING (Tenant-Customized Names)

Before saying "Kylas doesn’t have X entity", check this mapping first. If the user’s entity name is found here, use the standard type for all tool calls. Only say "entity not found" if the name is absent from both this mapping and the standard types.

{ENTITY_LABEL_MAPPING}

---

## PAGINATION — AVOID RATE LIMITS (429)

The Kylas API rate-limits rapid sequential requests. Every search tool returns `totalPages` — follow these rules strictly when there are multiple pages:

1. **Always use the largest page size available** (`size=50` is the max for most endpoints). Fewer calls = fewer 429s.
2. **Do NOT auto-fetch all pages sequentially.** Fetch page 1, show results, then ask: *"There are N more pages. Do you want me to fetch them?"* Wait for user confirmation before fetching page 2, 3, etc.
3. **Use `return_all=True` / `fetch_all_pages=True` where available** (e.g. `lookup_users`). These flags do the paging server-side in one tool call — far safer than manual iteration.
4. **One page at a time when manually paginating.** After showing page N, wait for the user to ask for the next page. Never fetch multiple pages in one reasoning step.
5. **If you receive a 429 error:** stop, wait at least 5 seconds, then retry once. If it fails again, tell the user the API is rate-limited and suggest retrying after 30 seconds. (The server retries automatically up to 3 times with backoff, so a 429 reaching you means retries were exhausted.)

---

## DEFAULT DATE RANGE — "SHOW ALL" / "GIVE ALL" QUERIES

When the user asks for "all" records without a date range, apply `updatedAt ≥ (today − 90 days)`.
- **Never use `search_entity_by_term` with `"*"` or blank** — returns no results.
- Call `get_current_user` first if timezone is unknown.
- Tell the user: *"Showing records updated in the last 3 months. Specify a date range for older records."*
- If the user specifies a date range, use that instead.

Use `search_entity(entity_type, [{"field": "updatedAt", "operator": "greater_or_equal", "value": "<ISO>"}])` where `entity_type` is one of: `lead`, `contact`, `task`, `deal`, `company`, `meeting`.

---

## COMMON RULES (apply to all entities)

### Building field_values
- Use ONLY fields the user provided — no defaults, no extras.
- Keys: API name for standard fields, or field ID string for custom fields.
- Custom fields: `"customFieldValues": {"<internalName>": <value>}` — never use field ID as the key.

### Emails
Shorthand: `"email": "user@example.com"` (normalized to OFFICE/primary).
Full: `[{"email": "...", "type": "OFFICE|PERSONAL", "primary": true}]`

### Phone numbers
Full: `[{"number": "...", "type": "MOBILE|WORK|HOME|PERSONAL", "code": "IN", "primary": true}]`
Shorthand: `"phone": "5551234567"` + top-level `"phone_country_code": "IN"` + `"phone_type": "MOBILE"` (both required whenever phone is included).
**If user gives phone but NO country/dial code: do NOT create/update — ask first. Never infer from currency, locale, or number format.**
**If user gives phone but NO type: do NOT create/update — ask: "Is this number MOBILE, WORK, HOME, or PERSONAL?"**

### Picklist fields
Use **Option ID** (number) from cheat sheet. Exceptions — use **internal name** (string): `requirementCurrency`, `companyBusinessType`, `country`, `timezone`, `companyIndustry`.

### Date / datetime fields
1. Call `get_current_user` to get user’s timezone (e.g. `Asia/Calcutta`).
2. **Create/update:** call `parse_datetime_to_utc_iso_tool(datetime_string, timezone)` → use the returned UTC ISO string in field_values.
3. **Filter/search:** keep value in user’s timezone; pass `timeZone` in the filter (or omit — server uses it). Do NOT convert filter values to UTC.

### Never guess IDs — always resolve first
- **Users** (createdBy, updatedBy, owner, ownerId, assignedTo, conductedBy, etc. - i.e., sales representatives and employees): call `lookup_users(query)`. Never use contact or lead lookup tools for employees/users. If multiple matches, list them and ask user to pick.
- **Products**: call `lookup_products(query)`. If multiple matches, list and ask.
- **Entity IDs** (for association filters — associatedLeads, associatedDeals, etc.): search for the entity first to get its real ID. Never invent IDs — this causes hallucinated results.
  - Example: "contacts associated with deals from Acme" → search deals for "Acme" first, confirm which deal, then search contacts by that deal ID.

---

## Lead Operations

### Create / Update
Build `field_values` from user input only. For `update_lead`: pass lead ID from search results + fields to update.

### Search / Filter
- Use `search_entity("lead", filters)`. Only `filterable=true` fields (from cheat sheet) are allowed.
- PICK_LIST/MULTI_PICKLIST: use Option ID, except `requirementCurrency`, `companyBusinessType`, `country`, `timezone`, `companyIndustry` → use internal name.

### Pipeline and Stage
1. Call `lookup_pipelines(entityType="LEAD")` first.
2. Multiple pipelines → list them and ask. Single pipeline → still confirm before proceeding.
3. After confirmation: `get_pipeline_stages(pipeline_id)` → map intent to stage → use in create/update/search.
4. **Move to stage:** `update_lead(lead_id, {"pipelineStage": stage_id})`
5. **Closed Lost / Closed Unqualified:** call `get_pipeline_details` for closing reasons; ask user to pick, then pass `{"pipelineStage": stage_id, "pipelineStageReason": reason}`.
6. If lead already has a pipeline and user moves to a different one: confirm first.

### Idle / Stagnant Leads
Use `search_idle_entities("lead", days)` for "no activity for N days" queries.
Fallback: `search_entity("lead", [...])` with `updatedAt ≤ threshold AND latestActivityCreatedAt ≤ threshold`.

---

## Contact Operations
No pipeline or pipelineStage fields. Use `search_entity("contact", filters)`, `create_contact`, `update_contact`.
All common rules (phone, email, date, custom fields, ID lookups) apply.

---

## Task Operations
No pipeline, pipelineStage, emails, or phoneNumbers fields.

**Association (link task to an entity):**
```json
"relation": [{"targetEntityId": <id>, "targetEntityType": "LEAD|CONTACT|DEAL|COMPANY", "targetEntityName": "<name>"}]
```

**Resolve entity before creating/updating a task:**
Use `lookup_entity_for_task(entity_type, search_term)` to find the entity ID by name.
- entity_type: "lead", "contact", "deal", or "company" (internal type, not tenant display name)
- Returns ID and name to use in the "relation" field above.

**Filter tasks by a specific entity ID:**
- Lead: `search_entity("task", [{"field": "associatedLeads", "operator": "equal", "value": <lead_id>}])`
- Contact: `search_entity("task", [{"field": "associatedContacts", "operator": "equal", "value": <contact_id>}])`
- Deal: `search_entity("task", [{"field": "associatedDeals", "operator": "equal", "value": <deal_id>}])`
- Company: `search_entity("task", [{"field": "associatedCompanies", "operator": "equal", "value": <company_id>}])`

**Tasks with ANY relation present (no specific entity):**
Use `search_tasks_with_any_relation()` — makes 4 parallel `is_not_null` calls and deduplicates.
Do NOT try to filter manually or post-process results for this case — use the dedicated tool.
"""

# ---------------------------------------------------------------------------
# Search: Operator mapping by field type & picklists that use internal name
# ---------------------------------------------------------------------------

OPERATOR_MAPPING = {
    "TEXT_FIELD": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "PARAGRAPH_TEXT": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "NUMBER": ["equal", "not_equal", "greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "in", "not_in", "is_null", "is_not_null"],
    "MONEY": ["equal", "not_equal", "greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "in", "not_in", "is_null", "is_not_null"],
    "URL": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "CHECKBOX": ["equal", "not_equal"],
    "PICK_LIST": ["equal", "not_equal", "is_not_null", "is_null", "in", "not_in"],
    "MULTI_PICKLIST": ["equal", "not_equal", "is_not_null", "is_null", "in", "not_in"],
    "DATETIME_PICKER": ["greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "is_not_null", "is_null", "today", "yesterday", "tomorrow", "last_seven_days", "next_seven_days", "last_fifteen_days", "next_fifteen_days", "last_thirty_days", "next_thirty_days", "week_to_date", "current_week", "last_week", "next_week", "month_to_date", "current_month", "last_month", "next_month", "quarter_to_date", "current_quarter", "last_quarter", "next_quarter", "year_to_date", "current_year", "last_year", "next_year", "before_current_date_and_time", "after_current_date_and_time"],
    "DATE": ["greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "is_not_null", "is_null", "today", "yesterday", "tomorrow", "last_seven_days", "next_seven_days", "last_fifteen_days", "next_fifteen_days", "last_thirty_days", "next_thirty_days", "week_to_date", "current_week", "last_week", "next_week", "month_to_date", "current_month", "last_month", "next_month", "quarter_to_date", "current_quarter", "last_quarter", "next_quarter", "year_to_date", "current_year", "last_year", "next_year", "before_current_date_and_time", "after_current_date_and_time"],
    "DATE_PICKER": ["greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "is_not_null", "is_null", "today", "yesterday", "tomorrow", "last_seven_days", "next_seven_days", "last_fifteen_days", "next_fifteen_days", "last_thirty_days", "next_thirty_days", "week_to_date", "current_week", "last_week", "next_week", "month_to_date", "current_month", "last_month", "next_month", "quarter_to_date", "current_quarter", "last_quarter", "next_quarter", "year_to_date", "current_year", "last_year", "next_year", "before_current_date_and_time", "after_current_date_and_time"],
    "EMAIL": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "PHONE": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "TOGGLE": ["equal", "not_equal"],
    "FORECASTING_TYPE": ["equal", "not_equal", "in", "not_in", "is_empty", "is_not_empty"],
    "ENTITY_FIELDS": ["equal", "not_equal", "in", "not_in", "is_not_null", "is_null"],
    "LOOK_UP": ["equal", "not_equal", "is_not_null", "is_null", "in", "not_in"],
    "MEETING_ORGANIZER": ["equal", "not_equal", "is_not_null", "is_null", "in", "not_in"],
    "PIPELINE_STAGE": ["equal", "not_equal", "in", "not_in"],
    "PIPELINE": ["equal", "not_equal", "is_not_null", "is_null", "in", "not_in"],
    "PARTICIPANTS_LOOKUP": ["in", "not_in"],
}

# Operator symbol → name mapping (normalize user input like ">" to "greater")
OPERATOR_SYMBOL_MAP = {
    ">": "greater",
    "<": "less",
    ">=": "greater_or_equal",
    "<=": "less_or_equal",
    "!=": "not_equal",
    "==": "equal",
    "=": "equal",
    "gte": "greater_or_equal",
    "lte": "less_or_equal",
    "gt": "greater",
    "lt": "less",
    "ne": "not_equal",
    "eq": "equal",
}

# Picklist fields that use internal name (string) in search; all others use Option ID (long)
PICKLIST_FIELDS_USE_INTERNAL_NAME = {"requirementCurrency", "companyBusinessType", "country", "timezone", "companyIndustry"}

# ---------------------------------------------------------------------------
# API call throttle: 100–500 ms random delay between subsequent calls per tool
# ---------------------------------------------------------------------------

import contextvars

_api_call_count: contextvars.ContextVar[int] = contextvars.ContextVar("api_call_count", default=0)

async def _before_api_call() -> None:
    """If this is not the first API call in this tool run, sleep 100–500 ms at random."""
    n = _api_call_count.get()
    if n > 0:
        delay = random.uniform(0.1, 0.5)
        await asyncio.sleep(delay)


def _after_api_call() -> None:
    """Mark that one API call has completed (for throttle counting)."""
    _api_call_count.set(_api_call_count.get() + 1)


def _reset_api_call_count() -> None:
    """Reset at the start of each tool so only subsequent calls within the same tool are delayed."""
    _api_call_count.set(0)


class _ThrottledClient:
    """Wraps httpx.AsyncClient to add 100–500 ms random delay before 2nd, 3rd, … request in the same tool."""

    def __init__(self, client: httpx.AsyncClient):
        self._client = client

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        await _before_api_call()
        try:
            return await self._client.get(url, **kwargs)
        finally:
            _after_api_call()

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        await _before_api_call()
        try:
            return await self._client.post(url, **kwargs)
        finally:
            _after_api_call()

    async def put(self, url: str, **kwargs: Any) -> httpx.Response:
        await _before_api_call()
        try:
            return await self._client.put(url, **kwargs)
        finally:
            _after_api_call()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


# ---------------------------------------------------------------------------
# HTTP Client & Errors
# ---------------------------------------------------------------------------

class KylasAPIError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(self.message)


def _get_mcp_client_name() -> str:
    """
    Resolve the MCP client identifier from the MCP initialize handshake.
    Returns '{name}({version})' (e.g. 'Claude Desktop(1.2.3)') or 'unknown'.
    Used for the outbound User-Agent to Kylas: kylas_mcp_server({version}) on {client}.
    """
    try:
        ctx = mcp.get_context()
        client_params = ctx.session.client_params
        if client_params and client_params.clientInfo:
            name = client_params.clientInfo.name or ""
            version = client_params.clientInfo.version or ""
            if name:
                result = f"{name}({version})" if version else name
                logger.debug("MCP client identified: %s", result)
                return result
            logger.warning("MCP clientInfo present but name is empty: %r", client_params.clientInfo)
        else:
            logger.warning("MCP client_params missing or has no clientInfo (proxy/unknown client)")
    except Exception as exc:
        logger.warning("Could not resolve MCP client name: %s", exc)
    return "unknown"


def _resolve_auth_headers() -> Dict[str, str]:
    """Resolve authentication headers:
    1. FastMCP OAuth Bearer token (if user is authenticated via OAuth)
    2. Per-request x-api-key header (if passed in the HTTP request)
    3. Environment variable fallback (KYLAS_API_KEY)
    """
    # 1. Try FastMCP OAuth access token
    try:
        from fastmcp.server.dependencies import get_access_token
        access_token = get_access_token()
        if access_token and access_token.token:
            logger.debug("Resolved OAuth Bearer token authentication")
            return {"Authorization": f"Bearer {access_token.token}"}
    except Exception as e:
        logger.debug("Could not resolve OAuth token: %s", e)

    # 2. Try per-request x-api-key HTTP header
    try:
        from fastmcp.server.dependencies import get_http_request
        request = get_http_request()
        header_key = request.headers.get("x-api-key")
        if header_key:
            logger.debug("Resolved x-api-key header authentication")
            return {"api-key": header_key}
    except Exception:
        pass

    # 3. Fall back to env var
    if API_KEY:
        logger.debug("Resolved environment KYLAS_API_KEY authentication")
        return {"api-key": API_KEY}

    raise KylasAPIError(
        "No authentication provided. Pass x-api-key header, set KYLAS_API_KEY, or configure Kylas OAuth."
    )


class _ThrottledClientContext:
    """Async context manager: wraps httpx.AsyncClient and yields _ThrottledClient for 100–500 ms delay between calls."""

    def __init__(self) -> None:
        auth_headers = _resolve_auth_headers()
        client_name = _get_mcp_client_name()
        user_agent = f"kylas_mcp_server({SERVER_VERSION}) on {client_name}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": user_agent,
        }
        headers.update(auth_headers)
        self._raw = httpx.AsyncClient(
            base_url=BASE_URL,
            headers=headers,
            timeout=30.0,
            event_hooks={
                "request": [_log_request],
                "response": [_log_response],
            },
        )

    async def __aenter__(self) -> "_ThrottledClient":
        entered = await self._raw.__aenter__()
        return _ThrottledClient(entered)

    async def __aexit__(self, *args: Any) -> Any:
        return await self._raw.__aexit__(*args)


def get_client() -> _ThrottledClientContext:
    """Return a context manager that yields a throttled HTTP client (100–500 ms delay between subsequent API calls)."""
    return _ThrottledClientContext()


async def handle_api_response(response: httpx.Response, operation: str) -> Dict[str, Any]:
    try:
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        logger.error(f"❌ {operation} failed: {e.response.status_code}")
        if e.response.status_code == 429:
            retry_after = e.response.headers.get("Retry-After", "30")
            raise KylasAPIError(
                f"Rate limit hit (429). Wait {retry_after} seconds before retrying. "
                "If you were paginating, stop and ask the user before fetching more pages.",
                status_code=429,
                response_body=error_body,
            )
        raise KylasAPIError(
            f"{operation} failed: {e.response.status_code}",
            status_code=e.response.status_code,
            response_body=error_body
        )
    except Exception as e:
        logger.error(f"{operation} failed: {str(e)}")
        raise KylasAPIError(f"{operation} failed: {str(e)}")


# ---------------------------------------------------------------------------
# Deal System Instructions (added alongside Lead instructions)
# ---------------------------------------------------------------------------

DEAL_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Deal Operations

### Create / Update
Build `field_values` from user input only. For `update_deal`: pass deal ID from search results + fields to update.

### Deal-Specific Field Formats
- **Monetary fields** (`estimatedValue`, `actualValue`, `value`): must be `{"currencyId": <id>, "value": <number>}`. Never pass a plain number — API will reject it. (e.g. `"estimatedValue": {"currencyId": 431, "value": 32}`)
- **Owner** (`ownedBy`): `{"id": <user_id>}` — resolve via `lookup_users`. (e.g. `"ownedBy": {"id": 7236}`)

### Search / Filter
- Use `search_entity("deal", filters)`. Only `filterable=true` fields (from cheat sheet) are allowed.
- PICK_LIST exceptions (use internal name string, not Option ID): `currency`, `country`, `dealSource`.

### Pipeline and Stage
- Call `lookup_pipelines(entity_type="DEAL")` first. List pipelines and confirm with user (even if only one).
- **Move to same-pipeline stage:** `update_deal(deal_id, {"pipelineStage": stage_id})`
- **Move to different pipeline:** `update_deal(deal_id, {"pipeline": {...}, "forecastingType": "..."})` with full pipeline object including nested stage.
- Closing reasons (Closed Lost/Unqualified): call `get_pipeline_details`, ask user to pick, then pass `pipelineStageReason`.
- **Sequential stage flow:** If the pipeline has `sequentialStageFlow=true` (visible in `get_pipeline_details` output), stages cannot be skipped. The server handles this automatically — if a direct jump is blocked, it creates the deal in stage 1 and advances stage-by-stage. You do NOT need to do anything differently; just pass the desired target stage and the server will handle the sequential advancement transparently.

### Idle / Stagnant Deals
`search_idle_entities("deal", days)` — or `search_entity("deal", [...])` with `updatedAt ≤ threshold AND latestActivityCreatedAt ≤ threshold`.

### Products on Deals
Products are a key part of a deal. Always display them when showing deal details.

**Adding products:** Before create/update, ask user for: product name (resolve via `lookup_products`), quantity, price per unit, currency, and optional discount. Do NOT add without confirming price, quantity, and currency first.
```
{"products": [{"id": <product_id>, "quantity": <qty>, "price": {"currencyId": <id>, "value": <price>}, "discount": {"value": <disc>, "type": "PERCENTAGE|FLAT"}}]}
```
Existing products preserved; new products merged (duplicates by ID skipped).

**Finding deals by product:** Call `lookup_products(query)` to resolve the product name to an ID, then:
```
search_entity("deal", [{"field": "products", "operator": "equal", "value": <product_id>}])
```
"""

# ---------------------------------------------------------------------------
# Company System Instructions
# ---------------------------------------------------------------------------

COMPANY_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Company Operations

### Create / Update
Build `field_values` from user input only. No pipeline, pipelineStage, associatedContacts, or products fields.
All common rules apply (phone, email, date, custom fields, ID lookups).

### Search / Filter
- Use `search_entity("company", filters)`. Only `filterable=true` fields (from cheat sheet) allowed.
- PICK_LIST exception: `country` → use internal name (string), not Option ID.

### Idle / Stagnant Companies
`search_idle_entities("company", days)`.
"""

# ---------------------------------------------------------------------------
# Meeting System Instructions
# ---------------------------------------------------------------------------

MEETING_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Meeting Operations

### Create / Update
Before creating, ask for:
1. **Title** (required)
2. **Start/end datetime** (required) — convert to UTC via `get_current_user` + `parse_datetime_to_utc_iso_tool`
3. **Participants** (required) — resolve via `lookup_meeting_related_entity` with `entity_type="invitee"`; pick row with correct `entity` type
   - **CRITICAL RULES FOR INVITEES**: 
     - Only leads/contacts with a VALID EMAIL can be added as invitees. Check the `emails` array from the lookup result.
     - Deals CANNOT be added as invitees. If the user asks to add a deal as an invitee, DO NOT add it to the `participants` payload, and inform them of this restriction.
4. **Related entities** (optional) — resolve IDs first via entity lookup tools
5. **Location**, **allDay** (optional)

### Payload Format
```json
{
  "title": "...", "from": "<UTC ISO>", "to": "<UTC ISO>", "allDay": false,
  "timezone": {"id": 372, "name": "Asia/Calcutta"},
  "participants": [{"id": <user_id>, "entity": "user|lead|contact|external"}],
  "relatedTo": [{"id": <entity_id>, "entity": "lead|contact|deal|company"}],
  "location": "Office", "description": "..."
}
```

### Resolving Entity IDs
- **Meeting Owner / Conductor (CRM Users / Sales Reps):** ALWAYS use `lookup_users` (never lookup contacts, leads, or companies for sales reps).
- **Participants/organizer:** `lookup_meeting_related_entity` with `entity_type="invitee"` (pick row with right `entity` — organizer is usually `user`)
- **relatedTo leads:** `lookup_meeting_related_entity` with `entity_type="lead"` → filter `associatedLeads`
- **relatedTo contacts:** `lookup_meeting_related_entity` with `entity_type="contact"` → filter `associatedContacts`
- **relatedTo deals:** `lookup_meeting_related_entity` with `entity_type="deal"` → filter `associatedDeals`
- **relatedTo companies:** `lookup_meeting_related_entity` with `entity_type="company"` → filter `associatedCompanies`
- Combine multiple rules with AND.

### Search / Filter
- By associated entity: resolve ID first via lookup tool, then `search_entity("meeting", filters)` with association filter.
- Presence check: `is_not_null` / `is_null` with value null.
- By owner: `lookup_users` to resolve the user ID first → `{"field": "owner", "operator": "equal", "value": <user_id>}`.
- By organizer: `lookup_meeting_related_entity` with `entity_type="invitee"` → `{"field": "organizer", "operator": "equal", "value": <user_id>}`.
- Status filter: use internal name — "scheduled", "conducted", "missed", "cancelled".
- By date/time: Use `from` (start datetime), `to` (end datetime), or `conductedAt` (conducted datetime) fields. Do NOT use `scheduledAt`.
- Sorting: Sort by `from` (e.g., `from,desc`) or `createdAt` (e.g., `createdAt,desc`). Do NOT sort by `scheduledAt`.

### Cancel vs Delete
- `cancel_meeting` — sets status to "cancelled" (reversible)
- `delete_meeting` — permanent; confirm with user first.

### Notes
`add_note("MEETING", meeting_id, "note text")`
"""

# ---------------------------------------------------------------------------
# Call Log System Instructions
# ---------------------------------------------------------------------------

CALL_LOG_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Call Log Operations

### Create
Before creating, ask for:
1. **Entity** — which lead, contact, or deal? Search to get the ID.
2. **Phone number**, **call type** (incoming/outgoing), **outcome** (connected/rejected/busy/no_answer/missed_call/in_progress), **start time** (convert to UTC).
3. **Duration** (seconds) and **notes** — optional.

### Payload — Lead or Contact
```json
{
  "outcome": "connected", "callType": "outgoing",
  "startTime": "<UTC ISO>", "phoneNumber": "9618488578", "duration": "420",
  "relatedTo": {"id": <entity_id>, "entity": "lead|contact", "phoneNumber": "..."},
  "notes": [{"description": "..."}]
}
```

### Payload — Deal
Same as above with `"entity": "deal"` in relatedTo. Optionally link a contact:
```json
"associatedTo": [{"id": <contact_id>, "entity": "contact", "phoneNumber": "..."}]
```

### Fetching Call Logs
`get_call_logs(entity_id, entity_type)` — for a lead, contact, or deal.

### Notes
`add_note("CALL_LOG", call_log_id, "note text")`
"""


QUOTATION_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Quotation Operations (READ-ONLY)

A quotation is a priced proposal, usually linked to a deal, made up of line-item products with
tax/discount and billing/shipping addresses.

Quotations are READ-ONLY here. You can search, view, and report on them — you CANNOT create,
update, or delete quotations through this server. If the user asks to create/edit a quotation, tell
them it must be done in the Kylas web app; do not attempt it via create_entity/update_entity
(those tools do not accept entity_type "quotation").

Available quotation tools (same read surface as lead, minus create/update):
- `get_quotation_field_instructions` — field cheat sheet (call once per session before filtering).
- `search_entity("quotation", filters)` — list/filter quotations by field criteria.
- `search_entity_by_term("quotation", term)` — free-text search across summary, quotation number, and associated deal/company/contact/product names.
- `search_idle_entities("quotation", days)` — quotations not updated for N days (based on updatedAt; quotations have no activity feed).
- `get_quotation(quotation_id)` — full details of one quotation (products, totals, deal, company, contacts).

### Field instructions FIRST
Call `get_quotation_field_instructions` the first time you touch quotations in a session to get
the field cheat sheet (standard + custom fields, filterable flags, picklist Option IDs).

### Search / Filter
- Use `search_entity("quotation", filters)`. Only `filterable=true` fields (from the cheat sheet) are allowed.
- Common filters: `quotationNumber`, `summary`, `status`, `grandTotal`, `associatedDeal`, `updatedAt`.
- To get all quotations: `[{"field": "summary", "operator": "is_not_empty"}]` (summary is required so this matches all), or an `updatedAt >= last 90 days` filter. Note: the `id` field (type ID) does NOT support is_not_null.
- Sorting: only sortable fields are accepted; if unsure, omit sort (server defaults to updatedAt,desc).

### Reporting
When showing multiple quotations, summarize by status or associated deal and total the grand totals
where useful (values are in the quotation's currency). Use `get_quotation(id)` to drill into one record.
"""


# ---------------------------------------------------------------------------
# Diagnosis & Reporting Instructions
# ---------------------------------------------------------------------------

DIAGNOSIS_AND_REPORTING_INSTRUCTIONS = """
# Intelligent Diagnosis & Reporting

---

## AUTOMATIC DIAGNOSIS — always append after displaying any entity

After showing a deal, lead, or task, **always** append a diagnosis block in this exact format:

```
── Diagnosis ──────────────────────────────────────────
🔴 <critical issue>
🟡 <warning>
🟢 <healthy signal>
💡 Suggested: <next step>
───────────────────────────────────────────────────────
```

Only include lines that apply. Skip severity levels that have no signals. Always include at least one 💡 line.

### Deal diagnosis signals
| Severity | Condition | Message |
|----------|-----------|---------|
| 🔴 | `closingDate` is in the past | "Closing date passed X days ago — update or move to Closed Lost" |
| 🔴 | No `associatedContacts` | "No contacts linked — can't track communication" |
| 🟡 | No activity for > 14 days (from `updatedAt` or `latestActivityCreatedAt`) | "No activity in X days" |
| 🟡 | `products` list is empty | "No products attached" |
| 🟡 | `value` is 0 or null | "Deal value not set" |
| 🟡 | `closingDate` within 7 days but pipeline stage is first or second stage | "Closing soon but still in early stage" |
| 🟢 | Products attached | "X product(s) attached — total ₹Y" |
| 🟢 | Closing date is in the future | "Closing in X days" |

💡 suggestions for deals:
- Overdue closing → "Reschedule closing date or mark as Closed Lost"
- No products → "Add products to complete the deal"
- No contacts → "Link a contact to enable follow-ups"
- Idle → "Schedule a follow-up call or meeting"
- Closing soon in early stage → "Accelerate through pipeline stages"

### Lead diagnosis signals
| Severity | Condition | Message |
|----------|-----------|---------|
| 🔴 | No activity for > 30 days | "Stagnant for X days — at risk of going cold" |
| 🟡 | No associated contact or company | "No contact or company linked" |
| 🟡 | No products | "No products associated" |
| 🟡 | No owner assigned | "Unassigned — no owner set" |
| 🟢 | Active within 7 days | "Recently active" |

💡 suggestions for leads:
- Stagnant → "Follow up immediately or reassign to another owner"
- No contact → "Create or link a contact for this lead"
- Unassigned → "Assign to a sales rep"

### Task diagnosis signals
| Severity | Condition | Message |
|----------|-----------|---------|
| 🔴 | Due date is in the past | "Overdue by X days" |
| 🟡 | No associated entity | "Not linked to any lead, deal, or contact" |
| 🟢 | Due date is in the future | "Due in X days" |

💡 suggestions for tasks:
- Overdue → "Complete, reschedule, or reassign this task"
- No link → "Associate with a lead or deal for context"

---

## REPORT FORMATTING — apply whenever presenting 3+ entities or a summary

### Structure every report like this:
1. **TL;DR** — one sentence capturing the most important signal (e.g. "3 deals are overdue, totalling ₹8.4L at risk")
2. **Body** — table or grouped list (see below)
3. **Key Takeaways** — 3–5 bullets, the most actionable insights only

### Tables over lists
Use a markdown table whenever showing 3+ entities. Columns: the most relevant 4–5 fields only. Never dump all fields.

Example deals table:
| Deal | Stage | Value | Closing | Last Activity |
|------|-------|-------|---------|---------------|
| Acme Corp | Proposal | ₹2.5L | 3 days ago ⚠️ | 10 days ago |

### Grouping
For 5+ results, group by the most meaningful dimension:
- Deals → by pipeline stage or owner
- Leads → by source or owner
- Tasks → by due status (Overdue / Due soon / Upcoming)

### Number formatting
- Currency: use ₹ symbol with K/L/Cr suffixes (₹45K, ₹1.2L, ₹3.5Cr) — never raw numbers like 1200000
- Dates: show relative ("3 days ago", "in 2 weeks") with absolute in parentheses where precision matters
- Counts: "3 of 12 deals" not just "3"

### Language
- Use plain English, not field names (say "closing date" not `closingDate`, "owner" not `ownedBy`)
- Highlight risks with ⚠️, wins with ✅, neutral info without icons
- Never show raw IDs in report output — use names

### Key Takeaways block format
```
Key Takeaways:
• [Most urgent action]
• [Biggest risk or opportunity]
• [Notable pattern or trend]
```
"""

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

_base_instructions = (
    SYSTEM_INSTRUCTIONS + "\n\n" + DEAL_SYSTEM_INSTRUCTIONS + "\n\n" +
    COMPANY_SYSTEM_INSTRUCTIONS + "\n\n" + MEETING_SYSTEM_INSTRUCTIONS + "\n\n" +
    CALL_LOG_SYSTEM_INSTRUCTIONS + "\n\n" + QUOTATION_SYSTEM_INSTRUCTIONS + "\n\n" +
    DIAGNOSIS_AND_REPORTING_INSTRUCTIONS
)


def _build_instructions() -> str:
    """Build final instructions with entity label mapping injected once."""
    label_block = _format_entity_labels_for_instructions(_ENTITY_LABELS)
    return _base_instructions.replace("{ENTITY_LABEL_MAPPING}", label_block)


def _format_label_summary() -> str:
    """One-line summary of entity labels for tool description."""
    if not _ENTITY_LABELS:
        return ""
    parts = []
    for entity_type in sorted(_ENTITY_LABELS.keys()):
        label_data = _ENTITY_LABELS[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        parts.append(f'"{display_name}"/"{display_plural}"={std_type}')
    return ", ".join(parts)


def _update_tool_description(app: FastMCP) -> None:
    """Patch get_entity_labels tool description with live label data so Claude sees it in the tool list."""
    tool = app.local_provider._components.get("get_entity_labels")
    if not tool:
        return
    summary = _format_label_summary()
    if summary:
        tool.description = (
            f"REQUIRED: Call this ONCE per session before any other action.\n"
            f"THIS TENANT'S ENTITY NAMES: {summary}\n"
            f"Returns the full mapping. Use standard types (right of =) in all tool calls."
        )
        logger.info(f"Updated get_entity_labels tool description: {summary}")


# Maps entity type keys to tool name substrings for description patching
_ENTITY_TOOL_SUBSTRINGS: Dict[str, str] = {
    "CONTACT": "contact",
    "LEAD": "lead",
    "DEAL": "deal",
    "TASK": "task",
    "COMPANY": "company",
    "MEETING": "meeting",
    "CALL_LOG": "call_log",
}

# Stores original tool descriptions so refreshes don't stack the prefix
_ORIGINAL_TOOL_DESCRIPTIONS: Dict[str, str] = {}

_ENTITY_LABELS_RESOURCE_URI = "kylas://entity-labels"


def _patch_entity_tool_descriptions(app: FastMCP) -> None:
    """
    Prepend a one-line resource reference to each entity tool's description.
    Claude reads tool descriptions before deciding which tool to call; seeing the
    resource URI there prompts it to read kylas://entity-labels for name resolution.
    """
    if not _ENTITY_LABELS:
        return
    for entity_type, substring in _ENTITY_TOOL_SUBSTRINGS.items():
        label_data = _ENTITY_LABELS.get(entity_type)
        if not label_data:
            continue
        display_name = label_data.get("displayName", "")
        display_plural = label_data.get("displayNamePlural", "")
        std_type = entity_type.lower()
        prefix = (
            f'[Tenant entity name: "{display_name}" / "{display_plural}" = {std_type}. '
            f"If user uses a custom name, read resource `{_ENTITY_LABELS_RESOURCE_URI}` to resolve it.]\n"
        )
        for tool_name, tool in app.local_provider._components.items():
            if substring in tool_name.lower() and tool_name != "get_entity_labels":
                if tool_name not in _ORIGINAL_TOOL_DESCRIPTIONS:
                    _ORIGINAL_TOOL_DESCRIPTIONS[tool_name] = tool.description
                tool.description = prefix + _ORIGINAL_TOOL_DESCRIPTIONS[tool_name]
    logger.info("Patched entity tool descriptions with resource reference")


_entity_labels_lock = asyncio.Lock()


class _EnsureEntityLabelsMiddleware(Middleware):
    """Lazily load entity labels on the first tool call that arrives with a valid API key."""

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        if not _ENTITY_LABELS:
            async with _entity_labels_lock:
                # Double-check inside lock — another request may have loaded them first
                if not _ENTITY_LABELS:
                    await _load_entity_labels()
                    if _ENTITY_LABELS:
                        mcp._mcp_server.instructions = _build_instructions()
                        _update_tool_description(mcp)
                        _patch_entity_tool_descriptions(mcp)
                        logger.info("Lazily loaded entity labels on first request")
        return await call_next(context)


@asynccontextmanager
async def _app_lifespan(app: FastMCP):
    """Startup: attempt eager label load (works if KYLAS_API_KEY env var set), else labels load lazily on first request."""
    await _load_entity_labels()
    if _ENTITY_LABELS:
        app._mcp_server.instructions = _build_instructions()
        _update_tool_description(app)
        _patch_entity_tool_descriptions(app)
        logger.info("🚀 Updated MCP server instructions with entity labels")
    else:
        logger.info("Entity labels not loaded at startup — will load lazily on first request")
    refresh_task = asyncio.create_task(_label_refresh_loop(interval_seconds=1800))
    try:
        yield {}
    finally:
        refresh_task.cancel()
        try:
            await refresh_task
        except asyncio.CancelledError:
            pass
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


mcp = FastMCP("Kylas CRM", instructions=_base_instructions, lifespan=_app_lifespan, auth=auth_provider)
mcp.add_middleware(_EnsureEntityLabelsMiddleware())


# ---------------------------------------------------------------------------
# Tool 1: Get Lead Field Instructions (call FIRST)
# ---------------------------------------------------------------------------

def _format_field(field: Dict[str, Any], include_filterable: bool = False) -> List[str]:
    lines = []
    label = field.get("displayName") or field.get("label") or "Unknown"
    name = field.get("name", "")
    field_id = field.get("id", "")
    field_type = field.get("type", "UNKNOWN")
    is_standard = field.get("standard", False)
    is_required = field.get("required", False)
    filterable = field.get("filterable", False)
    prefix = "[STANDARD]" if is_standard else "[CUSTOM]"
    if is_standard:
        identifier = f"API Name: '{name}'"
    else:
        identifier = f"Field ID: '{field_id}', Internal Name for customFieldValues: '{name}'"
    required_marker = " *REQUIRED*" if is_required else ""
    filterable_marker = " [FILTERABLE]" if (include_filterable and filterable) else ""
    lines.append(f"{prefix} '{label}' ({identifier}) - Type: {field_type}{required_marker}{filterable_marker}")
    if field_type in ["PICK_LIST", "MULTI_PICKLIST"]:
        picklist = field.get("picklist") or {}
        # Deals use "picklistValues", Leads use "values"
        values = picklist.get("values") or picklist.get("picklistValues", [])
        if values:
            use_name = name in PICKLIST_FIELDS_USE_INTERNAL_NAME
            lines.append("  └─ Options (use internal name in search)" if use_name else "  └─ Options (use ID in search):")
            for val in values:
                if not isinstance(val, dict):
                    continue
                val_label = val.get("displayName") or val.get("label") or val.get("name") or "Unknown"
                val_id = val.get("id", "")
                val_name = val.get("name", "")
                if use_name and val_name:
                    lines.append(f"     • {val_label} (internal name: '{val_name}')")
                else:
                    lines.append(f"     • {val_label} (ID: {val_id})")
    return lines


async def _fetch_lead_fields() -> List[Dict[str, Any]]:
    """Fetch lead field metadata from Kylas API. Returns list of field dicts."""
    async with get_client() as client:
        response = await client.get(
            "/entities/lead/fields",
            params={"entityType": "lead", "custom-only": "false", "page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch lead fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def _get_custom_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom field ID (string) -> internal name (e.g. cfLeadCheck)."""
    fields = await _fetch_lead_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


def _get_filterable_fields_map(fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return map of field name -> {type, standard} for active+filterable fields only."""
    return {
        (f.get("name") or str(f.get("id", ""))): {"type": f.get("type", "TEXT_FIELD"), "standard": f.get("standard", False)}
        for f in fields
        if f.get("active", True) and f.get("filterable", False) and (f.get("name") or f.get("id") is not None)
    }


def _rule_type_for_value(field_type: str, field_name: str, value: Any) -> str:
    """Return jsonRule rule 'type' (string, long, or date) for the given field type and value."""
    if field_type in ("PICK_LIST", "MULTI_PICKLIST"):
        return "string" if field_name in PICKLIST_FIELDS_USE_INTERNAL_NAME else "long"
    if field_type == "NUMBER":
        return "double"
    # User look-up fields: createdBy, updatedBy, convertedBy, ownerId, importedBy — value is user ID (long)
    if field_type in ("LOOK_UP", "ENTITY_FIELDS", "MEETING_ORGANIZER"):
        return "long"
    # Date/datetime: standard and custom (e.g. cfDateField); value = single ISO string, [start,end], or null
    if field_type in ("DATETIME_PICKER", "DATE", "DATE_PICKER"):
        return "date"
    if field_type == "PARTICIPANTS_LOOKUP":
        return "participants_lookup"
    return "string"


def _build_search_json_rule(
    filters: List[Dict[str, Any]],
    filterable_map: Dict[str, Dict[str, Any]],
    default_timezone: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /search/lead. Returns (jsonRule, error_message).
    Each filter: { "field": "<name>", "operator": "<op>", "value": <val>, "type": "<FIELD_TYPE>" }.
    default_timezone: used for date/datetime rules when filter has no timeZone (e.g. from get_current_user).
    """
    tz_for_date = default_timezone or DEFAULT_TIMEZONE
    rules = []
    for i, f in enumerate(filters):
        field_name = f.get("field")
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
        # Convert operator symbols (>, <, >=, <=, !=, ==) to operator names
        operator = OPERATOR_SYMBOL_MAP.get(operator, operator)
        value = f.get("value")
        field_type_key = (f.get("type") or "TEXT_FIELD").strip().upper().replace(" ", "_")

        if not field_name:
            return {}, f"Filter #{i + 1}: missing 'field'."
        if field_name not in filterable_map:
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only [FILTERABLE] fields from get_lead_field_instructions."
        meta = filterable_map[field_name]
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        rule_type = _rule_type_for_value(api_type, field_name, value)
        if rule_type in ("long", "double") and value is not None and not isinstance(value, (int, float)):
            try:
                value = float(value) if rule_type == "double" else int(value)
            except (TypeError, ValueError):
                value = value
        # Date/datetime fields: convert date values from user's timezone to UTC
        # e.g. "11th Aug 00:00" in Asia/Calcutta → "10th Aug 18:30" UTC
        if rule_type == "date" and value is not None:
            filter_tz = f.get("timeZone") or tz_for_date
            value = _convert_date_value_to_utc(value, filter_tz)

        # Custom fields: API expects field path "customFieldValues.cfFruits" or "customFieldValues.cfDateField"; standard fields use field name only
        is_custom = not meta.get("standard", True)
        rule_field = f"customFieldValues.{field_name}" if is_custom else field_name

        rule = {
            "operator": operator,
            "id": field_name,
            "field": rule_field,
            "type": rule_type,
            "value": value,
            "relatedFieldIds": None,
        }
        # Pipeline/pipelineStage: API expects dependentFieldIds and relatedFieldIds for lead search
        if field_name == "pipeline":
            rule["dependentFieldIds"] = ["pipelineStage", ""
                                                          ""]
        elif field_name == "pipelineStage":
            rule["relatedFieldIds"] = ["pipeline"]
        # Date/datetime fields: API requires timeZone; use filter's timeZone or current user's (default_timezone) or fallback
        if rule_type == "date":
            rule["timeZone"] = f.get("timeZone") or tz_for_date
        rules.append(rule)

    return {"rules": rules, "condition": "AND", "valid": True}, None


async def get_lead_field_instructions_logic() -> str:
    fields = await _fetch_lead_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    lines = [
        "=" * 60,
        "KYLAS CRM - LEAD FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True))
    lines.extend(["", "=" * 60, "END OF CHEAT SHEET", "=" * 60])
    return "\n".join(lines)


@mcp.tool()
async def get_entity_labels() -> str:
    """
    Returns the mapping of this tenant's custom entity display names to standard CRM entity types.
    CALL THIS ONCE at the start of every session, before any other tool.
    This tenant uses custom names (e.g. "animals" instead of contacts, "cars" instead of deals).
    Without this, you will fail to recognize entity requests from users.
    After calling this, when the user mentions a custom name, map it to the standard type for all tool calls.
    """
    if not _ENTITY_LABELS:
        return "No custom entity labels configured for this tenant. Standard names apply: lead, contact, deal, task, company, meeting, call_log."
    lines = ["# Entity Label Mapping — Custom Names for This Tenant\n"]
    lines.append("When the user says one of the custom names below, use the corresponding STANDARD TYPE in all tool calls.\n")
    for entity_type in sorted(_ENTITY_LABELS.keys()):
        label_data = _ENTITY_LABELS[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(f'- User says "{display_name}" or "{display_plural}" → use standard type: "{std_type}"')
    lines.append('\n**Example:** User says "get animals" → map "animals" to "contact" → call search_contacts(...)')
    lines.append('**Example:** User says "show cars" → map "cars" to "deal" → call search_deals(...)')
    return "\n".join(lines)


@mcp.resource(
    "kylas://entity-labels",
    name="Entity Label Mapping",
    description=(
        "Tenant-specific entity name mapping. Read this to resolve custom entity names to standard CRM types. "
        "Example: 'animals' may map to 'contact', 'cars' may map to 'deal'. "
        "Always read this resource when the user refers to an entity by an unfamiliar name."
    ),
    mime_type="text/plain",
)
def entity_labels_resource() -> str:
    """Serve current entity label mapping as a readable resource."""
    if not _ENTITY_LABELS:
        return "No custom entity labels. Standard names apply: lead, contact, deal, task, company, meeting, call_log."
    lines = ["# Entity Label Mapping — Tenant-Specific Custom Names", ""]
    lines.append("When the user says a custom name, use the STANDARD TYPE (right of →) in all tool calls.")
    lines.append("")
    for entity_type in sorted(_ENTITY_LABELS.keys()):
        label_data = _ENTITY_LABELS[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(f'- "{display_name}" / "{display_plural}" → "{std_type}"')
    lines.extend([
        "",
        "Examples:",
        '  User says "get animals" → standard type is "contact" → call search_contacts(...)',
        '  User says "show cars"   → standard type is "deal"    → call search_deals(...)',
    ])
    return "\n".join(lines)


@mcp.tool()
async def get_lead_field_instructions() -> str:
    """
    Get all lead fields for the current tenant. CALL THIS FIRST before creating a lead.
    Returns a cheat sheet with API names (standard fields), Field IDs (custom fields), and Picklist Option IDs.
    Use this to build field_values for create_lead based on what the user wants—do not use static fields.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching lead field instructions")
        result = await get_lead_field_instructions_logic()
        return result
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_lead_field_instructions")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 1b: Get current user (timezone, recordActions, etc.) – for date/datetime handling
# ---------------------------------------------------------------------------

async def _fetch_current_user() -> Dict[str, Any]:
    """Fetch current user from GET /users/me. Returns full user object (timezone, recordActions, name, etc.)."""
    async with get_client() as client:
        response = await client.get("/users/me")
        return await handle_api_response(response, "Fetch current user")


@mcp.tool()
async def get_current_user() -> str:
    """
    Get the current authenticated user's profile from Kylas (GET /users/me).
    Call this whenever a date or datetime-related query is involved.
    Returns timezone (IANA, e.g. Asia/Calcutta), recordActions (call, email, sms, etc.), name, and other profile fields.
    - For filtering (search_leads, search_idle_leads): use the returned timezone as the timeZone in date/datetime filters; keep the user's date/datetime as-is (do not convert to UTC).
    - For create_lead: when the user provides a datetime in their own words (e.g. "11th Feb 2026 at 7:30 AM"), interpret it in this timezone, convert to UTC using parse_datetime_to_utc_iso, and send the UTC ISO string in field_values.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching current user (users/me)")
        user = await _fetch_current_user()
        tz = user.get("timezone") or "UTC"
        name = user.get("name") or f"{user.get('firstName', '')} {user.get('lastName', '')}".strip() or "—"
        lines = [
            "=" * 50,
            "CURRENT USER (GET /users/me)",
            "=" * 50,
            f"Name: {name}",
            f"Timezone: {tz}",
            "",
            "recordActions (permissions):",
        ]
        ra = user.get("recordActions") or {}
        for k, v in sorted(ra.items()):
            lines.append(f"  • {k}: {v}")
        lines.extend([
            "",
            "Use this timezone for:",
            "  - Date/datetime filters in search_leads: pass timeZone in each date filter; do not convert filter values to UTC.",
            "  - create_lead with datetime fields: convert user's local datetime to UTC with parse_datetime_to_utc_iso, then send UTC ISO in field_values.",
            "=" * 50,
        ])
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_current_user")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 2: Lookup Users (for createdBy, updatedBy, ownerId, importedBy, convertedBy filters)
# ---------------------------------------------------------------------------

async def lookup_users_logic(
    query: str, page: int = 0, size: int = 50, fetch_all_pages: bool = False
) -> str:
    """
    Call GET /users/lookup?q=<query> and return a formatted list of users (id, name).
    Use this when the user asks for leads by "created by X", "owner is Y", etc., to resolve X/Y to a user ID.
    If fetch_all_pages is True, request all pages and return all users in one response (cap at 500).
    """
    if not query or not str(query).strip():
        return "Error: query cannot be empty. Provide a name or search term (e.g. 'last' or 'firstName:last'), or use query 'name:' with return_all=True to list all users."
    q = str(query).strip()
    page_size = min(size, 50)
    content: List[Dict[str, Any]] = []
    total = 0
    total_pages = 1
    current_page = page
    max_users = 500 if fetch_all_pages else page_size

    async with get_client() as client:
        while True:
            response = await client.get(
                "/users/lookup",
                params={"q": q, "page": current_page, "size": page_size},
            )
            data = await handle_api_response(response, "User lookup")
            chunk = data.get("content", data.get("data", []))
            total = data.get("totalElements", data.get("total", len(chunk) + len(content)))
            total_pages = data.get("totalPages", 1)
            content.extend(chunk)
            if not fetch_all_pages or current_page >= total_pages - 1 or len(content) >= max_users or len(chunk) < page_size:
                break
            current_page += 1

    if not content:
        return f"No users found matching '{q}'."
    if fetch_all_pages:
        header = f"Found {len(content)} user(s)" + (f" matching '{q}'" if q != "name:" else "") + f" (total {total}, all returned in one list)"
    else:
        header = f"Found {len(content)} user(s) matching '{q}' (total {total}, page {page + 1} of {total_pages})"
    lines = [header, "-" * 50]
    for u in content:
        uid = u.get("id", "?")
        name = u.get("name", "—")
        lines.append(f"  • ID: {uid}  |  Name: {name}")
    lines.append("-" * 50)
    if len(content) > 1 and not fetch_all_pages:
        lines.append("More than one user matched. Ask the user which one they mean, then use that ID in search_leads (e.g. filter createdBy / ownerId equal to that ID).")
    elif len(content) == 1:
        lines.append(f"Use user ID {content[0].get('id')} in search_leads when filtering by created by / owner / etc.")
    return "\n".join(lines)


@mcp.tool()
async def lookup_users(
    query: str = "name:",
    page: int = 0,
    size: int = 50,
    return_all: bool = False,
) -> str:
    """
    Look up users by name, or list all users in the system.
    - Use return_all=True (with query "name:" or empty) to fetch all users in one response (all pages combined).
    - For name search: query in field:value form (e.g. "firstName:last", "name:Last"). If one user is found, use that ID in search_leads; if multiple, ask which one.
    query: Search string (e.g. "firstName:last", "name:Last"). Use "name:" or leave default to list all when return_all=True.
    page: 0-based page (default 0). Ignored when return_all=True.
    size: Page size, max 50 (default 50). Used per page when return_all=True.
    return_all: If True, fetch all pages and return every user in one response (cap 500).
    """
    try:
        _reset_api_call_count()
        q = (query or "name:").strip() or "name:"
        logger.info("User lookup: q=%s return_all=%s", q, return_all)
        return await lookup_users_logic(q, page, size, fetch_all_pages=return_all)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("lookup_users")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 3b: Lookup Products (for products filter on leads)
# ---------------------------------------------------------------------------

async def lookup_products_logic(query: str, page: int = 0, size: int = 50) -> str:
    """
    Call GET /products/lookup?q=<query> and return a formatted list of products (id, name).
    Use this when the user asks for leads by product name (e.g. "leads with product X") to resolve X to a product ID.
    """
    if not query or not str(query).strip():
        return "Error: query cannot be empty. Provide a product name or search term (e.g. 'name:Widget' or 'Widget')."
    q = str(query).strip()
    # If user passed plain text, treat as product name for API (name:value form)
    if ":" not in q:
        q = f"name:{q}"
    async with get_client() as client:
        response = await client.get(
            "/products/lookup",
            params={"q": q, "page": page, "size": min(size, 50)},
        )
        data = await handle_api_response(response, "Product lookup")
    content = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(content)))
    total_pages = data.get("totalPages", 1)
    if not content:
        return f"No products found matching '{q}'."
    lines = [f"Found {len(content)} product(s) matching '{q}' (total {total}, page {page + 1} of {total_pages})", "-" * 50]
    for p in content:
        pid = p.get("id", "?")
        name = p.get("name", p.get("displayName", "—"))
        lines.append(f"  • ID: {pid}  |  Name: {name}")
    lines.append("-" * 50)
    if total > 1:
        lines.append("More than one product matched. Ask the user which one they mean, then use that ID in search_entity (e.g. filter products equal to that ID).")
    else:
        lines.append(f"Use product ID {content[0].get('id')} in search_entity when filtering by product (e.g. {{\"field\": \"products\", \"operator\": \"equal\", \"value\": <id>}}). Works for both leads and deals.")
    return "\n".join(lines)


@mcp.tool()
async def lookup_products(query: str, page: int = 0, size: int = 50) -> str:
    """
    Look up products by name. Use this BEFORE filtering leads or deals by product when the user gives a product name.
    - If one product is found, use that product's ID in search_entity for leads or deals (e.g. {"field": "products", "operator": "equal", "value": <id>}).
    - If multiple products are found, ask the user which product they mean (list the options), then use the chosen product's ID.
    query: Search string. Use "name:<product_name>" (e.g. "name:Widget") or just the product name (e.g. "Widget"); the server will send name:value to the API.
    page: 0-based page (default 0).
    size: Max 50 (default 50).
    """
    try:
        _reset_api_call_count()
        logger.info("Product lookup: q=%s", query)
        return await lookup_products_logic(query, page, size)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("lookup_products")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 3c: Lookup Pipelines (for pipeline + stage filters on leads)
# ---------------------------------------------------------------------------

async def lookup_pipelines_logic(
    query: str = "",
    entity_type: str = "LEAD",
    page: int = 0,
    size: int = 50,
) -> str:
    """
    Call GET /pipelines/lookup?entityType=<entity_type>&q=<query> and return a formatted list of pipelines (id, name).
    Use when the user asks for leads by stage (e.g. open/closed/won) but pipeline is not specified; then ask user to select a pipeline.
    """
    q = str(query).strip() if query else ""
    if ":" not in q and q:
        q = f"name:{q}"
    # Empty q: some APIs return all pipelines when q=name:
    if not q:
        q = "name:"
    async with get_client() as client:
        response = await client.get(
            "/pipelines/lookup",
            params={"entityType": entity_type, "q": q, "page": page, "size": min(size, 50)},
        )
        data = await handle_api_response(response, "Pipeline lookup")
    content = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(content)))
    total_pages = data.get("totalPages", 1)
    if not content:
        return f"No pipelines found for entity {entity_type}" + (f" matching '{q}'." if q else ".")
    lines = [
        f"Found {len(content)} pipeline(s) (entityType={entity_type}, total {total}, page {page + 1} of {total_pages})",
        "-" * 50,
    ]
    for p in content:
        pid = p.get("id", "?")
        name = p.get("name", p.get("displayName", "—"))
        lines.append(f"  • ID: {pid}  |  Name: {name}")
    lines.append("-" * 50)
    lines.append("Ask the user to confirm which pipeline to use (list id and name). Do NOT call get_pipeline_stages until the user has confirmed. After confirmation, call get_pipeline_stages with that pipeline ID only, then search or update with pipeline + pipelineStage filters.")
    return "\n".join(lines)


@mcp.tool()
async def lookup_pipelines(
    query: str = "",
    entity_type: str = "LEAD",
    page: int = 0,
    size: int = 50,
) -> str:
    """
    Look up pipelines by name for leads or deals. Use when the user asks for items by stage but does not specify which pipeline.

    **For Leads:** lookup_pipelines(query="", entity_type="LEAD")
    **For Deals:** lookup_pipelines(query="", entity_type="DEAL")

    Workflow:
    - Call this first; do NOT call get_pipeline_stages until after the user confirms the pipeline.
    - Present the pipeline(s) (id and name) and ask the user which pipeline they mean. If only one pipeline is found, still ask for confirmation.
    - Only after the user confirms, call get_pipeline_stages with that pipeline ID to get stages, then search_leads or update_deal/update_lead.

    query: Search string. Use "name:<pipeline_name>" or just the pipeline name; empty string returns all pipelines for the entity.
    entity_type: Entity type - "LEAD" (default) or "DEAL".
    page: 0-based page (default 0).
    size: Max 50 (default 50).
    """
    try:
        _reset_api_call_count()
        logger.info("Pipeline lookup: entityType=%s q=%s", entity_type, query)
        return await lookup_pipelines_logic(query, entity_type, page, size)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("lookup_pipelines")
        return f"Unexpected error: {str(e)}"


async def get_pipeline_stages_logic(pipeline_id: int) -> str:
    """
    Call POST /pipelines/summary with jsonRule filtering by pipeline id(s). Returns pipeline name and list of stages (id, name, forecastingType).
    Use after the user has selected a pipeline; then map user intent (open/closed/won/lost) to stage id(s) and call search_leads.
    """
    payload = {
        "jsonRule": {
            "condition": "AND",
            "rules": [{"operator": "in", "id": "id", "field": "id", "type": "long", "value": [pipeline_id]}],
            "valid": True,
        }
    }
    async with get_client() as client:
        response = await client.post("/pipelines/summary", json=payload)
        data = await handle_api_response(response, "Pipeline summary")
    # Response is array of {id, name, stages: [{id, name, position, forecastingType}]}
    pipelines = data if isinstance(data, list) else data.get("content", data.get("data", []))
    if not pipelines:
        return f"No pipeline found with ID {pipeline_id}."
    lines = []
    for pl in pipelines:
        pl_id = pl.get("id", "?")
        pl_name = pl.get("name", "—")
        lines.append(f"Pipeline: {pl_name} (ID: {pl_id})")
        stages = pl.get("stages", [])
        if not stages:
            lines.append("  (no stages)")
        else:
            for s in stages:
                sid = s.get("id", "?")
                sname = s.get("name", "—")
                ftype = s.get("forecastingType", "")
                lines.append(f"  • Stage ID: {sid}  |  Name: {sname}  |  forecastingType: {ftype}")
        lines.append("")
    lines.append("Map user intent to stage: 'open' → OPEN; 'won' → CLOSED_WON; 'lost' → CLOSED_LOST; 'closed unqualified' → CLOSED_UNQUALIFIED. If multiple stages match (e.g. several OPEN stages), ask the user which stage they mean, then use that stage ID in search_leads with pipeline and pipelineStage filters.")
    return "\n".join(lines).strip()


@mcp.tool()
async def get_pipeline_stages(pipeline_id: int) -> str:
    """
    Get stages for a pipeline. Call this only after the user has confirmed which pipeline to use (from lookup_pipelines). Do not call before pipeline confirmation.
    Returns pipeline name and list of stages for that pipeline only, with id, name, and forecastingType (OPEN, CLOSED_WON, CLOSED_LOST, CLOSED_UNQUALIFIED).
    Use the stage IDs in search_leads: filters [{"field": "pipeline", "operator": "equal", "value": pipeline_id}, {"field": "pipelineStage", "operator": "equal", "value": stage_id}].
    If the user said "open leads" or "closed leads" and more than one stage has the same forecastingType, ask which stage they mean.
    pipeline_id: The pipeline ID (from lookup_pipelines).
    """
    try:
        pipeline_id = int(pipeline_id)
    except (TypeError, ValueError):
        return "Error: pipeline_id must be a number."
    try:
        _reset_api_call_count()
        logger.info("Pipeline stages: pipeline_id=%s", pipeline_id)
        return await get_pipeline_stages_logic(pipeline_id)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_pipeline_stages")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 3d: Get pipeline details (GET /pipelines/{id}) – stages + lost/unqualified reasons
# ---------------------------------------------------------------------------

async def get_pipeline_details_logic(pipeline_id: int) -> str:
    """
    Call GET /pipelines/{id}. Returns pipeline name, stages (id, name, forecastingType),
    sequentialStageFlow flag, unqualifiedReasons (for Closed Unqualified), and lostReasons (for Closed Lost).
    Use when moving a lead to Closed Lost or Closed Unqualified: get reasons, ask the user to pick one,
    then update_lead with pipelineStageReason set to that exact string.
    """
    pipeline_id = int(pipeline_id)
    data = await _get_pipeline_details_raw(pipeline_id)
    name = data.get("name", "—")
    sequential = data.get("sequentialStageFlow", False)
    lines = [
        f"Pipeline: {name} (ID: {pipeline_id})",
        f"Sequential Stage Flow: {'YES — stages must be moved one at a time in order' if sequential else 'NO — any stage can be targeted directly'}",
        "",
        "Stages (ordered by position):",
    ]
    for s in sorted(data.get("stages", []), key=lambda x: x.get("position", 0)):
        sid = s.get("id", "?")
        sname = s.get("name", "—")
        ftype = s.get("forecastingType", "")
        pos = s.get("position", "?")
        lines.append(f"  • Position {pos} | Stage ID: {sid}  |  Name: {sname}  |  forecastingType: {ftype}")
    unq = data.get("unqualifiedReasons") or []
    lost = data.get("lostReasons") or []
    lines.extend([
        "",
        "Closed Unqualified reasons (use exact string as pipelineStageReason when moving to Closed Unqualified):",
    ])
    if unq:
        for r in unq:
            lines.append(f"  • \"{r}\"")
    else:
        lines.append("  (none configured)")
    lines.extend([
        "",
        "Closed Lost reasons (use exact string as pipelineStageReason when moving to Closed Lost):",
    ])
    if lost:
        for r in lost:
            lines.append(f"  • \"{r}\"")
    else:
        lines.append("  (none configured)")
    lines.append("")
    lines.append("When updating lead to Closed Lost or Closed Unqualified, ask the user to pick one reason from the list above, then call update_lead with pipelineStageReason set to that exact string.")
    return "\n".join(lines)


@mcp.tool()
async def get_pipeline_details(pipeline_id: int) -> str:
    """
    Get full pipeline details by ID (GET /pipelines/{id}): stages plus unqualifiedReasons and lostReasons.
    Call this when moving a lead to Closed Lost or Closed Unqualified. Present the relevant reasons list to the user,
    ask them to pick one, then call update_lead with pipelineStageReason set to that exact string (e.g. "No followup", "Booked with competitor").
    pipeline_id: The pipeline ID (from the lead's current pipeline or from lookup_pipelines).
    """
    try:
        pipeline_id = int(pipeline_id)
    except (TypeError, ValueError):
        return "Error: pipeline_id must be a number."
    try:
        _reset_api_call_count()
        logger.info("Pipeline details: pipeline_id=%s", pipeline_id)
        return await get_pipeline_details_logic(pipeline_id)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_pipeline_details")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 3a: Parse datetime in user timezone to UTC ISO (for create_lead datetime fields)
# ---------------------------------------------------------------------------

def parse_datetime_to_utc_iso(local_datetime: str, timezone: str) -> str:
    """
    Parse a datetime string as given in the user's local timezone and return UTC ISO string for the Kylas API.
    Use when creating a lead with a date/datetime field: the user says e.g. "11th Feb 2026 at 7:30 AM" in their timezone;
    call get_current_user to get timezone, then call this with (user's datetime string, user's timezone) and put the result in field_values.
    """
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    dt = dateutil_parser.parse(local_datetime)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    utc_dt = dt.astimezone(ZoneInfo("UTC"))
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


@mcp.tool()
def parse_datetime_to_utc_iso_tool(local_datetime: str, timezone: str) -> str:
    """
    Parse a datetime string in the user's timezone and return UTC ISO string for the Kylas API.
    Call get_current_user first to get the user's timezone. Use the returned string in create_lead field_values for date/datetime fields.
    Example: user says "create lead with follow-up 11th Feb 2026 at 7:30 AM" → get_current_user → timezone Asia/Calcutta → parse_datetime_to_utc_iso_tool("11 Feb 2026 7:30 AM", "Asia/Calcutta") → use result in field_values.
    local_datetime: Datetime as the user said it (e.g. "11 Feb 2026 7:30 AM", "11th Feb 2026 at 7:30 am").
    timezone: IANA timezone from get_current_user (e.g. Asia/Calcutta).
    """
    try:
        return parse_datetime_to_utc_iso(local_datetime, timezone)
    except Exception as e:
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Tool 4: Create Lead (single tool, dynamic field_values)
# ---------------------------------------------------------------------------

# Non-standard aliases not recognized as ISO region codes by the phonenumbers library.
_COUNTRY_CODE_ALIASES: Dict[str, str] = {
    "UK": "GB",
    "USA": "US",
    "INDIA": "IN",
}


def _normalize_country_code(code: Optional[str]) -> str:
    """Normalize user-provided country code/dial prefix to a Kylas 2-letter ISO region code.

    Accepts:
      - Dial prefixes: "+91" → "IN", "+1" → "US", "+44" → "GB", all 190+ countries
      - ISO 3166-1 alpha-2 codes: "IN", "US", "GB", etc. (validated via phonenumbers)
      - Common aliases: "UK" → "GB", "USA" → "US", "INDIA" → "IN"

    Returns empty string if the code is absent or unrecognised (caller enforces presence when phone given).
    """
    if not code or not str(code).strip():
        return ""
    raw = str(code).strip()

    # Dial code prefix e.g. "+91", "+1", "+44"
    if raw.startswith("+"):
        try:
            calling_code = int(raw[1:])
            region = phonenumbers.region_code_for_country_code(calling_code)
            if region and region != "ZZ":
                return region
        except (ValueError, Exception):
            pass

    upper = raw.upper()

    # Non-standard aliases (UK, USA, INDIA, …)
    if upper in _COUNTRY_CODE_ALIASES:
        return _COUNTRY_CODE_ALIASES[upper]

    # Validate 2-letter ISO region code via phonenumbers (returns 0 for unknown regions)
    if phonenumbers.country_code_for_region(upper) != 0:
        return upper

    return ""


def _ensure_single_primary(entries: List[Dict[str, Any]], allowed_types: List[str], default_type: str) -> List[Dict[str, Any]]:
    """Ensure exactly one entry has primary=True. Use first entry marked primary by user, else first entry. Types restricted to allowed_types."""
    if not entries or not isinstance(entries, list):
        return entries
    result = []
    for e in entries:
        if not e or not isinstance(e, dict):
            continue
        entry = dict(e)
        t = (entry.get("type") or default_type).upper()
        entry["type"] = t if t in allowed_types else default_type
        result.append(entry)
    primary_idx = 0
    for i, entry in enumerate(result):
        if entry.get("primary"):
            primary_idx = i
            break
    for i, entry in enumerate(result):
        entry["primary"] = i == primary_idx
    return result


EMAIL_TYPES = ["OFFICE", "PERSONAL"]
PHONE_TYPES = ["MOBILE", "WORK", "HOME", "PERSONAL"]

# Kylas error code returned when trying to skip a stage on a pipeline with sequentialStageFlow=true
STAGE_LOCK_ERROR_CODE = "01001086"


def _is_stage_lock_error(error: KylasAPIError) -> bool:
    """Return True if this error is the Kylas sequential-stage-flow lock (code 01001086)."""
    if not error.response_body:
        return False
    try:
        body = json.loads(error.response_body)
        return body.get("code") == STAGE_LOCK_ERROR_CODE
    except (json.JSONDecodeError, AttributeError, TypeError):
        return False


async def _get_pipeline_details_raw(pipeline_id: int) -> Dict[str, Any]:
    """Fetch raw pipeline details dict from GET /pipelines/{id}."""
    async with get_client() as client:
        response = await client.get(f"/pipelines/{int(pipeline_id)}")
        return await handle_api_response(response, "Get pipeline details")


async def _advance_deal_to_stage_sequentially(
    deal_id: int,
    stages: list,
    current_stage_id: int,
    target_stage_id: int,
    base_deal: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Move deal through pipeline stages one step at a time (required when sequentialStageFlow=true).
    stages: list of stage dicts ordered by position (each has id, forecastingType).
    Advances from current_stage_id → target_stage_id inclusive, returning the final deal state.
    """
    stage_ids = [s["id"] for s in stages]
    if current_stage_id not in stage_ids:
        raise KylasAPIError(f"Current stage {current_stage_id} not found in pipeline stages.")
    if target_stage_id not in stage_ids:
        raise KylasAPIError(f"Target stage {target_stage_id} not found in pipeline stages.")

    current_idx = stage_ids.index(current_stage_id)
    target_idx = stage_ids.index(target_stage_id)

    if target_idx <= current_idx:
        raise KylasAPIError(
            f"Target stage (position {target_idx + 1}) must be after current stage (position {current_idx + 1}) "
            "for sequential advancement."
        )

    result = base_deal
    async with get_client() as client:
        for idx in range(current_idx + 1, target_idx + 1):
            stage = stages[idx]
            stage_id = stage["id"]
            merged = dict(result)
            if isinstance(merged.get("pipeline"), dict):
                merged["pipeline"] = dict(merged["pipeline"])
                merged["pipeline"]["stage"] = dict(merged["pipeline"].get("stage") or {})
                merged["pipeline"]["stage"]["id"] = stage_id
                forecast_type = stage.get("forecastingType")
                if forecast_type:
                    merged["forecastingType"] = forecast_type
            response = await client.put(f"/deals/{deal_id}", json=merged)
            result = await handle_api_response(response, f"Advance deal {deal_id} to stage {stage_id}")
            logger.info("Deal %s advanced to stage %s (%s)", deal_id, stage_id, stage.get("name", ""))

    return result


def _normalize_field_values(
    field_values: Dict[str, Any],
    custom_field_id_to_name: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Build Kylas create-lead payload from dynamic field_values.
    - Custom fields (numeric keys or in customFieldValues) → customFieldValues with INTERNAL NAME as key (never ID).
    - Explicit "customFieldValues" dict → merged; keys must be internal names (e.g. cfLeadCheck).
    - "email" string → emails array (type OFFICE, primary true). One email must be primary.
    - "phone" / "phoneNumber" + "phone_country_code" (required when phone given) + "phone_type" (required when phone given; one of MOBILE|WORK|HOME|PERSONAL) → phoneNumbers array. Caller must ask user for country/dial code AND phone type when either is missing; do not assume or infer. One phone must be primary.
    - emails/phoneNumbers arrays: allowed types email OFFICE|PERSONAL, phone MOBILE|WORK|HOME|PERSONAL; exactly one primary (first if unspecified).
    - Rest → top-level payload (standard fields)
    """
    payload: Dict[str, Any] = {}
    custom: Dict[str, Any] = {}
    fv = dict(field_values)
    id_to_name = custom_field_id_to_name or {}

    phone_country_raw = fv.pop("phone_country_code", None)
    phone_country = _normalize_country_code(phone_country_raw)
    phone_type_raw = fv.pop("phone_type", None)
    phone_type = phone_type_raw.strip().upper() if isinstance(phone_type_raw, str) else None
    if phone_type and phone_type not in PHONE_TYPES:
        raise ValueError(
            f"Invalid phone type '{phone_type}'. Must be one of: {', '.join(PHONE_TYPES)}."
        )
    has_phone_data = (
        fv.get("phone") or fv.get("phoneNumber")
        or (isinstance(fv.get("phoneNumbers"), list) and len(fv["phoneNumbers"]) > 0)
    )
    # Require explicit phone_country_code whenever any phone number is present (do not assume India).
    # This applies even if phoneNumbers array already has "code" on each entry—caller must pass
    # phone_country_code at top level so we know the user was asked, not assumed.
    if has_phone_data and not phone_country:
        raise ValueError(
            "Phone number(s) were provided but country/dial code was not. "
            "Ask the user which country and dial code to use (e.g. India: IN or +91, US: US or +1) and include 'phone_country_code' in field_values."
        )

    # Explicit customFieldValues: merge into custom (keys must be internal names, e.g. cfLeadCheck)
    if "customFieldValues" in fv:
        cf = fv.pop("customFieldValues")
        if isinstance(cf, dict):
            for k, v in cf.items():
                if v is not None:
                    custom[str(k)] = v

    for key, value in fv.items():
        if value is None:
            continue
        # Custom field: key is numeric string (Field ID) → use internal name in customFieldValues
        if str(key).isdigit():
            custom_key = id_to_name.get(str(key), str(key))
            custom[custom_key] = value
            continue
        # Normalize single email string to Kylas emails array
        if key == "email" and isinstance(value, str):
            payload["emails"] = _ensure_single_primary(
                [{"type": "OFFICE", "value": value.strip(), "primary": True}],
                EMAIL_TYPES,
                "OFFICE",
            )
            continue
        # Normalize single phone string to Kylas phoneNumbers array (code = 2-letter; required when phone given)
        if key in ("phone", "phoneNumber") and isinstance(value, str):
            if not phone_country:
                raise ValueError(
                    "Phone number was provided but country/dial code was not. "
                    "Ask the user which country and dial code to use (e.g. India: IN or +91, US: US or +1) and include 'phone_country_code' in field_values."
                )
            if not phone_type:
                raise ValueError(
                    "Phone number was provided but type was not specified. "
                    "Ask the user whether this number is MOBILE, WORK, HOME, or PERSONAL and include 'phone_type' in field_values."
                )
            payload["phoneNumbers"] = _ensure_single_primary(
                [{"type": phone_type, "code": phone_country, "value": value.strip(), "primary": True}],
                PHONE_TYPES,
                "MOBILE",
            )
            continue
        # Already in API shape: ensure single primary and allowed types
        if key == "emails":
            payload["emails"] = _ensure_single_primary(
                value if isinstance(value, list) else [],
                EMAIL_TYPES,
                "OFFICE",
            )
            continue
        if key == "phoneNumbers":
            # Normalize code to 2-letter for each entry; use phone_country when entry missing code (already validated above)
            raw_phones = value if isinstance(value, list) else []
            phones = []
            for p in raw_phones:
                if not isinstance(p, dict):
                    continue
                entry = dict(p)
                if "code" not in entry or not entry.get("code"):
                    entry["code"] = phone_country
                elif len(str(entry["code"])) > 2:
                    entry["code"] = _normalize_country_code(entry["code"]) or entry["code"]
                phones.append(entry)
            payload["phoneNumbers"] = _ensure_single_primary(phones, PHONE_TYPES, "MOBILE")
            continue
        # All other standard fields at top level
        payload[key] = value

    if custom:
        payload["customFieldValues"] = custom
    return payload


async def create_lead_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a lead with the given dynamic field_values (Kylas API payload shape)."""
    fv = dict(field_values)
    # Resolve custom field IDs to internal names so customFieldValues uses names, not IDs
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    logger.info("📝 Creating lead with fields: %s", list(payload.keys()))
    async with get_client() as client:
        response = await client.post("/leads", json=payload)
        result = await handle_api_response(response, "Create lead")
        logger.info("✅ Lead created with ID: %s", result.get("id"))
        return result


# ---------------------------------------------------------------------------
# Tool 4b: Update Lead (PUT /leads/{id})
# ---------------------------------------------------------------------------

async def update_lead_logic(lead_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the lead first, merge field_values into it, then PUT the full body. No partial update."""
    lead_id = int(lead_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values produced an empty payload.")
    logger.info("🔄 Updating lead %s with fields: %s", lead_id, list(payload.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/leads/{lead_id}")
        existing = await handle_api_response(get_response, "Get lead")
        merged = dict(existing)
        for key, value in payload.items():
            if key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            elif key == "pipelineStage" and isinstance(value, (int, str)):
                # Special handling: pipelineStage should update the existing pipeline's stage ID
                # Also update forecastingType to match the stage's forecastingType
                stage_id = int(value)
                if isinstance(merged.get("pipeline"), dict):
                    pipeline_id = merged["pipeline"].get("id")
                    if not isinstance(merged["pipeline"].get("stage"), dict):
                        merged["pipeline"]["stage"] = {}
                    merged["pipeline"]["stage"]["id"] = stage_id

                    # Fetch pipeline details to get the correct forecastingType for this stage
                    try:
                        if pipeline_id:
                            pipeline_details = await get_pipeline_details_logic(pipeline_id)
                            # Find the stage in the pipeline details and get its forecastingType
                            if isinstance(pipeline_details, dict):
                                stages = pipeline_details.get("stages", [])
                                for stage in stages:
                                    if stage.get("id") == stage_id:
                                        forecast_type = stage.get("forecastingType")
                                        if forecast_type:
                                            merged["forecastingType"] = forecast_type
                                        break
                    except Exception as e:
                        logger.warning("Could not fetch forecastingType for stage %s: %s", stage_id, e)
                        # Continue without updating forecastingType; user can pass it explicitly if needed
                else:
                    # If no existing pipeline, we can't update stage
                    logger.warning("Trying to set pipelineStage but lead has no pipeline object")
                    raise KylasAPIError("Lead has no pipeline; cannot set stage. Use move_lead_to_stage instead.")
            else:
                merged[key] = value
        response = await client.put(f"/leads/{lead_id}", json=merged)
        result = await handle_api_response(response, "Update lead")
        logger.info("✅ Lead %s updated", lead_id)
        return result


# ---------------------------------------------------------------------------
# Tool 4c: Get lead by ID (full details)
# ---------------------------------------------------------------------------

async def get_lead_logic(lead_id: int) -> Dict[str, Any]:
    """Fetch a single lead by ID (GET /leads/{id}). Returns full lead object."""
    lead_id = int(lead_id)
    async with get_client() as client:
        response = await client.get(f"/leads/{lead_id}")
        return await handle_api_response(response, "Get lead")


def _format_lead_for_display(lead: Dict[str, Any]) -> str:
    """Format a lead object into a readable multi-line string."""
    lines = ["=" * 60, "LEAD DETAILS", "=" * 60]
    lines.append(f"ID: {lead.get('id', '—')}")
    lines.append(f"First Name: {lead.get('firstName', '—')}")
    lines.append(f"Last Name: {lead.get('lastName', '—')}")
    lines.append(f"Company Name: {lead.get('companyName') or '—'}")
    # Emails
    emails = lead.get("emails") or []
    if emails:
        for e in emails:
            val = e.get("value", "")
            typ = e.get("type", "")
            prim = " (primary)" if e.get("primary") else ""
            lines.append(f"Email ({typ}): {val}{prim}")
    else:
        lines.append("Email: —")
    # Phones
    phones = lead.get("phoneNumbers") or []
    if phones:
        for p in phones:
            code = p.get("code", "")
            val = p.get("value", "")
            typ = p.get("type", "")
            prim = " (primary)" if p.get("primary") else ""
            lines.append(f"Phone ({typ}): +{code} {val}{prim}")
    else:
        lines.append("Phone: —")
    # Pipeline / Stage
    pipeline = lead.get("pipeline") or {}
    if isinstance(pipeline, dict):
        pl_name = pipeline.get("name", "—")
        stage = pipeline.get("stage") or {}
        stage_name = stage.get("name", "—") if isinstance(stage, dict) else "—"
        lines.append(f"Pipeline: {pl_name}")
        lines.append(f"Stage: {stage_name}")
    else:
        lines.append(f"Pipeline: {pipeline}")
    lines.append(f"Pipeline Stage Reason: {lead.get('pipelineStageReason') or '—'}")
    lines.append(f"Owner ID: {lead.get('ownerId', '—')}")
    lines.append(f"Created At: {lead.get('createdAt', '—')}")
    lines.append(f"Updated At: {lead.get('updatedAt', '—')}")
    # Custom fields
    custom = lead.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    # Other common fields
    for key in ("address", "city", "state", "zipcode", "country", "salutation", "leadSource", "companyWebsite", "facebook", "twitter", "linkedIn"):
        val = lead.get(key)
        if val is not None and val != "":
            lines.append(f"{key}: {val}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_lead(lead_id: int) -> str:
    """
    Get full details of a lead by ID (GET /leads/{id}). Use when the user asks for complete lead info, lead details, or to view a specific lead.
    lead_id: The lead ID (e.g. from search_leads or search_leads_by_term results).
    """
    try:
        _reset_api_call_count()
        lead = await get_lead_logic(lead_id)
        return _format_lead_for_display(lead)
    except KylasAPIError as e:
        return f"✗ Failed to get lead: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_lead")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 5: Search / Filter Leads
# ---------------------------------------------------------------------------

def _extract_primary_email(emails: Any) -> str:
    if not emails or not isinstance(emails, list):
        return "-"
    for e in emails:
        if e and e.get("primary"):
            return e.get("value", "-")
    return emails[0].get("value", "-") if emails and emails[0] else "-"


def _extract_primary_phone(phones: Any) -> str:
    if not phones or not isinstance(phones, list):
        return "-"
    for p in phones:
        if p and p.get("primary"):
            return f"{p.get('code', '')} {p.get('value', '')}".strip() or "-"
    if phones and phones[0]:
        return f"{phones[0].get('code', '')} {phones[0].get('value', '')}".strip() or "-"
    return "-"


async def search_leads_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search leads with jsonRule; only filterable fields allowed. Uses current user timezone for date/datetime filters when timeZone is not provided."""
    fields_list = await _fetch_lead_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable lead fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {
        "fields": ["id", "firstName", "lastName", "emails", "phoneNumbers", "ownerId", "companyName", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching leads with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/search/lead", params=params, json=payload)
        data = await handle_api_response(response, "Search leads")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No leads found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} lead(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for lead in results:
        lid = lead.get("id", "?")
        fn = lead.get("firstName") or ""
        ln = lead.get("lastName") or ""
        name = f"{fn} {ln}".strip() or "—"
        email = _extract_primary_email(lead.get("emails"))
        phone = _extract_primary_phone(lead.get("phoneNumbers"))
        lines.append(f"• ID: {lid} | Name: {name} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_leads(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter leads. Only fields marked [FILTERABLE] in get_lead_field_instructions can be used.
    Call get_lead_field_instructions first to get filterable fields and their types.

    filters: List of filter objects. Each must have:
      - field (str): Field internal/API name (e.g. firstName, country, source, createdAt).
      - operator (str): One of the allowed operators for that field type (e.g. equal, contains, greater).
      - value: Value to compare. For PICK_LIST/MULTI_PICKLIST use Option ID (number), except
        requirementCurrency, companyBusinessType, country, timezone, companyIndustry — use internal name (string).
        For date/datetime (incl. custom e.g. cfDateField): value null for today/is_null/is_not_null; single ISO string
        for greater/greater_or_equal/less/less_or_equal e.g. "2026-02-02T18:30:00.000Z"; for between use [startISO, endISO].
      - timeZone (str, optional): For date/datetime filters only; default from server or env.
      - type (str, optional): Field type from cheat sheet. If omitted, inferred from schema.
    For user look-up fields (createdBy, updatedBy, convertedBy, ownerId, importedBy): value must be user ID (number). Call lookup_users first.
    For the products field: value must be product ID (number). Call lookup_products first; if multiple matches, ask which product, then use that ID here.
    For pipeline / pipelineStage (e.g. open leads, closed leads): call lookup_pipelines first, ask the user to confirm which pipeline, then call get_pipeline_stages for that pipeline only; if stage is ambiguous ask which stage, then use pipeline + pipelineStage filters here.
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).

    Operators by type (examples): TEXT_FIELD: equal, contains, is_empty. NUMBER: equal, greater, between, is_null. PICK_LIST: equal, in, is_null. DATETIME_PICKER: today, yesterday, between, is_not_null, greater, less, current_week, etc.
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty. Provide at least one filter with field, operator, and value."
        return await search_leads_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_leads")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Tool 5b: Search leads by term (multi-field search)
# ---------------------------------------------------------------------------

def _multi_field_json_rule(search_term: str) -> Dict[str, Any]:
    """Build jsonRule for POST /search/lead multi-field search (search across firstName, lastName, companyName, etc.)."""
    return {
        "rules": [
            {
                "id": "multi_field",
                "field": "multi_field",
                "type": "multi_field",
                "input": "multi_field",
                "operator": "multi_field",
                "value": search_term.strip(),
            }
        ],
        "condition": "AND",
        "valid": True,
    }


# ---------------------------------------------------------------------------
# Tool 6: Search idle / stagnant leads (no activity for N days)
# ---------------------------------------------------------------------------

async def search_idle_leads_logic(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Find leads with no activity for at least `days` days.
    Uses last-activity = max(updatedAt, latestActivityCreatedAt); a lead is idle when both
    updatedAt and latestActivityCreatedAt are on or before (now - days).
    If time_zone is not provided, uses current user's timezone from GET /users/me.
    """
    if time_zone:
        tz = time_zone
    else:
        try:
            user = await _fetch_current_user()
            tz = user.get("timezone") or DEFAULT_TIMEZONE
        except Exception:
            tz = DEFAULT_TIMEZONE
    threshold_iso = _threshold_iso_days_ago(days, tz)
    base = {"operator": "less_or_equal", "value": threshold_iso, "timeZone": tz}
    fields_list = await _fetch_lead_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    filters = []
    for name in ("updatedAt", "latestActivityCreatedAt"):
        if name in filterable_map:
            filters.append({"field": name, **base})
    if not filters:
        return "Error: Neither 'updatedAt' nor 'latestActivityCreatedAt' is filterable for this tenant. Check get_lead_field_instructions."
    return await search_leads_logic(filters, page=page, size=size, sort=sort)


# ---------------------------------------------------------------------------
# Contact Entity Support (similar to Lead but no pipeline/stage)
# ---------------------------------------------------------------------------

async def _fetch_contact_fields() -> List[Dict[str, Any]]:
    """Fetch contact field metadata from Kylas API."""
    async with get_client() as client:
        response = await client.get(
            "/entities/contact/fields",
            params={"entityType": "contact", "custom-only": "false", "page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch contact fields")
        if isinstance(data, list):
            fields = data
        else:
            fields = data.get("data", data.get("content", []))
        return [f for f in fields if f.get("active", True)]


async def _get_custom_contact_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom contact field ID -> internal name."""
    fields = await _fetch_contact_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


async def create_contact_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a contact with the given dynamic field_values."""
    fv = dict(field_values)
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_custom_contact_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    logger.info("Creating contact with fields: %s", list(payload.keys()))
    async with get_client() as client:
        response = await client.post("/contacts", json=payload)
        result = await handle_api_response(response, "Create contact")
        logger.info("Contact created with ID: %s", result.get("id"))
        return result


async def update_contact_logic(contact_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the contact first, merge field_values into it, then PUT the full body."""
    contact_id = int(contact_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_custom_contact_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values produced an empty payload.")
    logger.info("Updating contact %s with fields: %s", contact_id, list(payload.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/contacts/{contact_id}")
        existing = await handle_api_response(get_response, "Get contact")
        merged = dict(existing)
        for key, value in payload.items():
            if key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            else:
                merged[key] = value
        response = await client.put(f"/contacts/{contact_id}", json=merged)
        result = await handle_api_response(response, "Update contact")
        logger.info("Contact %s updated", contact_id)
        return result


def _format_contact_for_display(contact: Dict[str, Any]) -> str:
    """Format a contact object into a readable multi-line string."""
    lines = ["=" * 60, "CONTACT DETAILS", "=" * 60]
    lines.append(f"ID: {contact.get('id', '—')}")
    lines.append(f"First Name: {contact.get('firstName', '—')}")
    lines.append(f"Last Name: {contact.get('lastName', '—')}")
    lines.append(f"Department: {contact.get('department') or '—'}")
    lines.append(f"Designation: {contact.get('designation') or '—'}")
    lines.append(f"Company: {contact.get('company') or '—'}")
    # Emails
    emails = contact.get("emails") or []
    if emails:
        for e in emails:
            val = e.get("value", "")
            typ = e.get("type", "")
            prim = " (primary)" if e.get("primary") else ""
            lines.append(f"Email ({typ}): {val}{prim}")
    else:
        lines.append("Email: —")
    # Phones
    phones = contact.get("phoneNumbers") or []
    if phones:
        for p in phones:
            code = p.get("code", "")
            val = p.get("value", "")
            typ = p.get("type", "")
            prim = " (primary)" if p.get("primary") else ""
            lines.append(f"Phone ({typ}): +{code} {val}{prim}")
    else:
        lines.append("Phone: —")
    lines.append(f"Owner ID: {contact.get('ownerId', '—')}")
    lines.append(f"Created At: {contact.get('createdAt', '—')}")
    lines.append(f"Updated At: {contact.get('updatedAt', '—')}")
    ad = contact.get("associatedDeals") or []
    if ad:
        lines.append(f"Associated deal IDs: {ad}")
    # Custom fields
    custom = contact.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_contact_field_instructions() -> str:
    """
    Get contact field reference (API names, Field IDs, picklist options).
    ALWAYS call this FIRST before creating or updating a contact.
    """
    try:
        _reset_api_call_count()
        fields = await _fetch_contact_fields()
        lines = ["# Contact Field Reference", ""]
        for field in fields:
            lines.extend(_format_field(field, include_filterable=True))
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Failed to fetch fields: {e.message}"
    except Exception as e:
        logger.exception("get_contact_field_instructions")
        return f"✗ Unexpected error: {str(e)}"




async def get_contact_logic(contact_id: int) -> Dict[str, Any]:
    """Fetch a single contact by ID (GET /contacts/{id}). Returns full contact object."""
    contact_id = int(contact_id)
    async with get_client() as client:
        response = await client.get(f"/contacts/{contact_id}")
        return await handle_api_response(response, "Get contact")


@mcp.tool()
async def get_contact(contact_id: int) -> str:
    """Get full details of a contact by ID."""
    try:
        _reset_api_call_count()
        contact = await get_contact_logic(contact_id)
        return _format_contact_for_display(contact)
    except KylasAPIError as e:
        return f"✗ Failed to get contact: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_contact")
        return f"✗ Unexpected error: {str(e)}"


async def search_contacts_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search contacts with jsonRule; only filterable fields allowed."""
    fields_list = await _fetch_contact_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    # Add associated entity fields which are filterable but may not be marked as such in schema
    for associated_field in ["associatedLeads", "associatedDeals", "associatedCompanies"]:
        if associated_field not in filterable_map:
            filterable_map[associated_field] = {"type": "LOOK_UP", "standard": True}
    if not filterable_map:
        return "No filterable contact fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {
        "fields": ["id", "firstName", "lastName", "emails", "phoneNumbers", "ownerId", "department", "designation", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching contacts with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/search/contact", params=params, json=payload)
        data = await handle_api_response(response, "Search contacts")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No contacts found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} contact(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for contact in results:
        cid = contact.get("id", "?")
        fn = contact.get("firstName") or ""
        ln = contact.get("lastName") or ""
        name = f"{fn} {ln}".strip() or "—"
        email = _extract_primary_email(contact.get("emails"))
        phone = _extract_primary_phone(contact.get("phoneNumbers"))
        lines.append(f"• ID: {cid} | Name: {name} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_contacts(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter contacts by criteria. Only fields marked [FILTERABLE] can be used.
    Call get_contact_field_instructions first to see filterable fields and their types.
    Same filter format as search_leads (no pipeline/stage filters for contacts).

    Additional filterable fields (always available):
    - associatedLeads (long): Filter by lead IDs associated with contacts
    - associatedDeals (long): Filter by deal IDs associated with contacts
    - associatedCompanies (long): Filter by company IDs associated with contacts
    Use operators: equal, is_null, is_not_null
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty. Provide at least one filter."
        return await search_contacts_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_contacts")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Task Entity Support (same as Contact/Lead but task-specific fields)
# ---------------------------------------------------------------------------

# Task entity relationship lookup functions
async def lookup_leads_for_task(search_term: str = "") -> Dict[str, Any]:
    """Look up leads to associate with a task. Returns top 10 leads matching the search term."""
    query = f"firstName:{search_term}" if search_term else "firstName:"
    async with get_client() as client:
        response = await client.get(
            "/search/lead/lookup",
            params={"converted": "false", "q": query}
        )
        return await handle_api_response(response, "Lookup leads for task")


async def lookup_contacts_for_task(search_term: str = "") -> Dict[str, Any]:
    """Look up contacts to associate with a task. Returns top 10 contacts matching the search term."""
    query = f"name:{search_term}" if search_term else "name:"
    payload = {"deal": [], "idToExclude": [], "company": []}
    async with get_client() as client:
        response = await client.post(
            "/search/contact/associated-with-entity",
            params={"view": "task", "q": query},
            json=payload
        )
        return await handle_api_response(response, "Lookup contacts for task")


async def lookup_deals_for_task(search_term: str = "") -> Dict[str, Any]:
    """Look up deals to associate with a task. Returns top 10 deals matching the search term."""
    query = f"name:{search_term}" if search_term else "name:"
    payload = {"contact": [], "idToExclude": [], "company": []}
    async with get_client() as client:
        response = await client.post(
            "/search/deal/associated-with-entity",
            params={"view": "task", "q": query},
            json=payload
        )
        return await handle_api_response(response, "Lookup deals for task")


async def lookup_companies_for_task(search_term: str = "") -> Dict[str, Any]:
    """Look up companies to associate with a task. Returns top 10 companies matching the search term."""
    query = f"name:{search_term}" if search_term else "name:"
    payload = {"deal": [], "idToExclude": [], "contact": []}
    async with get_client() as client:
        response = await client.post(
            "/search/company/associated-with-entity",
            params={"view": "task", "q": query},
            json=payload
        )
        return await handle_api_response(response, "Lookup companies for task")


async def _fetch_task_fields() -> List[Dict[str, Any]]:
    """Fetch task field metadata from Kylas API."""
    async with get_client() as client:
        response = await client.get(
            "/entities/task/fields",
            params={"entityType": "task", "custom-only": "false", "page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch task fields")
        if isinstance(data, list):
            fields = data
        else:
            fields = data.get("data", data.get("content", []))
        return [f for f in fields if f.get("active", True)]


async def _get_custom_task_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom task field ID -> internal name."""
    fields = await _fetch_task_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


async def create_task_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a task with the given dynamic field_values."""
    fv = dict(field_values)
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_custom_task_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    logger.info("Creating task with fields: %s", list(payload.keys()))
    async with get_client() as client:
        response = await client.post("/tasks", json=payload)
        result = await handle_api_response(response, "Create task")
        logger.info("Task created with ID: %s", result.get("id"))
        return result


async def update_task_logic(task_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the task first, merge field_values into it, then PUT the full body."""
    task_id = int(task_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_custom_task_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values produced an empty payload.")
    logger.info("Updating task %s with fields: %s", task_id, list(payload.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/tasks/{task_id}")
        existing = await handle_api_response(get_response, "Get task")
        merged = dict(existing)
        for key, value in payload.items():
            if key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            else:
                merged[key] = value
        response = await client.put(f"/tasks/{task_id}", json=merged)
        result = await handle_api_response(response, "Update task")
        logger.info("Task %s updated", task_id)
        return result


def _format_task_for_display(task: Dict[str, Any]) -> str:
    """Format a task object into a readable multi-line string."""
    lines = ["=" * 60, "TASK DETAILS", "=" * 60]
    lines.append(f"ID: {task.get('id', '—')}")
    lines.append(f"Name: {task.get('name', '—')}")
    lines.append(f"Description: {task.get('description') or '—'}")
    lines.append(f"Status: {task.get('status') or '—'}")
    lines.append(f"Priority: {task.get('priority') or '—'}")
    lines.append(f"Due Date: {task.get('dueDate') or '—'}")
    lines.append(f"Assigned To: {task.get('assignedTo') or '—'}")
    lines.append(f"Reminder: {task.get('reminder') or '—'}")
    lines.append(f"Created At: {task.get('createdAt', '—')}")
    lines.append(f"Updated At: {task.get('updatedAt', '—')}")
    # Custom fields
    custom = task.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_task_field_instructions() -> str:
    """
    Get task field reference (API names, Field IDs, picklist options).
    ALWAYS call this FIRST before creating or updating a task.
    """
    try:
        _reset_api_call_count()
        fields = await _fetch_task_fields()
        lines = ["# Task Field Reference", ""]
        for field in fields:
            lines.extend(_format_field(field, include_filterable=True))
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Failed to fetch fields: {e.message}"
    except Exception as e:
        logger.exception("get_task_field_instructions")
        return f"✗ Unexpected error: {str(e)}"



@mcp.tool()
async def get_task(task_id: int) -> str:
    """Get full details of a task by ID."""
    try:
        _reset_api_call_count()
        async with get_client() as client:
            response = await client.get(f"/tasks/{task_id}")
            task = await handle_api_response(response, "Get task")
        return _format_task_for_display(task)
    except KylasAPIError as e:
        return f"✗ Failed to get task: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_task")
        return f"✗ Unexpected error: {str(e)}"


async def search_tasks_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search tasks with jsonRule; only filterable fields allowed."""
    fields_list = await _fetch_task_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    # Add associated entity fields which are filterable but may not be marked as such in schema
    for associated_field in ["associatedLeads", "associatedContacts", "associatedDeals", "associatedCompanies"]:
        if associated_field not in filterable_map:
            filterable_map[associated_field] = {"type": "LOOK_UP", "standard": True}
    if not filterable_map:
        return "No filterable task fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {
        "fields": ["id", "name", "status", "priority", "dueDate", "assignedTo", "relation", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching tasks with %d filter(s): jsonRule=%s", len(filters), json_rule)
    async with get_client() as client:
        response = await client.post("/tasks/search", params=params, json=payload)
        data = await handle_api_response(response, "Search tasks")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        filter_summary = "; ".join([f"{f.get('field')}={f.get('value')}" for f in filters])
        return f"No tasks found matching filters: {filter_summary}. (Total tasks in DB: {total})"
    lines = [f"Found {len(results)} task(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for task in results:
        tid = task.get("id", "?")
        name = task.get("name", "—")
        status = task.get("status", "—")
        priority = task.get("priority", "—")
        due_date = task.get("dueDate", "—")
        lines.append(f"• ID: {tid} | Name: {name} | Status: {status} | Priority: {priority} | Due: {due_date}")
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_tasks(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter tasks by criteria. Only fields marked [FILTERABLE] can be used.
    Call get_task_field_instructions first to see filterable fields and their types.
    Same filter format as search_leads/search_contacts (no pipeline filters for tasks).

    Additional filterable fields (always available):
    - associatedLeads (long): Filter by lead IDs associated with tasks
    - associatedContacts (long): Filter by contact IDs associated with tasks
    - associatedDeals (long): Filter by deal IDs associated with tasks
    - associatedCompanies (long): Filter by company IDs associated with tasks
    Use operators: equal, is_null, is_not_null
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty. Provide at least one filter."
        return await search_tasks_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_entity_for_task(entity_type: str, search_term: str = "") -> str:
    """
    Look up a lead, contact, deal, or company to associate with a task.
    Use this BEFORE create_entity or update_entity (task) when the user specifies an entity by name.

    entity_type: "lead", "contact", "deal", or "company" (use internal type, not tenant display name)
    search_term: optional name to filter results (e.g. "John", "Acme")

    Returns top matching entities with IDs. Use the returned id in task's "relation" field:
    {"targetEntityId": <id>, "targetEntityType": "<LEAD|CONTACT|DEAL|COMPANY>", "targetEntityName": "<name>"}
    """
    try:
        _reset_api_call_count()
        etype = entity_type.lower().strip()

        lead_label = _ENTITY_LABELS.get("lead", {}).get("displayName", "Lead")
        contact_label = _ENTITY_LABELS.get("contact", {}).get("displayName", "Contact")
        deal_label = _ENTITY_LABELS.get("deal", {}).get("displayName", "Deal")
        company_label = _ENTITY_LABELS.get("company", {}).get("displayName", "Company")

        if etype == "lead":
            result = await lookup_leads_for_task(search_term)
            items = result if isinstance(result, list) else result.get("data", result.get("content", []))
            if not items:
                return f"No {lead_label}s found matching '{search_term}'."
            lines = [f"Found {len(items)} {lead_label}(s) matching '{search_term}':", "-" * 60]
            for item in items[:10]:
                lines.append(
                    f"• ID: {item.get('id', '?')} | Name: {item.get('firstName', '—')} {item.get('lastName', '')} | Company: {item.get('companyName', '—')}"
                )
        elif etype == "contact":
            result = await lookup_contacts_for_task(search_term)
            items = result if isinstance(result, list) else result.get("data", result.get("content", []))
            if not items:
                return f"No {contact_label}s found matching '{search_term}'."
            lines = [f"Found {len(items)} {contact_label}(s) matching '{search_term}':", "-" * 60]
            for item in items[:10]:
                email = item.get("emails", [{}])[0].get("value", "—") if item.get("emails") else "—"
                lines.append(f"• ID: {item.get('id', '?')} | Name: {item.get('name', '—')} | Email: {email}")
        elif etype == "deal":
            result = await lookup_deals_for_task(search_term)
            items = result if isinstance(result, list) else result.get("data", result.get("content", []))
            if not items:
                return f"No {deal_label}s found matching '{search_term}'."
            lines = [f"Found {len(items)} {deal_label}(s) matching '{search_term}':", "-" * 60]
            for item in items[:10]:
                lines.append(f"• ID: {item.get('id', '?')} | Name: {item.get('name', '—')} | Value: {item.get('value', '—')}")
        elif etype == "company":
            result = await lookup_companies_for_task(search_term)
            items = result if isinstance(result, list) else result.get("data", result.get("content", []))
            if not items:
                return f"No {company_label}s found matching '{search_term}'."
            lines = [f"Found {len(items)} {company_label}(s) matching '{search_term}':", "-" * 60]
            for item in items[:10]:
                lines.append(f"• ID: {item.get('id', '?')} | Name: {item.get('name', '—')} | Industry: {item.get('industry', '—')}")
        else:
            valid = f"{lead_label} (lead), {contact_label} (contact), {deal_label} (deal), {company_label} (company)"
            return f"✗ Unknown entity_type '{entity_type}'. Valid types: {valid}"

        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Lookup failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_entity_for_task")
        return f"✗ Unexpected error: {str(e)}"


async def _fetch_raw_tasks_for_relation(
    relation_field: str,
    filterable_map: Dict[str, Any],
    size: int,
    sort: Optional[str],
) -> List[Dict[str, Any]]:
    """Fetch tasks where the given relation field is not null. Returns raw task list."""
    json_rule, err = _build_search_json_rule(
        [{"field": relation_field, "operator": "is_not_null", "value": None}],
        filterable_map,
    )
    if err:
        logger.warning("_fetch_raw_tasks_for_relation(%s): rule error: %s", relation_field, err)
        return []
    payload = {
        "fields": ["id", "name", "status", "priority", "dueDate", "assignedTo", "relation", "createdAt"],
        "jsonRule": json_rule,
    }
    params: Dict[str, Any] = {"page": 0, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    async with get_client() as client:
        response = await client.post("/tasks/search", params=params, json=payload)
        data = await handle_api_response(response, f"Search tasks ({relation_field} is_not_null)")
    return data.get("content", data.get("data", []))


async def _search_tasks_with_any_relation_logic(
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Find tasks that have at least one relation present.
    Makes 4 concurrent API calls (one per association field with is_not_null) and unions results by ID.
    After merging, re-sorts the full list so pagination is consistent regardless of sub-query order.
    """
    fields_list = await _fetch_task_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    for f in ["associatedLeads", "associatedContacts", "associatedDeals", "associatedCompanies"]:
        if f not in filterable_map:
            filterable_map[f] = {"type": "LOOK_UP", "standard": True}

    # Fetch more than needed per sub-query to account for cross-relation overlap after dedup
    fetch_size = min(max(size * 4, 50), 100)

    raw_results = await asyncio.gather(
        _fetch_raw_tasks_for_relation("associatedLeads", filterable_map, fetch_size, sort),
        _fetch_raw_tasks_for_relation("associatedContacts", filterable_map, fetch_size, sort),
        _fetch_raw_tasks_for_relation("associatedDeals", filterable_map, fetch_size, sort),
        _fetch_raw_tasks_for_relation("associatedCompanies", filterable_map, fetch_size, sort),
        return_exceptions=True,
    )

    seen_ids: set = set()
    all_tasks: List[Dict[str, Any]] = []
    for result in raw_results:
        if isinstance(result, Exception):
            logger.warning("Relation search partial failure: %s", result)
            continue
        for task in result:
            tid = task.get("id")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                all_tasks.append(task)

    if not all_tasks:
        return "No tasks with any relation found."

    # Re-sort merged list so page boundaries are deterministic
    sort_field, _, sort_dir = (sort or "createdAt,desc").partition(",")
    reverse = sort_dir.strip().lower() != "asc"
    all_tasks.sort(key=lambda t: (t.get(sort_field) or ""), reverse=reverse)

    start = page * size
    paginated = all_tasks[start:start + size]
    if not paginated:
        return f"No tasks on page {page + 1} (total found: {len(all_tasks)})."

    lead_label = _ENTITY_LABELS.get("lead", {}).get("displayName", "Lead")
    contact_label = _ENTITY_LABELS.get("contact", {}).get("displayName", "Contact")
    deal_label = _ENTITY_LABELS.get("deal", {}).get("displayName", "Deal")
    company_label = _ENTITY_LABELS.get("company", {}).get("displayName", "Company")
    entity_type_label_map = {
        "LEAD": lead_label,
        "CONTACT": contact_label,
        "DEAL": deal_label,
        "COMPANY": company_label,
    }

    lines = [
        f"Found {len(all_tasks)} task(s) with relation(s) (showing {len(paginated)}, page {page + 1})",
        "-" * 60,
    ]
    for task in paginated:
        tid = task.get("id", "?")
        name = task.get("name", "—")
        status = task.get("status", "—")
        priority = task.get("priority", "—")
        due = task.get("dueDate", "—")
        relation = task.get("relation") or []
        rel_parts = []
        for r in relation:
            etype = r.get("targetEntityType", "")
            ename = r.get("targetEntityName", "")
            eid = r.get("targetEntityId", "")
            elabel = entity_type_label_map.get(etype, etype)
            rel_parts.append(f"{elabel}: {ename} (ID: {eid})")
        rel_str = " | ".join(rel_parts) if rel_parts else "—"
        lines.append(f"• ID: {tid} | {name} | {status} | {priority} | Due: {due}")
        if rel_str != "—":
            lines.append(f"  Relations: {rel_str}")
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_tasks_with_any_relation(
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Find tasks that have at least one relation present (linked to a lead, contact, deal, or company).

    The API cannot filter across multiple association fields in a single request. This tool makes
    4 parallel calls (associatedLeads is_not_null, associatedContacts is_not_null,
    associatedDeals is_not_null, associatedCompanies is_not_null) and returns a unified,
    deduplicated list sorted by the requested field.

    Default sort is createdAt,desc (most recently created first).
    Other useful sorts: dueDate,asc, dueDate,desc, createdAt,asc.

    Use this when the user asks for tasks that "have a relation", "are linked to an entity",
    or "have an associated lead/contact/deal/company" without specifying a particular entity ID.

    Each result shows task details and its relation(s) using tenant-specific display names.
    """
    try:
        _reset_api_call_count()
        return await _search_tasks_with_any_relation_logic(page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks_with_any_relation")
        return f"✗ Unexpected error: {str(e)}"


# ===========================================================================
# DEAL TOOLS
# ===========================================================================

# Picklist fields that use internal name (string) in search; all others use Option ID (long)
DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"currency", "country", "dealSource"}

# Picklist fields that use internal name (string) in company search
COMPANY_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"country"}

# ---------------------------------------------------------------------------
# Deal field metadata helpers
# ---------------------------------------------------------------------------

async def _fetch_deal_fields() -> List[Dict[str, Any]]:
    """Fetch deal field metadata from Kylas API. Returns list of field dicts."""
    async with get_client() as client:
        response = await client.get(
            "/deals/fields",
            params={"page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch deal fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def _get_deal_custom_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom field ID (string) -> internal name (e.g. cfDealStatus)."""
    fields = await _fetch_deal_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


async def get_deal_field_instructions_logic() -> str:
    fields = await _fetch_deal_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    lines = [
        "=" * 60,
        "KYLAS CRM - DEAL FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True))
    lines.extend(["", "=" * 60, "END OF CHEAT SHEET", "=" * 60])
    return "\n".join(lines)


@mcp.tool()
async def get_deal_field_instructions() -> str:
    """
    Get all deal fields for the current tenant. CALL THIS FIRST before creating or updating a deal.
    Returns a cheat sheet with API names (standard fields), Field IDs (custom fields), and Picklist Option IDs.
    Use this to build field_values for create_deal based on what the user wants—do not use static fields.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching deal field instructions")
        result = await get_deal_field_instructions_logic()
        return result
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_deal_field_instructions")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Deal search rule builder (with deal-specific picklist fields)
# ---------------------------------------------------------------------------

def _build_deal_search_json_rule(
    filters: List[Dict[str, Any]],
    filterable_map: Dict[str, Dict[str, Any]],
    default_timezone: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /search/deal. Returns (jsonRule, error_message).
    Same as lead version but uses DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME.
    """
    tz_for_date = default_timezone or DEFAULT_TIMEZONE
    rules = []
    for i, f in enumerate(filters):
        field_name = f.get("field")
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
        operator = OPERATOR_SYMBOL_MAP.get(operator, operator)
        value = f.get("value")
        field_type_key = (f.get("type") or "TEXT_FIELD").strip().upper().replace(" ", "_")

        if not field_name:
            return {}, f"Filter #{i + 1}: missing 'field'."
        if field_name not in filterable_map:
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only [FILTERABLE] fields from get_deal_field_instructions."
        meta = filterable_map[field_name]
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        # Deal-specific picklist handling: use DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME instead of PICKLIST_FIELDS_USE_INTERNAL_NAME
        if api_type in ("PICK_LIST", "MULTI_PICKLIST"):
            rule_type = "string" if field_name in DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME else "long"
        else:
            rule_type = _rule_type_for_value(api_type, field_name, value)
        if rule_type in ("long", "double") and value is not None and not isinstance(value, (int, float)):
            try:
                value = float(value) if rule_type == "double" else int(value)
            except (TypeError, ValueError):
                value = value
        # Date/datetime fields: convert date values from user's timezone to UTC
        # e.g. "11th Aug 00:00" in Asia/Calcutta → "10th Aug 18:30" UTC
        if rule_type == "date" and value is not None:
            filter_tz = f.get("timeZone") or tz_for_date
            value = _convert_date_value_to_utc(value, filter_tz)

        is_custom = not meta.get("standard", True)
        rule_field = f"customFieldValues.{field_name}" if is_custom else field_name

        rule = {
            "operator": operator,
            "id": field_name,
            "field": rule_field,
            "type": rule_type,
            "value": value,
            "relatedFieldIds": None,
        }
        if field_name == "pipeline":
            rule["dependentFieldIds"] = ["pipelineStage", "pipelineStageReason"]
        elif field_name == "pipelineStage":
            rule["relatedFieldIds"] = ["pipeline"]
        if rule_type == "date":
            rule["timeZone"] = f.get("timeZone") or tz_for_date
        rules.append(rule)

    return {"rules": rules, "condition": "AND", "valid": True}, None


# ---------------------------------------------------------------------------
# Deal create/update/get logic
# ---------------------------------------------------------------------------

def _normalize_deal_payload(payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Apply deal-specific field normalization on top of generic _normalize_field_values output.
    - ownedBy: int → {"id": int}
    - estimatedValue / actualValue / value: number → {"currencyId": <id>, "value": number}
      (currencyId taken from existing deal if available)
    """
    _MONETARY_FIELDS = ("estimatedValue", "actualValue", "value")

    if "ownedBy" in payload and isinstance(payload["ownedBy"], (int, float)) and not isinstance(payload["ownedBy"], bool):
        payload["ownedBy"] = {"id": int(payload["ownedBy"])}

    for field in _MONETARY_FIELDS:
        if field not in payload:
            continue
        v = payload[field]
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            # Try to get currencyId from existing deal object for this field
            currency_id = None
            if existing and isinstance(existing.get(field), dict):
                currency_id = existing[field].get("currencyId")
            if currency_id:
                payload[field] = {"currencyId": currency_id, "value": v}
            else:
                # Can't wrap without a currencyId — leave as-is; system instructions tell Claude to pass full object
                logger.warning("Deal field '%s' is a plain number but no currencyId available; sending as-is.", field)
    return payload


async def create_deal_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a deal with the given dynamic field_values (Kylas API payload shape).
    If the target pipeline has sequentialStageFlow=true and the target stage is not the first stage,
    the deal is automatically created in the first stage then advanced sequentially to the target stage.
    """
    fv = dict(field_values)
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_deal_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    payload = _normalize_deal_payload(payload)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")

    # Extract pipeline/stage info for potential stage-lock fallback
    target_stage_id: Optional[int] = None
    pipeline_id_for_lock: Optional[int] = None
    if isinstance(payload.get("pipeline"), dict):
        pipeline_id_for_lock = payload["pipeline"].get("id")
        stage = payload["pipeline"].get("stage")
        if isinstance(stage, dict):
            target_stage_id = stage.get("id")

    logger.info("Creating deal with fields: %s", list(payload.keys()))
    async with get_client() as client:
        try:
            response = await client.post("/deals", json=payload)
            result = await handle_api_response(response, "Create deal")
            logger.info("Deal created with ID: %s", result.get("id"))
            return result
        except KylasAPIError as e:
            if not _is_stage_lock_error(e) or target_stage_id is None or pipeline_id_for_lock is None:
                raise

    # Stage lock hit — create in first stage then advance sequentially
    logger.info(
        "Stage lock on pipeline %s; creating deal in first stage then advancing to stage %s",
        pipeline_id_for_lock, target_stage_id,
    )
    pipeline_data = await _get_pipeline_details_raw(pipeline_id_for_lock)
    stages = sorted(pipeline_data.get("stages", []), key=lambda s: s.get("position", 0))
    if not stages:
        raise KylasAPIError("Pipeline has no stages; cannot create deal.")

    first_stage = stages[0]
    first_stage_id = first_stage["id"]

    if first_stage_id == target_stage_id:
        # Already targeting stage 1 but got lock error — unexpected, re-raise
        raise KylasAPIError(
            "Sequential stage lock error even when targeting the first stage. "
            "Check pipeline configuration."
        )

    # Build payload with first stage
    first_payload = dict(payload)
    first_payload["pipeline"] = dict(first_payload["pipeline"])
    first_payload["pipeline"]["stage"] = {"id": first_stage_id}
    first_payload["forecastingType"] = first_stage.get("forecastingType", first_payload.get("forecastingType"))

    async with get_client() as client:
        response = await client.post("/deals", json=first_payload)
        deal = await handle_api_response(response, "Create deal (first stage)")

    deal_id = deal["id"]
    logger.info("Deal %s created in first stage %s; advancing to target stage %s", deal_id, first_stage_id, target_stage_id)

    return await _advance_deal_to_stage_sequentially(deal_id, stages, first_stage_id, target_stage_id, deal)


async def update_deal_logic(deal_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the deal first, merge field_values into it, then PUT the full body. No partial update."""
    deal_id = int(deal_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_deal_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values produced an empty payload.")
    logger.info("Updating deal %s with fields: %s", deal_id, list(payload.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/deals/{deal_id}")
        existing = await handle_api_response(get_response, "Get deal")
        payload = _normalize_deal_payload(payload, existing=existing)
        merged = dict(existing)
        for key, value in payload.items():
            if key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            elif key in ["contacts", "associatedContacts"] and isinstance(value, list):
                # Handle contact associations: merge with existing associatedContacts
                new_contacts = []
                for contact in value:
                    if isinstance(contact, dict):
                        # Contact object with id and possibly name
                        contact_id = contact.get("id")
                        contact_name = contact.get("name")
                        if contact_id:
                            # If no name provided, fetch it from the API
                            if not contact_name:
                                try:
                                    contact_details = await get_contact_logic(contact_id)
                                    contact_name = f"{contact_details.get('firstName', '')} {contact_details.get('lastName', '')}".strip()
                                except Exception as e:
                                    logger.warning(f"Could not fetch contact {contact_id} details: {e}")
                                    contact_name = f"Contact {contact_id}"
                            new_contacts.append({"id": contact_id, "name": contact_name})
                    elif isinstance(contact, (int, str)):
                        # Just an ID, fetch the contact
                        try:
                            contact_id = int(contact)
                            contact_details = await get_contact_logic(contact_id)
                            contact_name = f"{contact_details.get('firstName', '')} {contact_details.get('lastName', '')}".strip()
                            new_contacts.append({"id": contact_id, "name": contact_name})
                        except Exception as e:
                            logger.warning(f"Could not fetch contact {contact}: {e}")

                # Merge with existing associatedContacts (avoid duplicates by ID)
                existing_contacts = merged.get("associatedContacts", []) or []
                existing_ids = {c.get("id") for c in existing_contacts if isinstance(c, dict)}
                for contact in new_contacts:
                    if contact.get("id") not in existing_ids:
                        existing_contacts.append(contact)
                merged["associatedContacts"] = existing_contacts
            elif key == "products" and isinstance(value, list):
                # Handle products: merge with existing products on the deal
                new_products = []
                for product in value:
                    if isinstance(product, dict) and product.get("id"):
                        # Build product object with defaults for missing fields
                        prod = {
                            "id": product["id"],
                            "name": product.get("name", ""),
                            "quantity": product.get("quantity", 1),
                            "discount": product.get("discount", {"value": 0, "type": "PERCENTAGE"}),
                            "price": product.get("price", {}),
                            "units": product.get("units"),
                            "category": product.get("category"),
                            "hsnSacCode": product.get("hsnSacCode"),
                            "countryOfOrigin": product.get("countryOfOrigin"),
                            "customFieldValues": product.get("customFieldValues", {}),
                        }
                        new_products.append(prod)
                    elif isinstance(product, (int, str)):
                        # Just a product ID — add with quantity 1
                        try:
                            prod_id = int(product)
                            new_products.append({
                                "id": prod_id,
                                "name": "",
                                "quantity": 1,
                                "discount": {"value": 0, "type": "PERCENTAGE"},
                                "price": {},
                                "units": None,
                                "category": None,
                                "hsnSacCode": None,
                                "countryOfOrigin": None,
                                "customFieldValues": {},
                            })
                        except (TypeError, ValueError):
                            logger.warning(f"Invalid product ID: {product}")

                # Merge with existing products (avoid duplicates by ID)
                existing_products = merged.get("products", []) or []
                existing_product_ids = {p.get("id") for p in existing_products if isinstance(p, dict)}
                for prod in new_products:
                    if prod.get("id") not in existing_product_ids:
                        existing_products.append(prod)
                merged["products"] = existing_products
            elif key == "pipelineStage" and isinstance(value, (int, str)):
                # Special handling: pipelineStage should update the existing pipeline's stage ID
                # Also update forecastingType to match the stage's forecastingType
                stage_id = int(value)
                if isinstance(merged.get("pipeline"), dict):
                    pipeline_id = merged["pipeline"].get("id")
                    if not isinstance(merged["pipeline"].get("stage"), dict):
                        merged["pipeline"]["stage"] = {}
                    merged["pipeline"]["stage"]["id"] = stage_id

                    # Fetch pipeline details to get the correct forecastingType for this stage
                    try:
                        if pipeline_id:
                            pipeline_data = await _get_pipeline_details_raw(pipeline_id)
                            for stage in pipeline_data.get("stages", []):
                                if stage.get("id") == stage_id:
                                    forecast_type = stage.get("forecastingType")
                                    if forecast_type:
                                        merged["forecastingType"] = forecast_type
                                    break
                    except Exception as e:
                        logger.warning("Could not fetch forecastingType for stage %s: %s", stage_id, e)
                else:
                    logger.warning("Trying to set pipelineStage but deal has no pipeline object")
                    raise KylasAPIError("Deal has no pipeline; cannot set stage.")
            else:
                merged[key] = value

        # Capture stage-move info before PUT for sequential fallback
        update_target_stage_id: Optional[int] = None
        current_stage_id_for_lock: Optional[int] = None
        pipeline_id_for_lock: Optional[int] = None
        if "pipelineStage" in payload:
            update_target_stage_id = int(payload["pipelineStage"])
            if isinstance(existing.get("pipeline"), dict):
                pipeline_id_for_lock = existing["pipeline"].get("id")
                existing_stage = existing["pipeline"].get("stage")
                if isinstance(existing_stage, dict):
                    current_stage_id_for_lock = existing_stage.get("id")

        try:
            response = await client.put(f"/deals/{deal_id}", json=merged)
            result = await handle_api_response(response, "Update deal")
            logger.info("Deal %s updated", deal_id)
            return result
        except KylasAPIError as e:
            if (
                not _is_stage_lock_error(e)
                or update_target_stage_id is None
                or current_stage_id_for_lock is None
                or pipeline_id_for_lock is None
            ):
                raise

        # Stage lock hit during update — advance sequentially
        logger.info(
            "Stage lock on pipeline %s; advancing deal %s from stage %s to stage %s sequentially",
            pipeline_id_for_lock, deal_id, current_stage_id_for_lock, update_target_stage_id,
        )
        pipeline_data = await _get_pipeline_details_raw(pipeline_id_for_lock)
        stages = sorted(pipeline_data.get("stages", []), key=lambda s: s.get("position", 0))
        return await _advance_deal_to_stage_sequentially(
            deal_id, stages, current_stage_id_for_lock, update_target_stage_id, existing
        )


async def get_deal_logic(deal_id: int) -> Dict[str, Any]:
    """Fetch a single deal by ID (GET /deals/{id}). Returns full deal object."""
    deal_id = int(deal_id)
    async with get_client() as client:
        response = await client.get(f"/deals/{deal_id}")
        return await handle_api_response(response, "Get deal")


def _format_deal_for_display(deal: Dict[str, Any]) -> str:
    """Format a deal object into a readable multi-line string."""
    lines = ["=" * 60, "DEAL DETAILS", "=" * 60]
    lines.append(f"ID: {deal.get('id', '—')}")
    lines.append(f"Name: {deal.get('name', '—')}")
    lines.append(f"Value: {deal.get('value', '—')}")
    lines.append(f"Currency: {deal.get('currency', '—')}")
    lines.append(f"Closing Date: {deal.get('closingDate', '—')}")
    # Emails
    emails = deal.get("emails") or []
    if emails:
        for e in emails:
            val = e.get("value", "")
            typ = e.get("type", "")
            prim = " (primary)" if e.get("primary") else ""
            lines.append(f"Email ({typ}): {val}{prim}")
    else:
        lines.append("Email: —")
    # Phones
    phones = deal.get("phoneNumbers") or []
    if phones:
        for p in phones:
            code = p.get("code", "")
            val = p.get("value", "")
            typ = p.get("type", "")
            prim = " (primary)" if p.get("primary") else ""
            lines.append(f"Phone ({typ}): +{code} {val}{prim}")
    else:
        lines.append("Phone: —")
    # Pipeline / Stage
    pipeline = deal.get("pipeline") or {}
    if isinstance(pipeline, dict):
        pl_name = pipeline.get("name", "—")
        stage = pipeline.get("stage") or {}
        stage_name = stage.get("name", "—") if isinstance(stage, dict) else "—"
        lines.append(f"Pipeline: {pl_name}")
        lines.append(f"Stage: {stage_name}")
    else:
        lines.append(f"Pipeline: {pipeline}")
    lines.append(f"Owner ID: {deal.get('ownerId', '—')}")
    lines.append(f"Created At: {deal.get('createdAt', '—')}")
    lines.append(f"Updated At: {deal.get('updatedAt', '—')}")
    # Products
    products = deal.get("products") or []
    lines.append("")
    if products:
        lines.append(f"Products ({len(products)}):")
        products_total = 0.0
        has_all_totals = True
        for prod in products:
            prod_name = prod.get("name") or prod.get("displayName") or f"ID:{prod.get('id', '?')}"
            qty = prod.get("quantity")
            price_obj = prod.get("price") or {}
            unit_price = price_obj.get("value")
            disc_obj = prod.get("discount") or {}
            disc_val = disc_obj.get("value")
            disc_type = disc_obj.get("type", "")
            # Build display parts
            qty_str = str(qty) if qty is not None else "?"
            price_str = str(unit_price) if unit_price is not None else "?"
            disc_str = f"{disc_val}{'%' if disc_type == 'PERCENTAGE' else ''}" if disc_val is not None else "—"
            # Compute line total
            line_total = None
            if qty is not None and unit_price is not None:
                try:
                    q, p = float(qty), float(unit_price)
                    if disc_val is not None:
                        d = float(disc_val)
                        if disc_type == "PERCENTAGE":
                            line_total = q * p * (1 - d / 100)
                        else:
                            line_total = q * p - d
                    else:
                        line_total = q * p
                    products_total += line_total
                except (TypeError, ValueError):
                    has_all_totals = False
            else:
                has_all_totals = False
            line_total_str = f"{line_total:,.2f}" if line_total is not None else "?"
            lines.append(f"  • {prod_name:<20} qty: {qty_str:<5} unit: {price_str:<10} discount: {disc_str:<10} line total: {line_total_str}")
        if has_all_totals and products:
            lines.append(f"  {'─' * 55}")
            lines.append(f"  Products Total: {products_total:,.2f}")
    else:
        lines.append("Products: —")
    # Custom fields
    custom = deal.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_deal(deal_id: int) -> str:
    """
    Get full details of a deal by ID (GET /deals/{id}). Use when the user asks for complete deal info.
    deal_id: The deal ID (e.g. from search_deals or search_deals_by_term results).
    """
    try:
        _reset_api_call_count()
        deal = await get_deal_logic(deal_id)
        return _format_deal_for_display(deal)
    except KylasAPIError as e:
        return f"✗ Failed to get deal: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_deal")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Deal search logic
# ---------------------------------------------------------------------------

def _extract_primary_deal_value(value: Any) -> str:
    """Extract deal value for display."""
    if value is None:
        return "-"
    return str(value)


async def search_deals_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search deals with jsonRule; only filterable fields allowed. Uses current user timezone for date/datetime filters."""
    fields_list = await _fetch_deal_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable deal fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_deal_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {
        "fields": ["id", "name", "value", "currency", "closingDate", "ownedBy", "createdAt", "actualValue", "estimatedValue", "associatedContacts", "associatedLeads", "associatedCompanies", "products"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching deals with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/search/deal", params=params, json=payload)
        data = await handle_api_response(response, "Search deals")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No deals found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} deal(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for deal in results:
        did = deal.get("id", "?")
        name = deal.get("name", "—")
        value = _extract_primary_deal_value(deal.get("value"))
        actual_val = _extract_primary_deal_value(deal.get("actualValue"))
        estimated_val = _extract_primary_deal_value(deal.get("estimatedValue"))
        owner_obj = deal.get("ownedBy", {})
        owner_name = owner_obj.get("name", "—") if isinstance(owner_obj, dict) else "—"
        associated_contacts = deal.get("associatedContacts") or []
        associated_contacts_str = ", ".join(str(cid) for cid in associated_contacts) if associated_contacts else "—"
        products = deal.get("products") or []
        if products:
            prod_names = [p.get("name") or p.get("displayName") or f"ID:{p.get('id','?')}" for p in products]
            products_str = ", ".join(prod_names[:3]) + (" ..." if len(prod_names) > 3 else "")
        else:
            products_str = "—"
        lines.append(f"• ID: {did} | Name: {name} | Owner: {owner_name} | Value: {value} | Products: {products_str} | Contacts: {associated_contacts_str}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_deals(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter deals by specific field criteria. Use this to:
    - Get ALL deals: filters=[{"field":"id","operator":"is_not_null"}]
    - Get deals by field criteria (e.g., value > 50000, status = "Won")
    - Filter by any [FILTERABLE] field from get_deal_field_instructions

    DO NOT use this for keyword/text searches - use search_deals_by_term instead.
    Call get_deal_field_instructions first to get filterable fields and their types.

    **Returned fields include associated entities:**
    - associatedContacts (array of contact IDs)
    - associatedLeads (array of lead IDs)
    - associatedCompanies (array of company IDs)
    These are real IDs that can be used directly in subsequent searches without needing to look them up separately.

    filters: List of filter objects. Each must have:
      - field (str): Field internal/API name (e.g. name, value, dealSource, createdAt).
      - operator (str): One of the allowed operators for that field type (e.g. equal, contains, greater).
      - value: Value to compare. For PICK_LIST/MULTI_PICKLIST use Option ID (number), except
        currency, country, dealSource — use internal name (string).
      - timeZone (str, optional): For date/datetime filters only.
      - type (str, optional): Field type from cheat sheet.
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty. Provide at least one filter with field, operator, and value."
        return await search_deals_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_deals")
        return f"✗ Unexpected error: {str(e)}"


async def search_deals_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search deals by a single term across multiple fields via POST /search/deal with multi_field jsonRule."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = _multi_field_json_rule(term)
    payload = {
        "fields": ["id", "name", "value", "currency", "closingDate", "ownedBy", "createdAt", "actualValue", "estimatedValue"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching deals by term: %r", term)
    async with get_client() as client:
        response = await client.post("/search/deal", params=params, json=payload)
        data = await handle_api_response(response, "Search deals by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No deals found matching '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} deal(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for deal in results:
        did = deal.get("id", "?")
        name = deal.get("name", "—")
        value = _extract_primary_deal_value(deal.get("value"))
        actual_val = _extract_primary_deal_value(deal.get("actualValue"))
        estimated_val = _extract_primary_deal_value(deal.get("estimatedValue"))
        owner_obj = deal.get("ownedBy", {})
        owner_name = owner_obj.get("name", "—") if isinstance(owner_obj, dict) else "—"
        lines.append(f"• ID: {did} | Name: {name} | Owner: {owner_name} | Value: {value} | Actual: {actual_val} | Estimated: {estimated_val}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_idle_deals_logic(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Find deals with no activity for at least `days` days.
    Uses last-activity = max(updatedAt, latestActivityCreatedAt).
    If time_zone is not provided, uses current user's timezone.
    """
    if time_zone:
        tz = time_zone
    else:
        try:
            user = await _fetch_current_user()
            tz = user.get("timezone") or DEFAULT_TIMEZONE
        except Exception:
            tz = DEFAULT_TIMEZONE
    threshold_iso = _threshold_iso_days_ago(days, tz)
    base = {"operator": "less_or_equal", "value": threshold_iso, "timeZone": tz}
    fields_list = await _fetch_deal_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    filters = []
    for name in ("updatedAt", "latestActivityCreatedAt"):
        if name in filterable_map:
            filters.append({"field": name, **base})
    if not filters:
        return "Error: Neither 'updatedAt' nor 'latestActivityCreatedAt' is filterable for this tenant. Check get_deal_field_instructions."
    return await search_deals_logic(filters, page=page, size=size, sort=sort)


# ===========================================================================
# COMPANY ENTITY
# ===========================================================================

# ---------------------------------------------------------------------------
# Company idle search helper
# ---------------------------------------------------------------------------

async def search_idle_companies_logic(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Find companies with no activity for at least `days` days.
    Uses last-activity = max(updatedAt, latestActivityCreatedAt).
    If time_zone is not provided, uses current user's timezone.
    """
    if time_zone:
        tz = time_zone
    else:
        try:
            user = await _fetch_current_user()
            tz = user.get("timezone") or DEFAULT_TIMEZONE
        except Exception:
            tz = DEFAULT_TIMEZONE
    threshold_iso = _threshold_iso_days_ago(days, tz)
    base = {"operator": "less_or_equal", "value": threshold_iso, "timeZone": tz}
    fields_list = await _fetch_company_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    filters = []
    for name in ("updatedAt", "latestActivityCreatedAt"):
        if name in filterable_map:
            filters.append({"field": name, **base})
    if not filters:
        return "Error: Neither 'updatedAt' nor 'latestActivityCreatedAt' is filterable for this tenant. Check get_company_field_instructions."
    return await search_companies_logic(filters, page=page, size=size, sort=sort)


# ---------------------------------------------------------------------------
# Company field metadata helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Company field metadata helpers
# ---------------------------------------------------------------------------

async def _fetch_company_fields() -> List[Dict[str, Any]]:
    """Fetch company field metadata from Kylas API. Returns list of field dicts."""
    async with get_client() as client:
        response = await client.get(
            "/companies/fields",
            params={"page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch company fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def _get_company_custom_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom field ID (string) -> internal name."""
    fields = await _fetch_company_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


async def get_company_field_instructions_logic() -> str:
    fields = await _fetch_company_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    lines = [
        "=" * 60,
        "KYLAS CRM - COMPANY FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True))
    lines.extend(["", "=" * 60, "END OF CHEAT SHEET", "=" * 60])
    return "\n".join(lines)


@mcp.tool()
async def get_company_field_instructions() -> str:
    """
    Get all company fields for the current tenant. CALL THIS FIRST before creating or updating a company.
    Returns a cheat sheet with API names (standard fields), Field IDs (custom fields), and Picklist Option IDs.
    Use this to build field_values for create_company based on what the user wants—do not use static fields.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching company field instructions")
        result = await get_company_field_instructions_logic()
        return result
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_company_field_instructions")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Company search rule builder
# ---------------------------------------------------------------------------

def _build_company_search_json_rule(
    filters: List[Dict[str, Any]],
    filterable_map: Dict[str, Dict[str, Any]],
    default_timezone: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /search/company. Returns (jsonRule, error_message).
    Uses COMPANY_PICKLIST_FIELDS_USE_INTERNAL_NAME for picklist rule_type.
    """
    tz_for_date = default_timezone or DEFAULT_TIMEZONE
    rules = []
    for i, f in enumerate(filters):
        field_name = f.get("field")
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
        operator = OPERATOR_SYMBOL_MAP.get(operator, operator)
        value = f.get("value")
        field_type_key = (f.get("type") or "TEXT_FIELD").strip().upper().replace(" ", "_")

        if not field_name:
            return {}, f"Filter #{i + 1}: missing 'field'."
        if field_name not in filterable_map:
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only [FILTERABLE] fields from get_company_field_instructions."
        meta = filterable_map[field_name]
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        # Company-specific picklist handling
        if api_type in ("PICK_LIST", "MULTI_PICKLIST"):
            rule_type = "string" if field_name in COMPANY_PICKLIST_FIELDS_USE_INTERNAL_NAME else "long"
        else:
            rule_type = _rule_type_for_value(api_type, field_name, value)
        if rule_type in ("long", "double") and value is not None and not isinstance(value, (int, float)):
            try:
                value = float(value) if rule_type == "double" else int(value)
            except (TypeError, ValueError):
                value = value
        # Date/datetime fields: convert date values from user's timezone to UTC
        # e.g. "11th Aug 00:00" in Asia/Calcutta → "10th Aug 18:30" UTC
        if rule_type == "date" and value is not None:
            filter_tz = f.get("timeZone") or tz_for_date
            value = _convert_date_value_to_utc(value, filter_tz)

        is_custom = not meta.get("standard", True)
        rule_field = f"customFieldValues.{field_name}" if is_custom else field_name

        rule = {
            "operator": operator,
            "id": field_name,
            "field": rule_field,
            "type": rule_type,
            "value": value,
            "relatedFieldIds": None,
        }
        if rule_type == "date":
            rule["timeZone"] = f.get("timeZone") or tz_for_date
        rules.append(rule)

    return {"rules": rules, "condition": "AND", "valid": True}, None


# ---------------------------------------------------------------------------
# Company create/update/get logic
# ---------------------------------------------------------------------------

async def create_company_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a company with the given dynamic field_values."""
    fv = dict(field_values)
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_company_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    logger.info("Creating company with fields: %s", list(payload.keys()))
    async with get_client() as client:
        response = await client.post("/companies", json=payload)
        result = await handle_api_response(response, "Create company")
        logger.info("Company created with ID: %s", result.get("id"))
        return result


async def update_company_logic(company_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the company first, merge field_values into it, then PUT the full body. No partial update."""
    company_id = int(company_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_company_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values produced an empty payload.")
    logger.info("Updating company %s with fields: %s", company_id, list(payload.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/companies/{company_id}")
        existing = await handle_api_response(get_response, "Get company")
        merged = dict(existing)
        for key, value in payload.items():
            if key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            else:
                merged[key] = value
        response = await client.put(f"/companies/{company_id}", json=merged)
        result = await handle_api_response(response, "Update company")
        logger.info("Company %s updated", company_id)
        return result


async def get_company_logic(company_id: int) -> Dict[str, Any]:
    """Fetch a single company by ID (GET /companies/{id}). Returns full company object."""
    company_id = int(company_id)
    async with get_client() as client:
        response = await client.get(f"/companies/{company_id}")
        return await handle_api_response(response, "Get company")


def _format_company_for_display(company: Dict[str, Any]) -> str:
    """Format a company object into a readable multi-line string."""
    lines = ["=" * 60, "COMPANY DETAILS", "=" * 60]
    lines.append(f"ID: {company.get('id', '—')}")
    lines.append(f"Name: {company.get('name', '—')}")
    lines.append(f"Website: {company.get('website', '—')}")
    # Emails
    emails = company.get("emails") or []
    if emails:
        for e in emails:
            val = e.get("value", "")
            typ = e.get("type", "")
            prim = " (primary)" if e.get("primary") else ""
            lines.append(f"Email ({typ}): {val}{prim}")
    else:
        lines.append("Email: —")
    # Phones
    phones = company.get("phoneNumbers") or []
    if phones:
        for p in phones:
            code = p.get("code", "")
            val = p.get("value", "")
            typ = p.get("type", "")
            prim = " (primary)" if p.get("primary") else ""
            lines.append(f"Phone ({typ}): +{code} {val}{prim}")
    else:
        lines.append("Phone: —")
    lines.append(f"Owner ID: {company.get('ownerId', '—')}")
    lines.append(f"Created At: {company.get('createdAt', '—')}")
    lines.append(f"Updated At: {company.get('updatedAt', '—')}")
    # Custom fields
    custom = company.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_company(company_id: int) -> str:
    """
    Get full details of a company by ID (GET /companies/{id}). Use when the user asks for complete company info.
    company_id: The company ID (e.g. from search_companies or search_companies_by_term results).
    """
    try:
        _reset_api_call_count()
        company = await get_company_logic(company_id)
        return _format_company_for_display(company)
    except KylasAPIError as e:
        return f"✗ Failed to get company: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_company")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Company search logic
# ---------------------------------------------------------------------------

async def search_companies_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search companies with jsonRule; only filterable fields allowed."""
    fields_list = await _fetch_company_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable company fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_company_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {
        "fields": ["id", "name", "website", "emails", "phoneNumbers", "ownerId", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching companies with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/search/company", params=params, json=payload)
        data = await handle_api_response(response, "Search companies")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No companies found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} company(ies) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for company in results:
        cid = company.get("id", "?")
        name = company.get("name", "—")
        website = company.get("website", "—") or "—"
        email = _extract_primary_email(company.get("emails"))
        phone = _extract_primary_phone(company.get("phoneNumbers"))
        lines.append(f"• ID: {cid} | Name: {name} | Website: {website} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


# ===========================================================================
# MEETING ENTITY
# ===========================================================================

# ---------------------------------------------------------------------------
# Meeting field metadata helpers
# ---------------------------------------------------------------------------

MEETING_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"status", "medium"}

async def _fetch_meeting_fields() -> List[Dict[str, Any]]:
    """Fetch meeting field metadata from Kylas API. Returns list of field dicts."""
    async with get_client() as client:
        response = await client.get(
            "/meetings/fields",
            params={"custom-only": "false", "page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch meeting fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def _get_meeting_custom_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom field ID (string) -> internal name."""
    fields = await _fetch_meeting_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


def _format_meeting_field(field: Dict[str, Any]) -> List[str]:
    """Format a single meeting field for the cheat sheet."""
    lines = []
    name = field.get("name", "")
    display = field.get("displayName", name)
    field_type = field.get("type", "TEXT_FIELD")
    is_required = field.get("required", False)
    filterable = field.get("filterable", False)
    is_internal = field.get("internal", False)
    is_standard = field.get("standard", True)

    identifier = f"API name: {name}" if is_standard else f"Field ID: {field.get('id', '?')}"
    required_marker = " *REQUIRED*" if is_required else ""
    filterable_marker = " [FILTERABLE]" if filterable else ""
    internal_marker = " (internal)" if is_internal else ""

    lines.append(f"  '{display}' ({identifier}) - Type: {field_type}{required_marker}{filterable_marker}{internal_marker}")

    if field_type in ["ENTITY_PICKLIST", "PICK_LIST", "MULTI_PICKLIST"]:
        picklist = field.get("picklist") or {}
        values = picklist.get("picklistValues") or picklist.get("values", [])
        if values and field_type != "PICK_LIST":
            # For ENTITY_PICKLIST like status/medium, show internal names
            lines.append("  └─ Options (use internal name):")
            for val in values:
                if not isinstance(val, dict):
                    continue
                val_label = val.get("displayName") or val.get("name") or "Unknown"
                val_name = val.get("name", "")
                lines.append(f"     • {val_label} (name: '{val_name}')")
    return lines


async def get_meeting_field_instructions_logic() -> str:
    fields = await _fetch_meeting_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    lines = [
        "=" * 60,
        "KYLAS CRM - MEETING FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_meeting_field(f))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_meeting_field(f))
    lines.extend([
        "",
        "## CREATE MEETING PAYLOAD FORMAT",
        "-" * 40,
        "Required fields: title, from, to, participants (at least one user)",
        "",
        "participants: [{\"id\": <user_id>, \"entity\": \"user\"}]",
        "  - Use lookup_users for users only; use lookup_meeting_related_entity with entity_type='invitee' to resolve users/leads/contacts/external for meetings",
        "  - RULES: Leads/contacts must have a valid email to be an invitee. Deals CANNOT be invitees. If user asks for a deal as invitee, omit it and inform them.",
        "organizer / invitee by name: call lookup_meeting_related_entity with entity_type='invitee' first, then use the matching id + entity",
        "",
        "relatedTo: [{\"id\": <entity_id>, \"entity\": \"lead|contact|deal|company\"}]",
        "  - Links meeting to leads, contacts, deals, or companies",
        "Meeting search: associatedLeads, associatedContacts, associatedDeals, associatedCompanies — is_null / is_not_null or equal <id> (type long). Resolve ids via lookup_meeting_related_entity first.",
        "",
        "timezone: {\"id\": <tz_picklist_id>, \"name\": \"Asia/Calcutta\"}",
        "  - Use timezone picklist ID from field instructions above",
        "",
        "from/to: UTC ISO datetime strings (e.g. \"2024-01-15T08:00:00.000Z\")",
        "  - Convert user's local time to UTC using parse_datetime_to_utc_iso_tool",
        "",
        "allDay: true/false (default false)",
        "",
        "=" * 60,
        "END OF CHEAT SHEET",
        "=" * 60,
    ])
    return "\n".join(lines)


@mcp.tool()
async def get_meeting_field_instructions() -> str:
    """
    Get all meeting fields for the current tenant. CALL THIS FIRST before creating or updating a meeting.
    Returns a cheat sheet with API names, picklist options, and required fields.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching meeting field instructions")
        result = await get_meeting_field_instructions_logic()
        return result
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_meeting_field_instructions")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Meeting invitee / organizer lookup (users, leads, contacts, external)
# ---------------------------------------------------------------------------

def _primary_email_from_invitee_row(row: Dict[str, Any]) -> str:
    emails = row.get("emails") or []
    if not emails:
        return "—"
    for e in emails:
        if isinstance(e, dict) and e.get("primary"):
            return str(e.get("value") or "—")
    first = emails[0]
    if isinstance(first, dict):
        return str(first.get("value") or "—")
    return "—"


async def lookup_meeting_invitees_logic(query: str) -> str:
    """
    GET /search/meeting-invitee/lookup?q=<query>
    Returns users, leads, contacts, and external invitees (id, name, entity).
    """
    q = (query or "").strip()
    if not q:
        return (
            "Error: query cannot be empty. Examples: "
            "'key:' for a broad list, 'name:Akshay' if the API supports field:value, "
            "or a partial name string."
        )
    logger.info("Meeting invitee lookup: q=%s", q)
    async with get_client() as client:
        response = await client.get(
            "/search/meeting-invitee/lookup",
            params={"q": q},
        )
        data = await handle_api_response(response, "Meeting invitee lookup")
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("content", data.get("data", []))
    else:
        rows = []
    if not rows:
        return f"No meeting invitees found matching '{q}'."
    lines = [
        f"Found {len(rows)} invitee(s) matching '{q}'",
        "-" * 60,
        "Use id + entity when adding participants or resolving organizer (organizer is usually entity=user).",
        "-" * 60,
    ]
    for row in rows:
        if not isinstance(row, dict):
            continue
        rid = row.get("id", "?")
        name = row.get("name", "—")
        entity = row.get("entity", "—")
        email = _primary_email_from_invitee_row(row)
        lines.append(f"  • ID: {rid}  |  Entity: {entity}  |  Name: {name}  |  Email: {email}")
    lines.append("-" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Meeting related-entity lookup (resolve IDs before meetings/search filters)
# ---------------------------------------------------------------------------

def _meeting_lookup_rows_from_response(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        inner = data.get("content", data.get("data", []))
        if isinstance(inner, list):
            return [r for r in inner if isinstance(r, dict)]
    return []


def _format_meeting_entity_lookup_result(label: str, q: str, rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return f"No {label} found matching lookup query '{q}'."
    lines = [
        f"Found {len(rows)} {label} matching '{q}' (use id in search_meetings filter associatedLeads / associatedContacts / associatedDeals / associatedCompanies)",
        "-" * 60,
    ]
    for row in rows:
        rid = row.get("id", "?")
        name = row.get("name") or row.get("displayName") or "—"
        extra = _primary_email_from_invitee_row(row)
        if extra != "—":
            lines.append(f"  • ID: {rid}  |  Name: {name}  |  Email: {extra}")
        else:
            lines.append(f"  • ID: {rid}  |  Name: {name}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def lookup_leads_for_meeting_logic(query: str) -> str:
    """GET /search/lead/lookup?q= — meeting context (same as Kylas web)."""
    q = (query or "firstName:").strip() or "firstName:"
    logger.info("Meeting lead lookup: q=%s", q)
    async with get_client() as client:
        response = await client.get("/search/lead/lookup", params={"q": q})
        data = await handle_api_response(response, "Lookup leads for meeting")
    rows = _meeting_lookup_rows_from_response(data)
    return _format_meeting_entity_lookup_result("lead(s)", q, rows)


async def lookup_contacts_for_meeting_logic(query: str) -> str:
    """GET /search/contact/lookup?q= — meeting context."""
    q = (query or "firstName:").strip() or "firstName:"
    logger.info("Meeting contact lookup: q=%s", q)
    async with get_client() as client:
        response = await client.get("/search/contact/lookup", params={"q": q})
        data = await handle_api_response(response, "Lookup contacts for meeting")
    rows = _meeting_lookup_rows_from_response(data)
    return _format_meeting_entity_lookup_result("contact(s)", q, rows)


async def lookup_deals_for_meeting_logic(query: str) -> str:
    """GET /search/deal/lookup?q= — meeting context."""
    q = (query or "name:").strip() or "name:"
    logger.info("Meeting deal lookup: q=%s", q)
    async with get_client() as client:
        response = await client.get("/search/deal/lookup", params={"q": q})
        data = await handle_api_response(response, "Lookup deals for meeting")
    rows = _meeting_lookup_rows_from_response(data)
    return _format_meeting_entity_lookup_result("deal(s)", q, rows)


async def lookup_companies_for_meeting_logic(query: str) -> str:
    """GET /companies/lookup?view=meeting&q= — meeting context."""
    q = (query or "comp:").strip() or "comp:"
    logger.info("Meeting company lookup: q=%s", q)
    async with get_client() as client:
        response = await client.get("/companies/lookup", params={"view": "meeting", "q": q})
        data = await handle_api_response(response, "Lookup companies for meeting")
    rows = _meeting_lookup_rows_from_response(data)
    return _format_meeting_entity_lookup_result("compan(y/ies)", q, rows)


# ---------------------------------------------------------------------------
# Meeting search rule builder
# ---------------------------------------------------------------------------

# POST /meetings/search supports these fields even when GET /meetings/fields omits them from metadata.
_MEETING_SEARCH_SYNTHETIC_FILTERABLE: Dict[str, Dict[str, Any]] = {
    "associatedLeads": {"type": "LOOK_UP", "standard": True},
    "associatedContacts": {"type": "LOOK_UP", "standard": True},
    "associatedDeals": {"type": "LOOK_UP", "standard": True},
    "associatedCompanies": {"type": "LOOK_UP", "standard": True},
    "participants": {"type": "PARTICIPANTS_LOOKUP", "standard": True},
}


def _build_meeting_search_json_rule(
    filters: List[Dict[str, Any]],
    filterable_map: Dict[str, Dict[str, Any]],
    default_timezone: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /meetings/search. Returns (jsonRule, error_message).
    Uses MEETING_PICKLIST_FIELDS_USE_INTERNAL_NAME for picklist rule_type.
    """
    tz_for_date = default_timezone or DEFAULT_TIMEZONE
    rules = []
    for i, f in enumerate(filters):
        field_name = f.get("field")
        if field_name == "scheduledAt":
            field_name = "from"
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
        operator = OPERATOR_SYMBOL_MAP.get(operator, operator)
        value = f.get("value")

        if not field_name:
            return {}, f"Filter #{i + 1}: missing 'field'."
        if field_name in _MEETING_SEARCH_SYNTHETIC_FILTERABLE:
            meta = dict(_MEETING_SEARCH_SYNTHETIC_FILTERABLE[field_name])
        elif field_name in filterable_map:
            meta = filterable_map[field_name]
        else:
            return {}, (
                f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. "
                "Use [FILTERABLE] from get_meeting_field_instructions, or synthetic meeting fields: "
                "associatedLeads, associatedContacts, associatedDeals, associatedCompanies (LOOK_UP; equal / is_null / is_not_null)."
            )
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        # Meeting-specific picklist handling
        if api_type in ("PICK_LIST", "MULTI_PICKLIST", "ENTITY_PICKLIST"):
            rule_type = "string" if field_name in MEETING_PICKLIST_FIELDS_USE_INTERNAL_NAME else "long"
        else:
            rule_type = _rule_type_for_value(api_type, field_name, value)
        if rule_type in ("long", "double") and value is not None and not isinstance(value, (int, float)):
            try:
                value = float(value) if rule_type == "double" else int(value)
            except (TypeError, ValueError):
                value = value
        # Date/datetime fields: convert date values from user's timezone to UTC
        # e.g. "11th Aug 00:00" in Asia/Calcutta → "10th Aug 18:30" UTC
        if rule_type == "date" and value is not None:
            filter_tz = f.get("timeZone") or tz_for_date
            value = _convert_date_value_to_utc(value, filter_tz)

        is_custom = not meta.get("standard", True)
        rule_field = f"customFieldValues.{field_name}" if is_custom else field_name

        rule = {
            "operator": operator,
            "id": field_name,
            "field": rule_field,
            "type": rule_type,
            "value": value,
            "relatedFieldIds": None,
        }
        if rule_type == "date":
            rule["timeZone"] = f.get("timeZone") or tz_for_date
        rules.append(rule)

    return {"rules": rules, "condition": "AND", "valid": True}, None


# ---------------------------------------------------------------------------
# Meeting create/update/get logic
# ---------------------------------------------------------------------------

async def create_meeting_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a meeting with the given field_values."""
    payload = dict(field_values)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    # Ensure required fields
    if not payload.get("title"):
        raise KylasAPIError("title is required for creating a meeting")
    if not payload.get("from"):
        raise KylasAPIError("'from' datetime is required for creating a meeting")
    if not payload.get("to"):
        raise KylasAPIError("'to' datetime is required for creating a meeting")
    if not payload.get("participants"):
        raise KylasAPIError("participants is required (at least one user). Use [{\"id\": <user_id>, \"entity\": \"user\"}]")
        
    # Strip deals from participants
    if "participants" in payload and isinstance(payload["participants"], list):
        filtered_participants = [p for p in payload["participants"] if p.get("entity") != "deal"]
        if not filtered_participants:
            raise KylasAPIError("participants is required and cannot be a deal. Deals cannot be invitees.")
        payload["participants"] = filtered_participants

    logger.info("Creating meeting: %s", payload.get("title", ""))
    async with get_client() as client:
        response = await client.post("/meetings", json=payload)
        result = await handle_api_response(response, "Create meeting")
        logger.info("Meeting created with ID: %s", result.get("id"))
        return result


async def update_meeting_logic(meeting_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the meeting first, merge field_values into it, then PUT the full body."""
    meeting_id = int(meeting_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    logger.info("Updating meeting %s with fields: %s", meeting_id, list(fv.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/meetings/{meeting_id}")
        existing = await handle_api_response(get_response, "Get meeting")
        merged = dict(existing)
        for key, value in fv.items():
            if key == "participants" and isinstance(value, list):
                # Merge participants, avoid duplicates by (id, entity)
                existing_participants = merged.get("participants", []) or []
                existing_keys = {(p.get("id"), p.get("entity")) for p in existing_participants if isinstance(p, dict)}
                for p in value:
                    if isinstance(p, dict) and p.get("entity") != "deal" and (p.get("id"), p.get("entity")) not in existing_keys:
                        existing_participants.append(p)
                merged["participants"] = existing_participants
            elif key == "relatedTo" and isinstance(value, list):
                # Merge relatedTo, avoid duplicates by (id, entity)
                existing_related = merged.get("relatedTo", []) or []
                existing_keys = {(r.get("id"), r.get("entity")) for r in existing_related if isinstance(r, dict)}
                for r in value:
                    if isinstance(r, dict) and (r.get("id"), r.get("entity")) not in existing_keys:
                        existing_related.append(r)
                merged["relatedTo"] = existing_related
            elif key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            else:
                merged[key] = value
        response = await client.put(f"/meetings/{meeting_id}", json=merged)
        result = await handle_api_response(response, "Update meeting")
        logger.info("Meeting %s updated", meeting_id)
        return result


async def get_meeting_logic(meeting_id: int) -> Dict[str, Any]:
    """Fetch a single meeting by ID (GET /meetings/{id})."""
    meeting_id = int(meeting_id)
    async with get_client() as client:
        response = await client.get(f"/meetings/{meeting_id}")
        return await handle_api_response(response, "Get meeting")


def _format_meeting_for_display(meeting: Dict[str, Any]) -> str:
    """Format a meeting object into a readable multi-line string."""
    lines = ["=" * 60, "MEETING DETAILS", "=" * 60]
    lines.append(f"ID: {meeting.get('id', '—')}")
    lines.append(f"Title: {meeting.get('title', '—')}")
    lines.append(f"Status: {meeting.get('status', '—')}")
    lines.append(f"From: {meeting.get('from', '—')}")
    lines.append(f"To: {meeting.get('to', '—')}")
    lines.append(f"All Day: {meeting.get('allDay', False)}")
    lines.append(f"Location: {meeting.get('location', '—')}")
    lines.append(f"Description: {meeting.get('description', '—')}")
    # Medium
    medium = meeting.get("medium")
    if isinstance(medium, dict):
        lines.append(f"Medium: {medium.get('displayName', medium.get('name', '—'))}")
    elif medium:
        lines.append(f"Medium: {medium}")
    # Provider Link
    provider_link = meeting.get("providerLink")
    if provider_link:
        lines.append(f"Joining Link: {provider_link}")
    # Timezone
    tz = meeting.get("timezone") or {}
    if isinstance(tz, dict):
        lines.append(f"Timezone: {tz.get('name', '—')}")
    # Owner
    owner = meeting.get("owner") or {}
    if isinstance(owner, dict):
        lines.append(f"Owner: {owner.get('name', '—')} (ID: {owner.get('id', '—')})")
    # Participants
    participants = meeting.get("participants") or []
    if participants:
        lines.append("Participants:")
        for p in participants:
            if isinstance(p, dict):
                pname = p.get("name", "—")
                pemail = p.get("email", "")
                pentity = p.get("entity", "")
                rsvp = p.get("rsvpResponse", "—")
                lines.append(f"  • {pname} ({pentity}) - {pemail} [RSVP: {rsvp}]")
    # Related To
    related = meeting.get("relatedTo") or []
    if related:
        lines.append("Related To:")
        for r in related:
            if isinstance(r, dict):
                rname = r.get("name", "—")
                rentity = r.get("entity", "")
                rid = r.get("id", "—")
                lines.append(f"  • {rname} ({rentity}, ID: {rid})")
    # Metadata
    lines.append(f"Created By: {(meeting.get('createdBy') or {}).get('name', '—')}")
    lines.append(f"Created At: {meeting.get('createdAt', '—')}")
    lines.append(f"Updated At: {meeting.get('updatedAt', '—')}")
    # Custom fields
    custom = meeting.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_meeting(meeting_id: int) -> str:
    """
    Get full details of a meeting by ID (GET /meetings/{id}).
    meeting_id: The meeting ID.
    """
    try:
        _reset_api_call_count()
        meeting = await get_meeting_logic(meeting_id)
        return _format_meeting_for_display(meeting)
    except KylasAPIError as e:
        return f"✗ Failed to get meeting: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_meeting")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Meeting cancel and delete
# ---------------------------------------------------------------------------

@mcp.tool()
async def cancel_meeting(meeting_id: int) -> str:
    """
    Cancel a scheduled meeting. Changes the meeting status to 'cancelled'.
    meeting_id: The meeting ID to cancel.
    """
    try:
        _reset_api_call_count()
        meeting_id = int(meeting_id)
        logger.info("Cancelling meeting %s", meeting_id)
        async with get_client() as client:
            # First get the meeting, then POST to cancel
            get_response = await client.get(f"/meetings/{meeting_id}")
            existing = await handle_api_response(get_response, "Get meeting")
            response = await client.post(f"/meetings/{meeting_id}/cancel", json=existing)
            await handle_api_response(response, "Cancel meeting")
            return f"✓ Meeting {meeting_id} cancelled successfully."
    except KylasAPIError as e:
        return f"✗ Failed to cancel meeting: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("cancel_meeting")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def delete_meeting(meeting_id: int) -> str:
    """
    Permanently delete a meeting. This action cannot be undone.
    meeting_id: The meeting ID to delete.
    """
    try:
        _reset_api_call_count()
        meeting_id = int(meeting_id)
        logger.info("Deleting meeting %s", meeting_id)
        async with get_client() as client:
            response = await client.delete(f"/meetings/{meeting_id}")
            await handle_api_response(response, "Delete meeting")
            return f"✓ Meeting {meeting_id} deleted successfully."
    except KylasAPIError as e:
        return f"✗ Failed to delete meeting: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("delete_meeting")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Meeting search logic
# ---------------------------------------------------------------------------

# Unfiltered list matches Kylas web: POST /meetings/search with empty rules (not jsonRule null).
_MEETING_SEARCH_JSON_RULE_ALL: Dict[str, Any] = {"condition": "AND", "rules": [], "valid": True}

# Meetings API only supports a limited set of sortable fields (e.g. 'from', 'createdAt').
# LLMs and the generic search dispatcher often send unsupported sort fields like
# 'updatedAt' or 'scheduledAt', causing 422 errors. This map normalizes them.
_MEETING_SORT_FIELD_ALIASES: Dict[str, str] = {
    "scheduledAt": "from",
    "updatedAt": "from",
    "startTime": "from",
    "startDate": "from",
}


def _normalize_meeting_sort(sort: Optional[str]) -> str:
    """Normalize meeting sort parameter: map unsupported fields to valid ones."""
    if not sort or not isinstance(sort, str):
        return "from,desc"
    parts = sort.split(",", 1)
    field = parts[0].strip()
    direction = parts[1].strip() if len(parts) > 1 else "desc"
    field = _MEETING_SORT_FIELD_ALIASES.get(field, field)
    return f"{field},{direction}"


def _meetings_search_api_page(page_zero_based: int) -> int:
    """POST /meetings/search uses 1-based page; tools stay 0-based."""
    return int(page_zero_based) + 1


def _format_meeting_summary_line(m: Dict[str, Any]) -> str:
    """Format a meeting object into a concise 1-line summary including owner, organizer, and conductor details."""
    mid = m.get("id", "?")
    title = m.get("title", "—")
    status = m.get("status", "—")
    from_dt = m.get("from", "—")
    to_dt = m.get("to", "—")
    location = m.get("location") or "—"
    
    # Extract owner
    owner_val = m.get("owner")
    owner_name = "—"
    if isinstance(owner_val, dict):
        owner_name = owner_val.get("name") or str(owner_val.get("id", "—"))
    elif owner_val:
        owner_name = str(owner_val)
        
    # Extract organizer
    organizer_val = m.get("organizer")
    org_name = "—"
    if isinstance(organizer_val, dict):
        org_name = organizer_val.get("name") or organizer_val.get("email") or str(organizer_val.get("id", "—"))
    elif organizer_val:
        org_name = str(organizer_val)
        
    # Extract conductedBy
    conducted_val = m.get("conductedBy")
    conducted_name = "—"
    if isinstance(conducted_val, dict):
        conducted_name = conducted_val.get("name") or str(conducted_val.get("id", "—"))
    elif conducted_val:
        conducted_name = str(conducted_val)
        
    return f"• ID: {mid} | Title: {title} | Status: {status} | From: {from_dt} | To: {to_dt} | Location: {location} | Owner: {owner_name} | Organizer: {org_name} | Conducted By: {conducted_name}"


async def search_meetings_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "from,desc",
) -> str:
    """Search meetings with jsonRule; only filterable fields allowed."""
    fields_list = await _fetch_meeting_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable meeting fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_meeting_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {"jsonRule": json_rule}
    sort = _normalize_meeting_sort(sort)
    params = {"page": _meetings_search_api_page(page), "size": min(size, 500), "sort": sort}
    logger.info("Searching meetings with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/meetings/search", params=params, json=payload)
        data = await handle_api_response(response, "Search meetings")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No meetings found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} meeting(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for m in results:
        lines.append(_format_meeting_summary_line(m))
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def lookup_meeting_related_entity(entity_type: str, query: str = "") -> str:
    """
    Look up related entities or invitees for meeting association / search filters.
    
    Call this FIRST when the user wants meetings linked to a specific entity or person by name.
    
    entity_type (str): One of "lead", "contact", "deal", "company", or "invitee".
      - For lead/contact/deal/company, use the returned ID with search_meetings on field:
        associatedLeads, associatedContacts, associatedDeals, associatedCompanies.
      - For invitee, use the FULL RETURNED OBJECT (e.g., {"id": 123, "entity": "lead", "name": "...", "emails": [...]})
        with search_meetings on the `participants` field with operator `in`.
    
    query (str): Lookup search string (e.g., "firstName:John" or "comp:Acme"). Leave empty for default.
    """
    try:
        _reset_api_call_count()
        entity_type = entity_type.strip().lower()
        if entity_type == "lead":
            return await lookup_leads_for_meeting_logic(query or "firstName:")
        elif entity_type == "contact":
            return await lookup_contacts_for_meeting_logic(query or "firstName:")
        elif entity_type == "deal":
            return await lookup_deals_for_meeting_logic(query or "name:")
        elif entity_type == "company":
            return await lookup_companies_for_meeting_logic(query or "comp:")
        elif entity_type == "invitee":
            return await lookup_meeting_invitees_logic(query)
        else:
            return f"Error: Unsupported entity_type '{entity_type}'. Must be lead, contact, deal, company, or invitee."
    except KylasAPIError as e:
        return f"Error: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_meeting_related_entity")
        return f"Unexpected error: {str(e)}"

async def search_meetings(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "from,desc",
) -> str:
    """
    Search/filter meetings. Use [FILTERABLE] from get_meeting_field_instructions, plus synthetic fields:
    associatedLeads, associatedContacts, associatedDeals, associatedCompanies (long; equal / is_null / is_not_null),
    and participants (participants_lookup; in / not_in).
    
    Resolve entity ids or participant objects with lookup_meeting_related_entity before filtering.
    For `participants`, value MUST be a list containing the full participant object from lookup_meeting_related_entity.

    filters: List of filter objects. Each must have:
      - field (str): e.g. title, status, from, owner, associatedLeads, participants.
      - operator (str): e.g. equal, contains, in.
      - value: For status use internal name. For associated* equal, numeric id. For participants, list of full objects.
      - timeZone (str, optional): For date/datetime filters.
    page: 0-based page (default 0).
    size: Page size, max 500 (default 20).
    sort: Sort e.g. "from,desc" (default).
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty."
        return await search_meetings_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_meetings")
        return f"✗ Unexpected error: {str(e)}"


# ===========================================================================
# CALL LOG ENTITY
# ===========================================================================

# ---------------------------------------------------------------------------
# Call Log field metadata helpers
# ---------------------------------------------------------------------------

CALL_LOG_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"outcome", "callType", "overallSentiment", "callDisposition", "customerEmotion"}


async def _fetch_call_log_fields() -> List[Dict[str, Any]]:
    """Fetch call log field metadata from Kylas API."""
    async with get_client() as client:
        response = await client.get(
            "/call-logs/fields",
            params={"custom-only": "false", "page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch call log fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def get_call_log_field_instructions_logic() -> str:
    fields = await _fetch_call_log_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    lines = [
        "=" * 60,
        "KYLAS CRM - CALL LOG FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_meeting_field(f))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_meeting_field(f))
    lines.extend([
        "",
        "## CREATE CALL LOG PAYLOAD FORMAT",
        "-" * 40,
        "Required: outcome, startTime, phoneNumber, callType",
        "",
        "outcome: 'connected', 'rejected', 'busy', 'no_answer', 'missed_call', 'in_progress'",
        "callType: 'incoming' or 'outgoing'",
        "startTime: UTC ISO datetime (e.g. '2024-01-15T08:00:00.000Z')",
        "phoneNumber: phone number string (e.g. '9618488578')",
        "duration: call duration in seconds (e.g. 420)",
        "",
        "relatedTo: {\"id\": <entity_id>, \"entity\": \"lead|contact|deal\", \"phoneNumber\": \"...\"}",
        "  - Links call log to a lead, contact, or deal",
        "",
        "associatedTo (optional, for deal calls): [{\"id\": <contact_id>, \"entity\": \"contact\", \"phoneNumber\": \"...\"}]",
        "  - Associate a contact when logging a call on a deal",
        "",
        "notes (optional): [{\"description\": \"Note text\"}]",
        "",
        "callRecording (optional): {\"url\": \"https://...\", \"fileName\": \"call.mp3\", \"data\": \"\"}",
        "",
        "=" * 60,
        "END OF CHEAT SHEET",
        "=" * 60,
    ])
    return "\n".join(lines)


@mcp.tool()
async def get_call_log_field_instructions() -> str:
    """
    Get all call log fields for the current tenant. CALL THIS FIRST before creating a call log.
    Returns a cheat sheet with field names, outcome/callType options, and payload format.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching call log field instructions")
        result = await get_call_log_field_instructions_logic()
        return result
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_call_log_field_instructions")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Call Log create/update/get logic
# ---------------------------------------------------------------------------

async def create_call_log_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a call log with the given field_values."""
    payload = dict(field_values)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    if not payload.get("outcome"):
        raise KylasAPIError("outcome is required (e.g. 'connected', 'rejected', 'busy', 'no_answer', 'missed_call')")
    if not payload.get("startTime"):
        raise KylasAPIError("startTime is required (UTC ISO datetime)")
    if not payload.get("phoneNumber"):
        raise KylasAPIError("phoneNumber is required")
    if not payload.get("callType"):
        raise KylasAPIError("callType is required ('incoming' or 'outgoing')")
    if not payload.get("relatedTo"):
        raise KylasAPIError("relatedTo is required — specify which entity this call is for: {\"id\": <id>, \"entity\": \"lead|contact|deal\", \"phoneNumber\": \"...\"}")

    logger.info("Creating call log: %s on %s", payload.get("callType"), payload.get("relatedTo", {}).get("entity", "?"))
    async with get_client() as client:
        response = await client.post("/call-logs/", json=payload)
        result = await handle_api_response(response, "Create call log")
        logger.info("Call log created with ID: %s", result.get("id"))
        return result


async def update_call_log_logic(call_log_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Update a call log via PUT (full replace)."""
    call_log_id = int(call_log_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    logger.info("Updating call log %s", call_log_id)
    async with get_client() as client:
        response = await client.put(f"/call-logs/{call_log_id}", json=fv)
        result = await handle_api_response(response, "Update call log")
        logger.info("Call log %s updated", call_log_id)
        return result


def _format_call_log_for_display(log: Dict[str, Any]) -> str:
    """Format a call log object into a readable multi-line string."""
    lines = ["=" * 60, "CALL LOG DETAILS", "=" * 60]
    lines.append(f"ID: {log.get('id', '—')}")
    lines.append(f"Call Type: {log.get('callType', '—')}")
    lines.append(f"Outcome: {log.get('outcome', '—')}")
    lines.append(f"Phone Number: {log.get('phoneNumber', '—')}")
    lines.append(f"Start Time: {log.get('startTime', '—')}")
    lines.append(f"Duration: {log.get('duration', '—')} seconds")
    # Related To
    related = log.get("relatedTo") or {}
    if isinstance(related, dict):
        lines.append(f"Related To: {related.get('name', '—')} ({related.get('entity', '—')}, ID: {related.get('id', '—')})")
    # Associated To
    associated = log.get("associatedTo") or []
    if associated:
        lines.append("Associated To:")
        for a in associated:
            if isinstance(a, dict):
                lines.append(f"  • {a.get('name', '—')} ({a.get('entity', '—')}, ID: {a.get('id', '—')})")
    # Notes
    notes = log.get("notes") or []
    if notes:
        lines.append("Notes:")
        for n in notes:
            if isinstance(n, dict):
                lines.append(f"  • {n.get('description', '—')}")
    # Call Recording
    recording = log.get("callRecording") or {}
    if isinstance(recording, dict) and recording.get("url"):
        lines.append(f"Recording: {recording.get('fileName', '—')} — {recording.get('url', '—')}")
    # Call Summary & Sentiment
    summary = log.get("callSummary")
    if summary:
        lines.append(f"Call Summary: {summary}")
    sentiment = log.get("overallSentiment")
    if sentiment:
        lines.append(f"Overall Sentiment: {sentiment}")
    disposition = log.get("callDisposition")
    if disposition:
        lines.append(f"Call Disposition: {disposition}")
    # Metadata
    owner = log.get("owner") or {}
    if isinstance(owner, dict):
        lines.append(f"Logged By: {owner.get('name', '—')} (ID: {owner.get('id', '—')})")
    lines.append(f"Created At: {log.get('createdAt', '—')}")
    lines.append(f"Updated At: {log.get('updatedAt', '—')}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_call_logs(entity_id: int, entity_type: str, page: int = 0, size: int = 20) -> str:
    """
    Get call logs for a specific lead, contact, or deal. Includes associated entity details.

    entity_id: The ID of the lead, contact, or deal.
    entity_type: "lead", "contact", or "deal".
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    """
    try:
        _reset_api_call_count()
        entity_type_lower = entity_type.strip().lower()
        if entity_type_lower not in ["lead", "contact", "deal"]:
            return f"✗ Invalid entity type: '{entity_type}'. Must be one of: lead, contact, deal"
        entity_id = int(entity_id)
        logger.info("Fetching call logs for %s %s", entity_type_lower, entity_id)

        # Fetch entity details to provide context
        entity_details_str = ""
        try:
            if entity_type_lower == "lead":
                entity = await get_lead_logic(entity_id)
                entity_details_str = _format_lead_for_display(entity)
            elif entity_type_lower == "contact":
                entity = await get_contact_logic(entity_id)
                entity_details_str = _format_contact_for_display(entity)
            elif entity_type_lower == "deal":
                entity = await get_deal_logic(entity_id)
                entity_details_str = _format_deal_for_display(entity)
        except Exception as e:
            logger.warning("Failed to fetch %s details for ID %s: %s", entity_type_lower, entity_id, str(e))

        async with get_client() as client:
            response = await client.get(
                "/call-logs",
                params={
                    "relatedToId": entity_id,
                    "relatedToType": entity_type_lower,
                    "page": int(page) + 1,  # Kylas call-logs listing is 1-based
                    "size": min(size, 100),
                },
            )
            data = await handle_api_response(response, "Get call logs")

        # Handle paginated response
        if isinstance(data, dict):
            results = data.get("content", data.get("data", []))
            total = data.get("totalElements", data.get("total", len(results)))
            total_pages = data.get("totalPages", 1)
        elif isinstance(data, list):
            results = data
            total = len(results)
            total_pages = 1
        else:
            results = []
            total = 0
            total_pages = 1

        if not results:
            return f"No call logs found for {entity_type_lower} {entity_id}."

        lines = []
        # Include entity details if available
        if entity_details_str:
            lines.append("=" * 60)
            lines.append("ENTITY DETAILS")
            lines.append("=" * 60)
            lines.extend(entity_details_str.split("\n"))
            lines.append("")

        lines.append("=" * 60)
        lines.append("CALL LOGS")
        lines.append("=" * 60)
        lines.append(f"Found {len(results)} call log(s) (total {total})")
        lines.append("")
        for log in results:
            lines.append(_format_call_log_for_display(log))
            lines.append("")
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Failed to get call logs: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_call_logs")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Call Log search logic
# ---------------------------------------------------------------------------

def _call_logs_search_api_page(page_zero_based: int) -> int:
    """POST /call-logs/search expects 1-based page (matches Kylas web app); tools stay 0-based."""
    return int(page_zero_based) + 1


def _build_call_log_search_json_rule(
    filters: List[Dict[str, Any]],
    filterable_map: Dict[str, Dict[str, Any]],
    default_timezone: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /call-logs/search. Returns (jsonRule, error_message).
    Uses CALL_LOG_PICKLIST_FIELDS_USE_INTERNAL_NAME for picklist rule_type.
    """
    tz_for_date = default_timezone or DEFAULT_TIMEZONE
    rules = []
    for i, f in enumerate(filters):
        field_name = f.get("field")
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
        operator = OPERATOR_SYMBOL_MAP.get(operator, operator)
        value = f.get("value")

        if not field_name:
            return {}, f"Filter #{i + 1}: missing 'field'."
        if field_name not in filterable_map:
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only [FILTERABLE] fields from get_call_log_field_instructions."
        meta = filterable_map[field_name]
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        # Call log picklist handling
        if api_type in ("PICK_LIST", "MULTI_PICKLIST", "ENTITY_PICKLIST"):
            rule_type = "string" if field_name in CALL_LOG_PICKLIST_FIELDS_USE_INTERNAL_NAME else "long"
        else:
            rule_type = _rule_type_for_value(api_type, field_name, value)
        if rule_type in ("long", "double") and value is not None and not isinstance(value, (int, float)):
            try:
                value = float(value) if rule_type == "double" else int(value)
            except (TypeError, ValueError):
                value = value
        # Date/datetime fields: convert date values from user's timezone to UTC
        # e.g. "11th Aug 00:00" in Asia/Calcutta → "10th Aug 18:30" UTC
        if rule_type == "date" and value is not None:
            filter_tz = f.get("timeZone") or tz_for_date
            value = _convert_date_value_to_utc(value, filter_tz)

        is_custom = not meta.get("standard", True)
        rule_field = f"customFieldValues.{field_name}" if is_custom else field_name

        rule = {
            "operator": operator,
            "id": field_name,
            "field": rule_field,
            "type": rule_type,
            "value": value,
            "relatedFieldIds": None,
        }
        if rule_type == "date":
            rule["timeZone"] = f.get("timeZone") or tz_for_date
        rules.append(rule)

    return {"rules": rules, "condition": "AND", "valid": True}, None


def _extract_call_log_data(log: Dict[str, Any]) -> dict:
    """Extract data from a call log record."""
    lid = str(log.get("id", "?"))
    call_type = log.get("callType", "—")
    outcome = log.get("outcome", "—")
    phone = log.get("phoneNumber", "—")
    start = log.get("startTime", "—")
    duration = log.get("duration", "—")

    # Extract sentiment: overallSentiment + customerEmotion (first one)
    overall_sentiment = log.get("overallSentiment", "—")
    customer_emotions = log.get("customerEmotion") or []
    emotion_name = "—"
    if isinstance(customer_emotions, list) and len(customer_emotions) > 0:
        emotion = customer_emotions[0]
        if isinstance(emotion, dict):
            emotion_name = emotion.get("name", "—")
    sentiment = f"{overall_sentiment}/{emotion_name}" if overall_sentiment != "—" else emotion_name

    # Extract entity info from relatedTo (it's an array)
    related_list = log.get("relatedTo") or []
    related_name = "—"
    if isinstance(related_list, list) and len(related_list) > 0:
        related = related_list[0]
        if isinstance(related, dict):
            name = related.get("name", "—")
            entity = related.get("entity", "—")
            eid = related.get("id", "?")
            related_name = f"{name} ({entity}#{eid})"

    return {
        "ID": lid,
        "Type": call_type,
        "Outcome": outcome,
        "Sentiment": sentiment,
        "Phone": phone,
        "Start Time": start,
        "Duration": duration,
        "Related To": related_name,
    }


def _format_call_logs_table(logs: List[Dict[str, Any]]) -> str:
    """Format call logs as a table."""
    if not logs:
        return "No call logs found."

    # Extract data for all logs
    rows = [_extract_call_log_data(log) for log in logs]

    # Get column headers
    headers = ["ID", "Type", "Outcome", "Sentiment", "Phone", "Start Time", "Duration", "Related To"]

    # Calculate column widths
    col_widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            col_widths[h] = max(col_widths[h], len(str(row.get(h, "—"))))

    # Build table
    lines = []

    # Header
    header_line = " | ".join(f"{h:<{col_widths[h]}}" for h in headers)
    lines.append(header_line)
    lines.append("-" * len(header_line))

    # Rows
    for row in rows:
        row_line = " | ".join(f"{str(row.get(h, '—')):<{col_widths[h]}}" for h in headers)
        lines.append(row_line)

    return "\n".join(lines)


async def search_call_logs_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search call logs with jsonRule via POST /call-logs/search."""
    fields_list = await _fetch_call_log_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable call log fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_call_log_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {"jsonRule": json_rule}
    params = {"page": _call_logs_search_api_page(page), "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching call logs with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/call-logs/search", params=params, json=payload)
        data = await handle_api_response(response, "Search call logs")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No call logs found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} call log(s) (page {page + 1} of {total_pages}, total {total})", ""]
    for log in results:
        lines.append(_format_call_log_for_display(log))
        lines.append("")
    lines.append("💡 HINT: For call logs showing 'Related: contact#123' or 'lead#456', use:")
    lines.append("  • get_call_logs(entity_id=123, entity_type='contact') to see full contact details with their call logs")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# NOTES: Add notes to Lead, Contact, Deal, Company, Meeting, Call Log
# ---------------------------------------------------------------------------

@mcp.tool()
async def add_note(entity_type: str, entity_id: int, note_text: str) -> str:
    """
    Add a note to a Lead, Contact, Deal, Company, Meeting, or Call Log.

    entity_type: The entity type ("LEAD", "CONTACT", "DEAL", "COMPANY", "MEETING", or "CALL_LOG").
    entity_id: The ID of the entity (e.g. lead ID, contact ID, deal ID, company ID, meeting ID, call log ID).
    note_text: The note text to add (supports basic HTML formatting).

    Returns the note details if successful.
    """
    try:
        _reset_api_call_count()
        entity_type_upper = entity_type.upper().strip()
        if entity_type_upper not in ["LEAD", "CONTACT", "DEAL", "COMPANY", "MEETING", "CALL_LOG"]:
            return f"✗ Invalid entity type: '{entity_type}'. Must be one of: LEAD, CONTACT, DEAL, COMPANY, MEETING, CALL_LOG"

        entity_id = int(entity_id)
        if not note_text or not note_text.strip():
            return "✗ Note text cannot be empty."

        # Wrap note text in <div> tags for API
        description = f"<div>{note_text}</div>"

        payload = {
            "sourceEntity": {
                "description": description,
                "mentions": None,
            },
            "targetEntityId": str(entity_id),
            "targetEntityType": entity_type_upper,
        }

        logger.info(
            f"Adding note to {entity_type_upper} {entity_id}"
        )

        async with get_client() as client:
            response = await client.post("/notes/relation", json=payload)
            await handle_api_response(response, "Add note")

            logger.info(f"Note added to {entity_type_upper} {entity_id}")
            return (
                f"✓ Note added successfully to {entity_type_upper} {entity_id}.\n"
                f"  Note: {note_text[:100]}..."
            )
    except ValueError as e:
        return f"✗ Invalid entity ID: {str(e)}"
    except KylasAPIError as e:
        return f"✗ Failed to add note: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("add_note")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# By-Term Search Logic Functions
# ---------------------------------------------------------------------------

async def search_leads_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search leads by a single term across multiple fields via POST /search/lead with multi_field jsonRule."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = _multi_field_json_rule(term)
    payload = {
        "fields": ["id", "firstName", "lastName", "emails", "phoneNumbers", "ownerId", "companyName", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching leads by term: %r", term)
    async with get_client() as client:
        response = await client.post("/search/lead", params=params, json=payload)
        data = await handle_api_response(response, "Search leads by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No leads found matching '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} lead(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for lead in results:
        lid = lead.get("id", "?")
        fn = lead.get("firstName") or ""
        ln = lead.get("lastName") or ""
        name = f"{fn} {ln}".strip() or "—"
        email = _extract_primary_email(lead.get("emails"))
        phone = _extract_primary_phone(lead.get("phoneNumbers"))
        lines.append(f"• ID: {lid} | Name: {name} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_contacts_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search contacts by a single term across multiple fields via POST /search/contact with multi_field jsonRule."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = _multi_field_json_rule(term)
    payload = {
        "fields": ["id", "firstName", "lastName", "emails", "phoneNumbers", "ownerId", "department", "designation", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching contacts by term: %r", term)
    async with get_client() as client:
        response = await client.post("/search/contact", params=params, json=payload)
        data = await handle_api_response(response, "Search contacts by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No contacts found matching '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} contact(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for contact in results:
        cid = contact.get("id", "?")
        fn = contact.get("firstName") or ""
        ln = contact.get("lastName") or ""
        name = f"{fn} {ln}".strip() or "—"
        email = _extract_primary_email(contact.get("emails"))
        phone = _extract_primary_phone(contact.get("phoneNumbers"))
        lines.append(f"• ID: {cid} | Name: {name} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_tasks_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search tasks by a single term across multiple fields via POST /tasks/search with multi_field jsonRule."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = _multi_field_json_rule(term)
    payload = {
        "fields": ["id", "name", "status", "priority", "dueDate", "assignedTo", "relation", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching tasks by term: %r", term)
    async with get_client() as client:
        response = await client.post("/tasks/search", params=params, json=payload)
        data = await handle_api_response(response, "Search tasks by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No tasks found matching '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} task(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for task in results:
        tid = task.get("id", "?")
        name = task.get("name", "—")
        status = task.get("status", "—")
        priority = task.get("priority", "—")
        due_date = task.get("dueDate", "—")
        lines.append(f"• ID: {tid} | Name: {name} | Status: {status} | Priority: {priority} | Due: {due_date}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_companies_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search companies by a single term across multiple fields via POST /search/company with multi_field jsonRule."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = _multi_field_json_rule(term)
    payload = {
        "fields": ["id", "name", "website", "emails", "phoneNumbers", "ownerId", "createdAt"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching companies by term: %r", term)
    async with get_client() as client:
        response = await client.post("/search/company", params=params, json=payload)
        data = await handle_api_response(response, "Search companies by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No companies found matching '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} company/ies for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for company in results:
        cid = company.get("id", "?")
        name = company.get("name", "—")
        website = company.get("website", "—") or "—"
        email = _extract_primary_email(company.get("emails"))
        phone = _extract_primary_phone(company.get("phoneNumbers"))
        lines.append(f"• ID: {cid} | Name: {name} | Website: {website} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_meetings_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "from,desc",
) -> str:
    """Search meetings by title field only (meetings API does not support multi_field search)."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = {
        "rules": [
            {
                "id": "title",
                "field": "title",
                "type": "string",
                "input": "text",
                "operator": "contains",
                "value": term,
            }
        ],
        "condition": "AND",
        "valid": True,
    }
    payload = {"jsonRule": json_rule}
    sort = _normalize_meeting_sort(sort)
    params = {"page": _meetings_search_api_page(page), "size": min(size, 500), "sort": sort}
    logger.info("Searching meetings by term (title only): %r", term)
    async with get_client() as client:
        response = await client.post("/meetings/search", params=params, json=payload)
        data = await handle_api_response(response, "Search meetings by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No meetings found matching '{term}' in title. (Total in DB: {total})"
    lines = [f"Found {len(results)} meeting(s) with title matching '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for m in results:
        lines.append(_format_meeting_summary_line(m))
    lines.append("-" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Quotation field instructions / get / search logic
# ---------------------------------------------------------------------------

async def _fetch_quotation_fields() -> List[Dict[str, Any]]:
    """Fetch quotation field metadata (GET /quotations/fields). Returns list of active field dicts."""
    async with get_client() as client:
        response = await client.get("/quotations/fields", params={"page": 0, "size": 100})
        data = await handle_api_response(response, "Fetch quotation fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]



async def get_quotation_field_instructions_logic() -> str:
    fields = await _fetch_quotation_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    lines = [
        "=" * 60,
        "KYLAS CRM - QUOTATION FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True))
    lines.extend(["", "=" * 60, "END OF CHEAT SHEET", "=" * 60])
    return "\n".join(lines)


@mcp.tool()
async def get_quotation_field_instructions() -> str:
    """
    Get all quotation fields for the current tenant. CALL THIS FIRST before searching or filtering quotations.
    Returns a cheat sheet with API names (standard fields), Field IDs (custom fields), Picklist Option IDs,
    and which fields are [FILTERABLE] for search_entity("quotation", ...).
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching quotation field instructions")
        return await get_quotation_field_instructions_logic()
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_quotation_field_instructions")
        return f"Unexpected error: {str(e)}"


def _format_quotation_for_display(q: Dict[str, Any]) -> str:
    """Format a quotation object into a readable multi-line string."""
    def _name_of(obj: Any) -> str:
        if isinstance(obj, dict):
            return obj.get("name") or (str(obj.get("id")) if obj.get("id") is not None else "—")
        return str(obj) if obj is not None else "—"

    lines = ["=" * 60, "QUOTATION DETAILS", "=" * 60]
    lines.append(f"ID: {q.get('id', '—')}")
    lines.append(f"Quotation Number: {q.get('quotationNumber', '—')}")
    lines.append(f"Summary: {q.get('summary', '—')}")
    status = q.get("status")
    lines.append(f"Status: {_name_of(status)}")

    def _money(v: Any) -> Any:
        return v.get("value") if isinstance(v, dict) else (v if v is not None else "—")

    lines.append(f"Sub Total: {_money(q.get('subTotal'))}")
    lines.append(f"Grand Total: {_money(q.get('grandTotal'))}")
    lines.append(f"Valid Till: {q.get('validTill', '—')}")
    lines.append(f"Owner: {_name_of(q.get('owner'))}")
    lines.append(f"Associated Deal: {_name_of(q.get('associatedDeal'))}")
    lines.append(f"Associated Company: {_name_of(q.get('associatedCompany'))}")
    contacts = q.get("associatedContacts") or []
    if contacts:
        lines.append("Associated Contacts: " + ", ".join(_name_of(c) for c in contacts))
    lines.append(f"Created At: {q.get('createdAt', '—')}")
    lines.append(f"Updated At: {q.get('updatedAt', '—')}")
    # Products
    products = q.get("products") or []
    lines.append("")
    if products:
        lines.append(f"Products ({len(products)}):")
        for prod in products:
            prod_name = prod.get("name") or f"ID:{prod.get('id', '?')}"
            qty = prod.get("quantity", "?")
            price_obj = prod.get("price") or {}
            unit_price = price_obj.get("value") if isinstance(price_obj, dict) else price_obj
            total = prod.get("totalAmount")
            total_val = total.get("value") if isinstance(total, dict) else total
            lines.append(f"  • {prod_name:<24} qty: {qty}  unit: {unit_price}  total: {total_val}")
    else:
        lines.append("Products: —")
    # Custom fields
    custom = q.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


async def get_quotation_logic(quotation_id: int) -> Dict[str, Any]:
    """Fetch a single quotation by ID (GET /quotations/{id})."""
    quotation_id = int(quotation_id)
    async with get_client() as client:
        response = await client.get(f"/quotations/{quotation_id}")
        return await handle_api_response(response, "Get quotation")


@mcp.tool()
async def get_quotation(quotation_id: int) -> str:
    """
    Get full details of a quotation by ID (GET /quotations/{id}). Use when the user asks for complete
    quotation info (products, totals, associated deal/company/contacts).
    quotation_id: The quotation ID (e.g. from search_entity("quotation", ...) results).
    """
    try:
        _reset_api_call_count()
        q = await get_quotation_logic(quotation_id)
        return _format_quotation_for_display(q)
    except KylasAPIError as e:
        return f"✗ Failed to get quotation: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_quotation")
        return f"✗ Unexpected error: {str(e)}"


async def search_quotations_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search quotations with jsonRule (POST /quotations/search); only filterable fields allowed."""
    fields_list = await _fetch_quotation_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable quotation fields found for this tenant."
    sortable = {f.get("name") for f in fields_list if f.get("sortable") and f.get("name")}

    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break

    json_rule, err = _build_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {"jsonRule": json_rule}
    params: Dict[str, Any] = {"page": page, "size": min(size, 100)}
    # Only pass sort if the field is sortable; otherwise let the server apply its default.
    if sort:
        sort_by = sort.split(",")[0].strip()
        if sort_by in sortable:
            params["sort"] = sort
    logger.info("Searching quotations with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/quotations/search", params=params, json=payload)
        data = await handle_api_response(response, "Search quotations")
    results = data.get("content", data.get("data", [])) if isinstance(data, dict) else []
    total = data.get("totalElements", data.get("total", len(results))) if isinstance(data, dict) else len(results)
    total_pages = data.get("totalPages", 1) if isinstance(data, dict) else 1
    if not results:
        return f"No quotations found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} quotation(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for q in results:
        qid = q.get("id", "?")
        number = q.get("quotationNumber", "—")
        summary = q.get("summary", "—")
        status = q.get("status")
        status_str = status.get("name", "—") if isinstance(status, dict) else (status or "—")
        gt = q.get("grandTotal")
        grand_total = gt.get("value") if isinstance(gt, dict) else (gt if gt is not None else "—")
        deal = q.get("associatedDeal")
        deal_str = deal.get("name", deal.get("id", "—")) if isinstance(deal, dict) else "—"
        lines.append(
            f"• ID: {qid} | No: {number} | Summary: {summary} | Status: {status_str} | Grand Total: {grand_total} | Deal: {deal_str}"
        )
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_quotations_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search quotations by a free-text term across summary, quotation number, and the associated
    deal / company / contact / product names (POST /quotations/search with a multi_field jsonRule).
    """
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    # Determine sortable fields so we only pass a valid sort (server rejects non-sortable sorts).
    fields_list = await _fetch_quotation_fields()
    sortable = {f.get("name") for f in fields_list if f.get("sortable") and f.get("name")}
    payload = {"jsonRule": _multi_field_json_rule(term)}
    params: Dict[str, Any] = {"page": page, "size": min(size, 100)}
    if sort and sort.split(",")[0].strip() in sortable:
        params["sort"] = sort
    logger.info("Searching quotations by term: %r", term)
    async with get_client() as client:
        response = await client.post("/quotations/search", params=params, json=payload)
        data = await handle_api_response(response, "Search quotations by term")
    results = data.get("content", data.get("data", [])) if isinstance(data, dict) else []
    total = data.get("totalElements", data.get("total", len(results))) if isinstance(data, dict) else len(results)
    total_pages = data.get("totalPages", 1) if isinstance(data, dict) else 1
    if not results:
        return f"No quotations found for '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} quotation(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for q in results:
        qid = q.get("id", "?")
        number = q.get("quotationNumber", "—")
        summary = q.get("summary", "—")
        status = q.get("status")
        status_str = status.get("name", "—") if isinstance(status, dict) else (status or "—")
        gt = q.get("grandTotal")
        grand_total = gt.get("value") if isinstance(gt, dict) else (gt if gt is not None else "—")
        deal = q.get("associatedDeal")
        deal_str = deal.get("name", deal.get("id", "—")) if isinstance(deal, dict) else "—"
        lines.append(
            f"• ID: {qid} | No: {number} | Summary: {summary} | Status: {status_str} | Grand Total: {grand_total} | Deal: {deal_str}"
        )
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_idle_quotations_logic(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Find quotations with no update for at least `days` days (updatedAt on or before now - days).
    Quotations have no activity feed, so idleness is based on updatedAt only.
    If time_zone is not provided, uses the current user's timezone.
    """
    if time_zone:
        tz = time_zone
    else:
        try:
            user = await _fetch_current_user()
            tz = user.get("timezone") or DEFAULT_TIMEZONE
        except Exception:
            tz = DEFAULT_TIMEZONE
    threshold_iso = _threshold_iso_days_ago(days, tz)
    fields_list = await _fetch_quotation_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    base = {"operator": "less_or_equal", "value": threshold_iso, "timeZone": tz}
    filters = []
    for name in ("updatedAt", "latestActivityCreatedAt"):
        if name in filterable_map:
            filters.append({"field": name, **base})
    if not filters:
        return "Error: 'updatedAt' is not filterable for quotations in this tenant. Check get_quotation_field_instructions."
    return await search_quotations_logic(filters, page=page, size=size, sort=sort)


# ---------------------------------------------------------------------------
# Entity Configuration Dictionary for Generic Search Tool Dispatch
# ---------------------------------------------------------------------------

_ENTITY_CONFIG = {
    "lead": {
        "get_fn": get_lead_logic,
        "search_fn": search_leads_logic,
        "by_term_fn": search_leads_by_term_logic,
        "idle_fn": search_idle_leads_logic,
        "search_fields": ["id", "firstName", "lastName", "emails", "phoneNumbers", "ownerId", "companyName", "createdAt"],
        "search_endpoint": "/search/lead",
        "search_page_offset": 0,
        "search_rule_builder": _build_search_json_rule,
        "normalize": True,
        "field_fmt": "standard",
    },
    "contact": {
        "get_fn": get_contact_logic,
        "search_fn": search_contacts_logic,
        "by_term_fn": search_contacts_by_term_logic,
        "idle_fn": None,
        "search_fields": ["id", "firstName", "lastName", "emails", "phoneNumbers", "ownerId", "department", "designation", "createdAt"],
        "search_endpoint": "/search/contact",
        "search_page_offset": 0,
        "search_rule_builder": _build_search_json_rule,
        "normalize": True,
        "field_fmt": "standard",
    },
    "task": {
        "search_fn": search_tasks_logic,
        "by_term_fn": search_tasks_by_term_logic,
        "idle_fn": None,
        "search_fields": ["id", "name", "status", "priority", "dueDate", "assignedTo", "relation", "createdAt"],
        "search_endpoint": "/tasks/search",
        "search_page_offset": 0,
        "search_rule_builder": _build_search_json_rule,
        "normalize": True,
        "field_fmt": "standard",
    },
    "deal": {
        "search_fn": search_deals_logic,
        "by_term_fn": search_deals_by_term_logic,
        "idle_fn": search_idle_deals_logic,
        "search_fields": ["id", "name", "value", "currency", "closingDate", "ownerId", "createdAt", "actualValue", "estimatedValue"],
        "search_endpoint": "/search/deal",
        "search_page_offset": 0,
        "search_rule_builder": _build_deal_search_json_rule,
        "normalize": True,
        "field_fmt": "standard",
    },
    "company": {
        "search_fn": search_companies_logic,
        "by_term_fn": search_companies_by_term_logic,
        "idle_fn": search_idle_companies_logic,
        "search_fields": ["id", "name", "website", "emails", "phoneNumbers", "ownerId", "createdAt"],
        "search_endpoint": "/search/company",
        "search_page_offset": 0,
        "search_rule_builder": _build_company_search_json_rule,
        "normalize": True,
        "field_fmt": "standard",
    },
    "meeting": {
        "search_fn": search_meetings_logic,
        "by_term_fn": search_meetings_by_term_logic,
        "idle_fn": None,
        "search_fields": None,
        "search_endpoint": "/meetings/search",
        "search_page_offset": 0,
        "search_rule_builder": _build_meeting_search_json_rule,
        "normalize": False,
        "field_fmt": "meeting",
    },
    "call_log": {
        "search_fn": search_call_logs_logic,
        "by_term_fn": None,
        "idle_fn": None,
        "search_fields": None,
        "search_endpoint": "/call-logs/search",
        "search_page_offset": 0,
        "search_rule_builder": _build_call_log_search_json_rule,
        "normalize": False,
        "field_fmt": "meeting",
    },
    "quotation": {
        "search_fn": search_quotations_logic,
        "by_term_fn": search_quotations_by_term_logic,
        "idle_fn": search_idle_quotations_logic,
        "search_fields": ["id", "quotationNumber", "summary", "status", "grandTotal", "associatedDeal", "updatedAt"],
        "search_endpoint": "/quotations/search",
        "search_page_offset": 0,
        "search_rule_builder": _build_search_json_rule,
        "normalize": True,
        "field_fmt": "standard",
    },
}


# ---------------------------------------------------------------------------
# CRUD Config for generic create/update/get tools
# ---------------------------------------------------------------------------

_ENTITY_CRUD_CONFIG: Dict[str, Dict[str, Any]] = {
    "lead": {
        "create_fn": create_lead_logic,
        "update_fn": update_lead_logic,
        "name_fn": lambda r: (f"{r.get('firstName', '')} {r.get('lastName', '')}".strip() or "Lead"),
    },
    "contact": {
        "create_fn": create_contact_logic,
        "update_fn": update_contact_logic,
        "name_fn": lambda r: (f"{r.get('firstName', '')} {r.get('lastName', '')}".strip() or "Contact"),
    },
    "deal": {
        "create_fn": create_deal_logic,
        "update_fn": update_deal_logic,
        "name_fn": lambda r: r.get("name", "Deal"),
    },
    "task": {
        "create_fn": create_task_logic,
        "update_fn": update_task_logic,
        "name_fn": lambda r: r.get("name", "Task"),
    },
    "company": {
        "create_fn": create_company_logic,
        "update_fn": update_company_logic,
        "name_fn": lambda r: r.get("name", "Company"),
    },
    "meeting": {
        "create_fn": create_meeting_logic,
        "update_fn": update_meeting_logic,
        "name_fn": lambda r: r.get("title", "Meeting"),
    },
    "call_log": {
        "create_fn": create_call_log_logic,
        "update_fn": update_call_log_logic,
        "name_fn": lambda r: f"{r.get('callType', '')} / {r.get('outcome', '')}",
    },
}


# ---------------------------------------------------------------------------
# Generic Get Router
#
# Mirrors search_entity_logic's shape (entity_type -> _ENTITY_CONFIG lookup ->
# real per-entity function), added so execute_request has a single generic
# router to call for "get" intents too, the same pattern as search/create/
# update. Scoped to whichever buckets have a "get_fn" wired in _ENTITY_CONFIG
# (currently lead, contact) — add one line there to extend to more buckets.
#
# Deliberately does NOT swallow exceptions into a formatted string the way
# search_entity_logic/search_entity_by_term_logic/search_idle_entities_logic
# do — get_lead_logic/get_contact_logic themselves already raise on failure
# rather than returning an error string, so this stays a thin,
# exception-propagating pass-through to match them, and to give
# execute_request a real signal to build its ok/error envelope from.
# ---------------------------------------------------------------------------

async def get_entity_logic(entity_type: str, entity_id: int) -> Any:
    cfg = _ENTITY_CONFIG.get(entity_type)
    if not cfg:
        raise ValueError(f"Unknown entity_type '{entity_type}'. Valid: {', '.join(_ENTITY_CONFIG.keys())}")
    get_fn = cfg.get("get_fn")
    if not get_fn:
        raise ValueError(f"Entity type '{entity_type}' does not support get.")
    return await get_fn(entity_id)


# ---------------------------------------------------------------------------
# Generic Search Tool
# ---------------------------------------------------------------------------

async def search_entity_logic(
    entity_type: str,
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Logic for generic entity search."""
    # Validate entity_type
    cfg = _ENTITY_CONFIG.get(entity_type)
    if not cfg:
        valid_types = ", ".join(_ENTITY_CONFIG.keys())
        return f"Unknown entity_type '{entity_type}'. Valid: {valid_types}"

    # Check if entity has a search function
    search_fn = cfg.get("search_fn")
    if not search_fn:
        return f"Entity type '{entity_type}' does not support search."

    # Validate filters for entities that require them
    if entity_type not in ("meeting", "call_log"):
        if not filters:
            return "Error: filters cannot be empty for this entity type. Provide at least one filter."

    # Handle pagination offset (meeting/call_log are 1-based)
    page_offset = cfg.get("search_page_offset", 0)
    api_page = page + page_offset

    # Call the entity-specific search logic
    try:
        result = await search_fn(filters, page=api_page, size=size, sort=sort)
        return result
    except Exception as e:
        return f"Error searching {entity_type}: {str(e)}"


@mcp.tool()
async def search_entity(
    entity_type: str,
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter any entity type (lead, contact, task, deal, company, meeting, call_log).
    Use this tool instead of entity-specific search tools.

    Valid entity_type values: lead, contact, task, deal, company, meeting, call_log, quotation

    filters: List of filter objects. Each must have:
      - field (str): Field internal/API name (e.g. firstName, country, source, createdAt).
      - operator (str): One of the allowed operators for that field type.
      - value: Value to compare (type depends on field type).
      - timeZone (str, optional): For date/datetime filters.
      - type (str, optional): Field type. If omitted, inferred from schema.

    For lead/contact/task/deal/company: Filters are REQUIRED (non-empty).
    For meeting/call_log: Filters are OPTIONAL (empty = all records).

    page: 0-based page for lead/contact/task/deal/company; 1-based for meeting/call_log (default 0).
    size: Page size, max 100 (max 500 for meeting/call_log) (default 20).
    sort: Sort e.g. "createdAt,desc" (default).

    Task association examples (replaces search_tasks_for_* tools):
      - search_entity("task", [{"field": "associatedLeads", "operator": "equal", "value": lead_id}])
      - search_entity("task", [{"field": "associatedContacts", "operator": "equal", "value": contact_id}])
      - search_entity("task", [{"field": "associatedDeals", "operator": "equal", "value": deal_id}])
      - search_entity("task", [{"field": "associatedCompanies", "operator": "equal", "value": company_id}])

    Meeting owner/organizer examples (DO NOT use search_entity_by_term):
      - search_entity("meeting", [{"field": "owner", "operator": "equal", "value": user_id}])
      - search_entity("meeting", [{"field": "organizer", "operator": "equal", "value": user_id}])
    """
    return await search_entity_logic(entity_type, filters, page, size, sort)


async def search_entity_by_term_logic(
    entity_type: str,
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Logic for generic entity search by term."""
    # Validate entity_type
    cfg = _ENTITY_CONFIG.get(entity_type)
    if not cfg:
        valid_types = ", ".join([k for k, v in _ENTITY_CONFIG.items() if v.get("by_term_fn")])
        return f"Unknown entity_type '{entity_type}'. Valid: {valid_types}"

    # Check if entity supports by_term search
    by_term_fn = cfg.get("by_term_fn")
    if not by_term_fn:
        return f"Entity type '{entity_type}' does not support search by term."

    # Handle pagination offset (meeting is 1-based)
    page_offset = cfg.get("search_page_offset", 0)
    api_page = page + page_offset

    # Call the entity-specific by_term logic
    try:
        _reset_api_call_count()
        result = await by_term_fn(search_term, page=api_page, size=size, sort=sort)
        return result
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_entity_by_term_logic")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_entity_by_term(
    entity_type: str,
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search/filter any entity type by a free-text search term (lead, contact, task, deal, company, meeting).
    Use this tool ONLY when the user provides a specific search term (e.g. a name, email, or keyword).

    ⚠️ NEVER use this tool with wildcard characters ("*"), empty strings, or blank terms.
    ⚠️ NEVER use this tool when the user asks for "all" records (e.g. "show all leads", "list all contacts").
       → For "all" / "list" queries, use `search_entity` with a date filter (e.g. updatedAt >= last 90 days).

    Valid entity_type values: lead, contact, task, deal, company, meeting, quotation

    search_term: A specific term to search across fields (e.g. "John", "acme@corp.com", "Acme Inc").
      - For meeting: searches 'title' field only. ⚠️ DO NOT use this tool to search/filter meetings by owner or organizer name; instead, look up their user ID and call `search_entity` with an `owner` or `organizer` filter.
      - For quotation: searches summary, quotation number, and the associated deal / company / contact / product names.
      - For others: multi-field search (first name, last name, email, phone, etc.).

    page: 0-based page for lead/contact/task/deal/company; 1-based for meeting (default 0).
    size: Page size, max 100 (max 500 for meeting) (default 20).
    sort: Sort e.g. "updatedAt,desc" (default).
    """
    return await search_entity_by_term_logic(entity_type, search_term, page, size, sort)


async def search_idle_entities_logic(
    entity_type: str,
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Logic for searching idle entities."""
    # Validate entity_type
    cfg = _ENTITY_CONFIG.get(entity_type)
    if not cfg:
        valid_types = "lead, deal, company"
        return f"Unknown entity_type '{entity_type}'. Valid: {valid_types}"

    # Check if entity supports idle search
    idle_fn = cfg.get("idle_fn")
    if not idle_fn:
        return f"Entity type '{entity_type}' does not support idle entity search. Valid types: lead, deal, company"

    # Handle pagination offset
    page_offset = cfg.get("search_page_offset", 0)
    api_page = page + page_offset

    # Call the entity-specific idle logic
    try:
        _reset_api_call_count()
        result = await idle_fn(days, time_zone=time_zone, page=api_page, size=size, sort=sort)
        return result
    except KylasAPIError as e:
        return f"✗ Search idle entities failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_idle_entities_logic")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_idle_entities(
    entity_type: str,
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search entities that have been idle (no recent activity) for N days.
    Use this tool to find stale or neglected records.

    Valid entity_type values: lead, deal, company, quotation
    (quotation has no activity feed, so idleness is based on updatedAt only.)

    days: Number of days of inactivity (e.g., 30 = last updated >30 days ago).
    time_zone: Timezone for date calculation (optional; defaults to current user's timezone or server default).
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).
    """
    return await search_idle_entities_logic(entity_type, days, time_zone, page, size, sort)


# ---------------------------------------------------------------------------
# Generic CRUD Tools
# ---------------------------------------------------------------------------
#
# create_entity_logic/update_entity_logic are the actual dispatch logic that
# used to live only inline inside the create_entity/update_entity tool
# bodies below. Extracted so execute_request can call the SAME dispatch
# (bucket -> _ENTITY_CRUD_CONFIG -> real create_fn/update_fn) directly and
# get the raw created/updated record back, instead of going through the
# tool's own "✓ ... created successfully" string formatting. The tools below
# now call these too, so there is exactly one real implementation of this
# dispatch, not two. Raises ValueError on an unknown entity_type (the tools'
# own "✗ Unknown entity_type" message, as an exception instead of a string)
# so a real caller needing an ok/fail signal — execute_request — gets one,
# without the tools' user-facing string format changing at all.
# ---------------------------------------------------------------------------

async def create_entity_logic(entity_type: str, field_values: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _ENTITY_CRUD_CONFIG.get(entity_type)
    if not cfg:
        valid = ", ".join(_ENTITY_CRUD_CONFIG.keys())
        raise ValueError(f"Unknown entity_type '{entity_type}'. Valid: {valid}")
    _reset_api_call_count()
    return await cfg["create_fn"](field_values)


async def update_entity_logic(entity_type: str, entity_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _ENTITY_CRUD_CONFIG.get(entity_type)
    if not cfg:
        valid = ", ".join(_ENTITY_CRUD_CONFIG.keys())
        raise ValueError(f"Unknown entity_type '{entity_type}'. Valid: {valid}")
    _reset_api_call_count()
    return await cfg["update_fn"](entity_id, field_values)


@mcp.tool()
async def create_entity(entity_type: str, field_values: Dict[str, Any]) -> str:
    """
    Create any CRM entity. Use instead of entity-specific create tools.
    Call get_entity_field_instructions(entity_type) FIRST to get field names and IDs.

    Valid entity_type values: lead, contact, deal, task, company, meeting, call_log

    === PHONE & EMAIL RULES (apply to lead, contact, deal, company) ===
    Phone:
      - Shorthand: "phone": "9876543210" + "phone_country_code": "IN"  (2-letter code or dial prefix e.g. "+91")
      - Full array: "phoneNumbers": [{"value": "9876543210", "code": "IN", "type": "MOBILE", "primary": true}]
        Still include "phone_country_code" at top level even with full array.
      - REQUIRED: If the user gave a phone number but NO country/dial code, DO NOT call this tool — ask them first.
      - REQUIRED: If the user gave a phone number but NO phone type, DO NOT call this tool — ask them first: "Is this number MOBILE, WORK, HOME, or PERSONAL?"
      - Include "phone_type": "<TYPE>" at top level alongside "phone_country_code".
      - Phone types: MOBILE, WORK, HOME, PERSONAL. First entry is primary by default.
    Email:
      - Shorthand: "email": "user@example.com"  (becomes OFFICE, primary)
      - Full array: "emails": [{"value": "user@example.com", "type": "OFFICE", "primary": true}]
      - Email types: OFFICE, PERSONAL. Exactly one must be primary.

    === FIELD_VALUES FORMAT PER ENTITY TYPE ===

    lead / contact:
      {"firstName": "Jane", "lastName": "Doe",
       "email": "jane@example.com",
       "phone": "9876543210", "phone_country_code": "IN",
       "customFieldValues": {"cfLeadSource": "Web"},   ← internal names only
       "leadSource": <picklist_option_id>}              ← picklist = Option ID number

    deal:
      {"name": "Big Deal",
       "closingDate": "2026-06-30T00:00:00.000Z",       ← UTC ISO
       "estimatedValue": {"currencyId": 431, "value": 50000},  ← NEVER a plain number
       "ownedBy": {"id": <user_id>},
       "customFieldValues": {"cfDealStatus": "Active"}}

    task:
      {"name": "Follow up",
       "dueDate": "2026-05-01T18:29:59.999Z",           ← UTC ISO
       "assignedTo": <user_id>,
       "relation": [{"targetEntityId": <id>, "targetEntityType": "LEAD", "targetEntityName": "<name>"}],
       "customFieldValues": {"cfPriority": "High"}}

    company:
      {"name": "Acme Corp", "website": "https://acme.com",
       "email": "contact@acme.com",
       "phone": "9876543210", "phone_country_code": "IN",
       "customFieldValues": {"cfIndustry": "SaaS"}}

    meeting:
      {"title": "Demo Call",                             ← REQUIRED
       "from": "2026-05-10T08:00:00.000Z",              ← REQUIRED, UTC ISO
       "to":   "2026-05-10T08:30:00.000Z",              ← REQUIRED, UTC ISO
       "participants": [{"id": <user_id>, "entity": "user"}],  ← REQUIRED; NO deals
       "relatedTo": [{"id": <lead_id>, "entity": "lead"}],
       "timezone": {"id": 372, "name": "Asia/Calcutta"}}
      Use get_current_user + parse_datetime_to_utc_iso_tool to convert local time to UTC.

    call_log:
      {"outcome": "connected",                           ← REQUIRED: connected/rejected/busy/no_answer/missed_call/in_progress
       "callType": "outgoing",                           ← REQUIRED: incoming/outgoing
       "startTime": "2026-05-10T08:00:00.000Z",         ← REQUIRED, UTC ISO
       "phoneNumber": "9876543210",                      ← REQUIRED
       "relatedTo": {"id": <entity_id>, "entity": "lead", "phoneNumber": "9876543210"},  ← REQUIRED
       "duration": 120,                                  ← seconds, optional
       "notes": [{"description": "Discussed pricing"}]} ← optional
    """
    cfg = _ENTITY_CRUD_CONFIG.get(entity_type)
    try:
        result = await create_entity_logic(entity_type, field_values)
        entity_id = result.get("id", "?")
        name = cfg["name_fn"](result)
        label = entity_type.replace("_", " ").title()
        return f"✓ {label} created successfully.\n  ID: {entity_id}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create {entity_type}: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_entity")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def update_entity(entity_type: str, entity_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update any CRM entity by type and ID. Use instead of entity-specific update tools.

    Valid entity_type values: lead, contact, deal, task, company, meeting, call_log

    entity_id: The ID of the entity to update (from search results or get_entity).
    field_values: Fields to update (same format as create_entity for that type).
      For most entities: fields are merged over the existing record (other fields unchanged).
      For call_log: full replace — include all required fields (outcome, startTime, phoneNumber, callType, relatedTo).

    Examples:
    lead:
      entity_type="lead", entity_id=123, field_values={"firstName": "Jane", "lastName": "Doe"}

    deal (change pipeline stage):
      entity_type="deal", entity_id=456, field_values={"pipelineStage": <stage_id>}

    deal (add contacts/products):
      entity_type="deal", entity_id=456,
      field_values={"associatedContacts": [{"id": 4942095, "name": "Alice"}],
                    "products": [{"id": 245208, "quantity": 2}]}

    meeting (merge participants):
      entity_type="meeting", entity_id=789,
      field_values={"participants": [{"id": <user_id>, "entity": "user"}]}

    call_log (full replace):
      entity_type="call_log", entity_id=321,
      field_values={"outcome": "connected", "callType": "outgoing",
                    "startTime": "2026-05-10T08:00:00.000Z",
                    "phoneNumber": "9876543210",
                    "relatedTo": {"id": <lead_id>, "entity": "lead", "phoneNumber": "9876543210"}}

    === PHONE & EMAIL RULES ===
    Phone updates: always include phone_country_code (e.g. "IN", "+91", "US") whenever
    phone or phoneNumbers is in field_values. Never assume a default country.
    """
    cfg = _ENTITY_CRUD_CONFIG.get(entity_type)
    try:
        result = await update_entity_logic(entity_type, entity_id, field_values)
        eid = result.get("id", entity_id)
        name = cfg["name_fn"](result)
        label = entity_type.replace("_", " ").title()
        return f"✓ {label} updated successfully.\n  ID: {eid}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update {entity_type}: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_entity")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Generic Registry (list_tool / build_payload) — 3-tool architecture POC
#
# Step 1 of 3: registry + discovery + schema-detail only. No execute_request
# yet, no _meta bucket yet. Additive — does not touch or replace any of the
# 36 tools above. See TOOL_CONSOLIDATION_DESIGN.md and
# YAML_REGISTRY_SCHEMA_PLAN.md for the full design this validates.
# ---------------------------------------------------------------------------

_REGISTRY_DIR = Path(__file__).parent / "registry"
_REGISTRY_REQUIRED_FIELDS = ["id", "bucket", "intent", "method", "path", "description"]


class RegistryError(Exception):
    """Raised when the YAML registry fails validation. Must prevent server startup."""


def _load_registry(registry_dir: Path = _REGISTRY_DIR) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """
    Load and validate registry_dir/index.yaml + registry_dir/*.yaml (all files
    except index.yaml). Returns (index, entries) where entries is {id: entry_dict}.
    Raises RegistryError, naming the offending file/id, on any problem. Called at
    module import time (not lazily) — a broken registry must stop the server from
    starting at all, not fail on the first tool call.
    """
    index_path = registry_dir / "index.yaml"
    if not index_path.exists():
        raise RegistryError(f"Registry index not found: {index_path}")
    with open(index_path, "r", encoding="utf-8") as f:
        index = yaml.safe_load(f) or {}
    valid_buckets = set(index.get("buckets") or [])
    valid_intents = set(index.get("intents") or [])
    if not valid_buckets:
        raise RegistryError(f"{index_path}: 'buckets' must be a non-empty list.")
    if not valid_intents:
        raise RegistryError(f"{index_path}: 'intents' must be a non-empty list.")

    entries: Dict[str, Dict[str, Any]] = {}

    for yml_path in sorted(registry_dir.glob("*.yaml")):
        if yml_path.name == "index.yaml":
            continue
        with open(yml_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
        if not isinstance(content, list):
            raise RegistryError(
                f"{yml_path}: expected a YAML list of entries, got {type(content).__name__}."
            )
        for i, entry in enumerate(content):
            if not isinstance(entry, dict):
                raise RegistryError(f"{yml_path}: entry #{i + 1} is not a mapping.")
            entry_id = entry.get("id")
            label = entry_id or f"entry #{i + 1} (no id)"
            missing = [f for f in _REGISTRY_REQUIRED_FIELDS if not entry.get(f)]
            if missing:
                raise RegistryError(
                    f"{yml_path}: '{label}' is missing required field(s): {', '.join(missing)}"
                )
            if entry_id in entries:
                raise RegistryError(
                    f"{yml_path}: duplicate id '{entry_id}' "
                    f"(already defined in {entries[entry_id]['_source_file']})"
                )
            if entry["bucket"] not in valid_buckets:
                raise RegistryError(
                    f"{yml_path}: '{entry_id}' has bucket '{entry['bucket']}' which is not in "
                    f"index.yaml's buckets list ({sorted(valid_buckets)})"
                )
            if entry["intent"] not in valid_intents:
                raise RegistryError(
                    f"{yml_path}: '{entry_id}' has intent '{entry['intent']}' which is not in "
                    f"index.yaml's intents list ({sorted(valid_intents)})"
                )
            entry = dict(entry)
            entry["_source_file"] = yml_path.name
            entries[entry_id] = entry

    if not entries:
        raise RegistryError(
            f"No registry entries found under {registry_dir} — refusing to start with an empty registry."
        )

    logger.info(
        "📋 Loaded %d registry entries from %s: %s",
        len(entries), registry_dir, sorted(entries.keys()),
    )
    return index, entries


# Loaded once at import time — see docstring above for why this isn't lazy.
_REGISTRY_INDEX, _REGISTRY = _load_registry()


@mcp.tool()
def list_tool(
    bucket: Optional[str] = None,
    intent: Optional[str] = None,
) -> str:
    """
    Step 1 of 3 in the generic CRM tool flow (list_tool -> build_payload ->
    execute_request): find which endpoint id you need, before knowing anything
    about its shape.

    Returns ONLY a short summary row per match:
      {"results": [{"id": "...", "bucket": "...", "intent": "...", "description": "..."}]}
    Never method, path, a parameter schema, or an example — no matter how specific
    bucket/intent are. That level of detail is build_payload(id)'s job alone.
    This tool exists purely to narrow the full registry down to the one id you
    actually need, without paying the cost of every endpoint's full schema.

    It is always safe and cheap to call this with NO arguments to see everything
    currently registered — do that first if you're not sure what's available, then
    narrow with bucket/intent once you have a sense of what you're looking for.

    Registry contents right now (this will grow — always confirm here rather than
    assuming an id exists):
      buckets: lead, contact
      intents: get, search, create
      (each bucket currently has exactly one endpoint per intent, e.g. "lead.get")

    bucket: restrict to one bucket (e.g. "lead", "contact"). Omit to search every bucket.
    intent: restrict to one intent — "get", "search", or "create". Omit to match any.

    Example: list_tool(bucket="lead") returns the 3 lead.* rows (get/search/create),
    each with its own id and one-line description — nothing more.
    """
    results = []
    for entry in _REGISTRY.values():
        if bucket and entry["bucket"] != bucket:
            continue
        if intent and entry["intent"] != intent:
            continue
        results.append({
            "id": entry["id"],
            "bucket": entry["bucket"],
            "intent": entry["intent"],
            "description": entry.get("description", ""),
        })
    return json.dumps({"results": results}, indent=2)


# Maps a bucket name to the existing per-entity field-metadata fetcher it
# should use — same functions the original get_*_field_instructions tools
# already call (_fetch_lead_fields, _fetch_contact_fields, ...). Add one line
# per new bucket; nothing else about the fold/build_payload logic changes.
_BUCKET_FIELD_FETCHERS: Dict[str, Any] = {
    "lead": _fetch_lead_fields,
    "contact": _fetch_contact_fields,
}

# Maps a bucket to the real, original get_* tool whose inputSchema
# build_payload copies verbatim for that bucket's ".get" entry.
_REGISTRY_BUCKET_TO_GET_TOOL: Dict[str, str] = {
    "lead": "get_lead",
    "contact": "get_contact",
}


async def _fold_field_metadata_into_schema(
    base_schema: Dict[str, Any], fetch_fields_fn: Any, for_search: bool
) -> Dict[str, Any]:
    """
    Fetch this tenant's real field metadata for one bucket (via whichever
    existing _fetch_*_fields() function fetch_fields_fn is) and fold it into a
    COPY of base_schema — never mutates the registry's own dict. Bucket-
    agnostic on purpose: was hardcoded to lead only until "contact" was added;
    kept generic from here so a 3rd/4th bucket is just one more
    _BUCKET_FIELD_FETCHERS entry, not a new copy of this function.
    for_search=True: adds tenant_filterable_fields (name/type/standard) for building
    filters. for_search=False (create/update): adds tenant_fields.standard/custom,
    each field including its live picklist option id/label pairs where applicable.
    """
    schema = json.loads(json.dumps(base_schema))  # cheap deep copy, no extra dependency
    fields = await fetch_fields_fn()

    if for_search:
        schema["tenant_filterable_fields"] = [
            {
                "name": f.get("name"),
                "displayName": f.get("displayName"),
                "type": f.get("type"),
                "standard": f.get("standard", False),
            }
            for f in fields
            if f.get("filterable", False)
        ]
        return schema

    def _field_summary(f: Dict[str, Any]) -> Dict[str, Any]:
        summary = {
            "name": f.get("name"),
            "displayName": f.get("displayName"),
            "type": f.get("type"),
            "required": f.get("required", False),
            "standard": f.get("standard", False),
        }
        if f.get("type") in ("PICK_LIST", "MULTI_PICKLIST"):
            picklist = f.get("picklist") or {}
            values = picklist.get("values") or picklist.get("picklistValues") or []
            summary["options"] = [
                {"id": v.get("id"), "label": v.get("displayName") or v.get("label") or v.get("name")}
                for v in values if isinstance(v, dict)
            ]
        return summary

    schema["tenant_fields"] = {
        "standard": [_field_summary(f) for f in fields if f.get("standard", False)],
        "custom": [_field_summary(f) for f in fields if not f.get("standard", False)],
    }

    # Override the static YAML's "required" guess with the REAL required-field
    # list from this tenant's live metadata (same `required` flag the original
    # get_*_field_instructions tools already read via _format_field) — a static
    # required list authored by hand can be, and was, simply wrong: e.g. this
    # registry originally hardcoded lead.create's required as
    # [firstName, lastName, email], but Kylas actually accepted a real create
    # with no email at all. Live data is the only real source of truth here.
    live_required = [f.get("name") for f in fields if f.get("standard", False) and f.get("required", False)]
    if live_required:
        schema["required"] = live_required

    return schema


@mcp.tool()
async def build_payload(id: str) -> str:
    """
    Step 2 of 3 in the generic CRM tool flow: get everything about the ONE
    endpoint you already picked via list_tool. Takes ONLY that id — nothing
    else — and never sees or checks the values you're about to send; that only
    happens (if at all) in execute_request, described there.

    Returns a JSON object with these fields:
      id             - the id you asked for, echoed back.
      method, path   - the real Kylas HTTP method and path for this endpoint,
                       path params written as "{name}" (e.g. "/leads/{lead_id}").
      usage_notes    - the ACTUAL rules for this one endpoint only: shorthand
                       field formats, which picklist fields use a name instead
                       of an Option ID, which fields must be resolved through
                       another endpoint first, and any other constraint specific
                       to it. Read this before building anything — it is not
                       decorative, it is the real, load-bearing documentation
                       for this endpoint (the same rules that used to live in
                       this server's old global instructions, now scoped to
                       exactly the one endpoint they apply to).
      schema         - required/properties for the payload you need to build.
                       If a field's own entry contains "resolve_via": "<other
                       id>", do NOT invent a value for it — go run the full
                       list_tool/build_payload/execute_request cycle on
                       <other id> first, then use the real value it returns.
      example        - one concrete, valid example payload for this endpoint.
      dynamic_fields - true if this endpoint's real shape depends on THIS
                       tenant's own custom fields/picklist options, which can
                       only be known by asking Kylas directly (never static).
      fetched_live   - true only if that tenant-specific data was actually
                       fetched successfully on this call.
      tenant_fields / tenant_filterable_fields - present only when
                       dynamic_fields and fetched_live are both true: this
                       tenant's REAL custom fields (with real picklist option
                       id/label pairs) for create/update, or the real
                       filterable-field list for search. Never placeholder data.
      live_fetch_error - present only when dynamic_fields is true and the live
                       fetch failed (e.g. no credentials configured yet). When
                       this is present, everything else above — method, path,
                       usage_notes, example — is still the real, correct static
                       shape; only the tenant-specific enrichment is missing.
                       Don't treat this field's presence as the whole call
                       having failed.

    This tool never validates the payload you build from this. Call
    execute_request(id, payload) next to actually send it — a malformed payload
    is caught there (or by Kylas itself), never here.

    id: an endpoint id from list_tool (e.g. "lead.create", "contact.search").
    """
    entry = _REGISTRY.get(id)
    if not entry:
        return json.dumps({
            "ok": False,
            "error": f"Unknown endpoint id '{id}'. Call list_tool first to find a valid id.",
        })

    # Schema comes from the REAL original tool's own inputSchema
    # (get_lead/get_contact/search_entity/create_entity), never a
    # hand-authored one — see _real_get_schema/_real_search_schema/
    # _real_create_schema and the snapshot mechanism above.
    if entry["intent"] == "get":
        original_get_tool = _REGISTRY_BUCKET_TO_GET_TOOL.get(entry["bucket"])
        schema = _real_get_schema(original_get_tool) if original_get_tool else (entry.get("schema") or {})
    elif entry["intent"] == "search":
        schema = _real_search_schema()
    elif entry["intent"] == "search_by_term":
        schema = _real_search_by_term_schema()
    elif entry["intent"] == "search_idle":
        schema = _real_idle_schema()
    elif entry["intent"] == "create":
        schema = _real_create_schema()
    elif entry["intent"] == "update":
        schema = _real_update_schema()
    elif entry["intent"] == "lookup":
        schema = _real_get_schema(_REGISTRY_ID_TO_META_TOOL.get(id, ""))
    else:
        schema = entry.get("schema") or {}

    fetched_live = False
    live_fetch_error = None
    if entry.get("dynamic_fields"):
        fetch_fields_fn = _BUCKET_FIELD_FETCHERS.get(entry["bucket"])
        if fetch_fields_fn is None:
            live_fetch_error = f"No field-metadata fetcher wired for bucket '{entry['bucket']}' yet."
        else:
            try:
                schema = await _fold_field_metadata_into_schema(
                    schema, fetch_fields_fn, for_search=(entry["intent"] == "search")
                )
                fetched_live = True
            except KylasAPIError as e:
                # Don't fail the whole call for this — method/path/usage_notes/example
                # need no network access at all and are still genuinely useful on
                # their own; only the live tenant field/picklist enrichment is missing.
                live_fetch_error = e.message

    result = {
        "id": entry["id"],
        "method": entry["method"],
        "path": entry["path"],
        "usage_notes": entry.get("usage_notes", ""),
        "schema": schema,
        "example": entry.get("example"),
        "dynamic_fields": bool(entry.get("dynamic_fields")),
        "fetched_live": fetched_live,
    }
    if live_fetch_error:
        result["live_fetch_error"] = (
            f"Could not fetch this tenant's live custom fields/picklist options: "
            f"{live_fetch_error}. The schema/usage_notes/example above are still the "
            f"real static shape — only the live tenant-specific enrichment is missing."
        )
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# execute_request — step 3. Routes purely by (bucket, intent) read straight
# off the registry entry into the SAME generic Python routers every other
# entity-agnostic tool already uses — get_entity_logic / search_entity_logic /
# search_entity_by_term_logic / search_idle_entities_logic / create_entity_logic
# / update_entity_logic — never a hardcoded per-id mapping, and never a
# reimplemented Kylas call. Adding a new bucket to the registry (e.g. "deal")
# needs a "get_fn"/"search_fn"/etc. entry in _ENTITY_CONFIG/_ENTITY_CRUD_CONFIG
# (if not already there) and nothing else here — no new Python per id.
#
# Known, deliberate asymmetry, not a bug: get_entity_logic/create_entity_logic/
# update_entity_logic return a raw dict (clean, structured "data"); the three
# search_*_logic routers return a pre-FORMATTED STRING (they already build
# "Found N lead(s)/contact(s)..." text themselves, same as the old
# search_leads/search_contacts tools did) — reused exactly as-is rather than
# changed, so every search/search_by_term/search_idle id's "data" is a
# display string, not a dict, unlike *.get/*.create/*.update. A related,
# still-open point: those same three search_*_logic routers catch their own
# KylasAPIError internally and return it as a "✗ ..." string rather than
# raising — execute_request's try/except below can't see that failure, so
# "ok" currently reads true even when the underlying search actually failed.
# Not fixed here; flagging it exactly where it now lives.
# ---------------------------------------------------------------------------

@mcp.tool()
async def execute_request(id: str, payload: Dict[str, Any]) -> str:
    """
    Step 3 of 3 in the generic CRM tool flow: actually perform the call for the
    payload you built via build_payload, using the SAME id you called it with.
    Runs this repo's existing, already-tested implementation for that specific
    endpoint — e.g. "lead.create" runs the exact same create_lead_logic() the
    original create_lead tool already used — never a newly written or
    reimplemented HTTP call.

    build_payload does not validate anything about your payload, so this is
    where a malformed one is caught first (or, if it's shaped correctly but
    semantically wrong, by Kylas's own API). Always returns a normalized JSON
    envelope, never a raw exception and never an unhandled crash:
      success: {"ok": true, "status": 200, "data": <result>}
      failure: {"ok": false, "error": {"code": "<CODE>", "message": "<why>"}}

    "data" on success is whatever that endpoint's real underlying logic already
    returns — do not assume it is always the same shape across ids. For *.get
    and *.create ids today, that's a structured dict (the real record). For
    *.search ids today, that's a pre-formatted, human-readable string (e.g.
    "Found 3 lead(s)..."), because that is genuinely what the reused search
    logic already returns — reused exactly as-is, not restructured.

    error.code values you may see:
      "UNKNOWN_ID"  - this id isn't in the registry at all — call
                      list_tool to find a real one.
      "NOT_WIRED"   - the id exists but has no execute_request handler yet.
      "BAD_PAYLOAD" - the payload was missing a required field or had the
                      wrong type for it — since build_payload never validates,
                      this is often the FIRST place that surfaces.
      anything else - Kylas's own HTTP status code, or "KYLAS_ERROR" for a
                      connection/auth failure reaching Kylas at all.

    id: an endpoint id from list_tool/build_payload (e.g. "lead.create").
    payload: the request body/args you built by hand from that SAME id's
        build_payload schema. Never send a payload built for one id against
        a different id — the two are not interchangeable even if they look
        similar (e.g. lead.create vs contact.create).
    """
    entry = _REGISTRY.get(id)
    if not entry:
        return json.dumps({
            "ok": False,
            "error": {"code": "UNKNOWN_ID", "message": f"Unknown endpoint id '{id}'. Call list_tool first."},
        }, indent=2)

    bucket = entry["bucket"]
    intent = entry["intent"]
    payload = payload or {}

    try:
        if intent == "get":
            # The id-field name genuinely differs per bucket (lead_id vs
            # contact_id) — derived from the same real snapshot build_payload
            # already uses for this bucket's schema, not a second hardcoded map.
            original_get_tool = _REGISTRY_BUCKET_TO_GET_TOOL.get(bucket)
            id_field = (_ORIGINAL_TOOL_PARAMETERS.get(original_get_tool, {}).get("required") or ["entity_id"])[0]
            result = await get_entity_logic(bucket, payload[id_field])
        elif intent == "search":
            result = await search_entity_logic(
                entity_type=bucket,
                filters=payload.get("filters", []),
                page=payload.get("page", 0),
                size=payload.get("size", 20),
                sort=payload.get("sort", "createdAt,desc"),
            )
        elif intent == "search_by_term":
            result = await search_entity_by_term_logic(
                entity_type=bucket,
                search_term=payload["search_term"],
                page=payload.get("page", 0),
                size=payload.get("size", 20),
                sort=payload.get("sort", "updatedAt,desc"),
            )
        elif intent == "search_idle":
            result = await search_idle_entities_logic(
                entity_type=bucket,
                days=payload["days"],
                time_zone=payload.get("time_zone"),
                page=payload.get("page", 0),
                size=payload.get("size", 20),
                sort=payload.get("sort", "createdAt,desc"),
            )
        elif intent == "create":
            result = await create_entity_logic(bucket, payload)
        elif intent == "update":
            update_payload = dict(payload)
            entity_id = update_payload.pop("entity_id")
            result = await update_entity_logic(bucket, entity_id, update_payload)
        elif intent == "lookup":
            router = _META_LOOKUP_ROUTERS.get(id)
            if not router:
                return json.dumps({
                    "ok": False,
                    "error": {"code": "NOT_WIRED", "message": f"'{id}' has no execute_request router wired yet."},
                }, indent=2)
            result = await router(payload)
        else:
            return json.dumps({
                "ok": False,
                "error": {"code": "NOT_WIRED", "message": f"intent '{intent}' has no execute_request router yet."},
            }, indent=2)
        return json.dumps({"ok": True, "status": 200, "data": result}, default=str, indent=2)
    except KylasAPIError as e:
        return json.dumps({
            "ok": False,
            "error": {"code": str(e.status_code or "KYLAS_ERROR"), "message": e.message},
        }, indent=2)
    except (KeyError, ValueError, TypeError) as e:
        # Missing/malformed payload field (e.g. lead.get called without "lead_id") —
        # never let this surface as a raw traceback to the client.
        return json.dumps({
            "ok": False,
            "error": {"code": "BAD_PAYLOAD", "message": f"{type(e).__name__}: {e}"},
        }, indent=2)
    except Exception as e:
        logger.exception("execute_request(%s)", id)
        return json.dumps({
            "ok": False,
            "error": {"code": "INTERNAL_ERROR", "message": str(e)},
        }, indent=2)


# ---------------------------------------------------------------------------
# Registry-only mode toggle
#
# MCP_TOOL_MODE=registry advertises ONLY the generic registry tools (and a
# short, matching set of instructions) to a connecting client instead of all
# 38 tools + the full SYSTEM_INSTRUCTIONS block. This is additive and fully
# reversible: no function above is deleted or edited by this, only what gets
# ADVERTISED at connection time changes. Default ("all", or the var unset)
# keeps today's exact behavior — every existing user is unaffected unless
# this is explicitly set.
# ---------------------------------------------------------------------------

_MCP_TOOL_MODE = os.getenv("MCP_TOOL_MODE", "all").strip().lower()

# The only tools kept when MCP_TOOL_MODE=registry.
_REGISTRY_ONLY_TOOL_NAMES = {"list_tool", "build_payload", "execute_request"}

_REGISTRY_MODE_INSTRUCTIONS = """
# Kylas CRM MCP Server — Generic Registry Mode

This server used to expose one dedicated tool per CRM operation, each with its
own always-visible instructions. It does not do that anymore. Those old
instructions do not apply here and must not be assumed or recalled — every
CRM operation is now reached through exactly 3 generic tools, and everything
you need to use any of them correctly is either below or returned by the
tools themselves, on demand.

## The 3-step flow

1. list_tool(bucket?, intent?)
   Find the id of the endpoint you need. Returns only a short
   id/bucket/intent/description row per match — never a schema. Call it with
   no arguments first if you don't know what's available yet; narrow with
   bucket/intent once you have a sense of what you're looking for.

2. build_payload(id)
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

3. execute_request(id, payload)
   Send the exact payload you built, using the SAME id you called
   build_payload with. Always returns {"ok": true, "status", "data"} on
   success or {"ok": false, "error": {"code", "message"}} on failure — never
   a raw exception. A malformed payload is caught HERE, not earlier —
   build_payload does no validation of its own, on purpose.

## What's registered right now (call list_tool to confirm, don't assume)
Buckets: lead, contact. Intents: get, search, create. Each bucket currently
has exactly one endpoint per intent (e.g. "lead.get", "contact.search").
This registry is expected to grow over time.

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
  everything automatically.
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
"""


def _plain_component_name(key: str) -> str:
    """
    FastMCP's internal `_components` dict keys aren't always plain function
    names — different versions have stored them as e.g. "build_payload" or
    "tool:build_payload@..." / "resource:<uri>@...". Strip any "<kind>:" prefix
    and any "@..." suffix so this keeps working across that difference (verified
    against fastmcp 3.4.7 here; requirements.txt pins 3.2.4 for the real deploy —
    don't assume they key it the same way without checking again if either changes).
    """
    name = key.split(":", 1)[1] if ":" in key else key
    return name.split("@", 1)[0] if "@" in name else name


# ---------------------------------------------------------------------------
# Snapshot the REAL, original tools' own inputSchema — captured HERE, before
# _apply_tool_mode() below potentially removes them from what's advertised —
# so build_payload can literally reuse them instead of a hand-authored
# schema. get_lead/get_contact's real schemas are entity-specific and copied
# as-is. search_entity/create_entity's real schemas are what ACTUALLY backed
# every lead/contact search & create in the old system (standalone
# search_leads/create_lead never existed as registered tools) — their
# "filters"/"field_values" properties are copied as-is too; only their
# "entity_type" property is dropped per bucket entry, since a registry id
# like "lead.create" already fixes the entity type that "entity_type" would
# otherwise redundantly ask for.
# ---------------------------------------------------------------------------

_ORIGINAL_TOOL_PARAMETERS: Dict[str, Dict[str, Any]] = {}


def _snapshot_original_tool_parameters(tool_name: str) -> None:
    for key, component in mcp.local_provider._components.items():
        if _plain_component_name(key) == tool_name:
            _ORIGINAL_TOOL_PARAMETERS[tool_name] = component.parameters
            return


for _name in (
    "get_lead", "get_contact",
    "search_entity", "search_entity_by_term", "search_idle_entities",
    "create_entity", "update_entity",
    "lookup_users", "lookup_products", "lookup_pipelines",
):
    _snapshot_original_tool_parameters(_name)

# Maps a registry id in the bucket-less "_meta" group to the real old tool
# whose inputSchema build_payload copies verbatim for it (reuses
# _real_get_schema — that function just returns a snapshot verbatim, nothing
# lead/contact-specific about it despite the name).
_REGISTRY_ID_TO_META_TOOL: Dict[str, str] = {
    "user.lookup": "lookup_users",
    "product.lookup": "lookup_products",
    "pipeline.lookup": "lookup_pipelines",
}


async def _lookup_users_router(payload: Dict[str, Any]) -> str:
    # Real schema's public param is "return_all" (lookup_users tool's own
    # name for it) — translated here to "fetch_all_pages", the name
    # lookup_users_logic itself actually takes. Not a new rule, just naming
    # the same real thing the tool wrapper already renames internally.
    return await lookup_users_logic(
        query=payload.get("query", "name:"),
        page=payload.get("page", 0),
        size=payload.get("size", 50),
        fetch_all_pages=payload.get("return_all", False),
    )


async def _lookup_products_router(payload: Dict[str, Any]) -> str:
    # lookup_products_logic's own param names match the real lookup_products
    # tool's schema exactly — no renaming needed here, unlike users above.
    return await lookup_products_logic(
        query=payload.get("query", ""),
        page=payload.get("page", 0),
        size=payload.get("size", 50),
    )


async def _lookup_pipelines_router(payload: Dict[str, Any]) -> str:
    # Same — lookup_pipelines_logic's params (query/entity_type/page/size)
    # match the real lookup_pipelines tool's schema as-is.
    return await lookup_pipelines_logic(
        query=payload.get("query", ""),
        entity_type=payload.get("entity_type", "LEAD"),
        page=payload.get("page", 0),
        size=payload.get("size", 50),
    )


# "_meta" entries are NOT entity-CRUD-shaped like lead/contact — each one is
# its own standalone old tool with its own real signature, so (unlike
# get/search/create/update) there's no single generic _ENTITY_CONFIG-style
# router to reuse for "lookup" intents; the old system never had one either
# (lookup_users/lookup_products/lookup_pipelines were always separate,
# unrelated tools). One small explicit map, one line per id, is the honest
# shape here — add a line per new "_meta" id, same as any other dict-based
# dispatch already in this file.
_META_LOOKUP_ROUTERS: Dict[str, Any] = {
    "user.lookup": _lookup_users_router,
    "product.lookup": _lookup_products_router,
    "pipeline.lookup": _lookup_pipelines_router,
}


def _without_entity_type(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Drop the "entity_type" property/requirement — redundant once a registry id fixes it."""
    props = schema.get("properties", {})
    props.pop("entity_type", None)
    schema["required"] = [f for f in schema.get("required", []) if f != "entity_type"]
    return schema


def _real_get_schema(tool_name: str) -> Dict[str, Any]:
    """get_lead/get_contact's real inputSchema, copied verbatim."""
    return json.loads(json.dumps(_ORIGINAL_TOOL_PARAMETERS.get(tool_name, {})))


def _real_search_schema() -> Dict[str, Any]:
    """
    search_entity's real inputSchema, with "entity_type" removed (redundant —
    the registry id already fixes it) and "page"/"size"/"sort" kept exactly
    as search_entity actually declares them, defaults included.
    """
    schema = json.loads(json.dumps(_ORIGINAL_TOOL_PARAMETERS.get("search_entity", {})))
    return _without_entity_type(schema)


def _real_search_by_term_schema() -> Dict[str, Any]:
    """search_entity_by_term's real inputSchema, "entity_type" removed."""
    schema = json.loads(json.dumps(_ORIGINAL_TOOL_PARAMETERS.get("search_entity_by_term", {})))
    return _without_entity_type(schema)


def _real_idle_schema() -> Dict[str, Any]:
    """search_idle_entities's real inputSchema, "entity_type" removed."""
    schema = json.loads(json.dumps(_ORIGINAL_TOOL_PARAMETERS.get("search_idle_entities", {})))
    return _without_entity_type(schema)


def _real_create_schema() -> Dict[str, Any]:
    """
    create_entity's real "field_values" property, unwrapped to the top level.
    The real create_entity schema is {entity_type, field_values: {type:
    object, additionalProperties: true}} — no field names anywhere; that
    open-object shape (never firstName/lastName/email) IS the real old
    system's schema, copied as-is. Unwrapped (not nested under
    "field_values") because that's how execute_request's payload for these
    ids actually is structured — a flat object, matching what
    create_lead_logic/create_contact_logic themselves take.
    """
    schema = json.loads(json.dumps(_ORIGINAL_TOOL_PARAMETERS.get("create_entity", {})))
    field_values_schema = schema.get("properties", {}).get("field_values", {"type": "object", "additionalProperties": True})
    return field_values_schema


def _real_update_schema() -> Dict[str, Any]:
    """
    update_entity's real inputSchema is {entity_type, entity_id, field_values:
    {type: object, additionalProperties: true}}. "entity_type" is dropped
    (redundant, same as elsewhere); "entity_id" is KEPT as its real name
    (not renamed to "lead_id") — a real naming inconsistency inherited
    as-is from the old system (get_lead's own real schema calls the same
    concept "lead_id"), not smoothed over here. "field_values" is unwrapped
    to the top level via additionalProperties, same reasoning as
    _real_create_schema — the payload here is flat, matching what
    update_lead_logic/update_contact_logic actually take (id + field dict).
    """
    schema = json.loads(json.dumps(_ORIGINAL_TOOL_PARAMETERS.get("update_entity", {})))
    schema = _without_entity_type(schema)
    props = schema.get("properties", {})
    props.pop("field_values", None)
    return {
        "type": "object",
        "properties": {"entity_id": props.get("entity_id", {"type": "integer"})},
        "required": ["entity_id"],
        "additionalProperties": True,
    }


def _apply_tool_mode() -> None:
    """
    If MCP_TOOL_MODE=registry: remove every registered tool/resource except
    _REGISTRY_ONLY_TOOL_NAMES from what gets advertised, and swap in the lean
    registry-mode instructions in place of the full SYSTEM_INSTRUCTIONS block.
    No-op (today's exact behavior) for any other value, including unset.
    """
    if _MCP_TOOL_MODE != "registry":
        return
    components = mcp.local_provider._components
    removed = [k for k in components if _plain_component_name(k) not in _REGISTRY_ONLY_TOOL_NAMES]
    for k in removed:
        del components[k]
    mcp._mcp_server.instructions = _REGISTRY_MODE_INSTRUCTIONS
    logger.info(
        "MCP_TOOL_MODE=registry — advertising only %s (%d component(s) hidden, %d remain)",
        sorted(_REGISTRY_ONLY_TOOL_NAMES), len(removed), len(components),
    )


_apply_tool_mode()


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
