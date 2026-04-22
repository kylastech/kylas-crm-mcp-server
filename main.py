"""
Kylas CRM MCP Server - Lead, Contact & Task Support

Model Context Protocol server for Kylas CRM operations:

LEAD OPERATIONS:
- get_lead_field_instructions (call FIRST to get schema)
- create_lead, update_lead, get_lead
- search_leads (filter by criteria)
- search_idle_leads (no activity for N days)
- lookup_pipelines, get_pipeline_stages, get_pipeline_details

CONTACT OPERATIONS:
- get_contact_field_instructions (call FIRST to get schema)
- create_contact, update_contact, get_contact
- search_contacts (filter by criteria - NO PIPELINE)

TASK OPERATIONS:
- get_task_field_instructions (call FIRST to get schema)
- create_task, update_task, get_task
- search_tasks (filter by criteria - NO PIPELINE)
- search_tasks_for_lead, search_tasks_for_contact, search_tasks_for_deal, search_tasks_for_company (find tasks associated with entity)

SHARED TOOLS (for all Lead, Contact, Task):
- get_current_user (timezone, ID; use for date/datetime handling)
- lookup_users (resolve user names to IDs for ownerId, createdBy, updatedBy)
- lookup_products (find products for field_values)
- parse_datetime_to_utc_iso_tool (convert user timezone to UTC ISO)
"""

import asyncio
import os
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo

import httpx
from dateutil import parser as dateutil_parser
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_context, get_http_request
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

BASE_URL = os.getenv("KYLAS_BASE_URL", "https://api.kylas.io/v1")
API_KEY = os.getenv("KYLAS_API_KEY")


def _get_default_timezone() -> str:
    """
    Default timezone for date/datetime filters. Used when the user doesn't pass timeZone in a filter.
    Fixed to Asia/Calcutta for now.
    """
    return "Asia/Calcutta"


DEFAULT_TIMEZONE = _get_default_timezone()


def _threshold_iso_days_ago(days: int, time_zone: str) -> str:
    """Return (now - days) in the given timezone as ISO string (UTC with Z)."""
    try:
        tz = ZoneInfo(time_zone)
    except Exception:
        tz = ZoneInfo("UTC")
    now = datetime.now(tz)
    threshold = now - timedelta(days=days)
    return threshold.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.000Z")


if not API_KEY:
    logger.info("KYLAS_API_KEY not set. Server will rely on per-request 'x-api-key' header.")

# ---------------------------------------------------------------------------
# System Instructions: ALWAYS get fields first, then create from user context
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Lead, Contact & Task Support

## ⚠️ MANDATORY SESSION RULE — READ THIS FIRST

**For ANY entity, call its field instructions tool ONCE per session — the very first time you interact with that entity. After that, reuse the field schema already in context; do NOT call it again for subsequent queries on the same entity.**

| Entity    | Call ONCE per session (first interaction only) |
|-----------|------------------------------------------------|
| Lead      | `get_lead_field_instructions`                  |
| Contact   | `get_contact_field_instructions`               |
| Task      | `get_task_field_instructions`                  |
| Deal      | `get_deal_field_instructions`                  |
| Company   | `get_company_field_instructions`               |
| Meeting   | `get_meeting_field_instructions`               |
| Call Log  | `get_call_log_field_instructions`              |

**Why:** All field API names, custom field IDs, and picklist option IDs are tenant-specific. You need this schema once to correctly build field_values. Once fetched, the schema is in your context — do not fetch it again for the same entity in the same session.

**Example:** User asks "show leads" → call `get_lead_field_instructions` then `search_leads`. User then asks "show leads again" → skip `get_lead_field_instructions` (already fetched), call `search_leads` directly.

---

## ⚠️ DEFAULT DATE RANGE RULE — "SHOW ALL" / "GIVE ALL" QUERIES

**When the user asks for "all" records of any entity (e.g. "show all leads", "give all deals", "list all contacts") WITHOUT specifying a date range, ALWAYS apply a default filter: `createdAt` in the last 3 months.**

- Do NOT fetch all records without a date filter — this could return thousands of records.
- Compute the 3-month threshold as: today minus 90 days, in the user's timezone (call `get_current_user` first if timezone is not yet known).
- Use `updatedAt` with operator `greater_or_equal` and value = (today − 90 days) ISO string.
- If the user explicitly provides a date range (e.g. "show leads from Jan to March"), use that instead — do not override it with the 3-month default.
- Inform the user that results are filtered to the last 3 months, e.g.: *"Showing leads updated in the last 3 months. Specify a date range if you need older records."*

**Example:**
- User: "show all leads" → apply `updatedAt >= (today - 90 days)` filter automatically.
- User: "show all leads from last year" → use the user-specified range, not the 3-month default.

## CRITICAL: Workflow (applies to Lead, Contact, and Task)

### Step 1: ALWAYS call `get_<entity>_field_instructions` FIRST
Before creating a lead, contact, or task, you MUST call the appropriate field instructions tool:
- **For Leads:** `get_lead_field_instructions`
- **For Contacts:** `get_contact_field_instructions`
- **For Tasks:** `get_task_field_instructions`

This provides:
- All available lead fields (standard and custom)
- API names for standard fields (e.g. firstName, lastName, emails, companyName)
- Field IDs for custom fields (e.g. "57256")
- Picklist option IDs for dropdowns (e.g. leadSource: 12345)

### Step 2: Create/Update Lead, Contact, or Task from user context only
- Do NOT use a fixed list of fields. Infer from the user's message what they want to create or update.
- Build `field_values` with ONLY the fields the user provided or implied.
- For **update_lead**, **update_contact**, or **update_task**: pass the entity ID (e.g. from search results) and the fields to update; same field_values format as create. For owner/ownerId use user ID from lookup_users.
- **Important differences:**
  - **Contacts do NOT have:** pipeline, pipelineStage fields (Lead-specific)
  - **Tasks do NOT have:** pipeline, pipelineStage fields (Lead-specific)
  - Use only entity-specific fields from field instructions
- Keys: use API Name for standard fields (from cheat sheet), or Field ID string for custom fields.
- Values: use the exact format expected by Kylas (see below).

### Field value formats (from Kylas API)
- **Standard fields** (firstName, lastName, companyName, isNew, etc.): use API name as key at top level.
- **emails**: array of objects (types OFFICE, PERSONAL only; exactly one must be primary). Or pass "email": "user@example.com" to normalize (OFFICE, primary). First entry is primary by default.
- **phoneNumbers**: array of objects (types MOBILE, WORK, HOME, PERSONAL only; exactly one must be primary; "code" = 2-letter country e.g. IN, US). Or pass "phone": "5551234567". You MUST also pass "phone_country_code": "IN" or "+91" at the top level whenever any phone is included. **If the user gave phone number(s) but did NOT specify country or dial code: do NOT call create_lead.** Reply asking for country/dial code (e.g. "Which country or dial code should I use for these phone numbers? (e.g. India: IN or +91, US: +1)"). Only after the user provides it, call create_lead with that phone_country_code. Do not infer country from currency (e.g. INR), locale, or number format—always ask. First entry is primary by default.
- **Picklist fields** (e.g. leadSource, salutation): use the **Option ID** (number) from the cheat sheet.
- **Custom fields**: MUST go in "customFieldValues" with **internal name** as key (e.g. "customFieldValues": {"cfLeadCheck": "Checked"}). Never use field ID as the key in the request—the API expects internal names (e.g. cfLeadCheck). If you pass a field ID by mistake, the server will resolve it to the internal name automatically. From the cheat sheet, use the "Field ID" only to identify the field; for the payload use the field's **name** (internal/API name) in customFieldValues.

### NEVER guess IDs
- Always use the cheat sheet from `get_lead_field_instructions` for API names and IDs.
- Omit any field the user did not mention; do not add static/default fields.

### Search/Filter leads
- **By specific field:** When the user specifies a field (e.g. "leads where phone number is X", "leads where first name is John"), use **search_leads** with the appropriate filter(s).
- Call `get_lead_field_instructions` first to see which fields are **filterable** (marked in cheat sheet) when using search_leads.
- Only fields with filterable=true can be used in search_leads filters.
- For PICK_LIST/MULTI_PICKLIST: use **Option ID** (number) in filter value, except for: requirementCurrency, companyBusinessType, country, timezone, companyIndustry — for these use **internal name** (string).
- Use the correct operator for the field type (see operator list in search_leads docstring).

### User look-up fields (createdBy, updatedBy, convertedBy, ownerId, importedBy)
- These fields reference **users**; filter value must be the **user ID** (number), not the name.
- When the user asks e.g. "leads where created by is Last": (1) Call **lookup_users** with query in field:value form (e.g. "firstName:Last" or "name:Last"). (2) If **more than one** user is returned, ask the user explicitly which person they mean and list the matches (id and name). (3) Once exactly one user is identified, call **search_leads** with filter e.g. {"field": "createdBy", "operator": "equal", "value": <user_id>}.
- Do not guess user IDs; always use lookup_users first when filtering by created by / updated by / owner / imported by / converted by.

### Product filter (products field)
- The **products** field on leads references products; filter value must be the **product ID** (number), not the name.
- When the user asks e.g. "leads with product X" or "leads that have product Y": (1) Call **lookup_products** with query e.g. "name:X" or "name:Y". (2) If **more than one** product is returned, ask the user which product they mean and list the matches (id and name). (3) Once exactly one product is identified, call **search_leads** with filter {"field": "products", "operator": "equal", "value": <product_id>}.
- Do not guess product IDs; always use lookup_products first when filtering by product name.

### Pipeline and pipeline stage (create, update, search)

**Always resolve pipeline first when stage is involved.** Call **lookup_pipelines** (entityType=LEAD); do **not** call get_pipeline_stages until the user has confirmed which pipeline.

- **If there are multiple pipelines:** Always list them (id and name) and ask which pipeline the user means. Even if the user says "default lead pipeline", there may be more than one—ask explicitly (e.g. "Do you mean 'Default Lead Pipeline' (id 996) or 'new PIpeline' (id 1232)?").
- **If there is only one pipeline:** Still ask for confirmation (e.g. "I found one pipeline: [name]. Should I use this one?") before calling get_pipeline_stages or updating the lead.
- **After confirmation only:** Call **get_pipeline_stages** with that pipeline ID, then map stage intent (e.g. "open" → OPEN, "won" → CLOSED_WON) and use that pipeline + stage in create_lead, update_lead, or search_leads as below.

**Create lead with stage** (e.g. "create lead with open stage"):
- User must confirm which pipeline before creating. Follow the rules above (list pipelines, get confirmation; if only one pipeline, still confirm). Then get_pipeline_stages for that pipeline, pick the matching stage (e.g. Open), and include pipeline + pipelineStage (or pipeline object with stage) in create_lead field_values.

**Move lead to a pipeline stage** (e.g. "move this lead to Open", "set stage to Won"):
- **Use `update_lead(lead_id, {"pipelineStage": stage_id})` to move a lead to a specific pipeline stage.**
- If the user did **not** specify which pipeline: call lookup_pipelines, list pipelines, and ask which pipeline to use. Then call get_pipeline_stages for that pipeline.
- Once the user confirms the pipeline and stage, call **update_lead** with the lead_id and field_values `{"pipelineStage": stage_id}` from get_pipeline_stages.
- The tool automatically fetches the correct `forecastingType` for the stage and sends the full lead object.
- If the lead **already has a pipeline** and the user is moving to a **different pipeline**: ask for confirmation first (e.g. "This lead is in [current pipeline]. Move it to [new pipeline]?").
- If only one pipeline exists and user didn’t name it: still ask for confirmation.
- **When moving to Closed Lost or Closed Unqualified:** A closing reason may be required (check with get_pipeline_details). If required, ask the user to pick a reason from lostReasons or unqualifiedReasons, then call update_lead with both `{"pipelineStage": stage_id, "pipelineStageReason": reason_string}`.

**Search/filter by stage** (e.g. "open leads", "closed leads", "leads in Won"):
- Same as above: ask for pipeline first (list and get confirmation), then get_pipeline_stages, then search_leads with pipeline + pipelineStage filters.
- Do not guess pipeline or pipeline stage IDs.

### Idle / Stagnant leads (no activity for N days)
- "Idle" or "stagnant" means no activity on the lead for at least N days. Use **last activity** = the **later** of `updatedAt` and `latestActivityCreatedAt`; the lead is idle if that date is before (today − N days).
- Since the API cannot filter on "max of two fields", use **both** conditions: `updatedAt` ≤ threshold **and** `latestActivityCreatedAt` ≤ threshold (threshold = now − N days in ISO). That way the lead is returned only when both dates are old, i.e. the effective last activity is before the threshold.
- Prefer the **search_idle_leads** tool when the user asks for idle/stagnant/inactive leads (e.g. "no activity since 10 days"). Otherwise build search_leads filters as above with operator "less_or_equal" and value = ISO date string for (now − N days).

### Contact-Specific Operations (NO PIPELINE/STAGE)
- **Search Contacts:** Use `search_contacts` with filters. **IMPORTANT:** Contacts do NOT support pipeline or pipelineStage filters.
- **Update Contact:** Use `update_contact` with contact ID and field_values. Same field format as create_contact.
- **Fields available:** firstName, lastName, emails, phoneNumbers, department, designation, company, ownerId, custom fields, etc.
- **Fields NOT available:** pipeline, pipelineStage (these are Lead-specific).
- All other instructions (email/phone normalization, timezone handling, lookup_users, lookup_products, custom fields) apply the same to contacts as leads.

### Task-Specific Operations (NO PIPELINE/STAGE)
- **Create Task with Entity Association:** When creating a task on a lead, contact, deal, or company, include the **"relation"** field:
  ```json
  "relation": [
    {"targetEntityId": <id>, "targetEntityType": "LEAD|CONTACT|DEAL|COMPANY", "targetEntityName": "<entity_name>"}
  ]
  ```
  Example: `create_task({"name": "Follow up", "relation": [{"targetEntityId": 45089710, "targetEntityType": "LEAD", "targetEntityName": "John Doe"}]})`
- **Search Tasks:** Use `search_tasks` with filters. **IMPORTANT:** Tasks do NOT support pipeline or pipelineStage filters.
- **Update Task:** Use `update_task` with task ID and field_values. Same field format as create_task (including relation if updating associations).
- **Fields available:** name, description, status, priority, dueDate, assignedTo, reminder, relation, custom fields, etc.
- **Fields NOT available:** pipeline, pipelineStage (these are Lead-specific), emails, phoneNumbers (Contact-specific).
- All other instructions (timezone handling, lookup_users, lookup_products, custom fields) apply the same to tasks.

### Task Association Filters (finding tasks for a specific entity)
- **Find tasks for a Lead:** Use `search_tasks_for_lead` with the lead ID. Internally filters by `associatedLeads` field.
- **Find tasks for a Contact:** Use `search_tasks_for_contact` with the contact ID. Internally filters by `associatedContacts` field.
- **Find tasks for a Deal:** Use `search_tasks_for_deal` with the deal ID. Internally filters by `associatedDeals` field.
- **Find tasks for a Company:** Use `search_tasks_for_company` with the company ID. Internally filters by `associatedCompanies` field.
- These are convenience tools; alternatively, use `search_tasks` with filters like `{"field": "associatedLeads", "operator": "equal", "value": <lead_id>}`.

### Date and datetime fields — timezone from current user (GET /users/me)
- Whenever a **date or datetime** is involved (create lead/contact with a date/datetime field, or filter by date/datetime), call **get_current_user** first to get the user's **timezone** (e.g. Asia/Calcutta).
- **Creating a lead/contact with a datetime field:** The user gives the datetime in their own timezone (e.g. "11th Feb 2026 at 7:30 AM"). You MUST convert it to UTC before sending: call **get_current_user** → get timezone → call **parse_datetime_to_utc_iso_tool**(user's datetime string, user's timezone) → put the returned UTC ISO string in field_values for that date/datetime field. Do not send the user's local time as-is.
- **Filtering by date/datetime (search_leads, search_contacts, search_idle_leads):** Use the user's timezone from get_current_user as the **timeZone** in the filter (or rely on the server using it when timeZone is omitted). Keep the date/datetime value as the user said it (in their timezone); do **not** convert filter values to UTC — the API interprets them using the timeZone field.
"""

# ---------------------------------------------------------------------------
# Search: Operator mapping by field type & picklists that use internal name
# ---------------------------------------------------------------------------

OPERATOR_MAPPING = {
    "TEXT_FIELD": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "PARAGRAPH_TEXT": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"],
    "NUMBER": ["equal", "not_equal", "greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "in", "not_in", "is_null", "is_not_null"],
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
    Resolve the MCP client name (e.g. cursor, claude) from request context or HTTP User-Agent.
    Used for the outbound User-Agent to Kylas: kylas_mcp_server/{clientName}.
    """
    try:
        ctx = get_context()
        if ctx and getattr(ctx, "client_id", None):
            cid = (ctx.client_id or "").lower()
            if "cursor" in cid:
                return "cursor"
            if "claude" in cid:
                return "claude"
    except Exception:
        pass
    try:
        req = get_http_request()
        if req and getattr(req, "headers", None):
            ua = (req.headers.get("user-agent") or req.headers.get("User-Agent") or "").lower()
            if "cursor" in ua:
                return "cursor"
            if "claude" in ua:
                return "claude"
    except Exception:
        pass
    return "unknown"


def _resolve_api_key() -> str:
    """Resolve API key: try per-request HTTP header first, then fall back to env var."""
    # 1. Try per-request header (multi-user HTTP mode)
    try:
        req = get_http_request()
        if req and getattr(req, "headers", None):
            header_key = req.headers.get("x-api-key")
            if header_key:
                return header_key
    except Exception:
        pass
    # 2. Fall back to env var (single-user / stdio mode)
    if API_KEY:
        return API_KEY
    raise KylasAPIError(
        "API key not provided. Pass 'x-api-key' header in your MCP client config "
        "or set KYLAS_API_KEY environment variable."
    )


class _ThrottledClientContext:
    """Async context manager: wraps httpx.AsyncClient and yields _ThrottledClient for 100–500 ms delay between calls."""

    def __init__(self) -> None:
        api_key = _resolve_api_key()
        client_name = _get_mcp_client_name()
        user_agent = f"kylas_mcp_server/{client_name}"
        self._raw = httpx.AsyncClient(
            base_url=BASE_URL,
            headers={
                "api-key": api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": user_agent,
            },
            timeout=30.0,
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
        logger.error(f"{operation} failed: {e.response.status_code} - {error_body}")
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

## CRITICAL: Workflow for Deals

### Step 1: ALWAYS call `get_deal_field_instructions` FIRST
Before creating or updating a deal, you MUST call `get_deal_field_instructions` to get:
- All available deal fields (standard and custom)
- API names for standard fields (e.g. name, value, closingDate, dealSource)
- Field IDs for custom fields (e.g. "57256")
- Picklist option IDs for dropdowns (e.g. dealSource: 12345)

### Step 2: Create/Update deal from user context only
- Do NOT use a fixed list of fields. Infer from the user's message what they want to create or update.
- Build `field_values` with ONLY the fields the user provided or implied.
- For **update_deal**: pass the deal ID (e.g. from search results) and the fields to update; same field_values format as create_deal. For owner/ownerId use user ID from lookup_users.
- Keys: use API Name for standard fields (from cheat sheet), or Field ID string for custom fields.
- Values: use the exact format expected by Kylas (see below).

### Field value formats
- **Standard fields** (name, value, closingDate, dealSource, etc.): use API name as key at top level.
- **emails**: array of objects (types OFFICE, PERSONAL only; exactly one must be primary). Or pass "email": "user@example.com" to normalize (OFFICE, primary). First entry is primary by default.
- **phoneNumbers**: array of objects (types MOBILE, WORK, HOME, PERSONAL only; exactly one must be primary; "code" = 2-letter country e.g. IN, US). Or pass "phone": "5551234567". You MUST also pass "phone_country_code": "IN" or "+91" at the top level whenever any phone is included. **If the user gave phone number(s) but did NOT specify country or dial code: do NOT call create_deal.** Reply asking for country/dial code. Only after the user provides it, call create_deal with that phone_country_code.
- **Picklist fields** (e.g. dealSource, dealStatus): use the **Option ID** (number) from the cheat sheet.
- **Custom fields**: MUST go in "customFieldValues" with **internal name** as key (e.g. "customFieldValues": {"cfDealStage": "Active"}). Never use field ID as the key in the request—the API expects internal names.
- **Date/datetime fields**: The user gives the datetime in their own timezone (e.g. "11th Feb 2026 at 7:30 AM"). You MUST convert it to UTC before sending: call **get_current_user** → get timezone → call **parse_datetime_to_utc_iso_tool**(user's datetime string, user's timezone) → put the returned UTC ISO string in field_values for that date/datetime field. Do not send the user's local time as-is.

### NEVER guess IDs
- Always use the cheat sheet from `get_deal_field_instructions` for API names and IDs.
- Omit any field the user did not mention; do not add static/default fields.

### Search/Filter deals
- **By specific field:** When the user specifies a field (e.g. "deals where value is > 10000", "deals where status is Won"), use **search_deals** with the appropriate filter(s).
- Call `get_deal_field_instructions` first to see which fields are **filterable** (marked in cheat sheet).
- Only fields with filterable=true can be used in search_deals filters.
- For PICK_LIST/MULTI_PICKLIST: use **Option ID** (number) in filter value, except for: currency, country, dealSource — for these use **internal name** (string).
- Use the correct operator for the field type (see operator list in search_deals docstring).

### User look-up fields (createdBy, updatedBy, ownerId, etc.)
- These fields reference **users**; filter value must be the **user ID** (number), not the name.
- When the user asks e.g. "deals where owner is John": (1) Call **lookup_users** with query (e.g. "name:John"). (2) If **more than one** user is returned, ask which person. (3) Once exactly one user is identified, call **search_deals** with filter {"field": "ownerId", "operator": "equal", "value": <user_id>}.
- Do not guess user IDs; always use lookup_users first.

### Pipeline and pipeline stage (create, update, search)
- **Always resolve pipeline first when stage is involved.**
- Call **lookup_pipelines** with `entity_type="DEAL"` when resolving a deal pipeline.
- **To move a deal to a different stage in the same pipeline:** Use `update_deal(deal_id, {"pipelineStage": stage_id})`. The tool automatically updates the nested `pipeline.stage.id` and fetches the correct `forecastingType`.
- **To move a deal to a completely different pipeline:** Use `update_deal(deal_id, {"pipeline": {...}, "forecastingType": "..."})` with the full pipeline object including nested stage and the matching forecastingType.
- For closing reasons (Closed Lost, Closed Unqualified): call get_pipeline_details and ask user to pick a reason, then use update_deal with `pipelineStageReason` field.
- Then follow the same workflow as leads: ask for confirmation, call get_pipeline_stages, get_pipeline_details for closing reasons, etc.

### Date and datetime fields — timezone from current user (GET /users/me)
- Same as leads: call **get_current_user** first to get timezone.
- For creating a deal with a datetime field: call parse_datetime_to_utc_iso_tool(user's datetime string, user's timezone).
- For filtering by date/datetime: use the user's timezone in the filter; do not convert filter values to UTC.

### Idle / Stagnant deals (no activity for N days)
- "Idle" or "stagnant" means no activity on the deal for at least N days.
- Use **last activity** = the **later** of `updatedAt` and `latestActivityCreatedAt`.
- Use **search_deals** with filters: `updatedAt` ≤ threshold **and** `latestActivityCreatedAt` ≤ threshold (threshold = now − N days in ISO).

### Adding products to a deal
- When the user wants to add a product to a deal, you MUST ask for the following details before calling update_deal or create_deal:
  1. **Product name** — use `lookup_products` to resolve the product ID. If multiple matches, ask the user which one.
  2. **Quantity** — how many units (e.g. 10). Ask: "How many units?"
  3. **Price per unit** — the unit price (e.g. 100). Ask: "What is the price per unit?"
  4. **Currency** — which currency for the price (e.g. INR, USD). Ask: "Which currency?" (use the currencyId from deal or tenant).
  5. **Discount** (optional) — discount value and type. Ask: "Any discount? If yes, is it a percentage or flat amount?"
- Do NOT add a product without confirming price, quantity, and currency with the user first.
- Product payload format: `{"products": [{"id": <product_id>, "quantity": <qty>, "price": {"currencyId": <currency_id>, "value": <price>}, "discount": {"value": <disc>, "type": "PERCENTAGE"|"FLAT"}}]}`
- Existing products on the deal are preserved; new products are merged (duplicates by ID are skipped).
"""

# ---------------------------------------------------------------------------
# Company System Instructions
# ---------------------------------------------------------------------------

COMPANY_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Company Operations

## CRITICAL: Workflow for Companies

### Step 1: ALWAYS call `get_company_field_instructions` FIRST
Before creating or updating a company, you MUST call `get_company_field_instructions` to get:
- All available company fields (standard and custom)
- API names for standard fields (e.g. name, website, employees)
- Field IDs for custom fields
- Picklist option IDs for dropdowns

### Step 2: Create/Update company from user context only
- Do NOT use a fixed list of fields. Infer from the user's message what they want to create or update.
- Build `field_values` with ONLY the fields the user provided or implied.
- For **update_company**: pass the company ID (e.g. from search results) and the fields to update; same field_values format as create_company. For owner/ownerId use user ID from lookup_users.
- Keys: use API Name for standard fields (from cheat sheet), or Field ID string for custom fields.
- Values: use the exact format expected by Kylas (see below).
- **Companies do NOT have:** pipeline, pipelineStage, associatedContacts, products fields.

### Field value formats
- **Standard fields** (name, website, employees, etc.): use API name as key at top level.
- **emails**: array of objects (types OFFICE, PERSONAL only; exactly one must be primary). Or pass "email": "user@example.com" to normalize (OFFICE, primary).
- **phoneNumbers**: array of objects (types MOBILE, WORK, HOME, PERSONAL only; exactly one must be primary; "code" = 2-letter country e.g. IN, US). Or pass "phone": "5551234567". You MUST also pass "phone_country_code": "IN" or "+91" at the top level whenever any phone is included. **If the user gave phone number(s) but did NOT specify country or dial code:** Reply asking for country/dial code first.
- **Picklist fields**: use the **Option ID** (number) from the cheat sheet.
- **Custom fields**: MUST go in "customFieldValues" with **internal name** as key.

### NEVER guess IDs
- Always use the cheat sheet from `get_company_field_instructions` for API names and IDs.
- Omit any field the user did not mention; do not add static/default fields.

### Search/Filter companies
- **By specific field:** Use **search_companies** with the appropriate filter(s).
- Call `get_company_field_instructions` first to see which fields are **filterable**.
- For PICK_LIST/MULTI_PICKLIST: use **Option ID** (number), except for country — use **internal name** (string).

### Idle / Stagnant companies (no activity for N days)
- Use **search_companies** with filters: `updatedAt` ≤ threshold **and** `latestActivityCreatedAt` ≤ threshold (threshold = now − N days in ISO).
"""

# ---------------------------------------------------------------------------
# Meeting System Instructions
# ---------------------------------------------------------------------------

MEETING_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Meeting Operations

## CRITICAL: Workflow for Meetings

### Step 1: ALWAYS call `get_meeting_field_instructions` FIRST
Before creating or updating a meeting, you MUST call `get_meeting_field_instructions` to get:
- All available meeting fields (standard and custom)
- Timezone picklist with IDs
- Status options (scheduled, missed, conducted, cancelled)
- Medium options (OFFLINE, GOOGLE, MICROSOFT)

### Step 2: Ask the user for required details
Before creating a meeting, ask for:
1. **Title** — meeting title (required)
2. **Date and time** — start (from) and end (to) datetime (required). Convert to UTC using get_current_user + parse_datetime_to_utc_iso_tool.
3. **Participants** — who should attend (required). Use **lookup_meeting_invitees** first (or lookup_users for users only). Format: [{"id": user_id, "entity": "user|lead|contact|external"}] per API
4. **Related entities** (optional) — link to leads, contacts, deals, or companies. Format: [{"id": entity_id, "entity": "lead|contact|deal|company"}]
5. **Location** (optional) — meeting location
6. **All day** (optional) — whether it's an all-day meeting

### Create meeting payload format
```json
{
  "title": "Meeting Title",
  "from": "2024-01-15T08:00:00.000Z",
  "to": "2024-01-15T08:30:00.000Z",
  "allDay": false,
  "timezone": {"id": 372, "name": "Asia/Calcutta"},
  "participants": [{"id": 2530, "entity": "user"}],
  "relatedTo": [{"id": 150, "entity": "contact"}, {"id": 169, "entity": "deal"}],
  "location": "Office",
  "description": "Discussion about project"
}
```

### NEVER guess IDs
- Use **lookup_meeting_invitees** when the user mentions organizer or invitees by name (GET /search/meeting-invitee/lookup). Pick the row with the right **entity** (organizer is usually **user**).
- Use `lookup_users` for plain user ID resolution outside meeting-invitee context
- Use entity search tools to find IDs for relatedTo (e.g. search_leads_by_term, search_contacts_by_term, search_deals_by_term)

### Search/Filter meetings
- **By associated lead / contact / deal / company:** Always **resolve the entity id first** using the meeting lookup tools (same endpoints as Kylas web), then `search_meetings` with **long** rules:
  - Lead: **lookup_leads_for_meeting** (GET /search/lead/lookup?q=…, default `firstName:`) → filter `associatedLeads` `equal` `<lead_id>`
  - Contact: **lookup_contacts_for_meeting** (GET /search/contact/lookup?q=…, default `firstName:`) → `associatedContacts`
  - Deal: **lookup_deals_for_meeting** (GET /search/deal/lookup?q=…, default `name:`) → `associatedDeals`
  - Company: **lookup_companies_for_meeting** (GET /companies/lookup?view=meeting&q=…, default `comp:`) → `associatedCompanies`
  - Combine multiple rules in one jsonRule with **AND** (same as web).
- **Presence only:** `associatedLeads` / `associatedContacts` / `associatedDeals` / `associatedCompanies` with `is_not_null` or `is_null` and value null.
- **By organizer:** Call **lookup_meeting_invitees** first, then `search_meetings` with filter `{"field": "organizer", "operator": "equal", "value": <user_id>}` using the **user** row's id (if the API accepts the rule).
- **By specific field (status, date range, title, etc.):** Use `search_meetings` with filters.
  - Status filter: use internal name — "scheduled", "conducted", "missed", "cancelled"
  - Date filters: use from/to fields with operators like "greater", "less", "between"
- Call `get_meeting_field_instructions` first to see filterable fields.

### Cancel vs Delete meetings
- **cancel_meeting** — changes status to "cancelled" (reversible, meeting record preserved)
- **delete_meeting** — permanently removes the meeting (irreversible). Confirm with user before deleting.

### Notes on meetings
- Use `add_note("MEETING", meeting_id, "note text")` to add notes to a meeting.
"""

# ---------------------------------------------------------------------------
# Call Log System Instructions
# ---------------------------------------------------------------------------

CALL_LOG_SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Call Log Operations

## CRITICAL: Workflow for Call Logs

### Step 1: ALWAYS call `get_call_log_field_instructions` FIRST
Before creating a call log, you MUST call `get_call_log_field_instructions` to get:
- All available call log fields
- Outcome options (connected, rejected, busy, no_answer, missed_call, in_progress)
- Call type options (incoming, outgoing)
- Call disposition options
- Sentiment options

### Step 2: Ask the user for required details before creating
Before creating a call log, ask for:
1. **Entity** — which lead, contact, or deal is this call for? Search to get the ID.
2. **Phone number** — the phone number used in the call
3. **Call type** — "incoming" or "outgoing"
4. **Outcome** — "connected", "rejected", "busy", "no_answer", "missed_call", "in_progress"
5. **Start time** — when did the call start? Convert to UTC.
6. **Duration** (optional) — how long was the call in seconds?
7. **Notes** (optional) — any call notes?

### Call log on a Lead
```json
{
  "outcome": "connected", "callType": "outgoing",
  "startTime": "2024-01-15T08:00:00.000Z",
  "phoneNumber": "9618488578", "duration": "420",
  "relatedTo": {"id": 297573, "entity": "lead", "phoneNumber": "9618488578"},
  "notes": [{"description": "Discussed pricing"}]
}
```

### Call log on a Contact
Same as lead but `"entity": "contact"` in relatedTo.

### Call log on a Deal
Same as lead but `"entity": "deal"` in relatedTo. Optionally add `associatedTo` to link a contact:
```json
"associatedTo": [{"id": 112936, "entity": "contact", "phoneNumber": "9848022338"}]
```

### Fetching call logs
- Use `get_call_logs(entity_id, entity_type)` to get call logs for a lead, contact, or deal.

### Notes on call logs
- Use `add_note("CALL_LOG", call_log_id, "note text")` to add notes to a call log.
"""

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP("Kylas CRM", instructions=SYSTEM_INSTRUCTIONS + "\n\n" + DEAL_SYSTEM_INSTRUCTIONS + "\n\n" + COMPANY_SYSTEM_INSTRUCTIONS + "\n\n" + MEETING_SYSTEM_INSTRUCTIONS + "\n\n" + CALL_LOG_SYSTEM_INSTRUCTIONS)


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
        # Date rules: value left as-is (user's timezone); API uses timeZone for interpretation — do not convert to UTC

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
        lines.append("More than one product matched. Ask the user which one they mean, then use that ID in search_leads (e.g. filter products equal to that ID).")
    else:
        lines.append(f"Use product ID {content[0].get('id')} in search_leads when filtering by product (e.g. {{\"field\": \"products\", \"operator\": \"equal\", \"value\": <id>}}).")
    return "\n".join(lines)


@mcp.tool()
async def lookup_products(query: str, page: int = 0, size: int = 50) -> str:
    """
    Look up products by name. Use this BEFORE filtering leads by product when the user gives a product name.
    - If one product is found, use that product's ID in search_leads (e.g. {"field": "products", "operator": "equal", "value": <id>}).
    - If multiple products are found, ask the user which product they mean (list the options), then use the chosen product's ID in search_leads.
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
    unqualifiedReasons (for Closed Unqualified), and lostReasons (for Closed Lost).
    Use when moving a lead to Closed Lost or Closed Unqualified: get reasons, ask the user to pick one,
    then update_lead with pipelineStageReason set to that exact string.
    """
    pipeline_id = int(pipeline_id)
    async with get_client() as client:
        response = await client.get(f"/pipelines/{pipeline_id}")
        data = await handle_api_response(response, "Get pipeline details")
    name = data.get("name", "—")
    lines = [
        f"Pipeline: {name} (ID: {pipeline_id})",
        "",
        "Stages:",
    ]
    for s in data.get("stages", []):
        sid = s.get("id", "?")
        sname = s.get("name", "—")
        ftype = s.get("forecastingType", "")
        lines.append(f"  • Stage ID: {sid}  |  Name: {sname}  |  forecastingType: {ftype}")
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

# Kylas API uses 2-letter country codes (e.g. IN, US). No default—caller must provide when phone is used.
COUNTRY_CODE_MAP = {
    "+91": "IN", "IN": "IN", "in": "IN", "india": "IN",
    "+1": "US", "US": "US", "us": "US", "usa": "US",
    "GB": "GB", "+44": "GB", "UK": "GB",
}


def _normalize_country_code(code: Optional[str]) -> str:
    """Normalize user-provided country code to Kylas 2-letter code. Returns empty string if not provided (caller must require it when phone is present)."""
    if not code or not str(code).strip():
        return ""
    raw = str(code).strip().upper() if len(str(code).strip()) <= 3 else str(code).strip()
    return COUNTRY_CODE_MAP.get(raw) or COUNTRY_CODE_MAP.get(code) or (raw if len(raw) == 2 else "")


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


def _normalize_field_values(
    field_values: Dict[str, Any],
    custom_field_id_to_name: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Build Kylas create-lead payload from dynamic field_values.
    - Custom fields (numeric keys or in customFieldValues) → customFieldValues with INTERNAL NAME as key (never ID).
    - Explicit "customFieldValues" dict → merged; keys must be internal names (e.g. cfLeadCheck).
    - "email" string → emails array (type OFFICE, primary true). One email must be primary.
    - "phone" / "phoneNumber" + "phone_country_code" (required when phone given) → phoneNumbers array (type MOBILE, code as 2-letter e.g. IN). Caller must ask user for country/dial code when phone is given without it; do not assume or infer. One phone must be primary.
    - emails/phoneNumbers arrays: allowed types email OFFICE|PERSONAL, phone MOBILE|WORK|HOME|PERSONAL; exactly one primary (first if unspecified).
    - Rest → top-level payload (standard fields)
    """
    payload: Dict[str, Any] = {}
    custom: Dict[str, Any] = {}
    fv = dict(field_values)
    id_to_name = custom_field_id_to_name or {}

    phone_country_raw = fv.pop("phone_country_code", None)
    phone_country = _normalize_country_code(phone_country_raw)
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
                    "Ask the user which country and dial code to use and include 'phone_country_code' in field_values."
                )
            payload["phoneNumbers"] = _ensure_single_primary(
                [{"type": "MOBILE", "code": phone_country, "value": value.strip(), "primary": True}],
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
    logger.info("Creating lead with fields: %s", list(payload.keys()))
    async with get_client() as client:
        response = await client.post("/leads", json=payload)
        result = await handle_api_response(response, "Create lead")
        logger.info("Lead created with ID: %s", result.get("id"))
        return result


@mcp.tool()
async def create_lead(field_values: Dict[str, Any]) -> str:
    """
    Create a lead in Kylas CRM with only the fields the user wants (no static field list).
    
    You MUST call get_lead_field_instructions FIRST to get valid API names and Field IDs.
    Infer from user context which fields to send; include only those in field_values.
    
    field_values: Map of field identifier to value.
    - Standard fields: use API name as key at top level (e.g. firstName, lastName, companyName, emails, phoneNumbers, leadSource, isNew).
    - Custom fields: MUST be under "customFieldValues" with **internal name** as key (e.g. "customFieldValues": {"cfLeadCheck": "Checked"}). Do not use field ID as key—Kylas expects internal names. If you pass a field ID (e.g. "1210985"), the server will resolve it to the internal name (e.g. cfLeadCheck) automatically.
    - For a single email use "email": "user@example.com". For phones use "phone": "5551234567" (or "phoneNumbers" array) and you MUST include "phone_country_code": "IN" or "+91" at top level. If the user provided phone(s) but did not specify country or dial code, do NOT call create_lead—ask the user (e.g. which country/dial code for these numbers?) and only call after they respond. Do not infer from currency or other context. Email types: OFFICE, PERSONAL. Phone types: MOBILE, WORK, HOME, PERSONAL. Exactly one email and at most one phone should be primary; first entry is primary by default.
    - For picklists use the Option ID (number) from the cheat sheet.
    - For date/datetime fields: the user gives a time in their timezone (e.g. "11th Feb 2026 at 7:30 AM"). Call get_current_user, then parse_datetime_to_utc_iso_tool(local_datetime, timezone) and put the returned UTC ISO string in field_values.
    """
    try:
        _reset_api_call_count()
        result = await create_lead_logic(field_values)
        lead_id = result.get("id", "?")
        name = f"{result.get('firstName', '')} {result.get('lastName', '')}".strip() or "Lead"
        return f"✓ Lead created successfully.\n  ID: {lead_id}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create lead: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_lead")
        return f"✗ Unexpected error: {str(e)}"


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
    logger.info("Updating lead %s with fields: %s", lead_id, list(payload.keys()))
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
        logger.info("Lead %s updated", lead_id)
        return result


@mcp.tool()
async def update_lead(lead_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a lead in Kylas CRM. Fetches the lead first, merges your field_values into it, then PUTs the full body.
    Same field_values format as create_lead. Call get_lead_field_instructions first for API names and custom field internal names.
    For owner: use lookup_users to get the user ID, then pass ownerId: <id> in field_values.

    lead_id: The lead ID to update (e.g. from search_leads or search_leads_by_term results).
    field_values: Map of field identifier to value (same as create_lead: firstName, lastName, email, phone with phone_country_code, customFieldValues, picklist Option IDs, date/datetime in UTC ISO, etc.). These are merged over the existing lead; other fields are left unchanged.
    """
    try:
        _reset_api_call_count()
        result = await update_lead_logic(lead_id, field_values)
        lid = result.get("id", lead_id)
        name = f"{result.get('firstName', '')} {result.get('lastName', '')}".strip() or "Lead"
        return f"✓ Lead updated successfully.\n  ID: {lid}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update lead: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_lead")
        return f"✗ Unexpected error: {str(e)}"


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


@mcp.tool()
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


@mcp.tool()
async def search_idle_leads(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search for idle/stagnant leads: no activity for at least the given number of days.
    Uses both updatedAt and latestActivityCreatedAt; a lead is returned only when BOTH dates
    are on or before (today − days), so the effective last activity is before the threshold.

    days: Minimum days with no activity (e.g. 10 for "no activity since 10 days").
    time_zone: IANA timezone for threshold (e.g. America/New_York). Default: Asia/Calcutta.
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        if days < 0:
            return "Error: days must be non-negative."
        return await search_idle_leads_logic(days, time_zone, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search idle leads failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_idle_leads")
        return f"✗ Unexpected error: {str(e)}"


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


@mcp.tool()
async def create_contact(field_values: Dict[str, Any]) -> str:
    """
    Create a contact with dynamic field values. Same format as create_lead but without pipeline fields.
    ALWAYS call get_contact_field_instructions FIRST to get field names and IDs.
    """
    try:
        _reset_api_call_count()
        result = await create_contact_logic(field_values)
        contact_id = result.get("id", "?")
        first_name = result.get("firstName", "")
        last_name = result.get("lastName", "")
        name = f"{first_name} {last_name}".strip() or "Contact"
        return f"✓ Contact created successfully.\n  ID: {contact_id}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create contact: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_contact")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def update_contact(contact_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a contact. Fetches current contact, merges your field_values into it, then updates.
    Same field_values format as create_contact. Call get_contact_field_instructions first.
    """
    try:
        _reset_api_call_count()
        result = await update_contact_logic(contact_id, field_values)
        cid = result.get("id", contact_id)
        first_name = result.get("firstName", "")
        last_name = result.get("lastName", "")
        name = f"{first_name} {last_name}".strip() or "Contact"
        return f"✓ Contact updated successfully.\n  ID: {cid}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update contact: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_contact")
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


@mcp.tool()
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
async def create_task(field_values: Dict[str, Any]) -> str:
    """
    Create a task with dynamic field values. Same format as create_contact/create_lead but task-specific.
    ALWAYS call get_task_field_instructions FIRST to get field names and IDs.

    **Important:** To assign the task to entities (lead, contact, deal, company), include "relation" in field_values:
    "relation": [
      {"targetEntityId": <lead_id>, "targetEntityType": "LEAD", "targetEntityName": "<lead_name>"},
      {"targetEntityId": <contact_id>, "targetEntityType": "CONTACT", "targetEntityName": "<contact_name>"}
    ]

    Example: create_task({
      "name": "Follow up",
      "dueDate": "2026-03-26T18:29:59.999Z",
      "assignedTo": 594,
      "relation": [
        {"targetEntityId": 45089710, "targetEntityType": "LEAD", "targetEntityName": "Akshay"}
      ]
    })
    """
    try:
        _reset_api_call_count()
        result = await create_task_logic(field_values)
        task_id = result.get("id", "?")
        name = result.get("name", "Task")
        return f"✓ Task created successfully.\n  ID: {task_id}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create task: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_task")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def update_task(task_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a task. Fetches current task, merges your field_values into it, then updates.
    Same field_values format as create_task. Call get_task_field_instructions first.
    """
    try:
        _reset_api_call_count()
        result = await update_task_logic(task_id, field_values)
        tid = result.get("id", task_id)
        name = result.get("name", "Task")
        return f"✓ Task updated successfully.\n  ID: {tid}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update task: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_task")
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
    logger.info("Searching tasks with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/tasks/search", params=params, json=payload)
        data = await handle_api_response(response, "Search tasks")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No tasks found matching the filters. (Total in DB: {total})"
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
async def lookup_leads_for_task_tool(search_term: str = "") -> str:
    """
    Look up leads to associate with a task.
    Returns top 10 leads matching the search term (searches by firstName).
    Use this when the user wants to create a task for a specific lead.
    """
    try:
        _reset_api_call_count()
        result = await lookup_leads_for_task(search_term)
        leads = result if isinstance(result, list) else result.get("data", result.get("content", []))
        if not leads:
            return f"No leads found matching '{search_term}'."
        lines = [f"Found {len(leads)} lead(s) matching '{search_term}':", "-" * 60]
        for lead in leads[:10]:
            lead_id = lead.get("id", "?")
            first_name = lead.get("firstName", "—")
            last_name = lead.get("lastName", "—")
            company = lead.get("companyName", "—")
            lines.append(f"• ID: {lead_id} | Name: {first_name} {last_name} | Company: {company}")
        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Lookup failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_leads_for_task_tool")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_contacts_for_task_tool(search_term: str = "") -> str:
    """
    Look up contacts to associate with a task.
    Returns top 10 contacts matching the search term (searches by name).
    Use this when the user wants to create a task for a specific contact.
    """
    try:
        _reset_api_call_count()
        result = await lookup_contacts_for_task(search_term)
        contacts = result if isinstance(result, list) else result.get("data", result.get("content", []))
        if not contacts:
            return f"No contacts found matching '{search_term}'."
        lines = [f"Found {len(contacts)} contact(s) matching '{search_term}':", "-" * 60]
        for contact in contacts[:10]:
            contact_id = contact.get("id", "?")
            name = contact.get("name", "—")
            email = contact.get("emails", [{}])[0].get("value", "—") if contact.get("emails") else "—"
            lines.append(f"• ID: {contact_id} | Name: {name} | Email: {email}")
        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Lookup failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_contacts_for_task_tool")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_deals_for_task_tool(search_term: str = "") -> str:
    """
    Look up deals to associate with a task.
    Returns top 10 deals matching the search term (searches by name).
    Use this when the user wants to create a task for a specific deal.
    """
    try:
        _reset_api_call_count()
        result = await lookup_deals_for_task(search_term)
        deals = result if isinstance(result, list) else result.get("data", result.get("content", []))
        if not deals:
            return f"No deals found matching '{search_term}'."
        lines = [f"Found {len(deals)} deal(s) matching '{search_term}':", "-" * 60]
        for deal in deals[:10]:
            deal_id = deal.get("id", "?")
            name = deal.get("name", "—")
            value = deal.get("value", "—")
            lines.append(f"• ID: {deal_id} | Name: {name} | Value: {value}")
        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Lookup failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_deals_for_task_tool")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_companies_for_task_tool(search_term: str = "") -> str:
    """
    Look up companies to associate with a task.
    Returns top 10 companies matching the search term (searches by name).
    Use this when the user wants to create a task for a specific company.
    """
    try:
        _reset_api_call_count()
        result = await lookup_companies_for_task(search_term)
        companies = result if isinstance(result, list) else result.get("data", result.get("content", []))
        if not companies:
            return f"No companies found matching '{search_term}'."
        lines = [f"Found {len(companies)} compan(ies) matching '{search_term}':", "-" * 60]
        for company in companies[:10]:
            company_id = company.get("id", "?")
            name = company.get("name", "—")
            industry = company.get("industry", "—")
            lines.append(f"• ID: {company_id} | Name: {name} | Industry: {industry}")
        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Lookup failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_companies_for_task_tool")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_tasks_for_lead(
    lead_id: int,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "dueDate,asc",
) -> str:
    """
    Find tasks associated with a specific lead.
    Pass the lead ID to get all tasks linked to that lead.
    Internally filters by associatedLeads field.
    """
    try:
        _reset_api_call_count()
        filters = [{"field": "associatedLeads", "operator": "equal", "value": int(lead_id)}]
        return await search_tasks_logic(filters, page, size, sort)
    except ValueError as e:
        return f"✗ Invalid lead ID: {e}"
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks_for_lead")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_tasks_for_contact(
    contact_id: int,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "dueDate,asc",
) -> str:
    """
    Find tasks associated with a specific contact.
    Pass the contact ID to get all tasks linked to that contact.
    Internally filters by associatedContacts field.
    """
    try:
        _reset_api_call_count()
        filters = [{"field": "associatedContacts", "operator": "equal", "value": int(contact_id)}]
        return await search_tasks_logic(filters, page, size, sort)
    except ValueError as e:
        return f"✗ Invalid contact ID: {e}"
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks_for_contact")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_tasks_for_deal(
    deal_id: int,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "dueDate,asc",
) -> str:
    """
    Find tasks associated with a specific deal.
    Pass the deal ID to get all tasks linked to that deal.
    Internally filters by associatedDeals field.
    """
    try:
        _reset_api_call_count()
        filters = [{"field": "associatedDeals", "operator": "equal", "value": int(deal_id)}]
        return await search_tasks_logic(filters, page, size, sort)
    except ValueError as e:
        return f"✗ Invalid deal ID: {e}"
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks_for_deal")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_tasks_for_company(
    company_id: int,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "dueDate,asc",
) -> str:
    """
    Find tasks associated with a specific company.
    Pass the company ID to get all tasks linked to that company.
    Internally filters by associatedCompanies field.
    """
    try:
        _reset_api_call_count()
        filters = [{"field": "associatedCompanies", "operator": "equal", "value": int(company_id)}]
        return await search_tasks_logic(filters, page, size, sort)
    except ValueError as e:
        return f"✗ Invalid company ID: {e}"
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks_for_company")
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

async def create_deal_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a deal with the given dynamic field_values (Kylas API payload shape)."""
    fv = dict(field_values)
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_deal_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")
    logger.info("Creating deal with fields: %s", list(payload.keys()))
    async with get_client() as client:
        response = await client.post("/deals", json=payload)
        result = await handle_api_response(response, "Create deal")
        logger.info("Deal created with ID: %s", result.get("id"))
        return result


@mcp.tool()
async def create_deal(field_values: Dict[str, Any]) -> str:
    """
    Create a deal in Kylas CRM with only the fields the user wants (no static field list).

    You MUST call get_deal_field_instructions FIRST to get valid API names and Field IDs.
    Infer from user context which fields to send; include only those in field_values.

    field_values: Map of field identifier to value.
    - Standard fields: use API name as key at top level (e.g. name, value, closingDate, dealSource).
    - Custom fields: MUST be under "customFieldValues" with **internal name** as key (e.g. "customFieldValues": {"cfDealStatus": "Active"}). Do not use field ID as key.
    - For a single email use "email": "user@example.com". For phones use "phone": "5551234567" (or "phoneNumbers" array) and you MUST include "phone_country_code": "IN" or "+91" at top level.
    - For picklists use the Option ID (number) from the cheat sheet.
    - For date/datetime fields: call get_current_user, then parse_datetime_to_utc_iso_tool and put the UTC ISO string in field_values.
    - For products: use "products" with a list of product objects, e.g. [{"id": 245208, "quantity": 10, "price": {"currencyId": 431, "value": 100}}].
      Use lookup_products to find product IDs first.
    """
    try:
        _reset_api_call_count()
        result = await create_deal_logic(field_values)
        deal_id = result.get("id", "?")
        name = result.get("name", "Deal")
        return f"✓ Deal created successfully.\n  ID: {deal_id}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create deal: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_deal")
        return f"✗ Unexpected error: {str(e)}"


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
                    logger.warning("Trying to set pipelineStage but deal has no pipeline object")
                    raise KylasAPIError("Deal has no pipeline; cannot set stage. Use move_deal_to_stage instead.")
            else:
                merged[key] = value
        response = await client.put(f"/deals/{deal_id}", json=merged)
        result = await handle_api_response(response, "Update deal")
        logger.info("Deal %s updated", deal_id)
        return result


@mcp.tool()
async def update_deal(deal_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a deal in Kylas CRM. Fetches the deal first, merges your field_values into it, then PUTs the full body.
    Same field_values format as create_deal. Call get_deal_field_instructions first for API names.

    deal_id: The deal ID to update (e.g. from search_deals or search_deals_by_term results).
    field_values: Map of field identifier to value (same as create_deal).

    **Pipeline updates:**
    - To change stage in current pipeline: {"pipelineStage": stage_id}
      (automatically updates pipeline.stage.id and forecastingType)
    - To change to different pipeline: {"pipeline": {"id": id, "name": "name", "stage": {"id": id, "name": "name"}}, "forecastingType": "..."}

    **Link contacts (Kylas deal PUT):** use `associatedContacts` with `{id, name}` per contact, e.g.
    `{"associatedContacts": [{"id": 4942095, "name": "aditya tambe"}]}`. Existing rows are kept; duplicates by id are skipped.
    You may also use `contacts` as an alias (same merge into `associatedContacts`). If `name` is omitted, it is loaded from GET /contacts/{id}.

    **Add products:** use `products` with a list of product objects. Each product needs at minimum `id` (product ID from lookup_products).
    Optional fields: `quantity` (default 1), `name`, `price` ({"currencyId": id, "value": amount}),
    `discount` ({"value": 0, "type": "PERCENTAGE"|"FLAT"}), `category` ({"id": id, "name": name}),
    `hsnSacCode`, `countryOfOrigin` ({"id": id, "name": name}), `customFieldValues`.
    Example: `{"products": [{"id": 245208, "quantity": 10, "price": {"currencyId": 431, "value": 100}}]}`.
    Existing products are kept; new products are merged (duplicates by id are skipped).
    """
    try:
        _reset_api_call_count()
        result = await update_deal_logic(deal_id, field_values)
        did = result.get("id", deal_id)
        name = result.get("name", "Deal")
        return f"✓ Deal updated successfully.\n  ID: {did}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update deal: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_deal")
        return f"✗ Unexpected error: {str(e)}"


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
        "fields": ["id", "name", "value", "emails", "phoneNumbers", "ownerId", "createdAt"],
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
        email = _extract_primary_email(deal.get("emails"))
        phone = _extract_primary_phone(deal.get("phoneNumbers"))
        lines.append(f"• ID: {did} | Name: {name} | Value: {value} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_deals(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter deals. Only fields marked [FILTERABLE] in get_deal_field_instructions can be used.
    Call get_deal_field_instructions first to get filterable fields and their types.

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


# ===========================================================================
# COMPANY ENTITY
# ===========================================================================

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


@mcp.tool()
async def create_company(field_values: Dict[str, Any]) -> str:
    """
    Create a company in Kylas CRM with only the fields the user wants (no static field list).

    You MUST call get_company_field_instructions FIRST to get valid API names and Field IDs.
    Infer from user context which fields to send; include only those in field_values.

    field_values: Map of field identifier to value.
    - Standard fields: use API name as key at top level (e.g. name, website, employees).
    - Custom fields: MUST be under "customFieldValues" with **internal name** as key (e.g. "customFieldValues": {"cfCompanyType": "Enterprise"}). Do not use field ID as key.
    - For a single email use "email": "user@example.com". For phones use "phone": "5551234567" (or "phoneNumbers" array) and you MUST include "phone_country_code": "IN" or "+91" at top level.
    - For picklists use the Option ID (number) from the cheat sheet.
    - For date/datetime fields: call get_current_user, then parse_datetime_to_utc_iso_tool and put the UTC ISO string in field_values.
    """
    try:
        _reset_api_call_count()
        result = await create_company_logic(field_values)
        company_id = result.get("id", "?")
        name = result.get("name", "Company")
        return f"✓ Company created successfully.\n  ID: {company_id}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create company: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_company")
        return f"✗ Unexpected error: {str(e)}"


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


@mcp.tool()
async def update_company(company_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a company in Kylas CRM. Fetches the company first, merges your field_values into it, then PUTs the full body.
    Same field_values format as create_company. Call get_company_field_instructions first for API names.

    company_id: The company ID to update (e.g. from search_companies or search_companies_by_term results).
    field_values: Map of field identifier to value (same as create_company).
    """
    try:
        _reset_api_call_count()
        result = await update_company_logic(company_id, field_values)
        cid = result.get("id", company_id)
        name = result.get("name", "Company")
        return f"✓ Company updated successfully.\n  ID: {cid}\n  Name: {name}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update company: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_company")
        return f"✗ Unexpected error: {str(e)}"


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


@mcp.tool()
async def search_companies(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter companies. Only fields marked [FILTERABLE] in get_company_field_instructions can be used.
    Call get_company_field_instructions first to get filterable fields and their types.

    filters: List of filter objects. Each must have:
      - field (str): Field internal/API name (e.g. name, website, country, createdAt).
      - operator (str): One of the allowed operators for that field type (e.g. equal, contains, greater).
      - value: Value to compare. For PICK_LIST/MULTI_PICKLIST use Option ID (number), except
        country — use internal name (string).
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
        return await search_companies_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_companies")
        return f"✗ Unexpected error: {str(e)}"


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
        "  - Use lookup_users for users only; use lookup_meeting_invitees to resolve users/leads/contacts/external for meetings",
        "organizer / invitee by name: call lookup_meeting_invitees first (GET /search/meeting-invitee/lookup), then use the matching id + entity",
        "",
        "relatedTo: [{\"id\": <entity_id>, \"entity\": \"lead|contact|deal|company\"}]",
        "  - Links meeting to leads, contacts, deals, or companies",
        "Meeting search: associatedLeads, associatedContacts, associatedDeals, associatedCompanies — is_null / is_not_null or equal <id> (type long). Resolve ids via lookup_leads_for_meeting, lookup_contacts_for_meeting, lookup_deals_for_meeting, lookup_companies_for_meeting first.",
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


@mcp.tool()
async def lookup_meeting_invitees(query: str) -> str:
    """
    Look up meeting invitees and organizer candidates (users, leads, contacts, external).
    Calls GET /search/meeting-invitee/lookup — same as the Kylas web app.

    Call this FIRST when the user mentions meeting organizer or invitees by name, before
    search_meetings (organizer filter) or create_meeting (participants). Rows include
    id, name, entity (user | lead | contact | external); for organizer, use the user row
    that matches the person's name.

    query: Search string (e.g. broad 'key:' or a name / field:value pattern per your tenant).
    """
    try:
        _reset_api_call_count()
        return await lookup_meeting_invitees_logic(query)
    except KylasAPIError as e:
        return f"Error: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_meeting_invitees")
        return f"Unexpected error: {str(e)}"


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


@mcp.tool()
async def lookup_leads_for_meeting(query: str = "firstName:") -> str:
    """
    Look up leads for meeting association / search filters. GET /search/lead/lookup (Kylas web).

    Call FIRST when the user wants meetings linked to a lead by name — then search_meetings with
    {"field": "associatedLeads", "operator": "equal", "value": <lead_id>}.

    query: Lookup q string (default firstName: for a broad list; e.g. firstName:John or companyName:Acme per tenant).
    """
    try:
        _reset_api_call_count()
        return await lookup_leads_for_meeting_logic(query)
    except KylasAPIError as e:
        return f"Error: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_leads_for_meeting")
        return f"Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_contacts_for_meeting(query: str = "firstName:") -> str:
    """
    Look up contacts for meeting association / search filters. GET /search/contact/lookup.

    Then search_meetings with {"field": "associatedContacts", "operator": "equal", "value": <contact_id>}.

    query: Lookup q (default firstName:).
    """
    try:
        _reset_api_call_count()
        return await lookup_contacts_for_meeting_logic(query)
    except KylasAPIError as e:
        return f"Error: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_contacts_for_meeting")
        return f"Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_deals_for_meeting(query: str = "name:") -> str:
    """
    Look up deals for meeting association / search filters. GET /search/deal/lookup.

    Then search_meetings with {"field": "associatedDeals", "operator": "equal", "value": <deal_id>}.

    query: Lookup q (default name:).
    """
    try:
        _reset_api_call_count()
        return await lookup_deals_for_meeting_logic(query)
    except KylasAPIError as e:
        return f"Error: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_deals_for_meeting")
        return f"Unexpected error: {str(e)}"


@mcp.tool()
async def lookup_companies_for_meeting(query: str = "comp:") -> str:
    """
    Look up companies for meeting association / search filters. GET /companies/lookup?view=meeting.

    Then search_meetings with {"field": "associatedCompanies", "operator": "equal", "value": <company_id>}.

    query: Lookup q (default comp:).
    """
    try:
        _reset_api_call_count()
        return await lookup_companies_for_meeting_logic(query)
    except KylasAPIError as e:
        return f"Error: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("lookup_companies_for_meeting")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Meeting search rule builder
# ---------------------------------------------------------------------------

# POST /meetings/search supports these fields even when GET /meetings/fields omits them from metadata.
_MEETING_SEARCH_SYNTHETIC_FILTERABLE: Dict[str, Dict[str, Any]] = {
    "associatedLeads": {"type": "LOOK_UP", "standard": True},
    "associatedContacts": {"type": "LOOK_UP", "standard": True},
    "associatedDeals": {"type": "LOOK_UP", "standard": True},
    "associatedCompanies": {"type": "LOOK_UP", "standard": True},
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
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
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

    logger.info("Creating meeting: %s", payload.get("title", ""))
    async with get_client() as client:
        response = await client.post("/meetings", json=payload)
        result = await handle_api_response(response, "Create meeting")
        logger.info("Meeting created with ID: %s", result.get("id"))
        return result


@mcp.tool()
async def create_meeting(field_values: Dict[str, Any]) -> str:
    """
    Create a meeting in Kylas CRM.

    You MUST call get_meeting_field_instructions FIRST. Then ask the user for required details.

    field_values: Meeting payload with these fields:
    - title (str, REQUIRED): Meeting title
    - from (str, REQUIRED): Start datetime in UTC ISO format (e.g. "2024-01-15T08:00:00.000Z")
    - to (str, REQUIRED): End datetime in UTC ISO format
    - allDay (bool): Whether it's an all-day meeting (default false)
    - participants (list, REQUIRED): List of invitees, e.g. [{"id": 2530, "entity": "user"}]
      Use lookup_users to find user IDs.
    - relatedTo (list): Related entities, e.g. [{"id": 150, "entity": "contact"}, {"id": 169, "entity": "deal"}]
      Entity can be: "lead", "contact", "deal", "company"
    - timezone (dict): e.g. {"id": 372, "name": "Asia/Calcutta"} — get ID from get_meeting_field_instructions
    - location (str): Meeting location
    - description (str): Meeting description

    IMPORTANT: Convert user's local datetime to UTC using get_current_user (for timezone) then parse_datetime_to_utc_iso_tool.
    """
    try:
        _reset_api_call_count()
        result = await create_meeting_logic(field_values)
        meeting_id = result.get("id", "?")
        title = result.get("title", "Meeting")
        return f"✓ Meeting created successfully.\n  ID: {meeting_id}\n  Title: {title}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create meeting: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_meeting")
        return f"✗ Unexpected error: {str(e)}"


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
                    if isinstance(p, dict) and (p.get("id"), p.get("entity")) not in existing_keys:
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


@mcp.tool()
async def update_meeting(meeting_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a meeting in Kylas CRM. Fetches the meeting first, merges your field_values, then PUTs the full body.

    meeting_id: The meeting ID to update.
    field_values: Map of field to value (same format as create_meeting).
      - Participants and relatedTo are merged with existing (duplicates by id+entity are skipped).
      - Other fields are overwritten.
    """
    try:
        _reset_api_call_count()
        result = await update_meeting_logic(meeting_id, field_values)
        mid = result.get("id", meeting_id)
        title = result.get("title", "Meeting")
        return f"✓ Meeting updated successfully.\n  ID: {mid}\n  Title: {title}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update meeting: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_meeting")
        return f"✗ Unexpected error: {str(e)}"


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


def _meetings_search_api_page(page_zero_based: int) -> int:
    """POST /meetings/search uses 1-based page; tools stay 0-based."""
    return int(page_zero_based) + 1


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
    params = {"page": _meetings_search_api_page(page), "size": min(size, 100)}
    if sort:
        params["sort"] = sort
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
        mid = m.get("id", "?")
        title = m.get("title", "—")
        status = m.get("status", "—")
        from_dt = m.get("from", "—")
        to_dt = m.get("to", "—")
        location = m.get("location", "—") or "—"
        lines.append(f"• ID: {mid} | Title: {title} | Status: {status} | From: {from_dt} | To: {to_dt} | Location: {location}")
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_meetings(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "from,desc",
) -> str:
    """
    Search/filter meetings. Use [FILTERABLE] from get_meeting_field_instructions, plus synthetic fields:
    associatedLeads, associatedContacts, associatedDeals, associatedCompanies (long; equal / is_null / is_not_null).
    Resolve entity ids with lookup_leads_for_meeting, lookup_contacts_for_meeting, lookup_deals_for_meeting,
    lookup_companies_for_meeting before filtering by equal.

    filters: List of filter objects. Each must have:
      - field (str): e.g. title, status, from, owner, associatedLeads, associatedContacts, associatedDeals, associatedCompanies.
      - operator (str): e.g. equal, contains, is_null, is_not_null.
      - value: For status use internal name. For associated* equal, numeric id. For is_null / is_not_null, omit or null.
      - timeZone (str, optional): For date/datetime filters.
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
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


@mcp.tool()
async def search_all_meetings(
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "from,asc",
) -> str:
    """
    Get all meetings (no filters). Use when user asks to see upcoming meetings or all meetings.

    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "from,asc" (default — upcoming first).
    """
    try:
        _reset_api_call_count()
        payload = {"jsonRule": _MEETING_SEARCH_JSON_RULE_ALL}
        params = {"page": _meetings_search_api_page(page), "size": min(size, 100)}
        if sort:
            params["sort"] = sort
        logger.info("Fetching all meetings (page %d)", page)
        async with get_client() as client:
            response = await client.post("/meetings/search", params=params, json=payload)
            data = await handle_api_response(response, "Search all meetings")
        results = data.get("content", data.get("data", []))
        total = data.get("totalElements", data.get("total", len(results)))
        total_pages = data.get("totalPages", 1)
        if not results:
            return "No meetings found."
        lines = [f"Found {len(results)} meeting(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
        for m in results:
            mid = m.get("id", "?")
            title = m.get("title", "—")
            status = m.get("status", "—")
            from_dt = m.get("from", "—")
            to_dt = m.get("to", "—")
            location = m.get("location", "—") or "—"
            lines.append(f"• ID: {mid} | Title: {title} | Status: {status} | From: {from_dt} | To: {to_dt} | Location: {location}")
        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_all_meetings")
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


@mcp.tool()
async def create_call_log(field_values: Dict[str, Any]) -> str:
    """
    Create a call log in Kylas CRM. Links a call to a lead, contact, or deal.

    You MUST call get_call_log_field_instructions FIRST. Then ask the user for required details:
    1. Which entity (lead/contact/deal) and its ID
    2. Phone number
    3. Call type: "incoming" or "outgoing"
    4. Outcome: "connected", "rejected", "busy", "no_answer", "missed_call", "in_progress"
    5. Start time (convert to UTC using get_current_user + parse_datetime_to_utc_iso_tool)
    6. Duration in seconds (optional)
    7. Notes (optional)

    field_values: Call log payload:
    - outcome (str, REQUIRED): "connected", "rejected", "busy", "no_answer", "missed_call", "in_progress"
    - callType (str, REQUIRED): "incoming" or "outgoing"
    - startTime (str, REQUIRED): UTC ISO datetime (e.g. "2024-01-15T08:00:00.000Z")
    - phoneNumber (str, REQUIRED): Phone number (e.g. "9618488578")
    - duration (int/str): Duration in seconds (e.g. 420)
    - relatedTo (dict, REQUIRED): {"id": <entity_id>, "entity": "lead|contact|deal", "phoneNumber": "..."}
    - associatedTo (list, optional): For deal calls, associate contacts: [{"id": <contact_id>, "entity": "contact", "phoneNumber": "..."}]
    - notes (list, optional): [{"description": "Note text"}]
    - callRecording (dict, optional): {"url": "https://...", "fileName": "call.mp3", "data": ""}
    """
    try:
        _reset_api_call_count()
        result = await create_call_log_logic(field_values)
        log_id = result.get("id", "?")
        outcome = result.get("outcome", "")
        call_type = result.get("callType", "")
        return f"✓ Call log created successfully.\n  ID: {log_id}\n  Type: {call_type}\n  Outcome: {outcome}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to create call log: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("create_call_log")
        return f"✗ Unexpected error: {str(e)}"


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


@mcp.tool()
async def update_call_log(call_log_id: int, field_values: Dict[str, Any]) -> str:
    """
    Update a call log in Kylas CRM (PUT — full replace).

    call_log_id: The call log ID to update.
    field_values: Full call log payload (same format as create_call_log).
      Must include all required fields: outcome, startTime, phoneNumber, callType, relatedTo.
    """
    try:
        _reset_api_call_count()
        result = await update_call_log_logic(call_log_id, field_values)
        lid = result.get("id", call_log_id)
        return f"✓ Call log updated successfully.\n  ID: {lid}"
    except ValueError as e:
        return f"✗ {e}"
    except KylasAPIError as e:
        return f"✗ Failed to update call log: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("update_call_log")
        return f"✗ Unexpected error: {str(e)}"


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
    Get call logs for a specific lead, contact, or deal.

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
        async with get_client() as client:
            response = await client.get(
                f"/call-logs/{entity_id}",
                params={"relatedToType": entity_type_lower, "page": page, "size": min(size, 100)}
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

        lines = [f"Found {len(results)} call log(s) for {entity_type_lower} {entity_id} (total {total})", "-" * 60]
        for log in results:
            lid = log.get("id", "?")
            call_type = log.get("callType", "—")
            outcome = log.get("outcome", "—")
            phone = log.get("phoneNumber", "—")
            start = log.get("startTime", "—")
            duration = log.get("duration", "—")
            lines.append(f"• ID: {lid} | Type: {call_type} | Outcome: {outcome} | Phone: {phone} | Start: {start} | Duration: {duration}s")
        lines.append("-" * 60)
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


def _format_call_log_search_result(log: Dict[str, Any]) -> str:
    """Format a single call log search result."""
    lid = log.get("id", "?")
    call_type = log.get("callType", "—")
    outcome = log.get("outcome", "—")
    phone = log.get("phoneNumber", "—")
    start = log.get("startTime", "—")
    duration = log.get("duration", "—")
    related = log.get("relatedTo") or {}
    related_name = related.get("name", "—") if isinstance(related, dict) else "—"
    related_entity = related.get("entity", "") if isinstance(related, dict) else ""
    return f"• ID: {lid} | Type: {call_type} | Outcome: {outcome} | Phone: {phone} | Start: {start} | Duration: {duration}s | Related: {related_name} ({related_entity})"


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
    lines = [f"Found {len(results)} call log(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for log in results:
        lines.append(_format_call_log_search_result(log))
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_call_logs(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter call logs. Only fields marked [FILTERABLE] in get_call_log_field_instructions can be used.
    Call get_call_log_field_instructions first.

    filters: List of filter objects. Each must have:
      - field (str): Field API name (e.g. outcome, callType, owner, createdAt, startTime, phoneNumber).
      - operator (str): Operator for that field type (e.g. equal, contains, greater).
      - value: Value to compare. For outcome use internal name (e.g. "connected", "busy").
        For callType use "incoming" or "outgoing". For owner/createdBy use user ID (number).
      - timeZone (str, optional): For date/datetime filters.
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty."
        return await search_call_logs_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_call_logs")
        return f"✗ Unexpected error: {str(e)}"


@mcp.tool()
async def search_all_call_logs(
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Get all call logs (no filters). Use when user asks to see recent call logs or all call logs.

    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        payload = {"jsonRule": None}
        params = {"page": _call_logs_search_api_page(page), "size": min(size, 100)}
        if sort:
            params["sort"] = sort
        logger.info("Fetching all call logs (page %d)", page)
        async with get_client() as client:
            response = await client.post("/call-logs/search", params=params, json=payload)
            data = await handle_api_response(response, "Search all call logs")
        results = data.get("content", data.get("data", []))
        total = data.get("totalElements", data.get("total", len(results)))
        total_pages = data.get("totalPages", 1)
        if not results:
            return "No call logs found."
        lines = [f"Found {len(results)} call log(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
        for log in results:
            lines.append(_format_call_log_search_result(log))
        lines.append("-" * 60)
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_all_call_logs")
        return f"✗ Unexpected error: {str(e)}"


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
