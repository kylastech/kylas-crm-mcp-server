"""
Kylas CRM MCP Server - Lead, Contact & Task Support

Model Context Protocol server for Kylas CRM operations:

LEAD OPERATIONS:
- get_lead_field_instructions (call FIRST to get schema)
- create_lead, update_lead, get_lead
- search_leads (filter by criteria)
- search_leads_by_term (multi-field search)
- search_idle_leads (no activity for N days)
- lookup_pipelines, get_pipeline_stages, get_pipeline_details

CONTACT OPERATIONS:
- get_contact_field_instructions (call FIRST to get schema)
- create_contact, update_contact, get_contact
- search_contacts (filter by criteria - NO PIPELINE)
- search_contacts_by_term (multi-field search)

TASK OPERATIONS:
- get_task_field_instructions (call FIRST to get schema)
- create_task, update_task, get_task
- search_tasks (filter by criteria - NO PIPELINE)
- search_tasks_by_term (multi-field search)
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
    logger.warning("KYLAS_API_KEY environment variable not set. API calls will fail.")

# ---------------------------------------------------------------------------
# System Instructions: ALWAYS get fields first, then create from user context
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """
# Kylas CRM MCP Server - Lead, Contact & Task Support

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
- **By term across multiple fields:** When the user asks for "leads with X", "leads containing Y", or "leads named Z" without specifying a field (e.g. "leads with akshay"), use **search_leads_by_term** with that term. The API will search across firstName, lastName, companyName, phoneNumbers, emails, etc.
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
- **Search Contacts:** Use `search_contacts` with filters or `search_contacts_by_term` for multi-field search. **IMPORTANT:** Contacts do NOT support pipeline or pipelineStage filters.
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
- **Search Tasks:** Use `search_tasks` with filters or `search_tasks_by_term` for multi-field search. **IMPORTANT:** Tasks do NOT support pipeline or pipelineStage filters.
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


class _ThrottledClientContext:
    """Async context manager: wraps httpx.AsyncClient and yields _ThrottledClient for 100–500 ms delay between calls."""

    def __init__(self) -> None:
        if not API_KEY:
            raise KylasAPIError("KYLAS_API_KEY environment variable is not set")
        client_name = _get_mcp_client_name()
        user_agent = f"kylas_mcp_server/{client_name}"
        self._raw = httpx.AsyncClient(
            base_url=BASE_URL,
            headers={
                "api-key": API_KEY,
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
- **By term across multiple fields:** When the user asks for "deals with X", "deals containing Y", or "deals named Z" without specifying a field, use **search_deals_by_term** with that term.
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
- Use the **search_idle_deals** tool when the user asks for idle/stagnant/inactive deals.
"""

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP("Kylas CRM (Lead + Deal)", instructions=SYSTEM_INSTRUCTIONS + "\n\n" + DEAL_SYSTEM_INSTRUCTIONS)


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
        return "long"
    # User look-up fields: createdBy, updatedBy, convertedBy, ownerId, importedBy — value is user ID (long)
    if field_type in ("LOOK_UP", "ENTITY_FIELDS"):
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
        if rule_type == "long" and value is not None and not isinstance(value, (int, float)):
            try:
                value = int(value)
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


async def search_leads_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search leads by a single term across multiple fields (firstName, lastName, companyName, phoneNumbers, emails, etc.) via POST /search/lead with multi_field jsonRule."""
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


@mcp.tool()
async def search_leads_by_term(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search leads by a single term across multiple fields (firstName, lastName, companyName, phoneNumbers, emails, etc.).
    Use this when the user asks for "leads with X", "leads containing Y", or "leads named Z" without specifying which field to filter on.
    For filtering by a specific field (e.g. "leads where phone number is X"), use search_leads instead.

    search_term: The term to search for (e.g. "akshay", "acme").
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "updatedAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        return await search_leads_by_term_logic(search_term, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_leads_by_term")
        return f"✗ Unexpected error: {str(e)}"


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


async def search_contacts_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search contacts by a single term across multiple fields."""
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


@mcp.tool()
async def search_contacts_by_term(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search contacts by a single term across multiple fields (firstName, lastName, emails, phones, department, designation, etc.).
    Use this when the user asks for "contacts with X", "contacts containing Y", or "contacts named Z" without specifying a field.
    """
    try:
        _reset_api_call_count()
        return await search_contacts_by_term_logic(search_term, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_contacts_by_term")
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


async def search_tasks_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search tasks by a single term across multiple fields."""
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
        lines.append(f"• ID: {tid} | Name: {name} | Status: {status} | Priority: {priority}")
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_tasks_by_term(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search tasks by a single term across multiple fields (title, description, status, etc.).
    Use this when the user asks for "tasks with X", "tasks containing Y", or "tasks titled Z" without specifying a field.
    """
    try:
        _reset_api_call_count()
        return await search_tasks_by_term_logic(search_term, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_tasks_by_term")
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
        if rule_type == "long" and value is not None and not isinstance(value, (int, float)):
            try:
                value = int(value)
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
        "fields": ["id", "name", "value", "emails", "phoneNumbers", "ownerId", "createdAt"],
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
        email = _extract_primary_email(deal.get("emails"))
        phone = _extract_primary_phone(deal.get("phoneNumbers"))
        lines.append(f"• ID: {did} | Name: {name} | Value: {value} | Email: {email} | Phone: {phone}")
    lines.append("-" * 60)
    return "\n".join(lines)


@mcp.tool()
async def search_deals_by_term(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search deals by a single term across multiple fields (name, value, etc.).
    Use this when the user asks for "deals with X", "deals containing Y", or "deals named Z" without specifying which field to filter on.
    For filtering by a specific field, use search_deals instead.

    search_term: The term to search for (e.g. "acme", "contract").
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "updatedAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        return await search_deals_by_term_logic(search_term, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_deals_by_term")
        return f"✗ Unexpected error: {str(e)}"


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


@mcp.tool()
async def search_idle_deals(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search for idle/stagnant deals: no activity for at least the given number of days.
    Uses both updatedAt and latestActivityCreatedAt.

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
        return await search_idle_deals_logic(days, time_zone, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search idle deals failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_idle_deals")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# NOTES: Add notes to Lead, Contact, Deal, Company
# ---------------------------------------------------------------------------

@mcp.tool()
async def add_note(entity_type: str, entity_id: int, note_text: str) -> str:
    """
    Add a note to a Lead, Contact, Deal, or Company.

    entity_type: The entity type ("LEAD", "CONTACT", "DEAL", or "COMPANY").
    entity_id: The ID of the entity (e.g. lead ID, contact ID, deal ID, company ID).
    note_text: The note text to add (supports basic HTML formatting).

    Returns the note details if successful.
    """
    try:
        _reset_api_call_count()
        entity_type_upper = entity_type.upper().strip()
        if entity_type_upper not in ["LEAD", "CONTACT", "DEAL", "COMPANY"]:
            return f"✗ Invalid entity type: '{entity_type}'. Must be one of: LEAD, CONTACT, DEAL, COMPANY"

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
    logger.info("Starting Kylas CRM MCP Server (Lead + Deal)...")
    logger.info("Starting Kylas CRM MCP Server (Lead + Contact + Task support)...")
    mcp.run()


if __name__ == "__main__":
    run()
