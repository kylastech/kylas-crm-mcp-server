"""
entities/task.py — everything specific to the "task" bucket: association
lookups (lead/contact/deal/company for a task's "relation" field), field
instructions, create/update/get, display formatting, search (+ by-term,
+ any-relation), and the task.lookup_entity / task.search_any_relation
registry ids' real implementations.

TASK_LARGE_PICKLIST_FIELDS mirrors entities/lead.py's fix for the same
_BUCKET_PICKLIST_RULES circular-import issue — see that file's docstring.
Unlike lead/contact, task already had its own internal_name constant
(TASK_PICKLIST_FIELDS_USE_INTERNAL_NAME) before this refactor.
"""

import asyncio
from typing import Any, Dict, List, Optional

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
    _epoch_to_iso_utc,
    _fetch_current_user,
    _fetch_entity_labels,
    _format_field,
    _get_filterable_fields_map,
    _build_search_json_rule,
    _normalize_field_values,
    _multi_field_json_rule,
)

# Picklist fields that use internal name (string) in task search.
TASK_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"reminder"}
# This bucket has no oversized picklists (matches main.py's _BUCKET_PICKLIST_RULES["task"]).
TASK_LARGE_PICKLIST_FIELDS: set = set()


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
    lines.append(f"Due Date: {_epoch_to_iso_utc(task.get('dueDate')) or '—'}")
    lines.append(f"Assigned To: {task.get('assignedTo') or '—'}")
    lines.append(f"Reminder: {task.get('reminder') or '—'}")
    lines.append(f"Created At: {_epoch_to_iso_utc(task.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(task.get('updatedAt', '—'))}")
    # Custom fields
    custom = task.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


async def get_task_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_task_fields()
    large_fields = {n.lower() for n in TASK_LARGE_PICKLIST_FIELDS}
    internal_name_fields = TASK_PICKLIST_FIELDS_USE_INTERNAL_NAME
    lines = ["# Task Field Reference", ""]
    for field in fields:
        lines.extend(_format_field(field, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists, internal_name_fields=internal_name_fields))
    return "\n".join(lines)


@mcp.tool()
async def get_task_field_instructions() -> str:
    """
    Get task field reference (API names, Field IDs, picklist options).
    ALWAYS call this FIRST before creating or updating a task.
    """
    try:
        _reset_api_call_count()
        return await get_task_field_instructions_logic()
    except KylasAPIError as e:
        return f"✗ Failed to fetch fields: {e.message}"
    except Exception as e:
        logger.exception("get_task_field_instructions")
        return f"✗ Unexpected error: {str(e)}"


async def get_task_logic(task_id: int) -> Dict[str, Any]:
    """Fetch a single task by ID (GET /tasks/{id}). Returns full task object.

    Extracted from the real, registered get_task tool below (which used to
    inline this GET directly, unlike get_lead/get_contact/get_meeting/
    get_deal/get_company, which already had their own _logic helper) so the
    generic get_entity_logic dispatch in main.py has a real function to call
    for entity_type "task" — same reasoning as every other *_logic split in
    this file: exactly one real implementation, not two.
    """
    task_id = int(task_id)
    async with get_client() as client:
        response = await client.get(f"/tasks/{task_id}")
        return await handle_api_response(response, "Get task")


@mcp.tool()
async def get_task(task_id: int) -> str:
    """Get full details of a task by ID."""
    try:
        _reset_api_call_count()
        task = await get_task_logic(task_id)
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
    json_rule, err = _build_search_json_rule(
        filters, filterable_map, default_timezone=default_tz,
        internal_name_fields=TASK_PICKLIST_FIELDS_USE_INTERNAL_NAME,
    )
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
        due_date = _epoch_to_iso_utc(task.get("dueDate", "—"))
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

        labels = await _fetch_entity_labels()
        lead_label = labels.get("lead", {}).get("displayName", "Lead")
        contact_label = labels.get("contact", {}).get("displayName", "Contact")
        deal_label = labels.get("deal", {}).get("displayName", "Deal")
        company_label = labels.get("company", {}).get("displayName", "Company")

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
        internal_name_fields=TASK_PICKLIST_FIELDS_USE_INTERNAL_NAME,
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

    labels = await _fetch_entity_labels()
    lead_label = labels.get("lead", {}).get("displayName", "Lead")
    contact_label = labels.get("contact", {}).get("displayName", "Contact")
    deal_label = labels.get("deal", {}).get("displayName", "Deal")
    company_label = labels.get("company", {}).get("displayName", "Company")
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
        due = _epoch_to_iso_utc(task.get("dueDate", "—"))
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


# ---------------------------------------------------------------------------
# Search tasks by term (multi-field search)
# ---------------------------------------------------------------------------

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
        due_date = _epoch_to_iso_utc(task.get("dueDate", "—"))
        lines.append(f"• ID: {tid} | Name: {name} | Status: {status} | Priority: {priority} | Due: {due_date}")
    lines.append("-" * 60)
    return "\n".join(lines)
