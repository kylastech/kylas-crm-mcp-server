"""
entities/lead.py — everything specific to the "lead" bucket: field
instructions, create/update/get, display formatting, search (+ by-term,
+ idle).

LEAD_LARGE_PICKLIST_FIELDS / this module reading PICKLIST_FIELDS_USE_INTERNAL_NAME
directly from shared.meta (rather than through main.py's _BUCKET_PICKLIST_RULES
dict) is deliberate: main.py's dispatch dicts need to import FROM this module,
so this module reaching back into main.py for its own picklist rules would
cycle. main.py's _BUCKET_PICKLIST_RULES["lead"] entry is built from this same
LEAD_LARGE_PICKLIST_FIELDS constant instead, one-way (main -> entities.lead).
"""

from typing import Any, Dict, List, Optional

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
    _threshold_iso_days_ago,
    _epoch_to_iso_utc,
    _fetch_current_user,
    PICKLIST_FIELDS_USE_INTERNAL_NAME,
    _LARGE_COMPANY_PICKLISTS,
    _format_field,
    _get_filterable_fields_map,
    _build_search_json_rule,
    _normalize_field_values,
    _extract_primary_email,
    _extract_primary_phone,
    _multi_field_json_rule,
    get_pipeline_details_logic,
)

# This bucket's own "large" picklist set — main.py's _BUCKET_PICKLIST_RULES["lead"]
# imports this same constant rather than redefining it (single source of truth).
LEAD_LARGE_PICKLIST_FIELDS = {"timezone", "requirementcurrency"} | _LARGE_COMPANY_PICKLISTS


# ---------------------------------------------------------------------------
# Tool 1: Get Lead Field Instructions (call FIRST)
# ---------------------------------------------------------------------------

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


async def get_lead_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_lead_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    large_fields = {n.lower() for n in LEAD_LARGE_PICKLIST_FIELDS}
    internal_name_fields = PICKLIST_FIELDS_USE_INTERNAL_NAME
    lines = [
        "=" * 60,
        "KYLAS CRM - LEAD FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists, internal_name_fields=internal_name_fields))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists, internal_name_fields=internal_name_fields))
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
# Tool 4: Create Lead (single tool, dynamic field_values)
# ---------------------------------------------------------------------------

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
    lines.append(f"Created At: {_epoch_to_iso_utc(lead.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(lead.get('updatedAt', '—'))}")
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
    json_rule, err = _build_search_json_rule(
        filters, filterable_map, default_timezone=default_tz,
        internal_name_fields=PICKLIST_FIELDS_USE_INTERNAL_NAME,
    )
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
        return "Error: Neither 'updatedAt' nor 'latestActivityCreatedAt' is filterable for this tenant. Check build_payload(\"lead.search\")'s tenant_filterable_fields."
    return await search_leads_logic(filters, page=page, size=size, sort=sort)
