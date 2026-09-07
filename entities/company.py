"""
entities/company.py — everything specific to the "company" bucket: field
instructions, company-specific search rule builder, create/update/get,
display formatting, search (+ by-term, + idle). No dead undecorated search
wrapper here (confirmed by grep) — unlike lead/contact/deal/meeting.

COMPANY_LARGE_PICKLIST_FIELDS mirrors entities/lead.py's fix for the same
_BUCKET_PICKLIST_RULES circular-import issue — see that file's docstring.
Company already had its own COMPANY_PICKLIST_FIELDS_USE_INTERNAL_NAME before
this refactor (both here and in _build_company_search_json_rule, which
already used it directly, no fix needed there).
"""

from typing import Any, Dict, List, Optional, Tuple

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
    _threshold_iso_days_ago,
    _convert_date_value_to_utc,
    _epoch_to_iso_utc,
    _fetch_current_user,
    OPERATOR_MAPPING,
    OPERATOR_SYMBOL_MAP,
    _LARGE_COMPANY_PICKLISTS,
    _format_field,
    _get_filterable_fields_map,
    _rule_type_for_value,
    _normalize_field_values,
    _extract_primary_email,
    _extract_primary_phone,
    _multi_field_json_rule,
)

# Picklist fields that use internal name (string) in company search
COMPANY_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"country"}
# This bucket's own "large" picklist set — main.py's _BUCKET_PICKLIST_RULES["company"]
# imports this same constant rather than redefining it (single source of truth).
COMPANY_LARGE_PICKLIST_FIELDS = {"timezone"} | _LARGE_COMPANY_PICKLISTS


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
        return "Error: Neither 'updatedAt' nor 'latestActivityCreatedAt' is filterable for this tenant. Check build_payload(\"company.search\")'s tenant_filterable_fields."
    return await search_companies_logic(filters, page=page, size=size, sort=sort)


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


async def get_company_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_company_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    large_fields = {n.lower() for n in COMPANY_LARGE_PICKLIST_FIELDS}
    internal_name_fields = COMPANY_PICKLIST_FIELDS_USE_INTERNAL_NAME
    lines = [
        "=" * 60,
        "KYLAS CRM - COMPANY FIELDS CHEAT SHEET",
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
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only a field listed as [FILTERABLE] in build_payload(\"company.search\")'s tenant_filterable_fields."
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
    lines.append(f"Created At: {_epoch_to_iso_utc(company.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(company.get('updatedAt', '—'))}")
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


# ---------------------------------------------------------------------------
# Search companies by term (multi-field search)
# ---------------------------------------------------------------------------

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
