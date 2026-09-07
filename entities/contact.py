"""
entities/contact.py — everything specific to the "contact" bucket: field
instructions, create/update/get, display formatting, search (+ by-term).
No search_idle for contact (matches the registry's documented shape).

CONTACT_LARGE_PICKLIST_FIELDS / reading PICKLIST_FIELDS_USE_INTERNAL_NAME
directly from shared.meta (rather than through main.py's _BUCKET_PICKLIST_RULES
dict) is the same fix as entities/lead.py — see that file's docstring for why.
"""

from typing import Any, Dict, List, Optional

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
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
)

# This bucket's own "large" picklist set — main.py's _BUCKET_PICKLIST_RULES["contact"]
# imports this same constant rather than redefining it (single source of truth).
CONTACT_LARGE_PICKLIST_FIELDS = {"timezone", "requirementcurrency"} | _LARGE_COMPANY_PICKLISTS


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
    lines.append(f"Created At: {_epoch_to_iso_utc(contact.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(contact.get('updatedAt', '—'))}")
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


async def get_contact_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_contact_fields()
    large_fields = {n.lower() for n in CONTACT_LARGE_PICKLIST_FIELDS}
    internal_name_fields = PICKLIST_FIELDS_USE_INTERNAL_NAME
    lines = ["# Contact Field Reference", ""]
    for field in fields:
        lines.extend(_format_field(field, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists, internal_name_fields=internal_name_fields))
    return "\n".join(lines)


@mcp.tool()
async def get_contact_field_instructions() -> str:
    """
    Get contact field reference (API names, Field IDs, picklist options).
    ALWAYS call this FIRST before creating or updating a contact.
    """
    try:
        _reset_api_call_count()
        return await get_contact_field_instructions_logic()
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
    json_rule, err = _build_search_json_rule(
        filters, filterable_map, default_timezone=default_tz,
        internal_name_fields=PICKLIST_FIELDS_USE_INTERNAL_NAME,
    )
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
# Search contacts by term (multi-field search)
# ---------------------------------------------------------------------------

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
