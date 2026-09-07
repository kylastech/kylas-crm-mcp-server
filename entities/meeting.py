"""
entities/meeting.py — everything specific to the "meeting" bucket: field
instructions (its own _format_meeting_field, distinct from the generic
_format_field), invitee/organizer lookup, related-entity lookup (resolve
lead/contact/deal/company IDs before meeting search filters), meeting-specific
search rule builder, create/update/get, cancel/delete (no _logic split —
these inline their own GET+POST/DELETE), search (+ by-term).

MEETING_LARGE_PICKLIST_FIELDS mirrors entities/lead.py's fix for the same
_BUCKET_PICKLIST_RULES circular-import issue — see that file's docstring.
Meeting already had its own MEETING_PICKLIST_FIELDS_USE_INTERNAL_NAME before
this refactor (both here and in _build_meeting_search_json_rule, which
already used it directly, no fix needed there) — and get_meeting_field_
instructions_logic never took an internal_name_fields param to begin with
(_format_meeting_field always shows internal names for ENTITY_PICKLIST/
PICK_LIST/MULTI_PICKLIST, no per-field choice), so only "large" needed fixing.
"""

from typing import Any, Dict, List, Optional, Tuple

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
    _convert_date_value_to_utc,
    _epoch_to_iso_utc,
    _fetch_current_user,
    OPERATOR_MAPPING,
    OPERATOR_SYMBOL_MAP,
    _get_filterable_fields_map,
    _rule_type_for_value,
)

# Picklist fields that use internal name (string) in meeting search (status/medium
# are ENTITY_PICKLIST, not PICK_LIST — Kylas always wants the internal name for these).
MEETING_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"status", "medium"}
# This bucket's own "large" picklist set — main.py's _BUCKET_PICKLIST_RULES["meeting"]
# imports this same constant rather than redefining it (single source of truth).
MEETING_LARGE_PICKLIST_FIELDS = {"timezone"}


# ---------------------------------------------------------------------------
# Meeting field metadata helpers
# ---------------------------------------------------------------------------

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


def _format_meeting_field(
    field: Dict[str, Any],
    large_fields: Optional[set] = None,
    requested_picklists: Optional[set] = None,
) -> List[str]:
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
            if name.strip().lower() in (large_fields or set()) and name.strip().lower() not in (requested_picklists or set()):
                # Oversized picklist (e.g. timezone) — build_payload's
                # tenant_fields_reference is this SAME text (it calls this
                # function directly), so there is only one omission rule to
                # keep in sync, driven by the shared _BUCKET_PICKLIST_RULES table.
                lines.append(f"  └─ {len(values)} options omitted to keep this reference compact.")
                lines.append(f"     Call build_payload(id, fields=[\"{name}\"]) to get them. Do NOT guess an option id or name.")
            else:
                # For ENTITY_PICKLIST like status/medium, show internal names
                lines.append("  └─ Options (use internal name):")
                for val in values:
                    if not isinstance(val, dict):
                        continue
                    val_label = val.get("displayName") or val.get("name") or "Unknown"
                    val_name = val.get("name", "")
                    lines.append(f"     • {val_label} (name: '{val_name}')")
    return lines


async def get_meeting_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_meeting_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    large_fields = {n.lower() for n in MEETING_LARGE_PICKLIST_FIELDS}
    lines = [
        "=" * 60,
        "KYLAS CRM - MEETING FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_meeting_field(f, large_fields=large_fields, requested_picklists=requested_picklists))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_meeting_field(f, large_fields=large_fields, requested_picklists=requested_picklists))
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
                "Use a field listed as [FILTERABLE] in build_payload(\"meeting.search\")'s tenant_filterable_fields, "
                "or synthetic meeting fields: "
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
    lines.append(f"From: {_epoch_to_iso_utc(meeting.get('from', '—'))}")
    lines.append(f"To: {_epoch_to_iso_utc(meeting.get('to', '—'))}")
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
    lines.append(f"Created At: {_epoch_to_iso_utc(meeting.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(meeting.get('updatedAt', '—'))}")
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
    from_dt = _epoch_to_iso_utc(m.get("from", "—"))
    to_dt = _epoch_to_iso_utc(m.get("to", "—"))
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


# ---------------------------------------------------------------------------
# Search meetings by term (title only — meetings API does not support
# multi_field search)
# ---------------------------------------------------------------------------

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
