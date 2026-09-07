"""
entities/call_log.py — everything specific to the "call_log" bucket: field
instructions (reuses entities.meeting._format_meeting_field directly — both
buckets share the same ENTITY_PICKLIST-style field shape), create/update,
display formatting, get_call_logs (the call_log.by_entity registry id's real
implementation — pulls in the related lead/contact/deal's own details),
call-log-specific search rule builder, search.

CALL_LOG_LARGE_PICKLIST_FIELDS mirrors entities/lead.py's fix for the same
_BUCKET_PICKLIST_RULES circular-import issue — see that file's docstring.
Call_log already had its own CALL_LOG_PICKLIST_FIELDS_USE_INTERNAL_NAME
before this refactor (both here and in _build_call_log_search_json_rule,
which already used it directly, no fix needed there) — and, like meeting,
get_call_log_field_instructions_logic never took an internal_name_fields
param, so only "large" needed fixing.

get_call_logs pulls in three other entities' own get_X_logic/_format_X_for_
display (lead, contact, deal) — the widest cross-entity import of any single
module so far, still no cycle since none of lead/contact/deal import
call_log back.
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
from entities.meeting import _format_meeting_field
from entities.lead import get_lead_logic, _format_lead_for_display
from entities.contact import get_contact_logic, _format_contact_for_display
from entities.deal import get_deal_logic, _format_deal_for_display

# Picklist fields that use internal name (string) in call log search
CALL_LOG_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"outcome", "callType", "overallSentiment", "callDisposition", "customerEmotion"}
# This bucket has no oversized picklists (matches main.py's _BUCKET_PICKLIST_RULES["call_log"]).
CALL_LOG_LARGE_PICKLIST_FIELDS: set = set()


# ---------------------------------------------------------------------------
# Call Log field metadata helpers
# ---------------------------------------------------------------------------

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


async def get_call_log_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_call_log_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    large_fields = {n.lower() for n in CALL_LOG_LARGE_PICKLIST_FIELDS}
    lines = [
        "=" * 60,
        "KYLAS CRM - CALL LOG FIELDS CHEAT SHEET",
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
    lines.append(f"Start Time: {_epoch_to_iso_utc(log.get('startTime', '—'))}")
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
    lines.append(f"Created At: {_epoch_to_iso_utc(log.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(log.get('updatedAt', '—'))}")
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
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only a field listed as [FILTERABLE] in build_payload(\"call_log.search\")'s tenant_filterable_fields."
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
    start = _epoch_to_iso_utc(log.get("startTime", "—"))
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
    lines.append("💡 HINT: For call logs showing 'Related: contact#123' or 'lead#456', resolve:")
    lines.append("  • call_log.by_entity (list_tool -> build_payload -> execute_request; not a standalone tool) with {entity_id: 123, entity_type: 'contact'} to see full contact details with their call logs")
    return "\n".join(lines)
