"""Shared helpers: dates/timezones, entity labels, field formatting, search rule building, payload normalization."""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import phonenumbers
from dateutil import parser as dateutil_parser

from app.config import BASE_URL, DEFAULT_TIMEZONE, logger
from app.client import KylasAPIError, get_client

# Entity label mapping (tenant-specific display names).
#
# Deliberately NOT cached process-wide. This server runs stateless_http=True
# (see run()) — every request is its own fresh transport with no session
# continuity (mcp/server/streamable_http_manager.py sets mcp_session_id=None
# in stateless mode) — so there is no per-tenant or per-session slot to cache
# this in safely. A module-level dict here previously caused tenant A's
# labels to leak to tenant B (and, via a background refresh loop with no
# request context, to whatever KYLAS_API_KEY the env fell back to). Always
# fetch live, scoped to the resolved auth of the current request. The extra
# /entities/label round-trip is the accepted cost of correctness.


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

    Parsing uses dateutil, the same parser datetime.parse_to_utc already uses,
    rather than the two hardcoded strptime formats this used to accept. Those
    two covered "2026-09-03T19:30:00.000" and "2026-09-03T19:30:00" and NOTHING
    else — a space instead of the "T", or omitted seconds, fell through to a
    bare `return v` and shipped the caller's local time to Kylas unconverted,
    silently and with no log line. The filter then searched a window offset by
    the timezone and quietly returned the wrong records:
        '2026-09-03 19:30:00'       -> passed through unconverted
        '3 September 2026 7:30 PM'  -> passed through unconverted
        '2026-09-03T19:30'          -> passed through unconverted
    All three convert correctly now.

    Timezone handling is unchanged for the normal case and stricter for one
    edge case: a naive value (including one with a trailing 'Z', which callers
    routinely add to a local time they were told to send as local) is still
    interpreted in `timezone_str`. But a value carrying a REAL utc offset, e.g.
    "2026-09-03T19:30:00+05:30", is now honoured as written instead of having
    its offset overwritten by timezone_str — previously such a value failed
    both strptime formats and was passed through untouched.
    """
    if value is None:
        return value

    try:
        tz = ZoneInfo(timezone_str)
    except Exception:
        # Never silently pretend the caller meant UTC — that returns a wrong
        # timestamp with no error anywhere. See the tzdata note in requirements.txt.
        logger.error(
            "Filter timezone %r could not be resolved — falling back to UTC. Date filter "
            "values will NOT be shifted and the search window will be wrong by that "
            "zone's offset. Ensure the 'tzdata' package is installed in the runtime.",
            timezone_str,
        )
        tz = ZoneInfo("UTC")
    utc = ZoneInfo("UTC")

    def _convert_single(v: Any) -> Any:
        if not isinstance(v, str) or not v.strip():
            return v
        # Strip trailing Z so a local time sent with a "Z" is still read as local.
        clean = v.strip().rstrip("Zz")
        try:
            dt = dateutil_parser.parse(clean)
        except (ValueError, OverflowError, TypeError):
            logger.warning(
                "Could not parse date filter value %r; sending it to Kylas unconverted. "
                "The search window may be wrong by the timezone offset.", v,
            )
            return v
        # An explicit offset in the value wins; a naive value means local time.
        local_dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=tz)
        utc_dt = local_dt.astimezone(utc)
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc_dt.microsecond // 1000:03d}Z"

    if isinstance(value, list):
        return [_convert_single(v) for v in value]
    return _convert_single(value)


def _epoch_to_iso_utc(value: Any) -> Any:
    """
    Convert a Kylas epoch timestamp to a UTC ISO string. Anything else is
    returned EXACTLY as received.

    Kylas is not consistent about this: the same field on the same record comes
    back as epoch milliseconds from one endpoint and as an ISO string from
    another (verified on task 11613888 — POST /tasks/search returned
    1789070400000, GET /tasks/{id} returned "2026-09-10T20:00:00.000+0000" for
    the same dueDate, same instant). A bare integer reaching the LLM is not a
    timestamp it can read, so it guesses — that is how a task due 7:00 PM was
    reported as 6:40 PM. This converts the epoch case only; ISO strings, None,
    and everything else pass through untouched, by design.

    Epoch is detected by TYPE first, then magnitude — never by field name:
      - milliseconds: 1e11 .. 4.1e12   (~1973 .. ~2100)
      - seconds:      1e9  .. 4.1e9    (~2001 .. ~2100)
    The two ranges cannot overlap: epoch seconds do not reach 1e11 until the
    year 5138, so a seconds value can never be misread as milliseconds.
    Anything outside both ranges is left alone rather than guessed at.
    """
    # bool is a subclass of int — exclude it before any numeric check.
    if isinstance(value, bool) or value is None:
        return value

    number = value
    if isinstance(value, str):
        stripped = value.strip()
        # Only a plain integer literal counts; "2026-09-10T..." must not match.
        if not (stripped.lstrip("-").isdigit() and stripped.lstrip("-")):
            return value
        try:
            number = int(stripped)
        except ValueError:
            return value
    elif not isinstance(value, (int, float)):
        return value

    magnitude = abs(number)
    if 1e11 <= magnitude <= 4.1e12:
        seconds = number / 1000.0
    elif 1e9 <= magnitude <= 4.1e9:
        seconds = float(number)
    else:
        # A number, but not in any plausible epoch range — leave it exactly as
        # it came rather than inventing a date from it.
        return value

    try:
        dt = datetime.fromtimestamp(seconds, tz=ZoneInfo("UTC"))
    except (OverflowError, OSError, ValueError):
        logger.warning("Could not convert epoch %r to a UTC datetime; passing through unchanged.", value)
        return value
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


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


async def _fetch_entity_labels() -> Dict[str, Dict[str, str]]:
    """
    Fetch entity labels from /v1/entities/label endpoint, live, for the caller's
    own resolved auth (get_client() reads x-api-key/OAuth off the current request).
    Returns mapping like: {"LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"}, ...}
    Returns empty dict if fetch fails — callers fall back to standard names.

    Not cached anywhere — this server runs stateless_http (see run()), so there's no
    session or tenant-scoped slot to cache this in without either leaking across
    tenants (the old bug) or reintroducing per-tenant credential storage for a
    background refresh. Call this fresh wherever the labels are needed.
    """
    try:
        async with get_client() as client:
            resp = await client.get("/v1/entities/label")
            labels = resp.json()
            summary = "\n".join(
                f"  • {etype:8} => {data.get('displayName', etype)} / {data.get('displayNamePlural', etype)}"
                for etype, data in sorted(labels.items())
            )
            logger.debug(f"\n📦 Fetched entity labels:\n{summary}")
            return labels
    except Exception as e:
        logger.warning(f"Failed to fetch entity labels: {e}")
        return {}

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
    # Verbose forms — common LLM/API convention, normalize to canonical names
    "greater_than": "greater",
    "less_than": "less",
    "greater_than_or_equal": "greater_or_equal",
    "less_than_or_equal": "less_or_equal",
    "greater_than_or_equal_to": "greater_or_equal",
    "less_than_or_equal_to": "less_or_equal",
    "equals": "equal",
    "not_equals": "not_equal",
}

# Picklist fields that use internal name (string) in search; all others use Option ID (long)
PICKLIST_FIELDS_USE_INTERNAL_NAME = {"requirementCurrency", "companyBusinessType", "country", "timezone", "companyIndustry","companyCountry"}

def _format_field(
    field: Dict[str, Any],
    include_filterable: bool = False,
    large_fields: Optional[set] = None,
    requested_picklists: Optional[set] = None,
    internal_name_fields: Optional[set] = None,
) -> List[str]:
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
            if name.strip().lower() in (large_fields or set()) and name.strip().lower() not in (requested_picklists or set()):
                lines.append(f"  └─ {len(values)} options omitted to keep this reference compact.")
                lines.append(f"     Call build_payload(id, fields=[\"{name}\"]) to get them. Do NOT guess an option id or name.")
            else:
                use_name = name in (internal_name_fields or set())
                lines.append("  └─ Options (use internal name in search)" if use_name else "  └─ Options (use ID in search):")
                for val in values:
                    if not isinstance(val, dict):
                        continue
                    val_label = val.get("displayName") or val.get("label") or val.get("name") or "Unknown"
                    val_id = val.get("id", "")
                    val_name = val.get("name", "")
                    lines.append(f"     • {val_label} (internal name: '{val_name}'),(ID: {val_id})")
    return lines


def _get_filterable_fields_map(fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Return map of field name -> {type, standard} for active+filterable fields only."""
    return {
        (f.get("name") or str(f.get("id", ""))): {"type": f.get("type", "TEXT_FIELD"), "standard": f.get("standard", False)}
        for f in fields
        if f.get("active", True) and f.get("filterable", False) and (f.get("name") or f.get("id") is not None)
    }


def _rule_type_for_value(
    field_type: str,
    field_name: str,
    value: Any,
    internal_name_fields: Optional[set] = None,
) -> str:
    """Return jsonRule rule 'type' (string, long, or date) for the given field type and value.

    internal_name_fields: this bucket's set of picklist fields that take the
    option's internal name (string) instead of its numeric id — from
    _BUCKET_PICKLIST_RULES[bucket]["internal_name"]. Defaults to the lead/
    contact set for backward compatibility with callers that don't pass it.
    """
    if field_type in ("PICK_LIST", "MULTI_PICKLIST"):
        allowed_internal_names = (
            internal_name_fields if internal_name_fields is not None else PICKLIST_FIELDS_USE_INTERNAL_NAME
        )
        return "string" if field_name in allowed_internal_names else "long"
    if field_type == "NUMBER":
        return "double"
    # MONEY (deal estimatedValue/actualValue, company annualRevenue, quotation
    # subTotal/grandTotal) is an ordered numeric field: OPERATOR_MAPPING gives it
    # greater/less/between, but without this branch it fell through to "string"
    # below and the API rejected every one of those with 003014 "Invalid string
    # operation". "double", not "long" — money carries decimals.
    if field_type == "MONEY":
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
    internal_name_fields: Optional[set] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /search/lead. Returns (jsonRule, error_message).
    Each filter: { "field": "<name>", "operator": "<op>", "value": <val>, "type": "<FIELD_TYPE>" }.
    default_timezone: used for date/datetime rules when filter has no timeZone (e.g. from get_current_user).
    internal_name_fields: this bucket's _BUCKET_PICKLIST_RULES[...]["internal_name"]
    set, forwarded to _rule_type_for_value for PICK_LIST/MULTI_PICKLIST fields.
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
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only a field listed as [FILTERABLE] in this endpoint's build_payload response (tenant_filterable_fields)."
        meta = filterable_map[field_name]
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        rule_type = _rule_type_for_value(api_type, field_name, value, internal_name_fields)
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

