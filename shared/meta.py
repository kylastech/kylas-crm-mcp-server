"""
"Meta" utilities: everything in shared/ that only ever gets imported
directly (by main.py or an entity module), never by shared/app.py or
shared/http_client.py — those two stay separate because the reverse isn't
true: this file needs `mcp` from app.py and get_client/handle_api_response
from http_client.py, so if either of those needed anything back from here
it would be a circular import. app.py additionally can't merge in here
because it holds the environment config every other shared/ module
(including http_client.py) depends on before this file is even importable.

Sections below (each independent of the others, grouped here for file
count, not for shared logic):
  - constants      operator mappings, email/phone types, country aliases
  - datetime_tools default timezone, date-filter UTC conversion, the
                   standalone datetime.parse_to_utc registry id
  - fields         field metadata formatting + search jsonRule construction
  - normalize      payload normalization used by every entity's create/update
  - labels         tenant entity-label mapping (fetch + the two registered
                   MCP surfaces: get_entity_labels tool, entity-labels resource)
  - pipeline       pipeline lookup/stages/details (lead + deal)
  - products       product lookup (products filter on leads/deals)
  - users          current-user profile + user lookup

pipeline/products/users mirror the registry's own bucket-less "_meta" group
(user.lookup, product.lookup, pipeline.lookup, pipeline.details) — the rest
aren't registry lookup ids themselves but are just as small and
single-purpose.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import phonenumbers
from dateutil import parser as dateutil_parser

from shared.app import mcp, logger, BASE_URL
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count

# ===========================================================================
# constants — cross-entity constant data: operator mappings for building
# search filters, and the fixed value sets Kylas accepts for email/phone
# entry types and non-standard country code aliases. Pure data — no
# functions, no logic.
# ===========================================================================

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

# Picklist fields that use internal name (string) in search; all others use
# Option ID (long). This is the default/shared set — _format_field and
# _rule_type_for_value below read this directly. deal, company, meeting,
# and call_log each have their own *_PICKLIST_FIELDS_USE_INTERNAL_NAME set
# instead and bypass this one for their own search-rule building; lead,
# contact, task, and the generic search_entity path use this one.
PICKLIST_FIELDS_USE_INTERNAL_NAME = {"requirementCurrency", "companyBusinessType", "country", "timezone", "companyIndustry","companyCountry"}

EMAIL_TYPES = ["OFFICE", "PERSONAL"]
PHONE_TYPES = ["MOBILE", "WORK", "HOME", "PERSONAL"]

# Non-standard country code aliases not recognized as ISO region codes by the
# phonenumbers library.
_COUNTRY_CODE_ALIASES: Dict[str, str] = {
    "UK": "GB",
    "USA": "US",
    "INDIA": "IN",
}

# ===========================================================================
# datetime_tools — date/datetime conversion helpers shared across every
# entity: the default timezone fallback, converting a *.search filter's
# date value(s) from the user's local timezone to the UTC ISO string Kylas
# expects, computing an "N days ago" threshold for idle-record searches,
# and the standalone datetime.parse_to_utc registry id (create/update
# field_values need a UTC ISO string; users think and speak in their own
# timezone).
# ===========================================================================

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


# ===========================================================================
# fields — field metadata formatting and search jsonRule construction,
# shared across every entity's field-instructions text and *.search
# endpoint. deal, company, meeting, and call_log each still special-case
# their own picklist-internal-name set inline before falling back to
# _rule_type_for_value — see PICKLIST_FIELDS_USE_INTERNAL_NAME's docstring
# above.
# ===========================================================================

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
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only a field listed as [FILTERABLE] in this endpoint's build_payload response (tenant_filterable_fields)."
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


# ===========================================================================
# normalize — payload normalization shared across every entity's
# create/update logic: country code resolution, enforcing exactly one
# primary email/phone entry, and building a Kylas create/update payload from
# dynamic field_values (custom field routing, email/phone shorthand,
# standard fields passthrough).
# ===========================================================================

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


# ===========================================================================
# labels — entity label mapping (tenant-specific display names).
#
# Deliberately NOT cached process-wide. This server runs stateless_http=True
# (see main.run()) — every request is its own fresh transport with no session
# continuity (mcp/server/streamable_http_manager.py sets mcp_session_id=None
# in stateless mode) — so there is no per-tenant or per-session slot to cache
# this in safely. A module-level dict here previously caused tenant A's
# labels to leak to tenant B (and, via a background refresh loop with no
# request context, to whatever KYLAS_API_KEY the env fell back to). Always
# fetch live, scoped to the resolved auth of the current request. The extra
# /entities/label round-trip is the accepted cost of correctness.
# ===========================================================================

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

    Not cached anywhere — this server runs stateless_http (see main.run()), so there's no
    session or tenant-scoped slot to cache this in without either leaking across
    tenants (the old bug) or reintroducing per-tenant credential storage for a
    background refresh. Call this fresh wherever the labels are needed.
    """
    try:
        async with get_client() as client:
            resp = await client.get(f"{BASE_URL}/entities/label")
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


@mcp.tool()
async def get_entity_labels() -> str:
    """
    Returns the mapping of this tenant's custom entity display names to standard CRM entity types.
    CALL THIS ONCE at the start of every session, before any other tool.
    This tenant uses custom names (e.g. "animals" instead of contacts, "cars" instead of deals).
    Without this, you will fail to recognize entity requests from users.
    After calling this, when the user mentions a custom name, map it to the standard type for all tool calls.
    """
    labels = await _fetch_entity_labels()
    if not labels:
        return "No custom entity labels configured for this tenant. Standard names apply: lead, contact, deal, task, company, meeting, call_log."
    lines = ["# Entity Label Mapping — Custom Names for This Tenant\n"]
    lines.append("When the user says one of the custom names below, use the corresponding STANDARD TYPE in all tool calls.\n")
    for entity_type in sorted(labels.keys()):
        label_data = labels[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(f'- User says "{display_name}" or "{display_plural}" → use standard type: "{std_type}"')
    lines.append('\n**Example:** User says "get animals" → map "animals" to "contact" → use "contact.search" (list_tool -> build_payload -> execute_request)')
    lines.append('**Example:** User says "show cars" → map "cars" to "deal" → use "deal.search" (list_tool -> build_payload -> execute_request)')
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
async def entity_labels_resource() -> str:
    """Serve current entity label mapping as a readable resource."""
    labels = await _fetch_entity_labels()
    if not labels:
        return "No custom entity labels. Standard names apply: lead, contact, deal, task, company, meeting, call_log."
    lines = ["# Entity Label Mapping — Tenant-Specific Custom Names", ""]
    lines.append("When the user says a custom name, use the STANDARD TYPE (right of →) in all tool calls.")
    lines.append("")
    for entity_type in sorted(labels.keys()):
        label_data = labels[entity_type]
        display_name = label_data.get("displayName", entity_type)
        display_plural = label_data.get("displayNamePlural", entity_type)
        std_type = entity_type.lower()
        lines.append(f'- "{display_name}" / "{display_plural}" → "{std_type}"')
    lines.extend([
        "",
        "Examples:",
        '  User says "get animals" → standard type is "contact" → use "contact.search" (list_tool -> build_payload -> execute_request)',
        '  User says "show cars"   → standard type is "deal"    → use "deal.search" (list_tool -> build_payload -> execute_request)',
    ])
    return "\n".join(lines)


# ===========================================================================
# pipeline — pipeline lookup/stages/details, shared across lead and deal
# (both have a pipeline + pipelineStage). _get_pipeline_details_raw is also
# reused by entities/deal.py's sequential-stage-flow handling (advancing a
# deal through stages one at a time when sequentialStageFlow=true).
# ===========================================================================

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
    lines.append("Ask the user to confirm which pipeline to use (list id and name). Do NOT resolve pipeline.details until the user has confirmed. After confirmation, resolve pipeline.details (list_tool -> build_payload -> execute_request; not a standalone tool) with that pipeline ID only, then search or update with pipeline + pipelineStage filters.")
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


async def _get_pipeline_details_raw(pipeline_id: int) -> Dict[str, Any]:
    """Fetch raw pipeline details dict from GET /pipelines/{id}."""
    async with get_client() as client:
        response = await client.get(f"/pipelines/{int(pipeline_id)}")
        return await handle_api_response(response, "Get pipeline details")


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
    lines.append("When updating a lead or deal to Closed Lost or Closed Unqualified, ask the user to pick one reason from the list above, then resolve lead.update or deal.update (list_tool -> build_payload -> execute_request; not a standalone tool) with pipelineStageReason set to that exact string.")
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


# ===========================================================================
# products — product lookup, resolves a product name to its ID for
# filtering leads or deals by product (search_entity's "products" field).
# ===========================================================================

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


# ===========================================================================
# users — current-user profile (timezone, permissions — needed before any
# date/datetime conversion) and user lookup (resolving a name to an ID for
# createdBy/updatedBy/ownerId/importedBy/convertedBy filters).
# ===========================================================================

async def _fetch_current_user() -> Dict[str, Any]:
    """Fetch current user from GET /users/me. Returns full user object (timezone, recordActions, name, etc.)."""
    async with get_client() as client:
        response = await client.get("/users/me")
        return await handle_api_response(response, "Fetch current user")


@mcp.tool()
async def get_current_user() -> str:
    """
    Get the current authenticated user's profile from Kylas (GET /users/me).
    Call this whenever a date or datetime-related query is involved, or whenever
    you need the current user's own ID (e.g. to set ownerId/createdBy to "me").
    Returns id, timezone (IANA, e.g. Asia/Calcutta), recordActions (call, email, sms, etc.), name, and other profile fields.
    - For filtering (search_leads, search_idle_leads): use the returned timezone as the timeZone in date/datetime filters; keep the user's date/datetime as-is (do not convert to UTC).
    - For create_lead: when the user provides a datetime in their own words (e.g. "11th Feb 2026 at 7:30 AM"), interpret it in this timezone, convert to UTC using parse_datetime_to_utc_iso, and send the UTC ISO string in field_values.
    - For ownerId/createdBy referring to the current user (e.g. "assign to me", "create a lead owned by me"): use the returned id directly — do NOT call user.lookup/lookup_users to resolve yourself by name.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching current user (users/me)")
        user = await _fetch_current_user()
        user_id = user.get("id")
        tz = user.get("timezone") or "UTC"
        name = user.get("name") or f"{user.get('firstName', '')} {user.get('lastName', '')}".strip() or "—"
        lines = [
            "=" * 50,
            "CURRENT USER (GET /users/me)",
            "=" * 50,
            f"ID: {user_id}",
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
            f"Use this ID ({user_id}) directly for ownerId/createdBy when the action refers to the current user — no need for a separate user.lookup call.",
            "=" * 50,
        ])
        return "\n".join(lines)
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_current_user")
        return f"Unexpected error: {str(e)}"


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
