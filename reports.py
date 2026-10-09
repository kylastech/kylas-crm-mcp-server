"""Report creation (report.create) — phase 1: ONE_DIMENSIONAL reports only.

Mirrors the sd-ui report builder (src/components/page/Reports/) so a report
created through MCP is byte-for-byte the shape the web app would save:

  - Field discovery: GET /v1/reports/config/{entityPlural} (sd-report builds
    it from the entity's list layout). Picklist options live ONLY under
    filters[].picklist.picklistValues; dimensions never carry options. Large
    picklists (timezone, country, ...) are trimmed exactly like the
    layout-based cheat sheets, with the same build_payload(fields=[...]) opt-in.
  - The model sends a SHORT payload (entity, name, chart_type, group_by,
    metrics, date_range, filters). build_report_body() expands it into the
    full Kylas body the same way sd-ui's getConfig/getDateRangeFilter/
    getAllFilters do (service.ts), after validating every choice against the
    live config — sd-report itself barely validates at save time (unknown
    fields/operators only fail later, when the report is run).
  - Save: POST /v3/reports.

Lookup-backed filters with no registry resolver yet (teams/ENTITY_FIELDS,
campaign, activities, pipeline stages, meeting invitees/organizer, record
lookups, ...) are reported as "not supported yet" and rejected; they arrive
as separate lookup ids in phase 2.
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as dateutil_parser

from app.config import BASE_URL, DEFAULT_TIMEZONE, logger
from app.client import KylasAPIError, _reset_api_call_count, get_client, handle_api_response
from app.helpers import _convert_date_value_to_utc
from app.entities import _LARGE_COMPANY_PICKLISTS, _fetch_current_user

# ---------------------------------------------------------------------------
# Entities and endpoints
# ---------------------------------------------------------------------------

# Order matches sd-ui's entity dropdown (Form.tsx getEntityOptions).
REPORT_ENTITIES: Tuple[str, ...] = (
    "lead", "deal", "contact", "company", "task", "meeting", "call", "email", "quotation",
)
_REPORT_ENTITY_PLURAL: Dict[str, str] = {
    "lead": "leads", "deal": "deals", "contact": "contacts", "company": "companies",
    "task": "tasks", "meeting": "meetings", "call": "calls", "email": "emails",
    "quotation": "quotations",
}
# sd-ui offers meeting regardless of the meeting READ permission (Form.tsx:1043).
_ALWAYS_REPORTABLE_ENTITIES = {"meeting"}

# Config lives on /v1 (BASE_URL); save/run live on /v3 — same split as sd-ui.
REPORTS_V3_BASE = re.sub(r"/v1/?$", "/v3", BASE_URL.rstrip("/"))

# ---------------------------------------------------------------------------
# Large picklists — same named list as the layout cheat sheets, plus deal/
# quotation "currency". Matched case-insensitively against filter ids.
# ---------------------------------------------------------------------------

_REPORT_LARGE_PICKLISTS = {"timezone", "requirementcurrency", "currency"} | _LARGE_COMPANY_PICKLISTS

# ---------------------------------------------------------------------------
# UI constants (sd-ui: Reports/constants.ts, utils/constants.ts, dataUtils.ts)
# ---------------------------------------------------------------------------

_CHART_TYPES = ("bar", "pie", "table")
_DATE_FIELD_TYPES = {"DATE_PICKER", "DATETIME_PICKER"}

_DIMENSION_FORMATS = ("YEARLY", "QUARTERLY", "FINANCIAL_YEARLY", "FINANCIAL_QUARTERLY", "MONTHLY", "WEEKLY", "DAILY")
_FINANCIAL_FORMATS = {"FINANCIAL_YEARLY", "FINANCIAL_QUARTERLY"}

# DATE_RANGES, in UI order: (operator, is_future_date)
_DATE_RANGES: Tuple[Tuple[str, bool], ...] = (
    ("today", False), ("yesterday", False), ("tomorrow", True),
    ("last_n_days", False), ("next_n_days", True),
    ("last_seven_days", False), ("next_seven_days", True),
    ("last_fifteen_days", False), ("next_fifteen_days", True),
    ("last_thirty_days", False), ("next_thirty_days", True),
    ("week_to_date", False), ("current_week", False), ("last_week", False), ("next_week", True),
    ("month_to_date", False), ("current_month", False), ("last_month", False), ("next_month", True),
    ("quarter_to_date", False), ("current_quarter", False), ("last_quarter", False), ("next_quarter", True),
    ("current_financial_quarter", False), ("last_financial_quarter", False), ("next_financial_quarter", True),
    ("year_to_date", False), ("current_year", False), ("last_year", False), ("next_year", True),
    ("current_financial_year", False), ("last_financial_year", False), ("next_financial_year", True),
    ("before_current_date_and_time", False), ("after_current_date_and_time", False),
    ("between", False),
)
# dateRangeWith90DaysSupport — email uses it (past ranges only) for every date dropdown.
_EMAIL_DATE_RANGES: Tuple[str, ...] = (
    "today", "yesterday", "last_seven_days", "last_fifteen_days", "last_thirty_days",
    "week_to_date", "current_week", "last_week", "month_to_date", "current_month", "last_month",
    "quarter_to_date", "current_quarter", "last_quarter",
    "current_financial_quarter", "last_financial_quarter", "between",
)
_RELATIVE_FOR_REPORT = {
    "last_n_days", "next_n_days", "last_seven_days", "next_seven_days", "last_fifteen_days",
    "next_fifteen_days", "last_thirty_days", "next_thirty_days", "week_to_date", "month_to_date",
    "quarter_to_date", "year_to_date", "last_financial_quarter", "current_financial_quarter",
    "next_financial_quarter", "next_financial_year", "current_financial_year", "last_financial_year",
}
_FINANCIAL_OPERATORS = {
    "last_financial_quarter", "current_financial_quarter", "next_financial_quarter",
    "last_financial_year", "current_financial_year", "next_financial_year",
}
# entitiesWithRelativeDateTimeOperators (singular form)
_ENTITIES_WITH_RELATIVE_DATES = {"lead", "deal", "contact", "company", "email", "task"}

_N_DAYS_OPERATORS = {"last_n_days", "next_n_days"}
# Date operators that carry no from/to time-of-day (DateFilter.tsx / FilterItem.tsx).
_NO_TIME_DATE_OPERATORS = {"between", "before_current_date_and_time", "after_current_date_and_time"}
_NO_INPUT_OPERATORS = {"is_null", "is_not_null", "is_empty", "is_not_empty"}
_MULTI_VALUE_OPERATORS = {"in", "not_in"}
_RANGE_OPERATORS = {"between", "not_between"}

_MAX_CUSTOM_RANGE_DAYS = 1825  # sd-report report.date-range.max-custom-range-in-days
_EMAIL_MAX_CUSTOM_RANGE_DAYS = 90  # sd-ui EMAIL_MAX_DATE_RANGE

_LOOKUP_OPS = ["equal", "not_equal", "is_not_null", "is_null", "in", "not_in"]
_TEXT_OPS = ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "is_empty", "is_not_empty", "begins_with"]
_NUMBER_OPS = ["equal", "not_equal", "greater", "greater_or_equal", "less", "less_or_equal",
               "between", "not_between", "in", "not_in", "is_null", "is_not_null"]
# getFilterOperatorsByFieldType + the in/not_in extras FilterItem adds for
# LOOK_UP/PICK_LIST/MULTI_PICKLIST and the related-field types.
_OPERATORS_BY_FIELD_TYPE: Dict[str, List[str]] = {
    "LOOK_UP": _LOOKUP_OPS,
    "PICK_LIST": _LOOKUP_OPS,
    "MULTI_PICKLIST": _LOOKUP_OPS,
    "PIPELINE": _LOOKUP_OPS,
    "CAMPAIGN": _LOOKUP_OPS,
    "PIPELINE_STAGE": ["equal", "not_equal", "in", "not_in"],
    "ACTIVITY": ["equal", "not_equal", "in", "not_in"],
    "CALL_LOG_TYPE": ["equal", "not_equal", "in", "not_in"],
    "MEETING_ORGANIZER": ["equal", "not_equal", "in", "not_in"],
    "MEETING_INVITEES": ["equal", "not_equal", "in", "not_in"],
    "FORECASTING_TYPE": ["equal", "not_equal", "in", "not_in", "is_empty", "is_not_empty"],
    "URL": _TEXT_OPS, "EMAIL": _TEXT_OPS, "PHONE": _TEXT_OPS, "SINGLE_LINE_TEXT": _TEXT_OPS, "TEXT_FIELD": _TEXT_OPS,
    "UUID": ["equal", "not_equal", "in", "not_in"],
    "ID": ["equal", "not_equal", "in", "not_in"],
    "PIPELINE_STAGE_REASON": ["equal", "not_equal", "contains", "not_contains", "is_empty", "is_not_empty", "begins_with"],
    "PARAGRAPH_TEXT": ["contains", "not_contains", "is_empty", "is_not_empty"],
    "RICH_TEXT": ["contains", "not_contains", "is_empty", "is_not_empty"],
    "NUMBER": _NUMBER_OPS,
    "MONEY": [op for op in _NUMBER_OPS if op not in ("is_null", "is_not_null")],
    "AUTO_INCREMENT": [op for op in _NUMBER_OPS if op not in ("is_null", "is_not_null")],
    "IMAGE": ["is_not_null", "is_null"], "FILE_PICKER": ["is_not_null", "is_null"],
    "FILE": ["is_not_null", "is_null"], "PAYMENT": ["is_not_null", "is_null"],
    "GPS_COORDINATES": ["range_within"],
}
_DEFAULT_OPERATORS = ["equal", "not_equal"]  # TOGGLE, CHECKBOX, RADIO_BUTTON, everything else

# Field types FilterInput.tsx renders a value input for. Any other type (PARAGRAPH_TEXT,
# TIME_PICKER, ID, UUID, AUTO_INCREMENT, ...) gets no input in the UI, so only the
# value-less operators (is_null, is_empty, ...) are usable on it.
_UI_VALUE_INPUT_TYPES = {
    "NUMBER", "MONEY", "PHONE", "EMAIL", "TEXT_FIELD", "SINGLE_LINE_TEXT", "PIPELINE_STAGE_REASON", "URL",
    "TOGGLE", "CHECKBOX", "PICK_LIST", "MULTI_PICKLIST", "PIPELINE_STAGE", "ACTIVITY",
    "LOOK_UP", "PIPELINE", "CAMPAIGN", "MEETING_ORGANIZER", "MEETING_INVITEES",
    "DATE_PICKER", "DATETIME_PICKER", "GPS_COORDINATES", "FORECASTING_TYPE",
}

# supportedFilterOperatorsByEntity (non-date fields only; the UI never applies
# it to date fields, which use the date-range list instead).
_ENTITY_OPERATOR_RESTRICTIONS: Dict[str, Dict[str, List[str]]] = {
    "deal": {"partPayments": ["is_not_null", "is_null"]},
    "email": {
        "subject": ["equal", "not_equal", "contains", "not_contains", "in", "not_in", "begins_with"],
        "sentBy": ["equal", "in"], "receivedBy": ["equal", "in"], "user": ["equal", "in"],
        "associatedLeads": ["equal", "is_null", "is_not_null", "in"],
        "associatedDeals": ["equal", "is_null", "is_not_null", "in"],
        "associatedContacts": ["equal", "is_null", "is_not_null", "in"],
    },
}

# FilterInput.tsx forecastingTypeOptions (value sent = option id)
_FORECASTING_TYPES = ("OPEN", "CLOSED_WON", "CLOSED_LOST", "CLOSED_UNQUALIFIED")

# ReportConfig.ts EXCLUDED_METRIC_FIELDS — funnel-only internals.
_EXCLUDED_METRIC_FIELDS = {"pipelineStageCompleted", "pipelineStageSkipped"}
_METRIC_TYPES = ("COUNT", "SUM", "AVERAGE")

# RELATED_FIELDS: child filter -> parent filter that must be present.
_RELATED_FIELD_PARENT = {"pipelineStage": "pipeline", "pipelineStageReason": "pipeline", "activities": "campaignActivities"}

# Lookup filters resolvable through an existing registry id in phase 1.
_LOOKUP_RESOLVERS = {"USER": "user.lookup", "PRODUCT": "product.lookup", "PIPELINE": "pipeline.lookup"}
# Email sender/recipient filters use a different lookup (email-recipient) — phase 2.
_PHASE2_FILTER_IDS = {"sentBy", "receivedBy"}
_PHASE2_FILTER_TYPES = {
    "ENTITY_FIELDS", "PIPELINE_STAGE", "ACTIVITY", "CAMPAIGN",
    "MEETING_INVITEES", "MEETING_ORGANIZER", "GPS_COORDINATES",
}

_HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

# The SHORT payload execute_request("report.create", payload) takes. The server
# expands it into the full POST /v3/reports body (see build_report_body).
REPORT_CREATE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "required": ["entity", "name", "group_by", "metrics", "date_range"],
    "properties": {
        "entity": {"type": "string", "enum": list(REPORT_ENTITIES),
                   "description": "Same entity you passed to build_payload."},
        "name": {"type": "string", "minLength": 3, "maxLength": 255},
        "description": {"type": "string", "maxLength": 255},
        "chart_type": {"type": "string", "enum": list(_CHART_TYPES), "default": "bar"},
        "group_by": {
            "type": "object",
            "required": ["field"],
            "properties": {
                "field": {"type": "string", "description": "A dimension id from tenant_fields_reference."},
                "format": {"type": "string", "description": "Required for date dimensions (YEARLY, MONTHLY, ...)."},
            },
        },
        "metrics": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["type", "field"],
                "properties": {"type": {"type": "string", "enum": list(_METRIC_TYPES)}, "field": {"type": "string"}},
            },
        },
        "date_range": {
            "type": "object",
            "required": ["field", "operator"],
            "properties": {
                "field": {"type": "string"},
                "operator": {"type": "string"},
                "value": {"description": "between -> [start, end] (user's local time); last_n_days/next_n_days -> N."},
                "from": {"type": "string", "description": "Optional HH:mm, DATETIME fields only (default 00:00)."},
                "to": {"type": "string", "description": "Optional HH:mm, DATETIME fields only (default 23:59)."},
            },
        },
        "filters": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["field", "operator"],
                "properties": {
                    "field": {"type": "string"},
                    "operator": {"type": "string"},
                    "value": {"description": "Omit for is_null/is_not_null/is_empty/is_not_empty; a list for in/not_in."},
                },
            },
        },
    },
}


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def normalize_report_entity(entity: Any) -> str:
    """Return the singular report entity ("lead", ...) or raise ValueError."""
    value = (entity or "").strip().lower() if isinstance(entity, str) else ""
    # Accept plurals too ("leads", "companies", "calls") — models use both.
    for singular, plural in _REPORT_ENTITY_PLURAL.items():
        if value in (singular, plural):
            return singular
    raise ValueError(
        f"Unknown report entity '{entity}'. Use one of: {', '.join(REPORT_ENTITIES)} "
        f"(the STANDARD type — call get_entity_labels() if the user used a renamed entity)."
    )


async def _fetch_report_config(entity: str) -> Dict[str, Any]:
    """GET /reports/config/{plural}. Drops organizerFields like sd-ui's filterReportConfig."""
    async with get_client() as client:
        response = await client.get(f"/reports/config/{_REPORT_ENTITY_PLURAL[entity]}")
        data = await handle_api_response(response, f"Fetch report config for {entity}")
    if isinstance(data, list):  # defensive: /reports/config (all entities) returns a list
        data = next((c for c in data if c.get("entity") == entity), {}) if data else {}
    data = data or {}
    return {
        **data,
        "dimensions": [d for d in data.get("dimensions") or [] if d.get("id") != "organizerFields"],
        "filters": [f for f in data.get("filters") or [] if f.get("id") != "organizerFields"],
    }


async def _fetch_report_permissions() -> List[Dict[str, Any]]:
    """GET /users/me/permissions — [{name, action: {read, readAll, write, ...}}] (sd-ui AppActions)."""
    async with get_client() as client:
        response = await client.get("/users/me/permissions")
        data = await handle_api_response(response, "Fetch user permissions")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("content") or []
    return []


async def _fetch_tenant_currency() -> Optional[str]:
    """GET /tenants -> currency (e.g. "INR"); sd-ui reads genSettings.payload.currency from it.
    Returns None on any failure — financial options are then hidden (safe default)."""
    try:
        async with get_client() as client:
            response = await client.get("/tenants")
            data = await handle_api_response(response, "Fetch tenant settings")
        return (data or {}).get("currency") if isinstance(data, dict) else None
    except KylasAPIError as e:
        logger.warning("Could not fetch tenant currency: %s", e.message)
        return None


def _permission_action(permissions: List[Dict[str, Any]], name: str) -> Dict[str, Any]:
    entry = next((p for p in permissions or [] if isinstance(p, dict) and p.get("name") == name), None)
    return (entry or {}).get("action") or {}


def readable_report_entities(permissions: List[Dict[str, Any]]) -> List[str]:
    """Entities the user may report on: READ or READ_ALL, meeting always (sd-ui getEntityOptions)."""
    result = []
    for entity in REPORT_ENTITIES:
        action = _permission_action(permissions, entity)
        if entity in _ALWAYS_REPORTABLE_ENTITIES or action.get("read") is True or action.get("readAll") is True:
            result.append(entity)
    return result


def can_create_report(permissions: List[Dict[str, Any]]) -> bool:
    """WRITE on "report" (sd-ui canCreateEntitySelector(entities.REPORTS))."""
    return _permission_action(permissions, "report").get("write") is True


# ---------------------------------------------------------------------------
# Config interpretation (shared by the cheat sheet and the payload builder)
# ---------------------------------------------------------------------------

def _usable(item: Dict[str, Any]) -> bool:
    return bool(item.get("active")) and bool(item.get("filterable"))


def _is_financial_tenant(tenant_currency: Optional[str]) -> bool:
    return (tenant_currency or "").upper() == "INR"


def date_operators_for(entity: str, tenant_currency: Optional[str]) -> List[str]:
    """getDateOperatorsForFieldId(fieldId, reportType, true, tenantCurrency)."""
    if entity == "email":
        return list(_EMAIL_DATE_RANGES)
    ops = [op for op, _future in _DATE_RANGES]
    if entity in _ENTITIES_WITH_RELATIVE_DATES:
        if not _is_financial_tenant(tenant_currency):
            ops = [op for op in ops if op not in _FINANCIAL_OPERATORS]
        return ops
    return [op for op in ops if op not in _RELATIVE_FOR_REPORT]


def dimension_formats_for(tenant_currency: Optional[str]) -> List[str]:
    if _is_financial_tenant(tenant_currency):
        return list(_DIMENSION_FORMATS)
    return [f for f in _DIMENSION_FORMATS if f not in _FINANCIAL_FORMATS]


def filter_operators_for(entity: str, flt: Dict[str, Any], tenant_currency: Optional[str]) -> List[str]:
    field_type = flt.get("fieldType") or ""
    if field_type in _DATE_FIELD_TYPES:
        return date_operators_for(entity, tenant_currency)
    ops = _OPERATORS_BY_FIELD_TYPE.get(field_type, _DEFAULT_OPERATORS)
    restricted = _ENTITY_OPERATOR_RESTRICTIONS.get(entity, {}).get(flt.get("id"))
    if restricted:
        ops = [op for op in ops if op in restricted]
    if field_type not in _UI_VALUE_INPUT_TYPES:
        ops = [op for op in ops if op in _NO_INPUT_OPERATORS]
    return list(ops)


def rule_type_for(flt: Dict[str, Any]) -> str:
    """getDataTypeByFieldType(fieldType, isInternal), with approvalState forced to long (service.ts:87)."""
    if flt.get("id") == "approvalState":
        return "long"
    field_type = flt.get("fieldType") or ""
    if field_type == "PICK_LIST":
        return "string" if flt.get("isInternal") else "long"
    if field_type in ("MULTI_PICKLIST", "LOOK_UP", "PIPELINE", "PIPELINE_STAGE", "CAMPAIGN", "ACTIVITY"):
        return "long"
    if field_type in ("NUMBER", "MONEY"):
        return "double"
    if field_type in ("DATE_PICKER", "TIME_PICKER", "DATETIME_PICKER"):
        return "date"
    if field_type == "TOGGLE":
        return "boolean"
    if field_type == "GPS_COORDINATES":
        return "GpsCoordinates"
    if field_type == "MEETING_INVITEES":
        return "participants_lookup"
    if field_type == "MEETING_ORGANIZER":
        return "organizer_lookup"
    if field_type == "UUID":
        return "uuid"
    return "string"


def filter_support(flt: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """(supported_in_phase_1, resolve_via registry id or None)."""
    field_type = flt.get("fieldType") or ""
    if flt.get("id") in _PHASE2_FILTER_IDS or field_type in _PHASE2_FILTER_TYPES:
        return False, None
    if field_type == "PIPELINE":
        return True, "pipeline.lookup"
    if field_type == "LOOK_UP":
        resolver = _LOOKUP_RESOLVERS.get(((flt.get("lookup") or {}).get("entity") or "").upper())
        return (resolver is not None), resolver
    # e.g. a TOGGLE-like type with no input and no value-less operator: nothing usable.
    if not filter_operators_for("", flt, None):
        return False, None
    return True, None


def metrics_for_dimension(dimension: Dict[str, Any]) -> List[Dict[str, Any]]:
    """ReportConfig.getMetricsFor — the SELECTED dimension's supportedMetrics, minus funnel internals, de-duplicated."""
    seen = set()
    result = []
    for m in dimension.get("supportedMetrics") or []:
        key = (m.get("type"), m.get("field"))
        if m.get("field") in _EXCLUDED_METRIC_FIELDS or key in seen:
            continue
        seen.add(key)
        result.append(m)
    return result


def _picklist_values(flt: Dict[str, Any]) -> List[Dict[str, Any]]:
    if flt.get("fieldType") in ("TOGGLE", "CHECKBOX") and not flt.get("picklist"):
        return [{"id": True, "name": None, "displayName": "Yes"}, {"id": False, "name": None, "displayName": "No"}]
    return [v for v in ((flt.get("picklist") or {}).get("picklistValues") or []) if isinstance(v, dict)]


def _uses_option_name(flt: Dict[str, Any]) -> bool:
    """FilterInput outPutId: name when isInternal (except TOGGLE and approvalState), otherwise id."""
    return bool(flt.get("isInternal")) and flt.get("fieldType") != "TOGGLE" and flt.get("id") != "approvalState"


# ---------------------------------------------------------------------------
# Cheat sheet (build_payload's tenant_fields_reference for report.create)
# ---------------------------------------------------------------------------

def get_report_config_instructions_logic(
    entity: str,
    config: Dict[str, Any],
    requested_picklists: Optional[set] = None,
    tenant_currency: Optional[str] = None,
) -> str:
    requested = {n.strip().lower() for n in (requested_picklists or set()) if isinstance(n, str)}
    dimensions = [d for d in config.get("dimensions") or [] if _usable(d)]
    filters = [f for f in config.get("filters") or [] if _usable(f)]
    date_fields = [f for f in filters if f.get("fieldType") in _DATE_FIELD_TYPES]
    financial = _is_financial_tenant(tenant_currency)

    lines = [
        "=" * 60,
        f"KYLAS REPORT BUILDER — {entity.upper()} (category ONE_DIMENSIONAL)",
        "=" * 60,
        f"Tenant currency: {tenant_currency or 'unknown'} — financial year/quarter options "
        f"{'AVAILABLE' if financial else 'NOT available'}.",
        "",
        "CHART TYPES (chart_type): bar, pie, table. With more than one metric only 'table' is",
        "allowed — the server switches to table automatically and says so.",
        "",
        "DIMENSIONS (group_by.field) — exactly one:",
    ]
    formats = ", ".join(dimension_formats_for(tenant_currency))
    for d in dimensions:
        extra = ""
        if d.get("fieldType") in _DATE_FIELD_TYPES:
            extra = f" — group_by.format REQUIRED: {formats}"
        elif d.get("fieldType") == "ENTITY_FIELDS":
            extra = (f" — groups by the {d.get('primaryField')} user's TEAM (property 'teams' is set "
                     f"automatically; a '{d.get('primaryField')} is_not_null' filter is added if you don't give one)")
        required = [r for r in d.get("requiredFilters") or [] if d.get("fieldType") != "ENTITY_FIELDS"]
        if required:
            extra += f" — REQUIRES filter(s): {', '.join(required)}"
        lines.append(f"  • '{d.get('id')}' {d.get('header')} [{d.get('fieldType') or 'n/a'}]{extra}")

    # Metrics are per dimension; print once when every dimension shares the same list.
    metric_sets = {}
    for d in dimensions:
        key = tuple((m.get("type"), m.get("field")) for m in metrics_for_dimension(d))
        metric_sets.setdefault(key, []).append(d)
    lines.extend(["", "METRICS (metrics[] = {type, field}) — at least one, no duplicates:"])
    for key, dims in metric_sets.items():
        if len(metric_sets) > 1:
            lines.append(f"  For dimension(s): {', '.join(d.get('id') for d in dims)}")
        for m in metrics_for_dimension(dims[0]):
            label = "Number of records" if (m.get("type"), m.get("field")) == ("COUNT", "id") else m.get("header")
            lines.append(f"  • {m.get('type')} {m.get('field')}  ({label})")

    lines.extend(["", "DATE RANGE (date_range) — required; field must be one of:"])
    for f in date_fields:
        lines.append(f"  • '{f.get('id')}' {f.get('header')} [{f.get('fieldType')}]")
    lines.append(f"  operators: {', '.join(date_operators_for(entity, tenant_currency))}")
    lines.append("  value: 'between' -> [start, end] in the user's local time (date or datetime; "
                 "converted to UTC by the server); 'last_n_days'/'next_n_days' -> N (1-364); others -> omit.")
    if entity == "email":
        lines.append("  Email: past ranges only; a custom 'between' range may span at most 90 days.")
    lines.append("  The date-range field can NOT also be used in filters.")

    lines.extend(["", "FILTERS (filters[] = {field, operator, value}) — optional, each field at most once:"])
    for f in filters:
        lines.extend(_format_report_filter(entity, f, requested, tenant_currency))
    lines.append("")
    lines.append("Operators is_null/is_not_null/is_empty/is_not_empty take no value. "
                 "in/not_in take a list. between/not_between (numbers) take [from, to].")
    return "\n".join(lines)


def _format_report_filter(entity: str, f: Dict[str, Any], requested: set, tenant_currency: Optional[str]) -> List[str]:
    fid = f.get("id") or ""
    field_type = f.get("fieldType") or ""
    supported, resolver = filter_support(f)
    head = f"  • '{fid}' {f.get('header')} [{field_type}]"
    if not supported:
        return [f"{head} — NOT SUPPORTED YET (needs a lookup not available in this version)"]
    ops = ", ".join(filter_operators_for(entity, f, tenant_currency))
    lines = [f"{head} — operators: {ops}"]
    parent = _RELATED_FIELD_PARENT.get(fid)
    required = list(f.get("requiredFilters") or []) + ([parent] if parent and parent not in (f.get("requiredFilters") or []) else [])
    if required:
        lines.append(f"     requires filter(s): {', '.join(required)}")
    if resolver:
        extra = ""
        if resolver == "pipeline.lookup":
            # The UI calls the filter's own lookupUrl (e.g. /pipelines/lookup?entityType=LEAD&q=name:).
            match = re.search(r"entityType=(\w+)", (f.get("lookup") or {}).get("lookupUrl") or "")
            extra = f" with entity_type={match.group(1) if match else entity.upper()}"
        lines.append(f"     value: numeric id — resolve_via: {resolver}{extra}. Never guess ids.")
    elif field_type in _DATE_FIELD_TYPES:
        lines.append("     value: same rules as date_range")
    elif field_type == "FORECASTING_TYPE":
        lines.append(f"     value: one of {', '.join(_FORECASTING_TYPES)}")
    elif field_type in ("PICK_LIST", "MULTI_PICKLIST", "TOGGLE", "CHECKBOX"):
        values = _picklist_values(f)
        send = "the option NAME (string)" if _uses_option_name(f) and field_type in ("PICK_LIST", "MULTI_PICKLIST") else "the option ID"
        if field_type in ("TOGGLE", "CHECKBOX"):
            send = "true / false"
        if fid.strip().lower() in _REPORT_LARGE_PICKLISTS and fid.strip().lower() not in requested:
            lines.append(f"     value: {send}. {len(values)} options omitted to keep this reference compact.")
            lines.append(f'     Call build_payload("report.create", entity="{entity}", fields=["{fid}"]) to get them. '
                         "Do NOT guess an option id or name.")
        elif field_type not in ("TOGGLE", "CHECKBOX"):
            lines.append(f"     value: {send}. Options:")
            for v in values:
                lines.append(f"       - {v.get('displayName')} (id: {v.get('id')}, name: '{v.get('name')}')")
        else:
            lines.append(f"     value: {send}")
    elif field_type in ("NUMBER", "MONEY", "AUTO_INCREMENT"):
        lines.append("     value: number")
    else:
        lines.append("     value: text")
    return lines


# ---------------------------------------------------------------------------
# Short payload -> full Kylas body (port of sd-ui service.ts saveReport/getConfig)
# ---------------------------------------------------------------------------

def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _time_or_default(value: Any, default: str, label: str) -> str:
    if value in (None, ""):
        return default
    _require(isinstance(value, str) and bool(_HHMM.match(value)), f"{label} must be a 24h 'HH:mm' time, got {value!r}.")
    return value


def _to_utc_range(value: Any, timezone: str, label: str, max_days: int) -> List[str]:
    """[start, end] in the user's local time -> UTC ISO, date-only values expanded to start/end of day
    (sd-ui DateSelect START_OF_DAY / END_OF_DAY)."""
    _require(isinstance(value, (list, tuple)) and len(value) == 2 and all(isinstance(v, str) and v.strip() for v in value),
             f"{label}: 'between' needs value [start, end] as two date/datetime strings.")
    start, end = (v.strip() for v in value)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
        start += "T00:00:00"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
        end += "T23:59:59.999"
    converted = _convert_date_value_to_utc([start, end], timezone)
    try:
        start_dt, end_dt = (dateutil_parser.isoparse(v) for v in converted)
    except (ValueError, TypeError):
        raise ValueError(f"{label}: could not parse dates {value!r}; use ISO format like '2026-01-31' or '2026-01-31T18:30'.")
    _require(start_dt <= end_dt, f"{label}: start date is after end date.")
    _require((end_dt - start_dt).days <= max_days, f"{label}: a custom date range may span at most {max_days} days.")
    return list(converted)


def _n_days(value: Any, label: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label}: last_n_days/next_n_days need value N (a whole number 1-364).")
    _require(str(value).strip().lstrip("-").isdigit() and 0 < number < 365,
             f"{label}: N must be a whole number between 1 and 364, got {value!r}.")
    return number


def _date_rule_parts(
    entity: str, flt: Dict[str, Any], operator: str, value: Any, spec: Dict[str, Any],
    timezone: str, tenant_currency: Optional[str], label: str,
) -> Dict[str, Any]:
    allowed = date_operators_for(entity, tenant_currency)
    _require(operator in allowed, f"{label}: operator '{operator}' is not allowed for date fields here. Allowed: {', '.join(allowed)}.")
    parts: Dict[str, Any] = {"value": None}
    if operator == "between":
        max_days = _EMAIL_MAX_CUSTOM_RANGE_DAYS if entity == "email" else _MAX_CUSTOM_RANGE_DAYS
        parts["value"] = _to_utc_range(value, timezone, label, max_days)
    elif operator in _N_DAYS_OPERATORS:
        parts["value"] = _n_days(value, label)
    if flt.get("fieldType") == "DATETIME_PICKER" and operator not in _NO_TIME_DATE_OPERATORS:
        parts["from"] = _time_or_default(spec.get("from"), "00:00", f"{label} 'from'")
        parts["to"] = _time_or_default(spec.get("to"), "23:59", f"{label} 'to'")
    return parts


def _resolve_option(flt: Dict[str, Any], raw: Any, label: str) -> Any:
    """Accept an option's id, internal name or display name; return what sd-ui would send."""
    values = _picklist_values(flt)
    # FilterInput's outPutId applies to PICK_LIST and MULTI_PICKLIST alike (rule type stays long
    # for MULTI_PICKLIST — that's what sd-ui sends).
    use_name = _uses_option_name(flt) and flt.get("fieldType") in ("PICK_LIST", "MULTI_PICKLIST")
    for v in values:
        candidates = [v.get("id"), v.get("name"), v.get("displayName")]
        if any(c is not None and (c == raw or str(c).strip().lower() == str(raw).strip().lower()) for c in candidates):
            return v.get("name") if use_name else v.get("id")
    hint = f'build_payload("report.create", entity=..., fields=["{flt.get("id")}"])' if values else "the config"
    raise ValueError(f"{label}: {raw!r} is not an option of '{flt.get('id')}'. Check the options via {hint}.")


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, str) and "," in value:
        return [v.strip() for v in value.split(",") if v.strip()]
    return [value]


def _to_int(value: Any, label: str) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError
        number = int(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{label}: expected a numeric id, got {value!r}. Resolve it with the lookup named in the reference.")
    return number


def _to_number(value: Any, label: str) -> float:
    try:
        if isinstance(value, bool):
            raise ValueError
        number = float(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{label}: expected a number, got {value!r}.")
    return int(number) if number.is_integer() else number


def _rule_value(entity: str, flt: Dict[str, Any], operator: str, value: Any, label: str) -> Any:
    """getFilterValue (service.ts:44) for non-date fields."""
    if operator in _NO_INPUT_OPERATORS:
        return None
    _require(value is not None and value != "" and value != [], f"{label}: operator '{operator}' needs a value.")
    field_type = flt.get("fieldType") or ""
    multi = operator in _MULTI_VALUE_OPERATORS

    if field_type in ("LOOK_UP", "PIPELINE"):
        return [_to_int(v, label) for v in _as_list(value)] if multi else _to_int(value, label)
    if field_type in ("PICK_LIST", "MULTI_PICKLIST"):
        return [_resolve_option(flt, v, label) for v in _as_list(value)] if multi else _resolve_option(flt, value, label)
    if field_type in ("TOGGLE", "CHECKBOX"):
        if isinstance(value, str):
            lowered = value.strip().lower()
            _require(lowered in ("true", "false", "yes", "no"), f"{label}: expected true/false, got {value!r}.")
            return lowered in ("true", "yes")
        _require(isinstance(value, bool), f"{label}: expected true/false, got {value!r}.")
        return value
    if field_type == "FORECASTING_TYPE":
        items = _as_list(value) if multi else [value]
        resolved = []
        for item in items:
            key = str(item).strip().upper().replace(" ", "_")
            key = {"WON": "CLOSED_WON", "LOST": "CLOSED_LOST", "UNQUALIFIED": "CLOSED_UNQUALIFIED"}.get(key, key)
            _require(key in _FORECASTING_TYPES, f"{label}: {item!r} must be one of {', '.join(_FORECASTING_TYPES)}.")
            resolved.append(key)
        return resolved if multi else resolved[0]
    if field_type in ("NUMBER", "MONEY", "AUTO_INCREMENT"):
        if operator in _RANGE_OPERATORS:
            _require(isinstance(value, (list, tuple)) and len(value) == 2, f"{label}: '{operator}' needs value [from, to].")
            return [_to_number(v, label) for v in value]
        if multi:
            # FreeTextTagsInput commits tags as one comma-separated string.
            return ",".join(str(_to_number(v, label)) for v in _as_list(value))
        return _to_number(value, label)
    # Text-like fields: trimmed (service.ts:200-205); in/not_in as a comma-separated string (FreeTextTagsInput).
    if multi:
        return ",".join(str(v).strip() for v in _as_list(value) if str(v).strip())
    return str(value).strip()


def _find(items: List[Dict[str, Any]], item_id: Any) -> Optional[Dict[str, Any]]:
    return next((i for i in items if i.get("id") == item_id), None)


def build_report_body(
    payload: Dict[str, Any],
    config: Dict[str, Any],
    timezone: str,
    tenant_currency: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Validate the short report.create payload against the live config and expand it into the
    full POST /v3/reports body. Returns (body, notes). Raises ValueError with a model-actionable message."""
    _require(isinstance(payload, dict), "payload must be an object.")
    entity = normalize_report_entity(payload.get("entity"))
    notes: List[str] = []
    dimensions = [d for d in config.get("dimensions") or [] if _usable(d)]
    filters = [f for f in config.get("filters") or [] if _usable(f)]

    # --- Basic information ---
    name = payload.get("name")
    _require(isinstance(name, str) and 3 <= len(name.strip()) <= 255, "name is required (3-255 characters).")
    description = payload.get("description") or ""
    _require(isinstance(description, str) and len(description) <= 255, "description must be text of at most 255 characters.")
    category = (payload.get("category") or "ONE_DIMENSIONAL").upper()
    _require(category == "ONE_DIMENSIONAL", "Only category ONE_DIMENSIONAL is supported in this version.")

    # --- Dimension (exactly one) ---
    group_by = payload.get("group_by")
    if isinstance(group_by, list):
        _require(len(group_by) == 1, "ONE_DIMENSIONAL reports take exactly one group_by dimension.")
        group_by = group_by[0]
    if isinstance(group_by, str):
        group_by = {"field": group_by}
    _require(isinstance(group_by, dict) and group_by.get("field"), "group_by.field is required.")
    dimension = _find(dimensions, group_by["field"])
    _require(dimension is not None, f"group_by.field '{group_by['field']}' is not a dimension of {entity}. "
             f"Valid: {', '.join(d.get('id') for d in dimensions)}.")
    fmt = group_by.get("format")
    if dimension.get("fieldType") in _DATE_FIELD_TYPES:
        allowed_formats = dimension_formats_for(tenant_currency)
        _require(isinstance(fmt, str) and fmt.upper() in allowed_formats,
                 f"group_by.format is required for date dimension '{dimension['id']}'. Use one of: {', '.join(allowed_formats)}.")
        fmt = fmt.upper()
    else:
        fmt = None
    is_entity_fields = dimension.get("fieldType") == "ENTITY_FIELDS"
    group_by_body = {
        "name": dimension["id"],
        "format": fmt,
        "primaryField": dimension.get("primaryField") or None,
        "property": "teams" if is_entity_fields else None,
    }

    # --- Metrics ---
    raw_metrics = payload.get("metrics")
    _require(isinstance(raw_metrics, list) and raw_metrics, "metrics is required: a list like [{\"type\": \"COUNT\", \"field\": \"id\"}].")
    available = metrics_for_dimension(dimension)
    valid_metrics = ", ".join("%s %s" % (a.get("type"), a.get("field")) for a in available)
    metrics_body = []
    for i, m in enumerate(raw_metrics):
        _require(isinstance(m, dict), f"metrics[{i}] must be an object {{type, field}}.")
        mtype = str(m.get("type") or "").upper()
        match = next((a for a in available if a.get("type") == mtype and a.get("field") == m.get("field")), None)
        _require(match is not None, f"metrics[{i}] {mtype} {m.get('field')} is not available for dimension "
                 f"'{dimension['id']}'. Valid: {valid_metrics}.")
        _require({"type": mtype, "field": match["field"]} not in metrics_body, f"metrics[{i}] is a duplicate.")
        metrics_body.append({"type": mtype, "field": match["field"]})

    # --- Chart type ---
    chart_type = str(payload.get("chart_type") or "bar").lower()
    _require(chart_type in _CHART_TYPES, f"chart_type must be one of: {', '.join(_CHART_TYPES)}.")
    if len(metrics_body) > 1 and chart_type != "table":
        notes.append(f"chart_type changed from '{chart_type}' to 'table' — reports with more than one metric can only be tables.")
        chart_type = "table"

    # --- Date range ---
    date_range = payload.get("date_range")
    _require(isinstance(date_range, dict) and date_range.get("field") and date_range.get("operator"),
             "date_range {field, operator} is required, e.g. {\"field\": \"createdAt\", \"operator\": \"current_month\"}.")
    date_field = _find(filters, date_range["field"])
    _require(date_field is not None and date_field.get("fieldType") in _DATE_FIELD_TYPES,
             f"date_range.field '{date_range['field']}' is not a date field of {entity}. Valid: "
             f"{', '.join(f.get('id') for f in filters if f.get('fieldType') in _DATE_FIELD_TYPES)}.")
    dr_operator = str(date_range["operator"]).strip().lower()
    dr_parts = _date_rule_parts(entity, date_field, dr_operator, date_range.get("value"), date_range,
                                timezone, tenant_currency, "date_range")
    date_range_body = {
        "id": date_field["id"],
        "field": date_field["id"],
        "operator": dr_operator,
        "type": "date",
        "fieldInputType": date_field.get("fieldType"),
        **dr_parts,
    }

    # --- Filters ---
    raw_filters = payload.get("filters") or []
    _require(isinstance(raw_filters, list), "filters must be a list of {field, operator, value}.")
    rules = []
    used = set()
    for i, spec in enumerate(raw_filters):
        label = f"filters[{i}]"
        _require(isinstance(spec, dict) and spec.get("field") and spec.get("operator"), f"{label} needs field and operator.")
        flt = _find(filters, spec["field"])
        _require(flt is not None, f"{label}: '{spec['field']}' is not a filter of {entity}.")
        label = f"{label} ('{flt['id']}')"
        _require(flt["id"] != date_field["id"], f"{label}: this field is already the date_range field; it cannot also be a filter.")
        _require(flt["id"] not in used, f"{label}: each field can be filtered only once.")
        used.add(flt["id"])
        supported, _resolver = filter_support(flt)
        _require(supported, f"{label}: filtering on this field is not supported yet in this version.")
        operator = str(spec["operator"]).strip().lower()
        if flt.get("fieldType") in _DATE_FIELD_TYPES:
            parts = _date_rule_parts(entity, flt, operator, spec.get("value"), spec, timezone, tenant_currency, label)
        else:
            allowed = filter_operators_for(entity, flt, tenant_currency)
            _require(operator in allowed, f"{label}: operator '{operator}' not allowed. Allowed: {', '.join(allowed)}.")
            parts = {"value": _rule_value(entity, flt, operator, spec.get("value"), label)}
        rules.append({
            "operator": operator,
            "id": flt["id"],
            "field": flt["id"],
            "fieldInputType": flt.get("fieldType"),
            "type": rule_type_for(flt),
            "value": parts.get("value"),
            "from": parts.get("from"),
            "to": parts.get("to"),
            "property": None,
            "primaryField": flt.get("primaryField") or None,
        })

    # --- Required filters (dimension + filter dependencies) ---
    if is_entity_fields:
        primary = (dimension.get("requiredFilters") or [dimension.get("primaryField")])[0]
        if primary and primary not in used:
            primary_filter = _find(filters, primary)
            _require(primary_filter is not None, f"Dimension '{dimension['id']}' needs filter '{primary}', which is not available.")
            rules.append({
                "operator": "is_not_null", "id": primary, "field": primary,
                "fieldInputType": primary_filter.get("fieldType"), "type": rule_type_for(primary_filter),
                "value": None, "from": None, "to": None, "property": None,
                "primaryField": primary_filter.get("primaryField") or None,
            })
            used.add(primary)
            notes.append(f"Added filter '{primary} is_not_null', required when grouping by '{dimension['id']}'.")
    else:
        for req in dimension.get("requiredFilters") or []:
            _require(req in used, f"Grouping by '{dimension['id']}' requires a '{req}' filter — add it to filters.")
    for rule in list(rules):
        flt = _find(filters, rule["id"])
        parent = _RELATED_FIELD_PARENT.get(rule["id"])
        for req in list((flt or {}).get("requiredFilters") or []) + ([parent] if parent else []):
            _require(req in used, f"Filter '{rule['id']}' requires a '{req}' filter — add it to filters.")
    pipeline_rule = next((r for r in rules if r["id"] == "pipeline"), None)
    if pipeline_rule and any(_RELATED_FIELD_PARENT.get(r["id"]) == "pipeline" for r in rules):
        _require(pipeline_rule["operator"] in ("equal", "in"),
                 "When filtering on a pipeline stage/reason, the 'pipeline' filter must use 'equal' or 'in'.")

    body = {
        "name": name.strip(),
        "description": description,
        "prorated": False,
        "goal": None,
        "config": {
            "groupBy": [group_by_body],
            "metrics": metrics_body,
            "dateRange": date_range_body,
            "rules": rules,
        },
        "reportType": entity,
        "chartType": chart_type,
        "category": "ONE_DIMENSIONAL",
    }
    return body, notes


# ---------------------------------------------------------------------------
# Entry points used by build_payload / execute_request
# ---------------------------------------------------------------------------

async def _check_entity_permission(entity: str) -> Optional[str]:
    """Return an error message when the user cannot report on `entity`; None when allowed
    or when permissions could not be fetched (Kylas then enforces it on save)."""
    try:
        permissions = await _fetch_report_permissions()
    except KylasAPIError as e:
        logger.warning("Could not fetch permissions for report checks: %s", e.message)
        return None
    readable = readable_report_entities(permissions)
    if entity not in readable:
        return (f"You don't have permission to report on '{entity}'. "
                f"Entities you can report on: {', '.join(readable) or 'none'}.")
    return None


async def build_report_reference(entity: Any, requested_picklists: Optional[List[str]] = None) -> Dict[str, Any]:
    """Live part of build_payload("report.create", entity=...): permission check + config cheat sheet."""
    _reset_api_call_count()
    entity = normalize_report_entity(entity)
    denied = await _check_entity_permission(entity)
    if denied:
        raise ValueError(denied)
    config = await _fetch_report_config(entity)
    tenant_currency = await _fetch_tenant_currency()
    requested = {n.strip().lower() for n in (requested_picklists or []) if isinstance(n, str)}
    return {
        "entity": entity,
        "tenant_currency": tenant_currency,
        "tenant_fields_reference": get_report_config_instructions_logic(entity, config, requested, tenant_currency),
    }


async def create_report_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """report.create: validate + expand the short payload, then POST /v3/reports."""
    _reset_api_call_count()
    entity = normalize_report_entity((payload or {}).get("entity"))

    try:
        permissions = await _fetch_report_permissions()
    except KylasAPIError as e:
        logger.warning("Could not fetch permissions for report create: %s", e.message)
        permissions = None
    if permissions is not None:
        _require(can_create_report(permissions), "You don't have permission to create reports.")
        readable = readable_report_entities(permissions)
        _require(entity in readable, f"You don't have permission to report on '{entity}'. "
                 f"Entities you can report on: {', '.join(readable) or 'none'}.")

    config = await _fetch_report_config(entity)
    tenant_currency = await _fetch_tenant_currency()
    user = await _fetch_current_user()
    timezone = user.get("timezone") or DEFAULT_TIMEZONE

    body, notes = build_report_body(payload, config, timezone, tenant_currency)

    async with get_client() as client:
        response = await client.post(f"{REPORTS_V3_BASE}/reports", json=body)
        created = await handle_api_response(response, "Create report")

    result = {
        "id": (created or {}).get("id"),
        "name": body["name"],
        "reportType": body["reportType"],
        "chartType": body["chartType"],
        "category": body["category"],
    }
    if notes:
        result["notes"] = notes
    return result
