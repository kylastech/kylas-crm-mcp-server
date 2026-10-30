"""Campaign and campaign-activity operations for the registry tools.

Same shape as app/reports.py: plain logic functions plus JSON schemas, wired
into list_tool / build_payload / execute_request by registry id. Nothing here
is registered as an @mcp.tool — the client only ever sees the registry tools.

  - CAMPAIGN_SCHEMAS: registry id -> input schema returned by build_payload.
  - CAMPAIGN_ROUTERS: registry id -> async router(payload) run by execute_request.

Every call goes to sd-campaign (/v1/campaigns/...). Two things about that
service shape the helpers below:

  - It answers a bare 403 with an EMPTY body for any input it can't handle
    (unknown filter field, wrong enum case, groupBy not valid for the level)
    — not only for missing permission. _campaign_call turns that into an
    actionable message; a 403 with a body is a real permission error and is
    passed through unchanged.
  - Search filters are validated against a static allow-list (the fields and
    operators sd-campaign's FilterRequestDto / *Specification accept) before
    any request is made, so the model gets a specific reason instead of the
    bare 403.
"""

from datetime import date, datetime, time, timedelta, timezone as dt_timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil import parser as dateutil_parser

from app.client import KylasAPIError, get_client, handle_api_response
from app.config import DEFAULT_TIMEZONE, logger
from app.entities import _fetch_current_user
from app.helpers import _convert_date_value_to_utc

CAMPAIGN_BUCKETS = frozenset({"campaign", "campaign_activity"})

# ---------------------------------------------------------------------------
# Enums (sd-campaign CampaignStatus / ActivityStatus / ActivityType / EntityType)
# ---------------------------------------------------------------------------

CAMPAIGN_STATUSES = ["DRAFT", "IN_PROGRESS", "PAUSED", "PROCESSED", "COMPLETED"]
ACTIVITY_STATUSES = ["DRAFT", "QUEUED", "FAILED", "IN_PROGRESS", "PAUSED", "RETRYING", "PROCESSED", "COMPLETED"]
ACTIVITY_TYPES = ["EMAIL", "WHATSAPP", "CALL"]
ACTIVITY_ENTITIES = ["LEAD", "CONTACT"]

# ---------------------------------------------------------------------------
# Search filter allow-lists: field -> (api type, allowed operators, enum values or None)
# ---------------------------------------------------------------------------

_ID_OPS = ["equal", "not_equal", "in", "not_in"]
_TEXT_OPS = ["equal", "not_equal", "contains", "not_contains", "begins_with", "in", "not_in"]
_NUMBER_OPS = ["equal", "not_equal", "greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between"]
_DATE_OPS = ["greater", "greater_or_equal", "less", "less_or_equal", "between", "not_between", "is_null", "is_not_null"]

_NO_VALUE_OPS = {"is_null", "is_not_null"}
_LIST_OPS = {"in", "not_in"}
_RANGE_OPS = {"between", "not_between"}

FilterSpec = Tuple[str, List[str], Optional[List[str]]]

_COMMON_FILTER_FIELDS: Dict[str, FilterSpec] = {
    "id": ("long", _ID_OPS, None),
    "name": ("string", _TEXT_OPS, None),
    "estimatedBudget": ("double", _NUMBER_OPS, None),
    "actualExpense": ("double", _NUMBER_OPS, None),
    **{f: ("date", _DATE_OPS, None) for f in ("createdAt", "updatedAt", "startedAt", "endedAt")},
    **{f: ("long", _ID_OPS, None) for f in ("createdBy", "updatedBy", "startedBy", "endedBy")},
}

CAMPAIGN_FILTER_FIELDS: Dict[str, FilterSpec] = {
    **_COMMON_FILTER_FIELDS,
    "status": ("string", _ID_OPS, CAMPAIGN_STATUSES),
}

ACTIVITY_FILTER_FIELDS: Dict[str, FilterSpec] = {
    **_COMMON_FILTER_FIELDS,
    "status": ("string", _ID_OPS, ACTIVITY_STATUSES),
    "type": ("string", _ID_OPS, ACTIVITY_TYPES),
    "entity": ("string", _ID_OPS, ACTIVITY_ENTITIES),
}

# Activity search has no campaign filter: root.get("campaign") is the Campaign
# entity, so any rule on it fails with the bare 403.
_CAMPAIGN_REFERENCE_FIELDS = {"campaign", "campaignId", "campaign_id", "campaign.id"}

# Sort fields sd-campaign accepts (Spring sort on entity attributes; money
# fields get ".value" appended server-side). Anything else is a bare 403.
CAMPAIGN_SORT_FIELDS = [
    "id", "name", "status", "startDate", "endDate", "startedAt", "endedAt",
    "createdAt", "updatedAt", "estimatedBudget", "actualExpense",
]

MAX_PAGE_SIZE = 50
MAX_ANALYTICS_DAYS = 366

# ---------------------------------------------------------------------------
# Analytics group_by (model-facing name -> API groupBy). Mirrors
# CampaignFacade.strategyMap and sd-ui CampaignAnalyticsDimension.
# ---------------------------------------------------------------------------

CAMPAIGN_GROUP_BY: Dict[str, List[str]] = {
    "DAY": ["DAY"],
    "DAY_ACTIVITIES": ["DAY", "ACTIVITIES"],
}

ACTIVITY_GROUP_BY: Dict[str, Dict[str, List[str]]] = {
    "EMAIL": {"ACTIVITY_STATUS": ["ACTIVITY_STATUS"], "DAY": ["DAY"]},
    "WHATSAPP": {"ACTIVITY_STATUS": ["ACTIVITY_STATUS"], "DAY": ["DAY"]},
    "CALL": {"CALL_DISPOSITION": ["CALL_DISPOSITION"], "DAY_OUTCOME": ["DAY", "OUTCOME"]},
}

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

_EMPTY_403_MESSAGE = (
    "{op} was rejected by the campaign service (403 with an empty body). It returns this for an "
    "invalid filter field, enum value or group_by — not only for missing permission. Re-check "
    "the values against this id's build_payload usage_notes."
)


async def _campaign_call(
    method: str,
    path: str,
    op: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Any:
    """One request to sd-campaign. Raises KylasAPIError on failure, like handle_api_response."""
    if method not in ("GET", "POST"):
        raise ValueError(f"Unsupported method for campaign call: {method}")
    kwargs: Dict[str, Any] = {"params": params} if params else {}
    async with get_client() as client:
        if method == "GET":
            response = await client.get(path, **kwargs)
        else:
            response = await client.post(path, json=body if body is not None else {}, **kwargs)
    if response.status_code == 403 and not (response.text or "").strip():
        raise KylasAPIError(_EMPTY_403_MESSAGE.format(op=op), status_code=403)
    return await handle_api_response(response, op)


# ---------------------------------------------------------------------------
# Request builders
# ---------------------------------------------------------------------------


def _number_value(value: Any, api_type: str, label: str, field: str) -> Any:
    """A filter value as the number sd-campaign compares against; "5000" -> 5000.0, "abc" -> ValueError."""
    if isinstance(value, bool):
        raise ValueError(f"{label}: '{field}' needs a number, got {value!r}.")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label}: '{field}' needs a number, got {value!r}.")
    if api_type == "long":
        if not number.is_integer():
            raise ValueError(f"{label}: '{field}' needs a whole-number id, got {value!r}.")
        return int(number)
    return number


def _date_value(value: Any, tz_name: str, end_of_day: bool, label: str, field: str) -> str:
    """A local date/datetime filter value as the UTC instant sd-campaign parses (OffsetDateTime).

    Same convention as the other entity searches: values are the user's local
    time (a trailing "Z" is ignored), unless they carry a real offset. A bare
    date means the whole day — its start, or its end when end_of_day is set
    (upper bounds, so "between 1 Oct and 31 Oct" includes 31 Oct).
    """
    text = value.strip() if isinstance(value, str) else ""
    try:
        return _utc_instant(date.fromisoformat(text), tz_name, end_of_day)
    except ValueError:
        pass
    try:
        dateutil_parser.parse(text.rstrip("Zz"))
    except (ValueError, OverflowError):
        raise ValueError(f"{label}: '{field}' needs a date (YYYY-MM-DD) or datetime, got {value!r}.")
    return _convert_date_value_to_utc(text, tz_name)


# Operators whose (upper-bound) bare date means the END of that day.
_END_OF_DAY_OPS = {"greater", "less_or_equal"}


def build_campaign_json_rule(
    filters: Optional[List[Dict[str, Any]]],
    allowed: Dict[str, FilterSpec],
    timezone: str = DEFAULT_TIMEZONE,
) -> Dict[str, Any]:
    """Validate [{field, operator, value}] and return the {"jsonRule": ...} body sd-campaign search expects.

    Raises ValueError (surfaced by execute_request as BAD_PAYLOAD) naming the
    filter and the allowed values, instead of letting sd-campaign answer 403.
    Number values are coerced to numbers; date values are converted from local
    time (the filter's own timeZone, else `timezone`) to UTC.
    """
    rules = []
    for i, f in enumerate(filters or []):
        label = f"Filter #{i + 1}"
        if not isinstance(f, dict):
            raise ValueError(f"{label}: expected an object with field, operator and value.")
        field = f.get("field")
        operator = str(f.get("operator") or "equal").strip().lower()
        value = f.get("value")

        if field in _CAMPAIGN_REFERENCE_FIELDS:
            raise ValueError(
                f"{label}: activities can't be filtered by campaign. Use campaign.get — "
                "it returns the campaign together with its activities."
            )
        if field not in allowed:
            raise ValueError(f"{label}: '{field}' is not filterable. Allowed fields: {sorted(allowed)}.")

        api_type, operators, enum_values = allowed[field]
        if operator not in operators:
            raise ValueError(f"{label}: operator '{operator}' isn't allowed on '{field}'. Allowed: {operators}.")

        if operator in _NO_VALUE_OPS:
            value = None
        elif operator in _LIST_OPS:
            if not isinstance(value, list) or not value:
                raise ValueError(f"{label}: '{operator}' needs a non-empty list value.")
        elif operator in _RANGE_OPS:
            if not isinstance(value, list) or len(value) != 2:
                raise ValueError(f"{label}: '{operator}' needs [from, to].")
        elif value is None or isinstance(value, list):
            raise ValueError(f"{label}: '{operator}' needs a single value.")

        if enum_values and value is not None:
            # sd-campaign maps enum values case-sensitively; a lowercase value is a bare 403.
            values = value if isinstance(value, list) else [value]
            normalized = [str(v).strip().upper() for v in values]
            invalid = [v for v in normalized if v not in enum_values]
            if invalid:
                raise ValueError(f"{label}: {invalid} not valid for '{field}'. Allowed: {enum_values}.")
            value = normalized if isinstance(value, list) else normalized[0]

        if api_type in ("long", "double") and value is not None:
            if isinstance(value, list):
                value = [_number_value(v, api_type, label, field) for v in value]
            else:
                value = _number_value(value, api_type, label, field)

        if api_type == "date" and value is not None:
            tz_name = str(f.get("timeZone") or timezone).strip()
            try:
                ZoneInfo(tz_name)
            except (ZoneInfoNotFoundError, ValueError):
                raise ValueError(f"{label}: unknown timeZone '{tz_name}'. Use an IANA name such as 'Asia/Calcutta'.")
            if isinstance(value, list):
                value = [_date_value(value[0], tz_name, False, label, field),
                         _date_value(value[1], tz_name, True, label, field)]
            else:
                value = _date_value(value, tz_name, operator in _END_OF_DAY_OPS, label, field)

        rules.append({"id": field, "field": field, "type": api_type, "operator": operator, "value": value})

    return {"jsonRule": {"condition": "AND", "valid": True, "rules": rules}}


def analytics_body(group_by: List[str], date_from: str, date_to: str, timezone: str) -> Dict[str, Any]:
    """Body for POST .../analytics — the shape sd-ui sends (Campaign/service.ts getCampaignAnalytics)."""
    return {
        "timeZone": timezone,
        "groupBy": group_by,
        "metric": {"type": "COUNT"},
        "dateRange": [date_from, date_to],
    }


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------


def _require_id(payload: Dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or value in (None, ""):
        raise ValueError(f"'{key}' is required (a positive integer id).")
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"'{key}' must be a positive integer id, got {value!r}.")
    if number <= 0:
        raise ValueError(f"'{key}' must be a positive integer id, got {value!r}.")
    return number


def _page_params(payload: Dict[str, Any], default_sort: str) -> Dict[str, Any]:
    """page/size/sort query params, validated so sd-campaign never sees a value it 403s on."""
    try:
        page = int(payload.get("page", 0))
        size = int(payload.get("size", 20))
    except (TypeError, ValueError):
        raise ValueError("'page' and 'size' must be integers.")
    if page < 0:
        raise ValueError("'page' must be 0 or more (pages are 0-based).")
    if not 1 <= size <= MAX_PAGE_SIZE:
        raise ValueError(f"'size' must be between 1 and {MAX_PAGE_SIZE}.")

    sort = str(payload.get("sort") or default_sort).strip()
    field, _, direction = sort.partition(",")
    direction = (direction or "asc").strip().lower()
    if field.strip() not in CAMPAIGN_SORT_FIELDS:
        raise ValueError(f"Can't sort by '{field.strip()}'. Sortable fields: {CAMPAIGN_SORT_FIELDS}.")
    if direction not in ("asc", "desc"):
        raise ValueError(f"Sort direction must be 'asc' or 'desc', got '{direction}'.")
    return {"page": page, "size": size, "sort": f"{field.strip()},{direction}"}


# ---------------------------------------------------------------------------
# Response trimming — keep what answers questions, drop UI-only fields
# (recordActions, bulkJobId, lastPaused/Resumed audit fields, ...).
# ---------------------------------------------------------------------------


async def _currency_names() -> Dict[int, str]:
    """currencyValueId -> code (INR, USD, ...) for this tenant. Reads never fail because of it."""
    try:
        rows = await _campaign_call("GET", "/v1/forex/currencies", "Fetch tenant currencies")
        return {r["currencyValueId"]: r.get("name") for r in rows if r.get("currencyValueId") is not None}
    except Exception as e:  # timeout, network error, unexpected shape — money just loses its currency code
        logger.warning("Campaign currencies unavailable, money shows currency_id only: %r", e)
        return {}


def _money(money: Optional[Dict[str, Any]], currencies: Dict[int, str]) -> Optional[Dict[str, Any]]:
    if not money:
        return None
    currency_id = money.get("currencyId")
    return {"value": money.get("value"), "currency": currencies.get(currency_id), "currency_id": currency_id}


def _user(user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return {"id": user.get("id"), "name": user.get("name")} if user else None


def _content_summary(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """What the activity sends and to whom, per type (EmailDetail / WhatsappDetail / CallDetail)."""
    payload = payload or {}
    summary: Dict[str, Any] = {"sent_to": payload.get("sentTo")}
    activity_type = payload.get("type")
    if activity_type == "EMAIL":
        summary["email_template"] = payload.get("emailTemplate")
        summary["sending_email_account"] = payload.get("connectedEmailAccount")
    elif activity_type == "WHATSAPP":
        summary["whatsapp_template"] = payload.get("whatsappTemplate")
        summary["whatsapp_account"] = payload.get("connectedAccount")
        summary["retry_times"] = (payload.get("retryConfig") or {}).get("noOfTimes")
    elif activity_type == "CALL":
        summary["dialer_app"] = (payload.get("marketplaceApp") or {}).get("name")
        summary["agents"] = payload.get("userAgents")
        summary["did_number"] = payload.get("didNumber")
    return summary


def _audience(filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rules = ((filters or {}).get("jsonRule") or {}).get("rules") or []
    return [{"field": r.get("field"), "operator": r.get("operator"), "value": r.get("value")} for r in rules]


def _activity_detail(activity: Dict[str, Any], currencies: Dict[int, str]) -> Dict[str, Any]:
    detail = {
        "id": activity.get("id"),
        "name": activity.get("name"),
        "type": activity.get("type"),
        "status": activity.get("status"),
        "entity": activity.get("entity"),
        "estimated_budget": _money(activity.get("estimatedBudget"), currencies),
        "actual_expense": _money(activity.get("actualExpense"), currencies),
        "start_date": activity.get("startDate"),
        "end_date": activity.get("endDate"),
        "started_at": activity.get("startedAt"),
        "ended_at": activity.get("endedAt"),
        "processed_at": activity.get("processedAt"),
        "utm": {k: activity.get(f"utm{k.title()}") for k in ("campaign", "source", "medium", "content", "term")},
        "content": _content_summary(activity.get("payload")),
        "audience_filters": _audience(activity.get("filters")),
        "total_engagement": activity.get("totalEngagement"),
        "records_generated": activity.get("recordsGenerated"),
        "click_through_rate": activity.get("clickThroughRate"),
    }
    if activity.get("type") == "CALL":
        for key in ("totalRecords", "dialedCalls", "connectedCalls", "conversionRate", "averageHandlingTime"):
            detail[key] = activity.get(key)
    return detail


def _campaign_summary(campaign: Dict[str, Any], currencies: Dict[int, str]) -> Dict[str, Any]:
    return {
        "id": campaign.get("id"),
        "name": campaign.get("name"),
        "status": campaign.get("status"),
        "estimated_budget": _money(campaign.get("estimatedBudget"), currencies),
        "actual_expense": _money(campaign.get("actualExpense"), currencies),
        "start_date": campaign.get("startDate"),
        "end_date": campaign.get("endDate"),
        "started_at": campaign.get("startedAt"),
        "ended_at": campaign.get("endedAt"),
        "created_by": _user(campaign.get("createdBy")),
        "total_engagement": campaign.get("totalEngagement"),
        "records_generated": campaign.get("recordsGenerated"),
        "activities": [
            {"id": a.get("id"), "name": a.get("name"), "type": a.get("type"), "status": a.get("status")}
            for a in campaign.get("activities") or []
        ],
    }


# ---------------------------------------------------------------------------
# Analytics dates — the model sends local calendar dates; sd-campaign wants a
# UTC instant range and answers one row per local day, named by that day's
# local midnight in UTC ("2026-10-12T18:30Z" is 13 Oct in Asia/Calcutta).
# ---------------------------------------------------------------------------


async def _resolve_timezone(requested: Optional[str]) -> str:
    tz_name = (requested or "").strip()
    if not tz_name:
        try:
            tz_name = (await _fetch_current_user()).get("timezone") or DEFAULT_TIMEZONE
        except KylasAPIError:
            tz_name = DEFAULT_TIMEZONE
    try:
        ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"Unknown timezone '{tz_name}'. Use an IANA name such as 'Asia/Calcutta'.")
    return tz_name


def _parse_local_date(value: Any, key: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        raise ValueError(f"'{key}' must be a calendar date YYYY-MM-DD, got {value!r}.")


def _resolve_date_range(payload: Dict[str, Any], tz_name: str) -> Tuple[date, date]:
    raw_from, raw_to = payload.get("date_from"), payload.get("date_to")
    if not raw_from and not raw_to:
        # Current week, Monday to Sunday — the web app's default for these charts.
        today = datetime.now(ZoneInfo(tz_name)).date()
        monday = today - timedelta(days=today.weekday())
        return monday, monday + timedelta(days=6)
    if not raw_from or not raw_to:
        raise ValueError("Give both 'date_from' and 'date_to', or neither (defaults to the current week).")
    date_from, date_to = _parse_local_date(raw_from, "date_from"), _parse_local_date(raw_to, "date_to")
    if date_from > date_to:
        raise ValueError("'date_from' must be on or before 'date_to'.")
    if (date_to - date_from).days + 1 > MAX_ANALYTICS_DAYS:
        raise ValueError(f"Date range can cover at most {MAX_ANALYTICS_DAYS} days.")
    return date_from, date_to


def _utc_instant(day: date, tz_name: str, end_of_day: bool = False) -> str:
    local = datetime.combine(day, time.max if end_of_day else time.min, tzinfo=ZoneInfo(tz_name))
    return local.astimezone(dt_timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{local.microsecond // 1000:03d}Z"


def _local_day(api_day: str, tz_name: str) -> str:
    instant = datetime.fromisoformat(str(api_day).replace("Z", "+00:00"))
    return instant.astimezone(ZoneInfo(tz_name)).date().isoformat()


# ---------------------------------------------------------------------------
# campaign.search / campaign.get / campaign.analytics
# ---------------------------------------------------------------------------


async def search_campaigns_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/search — what the campaign list page calls."""
    filters = payload.get("filters") or []
    needs_timezone = any(
        isinstance(f, dict) and not f.get("timeZone")
        and CAMPAIGN_FILTER_FIELDS.get(f.get("field"), ("",))[0] == "date"
        for f in filters
    )
    timezone = await _resolve_timezone(None) if needs_timezone else DEFAULT_TIMEZONE
    body = build_campaign_json_rule(filters, CAMPAIGN_FILTER_FIELDS, timezone)
    search_term = str(payload.get("search_term") or "").strip()
    if search_term:
        body["jsonRule"]["rules"].append(
            {"id": "multi_field", "field": "multi_field", "type": "string", "operator": "multi_field", "value": search_term}
        )
    params = _page_params(payload, "updatedAt,desc")
    data = await _campaign_call("POST", "/v1/campaigns/search", "Search campaigns", params=params, body=body)
    currencies = await _currency_names()
    return {
        "total": data.get("totalElements"),
        "page": params["page"],
        "size": params["size"],
        "has_more": not data.get("last", True),
        "campaigns": [_campaign_summary(c, currencies) for c in data.get("content") or []],
    }


async def get_campaign_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """GET /v1/campaigns/{id}/overview — what the campaign details page calls. Includes its activities."""
    campaign_id = _require_id(payload, "campaign_id")
    data = await _campaign_call("GET", f"/v1/campaigns/{campaign_id}/overview", "Fetch campaign")
    currencies = await _currency_names()
    campaign = _campaign_summary(data, currencies)
    campaign.update({
        "description": data.get("description"),
        "utm": {k: data.get(f"utm{k.title()}") for k in ("campaign", "source", "medium", "content", "term")},
        "updated_by": _user(data.get("updatedBy")),
        "started_by": _user(data.get("startedBy")),
        "ended_by": _user(data.get("endedBy")),
        "created_at": data.get("createdAt"),
        "updated_at": data.get("updatedAt"),
        "activities": [_activity_detail(a, currencies) for a in data.get("activities") or []],
        "facebook_channels": len(data.get("channels") or []),
    })
    return campaign


async def campaign_analytics_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/{id}/analytics — the two charts on the campaign details page."""
    campaign_id = _require_id(payload, "campaign_id")
    group_by = str(payload.get("group_by") or "DAY").strip().upper()
    if group_by not in CAMPAIGN_GROUP_BY:
        raise ValueError(f"group_by '{group_by}' isn't valid for a campaign. Use one of {sorted(CAMPAIGN_GROUP_BY)}.")
    tz_name = await _resolve_timezone(payload.get("timezone"))
    date_from, date_to = _resolve_date_range(payload, tz_name)

    body = analytics_body(
        CAMPAIGN_GROUP_BY[group_by],
        _utc_instant(date_from, tz_name),
        _utc_instant(date_to, tz_name, end_of_day=True),
        tz_name,
    )
    rows = await _campaign_call("POST", f"/v1/campaigns/{campaign_id}/analytics", "Campaign analytics", body=body)

    result: Dict[str, Any] = {
        "campaign_id": campaign_id,
        "group_by": group_by,
        "metric": "engagements (WhatsApp messages read, emails opened, calls made)",
        "timezone": tz_name,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
    }
    if group_by == "DAY":
        days = [{"date": _local_day(r["name"], tz_name), "engagements": r.get("value") or 0} for r in rows or []]
        result["days"] = [d for d in days if d["engagements"]]
    else:
        days, per_activity = [], {}
        for r in rows or []:
            activities = [
                {"id": d.get("id"), "name": d.get("name"), "type": d.get("type"), "engagements": d.get("value") or 0}
                for d in r.get("dimension") or [] if d.get("value")
            ]
            if activities:
                days.append({"date": _local_day(r["name"], tz_name), "activities": activities})
            for a in activities:
                entry = per_activity.setdefault(a["id"], {**a, "engagements": 0})
                entry["engagements"] += a["engagements"]
        result["days"] = days
        result["per_activity_total"] = sorted(per_activity.values(), key=lambda a: -a["engagements"])

    result["total"] = sum(
        d.get("engagements", 0) if group_by == "DAY" else sum(a["engagements"] for a in d["activities"])
        for d in result["days"]
    )
    if not result["total"]:
        result["note"] = "No engagement in this date range. Only days with engagement are listed."
    return result


# ---------------------------------------------------------------------------
# Input schemas (what build_payload returns for each id)
# ---------------------------------------------------------------------------

_ALL_FILTER_OPERATORS = sorted(set(_ID_OPS + _TEXT_OPS + _NUMBER_OPS + _DATE_OPS))


def _filters_schema(fields: Dict[str, FilterSpec]) -> Dict[str, Any]:
    return {
        "type": "array",
        "description": "AND-combined filters. Allowed fields/operators/values are listed in usage_notes.",
        "items": {
            "type": "object",
            "properties": {
                "field": {"type": "string", "enum": sorted(fields)},
                "operator": {"type": "string", "enum": _ALL_FILTER_OPERATORS, "default": "equal"},
                "value": {"description": "Single value; a list for in/not_in; [from, to] for between."},
                "timeZone": {"type": "string",
                             "description": "Date fields only: IANA timezone of the value; defaults to the current user's."},
            },
            "required": ["field"],
        },
    }


_PAGING_PROPERTIES = {
    "page": {"type": "integer", "minimum": 0, "default": 0, "description": "0-based page."},
    "size": {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_SIZE, "default": 20},
    "sort": {"type": "string", "default": "updatedAt,desc",
             "description": f"'<field>,<asc|desc>' (direction defaults to asc). "
                            f"Fields: {', '.join(CAMPAIGN_SORT_FIELDS)}."},
}

_CAMPAIGN_ID_PROPERTY = {"campaign_id": {"type": "integer", "description": "Campaign id (from campaign.search)."}}

CampaignRouter = Callable[[Dict[str, Any]], Awaitable[Any]]

CAMPAIGN_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "campaign.search": {
        "type": "object",
        "properties": {
            "filters": _filters_schema(CAMPAIGN_FILTER_FIELDS),
            "search_term": {"type": "string", "description": "Free text matched against the campaign name."},
            **_PAGING_PROPERTIES,
        },
    },
    "campaign.get": {
        "type": "object",
        "properties": dict(_CAMPAIGN_ID_PROPERTY),
        "required": ["campaign_id"],
    },
    "campaign.analytics": {
        "type": "object",
        "properties": {
            **_CAMPAIGN_ID_PROPERTY,
            "group_by": {"type": "string", "enum": sorted(CAMPAIGN_GROUP_BY), "default": "DAY",
                         "description": "DAY = engagements per day; DAY_ACTIVITIES = per day split by activity."},
            "date_from": {"type": "string", "format": "date", "description": "Local date YYYY-MM-DD."},
            "date_to": {"type": "string", "format": "date", "description": "Local date YYYY-MM-DD, inclusive."},
            "timezone": {"type": "string", "description": "IANA timezone; defaults to the current user's."},
        },
        "required": ["campaign_id"],
    },
}

CAMPAIGN_ROUTERS: Dict[str, CampaignRouter] = {
    "campaign.search": search_campaigns_logic,
    "campaign.get": get_campaign_logic,
    "campaign.analytics": campaign_analytics_logic,
}


def unwired_campaign_ids(registry: Dict[str, Dict[str, Any]]) -> List[str]:
    """Campaign-bucket registry ids missing a schema or a router (checked at server startup)."""
    return sorted(
        entry_id
        for entry_id, entry in registry.items()
        if entry.get("bucket") in CAMPAIGN_BUCKETS
        and (entry_id not in CAMPAIGN_SCHEMAS or entry_id not in CAMPAIGN_ROUTERS)
    )
