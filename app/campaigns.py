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
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil import parser as dateutil_parser

from app.client import KylasAPIError, get_client, handle_api_response
from app.config import DEFAULT_TIMEZONE, logger
from app.entities import _BUCKET_PICKLIST_RULES, _fetch_contact_fields, _fetch_current_user, _fetch_lead_fields
from app.helpers import _build_search_json_rule, _convert_date_value_to_utc, _get_filterable_fields_map

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

# What "how did this activity do?" means per type: the funnel for messages,
# connected/missed calls per day for calls.
DEFAULT_ACTIVITY_GROUP_BY = {"EMAIL": "ACTIVITY_STATUS", "WHATSAPP": "ACTIVITY_STATUS", "CALL": "DAY_OUTCOME"}

# Funnel steps in the order the activity page shows them (sd-ui CampaignActivityFunnelChart).
FUNNEL_ORDER = {
    "EMAIL": ["Sent", "Opened", "Clicked", "Failed"],
    "WHATSAPP": ["Sent", "Delivered", "Read", "Replied", "Failed"],
}

# ---------------------------------------------------------------------------
# Activity search / recipient list allow-lists
# ---------------------------------------------------------------------------

# sd-campaign sorts activities on any Activity attribute; these are the useful ones.
ACTIVITY_SORT_FIELDS = CAMPAIGN_SORT_FIELDS + ["type", "entity", "processedAt"]

CALL_OUTCOMES = ["CONNECTED", "REJECTED", "BUSY", "NO_ANSWER", "MISSED_CALL", "IN_PROGRESS"]

# Recipient filters per activity type — the filterable columns 
RECIPIENT_FILTER_FIELDS: Dict[str, Dict[str, FilterSpec]] = {
    "EMAIL": {
        "status": ("string", _ID_OPS, ["SENT", "OPENED", "CLICKED", "FAILED"]),
        **{f: ("date", _DATE_OPS, None) for f in ("sentAt", "openedAt", "clickedAt", "failedAt")},
    },
    "WHATSAPP": {
        "status": ("string", _ID_OPS, ["SENT", "DELIVERED", "READ", "REPLIED", "FAILED"]),
        **{f: ("date", _DATE_OPS, None) for f in ("sentAt", "deliveredAt", "readAt", "repliedAt", "failedAt")},
    },
    "CALL": {
        "outcome": ("string", _ID_OPS, CALL_OUTCOMES),
        "disposition": ("long", _ID_OPS, None),
        "userAgent": ("long", _ID_OPS, None),
        "callCreatedAt": ("date", _DATE_OPS, None),
    },
}

# Sortable recipient columns are the timestamps; the first is the page's default (desc).
RECIPIENT_SORT_FIELDS = {
    "EMAIL": ["sentAt", "openedAt", "clickedAt", "failedAt"],
    "WHATSAPP": ["sentAt", "deliveredAt", "readAt", "repliedAt", "failedAt"],
    "CALL": ["callCreatedAt"],
}

# Activities in these states have sent nothing, so their recipient list is empty.
_NOT_SENT_STATUSES = {"DRAFT", "QUEUED", "FAILED"}

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
    allow_empty: bool = False,
) -> Any:
    """One request to sd-campaign. Raises KylasAPIError on failure, like handle_api_response.

    PUT is for updates. There is deliberately no DELETE: campaigns and
    activities are never deleted through the MCP. allow_empty is for the
    lifecycle actions (start/pause/resume/complete), which answer 200 with an
    empty body that handle_api_response would otherwise fail to parse.
    """
    if method not in ("GET", "POST", "PUT"):
        raise ValueError(f"Unsupported method for campaign call: {method}")
    kwargs: Dict[str, Any] = {"params": params} if params else {}
    async with get_client() as client:
        if method == "GET":
            response = await client.get(path, **kwargs)
        elif method == "PUT":
            response = await client.put(path, json=body if body is not None else {}, **kwargs)
        else:
            response = await client.post(path, json=body if body is not None else {}, **kwargs)
    if response.status_code == 403 and not (response.text or "").strip():
        raise KylasAPIError(_EMPTY_403_MESSAGE.format(op=op), status_code=403)
    if allow_empty and response.is_success and not (response.text or "").strip():
        return {}
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


def _optional_id(payload: Dict[str, Any], key: str) -> Optional[int]:
    return _require_id(payload, key) if payload.get(key) not in (None, "") else None


def _page_params(
    payload: Dict[str, Any],
    default_sort: str,
    sort_fields: List[str] = CAMPAIGN_SORT_FIELDS,
) -> Dict[str, Any]:
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
    if field.strip() not in sort_fields:
        raise ValueError(f"Can't sort by '{field.strip()}'. Sortable fields: {sort_fields}.")
    if direction not in ("asc", "desc"):
        raise ValueError(f"Sort direction must be 'asc' or 'desc', got '{direction}'.")
    return {"page": page, "size": size, "sort": f"{field.strip()},{direction}"}


# ---------------------------------------------------------------------------
# Response trimming — keep what answers questions, drop UI-only fields
# (recordActions, bulkJobId, lastPaused/Resumed audit fields, ...).
# ---------------------------------------------------------------------------


async def _fetch_currencies() -> List[Dict[str, Any]]:
    """The tenant's currency rows — the same call sd-campaign's ForexService makes."""
    return await _campaign_call("GET", "/v1/forex/currencies", "Fetch tenant currencies") or []


async def _currency_names() -> Dict[int, str]:
    """currencyValueId -> code (INR, USD, ...) for this tenant. Reads never fail because of it."""
    try:
        rows = await _fetch_currencies()
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
        detail["agent_metrics"] = [
            {"agent": _user(m), "status": m.get("callDialerStatus"), "records_assigned": m.get("totalRecordsAssigned"),
             "dialed_calls": m.get("dialedCalls"), "connected_calls": m.get("connectedCalls"),
             "conversion_rate": m.get("conversionRate"), "average_handling_time": m.get("averageHandlingTime")}
            for m in activity.get("agentDialerMetrics") or []
        ]
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


def _activity_ref(activity: Dict[str, Any]) -> Dict[str, Any]:
    """Which activity a result is about — enough for the model to name it and its campaign."""
    campaign = activity.get("campaign") or {}
    return {
        "id": activity.get("id"),
        "name": activity.get("name"),
        "type": activity.get("type"),
        "status": activity.get("status"),
        "campaign": {"id": campaign.get("id"), "name": campaign.get("name")},
    }


def _activity_summary(activity: Dict[str, Any], currencies: Dict[int, str]) -> Dict[str, Any]:
    return {
        **_activity_ref(activity),
        "entity": activity.get("entity"),
        "estimated_budget": _money(activity.get("estimatedBudget"), currencies),
        "actual_expense": _money(activity.get("actualExpense"), currencies),
        "start_date": activity.get("startDate"),
        "started_at": activity.get("startedAt"),
        "ended_at": activity.get("endedAt"),
        "total_engagement": activity.get("totalEngagement"),
        "records_generated": activity.get("recordsGenerated"),
    }


def _recipient(row: Dict[str, Any], activity_type: str) -> Dict[str, Any]:
    """One row of the recipient list (Email/WhatsApp/CallRecipientStatusResponse), timestamps UTC."""
    recipient = {"name": row.get("recipientName"), "entity_type": row.get("entityType"), "entity_id": row.get("entityId")}
    if activity_type == "EMAIL":
        recipient.update({
            "email": row.get("email"), "status": row.get("status"), "sent_at": row.get("sentAt"),
            "opened_at": row.get("openedAt"), "clicked_at": row.get("clickedAt"), "failed_at": row.get("failedAt"),
        })
    elif activity_type == "WHATSAPP":
        recipient.update({
            "phone_number": row.get("phoneNumber"), "status": row.get("status"), "sent_at": row.get("sentAt"),
            "delivered_at": row.get("deliveredAt"), "read_at": row.get("readAt"),
            "replied_at": row.get("repliedAt"), "failed_at": row.get("failedAt"),
        })
    else:
        recipient.update({
            "phone_number": row.get("phoneNumber"), "outcome": row.get("outcome"),
            "disposition": (row.get("disposition") or {}).get("name"), "agent": _user(row.get("userAgent")),
            "duration_seconds": row.get("duration"), "call_id": row.get("callId"), "called_at": row.get("callCreatedAt"),
        })
    if row.get("errorMessage"):
        recipient["error"] = row["errorMessage"]
    return recipient


def _generated_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """A lead/contact created from the activity (GeneratedRecordResponse)."""
    return {
        "entity_type": row.get("entityType"),
        "entity_id": row.get("entityId"),
        "name": row.get("entityName"),
        "phone_numbers": [f"{p.get('dialCode') or ''}{p.get('value') or ''}" for p in row.get("phoneNumbers") or []],
        "emails": [e.get("value") for e in row.get("emails") or []],
        "created_at": row.get("createdAt"),
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


def _resolve_date_range(
    payload: Dict[str, Any],
    tz_name: str,
    default: Optional[Tuple[date, date]] = None,
) -> Tuple[date, date]:
    raw_from, raw_to = payload.get("date_from"), payload.get("date_to")
    if not raw_from and not raw_to:
        if default:
            return default
        # Current week, Monday to Sunday — the web app's default for these charts.
        today = datetime.now(ZoneInfo(tz_name)).date()
        monday = today - timedelta(days=today.weekday())
        return monday, monday + timedelta(days=6)
    if not raw_from or not raw_to:
        raise ValueError("Give both 'date_from' and 'date_to', or neither (defaults are in usage_notes).")
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


def _activity_default_range(activity: Dict[str, Any], tz_name: str) -> Optional[Tuple[date, date]]:
    """The activity page's default chart range: start date -> end date, or start date + 7 days.

    sd-ui CampaignActivityFunnelChart. None when the activity has no start
    date (the caller then falls back to the current week).
    """
    if not activity.get("startDate"):
        return None
    start = date.fromisoformat(_local_day(activity["startDate"], tz_name))
    end = date.fromisoformat(_local_day(activity["endDate"], tz_name)) if activity.get("endDate") else None
    end = end or start + timedelta(days=7)
    return start, min(max(end, start), start + timedelta(days=MAX_ANALYTICS_DAYS - 1))


def _engagement_days(rows: Optional[List[Dict[str, Any]]], tz_name: str) -> List[Dict[str, Any]]:
    """DAY analytics rows -> [{date, engagements}] for the days that had any."""
    days = [{"date": _local_day(r["name"], tz_name), "engagements": r.get("value") or 0} for r in rows or []]
    return [d for d in days if d["engagements"]]


async def _filter_timezone(filters: List[Any], allowed: Dict[str, FilterSpec]) -> str:
    """Timezone for date filters that don't carry their own; looks up the user only when one needs it."""
    needs_timezone = any(
        isinstance(f, dict) and not f.get("timeZone") and allowed.get(f.get("field"), ("",))[0] == "date"
        for f in filters
    )
    return await _resolve_timezone(None) if needs_timezone else DEFAULT_TIMEZONE


# ---------------------------------------------------------------------------
# campaign.search / campaign.get / campaign.analytics
# ---------------------------------------------------------------------------


async def search_campaigns_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/search — what the campaign list page calls."""
    filters = payload.get("filters") or []
    timezone = await _filter_timezone(filters, CAMPAIGN_FILTER_FIELDS)
    body = build_campaign_json_rule(filters, CAMPAIGN_FILTER_FIELDS, timezone)
    _add_search_term(body, payload)
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
        result["days"] = _engagement_days(rows, tz_name)
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
# campaign_activity.* — one activity (EMAIL / WHATSAPP / CALL) inside a campaign
# ---------------------------------------------------------------------------


def _add_search_term(body: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """search_term -> sd-campaign's name search (lower(name) LIKE %term%), as the list pages send it."""
    search_term = str(payload.get("search_term") or "").strip()
    if search_term:
        body["jsonRule"]["rules"].append(
            {"id": "multi_field", "field": "multi_field", "type": "string", "operator": "multi_field", "value": search_term}
        )


async def _search_activity_rows(rules: List[Dict[str, Any]], size: int) -> List[Dict[str, Any]]:
    body = {"jsonRule": {"condition": "AND", "valid": True, "rules": rules}}
    data = await _campaign_call("POST", "/v1/campaigns/activities/search", "Search campaign activities",
                                params={"page": 0, "size": size, "sort": "updatedAt,desc"}, body=body)
    return data.get("content") or []


async def _resolve_activity(payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """The activity a payload names, as (activity, None) — or (None, choice) when a name matches several.

    Users know activities by name, and Kylas needs the campaign id in most
    activity URLs (a wrong one is a 404), so: activity_id + campaign_id is a
    direct GET; activity_id alone or activity_name is looked up through
    activity search, whose rows carry the full activity and its campaign.
    """
    activity_id = _optional_id(payload, "activity_id")
    campaign_id = _optional_id(payload, "campaign_id")
    name = str(payload.get("activity_name") or "").strip()

    if activity_id:
        if campaign_id:
            path = f"/v1/campaigns/{campaign_id}/activities/{activity_id}"
            return await _campaign_call("GET", path, "Fetch campaign activity"), None
        rows = await _search_activity_rows(
            [{"id": "id", "field": "id", "type": "long", "operator": "equal", "value": activity_id}], 1)
        if not rows:
            raise KylasAPIError("Activity not found.", status_code=404)
        return rows[0], None

    if not name:
        raise ValueError("Give 'activity_id' or 'activity_name'.")
    rows = await _search_activity_rows(
        [{"id": "multi_field", "field": "multi_field", "type": "string", "operator": "multi_field", "value": name}],
        MAX_PAGE_SIZE)
    if campaign_id:
        rows = [r for r in rows if (r.get("campaign") or {}).get("id") == campaign_id]
    exact = [r for r in rows if (r.get("name") or "").strip().lower() == name.lower()]
    matches = exact or rows
    if not matches:
        raise KylasAPIError(
            f"No activity name contains '{name}'. Retry once with its most distinctive word. If that "
            "finds nothing either, STOP and tell the user no activity has that name — you may offer to "
            "check campaigns if they meant a campaign, but don't search campaigns (or try more words) "
            "unless they ask.", status_code=404)
    if len(matches) > 1:
        return None, {
            "matches": [{**_activity_ref(r), "total_engagement": r.get("totalEngagement")} for r in matches],
            "note": f"STOP: more than one activity matched '{name}' and nothing was fetched for any of them. "
                    "Ask the user to choose one: a single-choice question with one option per match showing "
                    "name, #id, status and engagement (and campaign when they differ) — as clickable choices "
                    "if your app offers them. Then call again with the chosen activity_id. Don't pick one "
                    "yourself, and don't run this for each match or for all of them unless the user "
                    "explicitly asked for all.",
        }
    return matches[0], None


async def search_activities_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/activities/search — activities across all campaigns."""
    filters = payload.get("filters") or []
    timezone = await _filter_timezone(filters, ACTIVITY_FILTER_FIELDS)
    body = build_campaign_json_rule(filters, ACTIVITY_FILTER_FIELDS, timezone)
    _add_search_term(body, payload)
    params = _page_params(payload, "updatedAt,desc", ACTIVITY_SORT_FIELDS)
    data = await _campaign_call("POST", "/v1/campaigns/activities/search", "Search campaign activities",
                                params=params, body=body)
    currencies = await _currency_names()
    return {
        "total": data.get("totalElements"),
        "page": params["page"],
        "size": params["size"],
        "has_more": not data.get("last", True),
        "activities": [_activity_summary(a, currencies) for a in data.get("content") or []],
    }


async def get_activity_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """One activity with everything the activity details page shows."""
    activity, choice = await _resolve_activity(payload)
    if choice:
        return choice
    currencies = await _currency_names()
    detail = _activity_detail(activity, currencies)
    detail.update({
        "campaign": _activity_ref(activity)["campaign"],
        "created_by": _user(activity.get("createdBy")),
        "started_by": _user(activity.get("startedBy")),
        "ended_by": _user(activity.get("endedBy")),
        "created_at": activity.get("createdAt"),
        "updated_at": activity.get("updatedAt"),
    })
    return detail


def _funnel(rows: Optional[List[Dict[str, Any]]], activity_type: str) -> List[Dict[str, Any]]:
    """ACTIVITY_STATUS rows -> funnel steps in the page's order; a step with no row counts 0."""
    counts = {r.get("name"): r.get("value") or 0 for r in rows or []}
    order = FUNNEL_ORDER.get(activity_type, [])
    steps = [{"step": s, "count": counts.pop(s, 0)} for s in order]
    return steps + [{"step": s, "count": c} for s, c in counts.items()]


async def activity_analytics_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/{cid}/activities/{aid}/analytics — the charts on the activity details page."""
    activity, choice = await _resolve_activity(payload)
    if choice:
        return choice
    activity_type = activity.get("type")
    allowed = ACTIVITY_GROUP_BY.get(activity_type)
    if not allowed:
        raise ValueError(f"Activity type '{activity_type}' has no analytics.")
    group_by = str(payload.get("group_by") or DEFAULT_ACTIVITY_GROUP_BY[activity_type]).strip().upper()
    if group_by not in allowed:
        # sd-campaign answers a bare 403 for a groupBy the activity type doesn't support.
        raise ValueError(f"group_by '{group_by}' isn't valid for a {activity_type} activity. Use one of {sorted(allowed)}.")
    agent_ids = payload.get("agent_ids") or []
    if agent_ids and activity_type != "CALL":
        raise ValueError("'agent_ids' only applies to CALL activities.")
    if not isinstance(agent_ids, list):
        raise ValueError("'agent_ids' must be a list of user ids.")
    agent_ids = [_number_value(a, "long", "agent_ids", "agent_ids") for a in agent_ids]

    tz_name = await _resolve_timezone(payload.get("timezone"))
    default_range = _activity_default_range(activity, tz_name)
    date_from, date_to = _resolve_date_range(payload, tz_name, default_range)
    if payload.get("date_from") or payload.get("date_to"):
        range_source = "as requested"
    elif default_range:
        range_source = "activity start date to its end date (or start + 7 days) — the activity page's default"
    else:
        range_source = "current week (the activity has no start date)"

    body = analytics_body(
        allowed[group_by],
        _utc_instant(date_from, tz_name),
        _utc_instant(date_to, tz_name, end_of_day=True),
        tz_name,
    )
    if agent_ids:
        body["rules"] = [{"id": "userAgent", "field": "userAgent", "type": "long", "operator": "in", "value": agent_ids}]
    campaign_id = (activity.get("campaign") or {}).get("id")
    rows = await _campaign_call(
        "POST", f"/v1/campaigns/{campaign_id}/activities/{activity['id']}/analytics", "Activity analytics", body=body)

    result: Dict[str, Any] = {
        "activity": _activity_ref(activity),
        "group_by": group_by,
        "timezone": tz_name,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "date_range": range_source,
    }
    if group_by == "ACTIVITY_STATUS":
        # No "total": the steps overlap (a read message was also sent and delivered).
        result["metric"] = "recipients reaching each step in the date range (each step by its own timestamp)"
        result["funnel"] = _funnel(rows, activity_type)
    elif group_by == "DAY":
        result["metric"] = "engagements: " + ("emails opened" if activity_type == "EMAIL" else "WhatsApp messages read")
        result["days"] = _engagement_days(rows, tz_name)
        result["total"] = sum(d["engagements"] for d in result["days"])
    elif group_by == "DAY_OUTCOME":
        result["metric"] = "calls per outcome per day"
        days, per_outcome = [], {}
        for r in rows or []:
            outcomes = [{"outcome": d.get("name"), "calls": d.get("value") or 0}
                        for d in r.get("dimension") or [] if d.get("value")]
            if outcomes:
                days.append({"date": _local_day(r["name"], tz_name), "outcomes": outcomes})
            for o in outcomes:
                per_outcome[o["outcome"]] = per_outcome.get(o["outcome"], 0) + o["calls"]
        result["days"] = days
        result["per_outcome_total"] = [{"outcome": k, "calls": v}
                                       for k, v in sorted(per_outcome.items(), key=lambda kv: -kv[1])]
        result["total"] = sum(per_outcome.values())
    else:  # CALL_DISPOSITION
        result["metric"] = "calls per disposition"
        result["dispositions"] = [
            {"id": r.get("id"), "disposition": r.get("name") or "No disposition", "calls": r.get("value") or 0}
            for r in rows or [] if r.get("value")
        ]
        result["total"] = sum(d["calls"] for d in result["dispositions"])

    if not (result.get("total") or any(step["count"] for step in result.get("funnel", []))):
        result["note"] = "Nothing in this date range."
        if default_range and range_source != "as requested":
            result["note"] += (" This was the activity page's default window (start date to end date or +7 days);"
                               " the activity may have run later — retry with date_from = its started_at date"
                               " and date_to = today (at most 365 days after date_from — the range limit is"
                               f" {MAX_ANALYTICS_DAYS} days) before telling the user there was no activity.")
    return result


async def activity_recipients_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/activities/{aid}/recipient-status — who it went to and what each one did."""
    activity, choice = await _resolve_activity(payload)
    if choice:
        return choice
    activity_type = activity.get("type")
    sort_fields = RECIPIENT_SORT_FIELDS[activity_type]
    params = _page_params(payload, f"{sort_fields[0]},desc", sort_fields)
    if activity.get("status") in _NOT_SENT_STATUSES:
        # sd-campaign answers these with an empty page anyway — skip the call.
        return {
            "activity": _activity_ref(activity), "total": 0, "page": params["page"], "size": params["size"],
            "has_more": False, "recipients": [],
            "note": f"This activity is {activity['status']}: it hasn't sent anything, so it has no recipients.",
        }
    allowed = RECIPIENT_FILTER_FIELDS[activity_type]
    filters = payload.get("filters") or []
    timezone = await _filter_timezone(filters, allowed)
    body = build_campaign_json_rule(filters, allowed, timezone)
    data = await _campaign_call("POST", f"/v1/campaigns/activities/{activity['id']}/recipient-status",
                                "Fetch activity recipients", params=params, body=body)
    return {
        "activity": _activity_ref(activity),
        "total": data.get("totalElements"),
        "page": params["page"],
        "size": params["size"],
        "has_more": not data.get("last", True),
        "recipients": [_recipient(r, activity_type) for r in data.get("content") or []],
    }


async def activity_generated_records_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns/activities/{aid}/generated-records — leads/contacts created from the activity."""
    activity_id = _optional_id(payload, "activity_id")
    activity_ref: Dict[str, Any] = {"id": activity_id}
    if not activity_id:
        activity, choice = await _resolve_activity(payload)
        if choice:
            return choice
        activity_id, activity_ref = activity["id"], _activity_ref(activity)
    params = _page_params(payload, "createdAt,desc", ["createdAt"])
    data = await _campaign_call("POST", f"/v1/campaigns/activities/{activity_id}/generated-records",
                                "Fetch generated records", params=params)
    return {
        "activity": activity_ref,
        "total": data.get("totalElements"),
        "page": params["page"],
        "size": params["size"],
        "has_more": not data.get("last", True),
        "records": [_generated_record(r) for r in data.get("content") or []],
    }


# ---------------------------------------------------------------------------
# Write helpers (Plan 2) — shared by campaign / activity create, update and
# lifecycle. Nothing here writes on its own.
# ---------------------------------------------------------------------------


def _write_date(value: Any, tz_name: str, end_of_day: bool, field: str) -> str:
    """start_date / end_date from the user's local time to the UTC instant sd-campaign stores.

    A plain date is the whole day: start_date -> 00:00:00.000, end_date ->
    23:59:59.999 local (end_of_day). A datetime is taken as given (local
    unless it carries an offset).
    """
    return _date_value(value, tz_name, end_of_day, "Invalid date", field)


def _check_date_order(start: str, end: Optional[str]) -> None:
    # Both come from _write_date in the same UTC format, so they compare as strings.
    if end and end < start:
        raise ValueError("'end_date' must be on or after 'start_date'.")


# Model-facing field -> API field, for the plain fields a user can change.
_CAMPAIGN_FIELD_MAP = {
    "name": "name", "description": "description", "start_date": "startDate", "end_date": "endDate",
    "utm_campaign": "utmCampaign", "utm_source": "utmSource", "utm_medium": "utmMedium",
    "utm_content": "utmContent", "utm_term": "utmTerm",
}
_ACTIVITY_FIELD_MAP = {k: v for k, v in _CAMPAIGN_FIELD_MAP.items() if k != "description"}

# Exactly the fields of sd-campaign's UpdateCampaignRequest / UpdateActivityRequest.
# A PUT body is built from these only, so read-only fields (status counters,
# recordActions, activities, ...) can never be sent back.
_CAMPAIGN_BODY_FIELDS = (
    "id", "name", "description", "estimatedBudget", "actualExpense", "startDate", "endDate",
    "startedAt", "endedAt", "startedBy", "endedBy",
    "utmSource", "utmCampaign", "utmMedium", "utmContent", "utmTerm",
)
_ACTIVITY_BODY_FIELDS = (
    "name", "status", "estimatedBudget", "actualExpense", "startDate", "endDate",
    "utmSource", "utmCampaign", "utmMedium", "utmContent", "utmTerm", "filters", "entity", "payload",
)


def merge_for_update(
    current: Dict[str, Any],
    changes: Dict[str, Any],
    field_map: Dict[str, str],
    body_fields: Iterable[str],
) -> Dict[str, Any]:
    """Full PUT body: the record's current value for every field the update takes, with only the user's changes.

    sd-campaign's PUT needs the whole object (name, startDate, UTM, payload,
    filters, ...); sending just the changed field fails validation or wipes
    the rest. `changes` uses model-facing names (field_map); anything not
    changed keeps its current value.
    """
    body = {field: current.get(field) for field in body_fields}
    for key, api_field in field_map.items():
        if key in changes:
            body[api_field] = changes[key]
    return body


async def _resolve_campaign_id(payload: Dict[str, Any]) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """The campaign a payload names, as (id, None) — or (None, choice) when a name matches several.

    Same rules as _resolve_activity: campaign_id is used as given; a
    campaign_name is looked up in the first page (50) of "name contains"
    matches, newest-updated first, and an exact name (any case) wins.
    """
    campaign_id = _optional_id(payload, "campaign_id")
    if campaign_id:
        return campaign_id, None
    name = str(payload.get("campaign_name") or "").strip()
    if not name:
        raise ValueError("Give 'campaign_id' or 'campaign_name'.")
    body = {"jsonRule": {"condition": "AND", "valid": True, "rules": []}}
    _add_search_term(body, {"search_term": name})
    data = await _campaign_call("POST", "/v1/campaigns/search", "Search campaigns",
                                params={"page": 0, "size": MAX_PAGE_SIZE, "sort": "updatedAt,desc"}, body=body)
    rows = data.get("content") or []
    matches = [r for r in rows if (r.get("name") or "").strip().lower() == name.lower()] or rows
    if not matches:
        raise KylasAPIError(
            f"No campaign name contains '{name}'. Retry once with its most distinctive word. If that finds "
            "nothing either, STOP and tell the user no campaign has that name.", status_code=404)
    if len(matches) > 1:
        return None, {
            "matches": [{"id": r.get("id"), "name": r.get("name"), "status": r.get("status"),
                         "total_engagement": r.get("totalEngagement"), "started_at": r.get("startedAt")}
                        for r in matches],
            "note": f"STOP: more than one campaign matched '{name}' and nothing was done to any of them. "
                    "Ask the user to choose one: a single-choice question with one option per match showing "
                    "name, #id, status, engagement and started date — as clickable choices if your app offers "
                    "them. Then call again with the chosen campaign_id. Don't pick one yourself, and don't run "
                    "this for each match.",
        }
    return matches[0]["id"], None


_AUDIENCE_FIELD_FETCHERS = {"LEAD": _fetch_lead_fields, "CONTACT": _fetch_contact_fields}


async def build_audience_rule(entity: str, filters: Optional[List[Dict[str, Any]]], timezone: str) -> Dict[str, Any]:
    """An activity's audience as the jsonRule its `filters` stores.

    Same filter format the model already uses for lead.search / contact.search
    (field / operator / value, checked against the tenant's filterable
    fields), because sd-campaign runs these rules through that same search.
    At least one filter is required, so an activity can never target every
    lead or contact by accident.
    """
    entity = str(entity or "").strip().upper()
    if entity not in _AUDIENCE_FIELD_FETCHERS:
        raise ValueError(f"'entity' must be one of {ACTIVITY_ENTITIES}, got {entity or None!r}.")
    if not filters:
        raise ValueError("'audience_filters' needs at least one filter — an activity can't target every "
                         f"{entity.lower()}.")
    filterable = _get_filterable_fields_map(await _AUDIENCE_FIELD_FETCHERS[entity]())
    json_rule, error = _build_search_json_rule(
        filters, filterable, default_timezone=timezone,
        internal_name_fields=_BUCKET_PICKLIST_RULES.get(entity.lower(), {}).get("internal_name", set()),
    )
    if error:
        raise ValueError(f"Invalid audience filter: {error}")
    return json_rule


async def count_audience(entity: str, json_rule: Dict[str, Any]) -> int:
    """How many leads / contacts the audience matches right now — the same search the activity will run.

    An activity can only start with 1–10,000 recipients (sd-campaign isActivityRunnable).
    """
    async with get_client() as client:
        response = await client.post(f"/v1/search/{entity.lower()}", params={"page": 0, "size": 1},
                                     json={"fields": ["id"], "jsonRule": json_rule})
        data = await handle_api_response(response, "Count audience")
    return int((data or {}).get("totalElements") or 0)


# ---------------------------------------------------------------------------
# Lookups for the create / update forms — the web form's dropdown sources
# (sd-ui formLayout getFormLookupUrl). Each "lookup" call answers at most
# 10 rows ({"content": [...]}, paging ignored), so a name is searched with
# q=<name> and an id is fetched directly by id.
# ---------------------------------------------------------------------------


async def _kylas_get(path: str, op: str, params: Optional[Dict[str, Any]] = None, not_found: str = "") -> Any:
    """GET from another Kylas service (email, messages). A 404 becomes `not_found` when given."""
    try:
        async with get_client() as client:
            response = await client.get(path, params=params)
            return await handle_api_response(response, op)
    except KylasAPIError as e:
        if e.status_code == 404 and not_found:
            raise KylasAPIError(not_found, status_code=404)
        raise


def _lookup_rows(data: Any) -> List[Dict[str, Any]]:
    return (data or {}).get("content") or [] if isinstance(data, dict) else list(data or [])


def _choose_by_name(
    rows: List[Dict[str, Any]],
    name: str,
    label: str,
    names: Callable[[Dict[str, Any]], List[str]] = lambda r: [r.get("name") or ""],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """One row for a name, as (row, None) — or (None, choice) when several match. Exact name (any case) wins."""
    exact = [r for r in rows if name.lower() in [n.strip().lower() for n in names(r)]]
    matches = exact or rows
    if not matches:
        raise KylasAPIError(f"No {label} name contains '{name}'. Check the name with the user (the matching "
                            f"lookup id lists the available ones).", status_code=404)
    if len(matches) > 1:
        return None, {
            "matches": matches,
            "note": f"STOP: more than one {label} matched '{name}' and nothing was done. Ask the user to choose "
                    "one (a single-choice question, clickable if your app offers it), then call again with its id.",
        }
    return matches[0], None


def _id_or_name(value: Any) -> Tuple[Optional[int], str]:
    """A template / account given as an id (number or digits) or as a name."""
    if isinstance(value, bool):
        raise ValueError(f"Expected an id or a name, got {value!r}.")
    if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
        return int(value), ""
    name = str(value or "").strip()
    if not name:
        raise ValueError("Expected an id or a name.")
    return None, name


async def _active_currencies() -> List[Dict[str, Any]]:
    return [{"currency_id": r.get("currencyValueId"), "code": r.get("name"), "base": bool(r.get("baseCurrency"))}
            for r in await _fetch_currencies() if r.get("active") and r.get("currencyValueId") is not None]


async def _resolve_currency(value: Any) -> int:
    """A currency code ("INR") or id -> the currencyValueId to send; none given -> the tenant's base currency.

    Kylas wants currencyValueId, not the currency row's id, and only an
    active currency is accepted.
    """
    currencies = await _active_currencies()
    if value in (None, ""):
        base = next((c for c in currencies if c["base"]), None)
        if not base:
            raise ValueError("This tenant has no base currency; give the budget's currency.")
        return base["currency_id"]
    text = str(value).strip()
    for c in currencies:
        if (text.isdigit() and c["currency_id"] == int(text)) or text.upper() == str(c["code"]).upper():
            return c["currency_id"]
    active = ", ".join(f"{c['code']} ({c['currency_id']})" for c in currencies)
    raise ValueError(f"Currency {value!r} isn't an active currency here. Active: {active}.")


async def _resolve_email_template(value: Any, entity: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """An email template by id or name, as the {id, name} an EMAIL activity's payload stores."""
    template_id, name = _id_or_name(value)
    if template_id:
        t = await _kylas_get(f"/v1/email-templates/{template_id}", "Fetch email template",
                             not_found=f"Email template {template_id} not found.")
        if str(t.get("category") or "").upper() != entity:
            raise ValueError(f"Email template '{t.get('name')}' is for {str(t.get('category')).upper()}s, "
                             f"not {entity}s.")
        return {"id": t["id"], "name": t.get("name")}, None
    data = await _kylas_get("/v1/email-templates/lookup", "Look up email templates",
                            params={"category": entity.lower(), "q": name})
    row, choice = _choose_by_name(_lookup_rows(data), name, "email template")
    return (None, choice) if choice else ({"id": row["id"], "name": row.get("name")}, None)


def _account_body(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": row.get("id"), "name": row.get("name"), "businessName": row.get("businessName")}


async def _resolve_whatsapp_account(value: Any) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """A WhatsApp account by id or name (its name or business name), as {id, name, businessName}."""
    account_id, name = _id_or_name(value)
    if account_id:
        rows = _lookup_rows(await _kylas_get("/v1/messages/connected-accounts/lookup",
                                             "Look up WhatsApp accounts", params={"q": ""}))
        row = next((r for r in rows if r.get("id") == account_id), None)
        if row:
            return _account_body(row), None
        # Not in the first 10. GET by id names the fields the other way round:
        # its "name" is the business name and "displayName" the account name.
        a = await _kylas_get(f"/v1/messages/connected-accounts/{account_id}", "Fetch WhatsApp account",
                             not_found=f"WhatsApp account {account_id} not found.")
        return {"id": a["id"], "name": a.get("displayName"), "businessName": a.get("name")}, None
    rows = _lookup_rows(await _kylas_get("/v1/messages/connected-accounts/lookup",
                                         "Look up WhatsApp accounts", params={"q": name}))
    if not rows:
        # The lookup searches the account name only, but users often say the business
        # name ("Kylas Devs"), so match both ourselves over the (short) account list.
        rows = [r for r in _lookup_rows(await _kylas_get("/v1/messages/connected-accounts/lookup",
                                                         "Look up WhatsApp accounts", params={"q": ""}))
                if name.lower() in f"{r.get('name') or ''}\n{r.get('businessName') or ''}".lower()]
    row, choice = _choose_by_name(rows, name, "WhatsApp account",
                                  names=lambda r: [r.get("name") or "", r.get("businessName") or ""])
    if choice:
        choice["matches"] = [_account_body(m) for m in choice["matches"]]
        return None, choice
    return _account_body(row), None


async def _resolve_whatsapp_template(
    value: Any, account_id: int, entity: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """A WhatsApp template of that account and entity, by id or name, as {id, name}. It must be APPROVED.

    A name goes through the web form's lookup, which only returns APPROVED
    templates of that account and entity, so whatever it picks is usable. An
    id skips the lookup, so that template is fetched and checked here.
    """
    template_id, name = _id_or_name(value)
    if not template_id:
        data = await _kylas_get("/v1/messages/whatsapp-templates/lookup", "Look up WhatsApp templates",
                                params={"connectedAccountId": account_id, "entityType": entity.lower(), "q": name})
        row, choice = _choose_by_name(_lookup_rows(data), name, "WhatsApp template")
        return (None, choice) if choice else ({"id": row["id"], "name": row.get("name")}, None)
    t = await _kylas_get(f"/v1/messages/whatsapp-templates/{template_id}", "Fetch WhatsApp template",
                         not_found=f"WhatsApp template {template_id} not found.")
    _check_whatsapp_template(t, account_id, entity)
    return {"id": t["id"], "name": t.get("name")}, None


def _check_whatsapp_template(t: Dict[str, Any], account_id: int, entity: str) -> None:
    """A template is usable only from its own account, for its own entity, once WhatsApp approved it."""
    if (t.get("connectedAccount") or {}).get("id") != account_id:
        raise ValueError(f"WhatsApp template '{t.get('name')}' belongs to another WhatsApp account.")
    if str(t.get("entityType") or "").upper() != entity:
        raise ValueError(f"WhatsApp template '{t.get('name')}' is for {str(t.get('entityType')).upper()}s, "
                         f"not {entity}s.")
    if t.get("status") != "APPROVED":
        raise ValueError(f"WhatsApp template '{t.get('name')}' is {t.get('status')}, not APPROVED.")


async def _utm_campaign_taken(value: str, activity_id: Optional[int] = None) -> bool:
    """Whether another activity already uses this utm_campaign (it must be unique per tenant).

    activity_id excludes that activity itself, for updates.
    """
    params: Dict[str, Any] = {"fieldName": "utmCampaign", "value": value}
    if activity_id:
        params["id"] = activity_id
    data = await _campaign_call("GET", "/v1/campaigns/activities/has-duplicates", "Check UTM campaign",
                                params=params)
    return bool((data or {}).get("duplicate"))


def _entity_arg(payload: Dict[str, Any]) -> str:
    entity = str(payload.get("entity") or "").strip().upper()
    if entity not in ACTIVITY_ENTITIES:
        raise ValueError(f"'entity' must be one of {ACTIVITY_ENTITIES} — who the activity targets.")
    return entity


async def currencies_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Active currencies for a campaign / activity budget."""
    return {"currencies": await _active_currencies()}


async def email_templates_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Email templates for LEAD or CONTACT activities, optionally narrowed by name (max 10 rows)."""
    data = await _kylas_get("/v1/email-templates/lookup", "Look up email templates",
                            params={"category": _entity_arg(payload).lower(), "q": str(payload.get("query") or "")})
    return {"templates": [{"id": r.get("id"), "name": r.get("name")} for r in _lookup_rows(data)]}


async def whatsapp_accounts_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Connected WhatsApp accounts, optionally narrowed by name (max 10 rows)."""
    data = await _kylas_get("/v1/messages/connected-accounts/lookup", "Look up WhatsApp accounts",
                            params={"q": str(payload.get("query") or "")})
    return {"accounts": [{"id": r.get("id"), "name": r.get("name"), "business_name": r.get("businessName")}
                         for r in _lookup_rows(data)]}


async def whatsapp_templates_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Approved WhatsApp templates of one account for LEAD or CONTACT, optionally narrowed by name (max 10 rows)."""
    entity = _entity_arg(payload)
    if payload.get("whatsapp_account") in (None, ""):
        raise ValueError("'whatsapp_account' is required (its name or id, from campaign_activity.whatsapp_accounts).")
    account, choice = await _resolve_whatsapp_account(payload["whatsapp_account"])
    if choice:
        return choice
    data = await _kylas_get("/v1/messages/whatsapp-templates/lookup", "Look up WhatsApp templates",
                            params={"connectedAccountId": account["id"], "entityType": entity.lower(),
                                    "q": str(payload.get("query") or "")})
    return {"account": account, "templates": [{"id": r.get("id"), "name": r.get("name")} for r in _lookup_rows(data)]}


async def check_utm_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Whether a utm_campaign is free to use."""
    value = str(payload.get("utm_campaign") or "").strip()
    if not value:
        raise ValueError("'utm_campaign' is required.")
    taken = await _utm_campaign_taken(value, _optional_id(payload, "activity_id"))
    return {"utm_campaign": value, "available": not taken}


# ---------------------------------------------------------------------------
# Confirmation — every create / update is two calls (plan W7). The first (no
# confirm) validates and builds the exact request body but writes nothing; it
# returns a preview. Only a second call with the same details and
# confirm: true writes — and it re-runs every check before writing. The code
# enforces this; it doesn't rely on the model.
# ---------------------------------------------------------------------------


async def _acting_user() -> Dict[str, Any]:
    """Who a write runs as — shown in every preview."""
    u = await _fetch_current_user()
    name = u.get("name") or f"{u.get('firstName') or ''} {u.get('lastName') or ''}".strip() or None
    return {"id": u.get("id"), "name": name, "tenant_id": u.get("tenantId"),
            "timezone": u.get("timezone") or DEFAULT_TIMEZONE}


def _preview_reply(op: str, details: Dict[str, Any], user: Dict[str, Any], what: str) -> Dict[str, Any]:
    return {
        "executed": False,
        "preview": details,
        "runs_as": {"user": user["name"], "user_id": user["id"], "tenant_id": user["tenant_id"]},
        "note": f"NOTHING WAS WRITTEN — this is a preview. Show the user every detail above and that it runs as "
                f"{user['name']}, then ask a Yes / No question to {what} (clickable choices if your app offers "
                f"them). Yes: call {op} again with exactly the same details plus confirm: true. No: stop — make "
                "no other call. If the user wants changes, call again without confirm for a new preview. Never "
                "say it was done until a call returns executed: true.",
    }


# ---------------------------------------------------------------------------
# campaign.create / campaign.update
# ---------------------------------------------------------------------------

_TEXT_FIELDS = ("name", "description", "utm_campaign", "utm_source", "utm_medium", "utm_content", "utm_term")
_REQUIRED_ON_CREATE = ("name", "utm_campaign", "utm_source", "utm_medium")
_CAMPAIGN_CHANGEABLE = _TEXT_FIELDS + ("budget", "start_date", "end_date")
# A started campaign: sd-campaign applies only these and silently ignores the rest
# (Campaign.applyCampaignStandardUpdates), so anything else is refused up front.
_STARTED_CHANGEABLE = {"budget", "end_date"}


def _text_value(payload: Dict[str, Any], key: str, required: bool) -> Optional[str]:
    value = payload.get(key)
    text = str(value).strip() if value is not None else ""
    if required and not text:
        raise ValueError(f"'{key}' is required.")
    if len(text) > 255 and key != "description":
        raise ValueError(f"'{key}' can be at most 255 characters.")
    return text or None


async def _budget_body(value: Any, default_currency_id: Optional[int]) -> Dict[str, Any]:
    """budget as a number or {value, currency} -> {currencyId, value}. No currency: default_currency_id, else base."""
    if isinstance(value, dict):
        amount, currency = value.get("value"), value.get("currency")
    else:
        amount, currency = value, None
    if amount in (None, ""):
        raise ValueError("'budget' needs a value, e.g. {\"value\": 50000, \"currency\": \"INR\"}.")
    number = _number_value(amount, "double", "budget", "value")
    if number < 0:
        raise ValueError("'budget' can't be negative.")
    currency_id = await _resolve_currency(currency) if currency not in (None, "") else (
        default_currency_id or await _resolve_currency(None))
    return {"currencyId": currency_id, "value": number}


def _instant(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _local_text(value: Optional[str], tz_name: str) -> Optional[str]:
    instant = _instant(value)
    return f"{instant.astimezone(ZoneInfo(tz_name)):%d %b %Y, %I:%M:%S %p} ({tz_name})" if instant else None


def _campaign_details(body: Dict[str, Any], currencies: Dict[int, str], tz_name: str) -> Dict[str, Any]:
    """A campaign body as the readable details the user confirms."""
    budget = body.get("estimatedBudget") or {}
    return {
        "name": body.get("name"),
        "description": body.get("description"),
        "budget": {"value": budget.get("value"), "currency": currencies.get(budget.get("currencyId")),
                   "currency_id": budget.get("currencyId")},
        "start_date": _local_text(body.get("startDate"), tz_name),
        "end_date": _local_text(body.get("endDate"), tz_name) or "none",
        "utm": {k: body.get(f"utm{k.title()}") for k in ("campaign", "source", "medium", "content", "term")},
    }


async def _campaign_create_body(payload: Dict[str, Any], tz_name: str) -> Dict[str, Any]:
    texts = {k: _text_value(payload, k, k in _REQUIRED_ON_CREATE) for k in _TEXT_FIELDS}
    if payload.get("budget") in (None, ""):
        raise ValueError("'budget' is required, e.g. {\"value\": 50000, \"currency\": \"INR\"} (currency "
                         "defaults to the tenant's base currency).")
    if payload.get("start_date") in (None, ""):
        raise ValueError("'start_date' is required (the user's local date, YYYY-MM-DD).")
    start = _write_date(payload["start_date"], tz_name, False, "start_date")
    end = _write_date(payload["end_date"], tz_name, True, "end_date") if payload.get("end_date") else None
    _check_date_order(start, end)
    budget = await _budget_body(payload["budget"], None)
    body = {_CAMPAIGN_FIELD_MAP[k]: v for k, v in texts.items()}
    body.update({"startDate": start, "endDate": end, "estimatedBudget": budget,
                 # sd-campaign sets the same itself; sent because the web form does.
                 "actualExpense": {"currencyId": budget["currencyId"], "value": 0}})
    return body


async def create_campaign_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST /v1/campaigns — in two calls: preview (nothing written), then the same details with confirm: true."""
    user = await _acting_user()
    tz_name = await _resolve_timezone(payload.get("timezone") or user["timezone"])
    body = await _campaign_create_body(payload, tz_name)
    if payload.get("confirm") is True:
        created = await _campaign_call("POST", "/v1/campaigns", "Create campaign", body=body)
        return {"executed": True, "campaign": await get_campaign_logic({"campaign_id": created["id"]})}
    details = {**_campaign_details(body, await _currency_names(), tz_name), "status": "DRAFT (nothing is sent)"}
    return _preview_reply("campaign.create", details, user, "create this campaign")


def _same(a: Any, b: Any) -> bool:
    return (a or None) == (b or None)


async def _campaign_update_body(
    payload: Dict[str, Any], current: Dict[str, Any], tz_name: str,
) -> Tuple[Dict[str, Any], Dict[str, Tuple[Any, Any]]]:
    """(full PUT body, {field: (before, after)}) for the fields the user changes — validated like sd-campaign."""
    asked = [k for k in _CAMPAIGN_CHANGEABLE if k in payload]
    if not asked:
        raise ValueError(f"Nothing to change. Give any of: {', '.join(_CAMPAIGN_CHANGEABLE)}.")
    status = current.get("status")
    if status != "DRAFT" and set(asked) - _STARTED_CHANGEABLE:
        raise ValueError(f"This campaign is {status}: only budget and end_date can change now (Kylas ignores the "
                         f"rest once a campaign has started). Not allowed: {sorted(set(asked) - _STARTED_CHANGEABLE)}.")

    changes: Dict[str, Any] = {}
    for key in asked:
        if key in _TEXT_FIELDS:
            changes[key] = _text_value(payload, key, key in _REQUIRED_ON_CREATE)
        elif key == "start_date":
            changes[key] = _write_date(payload[key], tz_name, False, key)
        elif key == "end_date":  # empty clears it
            changes[key] = _write_date(payload[key], tz_name, True, key) if payload.get(key) else None
    body = merge_for_update(current, changes, _CAMPAIGN_FIELD_MAP, _CAMPAIGN_BODY_FIELDS)
    if "budget" in asked:
        body["estimatedBudget"] = await _budget_body(
            payload["budget"], (current.get("estimatedBudget") or {}).get("currencyId"))

    # The same checks sd-campaign runs on update (Campaign.validateUpdateProperties).
    _check_date_order(body["startDate"], body.get("endDate"))
    start, end = _instant(body["startDate"]), _instant(body.get("endDate"))
    for a in current.get("activities") or []:
        for label, field in (("starts", "startDate"), ("ends", "endDate")):
            when = _instant(a.get(field))
            if when and (when < start or (end and when > end)):
                raise ValueError(f"Activity '{a.get('name')}' {label} on {_local_text(a.get(field), tz_name)}, "
                                 "outside the new campaign dates. Change that activity first, or keep its dates "
                                 "inside the campaign.")
    activity_budget = sum(((a.get("estimatedBudget") or {}).get("value") or 0) for a in current.get("activities") or [])
    if activity_budget > body["estimatedBudget"]["value"]:
        raise ValueError(f"The activities' budgets add up to {activity_budget:g}, more than the new campaign "
                         f"budget {body['estimatedBudget']['value']:g}.")

    api_names = {**_CAMPAIGN_FIELD_MAP, "budget": "estimatedBudget"}
    diff = {k: (current.get(api_names[k]), body.get(api_names[k])) for k in asked
            if not _same(current.get(api_names[k]), body.get(api_names[k]))}
    if not diff:
        raise ValueError("Those values are already set — nothing would change.")
    return body, diff


async def update_campaign_logic(payload: Dict[str, Any]) -> Dict[str, Any]:
    """PUT /v1/campaigns/{id} (full body, read-merge-write) — in two calls: preview, then confirm: true.

    Both calls read the campaign fresh and run every check, so the confirm
    writes the user's changes on top of its current values.
    """
    user = await _acting_user()
    tz_name = await _resolve_timezone(payload.get("timezone") or user["timezone"])
    campaign_id, choice = await _resolve_campaign_id(payload)
    if choice:
        return choice
    current = await _campaign_call("GET", f"/v1/campaigns/{campaign_id}", "Fetch campaign")
    body, diff = await _campaign_update_body(payload, current, tz_name)
    if payload.get("confirm") is True:
        await _campaign_call("PUT", f"/v1/campaigns/{campaign_id}", "Update campaign", body=body)
        return {"executed": True, "campaign": await get_campaign_logic({"campaign_id": campaign_id})}

    currencies = await _currency_names()

    def shown(field: str, value: Any) -> Any:
        if field == "budget":
            return {"value": (value or {}).get("value"), "currency": currencies.get((value or {}).get("currencyId"))}
        return _local_text(value, tz_name) or "none" if field in ("start_date", "end_date") else value

    details = {
        "campaign": {"id": campaign_id, "name": current.get("name"), "status": current.get("status")},
        "changes": {k: {"before": shown(k, before), "after": shown(k, after)} for k, (before, after) in diff.items()},
        "after_update": _campaign_details(body, currencies, tz_name),
    }
    return _preview_reply("campaign.update", details, user,
                          f"apply these changes to campaign '{current.get('name')}'")


# ---------------------------------------------------------------------------
# Input schemas (what build_payload returns for each id)
# ---------------------------------------------------------------------------

_ALL_FILTER_OPERATORS = sorted(set(_ID_OPS + _TEXT_OPS + _NUMBER_OPS + _DATE_OPS))


def _filters_schema(fields: Iterable[str]) -> Dict[str, Any]:
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


def _paging_properties(sort_description: str) -> Dict[str, Any]:
    return {
        "page": {"type": "integer", "minimum": 0, "default": 0, "description": "0-based page."},
        "size": {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_SIZE, "default": 20},
        "sort": {"type": "string",
                 "description": f"'<field>,<asc|desc>' (direction defaults to asc). {sort_description}"},
    }


_PAGING_PROPERTIES = _paging_properties(
    f"Fields: {', '.join(CAMPAIGN_SORT_FIELDS)}. Leave it out for newest-updated first.")

_CAMPAIGN_ID_PROPERTY = {"campaign_id": {"type": "integer", "description": "Campaign id (from campaign.search)."}}

_ACTIVITY_REF_PROPERTIES = {
    "activity_id": {"type": "integer", "description": "Activity id. Give this or activity_name."},
    "activity_name": {"type": "string",
                      "description": "Activity name or part of it, when the id isn't known. Several matches "
                                     "return a list to ask the user about."},
    "campaign_id": {"type": "integer",
                    "description": "Optional. The activity's campaign; found automatically when left out."},
}

_ANALYTICS_DATE_PROPERTIES = {
    "date_from": {"type": "string", "format": "date", "description": "Local date YYYY-MM-DD."},
    "date_to": {"type": "string", "format": "date", "description": "Local date YYYY-MM-DD, inclusive."},
    "timezone": {"type": "string", "description": "IANA timezone; defaults to the current user's."},
}

_RECIPIENT_FILTER_FIELD_NAMES = sorted({f for fields in RECIPIENT_FILTER_FIELDS.values() for f in fields})

_CAMPAIGN_WRITE_PROPERTIES = {
    "name": {"type": "string", "maxLength": 255},
    "description": {"type": "string"},
    "budget": {"type": "object", "description": "Estimated budget.",
               "properties": {"value": {"type": "number", "minimum": 0},
                              "currency": {"type": ["string", "integer"],
                                           "description": "Code (INR) or currency_id; defaults to the base currency "
                                                          "(create) or the current one (update)."}}},
    "start_date": {"type": "string", "description": "User's local date YYYY-MM-DD (start of that day) or datetime."},
    "end_date": {"type": ["string", "null"],
                 "description": "User's local date YYYY-MM-DD (end of that day) or datetime; empty clears it."},
    "timezone": {"type": "string", "description": "IANA timezone of the dates; defaults to the current user's."},
    **{f"utm_{k}": {"type": "string", "maxLength": 255} for k in ("campaign", "source", "medium", "content", "term")},
}

_CONFIRM_PROPERTIES = {
    "confirm": {"type": "boolean",
                "description": "Leave out for the preview. true only after the user said Yes to it, with the same details."},
}

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
            **_ANALYTICS_DATE_PROPERTIES,
        },
        "required": ["campaign_id"],
    },
    "campaign_activity.search": {
        "type": "object",
        "properties": {
            "filters": _filters_schema(ACTIVITY_FILTER_FIELDS),
            "search_term": {"type": "string", "description": "Free text matched against the activity name."},
            **_paging_properties(f"Fields: {', '.join(ACTIVITY_SORT_FIELDS)}. Leave it out for newest-updated first."),
        },
    },
    "campaign_activity.get": {
        "type": "object",
        "properties": dict(_ACTIVITY_REF_PROPERTIES),
    },
    "campaign_activity.analytics": {
        "type": "object",
        "properties": {
            **_ACTIVITY_REF_PROPERTIES,
            "group_by": {"type": "string",
                         "enum": sorted({g for groups in ACTIVITY_GROUP_BY.values() for g in groups}),
                         "description": "EMAIL/WHATSAPP: ACTIVITY_STATUS (funnel, default) or DAY. "
                                        "CALL: DAY_OUTCOME (default) or CALL_DISPOSITION."},
            **_ANALYTICS_DATE_PROPERTIES,
            "agent_ids": {"type": "array", "items": {"type": "integer"},
                          "description": "CALL only: limit to these agents' calls (user ids)."},
        },
    },
    "campaign_activity.recipients": {
        "type": "object",
        "properties": {
            **_ACTIVITY_REF_PROPERTIES,
            "filters": _filters_schema(_RECIPIENT_FILTER_FIELD_NAMES),
            **_paging_properties("Fields: EMAIL sentAt, openedAt, clickedAt, failedAt; WHATSAPP sentAt, "
                                 "deliveredAt, readAt, repliedAt, failedAt; CALL callCreatedAt. "
                                 "Leave it out for the newest first."),
        },
    },
    "campaign_activity.generated_records": {
        "type": "object",
        "properties": {
            **_ACTIVITY_REF_PROPERTIES,
            **_paging_properties("Field: createdAt. Leave it out for the newest first."),
        },
    },
    "campaign.currencies": {"type": "object", "properties": {}},
    "campaign.create": {"type": "object", "properties": {**_CAMPAIGN_WRITE_PROPERTIES, **_CONFIRM_PROPERTIES}},
    "campaign.update": {
        "type": "object",
        "properties": {
            "campaign_id": {"type": "integer", "description": "The campaign's id. Give this or campaign_name."},
            "campaign_name": {"type": "string",
                              "description": "The campaign's name (or part of it); several matches return a choice."},
            **_CAMPAIGN_WRITE_PROPERTIES,
            **_CONFIRM_PROPERTIES,
        },
    },
    "campaign_activity.email_templates": {
        "type": "object",
        "properties": {
            "entity": {"type": "string", "enum": ACTIVITY_ENTITIES, "description": "Who the activity targets."},
            "query": {"type": "string", "description": "Part of the template name; leave out to list the first 10."},
        },
        "required": ["entity"],
    },
    "campaign_activity.whatsapp_accounts": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Part of the account name; leave out to list them."},
        },
    },
    "campaign_activity.whatsapp_templates": {
        "type": "object",
        "properties": {
            "whatsapp_account": {"type": ["string", "integer"],
                                 "description": "The WhatsApp account's name or id."},
            "entity": {"type": "string", "enum": ACTIVITY_ENTITIES, "description": "Who the activity targets."},
            "query": {"type": "string", "description": "Part of the template name; leave out to list the first 10."},
        },
        "required": ["whatsapp_account", "entity"],
    },
    "campaign_activity.check_utm": {
        "type": "object",
        "properties": {
            "utm_campaign": {"type": "string"},
            "activity_id": {"type": "integer",
                            "description": "When updating an activity: its id, so it doesn't count itself."},
        },
        "required": ["utm_campaign"],
    },
}

CAMPAIGN_ROUTERS: Dict[str, CampaignRouter] = {
    "campaign.search": search_campaigns_logic,
    "campaign.get": get_campaign_logic,
    "campaign.analytics": campaign_analytics_logic,
    "campaign_activity.search": search_activities_logic,
    "campaign_activity.get": get_activity_logic,
    "campaign_activity.analytics": activity_analytics_logic,
    "campaign_activity.recipients": activity_recipients_logic,
    "campaign_activity.generated_records": activity_generated_records_logic,
    "campaign.currencies": currencies_logic,
    "campaign.create": create_campaign_logic,
    "campaign.update": update_campaign_logic,
    "campaign_activity.email_templates": email_templates_logic,
    "campaign_activity.whatsapp_accounts": whatsapp_accounts_logic,
    "campaign_activity.whatsapp_templates": whatsapp_templates_logic,
    "campaign_activity.check_utm": check_utm_logic,
}


def unwired_campaign_ids(registry: Dict[str, Dict[str, Any]]) -> List[str]:
    """Campaign-bucket registry ids missing a schema or a router (checked at server startup)."""
    return sorted(
        entry_id
        for entry_id, entry in registry.items()
        if entry.get("bucket") in CAMPAIGN_BUCKETS
        and (entry_id not in CAMPAIGN_SCHEMAS or entry_id not in CAMPAIGN_ROUTERS)
    )
