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

from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from app.client import KylasAPIError, get_client, handle_api_response

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


def build_campaign_json_rule(
    filters: Optional[List[Dict[str, Any]]],
    allowed: Dict[str, FilterSpec],
) -> Dict[str, Any]:
    """Validate [{field, operator, value}] and return the {"jsonRule": ...} body sd-campaign search expects.

    Raises ValueError (surfaced by execute_request as BAD_PAYLOAD) naming the
    filter and the allowed values, instead of letting sd-campaign answer 403.
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
# Registry wiring — filled per operation (Plan 1 phases R1/R2)
# ---------------------------------------------------------------------------

CampaignRouter = Callable[[Dict[str, Any]], Awaitable[Any]]

CAMPAIGN_SCHEMAS: Dict[str, Dict[str, Any]] = {}

CAMPAIGN_ROUTERS: Dict[str, CampaignRouter] = {}


def unwired_campaign_ids(registry: Dict[str, Dict[str, Any]]) -> List[str]:
    """Campaign-bucket registry ids missing a schema or a router (checked at server startup)."""
    return sorted(
        entry_id
        for entry_id, entry in registry.items()
        if entry.get("bucket") in CAMPAIGN_BUCKETS
        and (entry_id not in CAMPAIGN_SCHEMAS or entry_id not in CAMPAIGN_ROUTERS)
    )
