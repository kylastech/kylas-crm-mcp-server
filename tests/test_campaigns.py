"""Tests for app/campaigns.py and its registry wiring (Plan 1, phase R0).

Request shapes asserted here were verified against sd-campaign on QA:
campaign/activity search bodies, the analytics body sd-ui sends, and the
empty-body 403 the service returns for invalid filter values.
"""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.campaigns import (
    ACTIVITY_FILTER_FIELDS,
    CAMPAIGN_FILTER_FIELDS,
    CAMPAIGN_ROUTERS,
    CAMPAIGN_SCHEMAS,
    _campaign_call,
    analytics_body,
    build_campaign_json_rule,
    unwired_campaign_ids,
)
from app.client import KylasAPIError

_FAKE_ENTRY = {
    "id": "campaign.fake",
    "bucket": "campaign",
    "intent": "search",
    "method": "POST",
    "path": "/v1/campaigns/search",
    "description": "test-only entry",
}


def _mock_client(response: httpx.Response) -> AsyncMock:
    client = AsyncMock()
    client.get.return_value = response
    client.post.return_value = response
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    return client


def _response(status: int, text: str = "") -> httpx.Response:
    return httpx.Response(status, text=text, request=httpx.Request("POST", "https://api.test/v1/campaigns/search"))


# ---------------------------------------------------------------------------
# build_campaign_json_rule
# ---------------------------------------------------------------------------


def test_rule_builder_builds_sd_campaign_shape():
    body = build_campaign_json_rule(
        [{"field": "status", "operator": "equal", "value": "IN_PROGRESS"}], CAMPAIGN_FILTER_FIELDS
    )
    assert body == {"jsonRule": {"condition": "AND", "valid": True, "rules": [
        {"id": "status", "field": "status", "type": "string", "operator": "equal", "value": "IN_PROGRESS"},
    ]}}


def test_rule_builder_uppercases_enum_values():
    body = build_campaign_json_rule(
        [{"field": "status", "operator": "in", "value": ["draft", "Completed"]},
         {"field": "type", "operator": "equal", "value": "whatsapp"}],
        ACTIVITY_FILTER_FIELDS,
    )
    rules = body["jsonRule"]["rules"]
    assert rules[0]["value"] == ["DRAFT", "COMPLETED"]
    assert rules[1]["value"] == "WHATSAPP"


def test_rule_builder_defaults_operator_to_equal():
    rule = build_campaign_json_rule([{"field": "id", "value": 433}], CAMPAIGN_FILTER_FIELDS)["jsonRule"]["rules"][0]
    assert rule["operator"] == "equal"
    assert rule["type"] == "long"


def test_rule_builder_empty_filters():
    assert build_campaign_json_rule(None, CAMPAIGN_FILTER_FIELDS)["jsonRule"]["rules"] == []


def test_rule_builder_drops_value_for_null_checks():
    rule = build_campaign_json_rule(
        [{"field": "startedAt", "operator": "is_null", "value": "ignored"}], CAMPAIGN_FILTER_FIELDS
    )["jsonRule"]["rules"][0]
    assert rule["value"] is None


@pytest.mark.parametrize("field", ["campaign", "campaignId", "campaign_id"])
def test_rule_builder_rejects_campaign_filter_on_activities(field):
    with pytest.raises(ValueError, match="campaign.get"):
        build_campaign_json_rule([{"field": field, "operator": "equal", "value": 433}], ACTIVITY_FILTER_FIELDS)


def test_rule_builder_rejects_unknown_field():
    with pytest.raises(ValueError, match="'utmSource' is not filterable"):
        build_campaign_json_rule([{"field": "utmSource", "value": "x"}], CAMPAIGN_FILTER_FIELDS)


def test_rule_builder_rejects_operator_not_allowed_for_field():
    with pytest.raises(ValueError, match="operator 'contains' isn't allowed on 'status'"):
        build_campaign_json_rule([{"field": "status", "operator": "contains", "value": "DRAFT"}], CAMPAIGN_FILTER_FIELDS)


def test_rule_builder_rejects_invalid_enum_value():
    with pytest.raises(ValueError, match="RUNNING"):
        build_campaign_json_rule([{"field": "status", "value": "running"}], CAMPAIGN_FILTER_FIELDS)


def test_rule_builder_rejects_activity_only_field_on_campaigns():
    with pytest.raises(ValueError, match="'type' is not filterable"):
        build_campaign_json_rule([{"field": "type", "value": "EMAIL"}], CAMPAIGN_FILTER_FIELDS)


@pytest.mark.parametrize("operator,value,message", [
    ("in", "DRAFT", "non-empty list"),
    ("in", [], "non-empty list"),
    ("between", [1], r"\[from, to\]"),
    ("equal", None, "single value"),
    ("equal", [1, 2], "single value"),
])
def test_rule_builder_checks_value_shape(operator, value, message):
    field = "estimatedBudget" if operator == "between" else "id"
    with pytest.raises(ValueError, match=message):
        build_campaign_json_rule([{"field": field, "operator": operator, "value": value}], CAMPAIGN_FILTER_FIELDS)


# ---------------------------------------------------------------------------
# analytics_body
# ---------------------------------------------------------------------------


def test_analytics_body_matches_sd_ui_shape():
    assert analytics_body(["DAY", "ACTIVITIES"], "2026-10-25T18:30:00.000Z", "2026-11-01T18:29:59.999Z", "Asia/Calcutta") == {
        "timeZone": "Asia/Calcutta",
        "groupBy": ["DAY", "ACTIVITIES"],
        "metric": {"type": "COUNT"},
        "dateRange": ["2026-10-25T18:30:00.000Z", "2026-11-01T18:29:59.999Z"],
    }


# ---------------------------------------------------------------------------
# _campaign_call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_campaign_call_returns_json_on_success():
    client = _mock_client(_response(200, '{"totalElements": 9}'))
    with patch("app.campaigns.get_client", return_value=client):
        data = await _campaign_call("POST", "/v1/campaigns/search", "Search campaigns",
                                    params={"page": 0}, body={"jsonRule": None})
    assert data == {"totalElements": 9}
    client.post.assert_called_once_with("/v1/campaigns/search", json={"jsonRule": None}, params={"page": 0})


@pytest.mark.asyncio
async def test_campaign_call_post_without_body_sends_empty_object():
    client = _mock_client(_response(200, "{}"))
    with patch("app.campaigns.get_client", return_value=client):
        await _campaign_call("POST", "/v1/campaigns/search", "Search campaigns")
    client.post.assert_called_once_with("/v1/campaigns/search", json={})


@pytest.mark.asyncio
async def test_campaign_call_translates_empty_403():
    client = _mock_client(_response(403, ""))
    with patch("app.campaigns.get_client", return_value=client):
        with pytest.raises(KylasAPIError) as exc:
            await _campaign_call("POST", "/v1/campaigns/433/analytics", "Campaign analytics")
    assert exc.value.status_code == 403
    assert "Campaign analytics" in exc.value.message
    assert "invalid filter field, enum value or group_by" in exc.value.message


@pytest.mark.asyncio
async def test_campaign_call_keeps_403_with_body_as_permission_error():
    body = '{"code":"01601005","message":"Insufficient privileges"}'
    client = _mock_client(_response(403, body))
    with patch("app.campaigns.get_client", return_value=client):
        with pytest.raises(KylasAPIError) as exc:
            await _campaign_call("GET", "/v1/campaigns/433", "Fetch campaign")
    assert exc.value.status_code == 403
    assert exc.value.response_body == body
    assert "group_by" not in exc.value.message


@pytest.mark.asyncio
async def test_campaign_call_rejects_unsupported_method():
    with pytest.raises(ValueError, match="Unsupported method"):
        await _campaign_call("DELETE", "/v1/campaigns/1", "Delete campaign")


# ---------------------------------------------------------------------------
# Registry wiring
# ---------------------------------------------------------------------------


def test_index_declares_campaign_buckets_and_analytics_intent():
    from app.tools import _REGISTRY_INDEX

    assert {"campaign", "campaign_activity"} <= set(_REGISTRY_INDEX["buckets"])
    assert "analytics" in _REGISTRY_INDEX["intents"]


def test_unwired_campaign_ids_flags_missing_schema_or_router():
    registry = {"campaign.fake": _FAKE_ENTRY, "lead.get": {"bucket": "lead"}}
    assert unwired_campaign_ids(registry) == ["campaign.fake"]

    with patch.dict(CAMPAIGN_SCHEMAS, {"campaign.fake": {}}):
        assert unwired_campaign_ids(registry) == ["campaign.fake"]
        with patch.dict(CAMPAIGN_ROUTERS, {"campaign.fake": AsyncMock()}):
            assert unwired_campaign_ids(registry) == []


def test_every_registered_campaign_id_is_wired():
    from app.tools import _REGISTRY

    assert unwired_campaign_ids(_REGISTRY) == []


@pytest.mark.asyncio
async def test_build_payload_uses_campaign_schema():
    from app.tools import build_payload

    schema = {"type": "object", "properties": {"campaign_id": {"type": "integer"}}, "required": ["campaign_id"]}
    with patch.dict("app.tools._REGISTRY", {"campaign.fake": _FAKE_ENTRY}), \
            patch.dict(CAMPAIGN_SCHEMAS, {"campaign.fake": schema}):
        result = json.loads(await build_payload("campaign.fake"))
    assert result["schema"] == schema
    assert result["path"] == "/v1/campaigns/search"
    assert "filters" not in result["schema"]["properties"]  # not the generic search_entity schema


@pytest.mark.asyncio
async def test_execute_request_routes_campaign_ids_to_campaign_router():
    from app.tools import execute_request

    router = AsyncMock(return_value={"total": 9})
    with patch.dict("app.tools._REGISTRY", {"campaign.fake": _FAKE_ENTRY}), \
            patch.dict(CAMPAIGN_ROUTERS, {"campaign.fake": router}), \
            patch("app.tools.search_entity_logic", new_callable=AsyncMock) as generic_search:
        result = json.loads(await execute_request("campaign.fake", {"size": 5}))
    assert result == {"ok": True, "status": 200, "data": {"total": 9}}
    router.assert_awaited_once_with({"size": 5})
    generic_search.assert_not_called()


@pytest.mark.asyncio
async def test_execute_request_campaign_id_without_router_is_not_wired():
    from app.tools import execute_request

    with patch.dict("app.tools._REGISTRY", {"campaign.fake": _FAKE_ENTRY}):
        result = json.loads(await execute_request("campaign.fake", {}))
    assert result["ok"] is False
    assert result["error"]["code"] == "NOT_WIRED"


@pytest.mark.asyncio
async def test_execute_request_campaign_validation_error_is_bad_payload():
    from app.tools import execute_request

    async def router(payload):
        build_campaign_json_rule(payload["filters"], CAMPAIGN_FILTER_FIELDS)

    with patch.dict("app.tools._REGISTRY", {"campaign.fake": _FAKE_ENTRY}), \
            patch.dict(CAMPAIGN_ROUTERS, {"campaign.fake": router}):
        result = json.loads(await execute_request("campaign.fake", {"filters": [{"field": "nope", "value": 1}]}))
    assert result["ok"] is False
    assert result["error"]["code"] == "BAD_PAYLOAD"
    assert "'nope' is not filterable" in result["error"]["message"]


@pytest.mark.asyncio
async def test_execute_request_campaign_empty_403_surfaces_readable_error():
    from app.tools import execute_request

    async def router(payload):
        return await _campaign_call("POST", "/v1/campaigns/search", "Search campaigns", body={})

    client = _mock_client(_response(403, ""))
    with patch.dict("app.tools._REGISTRY", {"campaign.fake": _FAKE_ENTRY}), \
            patch.dict(CAMPAIGN_ROUTERS, {"campaign.fake": router}), \
            patch("app.campaigns.get_client", return_value=client):
        result = json.loads(await execute_request("campaign.fake", {}))
    assert result["ok"] is False
    assert result["error"]["code"] == "403"
    assert "Search campaigns was rejected" in result["error"]["message"]


# ---------------------------------------------------------------------------
# Phase R1 — campaign.search / campaign.get / campaign.analytics
#
# Fixtures are trimmed copies of real QA responses (campaign 433; its
# engagement on 13 Oct 2026 IST split between activities 713 and 716).
# ---------------------------------------------------------------------------

from app.campaigns import (  # noqa: E402
    _resolve_date_range,
    campaign_analytics_logic,
    get_campaign_logic,
    search_campaigns_logic,
)

_CURRENCIES = [
    {"id": 2980, "currencyValueId": 431, "name": "INR", "active": True, "baseCurrency": True},
    {"id": 2976, "currencyValueId": 400, "name": "USD", "active": True, "baseCurrency": False},
]

_ACTIVITY_713 = {
    "id": 713, "name": "Abhinav Whatsapp Reply", "status": "COMPLETED", "type": "WHATSAPP", "entity": "LEAD",
    "campaign": {"id": 433, "name": "Abhinav Whatsapp Replied Campaign"},
    "estimatedBudget": {"currencyId": 431, "value": 1000.0}, "actualExpense": {"currencyId": 431, "value": 0.0},
    "startDate": "2026-10-13T05:30:00.000Z", "endDate": None, "startedAt": "2026-10-13T06:24:00.000Z",
    "endedAt": None, "processedAt": None, "utmCampaign": "1123", "utmSource": "wa", "utmMedium": "msg",
    "utmContent": None, "utmTerm": None,
    "filters": {"jsonRule": {"condition": "AND", "valid": True, "rules": [
        {"id": "id", "field": "id", "type": "long", "operator": "in", "value": [753407, 753405]}]}},
    "payload": {"type": "WHATSAPP", "sentTo": "PRIMARY_PHONE_NUMBER",
                "connectedAccount": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"},
                "whatsappTemplate": {"id": 853, "name": "Diwali Special Offer from Kylas!"},
                "retryConfig": {"noOfTimes": 2}},
    "bulkJobId": 99, "totalEngagement": 4, "recordsGenerated": 0, "clickThroughRate": None,
    "recordActions": {"read": True},
}

_CAMPAIGN_433 = {
    "id": 433, "name": "Abhinav Whatsapp Replied Campaign", "status": "IN_PROGRESS", "description": None,
    "estimatedBudget": {"currencyId": 431, "value": 100000.0}, "actualExpense": {"currencyId": 431, "value": 790.0},
    "startDate": "2026-10-13T00:00:00.000Z", "endDate": None, "startedAt": "2026-10-13T06:20:00.000Z",
    "endedAt": None, "utmSource": "wa", "utmCampaign": "abhinav", "utmMedium": "msg", "utmContent": None,
    "utmTerm": None, "startedBy": {"id": 8112, "name": "Akash Nakhate"}, "endedBy": None,
    "createdBy": {"id": 8112, "name": "Akash Nakhate"}, "updatedBy": {"id": 8112, "name": "Akash Nakhate"},
    "activities": [_ACTIVITY_713], "createdAt": "2026-10-13T06:00:00.000Z", "updatedAt": "2026-10-20T06:00:00.000Z",
    "totalEngagement": 8, "recordsGenerated": 0, "channels": [], "recordActions": {"read": True},
}


def _days(*names_and_values):
    return [{"id": None, "name": n, "dimension": [], "value": v} for n, v in names_and_values]


class _FakeCampaignApi:
    """Stands in for _campaign_call: routes by path and records every request."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def __call__(self, method, path, op, *, params=None, body=None):
        self.calls.append({"method": method, "path": path, "params": params, "body": body})
        response = self.responses[path]
        if isinstance(response, Exception):
            raise response
        return response

    def call_to(self, path):
        return next(c for c in self.calls if c["path"] == path)


@pytest.mark.asyncio
async def test_search_sends_filters_search_term_and_paging():
    api = _FakeCampaignApi({
        "/v1/campaigns/search": {"content": [_CAMPAIGN_433], "totalElements": 9, "last": False},
        "/v1/forex/currencies": _CURRENCIES,
    })
    with patch("app.campaigns._campaign_call", api):
        result = await search_campaigns_logic({
            "filters": [{"field": "status", "operator": "in", "value": ["in_progress", "paused"]}],
            "search_term": "Abhinav", "size": 5, "sort": "estimatedBudget,asc",
        })

    call = api.call_to("/v1/campaigns/search")
    assert call["params"] == {"page": 0, "size": 5, "sort": "estimatedBudget,asc"}
    assert call["body"]["jsonRule"]["rules"] == [
        {"id": "status", "field": "status", "type": "string", "operator": "in", "value": ["IN_PROGRESS", "PAUSED"]},
        {"id": "multi_field", "field": "multi_field", "type": "string", "operator": "multi_field", "value": "Abhinav"},
    ]
    assert result["total"] == 9 and result["has_more"] is True
    campaign = result["campaigns"][0]
    assert campaign["estimated_budget"] == {"value": 100000.0, "currency": "INR", "currency_id": 431}
    assert campaign["activities"] == [{"id": 713, "name": "Abhinav Whatsapp Reply", "type": "WHATSAPP", "status": "COMPLETED"}]
    assert "recordActions" not in campaign


@pytest.mark.asyncio
async def test_search_defaults_to_updated_desc_and_no_rules():
    api = _FakeCampaignApi({"/v1/campaigns/search": {"content": [], "totalElements": 0, "last": True},
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await search_campaigns_logic({})
    call = api.call_to("/v1/campaigns/search")
    assert call["params"] == {"page": 0, "size": 20, "sort": "updatedAt,desc"}
    assert call["body"]["jsonRule"]["rules"] == []
    assert result == {"total": 0, "page": 0, "size": 20, "has_more": False, "campaigns": []}


@pytest.mark.parametrize("payload,message", [
    ({"sort": "bogus,desc"}, "Can't sort by 'bogus'"),
    ({"sort": "name,sideways"}, "'asc' or 'desc'"),
    ({"size": 51}, "between 1 and 50"),
    ({"size": 0}, "between 1 and 50"),
    ({"page": -1}, "0 or more"),
    ({"page": "x"}, "must be integers"),
])
@pytest.mark.asyncio
async def test_search_rejects_bad_paging_before_calling_api(payload, message):
    api = _FakeCampaignApi({})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(ValueError, match=message):
            await search_campaigns_logic(payload)
    assert api.calls == []


@pytest.mark.asyncio
async def test_get_uses_overview_and_trims_activities():
    api = _FakeCampaignApi({"/v1/campaigns/433/overview": _CAMPAIGN_433, "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_campaign_logic({"campaign_id": "433"})

    assert api.call_to("/v1/campaigns/433/overview")["method"] == "GET"
    assert result["actual_expense"] == {"value": 790.0, "currency": "INR", "currency_id": 431}
    assert result["utm"] == {"campaign": "abhinav", "source": "wa", "medium": "msg", "content": None, "term": None}
    assert result["started_by"] == {"id": 8112, "name": "Akash Nakhate"}
    assert result["facebook_channels"] == 0

    activity = result["activities"][0]
    assert activity["content"] == {
        "sent_to": "PRIMARY_PHONE_NUMBER",
        "whatsapp_template": {"id": 853, "name": "Diwali Special Offer from Kylas!"},
        "whatsapp_account": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"},
        "retry_times": 2,
    }
    assert activity["audience_filters"] == [{"field": "id", "operator": "in", "value": [753407, 753405]}]
    assert activity["total_engagement"] == 4
    assert "bulkJobId" not in activity and "recordActions" not in activity
    assert "dialedCalls" not in activity  # call metrics only for CALL activities


@pytest.mark.asyncio
async def test_get_email_activity_content_summary():
    email_activity = {**_ACTIVITY_713, "type": "EMAIL", "payload": {
        "type": "EMAIL", "sentTo": "PRIMARY_EMAIL", "emailTemplate": {"id": 812, "name": "Lead creation"},
        "connectedEmailAccount": "qa.kylas@gmail.com"}}
    api = _FakeCampaignApi({"/v1/campaigns/442/overview": {**_CAMPAIGN_433, "id": 442, "activities": [email_activity]},
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_campaign_logic({"campaign_id": 442})
    assert result["activities"][0]["content"] == {
        "sent_to": "PRIMARY_EMAIL",
        "email_template": {"id": 812, "name": "Lead creation"},
        "sending_email_account": "qa.kylas@gmail.com",
    }


@pytest.mark.asyncio
async def test_get_still_works_when_currencies_fail():
    api = _FakeCampaignApi({"/v1/campaigns/433/overview": _CAMPAIGN_433,
                            "/v1/forex/currencies": KylasAPIError("Fetch tenant currencies failed: 500", 500)})
    with patch("app.campaigns._campaign_call", api):
        result = await get_campaign_logic({"campaign_id": 433})
    assert result["estimated_budget"] == {"value": 100000.0, "currency": None, "currency_id": 431}


@pytest.mark.parametrize("campaign_id", [None, "", 0, -3, "abc", True])
@pytest.mark.asyncio
async def test_get_requires_positive_campaign_id(campaign_id):
    api = _FakeCampaignApi({})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(ValueError, match="campaign_id"):
            await get_campaign_logic({"campaign_id": campaign_id})
    assert api.calls == []


@pytest.mark.asyncio
async def test_analytics_day_converts_local_dates_and_drops_empty_days():
    rows = _days(("2026-10-09T18:30Z", 0), ("2026-10-10T18:30Z", 0), ("2026-10-11T18:30Z", 0),
                 ("2026-10-12T18:30Z", 8), ("2026-10-13T18:30Z", 0), ("2026-10-14T18:30Z", 0),
                 ("2026-10-15T18:30Z", 0))
    api = _FakeCampaignApi({"/v1/campaigns/433/analytics": rows})
    with patch("app.campaigns._campaign_call", api):
        result = await campaign_analytics_logic({
            "campaign_id": 433, "date_from": "2026-10-10", "date_to": "2026-10-16", "timezone": "Asia/Calcutta"})

    assert api.call_to("/v1/campaigns/433/analytics")["body"] == {
        "timeZone": "Asia/Calcutta",
        "groupBy": ["DAY"],
        "metric": {"type": "COUNT"},
        "dateRange": ["2026-10-09T18:30:00.000Z", "2026-10-16T18:29:59.999Z"],
    }
    assert result["days"] == [{"date": "2026-10-13", "engagements": 8}]
    assert result["total"] == 8
    assert "note" not in result


@pytest.mark.asyncio
async def test_analytics_day_activities_splits_and_totals_per_activity():
    rows = [
        {"id": None, "name": "2026-10-11T18:30Z", "dimension": [], "value": None},
        {"id": None, "name": "2026-10-12T18:30Z", "value": None, "dimension": [
            {"id": 716, "name": "Abhinav Whatsapp Reply Rerun 3.0 (#713)", "type": "WHATSAPP", "value": 4},
            {"id": 713, "name": "Abhinav Whatsapp Reply", "type": "WHATSAPP", "value": 3}]},
        {"id": None, "name": "2026-10-13T18:30Z", "value": None, "dimension": [
            {"id": 713, "name": "Abhinav Whatsapp Reply", "type": "WHATSAPP", "value": 2}]},
    ]
    api = _FakeCampaignApi({"/v1/campaigns/433/analytics": rows})
    with patch("app.campaigns._campaign_call", api):
        result = await campaign_analytics_logic({
            "campaign_id": 433, "group_by": "day_activities",
            "date_from": "2026-10-12", "date_to": "2026-10-14", "timezone": "Asia/Calcutta"})

    assert api.call_to("/v1/campaigns/433/analytics")["body"]["groupBy"] == ["DAY", "ACTIVITIES"]
    assert [d["date"] for d in result["days"]] == ["2026-10-13", "2026-10-14"]
    assert result["per_activity_total"] == [
        {"id": 713, "name": "Abhinav Whatsapp Reply", "type": "WHATSAPP", "engagements": 5},
        {"id": 716, "name": "Abhinav Whatsapp Reply Rerun 3.0 (#713)", "type": "WHATSAPP", "engagements": 4},
    ]
    assert result["total"] == 9


@pytest.mark.asyncio
async def test_analytics_no_engagement_adds_note():
    api = _FakeCampaignApi({"/v1/campaigns/440/analytics": _days(("2026-10-25T18:30Z", 0))})
    with patch("app.campaigns._campaign_call", api):
        result = await campaign_analytics_logic({
            "campaign_id": 440, "date_from": "2026-10-26", "date_to": "2026-10-26", "timezone": "Asia/Calcutta"})
    assert result["days"] == [] and result["total"] == 0
    assert "No engagement" in result["note"]


@pytest.mark.asyncio
async def test_analytics_defaults_timezone_to_current_user():
    api = _FakeCampaignApi({"/v1/campaigns/433/analytics": []})
    with patch("app.campaigns._campaign_call", api), \
            patch("app.campaigns._fetch_current_user", AsyncMock(return_value={"timezone": "America/New_York"})):
        result = await campaign_analytics_logic({"campaign_id": 433, "date_from": "2026-10-01", "date_to": "2026-10-01"})
    assert result["timezone"] == "America/New_York"
    assert api.call_to("/v1/campaigns/433/analytics")["body"]["dateRange"] == [
        "2026-10-01T04:00:00.000Z", "2026-10-02T03:59:59.999Z"]


def test_default_range_is_current_week_monday_to_sunday():
    date_from, date_to = _resolve_date_range({}, "Asia/Calcutta")
    assert date_from.weekday() == 0
    assert (date_to - date_from).days == 6


@pytest.mark.parametrize("payload,message", [
    ({"group_by": "ACTIVITY_STATUS"}, "isn't valid for a campaign"),
    ({"date_from": "2026-10-01"}, "both 'date_from' and 'date_to'"),
    ({"date_from": "2026-10-05", "date_to": "2026-10-01"}, "on or before"),
    ({"date_from": "01/10/2026", "date_to": "2026-10-05"}, "YYYY-MM-DD"),
    ({"date_from": "2025-01-01", "date_to": "2026-10-01"}, "at most 366 days"),
    ({"timezone": "Mars/Base"}, "Unknown timezone"),
])
@pytest.mark.asyncio
async def test_analytics_rejects_bad_input_before_calling_api(payload, message):
    api = _FakeCampaignApi({})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(ValueError, match=message):
            await campaign_analytics_logic({"campaign_id": 433, "timezone": "Asia/Calcutta", **payload})
    assert api.calls == []


# ---------------------------------------------------------------------------
# R1 through the real registry tools
# ---------------------------------------------------------------------------


def test_campaign_bucket_lists_the_three_read_ids():
    from app.tools import list_tool

    ids = sorted(r["id"] for r in json.loads(list_tool(bucket="campaign"))["results"])
    assert ids == ["campaign.analytics", "campaign.get", "campaign.search"]


@pytest.mark.asyncio
async def test_build_payload_returns_campaign_schemas():
    from app.tools import build_payload

    search = json.loads(await build_payload("campaign.search"))
    assert search["path"] == "/v1/campaigns/search"
    assert "status" in search["schema"]["properties"]["filters"]["items"]["properties"]["field"]["enum"]
    assert search["schema"]["properties"]["size"]["maximum"] == 50

    analytics = json.loads(await build_payload("campaign.analytics"))
    assert analytics["schema"]["required"] == ["campaign_id"]
    assert analytics["schema"]["properties"]["group_by"]["enum"] == ["DAY", "DAY_ACTIVITIES"]
    assert analytics["example"]["group_by"] == "DAY_ACTIVITIES"


@pytest.mark.asyncio
async def test_execute_request_campaign_search_end_to_end():
    from app.tools import execute_request

    api = _FakeCampaignApi({"/v1/campaigns/search": {"content": [_CAMPAIGN_433], "totalElements": 1, "last": True},
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = json.loads(await execute_request("campaign.search", {
            "filters": [{"field": "status", "value": "IN_PROGRESS"}]}))
    assert result["ok"] is True
    assert result["data"]["campaigns"][0]["id"] == 433


@pytest.mark.asyncio
async def test_execute_request_campaign_get_missing_id_is_bad_payload():
    from app.tools import execute_request

    result = json.loads(await execute_request("campaign.get", {}))
    assert result["ok"] is False
    assert result["error"]["code"] == "BAD_PAYLOAD"


# ---------------------------------------------------------------------------
# Review fixes — local date filters, number coercion, sort direction,
# currency lookup failures
# ---------------------------------------------------------------------------


def _rule(filters, timezone="Asia/Calcutta"):
    return build_campaign_json_rule(filters, CAMPAIGN_FILTER_FIELDS, timezone)["jsonRule"]["rules"][0]


def test_date_filter_bare_dates_cover_whole_local_days():
    rule = _rule([{"field": "startedAt", "operator": "between", "value": ["2026-10-01", "2026-10-31"]}])
    assert rule["value"] == ["2026-09-30T18:30:00.000Z", "2026-10-31T18:29:59.999Z"]


@pytest.mark.parametrize("operator,expected", [
    ("greater_or_equal", "2026-09-30T18:30:00.000Z"),
    ("less", "2026-09-30T18:30:00.000Z"),
    ("greater", "2026-10-01T18:29:59.999Z"),
    ("less_or_equal", "2026-10-01T18:29:59.999Z"),
])
def test_date_filter_bare_date_bound_per_operator(operator, expected):
    assert _rule([{"field": "createdAt", "operator": operator, "value": "2026-10-01"}])["value"] == expected


def test_date_filter_local_datetime_and_trailing_z_are_local():
    assert _rule([{"field": "createdAt", "operator": "greater", "value": "2026-10-01T10:00:00"}])["value"] \
        == "2026-10-01T04:30:00.000Z"
    assert _rule([{"field": "createdAt", "operator": "greater", "value": "2026-10-01T10:00:00Z"}])["value"] \
        == "2026-10-01T04:30:00.000Z"


def test_date_filter_explicit_offset_and_filter_timezone_win():
    assert _rule([{"field": "createdAt", "operator": "less", "value": "2026-10-01T10:00:00+00:00"}])["value"] \
        == "2026-10-01T10:00:00.000Z"
    assert _rule([{"field": "createdAt", "operator": "less", "value": "2026-10-01",
                   "timeZone": "America/New_York"}])["value"] == "2026-10-01T04:00:00.000Z"


@pytest.mark.parametrize("filters,message", [
    ([{"field": "createdAt", "operator": "greater", "value": "not a date"}], "needs a date"),
    ([{"field": "createdAt", "operator": "greater", "value": 20261001}], "needs a date"),
    ([{"field": "createdAt", "operator": "greater", "value": "2026-10-01", "timeZone": "Mars/Base"}], "unknown timeZone"),
])
def test_date_filter_rejects_bad_values(filters, message):
    with pytest.raises(ValueError, match=message):
        build_campaign_json_rule(filters, CAMPAIGN_FILTER_FIELDS)


def test_number_filters_coerce_strings():
    assert _rule([{"field": "estimatedBudget", "operator": "greater", "value": "5000"}])["value"] == 5000.0
    assert _rule([{"field": "id", "operator": "in", "value": ["433", 425.0]}])["value"] == [433, 425]
    assert _rule([{"field": "actualExpense", "operator": "between", "value": [100, "2000.5"]}])["value"] == [100.0, 2000.5]


@pytest.mark.parametrize("filters,message", [
    ([{"field": "estimatedBudget", "operator": "greater", "value": "lots"}], "needs a number"),
    ([{"field": "id", "operator": "equal", "value": "abc"}], "needs a number"),
    ([{"field": "id", "operator": "equal", "value": 4.5}], "whole-number id"),
    ([{"field": "createdBy", "operator": "equal", "value": True}], "needs a number"),
])
def test_number_filters_reject_non_numbers(filters, message):
    with pytest.raises(ValueError, match=message):
        build_campaign_json_rule(filters, CAMPAIGN_FILTER_FIELDS)


@pytest.mark.asyncio
async def test_search_date_filter_uses_current_user_timezone():
    api = _FakeCampaignApi({"/v1/campaigns/search": {"content": [], "totalElements": 0, "last": True},
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api), \
            patch("app.campaigns._fetch_current_user", AsyncMock(return_value={"timezone": "America/New_York"})):
        await search_campaigns_logic({"filters": [{"field": "createdAt", "operator": "greater_or_equal",
                                                   "value": "2026-10-01"}]})
    assert api.call_to("/v1/campaigns/search")["body"]["jsonRule"]["rules"][0]["value"] == "2026-10-01T04:00:00.000Z"


@pytest.mark.asyncio
async def test_search_without_date_filters_skips_user_lookup():
    api = _FakeCampaignApi({"/v1/campaigns/search": {"content": [], "totalElements": 0, "last": True},
                            "/v1/forex/currencies": _CURRENCIES})
    current_user = AsyncMock()
    with patch("app.campaigns._campaign_call", api), patch("app.campaigns._fetch_current_user", current_user):
        await search_campaigns_logic({"filters": [{"field": "status", "value": "DRAFT"}]})
    current_user.assert_not_called()


@pytest.mark.asyncio
async def test_search_sort_without_direction_is_ascending():
    api = _FakeCampaignApi({"/v1/campaigns/search": {"content": [], "totalElements": 0, "last": True},
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        await search_campaigns_logic({"sort": "createdAt"})
    assert api.call_to("/v1/campaigns/search")["params"]["sort"] == "createdAt,asc"


@pytest.mark.parametrize("failure", [
    httpx.ReadTimeout("timed out"),
    {"error": "unexpected"},
])
@pytest.mark.asyncio
async def test_get_still_works_when_currencies_time_out_or_look_wrong(failure):
    api = _FakeCampaignApi({"/v1/campaigns/433/overview": _CAMPAIGN_433, "/v1/forex/currencies": failure})
    with patch("app.campaigns._campaign_call", api):
        result = await get_campaign_logic({"campaign_id": 433})
    assert result["estimated_budget"] == {"value": 100000.0, "currency": None, "currency_id": 431}
