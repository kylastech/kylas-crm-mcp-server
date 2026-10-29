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


# ---------------------------------------------------------------------------
# Phase R2 — campaign_activity.search / get / analytics / recipients /
# generated_records. Fixtures are trimmed copies of real QA responses:
# WhatsApp activity 713 (tenant 3691) and CALL activity 685 (tenant 4377).
# ---------------------------------------------------------------------------

from app.campaigns import (  # noqa: E402
    activity_analytics_logic,
    activity_generated_records_logic,
    activity_recipients_logic,
    get_activity_logic,
    search_activities_logic,
)

_SEARCH_ACTIVITIES = "/v1/campaigns/activities/search"

_ACTIVITY_716 = {**_ACTIVITY_713, "id": 716, "name": "Abhinav Whatsapp Reply Rerun 3.0 (#713)", "status": "PROCESSED"}

_ACTIVITY_685 = {
    "id": 685, "name": "Testing", "status": "IN_PROGRESS", "type": "CALL", "entity": "LEAD",
    "campaign": {"id": 399, "name": "Testing"},
    "estimatedBudget": {"currencyId": 431, "value": 300.0}, "actualExpense": {"currencyId": 431, "value": 500.0},
    "startDate": "2026-04-01T08:00:00.000Z", "endDate": None, "startedAt": "2026-05-11T08:59:00.000Z",
    "payload": {"type": "CALL", "marketplaceApp": {"id": 246, "name": "Campaign Test Tata Tele"},
                "didNumber": "+918065489741", "userAgents": [{"id": 11220, "name": "Aditya Tambe"}],
                "sentTo": "PRIMARY_PHONE_NUMBER"},
    "totalRecords": 4, "dialedCalls": 4, "connectedCalls": 2, "conversionRate": 0.0, "averageHandlingTime": 20.0,
    "agentDialerMetrics": [{"id": 11220, "name": "Aditya Tambe", "totalRecordsAssigned": 4, "dialedCalls": 4,
                            "connectedCalls": 2, "conversionRate": 0.0, "averageHandlingTime": 20.5,
                            "callDialerStatus": "COMPLETED"}],
    "totalEngagement": 4, "recordsGenerated": 0,
}


def _activity_rows(*activities):
    return {"content": list(activities), "totalElements": len(activities), "last": True}


# --- resolving the activity -------------------------------------------------


@pytest.mark.asyncio
async def test_activity_with_campaign_id_is_fetched_directly():
    api = _FakeCampaignApi({"/v1/campaigns/433/activities/713": _ACTIVITY_713, "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_activity_logic({"activity_id": 713, "campaign_id": 433})
    assert result["id"] == 713 and result["campaign"] == {"id": 433, "name": "Abhinav Whatsapp Replied Campaign"}
    assert [c["path"] for c in api.calls] == ["/v1/campaigns/433/activities/713", "/v1/forex/currencies"]


@pytest.mark.asyncio
async def test_activity_id_alone_is_found_through_search():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_713), "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_activity_logic({"activity_id": 713})
    assert result["id"] == 713
    assert api.call_to(_SEARCH_ACTIVITIES)["body"]["jsonRule"]["rules"] == [
        {"id": "id", "field": "id", "type": "long", "operator": "equal", "value": 713}]


@pytest.mark.asyncio
async def test_unknown_activity_id_is_not_found():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows()})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(KylasAPIError, match="Activity not found"):
            await get_activity_logic({"activity_id": 999999})


@pytest.mark.asyncio
async def test_activity_name_prefers_the_exact_match():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_716, _ACTIVITY_713),
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_activity_logic({"activity_name": "abhinav whatsapp reply"})
    assert result["id"] == 713
    assert api.call_to(_SEARCH_ACTIVITIES)["body"]["jsonRule"]["rules"][0]["value"] == "abhinav whatsapp reply"


@pytest.mark.asyncio
async def test_activity_name_with_several_matches_asks_the_user():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_716, _ACTIVITY_713)})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_name": "Abhinav"})
    assert [m["id"] for m in result["matches"]] == [716, 713]
    assert result["matches"][0]["campaign"]["name"] == "Abhinav Whatsapp Replied Campaign"
    assert [m["total_engagement"] for m in result["matches"]] == [4, 4]
    assert result["note"].startswith("STOP")
    assert "Ask the user to choose one" in result["note"]
    assert "don't run this for each match" in result["note"]
    assert [c["path"] for c in api.calls] == [_SEARCH_ACTIVITIES]


@pytest.mark.asyncio
async def test_activity_name_is_narrowed_by_campaign_id():
    other = {**_ACTIVITY_716, "campaign": {"id": 425, "name": "Diwali Special Offer from Kylas!"}}
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(other, _ACTIVITY_713), "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_activity_logic({"activity_name": "Abhinav", "campaign_id": 433})
    assert result["id"] == 713


@pytest.mark.parametrize("payload,error,message", [
    ({"activity_name": "Holi Mega Blast"}, KylasAPIError, "No activity name contains 'Holi Mega Blast'"),
    ({}, ValueError, "Give 'activity_id' or 'activity_name'"),
    ({"activity_id": "abc"}, ValueError, "positive integer id"),
])
@pytest.mark.asyncio
async def test_activity_lookup_errors(payload, error, message):
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows()})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(error, match=message):
            await get_activity_logic(payload)


# --- campaign_activity.search / get -----------------------------------------


@pytest.mark.asyncio
async def test_search_activities_sends_filters_and_shows_campaign():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: {"content": [_ACTIVITY_713], "totalElements": 4, "last": False},
                            "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await search_activities_logic({
            "filters": [{"field": "type", "value": "whatsapp"}, {"field": "status", "operator": "in", "value": ["paused"]}],
            "search_term": "Abhinav", "sort": "type,desc", "size": 5,
        })
    call = api.call_to(_SEARCH_ACTIVITIES)
    assert call["params"] == {"page": 0, "size": 5, "sort": "type,desc"}
    assert [(r["field"], r["value"]) for r in call["body"]["jsonRule"]["rules"]] == [
        ("type", "WHATSAPP"), ("status", ["PAUSED"]), ("multi_field", "Abhinav")]
    assert result["total"] == 4 and result["has_more"] is True
    activity = result["activities"][0]
    assert activity["campaign"] == {"id": 433, "name": "Abhinav Whatsapp Replied Campaign"}
    assert activity["actual_expense"] == {"value": 0.0, "currency": "INR", "currency_id": 431}


@pytest.mark.parametrize("payload,message", [
    ({"filters": [{"field": "campaign_id", "value": 433}]}, "can't be filtered by campaign"),
    ({"filters": [{"field": "type", "value": "SMS"}]}, "not valid for 'type'"),
    ({"sort": "totalEngagement,desc"}, "Can't sort by 'totalEngagement'"),
])
@pytest.mark.asyncio
async def test_search_activities_rejects_bad_input_before_calling_api(payload, message):
    api = _FakeCampaignApi({})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(ValueError, match=message):
            await search_activities_logic(payload)
    assert api.calls == []


@pytest.mark.asyncio
async def test_get_call_activity_includes_dialer_and_agent_metrics():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_685), "/v1/forex/currencies": _CURRENCIES})
    with patch("app.campaigns._campaign_call", api):
        result = await get_activity_logic({"activity_id": 685})
    assert result["content"]["dialer_app"] == "Campaign Test Tata Tele"
    assert result["connectedCalls"] == 2
    assert result["agent_metrics"] == [{
        "agent": {"id": 11220, "name": "Aditya Tambe"}, "status": "COMPLETED", "records_assigned": 4,
        "dialed_calls": 4, "connected_calls": 2, "conversion_rate": 0.0, "average_handling_time": 20.5}]


# --- campaign_activity.analytics --------------------------------------------


_ANALYTICS_713 = "/v1/campaigns/433/activities/713/analytics"
_ANALYTICS_685 = "/v1/campaigns/399/activities/685/analytics"


@pytest.mark.asyncio
async def test_activity_funnel_defaults_to_the_activity_page_range():
    rows = [{"id": None, "name": n, "dimension": [], "value": v}
            for n, v in [("Read", 4), ("Delivered", 4), ("Failed", 2), ("Replied", 1), ("Sent", 6)]]
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_713), _ANALYTICS_713: rows})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 713, "timezone": "Asia/Calcutta"})
    body = api.call_to(_ANALYTICS_713)["body"]
    assert body["groupBy"] == ["ACTIVITY_STATUS"]
    # startDate 13 Oct (IST), no endDate -> 13 Oct to 20 Oct, like CampaignActivityFunnelChart.
    assert body["dateRange"] == ["2026-10-12T18:30:00.000Z", "2026-10-20T18:29:59.999Z"]
    assert result["date_from"] == "2026-10-13" and result["date_to"] == "2026-10-20"
    assert "activity page's default" in result["date_range"]
    assert result["funnel"] == [{"step": "Sent", "count": 6}, {"step": "Delivered", "count": 4},
                                {"step": "Read", "count": 4}, {"step": "Replied", "count": 1},
                                {"step": "Failed", "count": 2}]
    assert "total" not in result  # steps overlap, a sum would double-count
    assert "note" not in result


@pytest.mark.asyncio
async def test_empty_funnel_gets_a_note():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_713),
                            _ANALYTICS_713: [{"name": "Sent", "value": 0}]})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 713, "timezone": "Asia/Calcutta",
                                                 "date_from": "2026-01-01", "date_to": "2026-01-31"})
    assert result["note"] == "Nothing in this date range."


@pytest.mark.asyncio
async def test_activity_default_range_ends_on_the_end_date():
    activity = {**_ACTIVITY_713, "endDate": "2026-10-15T05:30:00.000Z"}
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(activity), _ANALYTICS_713: []})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 713, "timezone": "Asia/Calcutta"})
    assert (result["date_from"], result["date_to"]) == ("2026-10-13", "2026-10-15")


@pytest.mark.asyncio
async def test_email_funnel_fills_missing_steps_with_zero():
    email = {**_ACTIVITY_713, "type": "EMAIL"}
    rows = [{"name": "Opened", "value": 70}, {"name": "Failed", "value": 66}, {"name": "Sent", "value": 70}]
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(email), _ANALYTICS_713: rows})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 713, "timezone": "Asia/Calcutta"})
    assert result["funnel"] == [{"step": "Sent", "count": 70}, {"step": "Opened", "count": 70},
                                {"step": "Clicked", "count": 0}, {"step": "Failed", "count": 66}]


@pytest.mark.asyncio
async def test_activity_day_engagement_uses_requested_dates():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_713),
                            _ANALYTICS_713: _days(("2026-10-12T18:30Z", 4), ("2026-10-13T18:30Z", 0))})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 713, "group_by": "day", "timezone": "Asia/Calcutta",
                                                 "date_from": "2026-10-01", "date_to": "2026-10-31"})
    assert api.call_to(_ANALYTICS_713)["body"]["groupBy"] == ["DAY"]
    assert result["date_range"] == "as requested"
    assert result["days"] == [{"date": "2026-10-13", "engagements": 4}] and result["total"] == 4
    assert "WhatsApp messages read" in result["metric"]


@pytest.mark.asyncio
async def test_call_activity_defaults_to_outcomes_per_day():
    rows = [{"id": None, "name": "2026-05-10T18:30Z", "value": None,
             "dimension": [{"name": "CONNECTED", "value": 2}, {"name": "MISSED_CALL", "value": 2}]},
            {"id": None, "name": "2026-05-11T18:30Z", "value": None, "dimension": []}]
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_685), _ANALYTICS_685: rows})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 685, "timezone": "Asia/Calcutta", "agent_ids": ["11220"],
                                                 "date_from": "2026-05-01", "date_to": "2026-05-31"})
    body = api.call_to(_ANALYTICS_685)["body"]
    assert body["groupBy"] == ["DAY", "OUTCOME"]
    assert body["rules"] == [{"id": "userAgent", "field": "userAgent", "type": "long", "operator": "in", "value": [11220]}]
    assert result["days"] == [{"date": "2026-05-11", "outcomes": [{"outcome": "CONNECTED", "calls": 2},
                                                                  {"outcome": "MISSED_CALL", "calls": 2}]}]
    assert result["per_outcome_total"] == [{"outcome": "CONNECTED", "calls": 2}, {"outcome": "MISSED_CALL", "calls": 2}]
    assert result["total"] == 4


@pytest.mark.asyncio
async def test_call_dispositions_name_the_unset_one():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_685),
                            _ANALYTICS_685: [{"id": None, "name": None, "dimension": [], "value": 4}]})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 685, "group_by": "CALL_DISPOSITION",
                                                 "timezone": "Asia/Calcutta"})
    assert result["dispositions"] == [{"id": None, "disposition": "No disposition", "calls": 4}]


@pytest.mark.asyncio
async def test_empty_default_window_tells_the_model_to_widen_it():
    # 685 was scheduled for 1 Apr but started on 11 May: the page's default window is empty.
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_685), _ANALYTICS_685: []})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_analytics_logic({"activity_id": 685, "timezone": "Asia/Calcutta"})
    assert (result["date_from"], result["date_to"]) == ("2026-04-01", "2026-04-08")
    assert "retry with date_from" in result["note"]
    # Following the advice must not break the 366-day range limit.
    assert "at most 365 days after date_from" in result["note"]


@pytest.mark.parametrize("activity,payload,message", [
    (_ACTIVITY_713, {"group_by": "DAY_OUTCOME"}, "isn't valid for a WHATSAPP activity"),
    (_ACTIVITY_685, {"group_by": "ACTIVITY_STATUS"}, "isn't valid for a CALL activity"),
    (_ACTIVITY_713, {"agent_ids": [11220]}, "only applies to CALL"),
    (_ACTIVITY_685, {"agent_ids": ["x"]}, "needs a number"),
])
@pytest.mark.asyncio
async def test_activity_analytics_rejects_bad_input_before_analytics_call(activity, payload, message):
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(activity)})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(ValueError, match=message):
            await activity_analytics_logic({"activity_id": activity["id"], **payload})
    assert [c["path"] for c in api.calls] == [_SEARCH_ACTIVITIES]


# --- campaign_activity.recipients -------------------------------------------


_RECIPIENTS_713 = "/v1/campaigns/activities/713/recipient-status"
_RECIPIENTS_685 = "/v1/campaigns/activities/685/recipient-status"


@pytest.mark.asyncio
async def test_whatsapp_recipients_filter_by_status_newest_sent_first():
    row = {"id": "e94ac5af", "entityId": 753402, "entityType": "LEAD", "recipientName": "Aditya T",
           "phoneNumber": "+917350806466", "status": "REPLIED", "sentAt": "2026-10-13T06:24:19.000Z",
           "deliveredAt": "2026-10-13T06:24:20.000Z", "readAt": "2026-10-13T06:25:10.000Z", "failedAt": None,
           "repliedAt": "2026-10-19T06:18:44.312Z", "errorMessage": None}
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_713),
                            _RECIPIENTS_713: {"content": [row], "totalElements": 1, "last": True}})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_recipients_logic({
            "activity_name": "Abhinav Whatsapp Reply",
            "filters": [{"field": "status", "operator": "in", "value": ["read", "replied"]}],
        })
    call = api.call_to(_RECIPIENTS_713)
    assert call["params"] == {"page": 0, "size": 20, "sort": "sentAt,desc"}
    assert call["body"]["jsonRule"]["rules"][0]["value"] == ["READ", "REPLIED"]
    assert result["activity"]["id"] == 713 and result["total"] == 1
    assert result["recipients"] == [{
        "name": "Aditya T", "entity_type": "LEAD", "entity_id": 753402, "phone_number": "+917350806466",
        "status": "REPLIED", "sent_at": "2026-10-13T06:24:19.000Z", "delivered_at": "2026-10-13T06:24:20.000Z",
        "read_at": "2026-10-13T06:25:10.000Z", "replied_at": "2026-10-19T06:18:44.312Z", "failed_at": None}]


@pytest.mark.asyncio
async def test_email_recipient_keeps_the_failure_reason():
    email = {**_ACTIVITY_713, "type": "EMAIL"}
    row = {"entityId": 499675, "entityType": "LEAD", "recipientName": "Jessika Mante", "email": "j@example.com",
           "status": "FAILED", "sentAt": "2026-05-22T07:31:47.417Z", "openedAt": None, "clickedAt": None,
           "failedAt": "2026-05-22T07:31:56.913Z", "errorMessage": "550-5.1.1 The email account does not exist."}
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(email),
                            _RECIPIENTS_713: {"content": [row], "totalElements": 1, "last": True}})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_recipients_logic({"activity_id": 713, "sort": "failedAt"})
    assert api.call_to(_RECIPIENTS_713)["params"]["sort"] == "failedAt,asc"
    recipient = result["recipients"][0]
    assert recipient["email"] == "j@example.com" and recipient["error"].startswith("550-5.1.1")


@pytest.mark.asyncio
async def test_call_recipients_filter_by_outcome_newest_call_first():
    row = {"entityId": 676016, "entityType": "LEAD", "recipientName": "Aditya Campaign Test 3",
           "phoneNumber": "+919823289810", "outcome": "MISSED_CALL", "disposition": None,
           "userAgent": {"id": 11220, "name": "Aditya Tambe"}, "duration": None, "callId": 157799,
           "callCreatedAt": "2026-05-11T09:01:38.723Z"}
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_685),
                            _RECIPIENTS_685: {"content": [row], "totalElements": 1, "last": True}})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_recipients_logic({"activity_id": 685,
                                                  "filters": [{"field": "outcome", "value": "missed_call"}]})
    call = api.call_to(_RECIPIENTS_685)
    assert call["params"]["sort"] == "callCreatedAt,desc"
    assert call["body"]["jsonRule"]["rules"][0]["value"] == "MISSED_CALL"
    assert result["recipients"][0] == {
        "name": "Aditya Campaign Test 3", "entity_type": "LEAD", "entity_id": 676016, "phone_number": "+919823289810",
        "outcome": "MISSED_CALL", "disposition": None, "agent": {"id": 11220, "name": "Aditya Tambe"},
        "duration_seconds": None, "call_id": 157799, "called_at": "2026-05-11T09:01:38.723Z"}


@pytest.mark.parametrize("activity,payload,message", [
    (_ACTIVITY_713, {"filters": [{"field": "openedAt", "operator": "is_not_null"}]}, "'openedAt' is not filterable"),
    (_ACTIVITY_713, {"filters": [{"field": "status", "value": "OPENED"}]}, "not valid for 'status'"),
    (_ACTIVITY_685, {"filters": [{"field": "status", "value": "READ"}]}, "'status' is not filterable"),
    (_ACTIVITY_713, {"sort": "callCreatedAt,desc"}, "Can't sort by 'callCreatedAt'"),
])
@pytest.mark.asyncio
async def test_recipient_filters_depend_on_activity_type(activity, payload, message):
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(activity)})
    with patch("app.campaigns._campaign_call", api):
        with pytest.raises(ValueError, match=message):
            await activity_recipients_logic({"activity_id": activity["id"], **payload})
    assert [c["path"] for c in api.calls] == [_SEARCH_ACTIVITIES]


@pytest.mark.asyncio
async def test_draft_activity_recipients_explain_why_empty():
    draft = {**_ACTIVITY_713, "status": "DRAFT"}
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(draft)})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_recipients_logic({"activity_id": 713, "size": 5})
    assert result["total"] == 0 and result["recipients"] == [] and result["size"] == 5
    assert "hasn't sent anything" in result["note"]
    # sd-campaign always answers an empty page for unsent activities, so it isn't asked.
    assert [c["path"] for c in api.calls] == [_SEARCH_ACTIVITIES]


# --- campaign_activity.generated_records ------------------------------------


_GENERATED_713 = "/v1/campaigns/activities/713/generated-records"


@pytest.mark.asyncio
async def test_generated_records_by_id_skip_the_lookup():
    row = {"id": 1, "entityId": 900001, "entityType": "LEAD", "entityName": "Riya P",
           "phoneNumbers": [{"type": "MOBILE", "code": "IN", "value": "9876543210", "dialCode": "+91", "primary": True}],
           "emails": [{"type": "OFFICE", "value": "riya@example.com", "primary": True}],
           "createdAt": "2026-10-14T05:00:00.000Z"}
    api = _FakeCampaignApi({_GENERATED_713: {"content": [row], "totalElements": 1, "last": True}})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_generated_records_logic({"activity_id": 713})
    assert [c["path"] for c in api.calls] == [_GENERATED_713]
    assert api.calls[0]["params"] == {"page": 0, "size": 20, "sort": "createdAt,desc"}
    assert result["activity"] == {"id": 713}
    assert result["records"] == [{"entity_type": "LEAD", "entity_id": 900001, "name": "Riya P",
                                  "phone_numbers": ["+919876543210"], "emails": ["riya@example.com"],
                                  "created_at": "2026-10-14T05:00:00.000Z"}]


@pytest.mark.asyncio
async def test_generated_records_by_name_resolve_the_activity():
    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_713),
                            _GENERATED_713: {"content": [], "totalElements": 0, "last": True}})
    with patch("app.campaigns._campaign_call", api):
        result = await activity_generated_records_logic({"activity_name": "Abhinav Whatsapp Reply"})
    assert result["activity"]["campaign"]["id"] == 433 and result["records"] == []


# --- registry wiring ----------------------------------------------------------


def test_campaign_activity_bucket_lists_the_five_read_ids():
    from app.tools import list_tool

    ids = sorted(r["id"] for r in json.loads(list_tool(bucket="campaign_activity"))["results"])
    assert ids == ["campaign_activity.analytics", "campaign_activity.generated_records", "campaign_activity.get",
                   "campaign_activity.recipients", "campaign_activity.search"]


@pytest.mark.asyncio
async def test_build_payload_returns_campaign_activity_schemas():
    from app.tools import build_payload

    analytics = json.loads(await build_payload("campaign_activity.analytics"))
    assert analytics["path"] == "/v1/campaigns/{campaign_id}/activities/{activity_id}/analytics"
    assert analytics["schema"]["properties"]["group_by"]["enum"] == [
        "ACTIVITY_STATUS", "CALL_DISPOSITION", "DAY", "DAY_OUTCOME"]
    assert {"activity_id", "activity_name", "campaign_id"} <= set(analytics["schema"]["properties"])

    recipients = json.loads(await build_payload("campaign_activity.recipients"))
    fields = recipients["schema"]["properties"]["filters"]["items"]["properties"]["field"]["enum"]
    assert {"status", "readAt", "openedAt", "outcome", "callCreatedAt"} <= set(fields)


@pytest.mark.asyncio
async def test_execute_request_returns_activity_choices_as_ok():
    from app.tools import execute_request

    api = _FakeCampaignApi({_SEARCH_ACTIVITIES: _activity_rows(_ACTIVITY_716, _ACTIVITY_713)})
    with patch("app.campaigns._campaign_call", api):
        result = json.loads(await execute_request("campaign_activity.get", {"activity_name": "Abhinav"}))
    assert result["ok"] is True
    assert len(result["data"]["matches"]) == 2
