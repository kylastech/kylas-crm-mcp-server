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
