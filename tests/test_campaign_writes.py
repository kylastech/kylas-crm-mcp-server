"""Tests for campaign writes (Plan 2).

Phase C0 — write foundation: PUT / empty-body handling in _campaign_call,
local write dates, read-merge-write, campaign name resolution, and the
activity audience builder. Nothing here talks to Kylas (HTTP is mocked).
"""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.campaigns import (
    _ACTIVITY_BODY_FIELDS,
    _ACTIVITY_FIELD_MAP,
    _CAMPAIGN_BODY_FIELDS,
    _CAMPAIGN_FIELD_MAP,
    _campaign_call,
    _check_date_order,
    _resolve_campaign_id,
    _write_date,
    build_audience_rule,
    count_audience,
    merge_for_update,
)
from app.client import KylasAPIError


def _mock_client(response: httpx.Response) -> AsyncMock:
    client = AsyncMock()
    client.get.return_value = response
    client.post.return_value = response
    client.put.return_value = response
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    return client


def _response(status: int, text: str = "", method: str = "PUT") -> httpx.Response:
    return httpx.Response(status, text=text, request=httpx.Request(method, "https://api.test/v1/campaigns/1"))


# ---------------------------------------------------------------------------
# _campaign_call — PUT and empty 200 bodies
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_campaign_call_put_sends_the_body():
    client = _mock_client(_response(200, '{"id": 1}'))
    with patch("app.campaigns.get_client", return_value=client):
        result = await _campaign_call("PUT", "/v1/campaigns/1", "Update campaign", body={"name": "X"})
    client.put.assert_awaited_once_with("/v1/campaigns/1", json={"name": "X"})
    assert result == {"id": 1}


@pytest.mark.asyncio
async def test_campaign_call_allow_empty_returns_empty_dict_for_empty_200():
    # start/pause/resume/complete answer 200 with no body.
    client = _mock_client(_response(200, "", method="POST"))
    with patch("app.campaigns.get_client", return_value=client):
        result = await _campaign_call("POST", "/v1/campaigns/1/start", "Start campaign", allow_empty=True)
    assert result == {}


@pytest.mark.asyncio
async def test_campaign_call_without_allow_empty_still_fails_on_empty_200():
    client = _mock_client(_response(200, "", method="POST"))
    with patch("app.campaigns.get_client", return_value=client):
        with pytest.raises(Exception):
            await _campaign_call("POST", "/v1/campaigns/1/start", "Start campaign")


@pytest.mark.asyncio
async def test_campaign_call_never_deletes():
    with patch("app.campaigns.get_client") as get_client:
        with pytest.raises(ValueError, match="Unsupported method"):
            await _campaign_call("DELETE", "/v1/campaigns/1", "Delete campaign")
    get_client.assert_not_called()


# ---------------------------------------------------------------------------
# Write dates — the user's local time; a plain date is the whole day
# ---------------------------------------------------------------------------


def test_plain_dates_become_start_and_end_of_local_day():
    assert _write_date("2026-11-01", "Asia/Calcutta", False, "start_date") == "2026-10-31T18:30:00.000Z"
    assert _write_date("2026-11-30", "Asia/Calcutta", True, "end_date") == "2026-11-30T18:29:59.999Z"


def test_write_datetime_is_kept_as_given_local_time():
    assert _write_date("2026-11-01T10:00", "Asia/Calcutta", False, "start_date") == "2026-11-01T04:30:00.000Z"
    assert _write_date("2026-11-01T10:00:00+00:00", "Asia/Calcutta", True, "end_date") == "2026-11-01T10:00:00.000Z"


def test_write_date_rejects_garbage():
    with pytest.raises(ValueError, match="'start_date' needs a date"):
        _write_date("next diwali", "Asia/Calcutta", False, "start_date")


def test_end_date_before_start_date_is_rejected():
    _check_date_order("2026-10-31T18:30:00.000Z", None)
    _check_date_order("2026-10-31T18:30:00.000Z", "2026-11-30T18:29:59.999Z")
    with pytest.raises(ValueError, match="on or after 'start_date'"):
        _check_date_order("2026-10-31T18:30:00.000Z", "2026-10-30T18:29:59.999Z")


# ---------------------------------------------------------------------------
# Read-merge-write
# ---------------------------------------------------------------------------

# Trimmed copy of GET /v1/campaigns/442 on QA (tenant 3691).
_CAMPAIGN_442 = {
    "id": 442, "name": "MCP Test Campaign (Claude)", "description": None, "status": "IN_PROGRESS",
    "estimatedBudget": {"currencyId": 431, "value": 10000.0}, "actualExpense": {"currencyId": 431, "value": 3000.0},
    "startDate": "2026-10-26T00:00:00.000Z", "endDate": None, "startedAt": "2026-10-26T07:33:14.806Z",
    "endedAt": None, "startedBy": {"id": 8112, "name": "Akash Nakhate"}, "endedBy": None,
    "utmSource": "mcp", "utmCampaign": "mcp-test-campaign-20261026", "utmMedium": "test",
    "utmContent": None, "utmTerm": None,
    "createdBy": {"id": 8112, "name": "Akash Nakhate"}, "activities": [{"id": 718}], "totalEngagement": 2,
    "recordsGenerated": 0, "recordActions": {"update": True},
}


def test_merge_keeps_untouched_fields_and_applies_changes():
    body = merge_for_update(_CAMPAIGN_442, {"description": "Diwali push", "end_date": "2026-11-30T18:29:59.999Z"},
                            _CAMPAIGN_FIELD_MAP, _CAMPAIGN_BODY_FIELDS)
    assert body["description"] == "Diwali push"
    assert body["endDate"] == "2026-11-30T18:29:59.999Z"
    assert body["name"] == "MCP Test Campaign (Claude)"
    assert body["utmCampaign"] == "mcp-test-campaign-20261026"
    assert body["estimatedBudget"] == {"currencyId": 431, "value": 10000.0}


def test_merge_never_sends_read_only_fields():
    body = merge_for_update(_CAMPAIGN_442, {}, _CAMPAIGN_FIELD_MAP, _CAMPAIGN_BODY_FIELDS)
    assert set(body) == set(_CAMPAIGN_BODY_FIELDS)
    for read_only in ("status", "createdBy", "activities", "totalEngagement", "recordsGenerated", "recordActions"):
        assert read_only not in body


def test_activity_merge_has_no_description_and_keeps_payload():
    activity = {"name": "Old", "status": "DRAFT", "payload": {"type": "EMAIL", "emailTemplate": {"id": 812}},
                "filters": {"jsonRule": {"rules": []}}, "entity": "LEAD", "totalEngagement": 0}
    body = merge_for_update(activity, {"name": "New", "description": "ignored"},
                            _ACTIVITY_FIELD_MAP, _ACTIVITY_BODY_FIELDS)
    assert body["name"] == "New" and "description" not in body
    assert body["payload"] == {"type": "EMAIL", "emailTemplate": {"id": 812}}
    assert "totalEngagement" not in body


# ---------------------------------------------------------------------------
# Campaign by name
# ---------------------------------------------------------------------------


class _FakeCampaignApi:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def __call__(self, method, path, op, *, params=None, body=None, allow_empty=False):
        self.calls.append({"method": method, "path": path, "params": params, "body": body})
        return self.response


def _campaign_rows(*rows):
    return {"content": list(rows), "totalElements": len(rows), "last": True}


@pytest.mark.asyncio
async def test_campaign_id_is_used_without_a_lookup():
    api = _FakeCampaignApi(None)
    with patch("app.campaigns._campaign_call", api):
        assert await _resolve_campaign_id({"campaign_id": "442"}) == (442, None)
    assert api.calls == []


@pytest.mark.asyncio
async def test_campaign_name_prefers_the_exact_match():
    api = _FakeCampaignApi(_campaign_rows({"id": 425, "name": "Diwali Special Offer from Kylas!"},
                                          {"id": 440, "name": "Diwali Offer"}))
    with patch("app.campaigns._campaign_call", api):
        assert await _resolve_campaign_id({"campaign_name": "diwali offer"}) == (440, None)
    call = api.calls[0]
    assert call["path"] == "/v1/campaigns/search"
    assert call["params"] == {"page": 0, "size": 50, "sort": "updatedAt,desc"}
    assert call["body"]["jsonRule"]["rules"][0]["value"] == "diwali offer"


@pytest.mark.asyncio
async def test_campaign_name_with_several_matches_returns_a_choice():
    api = _FakeCampaignApi(_campaign_rows(*[{"id": i, "name": "Summer Promo", "status": "DRAFT"} for i in (13, 14)]
                                          + [{"id": 21, "name": "Summer Promo 2", "status": "DRAFT"}]))
    with patch("app.campaigns._campaign_call", api):
        campaign_id, choice = await _resolve_campaign_id({"campaign_name": "Summer Promo"})
    assert campaign_id is None
    assert [m["id"] for m in choice["matches"]] == [13, 14]
    assert choice["note"].startswith("STOP") and "nothing was done" in choice["note"]


@pytest.mark.parametrize("payload,error,message", [
    ({"campaign_name": "Holi Mega Blast"}, KylasAPIError, "No campaign name contains 'Holi Mega Blast'"),
    ({}, ValueError, "Give 'campaign_id' or 'campaign_name'"),
    ({"campaign_id": "abc"}, ValueError, "positive integer id"),
])
@pytest.mark.asyncio
async def test_campaign_lookup_errors(payload, error, message):
    with patch("app.campaigns._campaign_call", _FakeCampaignApi(_campaign_rows())):
        with pytest.raises(error, match=message):
            await _resolve_campaign_id(payload)


# ---------------------------------------------------------------------------
# Audience
# ---------------------------------------------------------------------------

_LEAD_FIELDS = [
    {"name": "id", "type": "NUMBER", "filterable": True, "active": True, "standard": True},
    {"name": "firstName", "type": "TEXT_FIELD", "filterable": True, "active": True, "standard": True},
    {"name": "notes", "type": "TEXT_FIELD", "filterable": False, "active": True, "standard": True},
]


@pytest.mark.asyncio
async def test_audience_uses_the_lead_search_filter_format():
    with patch("app.campaigns._fetch_lead_fields", AsyncMock(return_value=_LEAD_FIELDS)):
        rule = await build_audience_rule("lead", [{"field": "id", "operator": "equal", "value": 753407}],
                                         "Asia/Calcutta")
    assert rule["condition"] == "AND"
    assert [(r["field"], r["operator"], r["value"]) for r in rule["rules"]] == [("id", "equal", 753407)]


@pytest.mark.parametrize("entity,filters,message", [
    ("LEAD", [], "needs at least one filter"),
    ("LEAD", None, "needs at least one filter"),
    ("DEAL", [{"field": "id", "value": 1}], "'entity' must be one of"),
    ("LEAD", [{"field": "notes", "operator": "equal", "value": "x"}], "Invalid audience filter"),
])
@pytest.mark.asyncio
async def test_audience_rejects_bad_input(entity, filters, message):
    with patch("app.campaigns._fetch_lead_fields", AsyncMock(return_value=_LEAD_FIELDS)):
        with pytest.raises(ValueError, match=message):
            await build_audience_rule(entity, filters, "Asia/Calcutta")


@pytest.mark.asyncio
async def test_count_audience_runs_the_entity_search_for_one_row():
    client = _mock_client(httpx.Response(200, json={"content": [{"id": 753407}], "totalElements": 1},
                                         request=httpx.Request("POST", "https://api.test/v1/search/lead")))
    rule = {"condition": "AND", "valid": True, "rules": [{"field": "id", "operator": "equal", "value": 753407}]}
    with patch("app.campaigns.get_client", return_value=client):
        assert await count_audience("LEAD", rule) == 1
    client.post.assert_awaited_once_with("/v1/search/lead", params={"page": 0, "size": 1},
                                         json={"fields": ["id"], "jsonRule": rule})


# ---------------------------------------------------------------------------
# Lookups — fixtures are real QA responses (tenant 3691, 2026-10-30)
# ---------------------------------------------------------------------------

from app.campaigns import (  # noqa: E402
    _id_or_name,
    _resolve_currency,
    _resolve_email_template,
    _resolve_whatsapp_account,
    _resolve_whatsapp_template,
    check_utm_logic,
    currencies_logic,
    email_templates_logic,
    whatsapp_templates_logic,
)

_CURRENCY_ROWS = [
    {"id": 2980, "currencyValueId": 431, "name": "INR", "active": True, "baseCurrency": True},
    {"id": 2976, "currencyValueId": 400, "name": "USD", "active": True, "baseCurrency": False},
    {"id": 2999, "currencyValueId": 999, "name": "GBP", "active": False, "baseCurrency": False},
]
_EMAIL_LOOKUP = {"content": [{"id": 830, "name": "Lead meta Data 2"}, {"id": 812, "name": "Lead creation"},
                             {"id": 829, "name": "Lead Meta data"}]}
_EMAIL_812 = {"id": 812, "name": "Lead creation", "category": "lead", "status": "Active", "subject": "Lead create"}
_EMAIL_772 = {"id": 772, "name": "Contact 1", "category": "contact"}
_ACCOUNT_LOOKUP = {"content": [{"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs",
                                "isChatbotConfigured": True}]}
# GET by id names the fields the other way round.
_ACCOUNT_77 = {"id": 77, "name": "Acme Business", "displayName": "Acme Sender", "status": "active"}
_WA_1040 = {"id": 1040, "name": "Please Reply copy", "entityType": "lead", "status": "APPROVED",
            "connectedAccount": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"}}


class _FakeKylasGet:
    """Stands in for _kylas_get: routes by path; a 404 raises the not_found message like the real one."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def __call__(self, path, op, params=None, not_found=""):
        self.calls.append({"path": path, "params": params})
        if path not in self.responses:
            raise KylasAPIError(not_found or f"{op} failed: 404", status_code=404)
        response = self.responses[path]
        return response(params) if callable(response) else response


def test_id_or_name():
    assert _id_or_name(812) == (812, "")
    assert _id_or_name(" 812 ") == (812, "")
    assert _id_or_name("Lead creation") == (None, "Lead creation")
    for bad in (True, "", None):
        with pytest.raises(ValueError):
            _id_or_name(bad)


@pytest.mark.parametrize("value,expected", [(None, 431), ("", 431), ("inr", 431), ("USD", 400), (400, 400), ("431", 431)])
@pytest.mark.asyncio
async def test_currency_by_code_or_id_defaults_to_base(value, expected):
    with patch("app.campaigns._fetch_currencies", AsyncMock(return_value=_CURRENCY_ROWS)):
        assert await _resolve_currency(value) == expected


@pytest.mark.parametrize("value", ["GBP", "XYZ", 2999])
@pytest.mark.asyncio
async def test_inactive_or_unknown_currency_is_rejected(value):
    with patch("app.campaigns._fetch_currencies", AsyncMock(return_value=_CURRENCY_ROWS)):
        with pytest.raises(ValueError, match=r"isn't an active currency here. Active: INR \(431\), USD \(400\)"):
            await _resolve_currency(value)


@pytest.mark.asyncio
async def test_currencies_lookup_lists_active_only():
    with patch("app.campaigns._fetch_currencies", AsyncMock(return_value=_CURRENCY_ROWS)):
        result = await currencies_logic({})
    assert result == {"currencies": [{"currency_id": 431, "code": "INR", "base": True},
                                     {"currency_id": 400, "code": "USD", "base": False}]}


@pytest.mark.asyncio
async def test_email_template_by_name_prefers_exact_match():
    fake = _FakeKylasGet({"/v1/email-templates/lookup": _EMAIL_LOOKUP})
    with patch("app.campaigns._kylas_get", fake):
        template, choice = await _resolve_email_template("lead creation", "LEAD")
    assert (template, choice) == ({"id": 812, "name": "Lead creation"}, None)
    assert fake.calls[0]["params"] == {"category": "lead", "q": "lead creation"}


@pytest.mark.asyncio
async def test_email_template_name_with_several_matches_returns_a_choice():
    fake = _FakeKylasGet({"/v1/email-templates/lookup": _EMAIL_LOOKUP})
    with patch("app.campaigns._kylas_get", fake):
        template, choice = await _resolve_email_template("lead", "LEAD")
    assert template is None and len(choice["matches"]) == 3 and choice["note"].startswith("STOP")


@pytest.mark.asyncio
async def test_email_template_by_id_checks_its_entity():
    fake = _FakeKylasGet({"/v1/email-templates/812": _EMAIL_812, "/v1/email-templates/772": _EMAIL_772})
    with patch("app.campaigns._kylas_get", fake):
        assert await _resolve_email_template(812, "LEAD") == ({"id": 812, "name": "Lead creation"}, None)
        with pytest.raises(ValueError, match="is for CONTACTs, not LEADs"):
            await _resolve_email_template("772", "LEAD")
        with pytest.raises(KylasAPIError, match="Email template 999 not found"):
            await _resolve_email_template(999, "LEAD")


def _account_lookup(params):
    # Like Kylas: q matches the account name only, never the business name.
    q = (params or {}).get("q", "").lower()
    return {"content": [r for r in _ACCOUNT_LOOKUP["content"] if q in r["name"].lower()]}


@pytest.mark.parametrize("name", ["Kylas Devs", "kylas devoloper", "devs"])
@pytest.mark.asyncio
async def test_whatsapp_account_by_name_or_business_name(name):
    with patch("app.campaigns._kylas_get", _FakeKylasGet({"/v1/messages/connected-accounts/lookup": _account_lookup})):
        account, choice = await _resolve_whatsapp_account(name)
    assert account == {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"} and choice is None


@pytest.mark.asyncio
async def test_unknown_whatsapp_account_name_is_not_found():
    with patch("app.campaigns._kylas_get", _FakeKylasGet({"/v1/messages/connected-accounts/lookup": _account_lookup})):
        with pytest.raises(KylasAPIError, match="No WhatsApp account name contains 'Acme'"):
            await _resolve_whatsapp_account("Acme")


@pytest.mark.asyncio
async def test_whatsapp_account_by_id_falls_back_to_get_by_id():
    fake = _FakeKylasGet({"/v1/messages/connected-accounts/lookup": _ACCOUNT_LOOKUP,
                          "/v1/messages/connected-accounts/77": _ACCOUNT_77})
    with patch("app.campaigns._kylas_get", fake):
        assert (await _resolve_whatsapp_account(20))[0]["name"] == "Kylas Devoloper"
        account, _ = await _resolve_whatsapp_account("77")
    # Swapped back to the lookup's naming: name = account name, businessName = business name.
    assert account == {"id": 77, "name": "Acme Sender", "businessName": "Acme Business"}


@pytest.mark.asyncio
async def test_whatsapp_template_by_name_uses_the_lookup_only():
    # The lookup only returns APPROVED templates of that account and entity, so no extra fetch.
    fake = _FakeKylasGet({"/v1/messages/whatsapp-templates/lookup": {"content": [{"id": 1040, "name": "Please Reply copy"}]}})
    with patch("app.campaigns._kylas_get", fake):
        template, _ = await _resolve_whatsapp_template("please reply copy", 20, "LEAD")
    assert template == {"id": 1040, "name": "Please Reply copy"}
    assert fake.calls == [{"path": "/v1/messages/whatsapp-templates/lookup",
                           "params": {"connectedAccountId": 20, "entityType": "lead", "q": "please reply copy"}}]


@pytest.mark.asyncio
async def test_whatsapp_template_name_with_several_matches_returns_a_choice():
    fake = _FakeKylasGet({"/v1/messages/whatsapp-templates/lookup": {"content": [
        {"id": 1044, "name": "Test Content"}, {"id": 1030, "name": "Test Template - Lead"}]}})
    with patch("app.campaigns._kylas_get", fake):
        template, choice = await _resolve_whatsapp_template("test", 20, "LEAD")
    assert template is None and [m["id"] for m in choice["matches"]] == [1044, 1030]
    assert len(fake.calls) == 1


@pytest.mark.parametrize("template,account_id,entity,message", [
    ({**_WA_1040, "connectedAccount": {"id": 21}}, 20, "LEAD", "belongs to another WhatsApp account"),
    (_WA_1040, 20, "CONTACT", "is for LEADs, not CONTACTs"),
    ({**_WA_1040, "status": "PENDING"}, 20, "LEAD", "is PENDING, not APPROVED"),
])
@pytest.mark.asyncio
async def test_whatsapp_template_by_id_is_checked(template, account_id, entity, message):
    with patch("app.campaigns._kylas_get", _FakeKylasGet({"/v1/messages/whatsapp-templates/1040": template})):
        with pytest.raises(ValueError, match=message):
            await _resolve_whatsapp_template(1040, account_id, entity)


@pytest.mark.asyncio
async def test_whatsapp_templates_lookup_resolves_the_account_first():
    fake = _FakeKylasGet({"/v1/messages/connected-accounts/lookup": _ACCOUNT_LOOKUP,
                          "/v1/messages/whatsapp-templates/lookup": {"content": [{"id": 1040, "name": "Please Reply copy"}]}})
    with patch("app.campaigns._kylas_get", fake):
        result = await whatsapp_templates_logic({"whatsapp_account": "Kylas Devs", "entity": "lead"})
    assert result["account"]["id"] == 20 and result["templates"] == [{"id": 1040, "name": "Please Reply copy"}]
    assert fake.calls[-1]["params"] == {"connectedAccountId": 20, "entityType": "lead", "q": ""}


@pytest.mark.asyncio
async def test_email_templates_lookup_needs_an_entity():
    with pytest.raises(ValueError, match="'entity' must be one of"):
        await email_templates_logic({"query": "lead"})


@pytest.mark.asyncio
async def test_check_utm_excludes_the_activity_itself():
    api = _FakeCampaignApi({"duplicate": False})
    with patch("app.campaigns._campaign_call", api):
        result = await check_utm_logic({"utm_campaign": "abhinav", "activity_id": 713})
    assert result == {"utm_campaign": "abhinav", "available": True}
    assert api.calls[0]["params"] == {"fieldName": "utmCampaign", "value": "abhinav", "id": 713}
