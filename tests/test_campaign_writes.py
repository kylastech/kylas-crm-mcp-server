import json
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
    with patch.dict("app.campaigns._AUDIENCE_FIELD_FETCHERS", {"LEAD": AsyncMock(return_value=_LEAD_FIELDS)}):
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
    with patch.dict("app.campaigns._AUDIENCE_FIELD_FETCHERS", {"LEAD": AsyncMock(return_value=_LEAD_FIELDS)}):
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


# ---------------------------------------------------------------------------
# Phase C1 — campaign.create / campaign.update: a preview first (nothing
# written), then the same details with confirm: true.
# ---------------------------------------------------------------------------

from app.campaigns import create_campaign_logic, update_campaign_logic  # noqa: E402

_USER = {"id": 8112, "name": "Akash Nakhate", "tenant_id": 3691, "timezone": "Asia/Calcutta"}

_CREATE = {
    "name": "Diwali Offer 2026", "description": "Festive push",
    "budget": {"value": 50000, "currency": "INR"}, "start_date": "2026-11-01", "end_date": "2026-11-30",
    "utm_campaign": "diwali-2026", "utm_source": "kylas", "utm_medium": "whatsapp",
}
_CREATE_BODY = {
    "name": "Diwali Offer 2026", "description": "Festive push",
    "utmCampaign": "diwali-2026", "utmSource": "kylas", "utmMedium": "whatsapp", "utmContent": None, "utmTerm": None,
    "startDate": "2026-10-31T18:30:00.000Z", "endDate": "2026-11-30T18:29:59.999Z",
    "estimatedBudget": {"currencyId": 431, "value": 50000.0}, "actualExpense": {"currencyId": 431, "value": 0},
}


class _FakeWriteApi:
    """Stands in for _campaign_call in write tests: routes by (method, path), records every call."""

    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    async def __call__(self, method, path, op, *, params=None, body=None, allow_empty=False):
        self.calls.append({"method": method, "path": path, "body": body})
        return self.responses.get((method, path), {})

    def writes(self):
        return [c for c in self.calls if c["method"] == "PUT" or (c["method"] == "POST" and not c["path"].endswith("/search"))]


class _Patched:
    """The write tests' doubles: Kylas calls, the acting user, currencies, and the final campaign.get."""

    def __init__(self, api, user=_USER):
        self.patches = [
            patch("app.campaigns._campaign_call", api),
            patch("app.campaigns._acting_user", AsyncMock(return_value=user)),
            patch("app.campaigns._fetch_currencies", AsyncMock(return_value=_CURRENCY_ROWS)),
            patch("app.campaigns._currency_names", AsyncMock(return_value={431: "INR", 400: "USD"})),
            patch("app.campaigns.get_campaign_logic", AsyncMock(side_effect=lambda p: {"id": p["campaign_id"]})),
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.stop()


# --- campaign.create ------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_preview_writes_nothing_and_shows_everything():
    api = _FakeWriteApi()
    with _Patched(api):
        result = await create_campaign_logic(dict(_CREATE))
    assert api.calls == []
    assert result["executed"] is False
    preview = result["preview"]
    assert preview["name"] == "Diwali Offer 2026"
    assert preview["budget"] == {"value": 50000.0, "currency": "INR", "currency_id": 431}
    assert preview["start_date"] == "01 Nov 2026, 12:00:00 AM (Asia/Calcutta)"
    assert preview["end_date"] == "30 Nov 2026, 11:59:59 PM (Asia/Calcutta)"
    assert preview["utm"]["campaign"] == "diwali-2026"
    assert preview["status"].startswith("DRAFT")
    assert result["runs_as"] == {"user": "Akash Nakhate", "user_id": 8112, "tenant_id": 3691}
    assert "NOTHING WAS WRITTEN" in result["note"] and "Yes / No" in result["note"]
    assert "confirm: true" in result["note"]


@pytest.mark.asyncio
async def test_create_with_confirm_sends_the_body():
    api = _FakeWriteApi({("POST", "/v1/campaigns"): {"id": 500}})
    with _Patched(api):
        result = await create_campaign_logic({**_CREATE, "confirm": True})
    assert api.writes() == [{"method": "POST", "path": "/v1/campaigns", "body": _CREATE_BODY}]
    assert result == {"executed": True, "campaign": {"id": 500}}


@pytest.mark.parametrize("confirm", [False, "true", 1, None])
@pytest.mark.asyncio
async def test_only_confirm_true_writes(confirm):
    api = _FakeWriteApi()
    with _Patched(api):
        result = await create_campaign_logic({**_CREATE, "confirm": confirm})
    assert result["executed"] is False and api.writes() == []


@pytest.mark.asyncio
async def test_create_defaults_to_the_base_currency_and_allows_no_end_date():
    payload = {k: v for k, v in _CREATE.items() if k != "end_date"}
    payload["budget"] = 1000
    api = _FakeWriteApi({("POST", "/v1/campaigns"): {"id": 501}})
    with _Patched(api):
        preview = await create_campaign_logic(dict(payload))
        await create_campaign_logic({**payload, "confirm": True})
    assert preview["preview"]["end_date"] == "none"
    body = api.writes()[0]["body"]
    assert body["estimatedBudget"] == {"currencyId": 431, "value": 1000.0} and body["endDate"] is None


@pytest.mark.parametrize("change,message", [
    ({"name": ""}, "'name' is required"),
    ({"utm_source": None}, "'utm_source' is required"),
    ({"name": "x" * 256}, "at most 255 characters"),
    ({"budget": None}, "'budget' is required"),
    ({"budget": {"value": -5}}, "can't be negative"),
    ({"budget": {"value": 10, "currency": "GBP"}}, "isn't an active currency"),
    ({"start_date": None}, "'start_date' is required"),
    ({"end_date": "2026-10-01"}, "on or after 'start_date'"),
])
@pytest.mark.parametrize("confirm", [False, True])
@pytest.mark.asyncio
async def test_create_is_validated_before_anything_is_written(change, message, confirm):
    api = _FakeWriteApi()
    with _Patched(api):
        with pytest.raises(ValueError, match=message):
            await create_campaign_logic({**_CREATE, **change, "confirm": confirm})
    assert api.calls == []


# --- campaign.update ------------------------------------------------------------

_CURRENT = {
    "id": 440, "name": "Diwali Offer", "description": None, "status": "DRAFT",
    "estimatedBudget": {"currencyId": 431, "value": 30000.0}, "actualExpense": {"currencyId": 431, "value": 0.0},
    "startDate": "2026-10-31T18:30:00.000Z", "endDate": "2026-11-30T18:29:59.999Z", "startedAt": None,
    "endedAt": None, "startedBy": None, "endedBy": None, "utmSource": "kylas", "utmCampaign": "diwali-offer",
    "utmMedium": "email", "utmContent": None, "utmTerm": None, "updatedAt": "2026-10-23T11:18:38.483Z",
    "activities": [{"id": 900, "name": "Diwali email", "startDate": "2026-11-05T04:30:00.000Z",
                    "endDate": "2026-11-10T04:30:00.000Z", "estimatedBudget": {"currencyId": 431, "value": 10000.0}}],
    "recordActions": {"update": True}, "totalEngagement": 0,
}


def _update_api(current=_CURRENT):
    return _FakeWriteApi({("GET", "/v1/campaigns/440"): current, ("PUT", "/v1/campaigns/440"): {"id": 440}})


@pytest.mark.asyncio
async def test_update_preview_shows_before_and_after_and_writes_nothing():
    api = _update_api()
    with _Patched(api):
        result = await update_campaign_logic({"campaign_id": 440, "budget": {"value": 75000},
                                              "description": "Bigger push"})
    assert api.writes() == []
    assert result["executed"] is False
    changes = result["preview"]["changes"]
    assert changes["budget"] == {"before": {"value": 30000.0, "currency": "INR"},
                                 "after": {"value": 75000.0, "currency": "INR"}}
    assert changes["description"] == {"before": None, "after": "Bigger push"}
    assert result["preview"]["after_update"]["name"] == "Diwali Offer"
    assert result["preview"]["campaign"] == {"id": 440, "name": "Diwali Offer", "status": "DRAFT"}


@pytest.mark.asyncio
async def test_update_with_confirm_puts_the_merged_body():
    api = _update_api()
    with _Patched(api):
        result = await update_campaign_logic({"campaign_id": 440, "budget": {"value": 75000},
                                              "end_date": "2026-12-15", "confirm": True})
    (put,) = api.writes()
    body = put["body"]
    assert put["path"] == "/v1/campaigns/440"
    assert body["estimatedBudget"] == {"currencyId": 431, "value": 75000.0}
    assert body["endDate"] == "2026-12-15T18:29:59.999Z"
    assert body["utmCampaign"] == "diwali-offer" and body["name"] == "Diwali Offer"
    assert "recordActions" not in body and "activities" not in body and "status" not in body
    assert result == {"executed": True, "campaign": {"id": 440}}


@pytest.mark.asyncio
async def test_update_confirm_merges_onto_the_current_values():
    # Someone else renamed it after the preview: the confirm keeps their name and applies only our change.
    renamed = {**_CURRENT, "name": "Diwali Mega Offer"}
    api = _update_api(renamed)
    with _Patched(api):
        await update_campaign_logic({"campaign_id": 440, "budget": 75000, "confirm": True})
    body = api.writes()[0]["body"]
    assert body["name"] == "Diwali Mega Offer" and body["estimatedBudget"]["value"] == 75000.0


@pytest.mark.asyncio
async def test_update_by_name_with_several_matches_asks_first():
    api = _FakeWriteApi({("POST", "/v1/campaigns/search"): _campaign_rows(
        {"id": 13, "name": "Summer Promo"}, {"id": 14, "name": "Summer Promo"})})
    with _Patched(api):
        result = await update_campaign_logic({"campaign_name": "Summer Promo", "budget": 1, "confirm": True})
    assert [m["id"] for m in result["matches"]] == [13, 14] and "nothing was done" in result["note"]
    assert api.writes() == []


@pytest.mark.asyncio
async def test_started_campaign_only_takes_budget_and_end_date():
    started = {**_CURRENT, "status": "IN_PROGRESS"}
    with _Patched(_update_api(started)):
        with pytest.raises(ValueError, match=r"only budget and end_date.*Not allowed: \['name'\]"):
            await update_campaign_logic({"campaign_id": 440, "name": "New name", "budget": 40000})
        result = await update_campaign_logic({"campaign_id": 440, "budget": 40000, "end_date": "2026-12-31"})
    assert set(result["preview"]["changes"]) == {"budget", "end_date"}


@pytest.mark.parametrize("change,message", [
    ({"start_date": "2026-11-07"}, "Activity 'Diwali email' starts on .* outside the new campaign dates"),
    ({"end_date": "2026-11-08"}, "Activity 'Diwali email' ends on .* outside the new campaign dates"),
    ({"budget": 5000}, "activities' budgets add up to 10000, more than the new campaign budget 5000"),
    ({"end_date": "2026-10-15"}, "on or after 'start_date'"),
    ({}, "Nothing to change"),
    ({"name": "Diwali Offer"}, "already set"),
])
@pytest.mark.parametrize("confirm", [False, True])
@pytest.mark.asyncio
async def test_update_runs_the_backend_checks_before_anything_is_written(change, message, confirm):
    api = _update_api()
    with _Patched(api):
        with pytest.raises(ValueError, match=message):
            await update_campaign_logic({"campaign_id": 440, **change, "confirm": confirm})
    assert api.writes() == []


@pytest.mark.asyncio
async def test_update_can_clear_the_end_date():
    api = _update_api()
    with _Patched(api):
        preview = await update_campaign_logic({"campaign_id": 440, "end_date": None})
        await update_campaign_logic({"campaign_id": 440, "end_date": None, "confirm": True})
    assert preview["preview"]["changes"]["end_date"]["after"] == "none"
    assert api.writes()[0]["body"]["endDate"] is None


# ---------------------------------------------------------------------------
# Phase C2 — campaign_activity.create / campaign_activity.update (EMAIL and
# WHATSAPP): preview first, then the same details with confirm: true.
# Campaign 449 "MCP Plan2 Test" is the real QA campaign created in C1.
# ---------------------------------------------------------------------------

from app.campaigns import create_activity_logic, update_activity_logic  # noqa: E402

_CAMPAIGN_449 = {
    "id": 449, "name": "MCP Plan2 Test", "status": "DRAFT",
    "estimatedBudget": {"currencyId": 431, "value": 2000.0},
    "startDate": "2026-10-31T18:30:00.000Z", "endDate": "2026-11-30T18:29:59.999Z", "activities": [],
}
_RULE_753407 = {"field": "id", "operator": "equal", "value": 753407}

_EMAIL_CREATE = {
    "campaign_id": 449, "type": "email", "entity": "lead", "name": "Welcome email",
    "audience_filters": [_RULE_753407], "email_template": "Lead creation", "budget": 500,
    "start_date": "2026-11-05", "utm_campaign": "mcp-plan2-email-1", "utm_source": "mcp", "utm_medium": "email",
}
_WHATSAPP_CREATE = {
    "campaign_id": 449, "type": "WHATSAPP", "entity": "LEAD", "name": "Welcome WhatsApp",
    "audience_filters": [_RULE_753407], "whatsapp_account": "Kylas Devs", "whatsapp_template": "Please Reply copy",
    "retry_times": 2, "budget": 300, "start_date": "2026-11-05", "end_date": "2026-11-10",
    "utm_campaign": "mcp-plan2-wa-1", "utm_source": "mcp", "utm_medium": "whatsapp",
}


def _kylas_lookups():
    return _FakeKylasGet({
        "/v1/email-templates/lookup": lambda params: {"content": [r for r in _EMAIL_LOOKUP["content"]
                                                                   if params["q"].lower() in r["name"].lower()]},
        "/v1/email-templates/812": _EMAIL_812,
        "/v1/email-templates/772": _EMAIL_772,
        "/v1/messages/connected-accounts/lookup": _account_lookup,
        "/v1/messages/whatsapp-templates/lookup": {"content": [{"id": 1040, "name": "Please Reply copy"}]},
        "/v1/messages/whatsapp-templates/1040": _WA_1040,
    })


class _ActivityPatched(_Patched):
    """C1's doubles plus template/account lookups, lead fields, the audience count and the final activity.get."""

    def __init__(self, api, recipients=1, current=None):
        super().__init__(api)
        self.count = AsyncMock(return_value=recipients)
        self.patches += [
            patch("app.campaigns._kylas_get", _kylas_lookups()),
            patch("app.campaigns._fetch_lead_fields", AsyncMock(return_value=_LEAD_FIELDS)),
            patch("app.campaigns.count_audience", self.count),
            patch("app.campaigns.get_activity_logic", AsyncMock(side_effect=lambda p: {"id": p["activity_id"]})),
        ]
        if current is not None:
            self.patches.append(patch("app.campaigns._resolve_activity", AsyncMock(return_value=(current, None))))


def _activity_api(campaign=_CAMPAIGN_449, duplicate=False):
    return _FakeWriteApi({
        ("GET", "/v1/campaigns/449"): campaign,
        ("GET", "/v1/campaigns/activities/has-duplicates"): {"duplicate": duplicate},
        ("POST", "/v1/campaigns/449/activities"): {"id": 800},
        ("PUT", "/v1/campaigns/449/activities/800"): {"id": 800},
    })


# --- campaign_activity.create -----------------------------------------------------


@pytest.mark.asyncio
async def test_activity_create_preview_writes_nothing_and_shows_everything():
    api = _activity_api()
    with _ActivityPatched(api):
        result = await create_activity_logic(dict(_EMAIL_CREATE))
    assert api.writes() == [] and result["executed"] is False
    preview = result["preview"]
    assert preview["campaign"] == {"id": 449, "name": "MCP Plan2 Test", "status": "DRAFT"}
    assert (preview["type"], preview["entity"], preview["sent_to"]) == ("EMAIL", "LEAD", "PRIMARY_EMAIL")
    assert preview["email_template"] == "Lead creation"
    assert preview["audience_filters"] == [{"field": "id", "operator": "equal", "value": 753407}]
    assert preview["recipients_now"] == 1 and preview["recipients_note"].startswith("Can start")
    # Same money shape as activity reads and campaign previews.
    assert preview["budget"] == {"value": 500.0, "currency": "INR", "currency_id": 431}
    assert preview["actual_expense"] == {"value": 0.0, "currency": "INR", "currency_id": 431}
    assert preview["start_date"] == "05 Nov 2026, 12:00:00 AM (Asia/Calcutta)" and preview["end_date"] == "none"
    assert preview["status"].startswith("DRAFT")
    assert "NOTHING WAS WRITTEN" in result["note"]


@pytest.mark.asyncio
async def test_activity_create_confirm_posts_the_web_form_body():
    api = _activity_api()
    with _ActivityPatched(api) as _:
        result = await create_activity_logic({**_EMAIL_CREATE, "confirm": True})
    (post,) = api.writes()
    assert post["path"] == "/v1/campaigns/449/activities"
    body = post["body"]
    assert body["status"] == "DRAFT" and body["entity"] == "LEAD" and body["name"] == "Welcome email"
    assert body["estimatedBudget"] == {"currencyId": 431, "value": 500.0}
    assert body["actualExpense"] == {"currencyId": 431, "value": 0.0}
    assert (body["startDate"], body["endDate"]) == ("2026-11-04T18:30:00.000Z", None)
    assert (body["utmCampaign"], body["utmSource"], body["utmMedium"]) == ("mcp-plan2-email-1", "mcp", "email")
    assert body["payload"] == {"type": "EMAIL", "sentTo": "PRIMARY_EMAIL", "emailTemplate": {"id": 812, "name": "Lead creation"}}
    assert body["filters"]["jsonRule"]["condition"] == "AND"
    assert [(r["field"], r["value"]) for r in body["filters"]["jsonRule"]["rules"]] == [("id", 753407)]
    assert result == {"executed": True, "activity": {"id": 800}}


@pytest.mark.asyncio
async def test_whatsapp_activity_payload_has_account_template_and_retries():
    api = _activity_api()
    with _ActivityPatched(api):
        await create_activity_logic({**_WHATSAPP_CREATE, "confirm": True})
    body = api.writes()[0]["body"]
    assert body["payload"] == {
        "type": "WHATSAPP", "sentTo": "PRIMARY_PHONE_NUMBER",
        "connectedAccount": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"},
        "whatsappTemplate": {"id": 1040, "name": "Please Reply copy"}, "retryConfig": {"noOfTimes": 2},
    }
    assert body["endDate"] == "2026-11-10T18:29:59.999Z"


@pytest.mark.parametrize("payload,campaign,message", [
    ({**_EMAIL_CREATE, "type": "CALL"}, _CAMPAIGN_449, "CALL activities can't"),
    ({**_EMAIL_CREATE, "type": None}, _CAMPAIGN_449, "'type' must be EMAIL or WHATSAPP"),
    ({**_EMAIL_CREATE, "entity": "deal"}, _CAMPAIGN_449, "'entity' must be one of"),
    ({**_EMAIL_CREATE, "name": ""}, _CAMPAIGN_449, "'name' is required"),
    ({**_EMAIL_CREATE, "utm_medium": None}, _CAMPAIGN_449, "'utm_medium' is required"),
    ({**_EMAIL_CREATE, "budget": None}, _CAMPAIGN_449, "'budget' is required"),
    ({**_EMAIL_CREATE, "start_date": None}, _CAMPAIGN_449, "'start_date' is required"),
    ({**_EMAIL_CREATE, "start_date": "2026-10-20"}, _CAMPAIGN_449, "outside the campaign's dates"),
    ({**_EMAIL_CREATE, "end_date": "2026-12-05"}, _CAMPAIGN_449, "outside the campaign's dates"),
    ({**_EMAIL_CREATE, "end_date": "2026-11-01"}, _CAMPAIGN_449, "on or after 'start_date'"),
    (_EMAIL_CREATE, {**_CAMPAIGN_449, "activities": [{"id": i} for i in range(4)]}, "already has 4 activities"),
    ({**_EMAIL_CREATE, "budget": 600},
     {**_CAMPAIGN_449, "activities": [{"id": 1, "estimatedBudget": {"value": 1500.0}}]},
     "budgets of 2100 in total, more than the campaign budget 2000"),
    ({**_EMAIL_CREATE, "budget": {"value": 100, "currency": "USD"}}, _CAMPAIGN_449, "always in its campaign's currency"),
    ({**_EMAIL_CREATE, "actual_expense": -1}, _CAMPAIGN_449, "can't be negative"),
    ({**_EMAIL_CREATE, "email_template": None}, _CAMPAIGN_449, "'email_template' is required"),
    ({**_EMAIL_CREATE, "email_template": 772}, _CAMPAIGN_449, "is for CONTACTs, not LEADs"),
    ({**_EMAIL_CREATE, "sent_to": "ALL_PHONE_NUMBERS"}, _CAMPAIGN_449, "'sent_to' for EMAIL must be one of"),
    ({**_WHATSAPP_CREATE, "whatsapp_template": None}, _CAMPAIGN_449, "'whatsapp_template' is required"),
    ({**_WHATSAPP_CREATE, "retry_times": 9}, _CAMPAIGN_449, "between 0 and 7"),
    ({**_EMAIL_CREATE, "audience_filters": []}, _CAMPAIGN_449, "needs at least one filter"),
])
@pytest.mark.parametrize("confirm", [False, True])
@pytest.mark.asyncio
async def test_activity_create_is_validated_before_anything_is_written(payload, campaign, message, confirm):
    api = _activity_api(campaign)
    with _ActivityPatched(api):
        with pytest.raises(ValueError, match=message):
            await create_activity_logic({**payload, "confirm": confirm})
    assert api.writes() == []


@pytest.mark.asyncio
async def test_activity_create_refuses_a_used_utm_campaign():
    api = _activity_api(duplicate=True)
    with _ActivityPatched(api):
        with pytest.raises(ValueError, match="'mcp-plan2-email-1' is already used by another activity"):
            await create_activity_logic({**_EMAIL_CREATE, "confirm": True})
    assert api.writes() == []


@pytest.mark.asyncio
async def test_activity_create_with_an_ambiguous_template_asks_first():
    api = _activity_api()
    with _ActivityPatched(api):
        result = await create_activity_logic({**_EMAIL_CREATE, "email_template": "lead", "confirm": True})
    assert result["note"].startswith("STOP") and len(result["matches"]) == 3
    assert api.writes() == []


@pytest.mark.parametrize("recipients,note", [(0, "No leads/contacts match"), (10001, "More than 10,000 match")])
@pytest.mark.asyncio
async def test_activity_preview_warns_when_it_could_not_start(recipients, note):
    with _ActivityPatched(_activity_api(), recipients=recipients):
        result = await create_activity_logic(dict(_EMAIL_CREATE))
    assert result["preview"]["recipients_now"] == recipients and result["preview"]["recipients_note"].startswith(note)


# --- campaign_activity.update -----------------------------------------------------

_ACTIVITY_800 = {
    "id": 800, "name": "Welcome email", "status": "DRAFT", "type": "EMAIL", "entity": "LEAD",
    "campaign": {"id": 449, "name": "MCP Plan2 Test"},
    "estimatedBudget": {"currencyId": 431, "value": 500.0}, "actualExpense": {"currencyId": 431, "value": 0.0},
    "startDate": "2026-11-04T18:30:00.000Z", "endDate": None,
    "utmCampaign": "mcp-plan2-email-1", "utmSource": "mcp", "utmMedium": "email", "utmContent": None, "utmTerm": None,
    "filters": {"jsonRule": {"condition": "AND", "valid": True, "rules": [
        {"id": "id", "field": "id", "type": "double", "operator": "equal", "value": 753407}]}},
    "payload": {"type": "EMAIL", "sentTo": "PRIMARY_EMAIL", "emailTemplate": {"id": 812, "name": "Lead creation"},
                "connectedEmailAccount": "qa.kylas@gmail.com"},
    "totalEngagement": 0, "recordActions": {"update": True}, "bulkJobId": None,
}
_CAMPAIGN_449_WITH_800 = {**_CAMPAIGN_449, "activities": [_ACTIVITY_800]}


def _update(activity=_ACTIVITY_800, campaign=_CAMPAIGN_449_WITH_800, **kw):
    api = _activity_api(campaign)
    return api, _ActivityPatched(api, current=activity, **kw)


@pytest.mark.asyncio
async def test_activity_update_preview_shows_before_and_after():
    api, patched = _update()
    with patched:
        result = await update_activity_logic({"activity_name": "Welcome email", "budget": 800, "name": "Welcome mail"})
    assert api.writes() == [] and result["executed"] is False
    changes = result["preview"]["changes"]
    assert changes == {"name": {"before": "Welcome email", "after": "Welcome mail"},
                       "budget": {"before": {"value": 500.0, "currency": "INR", "currency_id": 431},
                                  "after": {"value": 800.0, "currency": "INR", "currency_id": 431}}}
    assert result["preview"]["activity"]["id"] == 800


@pytest.mark.asyncio
async def test_activity_update_confirm_puts_the_merged_body():
    api, patched = _update()
    with patched:
        result = await update_activity_logic({"activity_id": 800, "budget": 800, "confirm": True})
    (put,) = api.writes()
    body = put["body"]
    assert put["path"] == "/v1/campaigns/449/activities/800"
    assert body["estimatedBudget"] == {"currencyId": 431, "value": 800.0}
    assert body["payload"] == _ACTIVITY_800["payload"] and body["filters"] == _ACTIVITY_800["filters"]
    assert body["status"] == "DRAFT" and body["utmCampaign"] == "mcp-plan2-email-1"
    assert "recordActions" not in body and "totalEngagement" not in body and "campaign" not in body
    assert result == {"executed": True, "activity": {"id": 800}}


@pytest.mark.asyncio
async def test_activity_update_template_keeps_the_sending_account():
    api, patched = _update()
    with patched:
        await update_activity_logic({"activity_id": 800, "email_template": 812, "sent_to": "ALL_EMAILS", "confirm": True})
    assert api.writes()[0]["body"]["payload"] == {
        "type": "EMAIL", "sentTo": "ALL_EMAILS", "emailTemplate": {"id": 812, "name": "Lead creation"},
        "connectedEmailAccount": "qa.kylas@gmail.com"}


@pytest.mark.asyncio
async def test_activity_update_new_audience_is_counted_and_replaced():
    api, patched = _update()
    with patched:
        preview = await update_activity_logic({"activity_id": 800,
                                               "audience_filters": [{"field": "firstName", "operator": "contains",
                                                                     "value": "Atharva"}]})
        await update_activity_logic({"activity_id": 800, "confirm": True,
                                     "audience_filters": [{"field": "firstName", "operator": "contains", "value": "Atharva"}]})
    assert preview["preview"]["recipients_now"] == 1
    assert preview["preview"]["changes"]["audience_filters"]["after"] == [
        {"field": "firstName", "operator": "contains", "value": "Atharva"}]
    assert api.writes()[0]["body"]["filters"]["jsonRule"]["rules"][0]["field"] == "firstName"


@pytest.mark.parametrize("status", ["IN_PROGRESS", "PAUSED", "COMPLETED", "QUEUED", "PROCESSED"])
@pytest.mark.asyncio
async def test_started_activity_only_takes_budget_expense_and_end_date(status):
    started = {**_ACTIVITY_800, "status": status}
    api, patched = _update(started, {**_CAMPAIGN_449, "activities": [started]})
    with patched:
        with pytest.raises(ValueError, match=rf"This activity is {status}: only budget, actual_expense and end_date"):
            await update_activity_logic({"activity_id": 800, "name": "New name", "budget": 700})
        result = await update_activity_logic({"activity_id": 800, "budget": 700, "actual_expense": 120,
                                              "end_date": "2026-11-20"})
    assert set(result["preview"]["changes"]) == {"budget", "actual_expense", "end_date"}


@pytest.mark.parametrize("change,message", [
    ({"type": "WHATSAPP"}, "'whatsapp_account' is required"),
    ({"type": "WHATSAPP", "whatsapp_account": "Kylas Devs"}, "'whatsapp_template' is required"),
    ({"type": "CALL"}, "CALL activities can't"),
    ({"entity": "CONTACT"}, "needs new audience_filters"),
    ({}, "Nothing to change"),
    ({"budget": 500}, "already set"),
    ({"budget": 2500}, "budgets of 2500 in total, more than the campaign budget 2000"),
    ({"start_date": "2026-10-01"}, "outside the campaign's dates"),
    ({"email_template": 772}, "is for CONTACTs, not LEADs"),
])
@pytest.mark.parametrize("confirm", [False, True])
@pytest.mark.asyncio
async def test_activity_update_is_validated_before_anything_is_written(change, message, confirm):
    api, patched = _update()
    with patched:
        with pytest.raises(ValueError, match=message):
            await update_activity_logic({"activity_id": 800, **change, "confirm": confirm})
    assert api.writes() == []


@pytest.mark.asyncio
async def test_whatsapp_account_change_rechecks_the_template():
    whatsapp = {**_ACTIVITY_800, "type": "WHATSAPP", "payload": {
        "type": "WHATSAPP", "sentTo": "PRIMARY_PHONE_NUMBER",
        "connectedAccount": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"},
        "whatsappTemplate": {"id": 1040, "name": "Please Reply copy"}, "retryConfig": None}}
    api, patched = _update(whatsapp, {**_CAMPAIGN_449, "activities": [whatsapp]})
    patched.patches.append(patch("app.campaigns._resolve_whatsapp_account",
                                 AsyncMock(return_value=({"id": 77, "name": "Acme", "businessName": "Acme Biz"}, None))))
    with patched:
        with pytest.raises(ValueError, match="belongs to another WhatsApp account"):
            await update_activity_logic({"activity_id": 800, "whatsapp_account": "Acme", "confirm": True})
    assert api.writes() == []


@pytest.mark.asyncio
async def test_build_payload_folds_lead_fields_for_activity_create():
    from app.tools import build_payload

    with patch("app.tools._fold_field_metadata_into_schema",
               AsyncMock(side_effect=lambda schema, *a, **k: {**schema, "tenant_fields_reference": "lead fields"})) as fold:
        result = json.loads(await build_payload("campaign_activity.create", entity="lead"))
        plain = json.loads(await build_payload("campaign_activity.create"))
        wrong = json.loads(await build_payload("campaign_activity.create", entity="deal"))
    assert result["fetched_live"] is True and result["schema"]["tenant_fields_reference"] == "lead fields"
    assert fold.await_args.kwargs["bucket"] == "lead" and fold.await_args.kwargs["for_search"] is True
    assert plain["fetched_live"] is False and "tenant_fields_reference" not in plain["schema"]
    assert "must be lead or contact" in wrong["live_fetch_error"]


@pytest.mark.asyncio
async def test_draft_email_activity_can_become_whatsapp():
    api, patched = _update()
    with patched:
        preview = await update_activity_logic({"activity_id": 800, "type": "whatsapp", "whatsapp_account": "Kylas Devs",
                                               "whatsapp_template": "Please Reply copy"})
        await update_activity_logic({"activity_id": 800, "type": "WHATSAPP", "whatsapp_account": "Kylas Devs",
                                     "whatsapp_template": "Please Reply copy", "confirm": True})
    changes = preview["preview"]["changes"]
    assert changes["type"] == {"before": "EMAIL", "after": "WHATSAPP"}
    assert changes["email_template"] == {"before": "Lead creation", "after": None}
    assert changes["whatsapp_template"] == {"before": None, "after": "Please Reply copy"}
    assert changes["sent_to"] == {"before": "PRIMARY_EMAIL", "after": "PRIMARY_PHONE_NUMBER"}
    body = api.writes()[0]["body"]
    # A whole new payload: nothing from the email payload (template, sending account) carries over.
    assert body["payload"] == {
        "type": "WHATSAPP", "sentTo": "PRIMARY_PHONE_NUMBER",
        "connectedAccount": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"},
        "whatsappTemplate": {"id": 1040, "name": "Please Reply copy"}, "retryConfig": None}
    assert body["name"] == "Welcome email" and body["filters"] == _ACTIVITY_800["filters"]


@pytest.mark.asyncio
async def test_draft_whatsapp_activity_can_become_email():
    whatsapp = {**_ACTIVITY_800, "type": "WHATSAPP", "payload": {
        "type": "WHATSAPP", "sentTo": "ALL_PHONE_NUMBERS",
        "connectedAccount": {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"},
        "whatsappTemplate": {"id": 1040, "name": "Please Reply copy"}, "retryConfig": {"noOfTimes": 2}}}
    api, patched = _update(whatsapp, {**_CAMPAIGN_449, "activities": [whatsapp]})
    with patched:
        await update_activity_logic({"activity_id": 800, "type": "EMAIL", "email_template": "Lead creation",
                                     "confirm": True})
    assert api.writes()[0]["body"]["payload"] == {
        "type": "EMAIL", "sentTo": "PRIMARY_EMAIL", "emailTemplate": {"id": 812, "name": "Lead creation"}}


@pytest.mark.asyncio
async def test_same_type_given_again_is_not_a_change():
    api, patched = _update()
    with patched:
        result = await update_activity_logic({"activity_id": 800, "type": "EMAIL", "budget": 900})
    assert set(result["preview"]["changes"]) == {"budget"}


@pytest.mark.parametrize("status", ["IN_PROGRESS", "PAUSED", "COMPLETED", "QUEUED"])
@pytest.mark.asyncio
async def test_started_activity_type_cannot_change(status):
    started = {**_ACTIVITY_800, "status": status}
    api, patched = _update(started, {**_CAMPAIGN_449, "activities": [started]})
    with patched:
        with pytest.raises(ValueError, match=r"only budget, actual_expense and end_date.*Not allowed: .*'type'"):
            await update_activity_logic({"activity_id": 800, "type": "WHATSAPP", "whatsapp_account": "Kylas Devs",
                                         "whatsapp_template": "Please Reply copy", "confirm": True})
    assert api.writes() == []


# ---------------------------------------------------------------------------
# Review fixes: read-back after a write, nulls / unchanged values on update,
# no audience count on confirm, fields of the other activity type refused.
# ---------------------------------------------------------------------------


def _failing_read(*_args, **_kwargs):
    raise KylasAPIError("Fetch campaign activity failed: 504", status_code=504)


@pytest.mark.parametrize("call,payload,patched_get", [
    ("activity_create", {**_EMAIL_CREATE, "confirm": True}, "get_activity_logic"),
    ("activity_update", {"activity_id": 800, "budget": 900, "confirm": True}, "get_activity_logic"),
])
@pytest.mark.asyncio
async def test_activity_write_still_reports_success_when_the_read_back_fails(call, payload, patched_get):
    if call == "activity_create":
        api = _activity_api()
        patched = _ActivityPatched(api)
        fn = create_activity_logic
    else:
        api, patched = _update()
        fn = update_activity_logic
    patched.patches.append(patch(f"app.campaigns.{patched_get}", AsyncMock(side_effect=_failing_read)))
    with patched:
        result = await fn(payload)
    assert result["executed"] is True and result["activity"] == {"id": 800}
    assert "WAS saved" in result["note"] and "Do not retry" in result["note"]
    assert len(api.writes()) == 1


@pytest.mark.parametrize("payload,api_responses", [
    ({**_CREATE, "confirm": True}, {("POST", "/v1/campaigns"): {"id": 500}}),
    ({"campaign_id": 440, "budget": 75000, "confirm": True}, None),
])
@pytest.mark.asyncio
async def test_campaign_write_still_reports_success_when_the_read_back_fails(payload, api_responses):
    api = _FakeWriteApi(api_responses) if api_responses else _update_api()
    patched = _Patched(api)
    patched.patches[-1] = patch("app.campaigns.get_campaign_logic", AsyncMock(side_effect=_failing_read))
    fn = create_campaign_logic if api_responses else update_campaign_logic
    with patched:
        result = await fn(payload)
    assert result["executed"] is True and "WAS saved" in result["note"]
    assert result["campaign"]["id"] in (500, 440) and len(api.writes()) == 1


@pytest.mark.asyncio
async def test_activity_update_treats_nulls_as_not_given():
    current = {**_ACTIVITY_800, "utmContent": "banner"}
    api, patched = _update(current, {**_CAMPAIGN_449, "activities": [current]})
    with patched:
        preview = await update_activity_logic({"activity_id": 800, "budget": 900, "name": None, "utm_content": None,
                                               "retry_times": None, "sent_to": None, "audience_filters": None})
        await update_activity_logic({"activity_id": 800, "budget": 900, "name": None, "utm_content": None,
                                     "confirm": True})
    assert set(preview["preview"]["changes"]) == {"budget"}
    body = api.writes()[0]["body"]
    assert body["name"] == "Welcome email" and body["utmContent"] == "banner"


@pytest.mark.asyncio
async def test_campaign_update_treats_nulls_as_not_given():
    with _Patched(_update_api()):
        result = await update_campaign_logic({"campaign_id": 440, "budget": 40000, "name": None, "description": None})
    assert set(result["preview"]["changes"]) == {"budget"}


@pytest.mark.asyncio
async def test_started_activity_accepts_unchanged_values_resent():
    started = {**_ACTIVITY_800, "status": "IN_PROGRESS"}
    api, patched = _update(started, {**_CAMPAIGN_449, "activities": [started]})
    with patched:
        result = await update_activity_logic({"activity_id": 800, "entity": "LEAD", "name": "Welcome email",
                                              "utm_campaign": "mcp-plan2-email-1", "budget": 700})
    assert set(result["preview"]["changes"]) == {"budget"}


@pytest.mark.asyncio
async def test_started_campaign_accepts_unchanged_values_resent():
    started = {**_CURRENT, "status": "IN_PROGRESS"}
    with _Patched(_update_api(started)):
        result = await update_campaign_logic({"campaign_id": 440, "name": "Diwali Offer", "utm_source": "kylas",
                                              "budget": 40000})
    assert set(result["preview"]["changes"]) == {"budget"}


@pytest.mark.asyncio
async def test_activity_update_counts_the_audience_only_on_the_preview():
    new_audience = [{"field": "firstName", "operator": "contains", "value": "Atharva"}]
    api, patched = _update()
    with patched:
        await update_activity_logic({"activity_id": 800, "audience_filters": new_audience})
        assert patched.count.await_count == 1
        await update_activity_logic({"activity_id": 800, "audience_filters": new_audience, "confirm": True})
    assert patched.count.await_count == 1  # not again on confirm
    assert len(api.writes()) == 1


@pytest.mark.parametrize("fn,payload,message", [
    ("create", {**_EMAIL_CREATE, "retry_times": 2}, "retry_times does not apply to EMAIL activities"),
    ("create", {**_EMAIL_CREATE, "whatsapp_account": "Kylas Devs", "whatsapp_template": "x"},
     "whatsapp_account, whatsapp_template do not apply to EMAIL"),
    ("create", {**_WHATSAPP_CREATE, "email_template": "Lead creation"}, "email_template does not apply to WHATSAPP"),
    ("update", {"activity_id": 800, "whatsapp_template": "Please Reply copy"}, "whatsapp_template does not apply to EMAIL"),
    ("update", {"activity_id": 800, "retry_times": 3}, "retry_times does not apply to EMAIL"),
    ("update", {"activity_id": 800, "type": "WHATSAPP", "whatsapp_account": "Kylas Devs",
                "whatsapp_template": "Please Reply copy", "email_template": "Lead creation"},
     "email_template does not apply to WHATSAPP"),
])
@pytest.mark.parametrize("confirm", [False, True])
@pytest.mark.asyncio
async def test_fields_of_the_other_activity_type_are_refused(fn, payload, message, confirm):
    if fn == "create":
        api = _activity_api()
        patched = _ActivityPatched(api)
    else:
        api, patched = _update()
    with patched:
        with pytest.raises(ValueError, match=message):
            await (create_activity_logic if fn == "create" else update_activity_logic)({**payload, "confirm": confirm})
    assert api.writes() == []
