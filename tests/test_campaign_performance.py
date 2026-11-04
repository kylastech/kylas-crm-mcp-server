"""campaign.performance — rank past campaigns and rebuild the best ones' setup for campaign_activity.create."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from app import campaigns as campaigns_module
from app.campaigns import (
    _performance_numbers,
    _position_scores,
    build_audience_rule,
    campaign_performance_logic,
    rebuild_audience,
)

TZ = "Asia/Calcutta"

_CURRENCIES = [
    {"id": 2980, "currencyValueId": 431, "name": "INR", "active": True, "baseCurrency": True},
    {"id": 2976, "currencyValueId": 400, "name": "USD", "active": True, "baseCurrency": False},
]

_LEAD_FIELDS = [
    {"name": "lastName", "displayName": "Last Name", "type": "TEXT_FIELD", "filterable": True, "standard": True},
    {"name": "id", "displayName": "ID", "type": "NUMBER", "filterable": True, "standard": True},
    {"name": "source", "displayName": "Source", "type": "PICK_LIST", "filterable": True, "standard": True,
     "picklist": {"values": [{"id": 3812, "displayName": "Website"}, {"id": 3813, "displayName": "Referral"}]}},
    {"name": "updatedAt", "displayName": "Updated At", "type": "DATETIME_PICKER", "filterable": True, "standard": True},
    {"name": "createdAt", "displayName": "Created At", "type": "DATETIME_PICKER", "filterable": True, "standard": True},
    {"name": "cfNumber", "displayName": "Lucky Number", "type": "NUMBER", "filterable": True, "standard": False},
    {"name": "notes", "displayName": "Notes", "type": "TEXT_FIELD", "filterable": False, "standard": True},
]


_ACCOUNT = {"id": 20, "name": "Kylas Devoloper", "businessName": "Kylas Devs"}
_THAKARE = {"field": "lastName", "type": "string", "operator": "equal", "value": "Thakare"}


def _rules(*rules):
    return {"jsonRule": {"condition": "AND", "valid": True, "rules": list(rules)}}


def _act(activity_id, activity_type="EMAIL", status="PROCESSED", engagement=0, expense=0.0, budget=0.0,
         started="2026-10-01T00:00:00.000Z", ended=None, entity="LEAD", rules=None, payload=None, currency=431):
    if payload is None:
        payload = ({"type": "EMAIL", "emailTemplate": {"id": 812, "name": "Lead creation"}, "sentTo": "PRIMARY_EMAIL"}
                   if activity_type == "EMAIL" else
                   {"type": "WHATSAPP", "connectedAccount": _ACCOUNT,
                    "whatsappTemplate": {"id": 853, "name": "Diwali Special Offer from Kylas!"},
                    "sentTo": "PRIMARY_PHONE_NUMBER", "retryConfig": {"noOfTimes": 2}}
                   if activity_type == "WHATSAPP" else {"type": activity_type})
    return {
        "id": activity_id, "name": f"Activity {activity_id}", "type": activity_type, "status": status,
        "entity": entity, "totalEngagement": engagement,
        "actualExpense": {"currencyId": currency, "value": expense},
        "estimatedBudget": {"currencyId": currency, "value": budget},
        "startedAt": started if status != "DRAFT" else None, "endedAt": ended,
        "filters": _rules(*(rules or [_THAKARE])),
        "payload": payload,
    }


def _campaign(campaign_id, activities, started="2026-10-01T00:00:00.000Z", ended=None, status="IN_PROGRESS",
              budget=10000.0, currency=431):
    return {"id": campaign_id, "name": f"Campaign {campaign_id}", "status": status,
            "estimatedBudget": {"currencyId": currency, "value": budget},
            "startedAt": started, "endedAt": ended, "activities": activities}


# The four live QA campaigns from the design discussion, with fixed end dates so days are exact.
_END = "2026-11-03T00:00:00.000Z"
_LIVE = [
    _campaign(372, [_act(3721, engagement=35, expense=2000), _act(3722, engagement=70, expense=20000),
                    _act(3723, status="DRAFT", expense=11)],
              started="2026-03-23T00:00:00.000Z", ended=_END),
    _campaign(433, [_act(4331, "WHATSAPP", engagement=8, expense=790, budget=2000)],
              started="2026-10-13T00:00:00.000Z", ended=_END),
    _campaign(425, [_act(4251, "WHATSAPP", engagement=8, expense=27010)],
              started="2026-08-12T00:00:00.000Z", ended=_END),
    _campaign(442, [_act(4421, engagement=2, expense=3000)], started="2026-10-26T00:00:00.000Z", ended=_END),
]


class _PerformanceApi:
    """Stands in for _campaign_call: campaign search pages and currencies, recording every request."""

    def __init__(self, campaigns, total=None):
        self.campaigns = campaigns
        self.total = len(campaigns) if total is None else total
        self.calls = []

    async def __call__(self, method, path, op, *, params=None, body=None, allow_empty=False):
        self.calls.append({"method": method, "path": path, "params": params, "body": body})
        if path == "/v1/forex/currencies":
            return _CURRENCIES
        assert path == "/v1/campaigns/search" and method == "POST"
        start = params["page"] * params["size"]
        return {"content": self.campaigns[start:start + params["size"]], "totalElements": self.total}

    def searches(self):
        return [c for c in self.calls if c["path"] == "/v1/campaigns/search"]


class _Patched:
    def __init__(self, campaigns, total=None, lead_fields=_LEAD_FIELDS):
        self.api = _PerformanceApi(campaigns, total)
        self.lead_fields = AsyncMock(return_value=lead_fields) if not isinstance(lead_fields, Exception) \
            else AsyncMock(side_effect=lead_fields)
        self.contact_fields = AsyncMock(return_value=[])
        self.patches = [
            patch("app.campaigns._campaign_call", self.api),
            patch("app.campaigns._resolve_timezone", AsyncMock(return_value=TZ)),
            patch.dict(campaigns_module._AUDIENCE_FIELD_FETCHERS,
                       {"LEAD": self.lead_fields, "CONTACT": self.contact_fields}),
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.stop()


async def _run(payload=None, campaigns=_LIVE, **kw):
    with _Patched(campaigns, **kw) as patched:
        result = await campaign_performance_logic(payload or {})
    return result, patched


# ---------------------------------------------------------------------------
# Numbers and scores
# ---------------------------------------------------------------------------


def test_numbers_count_days_actually_run_and_speed_uses_at_least_seven_days():
    now = datetime(2026, 11, 4, tzinfo=timezone.utc)
    new = _act(1, engagement=6, expense=0, started="2026-11-03T00:00:00.000Z")
    numbers = _performance_numbers(new, [new], now)
    assert numbers["days"] == 1.0
    assert numbers["per_day"] == pytest.approx(6 / 7)  # a day-old campaign isn't "6 a day"
    assert numbers["cost_per_engagement"] is None  # expense 0 -> no cost data, never "free"


def test_position_scores_best_100_worst_0_ties_share():
    assert _position_scores([105, 8, 8, 2]) == [100.0, 50.0, 50.0, 0.0]
    assert _position_scores([210, 98.75, None], higher_is_better=False) == [0.0, 100.0, None]
    assert _position_scores([5]) == [100.0]
    assert _position_scores([5], alone=50.0) == [50.0]


@pytest.mark.asyncio
async def test_balanced_ranking_matches_the_worked_example():
    result, _ = await _run()
    rows = {r["id"]: r for r in result["top"]}
    assert [r["id"] for r in result["top"]] == [372, 433, 425, 442]
    assert [rows[i]["score"] for i in (372, 433, 425, 442)] == [86.7, 72.5, 22.5, 18.3]
    assert rows[433]["parts"] == {"engagement": 50.0, "cost": 100.0, "speed": 66.7}
    assert rows[372]["engagement"] == 105 and rows[372]["cost_per_engagement"] == 209.52
    assert rows[372]["days_running"] == 225.0 and rows[372]["actual_expense"] == {"value": 22000.0, "currency": "INR"}
    assert result["recommended_campaign_id"] == 372 and result["compared"] == 4
    assert result["ranked_by"].startswith("balanced: engagement 45%, cost per engagement 40%, engagement per day 15%")


@pytest.mark.asyncio
async def test_draft_activities_and_their_expense_are_left_out_and_call_is_listed():
    campaign = _campaign(1, [_act(11, engagement=10, expense=100), _act(12, status="DRAFT", expense=5000),
                             _act(13, "CALL", engagement=99, expense=50)])
    result, _ = await _run(campaigns=[campaign])
    row = result["top"][0]
    assert row["engagement"] == 10 and row["actual_expense"]["value"] == 100.0
    assert [a["id"] for a in row["activities"]] == [11]
    assert row["not_scored"] == ["EMAIL activity 'Activity 12': never ran (DRAFT)",
                                 "CALL activity 'Activity 13': call results aren't in this data"]


@pytest.mark.asyncio
async def test_new_campaigns_are_kept_and_never_started_ones_are_skipped():
    day = "2026-11-03T00:00:00.000Z"
    today = _campaign(2, [_act(21, engagement=6, started=day)], started=day)
    never = _campaign(3, [_act(31, status="DRAFT")], started=None, status="COMPLETED")
    only_call = _campaign(4, [_act(41, "CALL")])
    result, _ = await _run(campaigns=[today, never, only_call])
    assert [r["id"] for r in result["top"]] == [2]
    assert result["skipped"] == {"never_started": 1, "nothing_to_score": 1}


@pytest.mark.asyncio
async def test_zero_expense_gives_a_partial_score_with_a_neutral_cost_part():
    paid = _campaign(1, [_act(11, engagement=10, expense=100)])
    free = _campaign(2, [_act(21, engagement=10, expense=0)])
    result, _ = await _run(campaigns=[paid, free])
    rows = {r["id"]: r for r in result["top"]}
    assert rows[2]["parts"]["cost"] is None
    assert rows[2]["score_quality"] == "partial: no cost data (actual expense is 0)"
    # The lone campaign with cost data isn't rewarded for being alone either.
    assert rows[1]["parts"]["cost"] == 50.0 and rows[1]["score_quality"] == "full"


@pytest.mark.asyncio
async def test_campaigns_without_engagement_are_never_recommended():
    result, _ = await _run(campaigns=[_campaign(1, [_act(11, engagement=0, expense=500)])])
    assert result["recommended_campaign_id"] is None
    assert result["top"][0]["score_quality"] == "partial: no engagement"
    assert "none can be recommended" in result["note"]


@pytest.mark.asyncio
async def test_cost_is_only_compared_within_one_currency():
    inr = _campaign(1, [_act(11, engagement=10, expense=1000)])
    inr_cheap = _campaign(2, [_act(21, engagement=10, expense=100)])
    usd = _campaign(3, [_act(31, engagement=10, expense=5, currency=400)], currency=400)
    result, _ = await _run(campaigns=[inr, inr_cheap, usd])
    rows = {r["id"]: r for r in result["top"]}
    assert rows[2]["parts"]["cost"] == 100.0 and rows[1]["parts"]["cost"] == 0.0
    assert rows[3]["parts"]["cost"] == 50.0  # alone in USD
    assert rows[3]["actual_expense"]["currency"] == "USD"
    assert "different currencies" in result["currencies_note"]


@pytest.mark.parametrize("rank_by,order", [
    ("engagement", [372, 433, 425, 442]),  # 433 and 425 tie on 8 reads: the higher balanced score goes first
    ("cost", [433, 372, 442, 425]),
    ("speed", [372, 433, 442, 425]),
])
@pytest.mark.asyncio
async def test_rank_by_one_measure_when_the_user_asks(rank_by, order):
    result, _ = await _run({"rank_by": rank_by})
    assert [r["id"] for r in result["top"]] == order
    assert result["ranked_by"] == rank_by


@pytest.mark.asyncio
async def test_top_limits_the_detailed_campaigns():
    result, _ = await _run({"top": 2})
    assert [r["id"] for r in result["top"]] == [372, 433] and result["compared"] == 4


@pytest.mark.asyncio
async def test_channel_compares_only_that_channels_activities():
    mixed = _campaign(1, [_act(11, "EMAIL", engagement=100, expense=100),
                          _act(12, "WHATSAPP", engagement=3, expense=30)])
    email_only = _campaign(2, [_act(21, "EMAIL", engagement=50, expense=50)])
    result, _ = await _run({"channel": "whatsapp"}, campaigns=[mixed, email_only])
    assert [r["id"] for r in result["top"]] == [1]
    assert result["top"][0]["engagement"] == 3
    assert [a["id"] for a in result["top"][0]["activities"]] == [12]
    assert list(result["best_activity_per_channel"]) == ["WHATSAPP"]
    assert result["channel"] == "WHATSAPP"


@pytest.mark.asyncio
async def test_best_activity_per_channel_across_all_campaigns():
    result, _ = await _run()
    best = result["best_activity_per_channel"]
    assert best["EMAIL"]["id"] == 3722 and best["EMAIL"]["campaign"] == {"id": 372, "name": "Campaign 372"}
    assert best["WHATSAPP"]["id"] == 4331  # same reads as 4251, far cheaper
    assert best["WHATSAPP"]["setup"]["whatsapp_template"] == {"id": 853, "name": "Diwali Special Offer from Kylas!"}


@pytest.mark.parametrize("payload,message", [
    ({"channel": "CALL"}, "'channel' must be EMAIL or WHATSAPP"),
    ({"rank_by": "fastest"}, "'rank_by' must be one of"),
    ({"top": 0}, "'top' must be a whole number from 1 to 10"),
    ({"top": 11}, "'top' must be a whole number from 1 to 10"),
    ({"top": True}, "'top' must be a whole number"),
    ({"started_from": "2026-10-01", "started_to": "2026-09-01"}, "'started_from' must be on or before"),
    ({"started_from": "1 Oct"}, "'started_from' must be a calendar date"),
])
@pytest.mark.asyncio
async def test_bad_inputs_are_refused_before_any_call(payload, message):
    with _Patched(_LIVE) as patched:
        with pytest.raises(ValueError, match=message):
            await campaign_performance_logic(payload)
    assert patched.api.calls == []


@pytest.mark.asyncio
async def test_nothing_to_compare_says_so():
    result, _ = await _run(campaigns=[])
    assert result["compared"] == 0 and result["top"] == []
    assert "nothing to compare" in result["note"]


# ---------------------------------------------------------------------------
# Search request and paging
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_asks_only_for_started_campaigns_in_the_date_range():
    _, patched = await _run({"started_from": "2026-08-01", "started_to": "2026-10-31"})
    search = patched.api.searches()[0]
    assert search["params"] == {"page": 0, "size": 50, "sort": "id,asc"}
    rules = search["body"]["jsonRule"]["rules"]
    assert rules[0]["field"] == "status" and rules[0]["value"] == ["IN_PROGRESS", "PAUSED", "PROCESSED", "COMPLETED"]
    # Local dates in Asia/Calcutta: from the start of 1 Aug, to the end of 31 Oct.
    assert (rules[1]["field"], rules[1]["operator"], rules[1]["value"]) == \
        ("startedAt", "greater_or_equal", "2026-07-31T18:30:00.000Z")
    assert (rules[2]["field"], rules[2]["operator"], rules[2]["value"]) == \
        ("startedAt", "less_or_equal", "2026-10-31T18:29:59.999Z")


@pytest.mark.asyncio
async def test_every_page_is_read():
    many = [_campaign(i, [_act(i * 10, engagement=i, expense=i)]) for i in range(1, 121)]
    result, patched = await _run(campaigns=many)
    assert sorted(c["params"]["page"] for c in patched.api.searches()) == [0, 1, 2]
    assert result["compared"] == 120 and result["top"][0]["id"] == 120
    assert "only_first" not in result


@pytest.mark.asyncio
async def test_stops_at_500_campaigns_and_says_so():
    many = [_campaign(i, [_act(i * 10, engagement=1, expense=1)]) for i in range(1, 601)]
    result, patched = await _run(campaigns=many)
    assert len(patched.api.searches()) == 10
    assert result["compared"] == 500 and result["only_first"] == "Compared the first 500 of 600 started campaigns."


@pytest.mark.asyncio
async def test_the_field_list_is_read_once_per_entity_shown():
    lead = _campaign(1, [_act(11, engagement=5, expense=10), _act(12, "WHATSAPP", engagement=4, expense=10)])
    result, patched = await _run(campaigns=[lead])
    assert patched.lead_fields.await_count == 1
    assert patched.contact_fields.await_count == 0  # no CONTACT activity shown
    assert [c["path"] for c in patched.api.calls] == ["/v1/campaigns/search", "/v1/forex/currencies"]


# ---------------------------------------------------------------------------
# Activity setup and audience rebuild
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_activity_setup_has_what_create_needs():
    result, _ = await _run()
    whatsapp = next(r for r in result["top"] if r["id"] == 433)["activities"][0]
    assert whatsapp == {
        "id": 4331, "name": "Activity 4331", "type": "WHATSAPP", "status": "PROCESSED", "entity": "LEAD",
        "engagement": 8, "actual_expense": 790.0, "budget": 2000.0, "budget_share": "20%",
        "sent_to": "PRIMARY_PHONE_NUMBER",
        "whatsapp_account": {"id": 20, "name": "Kylas Devoloper", "business_name": "Kylas Devs"},
        "whatsapp_template": {"id": 853, "name": "Diwali Special Offer from Kylas!"}, "retry_times": 2,
        "audience": {"audience_filters": [{"field": "lastName", "operator": "equal", "value": "Thakare"}],
                     "readable": ["Last Name equal Thakare"], "cannot_reuse": [], "warnings": []},
    }
    email = next(r for r in result["top"] if r["id"] == 372)["activities"][0]
    assert email["email_template"] == {"id": 812, "name": "Lead creation"} and email["sent_to"] == "PRIMARY_EMAIL"


def test_rebuild_turns_saved_rules_into_create_input():
    rules = [
        {"field": "lastName", "type": "string", "operator": "equal", "value": "Thakare"},
        {"field": "id", "type": "double", "operator": "in", "value": "297933, 296693"},
        {"field": "customFieldValues.cfNumber", "type": "double", "operator": "equal", "value": "111"},
        {"field": "source", "type": "long", "operator": "equal", "value": 3812},
        {"field": "createdAt", "type": "date", "operator": "current_month", "value": None},
        {"field": "updatedAt", "type": "date", "operator": "greater", "value": 1774240200000},
    ]
    result = rebuild_audience("LEAD", rules, _LEAD_FIELDS, TZ)
    assert result["audience_filters"] == [
        {"field": "lastName", "operator": "equal", "value": "Thakare"},
        {"field": "id", "operator": "in", "value": [297933, 296693]},
        {"field": "cfNumber", "operator": "equal", "value": 111},
        {"field": "source", "operator": "equal", "value": 3812},
        {"field": "createdAt", "operator": "current_month", "value": None},
        {"field": "updatedAt", "operator": "greater", "value": "2026-03-23T10:00:00"},  # 04:30 UTC in IST
    ]
    assert result["readable"] == [
        "Last Name equal Thakare", "ID in 297933, 296693", "Lucky Number equal 111", "Source equal Website",
        "Created At current month", "Updated At greater 2026-03-23T10:00:00",
    ]
    assert result["cannot_reuse"] == []
    assert result["warnings"] == [
        "'ID in 297933, 296693' targets specific records by id, so it reaches only those people.",
        "'Updated At greater 2026-03-23T10:00:00' uses a fixed date; for a new campaign a newer date or a relative "
        "one (e.g. last_thirty_days) may fit better.",
    ]


@pytest.mark.asyncio
async def test_rebuilt_filters_pass_campaign_activity_creates_own_check():
    rules = [
        {"field": "lastName", "type": "string", "operator": "equal", "value": "Thakare"},
        {"field": "customFieldValues.cfNumber", "type": "double", "operator": "equal", "value": 111},
        {"field": "updatedAt", "type": "date", "operator": "greater", "value": 1774240200000},
    ]
    filters = rebuild_audience("LEAD", rules, _LEAD_FIELDS, TZ)["audience_filters"]
    with patch.dict(campaigns_module._AUDIENCE_FIELD_FETCHERS, {"LEAD": AsyncMock(return_value=_LEAD_FIELDS)}):
        json_rule = await build_audience_rule("LEAD", filters, TZ)
    built = {r["id"]: r for r in json_rule["rules"]}
    # Kylas gets back the same conditions it saved: the custom-field path and the same instant.
    assert built["cfNumber"]["field"] == "customFieldValues.cfNumber" and built["cfNumber"]["value"] == 111
    assert built["updatedAt"]["value"].startswith("2026-03-23T04:30:00")


def test_rules_that_cant_be_rebuilt_are_listed_with_the_reason():
    rules = [
        {"field": "multi_field", "type": "multi_field", "operator": "multi_field", "value": "new"},
        {"field": "addressCoordinate", "type": "GpsCoordinates", "operator": "range_within",
         "value": {"lat": 18.5, "lon": 73.8, "unit": "KILOMETERS", "radius": 8, "address": "Pune"}},
        {"field": "notes", "type": "string", "operator": "contains", "value": "vip"},
        {"field": "lastName", "type": "string", "operator": "greater", "value": "A"},
    ]
    result = rebuild_audience("LEAD", rules, _LEAD_FIELDS, TZ)
    assert result["audience_filters"] == []
    assert [c["condition"] for c in result["cannot_reuse"]] == [
        "search text 'new'", "addressCoordinate within 8 kilometers of Pune", "Notes contains vip",
        "Last Name greater A"]
    assert "Search-box text" in result["cannot_reuse"][0]["why"]
    assert "Map-radius" in result["cannot_reuse"][1]["why"]
    assert "isn't a filterable lead field" in result["cannot_reuse"][2]["why"]
    assert "operator 'greater' not allowed" in result["cannot_reuse"][3]["why"]
    assert result["warnings"] == ["Nothing here can be reused as is: ask the user for the new audience."]


@pytest.mark.asyncio
async def test_unreadable_field_list_keeps_the_ranking_and_asks_for_the_audience():
    result, _ = await _run(lead_fields=RuntimeError("fields down"))
    assert [r["id"] for r in result["top"]] == [372, 433, 425, 442]
    audience = result["top"][0]["activities"][0]["audience"]
    assert audience["audience_filters"] == []
    assert "field list couldn't be read" in audience["warnings"][0]


# ---------------------------------------------------------------------------
# Registry wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_payload_and_execute_request_wire_campaign_performance():
    from app.tools import build_payload, execute_request, list_tool

    rows = json.loads(list_tool(bucket="campaign", intent="analytics"))["results"]
    assert "campaign.performance" in [r["id"] for r in rows]
    schema = json.loads(await build_payload("campaign.performance"))["schema"]
    assert schema["properties"]["rank_by"]["enum"] == ["balanced", "engagement", "cost", "speed"]
    assert schema["properties"]["channel"]["enum"] == ["EMAIL", "WHATSAPP"]
    assert "confirm" not in schema["properties"]  # read-only

    with _Patched(_LIVE):
        reply = json.loads(await execute_request("campaign.performance", {"top": 1}))
    assert reply["ok"] is True and reply["data"]["top"][0]["id"] == 372
