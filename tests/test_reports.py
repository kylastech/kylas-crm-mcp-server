"""Tests for report.create (app/reports.py) — phase 1, ONE_DIMENSIONAL.

The config fixture is a trimmed copy of a real GET /v1/reports/config/leads
response; payload expectations mirror what sd-ui's saveReport sends.

Run: pytest tests/ -v
"""

import copy
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import reports
from app.reports import build_report_body, get_report_config_instructions_logic
from app.tools import build_payload, execute_request

METRICS = [
    {"type": "COUNT", "field": "id", "header": "Id"},
    {"type": "SUM", "field": "requirementBudget", "header": "Budget"},
    {"type": "AVERAGE", "field": "requirementBudget", "header": "Budget"},
    {"type": "COUNT", "field": "pipelineStageCompleted", "header": "pipelineStageCompleted"},
    {"type": "COUNT", "field": "pipelineStageSkipped", "header": "pipelineStageSkipped"},
    {"type": "COUNT", "field": "cfVillage", "header": "Village"},
]


def _dim(id_, header, field_type, **extra):
    return {"id": id_, "header": header, "supportedMetrics": METRICS, "requiredFilters": [],
            "filterable": True, "active": True, "fieldType": field_type, "lookup": None,
            "primaryField": None, "property": None, **extra}


def _flt(id_, header, field_type, picklist=None, is_internal=False, **extra):
    return {"id": id_, "header": header, "fieldType": field_type, "lookup": None,
            "picklist": {"picklistValues": picklist} if picklist is not None else None,
            "showDefaultOptions": False, "filterable": True, "active": True, "requiredFilters": [],
            "primaryField": None, "property": None, "isInternal": is_internal, "isStandard": True, **extra}


USER_LOOKUP = {"entity": "USER", "lookupUrl": "/users/lookup?q=firstName:"}
COUNTRIES = [{"id": 175, "name": "IN", "displayName": "India"}, {"id": 308, "name": "US", "displayName": "United States"}]
TIMEZONES = [{"id": 372, "name": "Asia/Calcutta", "displayName": "(GMT+05:30) Kolkata"}]

LEAD_CONFIG = {
    "entity": "lead",
    "dimensions": [
        _dim("source", "Source", "PICK_LIST"),
        _dim("ownerId", "Owner", "LOOK_UP", lookup=USER_LOOKUP),
        _dim("createdAt", "Created At", "DATETIME_PICKER"),
        _dim("ownerFields", "Owner Fields", "ENTITY_FIELDS", requiredFilters=["ownerId"],
             primaryField="ownerId", property="teams", lookup={"entity": "USER", "lookupUrl": ""}),
        _dim("pipelineStage", "Pipeline Stage", "PIPELINE_STAGE", requiredFilters=["pipeline"]),
        _dim("cfCompanySize", "Company size", "TEXT_FIELD", filterable=False),
        _dim("organizerFields", "Organizer Fields", "ENTITY_FIELDS"),
    ],
    "filters": [
        _flt("country", "Country", "PICK_LIST", COUNTRIES, is_internal=True),
        _flt("timezone", "Timezone", "PICK_LIST", TIMEZONES, is_internal=True),
        _flt("source", "Source", "PICK_LIST", [
            {"id": 650646, "name": "GOOGLE", "displayName": "Google"},
            {"id": 650647, "name": "FACEBOOK", "displayName": "Facebook"},
        ]),
        _flt("ownerId", "Owner", "LOOK_UP", lookup=USER_LOOKUP, showDefaultOptions=True),
        _flt("createdAt", "Created At", "DATETIME_PICKER", is_internal=True),
        _flt("updatedAt", "Updated At", "DATETIME_PICKER", is_internal=True),
        _flt("expectedClosureOn", "Expected Closure On", "DATE_PICKER"),
        _flt("score", "Score", "NUMBER", is_internal=True),
        _flt("firstName", "First Name", "TEXT_FIELD"),
        _flt("dnd", "Do Not Disturb", "TOGGLE"),
        _flt("forecastingType", "Forecasting Type", "FORECASTING_TYPE", is_internal=True),
        _flt("pipeline", "Pipeline", "PIPELINE",
             lookup={"entity": "PIPELINE", "lookupUrl": "/pipelines/lookup?entityType=LEAD&q=name:"}),
        _flt("pipelineStageReason", "Pipeline Stage Reason", "PIPELINE_STAGE_REASON", requiredFilters=["pipeline"]),
        _flt("campaignActivities", "Campaign Activities", "CAMPAIGN",
             lookup={"entity": "CAMPAIGN", "lookupUrl": "/campaigns/lookup?q=name:"}),
        _flt("cfCountry", "Country", "TEXT_FIELD", filterable=False),
        _flt("cfCompanyDescription", "company description", "TEXT_FIELD", active=False, filterable=False),
    ],
}


def _config():
    cfg = copy.deepcopy(LEAD_CONFIG)
    # Same as _fetch_report_config: organizerFields is dropped.
    cfg["dimensions"] = [d for d in cfg["dimensions"] if d["id"] != "organizerFields"]
    return cfg


def _payload(**overrides):
    base = {
        "entity": "lead",
        "name": "Leads by source",
        "chart_type": "bar",
        "group_by": {"field": "source"},
        "metrics": [{"type": "COUNT", "field": "id"}],
        "date_range": {"field": "createdAt", "operator": "current_month"},
        "filters": [],
    }
    base.update(overrides)
    return base


def _build(payload, currency="INR", tz="Asia/Kolkata"):
    return build_report_body(payload, _config(), tz, currency)


# ---------------------------------------------------------------------------
# Cheat sheet
# ---------------------------------------------------------------------------

def test_reference_trims_large_picklists_only_in_filters():
    text = get_report_config_instructions_logic("lead", _config(), set(), "INR")
    assert "India" not in text and "Asia/Calcutta" not in text
    assert 'build_payload("report.create", entity="lead", fields=["country"])' in text
    # small picklists stay inline
    assert "Google (id: 650646, name: 'GOOGLE')" in text


def test_reference_requested_picklist_is_inlined():
    text = get_report_config_instructions_logic("lead", _config(), {"COUNTRY"}, "INR")
    assert "India (id: 175, name: 'IN')" in text
    assert "Asia/Calcutta" not in text  # timezone still trimmed


def test_reference_hides_unusable_and_marks_phase2_filters():
    text = get_report_config_instructions_logic("lead", _config(), set(), "INR")
    assert "cfCountry" not in text and "cfCompanyDescription" not in text and "cfCompanySize" not in text
    assert "'campaignActivities' Campaign Activities [CAMPAIGN] — NOT SUPPORTED YET" in text
    assert "resolve_via: user.lookup" in text
    assert "resolve_via: pipeline.lookup with entity_type=LEAD" in text
    # funnel-only metrics never offered
    assert "pipelineStageCompleted" not in text


def test_reference_financial_options_follow_tenant_currency():
    inr = get_report_config_instructions_logic("lead", _config(), set(), "INR")
    usd = get_report_config_instructions_logic("lead", _config(), set(), "USD")
    assert "current_financial_year" in inr and "FINANCIAL_YEARLY" in inr
    assert "current_financial_year" not in usd and "FINANCIAL_YEARLY" not in usd


def test_date_operators_per_entity():
    assert "last_n_days" not in reports.date_operators_for("meeting", "INR")
    assert "current_month" in reports.date_operators_for("meeting", "INR")
    email_ops = reports.date_operators_for("email", "USD")
    assert "next_week" not in email_ops and "current_financial_quarter" in email_ops


# ---------------------------------------------------------------------------
# Short payload -> full body
# ---------------------------------------------------------------------------

def test_build_body_matches_sd_ui_shape():
    body, notes = _build(_payload(filters=[
        {"field": "country", "operator": "equal", "value": "IN"},
        {"field": "score", "operator": "greater", "value": 50},
    ]))
    assert notes == []
    assert body == {
        "name": "Leads by source", "description": "", "prorated": False, "goal": None,
        "config": {
            "groupBy": [{"name": "source", "format": None, "primaryField": None, "property": None}],
            "metrics": [{"type": "COUNT", "field": "id"}],
            "dateRange": {"id": "createdAt", "field": "createdAt", "operator": "current_month", "type": "date",
                          "fieldInputType": "DATETIME_PICKER", "value": None, "from": "00:00", "to": "23:59"},
            "rules": [
                {"operator": "equal", "id": "country", "field": "country", "fieldInputType": "PICK_LIST",
                 "type": "string", "value": "IN", "from": None, "to": None, "property": None, "primaryField": None},
                {"operator": "greater", "id": "score", "field": "score", "fieldInputType": "NUMBER",
                 "type": "double", "value": 50, "from": None, "to": None, "property": None, "primaryField": None},
            ],
        },
        "reportType": "lead", "chartType": "bar", "category": "ONE_DIMENSIONAL",
    }


def test_picklist_accepts_label_and_sends_id_or_name():
    body, _ = _build(_payload(filters=[
        {"field": "country", "operator": "in", "value": ["India", "us"]},
        {"field": "source", "operator": "equal", "value": "Google"},
    ]))
    country, source = body["config"]["rules"]
    assert country["value"] == ["IN", "US"] and country["type"] == "string"
    assert source["value"] == 650646 and source["type"] == "long"


def test_multi_picklist_follows_is_internal_like_ui():
    cfg = _config()
    tags = [{"id": 1, "name": "HOT", "displayName": "Hot"}, {"id": 2, "name": "COLD", "displayName": "Cold"}]
    cfg["filters"] += [_flt("cfTags", "Tags", "MULTI_PICKLIST", tags),
                       _flt("cfInternalTags", "Internal Tags", "MULTI_PICKLIST", tags, is_internal=True)]
    body, _ = build_report_body(_payload(filters=[
        {"field": "cfTags", "operator": "in", "value": ["Hot"]},
        {"field": "cfInternalTags", "operator": "equal", "value": "Cold"},
    ]), cfg, "Asia/Kolkata", "INR")
    tags_rule, internal_rule = body["config"]["rules"]
    assert tags_rule["value"] == [1] and tags_rule["type"] == "long"
    assert internal_rule["value"] == "COLD" and internal_rule["type"] == "long"


def test_unknown_picklist_option_rejected():
    with pytest.raises(ValueError, match="not an option"):
        _build(_payload(filters=[{"field": "country", "operator": "equal", "value": "Atlantis"}]))


def test_multiple_metrics_force_table():
    body, notes = _build(_payload(metrics=[{"type": "COUNT", "field": "id"}, {"type": "SUM", "field": "requirementBudget"}]))
    assert body["chartType"] == "table"
    assert notes and "table" in notes[0]


def test_funnel_only_metric_rejected():
    with pytest.raises(ValueError, match="not available"):
        _build(_payload(metrics=[{"type": "COUNT", "field": "pipelineStageCompleted"}]))


def test_date_dimension_requires_format_and_respects_currency():
    with pytest.raises(ValueError, match="format is required"):
        _build(_payload(group_by={"field": "createdAt"}))
    body, _ = _build(_payload(group_by={"field": "createdAt", "format": "monthly"}))
    assert body["config"]["groupBy"][0]["format"] == "MONTHLY"
    with pytest.raises(ValueError, match="format is required"):
        _build(_payload(group_by={"field": "createdAt", "format": "FINANCIAL_YEARLY"}), currency="USD")


def test_non_filterable_dimension_rejected():
    with pytest.raises(ValueError, match="not a dimension"):
        _build(_payload(group_by={"field": "cfCompanySize"}))


def test_entity_fields_dimension_adds_primary_filter():
    body, notes = _build(_payload(group_by={"field": "ownerFields"}))
    assert body["config"]["groupBy"][0] == {"name": "ownerFields", "format": None, "primaryField": "ownerId", "property": "teams"}
    rule = body["config"]["rules"][-1]
    assert rule["id"] == "ownerId" and rule["operator"] == "is_not_null" and rule["value"] is None
    assert any("ownerId is_not_null" in n for n in notes)


def test_dimension_required_filter_enforced():
    with pytest.raises(ValueError, match="requires a 'pipeline' filter"):
        _build(_payload(group_by={"field": "pipelineStage"}))
    body, _ = _build(_payload(group_by={"field": "pipelineStage"},
                              filters=[{"field": "pipeline", "operator": "equal", "value": "12"}]))
    assert body["config"]["rules"][0]["value"] == 12 and body["config"]["rules"][0]["type"] == "long"


def test_related_filter_requires_parent_and_pipeline_operator():
    with pytest.raises(ValueError, match="requires a 'pipeline' filter"):
        _build(_payload(filters=[{"field": "pipelineStageReason", "operator": "contains", "value": "price"}]))
    with pytest.raises(ValueError, match="must use 'equal' or 'in'"):
        _build(_payload(filters=[
            {"field": "pipeline", "operator": "not_equal", "value": 12},
            {"field": "pipelineStageReason", "operator": "contains", "value": "price"},
        ]))


def test_date_range_field_cannot_be_filter():
    with pytest.raises(ValueError, match="already the date_range field"):
        _build(_payload(filters=[{"field": "createdAt", "operator": "today"}]))


def test_duplicate_and_unsupported_filters_rejected():
    with pytest.raises(ValueError, match="only once"):
        _build(_payload(filters=[{"field": "firstName", "operator": "contains", "value": "a"},
                                 {"field": "firstName", "operator": "contains", "value": "b"}]))
    with pytest.raises(ValueError, match="not supported yet"):
        _build(_payload(filters=[{"field": "campaignActivities", "operator": "equal", "value": 1}]))


def test_operator_validation_uses_field_type():
    with pytest.raises(ValueError, match="not allowed"):
        _build(_payload(filters=[{"field": "dnd", "operator": "contains", "value": True}]))
    with pytest.raises(ValueError, match="not allowed for date fields"):
        _build(_payload(filters=[{"field": "updatedAt", "operator": "greater", "value": "2026-01-01"}]))


def test_types_without_ui_input_only_take_valueless_operators():
    cfg = _config()
    cfg["filters"] += [_flt("notes", "Notes", "PARAGRAPH_TEXT"), _flt("uuid", "UUID", "UUID")]
    ok, _ = build_report_body(_payload(filters=[{"field": "notes", "operator": "is_not_empty"}]), cfg, "Asia/Kolkata", "INR")
    assert ok["config"]["rules"][0]["value"] is None
    with pytest.raises(ValueError, match="not allowed"):
        build_report_body(_payload(filters=[{"field": "notes", "operator": "contains", "value": "x"}]), cfg, "Asia/Kolkata", "INR")
    # UUID offers no value-less operator at all -> not usable
    assert reports.filter_support(_flt("uuid", "UUID", "UUID")) == (False, None)
    text = get_report_config_instructions_logic("lead", cfg, set(), "INR")
    assert "'notes' Notes [PARAGRAPH_TEXT] — operators: is_empty, is_not_empty" in text


def test_between_converts_local_dates_to_utc():
    body, _ = _build(_payload(date_range={"field": "createdAt", "operator": "between",
                                          "value": ["2026-01-01", "2026-01-31"]}))
    dr = body["config"]["dateRange"]
    assert dr["value"] == ["2025-12-31T18:30:00.000Z", "2026-01-31T18:29:59.999Z"]
    assert "from" not in dr and "to" not in dr


def test_between_span_limit():
    with pytest.raises(ValueError, match="at most 1825 days"):
        _build(_payload(date_range={"field": "createdAt", "operator": "between", "value": ["2015-01-01", "2026-01-01"]}))


def test_n_days_and_date_picker_filter():
    body, _ = _build(_payload(
        date_range={"field": "createdAt", "operator": "last_n_days", "value": 10},
        filters=[{"field": "expectedClosureOn", "operator": "next_month"}],
    ))
    assert body["config"]["dateRange"]["value"] == 10 and body["config"]["dateRange"]["from"] == "00:00"
    rule = body["config"]["rules"][0]
    assert rule["type"] == "date" and rule["value"] is None and rule["from"] is None  # DATE_PICKER: no time
    with pytest.raises(ValueError, match="between 1 and 364"):
        _build(_payload(date_range={"field": "createdAt", "operator": "last_n_days", "value": 400}))


def test_value_shapes_per_field_type():
    body, _ = _build(_payload(filters=[
        {"field": "firstName", "operator": "in", "value": [" Ram ", "Shyam"]},
        {"field": "score", "operator": "between", "value": [10, "20.5"]},
        {"field": "dnd", "operator": "equal", "value": "yes"},
        {"field": "forecastingType", "operator": "in", "value": ["won", "OPEN"]},
        {"field": "ownerId", "operator": "is_null", "value": 5},
    ]))
    first, score, dnd, forecast, owner = body["config"]["rules"]
    assert first["value"] == "Ram,Shyam"
    assert score["value"] == [10, 20.5]
    assert dnd["value"] is True and dnd["type"] == "boolean"
    assert forecast["value"] == ["CLOSED_WON", "OPEN"] and forecast["type"] == "string"
    assert owner["value"] is None


def test_basic_validation_errors():
    with pytest.raises(ValueError, match="name is required"):
        _build(_payload(name="ab"))
    with pytest.raises(ValueError, match="ONE_DIMENSIONAL"):
        _build(_payload(category="MULTI_DIMENSIONAL"))
    with pytest.raises(ValueError, match="Unknown report entity"):
        _build(_payload(entity="widgets"))


def test_permissions_helpers():
    perms = [{"name": "lead", "action": {"read": True}}, {"name": "deal", "action": {"readAll": True}},
             {"name": "report", "action": {"write": True}}]
    assert reports.readable_report_entities(perms) == ["lead", "deal", "meeting"]
    assert reports.can_create_report(perms) is True
    assert reports.can_create_report([]) is False


# ---------------------------------------------------------------------------
# Wiring: build_payload / execute_request
# ---------------------------------------------------------------------------

def _mock_client(routes, posted):
    """A get_client() replacement answering GETs from `routes` (path suffix -> json) and recording POSTs."""
    def _resp(data):
        r = MagicMock()
        r.json.return_value = data
        r.raise_for_status = MagicMock()
        return r

    async def _get(url, **kwargs):
        for suffix, data in routes.items():
            if url.endswith(suffix):
                return _resp(data)
        raise AssertionError(f"unexpected GET {url}")

    async def _post(url, json=None, **kwargs):
        posted.append((url, json))
        return _resp({"id": 9876})

    client = AsyncMock()
    client.get.side_effect = _get
    client.post.side_effect = _post
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    return MagicMock(return_value=client)


ROUTES = {
    "/users/me/permissions": [{"name": "lead", "action": {"read": True}}, {"name": "report", "action": {"write": True}}],
    "/reports/config/leads": LEAD_CONFIG,
    "/tenants": {"currency": "INR"},
    "/users/me": {"id": 1, "timezone": "Asia/Kolkata"},
}


@pytest.mark.asyncio
async def test_build_payload_requires_entity():
    result = json.loads(await build_payload("report.create"))
    assert result["ok"] is False and "entity" in result["error"]


@pytest.mark.asyncio
async def test_build_payload_returns_live_reference():
    with patch("app.reports.get_client", _mock_client(ROUTES, [])):
        result = json.loads(await build_payload("report.create", entity="leads", fields=["country"]))
    assert result["fetched_live"] is True
    assert result["schema"]["entity"] == "lead"
    assert "India (id: 175, name: 'IN')" in result["schema"]["tenant_fields_reference"]
    assert result["method"] == "POST" and result["path"] == "/v3/reports"


@pytest.mark.asyncio
async def test_build_payload_denies_unreadable_entity():
    with patch("app.reports.get_client", _mock_client(ROUTES, [])):
        result = json.loads(await build_payload("report.create", entity="deal"))
    assert result["ok"] is False and "permission to report on 'deal'" in result["error"]


@pytest.mark.asyncio
async def test_execute_request_creates_report():
    posted = []
    with patch("app.reports.get_client", _mock_client(ROUTES, posted)), \
         patch("app.entities.get_client", _mock_client(ROUTES, posted)):
        result = json.loads(await execute_request("report.create", _payload(
            filters=[{"field": "ownerId", "operator": "equal", "value": 42}])))
    assert result["ok"] is True
    assert result["data"]["id"] == 9876 and result["data"]["category"] == "ONE_DIMENSIONAL"
    (url, body), = posted
    assert url.endswith("/v3/reports")
    assert body["config"]["rules"][0]["value"] == 42


@pytest.mark.asyncio
async def test_execute_request_without_report_write_permission():
    routes = dict(ROUTES, **{"/users/me/permissions": [{"name": "lead", "action": {"read": True}}]})
    posted = []
    with patch("app.reports.get_client", _mock_client(routes, posted)):
        result = json.loads(await execute_request("report.create", _payload()))
    assert result["ok"] is False and result["error"]["code"] == "BAD_PAYLOAD"
    assert "permission to create reports" in result["error"]["message"]
    assert posted == []


@pytest.mark.asyncio
async def test_execute_request_bad_payload_never_posts():
    posted = []
    with patch("app.reports.get_client", _mock_client(ROUTES, posted)), \
         patch("app.entities.get_client", _mock_client(ROUTES, posted)):
        result = json.loads(await execute_request("report.create", _payload(group_by={"field": "nope"})))
    assert result["ok"] is False and "not a dimension" in result["error"]["message"]
    assert posted == []
