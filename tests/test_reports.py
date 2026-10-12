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
    assert "campaign id(s)" not in text  # generic hint wording
    assert "resolve_via: campaign.lookup" in text
    assert "resolve_via: user.lookup" in text
    assert "resolve_via: pipeline.lookup with entity_type=LEAD" in text
    # funnel-only metrics never offered
    assert "pipelineStageCompleted" not in text


def test_reference_states_range_limit_and_pipeline_operator_rule():
    text = get_report_config_instructions_logic("lead", _config(), set(), "INR")
    assert "at most 1825 days" in text
    assert "the 'pipeline' filter must then use operator 'equal' or 'in'" in text
    email_text = get_report_config_instructions_logic("email", _config(), set(), "INR")
    assert "at most 90 days" in email_text and "1825" not in email_text


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
    cfg = _config()
    cfg["filters"].append(_flt("addressCoordinate", "Address Coordinate", "GPS_COORDINATES"))
    with pytest.raises(ValueError, match="not supported"):
        build_report_body(_payload(filters=[{"field": "addressCoordinate", "operator": "range_within", "value": 1}]),
                          cfg, "Asia/Kolkata", "INR")


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
    with pytest.raises(ValueError, match="category must be one of"):
        _build(_payload(category="PIVOT"))
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


# ---------------------------------------------------------------------------
# Lookup-backed filters (teams, stages, campaigns, activities, meeting
# participants, linked records, quotation status/approval state)
# ---------------------------------------------------------------------------

def _lookup_config():
    cfg = _config()
    cfg["filters"] += [
        _flt("ownerFields", "Owner Fields", "ENTITY_FIELDS", requiredFilters=["ownerId"], primaryField="ownerId",
             property="teams", lookup={"entity": "USER", "lookupUrl": ""}),
        _flt("pipelineStage", "Pipeline Stage", "PIPELINE_STAGE", requiredFilters=["pipeline"],
             lookup={"entity": "PIPELINE_STAGE", "lookupUrl": ""}),
        _flt("activities", "Activities", "ACTIVITY", requiredFilters=["campaignActivities"],
             lookup={"entity": "ACTIVITY", "lookupUrl": ""}),
    ]
    return cfg


def _build_cfg(cfg, payload, entity="lead"):
    return build_report_body(dict(payload, entity=entity), cfg, "Asia/Kolkata", "INR")


def test_reference_names_each_lookup():
    text = get_report_config_instructions_logic("lead", _lookup_config(), set(), "INR")
    assert "team id(s) — resolve_via: team.lookup" in text and "'ownerId is_not_null' automatically" in text
    assert "stage id(s) — resolve_via: pipeline.details" in text
    assert "activity id(s) — resolve_via: campaign.activities" in text
    assert "resolve_via: campaign.lookup" in text
    assert "NOT SUPPORTED" not in text


def test_team_filter_sets_property_and_primary_is_set():
    body, notes = _build_cfg(_lookup_config(), _payload(filters=[{"field": "ownerFields", "operator": "in", "value": [11, "12"]}]))
    team, primary = body["config"]["rules"]
    assert team == {"operator": "in", "id": "ownerFields", "field": "ownerFields", "fieldInputType": "ENTITY_FIELDS",
                    "type": "long", "value": [11, 12], "from": None, "to": None, "property": "teams", "primaryField": "ownerId"}
    assert primary["id"] == "ownerId" and primary["operator"] == "is_not_null" and primary["value"] is None
    assert any("ownerId is_not_null" in n for n in notes)


def test_team_filter_forces_existing_primary_to_is_set():
    body, notes = _build_cfg(_lookup_config(), _payload(filters=[
        {"field": "ownerId", "operator": "equal", "value": 5},
        {"field": "ownerFields", "operator": "equal", "value": 11},
    ]))
    owner = body["config"]["rules"][0]
    assert owner["operator"] == "is_not_null" and owner["value"] is None
    assert any("changed to is_not_null" in n for n in notes)


def test_pipeline_stage_filter_needs_pipeline():
    with pytest.raises(ValueError, match="requires a 'pipeline' filter"):
        _build_cfg(_lookup_config(), _payload(filters=[{"field": "pipelineStage", "operator": "in", "value": [1, 2]}]))
    body, _ = _build_cfg(_lookup_config(), _payload(filters=[
        {"field": "pipeline", "operator": "equal", "value": 7875},
        {"field": "pipelineStage", "operator": "in", "value": ["101", 102]},
    ]))
    stage = body["config"]["rules"][1]
    assert stage["value"] == [101, 102] and stage["type"] == "long" and stage["fieldInputType"] == "PIPELINE_STAGE"


def test_campaign_and_activities():
    body, _ = _build_cfg(_lookup_config(), _payload(filters=[
        {"field": "campaignActivities", "operator": "equal", "value": 9},
        {"field": "activities", "operator": "in", "value": [31, 32]},
    ]))
    campaign, activities = body["config"]["rules"]
    assert campaign["value"] == 9 and campaign["type"] == "long"
    assert activities["value"] == [31, 32] and activities["type"] == "long"
    with pytest.raises(ValueError, match="must use 'equal'"):
        _build_cfg(_lookup_config(), _payload(filters=[
            {"field": "campaignActivities", "operator": "in", "value": [9]},
            {"field": "activities", "operator": "equal", "value": 31},
        ]))


MEETING_CONFIG = {
    "entity": "meeting",
    "dimensions": [_dim("owner", "Owner", "LOOK_UP", lookup=USER_LOOKUP)],
    "filters": [
        _flt("from", "From", "DATETIME_PICKER", is_internal=True),
        _flt("participants", "Participants", "MEETING_INVITEES"),
        _flt("organizer", "Organizer", "MEETING_ORGANIZER"),
    ],
}


def test_meeting_participants_use_objects():
    payload = _payload(group_by={"field": "owner"}, date_range={"field": "from", "operator": "current_month"}, filters=[
        {"field": "participants", "operator": "in", "value": [{"id": "7", "entity": "Lead", "name": "Ravi", "emails": []}]},
        {"field": "organizer", "operator": "equal", "value": {"id": 3, "entity": "user"}},
    ])
    body, _ = _build_cfg(copy.deepcopy(MEETING_CONFIG), payload, entity="meeting")
    participants, organizer = body["config"]["rules"]
    assert participants["value"] == [{"id": 7, "entity": "lead", "name": "Ravi"}]
    assert participants["type"] == "participants_lookup"
    assert organizer["value"] == {"id": 3, "entity": "user"} and organizer["type"] == "organizer_lookup"
    with pytest.raises(ValueError, match="needs an object"):
        _build_cfg(copy.deepcopy(MEETING_CONFIG), dict(payload, filters=[
            {"field": "organizer", "operator": "equal", "value": 3}]), entity="meeting")
    text = get_report_config_instructions_logic("meeting", copy.deepcopy(MEETING_CONFIG), set(), "INR")
    assert 'meeting.lookup_related with entity_type="invitee"' in text


def _record_lookup(entity):
    return {"entity": entity, "lookupUrl": f"/search/{entity.lower()}/lookup?q=name:"}


TASK_CONFIG = {
    "entity": "task",
    "dimensions": [_dim("ownerId", "Owner", "LOOK_UP", lookup=USER_LOOKUP)],
    "filters": [
        _flt("createdAt", "Created At", "DATETIME_PICKER", is_internal=True),
        _flt("associatedLeads", "Leads", "LOOK_UP", lookup=_record_lookup("LEAD")),
        _flt("associatedDeals", "Deals", "LOOK_UP", lookup=_record_lookup("DEAL")),
    ],
}


def test_task_linked_records_use_search_by_term_and_one_at_a_time():
    text = get_report_config_instructions_logic("task", copy.deepcopy(TASK_CONFIG), set(), "INR")
    assert "record id(s) — resolve_via: lead.search_by_term" in text
    assert "resolve_via: deal.search_by_term" in text and "only ONE of associatedLeads" in text
    base = _payload(group_by={"field": "ownerId"})
    body, _ = _build_cfg(copy.deepcopy(TASK_CONFIG), dict(base, filters=[
        {"field": "associatedLeads", "operator": "in", "value": [5, 6]}]), entity="task")
    assert body["config"]["rules"][0]["value"] == [5, 6]
    with pytest.raises(ValueError, match="only one associated-entity filter"):
        _build_cfg(copy.deepcopy(TASK_CONFIG), dict(base, filters=[
            {"field": "associatedLeads", "operator": "equal", "value": 5},
            {"field": "associatedDeals", "operator": "equal", "value": 8}]), entity="task")


def test_quotation_approval_state_picklist_and_email_sender_unsupported():
    # On live tenants quotation status/approvalState are inline picklists; approvalState sends the id as long.
    quotation_cfg = {"entity": "quotation", "dimensions": [_dim("owner", "Owner", "LOOK_UP", lookup=USER_LOOKUP)], "filters": [
        _flt("createdAt", "Created At", "DATETIME_PICKER", is_internal=True),
        _flt("approvalState", "Approval State", "PICK_LIST", [{"id": 2, "name": "APPROVED", "displayName": "Approved"}],
             is_internal=True),
    ]}
    body, _ = _build_cfg(quotation_cfg, _payload(group_by={"field": "owner"},
                                                filters=[{"field": "approvalState", "operator": "equal", "value": "Approved"}]), entity="quotation")
    assert body["config"]["rules"][0]["type"] == "long" and body["config"]["rules"][0]["value"] == 2

    email_cfg = {"entity": "email", "dimensions": [_dim("user", "User", "LOOK_UP", lookup=USER_LOOKUP)], "filters": [
        _flt("createdAt", "Created At", "DATETIME_PICKER", is_internal=True),
        _flt("sentBy", "Sent By", "LOOK_UP", lookup=USER_LOOKUP),
    ]}
    email_cfg["filters"].append(_flt("receivedBy", "Received By", "LOOK_UP", lookup=USER_LOOKUP))
    text = get_report_config_instructions_logic("email", email_cfg, set(), "INR")
    assert "'receivedBy' Received By [LOOK_UP] — NOT SUPPORTED" in text
    # sentBy: bare user id(s) via user.lookup, operators limited to equal/in for email
    assert "'sentBy' Sent By [LOOK_UP] — operators: equal, in" in text and "resolve_via: user.lookup" in text
    body, _ = _build_cfg(email_cfg, _payload(group_by={"field": "user"}, filters=[
        {"field": "sentBy", "operator": "in", "value": [31800, "42"]}]), entity="email")
    rule = body["config"]["rules"][0]
    assert rule["value"] == [31800, 42] and rule["type"] == "long" and rule["property"] is None
    with pytest.raises(ValueError, match="not allowed"):
        _build_cfg(email_cfg, _payload(group_by={"field": "user"}, filters=[
            {"field": "sentBy", "operator": "not_equal", "value": 31800}]), entity="email")


@pytest.mark.asyncio
async def test_new_lookup_ids_route_through_execute_request():
    routes = {
        "/v1/teams/lookup": {"content": [{"id": 11, "name": "Sales"}], "totalElements": 1, "totalPages": 1},
        "/v1/campaigns/9": {"id": 9, "name": "Summer", "activities": [{"id": 31, "name": "Email blast"}]},
        "/v1/campaigns/lookup": {"content": [{"id": 9, "name": "Summer"}, {"id": 10, "name": "Summer Sale"}]},
    }
    with patch("app.entities.get_client", _mock_client(routes, [])):
        team = json.loads(await execute_request("team.lookup", {"query": "Sales"}))
        activities = json.loads(await execute_request("campaign.activities", {"campaign_id": 9}))
        campaigns = json.loads(await execute_request("campaign.lookup", {"query": "Summer"}))
    assert team["ok"] and "ID: 11" in team["data"] and "Sales" in team["data"]
    assert activities["ok"] and "ID: 31" in activities["data"] and "Email blast" in activities["data"]
    assert campaigns["ok"] and "ID: 10" in campaigns["data"] and "More than one" in campaigns["data"]


# ---------------------------------------------------------------------------
# report.preview
# ---------------------------------------------------------------------------

PREVIEW_RECORDS = [
    {"id": 650646, "name": "Google", "values": [14, 3692500.0], "recordDetails": None, "dimensions": []},
    {"id": 650647, "name": "Facebook", "values": [4, 1765000.0], "recordDetails": None, "dimensions": []},
    {"id": None, "name": None, "values": [2, 12345.5], "recordDetails": None, "dimensions": []},
]


def test_format_preview_rows_labels_totals_and_share():
    body, _ = _build(_payload(metrics=[{"type": "COUNT", "field": "id"}, {"type": "SUM", "field": "requirementBudget"}]))
    dimension = next(d for d in _config()["dimensions"] if d["id"] == "source")
    out = reports.format_preview_rows("lead", body, dimension, PREVIEW_RECORDS, "INR")
    assert out["columns"] == ["Source", "Number of Leads", "Sum of Budget", "Share of total"]
    assert out["rows"][0] == {"Source": "Google", "Number of Leads": 14, "Sum of Budget": 3692500, "Share of total": "70.0%"}
    assert out["rows"][2]["Source"] == "Not Available" and out["rows"][2]["Sum of Budget"] == 12345.5
    assert out["totals"] == {"Number of Leads": 20, "Sum of Budget": 5469845.5}
    assert out["total_groups"] == 3 and out["currency"] == "Sum of Budget in INR"


def test_format_preview_rows_date_dimension_label_and_average_has_no_total():
    body, _ = _build(_payload(group_by={"field": "createdAt", "format": "MONTHLY"},
                              metrics=[{"type": "AVERAGE", "field": "requirementBudget"}]))
    dimension = next(d for d in _config()["dimensions"] if d["id"] == "createdAt")
    out = reports.format_preview_rows("lead", body, dimension, [{"id": None, "name": "Jul'2026", "values": [10.0]}])
    assert out["columns"] == ["Created At (Monthly)", "Average of Budget"]
    assert out["rows"] == [{"Created At (Monthly)": "Jul'2026", "Average of Budget": 10}]
    assert out["totals"] == {}


@pytest.mark.asyncio
async def test_execute_preview_runs_without_saving_and_name_optional():
    posted = []
    client = _mock_client(ROUTES, posted)

    async def _post(url, json=None, params=None, **kwargs):
        posted.append((url, json, params))
        r = MagicMock()
        r.json.return_value = PREVIEW_RECORDS
        r.raise_for_status = MagicMock()
        return r

    client.return_value.post.side_effect = _post
    payload = _payload()
    payload.pop("name")
    with patch("app.reports.get_client", client), patch("app.entities.get_client", client):
        result = json.loads(await execute_request("report.preview", payload))
    assert result["ok"] is True, result
    data = result["data"]
    assert data["saved"] is False and data["rows"][0]["Source"] == "Google"
    (url, body, params), = posted
    assert url == "/v3/reports/leads" and params == {"timezone": "Asia/Kolkata"}
    assert body["category"] == "ONE_DIMENSIONAL" and "name" not in body and body["groupBy"][0]["name"] == "source"


@pytest.mark.asyncio
async def test_preview_deal_sends_base_currency_id():
    deal_config = dict(copy.deepcopy(LEAD_CONFIG), entity="deal")
    routes = dict(ROUTES, **{
        "/users/me/permissions": [{"name": "deal", "action": {"read": True}}],
        "/reports/config/deals": deal_config,
        "/v1/picklists/standard": {"CURRENCY": [{"id": 400, "name": "USD"}, {"id": 431, "name": "INR"}]},
    })
    posted = []
    client = _mock_client(routes, posted)

    async def _post(url, json=None, params=None, **kwargs):
        posted.append((url, params))
        r = MagicMock()
        r.json.return_value = []
        r.raise_for_status = MagicMock()
        return r

    client.return_value.post.side_effect = _post
    with patch("app.reports.get_client", client), patch("app.entities.get_client", client):
        result = json.loads(await execute_request("report.preview", _payload(entity="deal")))
    assert result["ok"] is True, result
    assert posted == [("/v3/reports/deals", {"timezone": "Asia/Kolkata", "baseCurrencyId": 431})]
    assert result["data"]["rows"] == [] and result["data"]["total_groups"] == 0


@pytest.mark.asyncio
async def test_build_payload_preview_schema_name_optional():
    with patch("app.reports.get_client", _mock_client(ROUTES, [])):
        result = json.loads(await build_payload("report.preview", entity="lead"))
    assert result["fetched_live"] is True and "name" not in result["schema"]["required"]
    assert "tenant_fields_reference" in result["schema"]


# ---------------------------------------------------------------------------
# MULTI_DIMENSIONAL
# ---------------------------------------------------------------------------

def _multi(**overrides):
    return _payload(category="MULTI_DIMENSIONAL", **overrides)


def test_multi_dimensional_body_matches_sd_ui_get_config():
    body, notes = _build(_multi(group_by=[{"field": "createdAt", "format": "monthly"}, {"field": "ownerId"}, "source"]))
    assert body["category"] == "MULTI_DIMENSIONAL" and notes == []
    assert body["config"]["groupBy"] == [
        {"name": "createdAt", "format": "MONTHLY", "primaryField": None, "property": None},
        {"name": "ownerId", "format": None, "primaryField": None, "property": None},
        {"name": "source", "format": None, "primaryField": None, "property": None},
    ]


def test_multi_dimensional_rules():
    with pytest.raises(ValueError, match="at most 3"):
        _build(_multi(group_by=["ownerId", "source", "createdAt", "pipelineStage"]))
    with pytest.raises(ValueError, match="already a dimension"):
        _build(_multi(group_by=["ownerId", "ownerId"]))
    with pytest.raises(ValueError, match="format is required"):
        _build(_multi(group_by=["ownerId", "createdAt"]))
    with pytest.raises(ValueError, match="bar, table"):
        _build(_multi(group_by=["ownerId", "source"], chart_type="pie"))
    with pytest.raises(ValueError, match="exactly one group_by"):
        _build(_payload(group_by=["ownerId", "source"]))  # ONE_DIMENSIONAL with two
    body, notes = _build(_multi(group_by=["ownerId", "source"], metrics=[{"type": "COUNT", "field": "id"},
                                                                         {"type": "SUM", "field": "requirementBudget"}]))
    assert body["chartType"] == "table" and notes


def test_multi_dimensional_parent_child_and_required_filter():
    cfg = _config()
    cfg["dimensions"].append(_dim("pipeline", "Pipeline", "PIPELINE"))
    # stage grouped AFTER pipeline: no pipeline filter needed
    body, _ = build_report_body(_multi(group_by=["pipeline", "pipelineStage"]), cfg, "Asia/Kolkata", "INR")
    assert [g["name"] for g in body["config"]["groupBy"]] == ["pipeline", "pipelineStage"]
    # stage before pipeline: rejected
    with pytest.raises(ValueError, match="'pipeline' must come before 'pipelineStage'"):
        build_report_body(_multi(group_by=["pipelineStage", "pipeline"]), cfg, "Asia/Kolkata", "INR")
    # stage without pipeline dimension: needs the pipeline filter
    with pytest.raises(ValueError, match="requires a 'pipeline' filter"):
        build_report_body(_multi(group_by=["ownerId", "pipelineStage"]), cfg, "Asia/Kolkata", "INR")


def test_multi_dimensional_team_dimension_adds_primary_filter():
    body, notes = _build(_multi(group_by=["ownerFields", "source"]))
    assert body["config"]["groupBy"][0]["property"] == "teams"
    assert body["config"]["rules"][-1]["id"] == "ownerId" and body["config"]["rules"][-1]["operator"] == "is_not_null"


NESTED_RECORDS = [
    {"id": 31800, "name": "Janardan Borse", "values": [], "dimensions": [
        {"id": 650646, "name": "Google", "values": [14, 3692500.0], "dimensions": []},
        {"id": None, "name": None, "values": [91, 12345.0], "dimensions": []},
    ]},
    {"id": 32551, "name": "Michael parker", "values": [], "dimensions": [
        {"id": None, "name": None, "values": [1, 0], "dimensions": []},
    ]},
]


def test_preview_flattens_nested_results_with_subtotals():
    body, _ = _build(_multi(group_by=["ownerId", "source"], metrics=[{"type": "COUNT", "field": "id"},
                                                                    {"type": "SUM", "field": "requirementBudget"}]))
    dims = [next(d for d in _config()["dimensions"] if d["id"] == i) for i in ("ownerId", "source")]
    out = reports.format_preview_rows("lead", body, dims, NESTED_RECORDS, "INR")
    assert out["columns"] == ["Owner", "Source", "Number of Leads", "Sum of Budget"]
    assert out["rows"] == [
        {"Owner": "Janardan Borse", "Source": "Google", "Number of Leads": 14, "Sum of Budget": 3692500},
        {"Owner": "Janardan Borse", "Source": "Not Available", "Number of Leads": 91, "Sum of Budget": 12345},
        {"Owner": "Michael parker", "Source": "Not Available", "Number of Leads": 1, "Sum of Budget": 0},
    ]
    assert out["subtotals"] == [
        {"Owner": "Janardan Borse", "Number of Leads": 105, "Sum of Budget": 3704845},
        {"Owner": "Michael parker", "Number of Leads": 1, "Sum of Budget": 0},
    ]
    assert out["totals"] == {"Number of Leads": 106, "Sum of Budget": 3704845}


def test_reference_documents_multi_dimensional():
    text = get_report_config_instructions_logic("lead", _config(), set(), "INR")
    assert "MULTI_DIMENSIONAL = group_by is a" in text and "must come after its parent" in text


# ---------------------------------------------------------------------------
# HIERARCHY and FUNNEL
# ---------------------------------------------------------------------------

def test_hierarchy_body_and_rules():
    body, notes = _build(_payload(category="HIERARCHY", group_by={"field": "ownerId"}, chart_type="bar",
                                  metrics=[{"type": "COUNT", "field": "id"}, {"type": "SUM", "field": "requirementBudget"}]))
    assert body["category"] == "HIERARCHY" and body["chartType"] == "hierarchy"
    assert body["config"]["groupBy"] == [{"name": "ownerId", "format": None, "primaryField": None, "property": None}]
    assert any("replaced by 'hierarchy'" in n for n in notes)
    assert not any("table" in n for n in notes)  # no table switch for hierarchy
    with pytest.raises(ValueError, match="group by a user field"):
        _build(_payload(category="HIERARCHY", group_by={"field": "source"}))
    with pytest.raises(ValueError, match="exactly one group_by"):
        _build(_payload(category="HIERARCHY", group_by=["ownerId", "createdAt"]))
    cfg = _config()
    cfg["dimensions"][1]["supportedMetrics"] = METRICS + [{"type": "SUM", "field": "score", "header": "Score"},
                                                          {"type": "AVERAGE", "field": "score", "header": "Score"}]
    with pytest.raises(ValueError, match="at most 3 metrics"):
        build_report_body(_payload(category="HIERARCHY", group_by={"field": "ownerId"}, metrics=[
            {"type": "COUNT", "field": "id"}, {"type": "SUM", "field": "requirementBudget"},
            {"type": "AVERAGE", "field": "requirementBudget"}, {"type": "SUM", "field": "score"}]), cfg, "Asia/Kolkata", "INR")


def test_hierarchy_preview_rolls_values_up_the_tree():
    body, _ = _build(_payload(category="HIERARCHY", group_by={"field": "ownerId"}))
    records = [
        {"id": 1, "name": "CEO", "reportingManagers": [], "active": True, "values": [5]},
        {"id": 2, "name": "Manager", "reportingManagers": [{"id": 1, "name": "CEO"}], "active": True, "values": [3]},
        {"id": 3, "name": "Rep A", "reportingManagers": [{"id": 2, "name": "Manager"}], "active": True, "values": [2]},
        {"id": 4, "name": "Rep B", "reportingManagers": [{"id": 2, "name": "Manager"}, {"id": 1, "name": "CEO"}], "active": False, "values": []},
    ]
    out = reports.format_hierarchy_rows("lead", body, {}, records)
    by_user = {r["User"]: r for r in out["rows"]}
    assert by_user["CEO"]["Number of Leads (reportees)"] == 5 and by_user["CEO"]["Number of Leads (total)"] == 10
    assert by_user["Manager"]["Number of Leads (reportees)"] == 2 and by_user["Manager"]["Reports to"] == "CEO"
    assert by_user["Rep B (inactive)"]["Number of Leads (self)"] == 0 and by_user["Rep B (inactive)"]["Reports to"] == "Manager, CEO"
    assert out["totals"] == {"Number of Leads": 10}


def _funnel_config():
    cfg = _config()
    cfg["dimensions"].append(_dim("pipeline", "Pipeline", "PIPELINE"))
    return cfg


def test_funnel_body_matches_sd_ui_save():
    body, _ = build_report_body(_payload(chart_type="funnel", group_by=None, metrics=None, funnel_color="#da2d1f",
                                         filters=[{"field": "pipeline", "operator": "equal", "value": 872}]),
                                _funnel_config(), "Asia/Kolkata", "INR")
    assert body["chartType"] == "funnel" and body["category"] == "ONE_DIMENSIONAL" and body["funnelColor"] == "#DA2D1F"
    assert body["config"]["groupBy"] == [{"name": "pipelineStage", "format": None, "primaryField": None, "property": None}]
    assert body["config"]["metrics"] == [{"type": "COUNT", "field": "id"}]
    assert "colorCodes" not in body["config"] and "chartType" not in body["config"]
    assert body["config"]["rules"][0]["value"] == 872 and body["config"]["rules"][0]["operator"] == "equal"


def _funnel_payload(**overrides):
    base = _payload(chart_type="funnel", group_by=None, metrics=None)
    base.update(overrides)
    return base


def test_funnel_rules():
    ok = {"filters": [{"field": "pipeline", "operator": "equal", "value": 872}]}
    with pytest.raises(ValueError, match="lead and deal only"):
        build_report_body(dict(_funnel_payload(**ok), entity="contact"), _funnel_config(), "Asia/Kolkata", "INR")
    with pytest.raises(ValueError, match="must be ONE_DIMENSIONAL"):
        build_report_body(_funnel_payload(category="MULTI_DIMENSIONAL", **ok), _funnel_config(), "Asia/Kolkata", "INR")
    with pytest.raises(ValueError, match="always group by 'pipelineStage'"):
        build_report_body(_funnel_payload(group_by={"field": "source"}, **ok), _funnel_config(), "Asia/Kolkata", "INR")
    with pytest.raises(ValueError, match="exactly one pipeline"):
        build_report_body(_funnel_payload(filters=[{"field": "pipeline", "operator": "in", "value": [1, 2]}]),
                          _funnel_config(), "Asia/Kolkata", "INR")
    with pytest.raises(ValueError, match="funnel_color must be one of"):
        build_report_body(_funnel_payload(funnel_color="#000000", **ok), _funnel_config(), "Asia/Kolkata", "INR")


def test_funnel_preview_rows():
    records = [
        {"id": 1, "name": "Open", "values": [10, 0], "recordDetails": {"forecastingType": "OPEN"}},
        {"id": 2, "name": "Demo", "values": [6, 2], "recordDetails": {"forecastingType": "OPEN"}},
        {"id": 3, "name": "Won", "values": [3, 0], "recordDetails": {"forecastingType": "CLOSED_WON"}},
        {"id": 4, "name": "Lost", "values": [1, 0], "recordDetails": {"forecastingType": "CLOSED_LOST"}},
    ]
    out = reports.format_funnel_rows("deal", records)
    assert out["rows"][1] == {"Stage": "Demo", "Outcome": "Open", "Deals visited": 6, "Deals skipped": 2}
    assert out["rows"][2]["% of entered"] == "30.0%" and out["rows"][3]["Outcome"] == "Lost"
    assert out["totals"] == {"Deals entering the funnel": 10}


@pytest.mark.asyncio
async def test_funnel_preview_sends_chart_type_funnel():
    posted = []
    client = _mock_client(dict(ROUTES, **{"/reports/config/leads": _funnel_config()}), posted)

    async def _post(url, json=None, params=None, **kwargs):
        posted.append(json)
        r = MagicMock()
        r.json.return_value = []
        r.raise_for_status = MagicMock()
        return r

    client.return_value.post.side_effect = _post
    with patch("app.reports.get_client", client), patch("app.entities.get_client", client):
        result = json.loads(await execute_request("report.preview", _funnel_payload(
            filters=[{"field": "pipeline", "operator": "equal", "value": 872}])))
    assert result["ok"] is True, result
    assert posted[0]["chartType"] == "FUNNEL" and posted[0]["category"] == "ONE_DIMENSIONAL"


# ---------------------------------------------------------------------------
# GOALS_VS_ACHIEVEMENT
# ---------------------------------------------------------------------------

GOAL = {
    "id": 548, "name": "ewewe", "entityType": "LEAD", "active": True, "value": 50.0,
    "dimension": {"field": "ownerId", "header": "Owner", "format": None, "fieldType": "LOOK_UP"},
    "metric": {"field": "id", "header": "Number of NPC", "type": "COUNT"},
    "frequency": {"type": "WEEKLY", "header": "Weekly"},
    "dateRange": {"operator": "current_week", "field": "createdAt", "fieldType": "DATETIME_PICKER"},
    "filters": [{"operator": "equal", "field": "source", "header": "Source", "type": "long", "value": 650646,
                 "fieldType": "PICK_LIST", "primaryField": None, "property": None}],
}


def _goal_payload(**overrides):
    base = {"entity": "lead", "name": "Goal Report (Made by MCP)", "category": "GOALS_VS_ACHIEVEMENT", "goal_id": 548}
    base.update(overrides)
    return base


def test_goal_body_fills_from_goal():
    body, notes = build_report_body(_goal_payload(), _config(), "Asia/Kolkata", "INR", copy.deepcopy(GOAL))
    assert body["category"] == "GOALS_VS_ACHIEVEMENT" and body["chartType"] == "donut"
    assert body["goal"] == {"id": 548, "name": "ewewe"} and body["prorated"] is True
    cfg = body["config"]
    assert cfg["groupBy"][0]["name"] == "ownerId" and cfg["metrics"] == [{"type": "COUNT", "field": "id"}]
    assert cfg["dateRange"]["operator"] == "current_week" and cfg["dateRange"]["from"] == "00:00"
    assert cfg["rules"][0] == {"operator": "equal", "id": "source", "field": "source", "fieldInputType": "PICK_LIST",
                               "type": "long", "value": 650646, "from": None, "to": None, "property": None, "primaryField": None}
    assert [b["hexCode"] for b in cfg["colorCodes"]] == ["#DB2E20", "#FF9300", "#FFD10F", "#52A351"]


def test_goal_rules():
    goal = copy.deepcopy(GOAL)
    with pytest.raises(ValueError, match="is on lead, not deal"):
        build_report_body(_goal_payload(entity="deal"), _config(), "Asia/Kolkata", "INR", goal)
    with pytest.raises(ValueError, match="inactive"):
        build_report_body(_goal_payload(), _config(), "Asia/Kolkata", "INR", dict(goal, active=False))
    with pytest.raises(ValueError, match="set by the goal"):
        build_report_body(_goal_payload(filters=[{"field": "source", "operator": "equal", "value": "Facebook"}]),
                          _config(), "Asia/Kolkata", "INR", goal)
    with pytest.raises(ValueError, match="available for lead, deal, meeting, call"):
        build_report_body(_goal_payload(entity="contact"), _config(), "Asia/Kolkata", "INR", goal)
    # extra filters allowed
    body, _ = build_report_body(_goal_payload(filters=[{"field": "country", "operator": "equal", "value": "IN"}]),
                                _config(), "Asia/Kolkata", "INR", goal)
    assert [r["id"] for r in body["config"]["rules"]] == ["source", "country"]


def test_goal_prorated_rules():
    goal = copy.deepcopy(GOAL)
    body, notes = build_report_body(_goal_payload(date_range={"field": "createdAt", "operator": "current_year"}),
                                    _config(), "Asia/Kolkata", "INR", goal)
    assert body["prorated"] is False and any("longer than the goal's frequency" in n for n in notes)
    body, _ = build_report_body(_goal_payload(date_range={"field": "createdAt", "operator": "today"}),
                                _config(), "Asia/Kolkata", "INR", goal)
    assert body["prorated"] is True
    body, _ = build_report_body(_goal_payload(prorated=False), _config(), "Asia/Kolkata", "INR", goal)
    assert body["prorated"] is False


def test_format_goal_rows():
    body, _ = build_report_body(_goal_payload(), _config(), "Asia/Kolkata", "INR", copy.deepcopy(GOAL))
    data = {"result": [{"id": 1, "name": "Ravi", "value": [{"achieved": 8, "goal": 10.0, "percentage": 80.0}]},
                       {"id": 2, "name": "Asha", "value": [{"achieved": 2, "goal": 10.0, "percentage": 20.0}]}],
            "goalSummary": {"achieved": 10.0, "goal": 50.0, "percentage": 20.0}}
    out = reports.format_goal_rows(body, GOAL, data, [{"header": "Owner"}])
    assert out["rows"][0] == {"Owner": "Ravi", "Achieved": 8, "Goal": 10, "% achieved": "80%", "Band": "75%+ (green)"}
    assert out["rows"][1]["Band"] == "below 25% (red)"
    assert out["totals"] == {"Achieved": 10, "Goal": 50, "% achieved": "20%", "Band": "below 25% (red)"}


@pytest.mark.asyncio
async def test_goal_preview_uses_goal_endpoint_and_lookups():
    posted = []
    routes = dict(ROUTES, **{"/v1/reports/goals/548": copy.deepcopy(GOAL),
                             "/v1/reports/goals": [{"id": 548, "name": "ewewe"}]})
    client = _mock_client(routes, posted)

    async def _post(url, json=None, params=None, **kwargs):
        posted.append((url, params, json))
        r = MagicMock()
        r.json.return_value = {"result": [], "goalSummary": {"achieved": 0, "goal": 50, "percentage": 0}}
        r.raise_for_status = MagicMock()
        return r

    client.return_value.post.side_effect = _post
    with patch("app.reports.get_client", client), patch("app.entities.get_client", client):
        result = json.loads(await execute_request("report.preview", _goal_payload()))
        goals = json.loads(await execute_request("goal.lookup", {"entity_type": "lead"}))
        details = json.loads(await execute_request("goal.details", {"goal_id": 548}))
    assert result["ok"] is True, result
    url, params, body = posted[0]
    assert url == "/v1/reports/leads/goals/548" and params == {"timezone": "Asia/Kolkata", "prorated": "true"}
    assert body["category"] == "GOALS_VS_ACHIEVEMENT" and "colorCodes" in body
    assert result["data"]["goal"]["name"] == "ewewe"
    assert goals["ok"] and "ID: 548" in goals["data"]
    assert details["ok"] and "Weekly" in details["data"] and "COUNT id" in details["data"]
