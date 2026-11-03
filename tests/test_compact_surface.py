"""What the server sends a model on every call, kept short without losing a rule:
the filterable-fields table, failed searches as ok:false, and the instructions'
load-bearing rules."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from app import tools
from app.client import KylasAPIError
from app.config import SYSTEM_INSTRUCTIONS
from app.tools import _filterable_fields_table, execute_request


def test_filterable_fields_list_each_types_operators_once():
    fields = [
        {"name": "createdAt", "displayName": "Created At", "type": "DATETIME_PICKER", "standard": True},
        {"name": "updatedAt", "displayName": "Updated At", "type": "DATETIME_PICKER", "standard": True},
        {"name": "cfScore", "displayName": "Score", "type": "NUMBER", "standard": False},
    ]

    table = _filterable_fields_table(fields)

    assert sorted(table["operators"]) == ["DATETIME_PICKER", "NUMBER"]
    assert table["operators"]["NUMBER"] == tools._lean_filter_field(fields[2])["allowed_operators"]
    assert table["fields"] == [
        "createdAt | Created At | DATETIME_PICKER",
        "updatedAt | Updated At | DATETIME_PICKER",
        "customFieldValues.cfScore | Score | NUMBER",   # the path a filter's "field" takes
    ]


@pytest.mark.asyncio
async def test_a_failed_search_is_not_ok_and_keeps_kylas_reason():
    refused = KylasAPIError("Search call logs failed: 422", 422,
                            '{"code": "01", "message": "Sort on startTime is not allowed"}')
    with patch.dict(tools._ENTITY_CONFIG["call_log"], {"search_fn": AsyncMock(side_effect=refused)}):
        result = json.loads(await execute_request("call_log.search", {"filters": [], "sort": "startTime,desc"}))

    assert result["ok"] is False
    assert result["error"]["code"] == "SEARCH_FAILED"
    assert "Sort on startTime is not allowed" in result["error"]["message"]


@pytest.mark.asyncio
async def test_a_search_that_found_records_stays_ok():
    found = AsyncMock(return_value="Found 2 call log(s) (page 1 of 1, total 2)")
    with patch.dict(tools._ENTITY_CONFIG["call_log"], {"search_fn": found}):
        result = json.loads(await execute_request("call_log.search", {"filters": []}))

    assert result["ok"] is True and result["data"].startswith("Found 2")


@pytest.mark.parametrize("rule", [
    "get_entity_labels() AND get_current_user()", "ONCE per", "STANDARD type",
    "resolve_via", "Never guess", '"ok": true', "error.details", "429",
    "(Made by MCP)", "report.preview", "Never say a preview was saved",
    "updatedAt >= (today - 90 days)", "Showing records updated in the last 3 months",
    'search_by_term with "*"', "Never show a raw UTC timestamp", "₹", "Never show raw IDs",
    "datetime.parse_to_utc", "task.search_any_relation", "READ-ONLY", "Diagnosis",
])
def test_the_instructions_keep_every_rule(rule):
    assert rule in " ".join(SYSTEM_INSTRUCTIONS.split())   # line wraps aside


def test_the_instructions_no_longer_demand_build_payload_every_time():
    assert "every time" not in SYSTEM_INSTRUCTIONS
    assert "largest page size" not in SYSTEM_INSTRUCTIONS


def test_today_is_given_in_the_users_timezone():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.entities import _today_in

    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    assert _today_in("Asia/Kolkata") == f"{now:%A}, {now.day} {now:%b %Y}"
    assert _today_in("Not/AZone")   # unknown zone: falls back to UTC, never fails
