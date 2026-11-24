"""execute_request's resource_link blocks: what a call did to the CRM."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastmcp import Client

from app import resource_links as rl
from app.tools import _REGISTRY, mcp


def _ok(data):
    return json.dumps({"ok": True, "status": 200, "data": data})


def _links(endpoint_id, payload, data):
    found = rl.links_for(endpoint_id, _REGISTRY.get(endpoint_id), payload, _ok(data))
    return [link.model_dump(mode="json", by_alias=True, exclude_none=True) for link in found]


# --- which calls earn a link --------------------------------------------------------

def test_a_created_lead_links_to_itself_by_its_name():
    assert _links("lead.create", {"firstName": "Asha"}, {"id": 9, "firstName": "Asha", "lastName": "Rao"}) == [{
        "type": "resource_link", "uri": "kylas://lead/9", "name": "Asha Rao",
        "_meta": {"com.kylas/action": "CreateAction", "com.kylas/entity": "lead", "com.kylas/id": 9},
    }]


def test_an_update_links_to_the_record_it_changed():
    [found] = _links("deal.update", {"entity_id": 5, "name": "Big"}, {"id": 5, "name": "Big"})
    assert (found["uri"], found["name"], found["_meta"]["com.kylas/action"]) == \
        ("kylas://deal/5", "Big", "UpdateAction")


def test_a_note_says_which_record_it_is_on():
    [found] = _links("note.create", {"entity_type": "lead", "entity_id": 999, "note_text": "x"}, {"id": 1234})
    assert found["uri"] == "kylas://note/1234"
    assert found["_meta"]["com.kylas/about"] == "kylas://lead/999"


def test_a_cancelled_or_deleted_meeting_links_only_when_it_went_through():
    [cancelled] = _links("meeting.cancel", {"meeting_id": 3}, "✓ Meeting 3 cancelled successfully.")
    [deleted] = _links("meeting.delete", {"meeting_id": 3}, "✓ Meeting 3 deleted successfully.")
    assert cancelled["_meta"]["com.kylas/action"] == "UpdateAction"
    assert deleted["_meta"]["com.kylas/action"] == "DeleteAction"
    assert deleted["uri"] == "kylas://meeting/3"
    assert _links("meeting.delete", {"meeting_id": 3}, "✗ Failed to delete meeting: 404") == []


def test_a_search_with_no_rule_recorded_carries_its_total_but_names_no_list():
    """Only the rule Kylas ran may open a list; the filters as asked are not it."""
    payload = {"filters": [{"field": "city", "operator": "==", "value": "Pune", "type": "TEXT_FIELD"}]}
    [found] = _links("lead.search", payload, "Found 12 lead(s) (page 1 of 1, total 12)\n----")

    assert found["uri"] == "kylas://lead"
    assert found["_meta"]["com.kylas/count"] == 12
    assert "com.kylas/query" not in found["_meta"]


def test_a_term_or_idle_search_carries_what_it_searched_by():
    [by_term] = _links("lead.search_by_term", {"search_term": "pune"}, "Found 3 lead(s) for 'pune' (page 1 of 1, total 3)")
    [idle] = _links("deal.search_idle", {"days": 30}, "No deals found matching the filters. (Total in DB: 80)")
    assert by_term["_meta"]["com.kylas/query"] == {"term": "pune"}
    assert by_term["_meta"]["com.kylas/count"] == 3
    assert idle["_meta"]["com.kylas/query"] == {"idleDays": 30}
    assert idle["_meta"]["com.kylas/count"] == 0


@pytest.mark.parametrize("text, count", [
    ("Found 12 lead(s) (page 1 of 2, total 40)\n...", 40),
    ("Found 3 lead(s) matching 'x':\n...", 3),
    ("No leads found matching the filters. (Total in DB: 900)", 0),
    ("something else", None),
    ({"not": "text"}, None),
])
def test_the_count_is_read_from_the_searchs_own_header(text, count):
    assert rl.search_count(text) == count


def test_reads_lookups_and_failures_earn_no_link():
    assert _links("lead.get", {"lead_id": 1}, {"id": 1}) == []
    assert _links("user.lookup", {"q": "a"}, "Found 1 user(s)") == []
    assert _links("lead.create", {}, {"no": "id"}) == []
    failed = json.dumps({"ok": False, "error": {"code": "400", "message": "bad"}})
    assert rl.links_for("lead.create", _REGISTRY["lead.create"], {}, failed) == []
    assert rl.links_for("nope.create", None, {}, _ok({"id": 1})) == []
    assert rl.links_for("lead.create", _REGISTRY["lead.create"], {}, "not json") == []


# --- over MCP ------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_the_tool_answers_its_usual_text_then_the_links():
    record = {"id": 9, "firstName": "Asha", "lastName": "Rao"}
    with patch("app.tools.create_entity_logic", new=AsyncMock(return_value=record)):
        async with Client(mcp) as client:
            result = await client.call_tool("execute_request", {"id": "lead.create", "payload": {"firstName": "Asha"}})

    text, link = result.content
    assert json.loads(text.text) == {"ok": True, "status": 200, "data": record}
    assert result.structured_content == {"result": text.text}
    assert link.type == "resource_link"
    assert str(link.uri) == "kylas://lead/9"
    assert link.name == "Asha Rao"
    assert link.meta["com.kylas/action"] == "CreateAction"


@pytest.mark.asyncio
async def test_a_read_over_mcp_answers_with_its_text_alone():
    with patch("app.tools.get_entity_logic", new=AsyncMock(return_value={"id": 1})):
        async with Client(mcp) as client:
            result = await client.call_tool("execute_request", {"id": "lead.get", "payload": {"lead_id": 1}})

    assert [block.type for block in result.content] == ["text"]


# --- a search link carries the rule Kylas ran -------------------------------------------

import httpx

from app.client import _record_search

EXECUTED = {"condition": "AND", "valid": True, "rules": [
    {"operator": "greater", "id": "createdAt", "field": "createdAt", "type": "date",
     "value": "2026-10-31T18:30:00.000Z", "timeZone": "Asia/Calcutta", "relatedFieldIds": None},
    {"operator": "equal", "id": "cfRegion", "field": "customFieldValues.cfRegion", "type": "long",
     "value": 41, "relatedFieldIds": None}]}


def _sent(method="POST", path="/v1/search/lead", body=None):
    return httpx.Request(method, f"https://api.test{path}", json=body if body is not None else {"jsonRule": EXECUTED})


@pytest.mark.asyncio
async def test_the_search_rules_sent_inside_the_block_are_recorded():
    with rl.records_searches() as searches:
        await _record_search(_sent())
        await _record_search(_sent("GET"))
        await _record_search(_sent(path="/v1/leads/9"))
        await _record_search(_sent(body={"no": "rule"}))
    await _record_search(_sent())          # outside: not recorded, no error
    assert searches == [("/v1/search/lead", EXECUTED)]


def test_the_executed_rule_becomes_the_links_query():
    assert rl.executed_query(EXECUTED) == {"condition": "AND", "rules": [
        {"field": "createdAt", "operator": "greater", "type": "date", "value": "2026-10-31T18:30:00.000Z"},
        {"field": "cfRegion", "operator": "equal", "type": "long", "value": 41}]}
    assert rl.executed_query({"rules": [{"condition": "OR", "rules": []}]}) is None
    assert rl.executed_query("nope") is None


@pytest.mark.asyncio
async def test_a_search_over_mcp_links_with_the_rule_kylas_ran_not_the_filters_asked():
    async def search(**kwargs):
        await _record_search(_sent())
        return "Found 4 lead(s) (page 1 of 1, total 4)"

    with patch("app.tools.search_entity_logic", new=search):
        async with Client(mcp) as client:
            result = await client.call_tool("execute_request", {"id": "lead.search", "payload": {
                "filters": [{"field": "createdAt", "operator": ">", "value": "2026-11-01"}]}})

    link = result.content[1]
    assert link.meta["com.kylas/query"]["rules"][0] == {
        "field": "createdAt", "operator": "greater", "type": "date", "value": "2026-10-31T18:30:00.000Z"}
    assert link.meta["com.kylas/count"] == 4


# --- production safety -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_links_that_cannot_be_built_never_fail_the_call():
    """The lead was created: a failure building its link must not turn that
    into an error a model would retry, creating the lead twice."""
    record = {"id": 9, "firstName": "Asha"}
    with patch("app.tools.create_entity_logic", new=AsyncMock(return_value=record)), \
         patch.object(rl, "links_for", side_effect=RuntimeError("boom")):
        async with Client(mcp) as client:
            result = await client.call_tool("execute_request", {"id": "lead.create", "payload": {}})

    assert [block.type for block in result.content] == ["text"]
    assert json.loads(result.content[0].text)["data"] == record
    assert not result.is_error


@pytest.mark.asyncio
async def test_the_request_hook_never_breaks_a_request():
    """A body httpx has not read (a streamed upload) cannot be recorded — and
    must not stop the request from being sent."""
    async def stream():
        yield b'{"jsonRule": {}}'
    streamed = httpx.Request("POST", "https://api.test/v1/search/lead", content=stream())
    with rl.records_searches() as searches:
        await _record_search(streamed)          # no exception
    assert searches == []


@pytest.mark.asyncio
async def test_links_can_be_switched_off_and_the_answer_is_the_same_text(monkeypatch):
    record = {"id": 9, "firstName": "Asha"}
    monkeypatch.setenv("MCP_RESOURCE_LINKS_ENABLED", "false")
    with patch("app.tools.create_entity_logic", new=AsyncMock(return_value=record)):
        async with Client(mcp) as client:
            result = await client.call_tool("execute_request", {"id": "lead.create", "payload": {}})
    assert [block.type for block in result.content] == ["text"]
    assert json.loads(result.content[0].text)["data"] == record


def test_a_search_link_carries_the_rule_sent_to_its_own_entitys_search_only():
    other = {"condition": "AND", "rules": [{"id": "name", "field": "name", "operator": "equal", "value": "x"}]}
    sent = [("/v1/search/lead", EXECUTED), ("/v1/search/lead/lookup", other), ("/v1/search/deal", other)]
    assert rl.executed_rule_for("lead", sent) is EXECUTED
    assert rl.executed_rule_for("task", sent) is None
    assert rl.executed_rule_for("note", sent) is None


def test_a_search_rule_too_large_for_a_link_is_left_out_but_the_count_stays():
    big = {"condition": "AND", "rules": [{"id": "ownerId", "field": "ownerId", "operator": "in",
                                          "type": "long", "value": list(range(2000))}]}
    found = rl.links_for("lead.search", _REGISTRY["lead.search"], {}, _ok("Found 5 lead(s) (page 1 of 1, total 5)"),
                         [("/v1/search/lead", big)])[0].model_dump(mode="json", by_alias=True, exclude_none=True)
    assert "com.kylas/query" not in found["_meta"]
    assert found["_meta"]["com.kylas/count"] == 5
