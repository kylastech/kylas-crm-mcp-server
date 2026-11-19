"""lead.search / contact.search with a smart list, and the smart_list.lookup helper."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.client import KylasAPIError
from app.tools import build_payload, execute_request, search_entity_logic

_RADIUS_RULE = {"id": "addressCoordinate", "field": "addressCoordinate", "type": "GpsCoordinates",
                "operator": "range_within", "group": False,
                "value": {"radius": 8, "unit": "KILOMETERS", "lat": 18.5291453, "lon": 73.8473186}}
_FIELDS = [{"id": 1, "name": "firstName", "type": "TEXT_FIELD", "active": True, "filterable": True, "standard": True}]
_FILTER = {"field": "firstName", "operator": "equal", "value": "A"}
_ROW = {"id": 1, "firstName": "A", "lastName": "B", "emails": [{"value": "a@b.com", "primary": True}],
        "phoneNumbers": []}


def _smart_list(rules, name="Holy West Team", list_id=146421):
    return AsyncMock(return_value=({"id": list_id, "name": name, "rules": rules}, None))


class _Search:
    """Patches the Kylas search call and the field list; records what was posted."""

    def __init__(self, resolver):
        self.resolver = resolver
        self.client = AsyncMock()
        response = MagicMock()
        response.json.return_value = {"content": [_ROW], "totalElements": 1, "totalPages": 1}
        response.raise_for_status = MagicMock()
        self.client.post.return_value = response
        self.client.__aenter__.return_value = self.client
        self.client.__aexit__.return_value = None
        self._patches = [
            patch("app.entities.get_client", return_value=self.client),
            patch("app.entities._fetch_lead_fields", AsyncMock(return_value=_FIELDS)),
            patch("app.entities._fetch_contact_fields", AsyncMock(return_value=_FIELDS)),
            patch("app.tools._resolve_smart_list", resolver),
        ]

    def __enter__(self):
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()

    @property
    def posted(self):
        return self.client.post.call_args


@pytest.mark.asyncio
async def test_smart_list_conditions_are_sent_exactly_as_saved():
    with _Search(_smart_list([_RADIUS_RULE])) as s:
        result = await search_entity_logic("lead", [], smart_list="Holy West Team")
    assert s.posted.args == ("/v1/search/lead",)
    assert s.posted.kwargs["json"]["jsonRule"] == {"condition": "AND", "valid": True, "rules": [_RADIUS_RULE]}
    assert s.resolver.await_args.args == ("Holy West Team", "LEAD")
    assert 'Smart list "Holy West Team" (id 146421) used with its 1 saved condition(s)' in result
    assert "Found 1 lead" in result and "a@b.com" in result


@pytest.mark.asyncio
async def test_smart_list_plus_filters_keeps_its_rules_first_then_the_filters():
    with _Search(_smart_list([_RADIUS_RULE])) as s:
        result = await search_entity_logic("lead", [dict(_FILTER)], smart_list="Holy West Team")
    rule = s.posted.kwargs["json"]["jsonRule"]
    assert rule["condition"] == "AND"
    assert rule["rules"][0] == _RADIUS_RULE and [r["field"] for r in rule["rules"]] == ["addressCoordinate", "firstName"]
    assert "plus 1 extra filter(s)" in result


@pytest.mark.asyncio
async def test_smart_list_without_conditions_means_everyone_and_sends_null():
    with _Search(_smart_list([], name="All leads", list_id=98341)) as s:
        result = await search_entity_logic("lead", [], smart_list="All leads")
    assert s.posted.kwargs["json"]["jsonRule"] is None  # Kylas rejects an empty rules list, accepts null
    assert "no conditions (everyone)" in result


@pytest.mark.asyncio
async def test_smart_list_without_conditions_plus_filters_sends_only_the_filters():
    with _Search(_smart_list([], name="All leads", list_id=98341)) as s:
        result = await search_entity_logic("lead", [dict(_FILTER)], smart_list="All leads")
    assert [r["field"] for r in s.posted.kwargs["json"]["jsonRule"]["rules"]] == ["firstName"]
    assert "so only your 1 filter(s)" in result


@pytest.mark.asyncio
async def test_contact_search_uses_the_contact_smart_lists_and_endpoint():
    with _Search(_smart_list([_RADIUS_RULE], name="Marketing")) as s:
        await search_entity_logic("contact", [], smart_list="Marketing")
    assert s.resolver.await_args.args == ("Marketing", "CONTACT")
    assert s.posted.args == ("/v1/search/contact",)


@pytest.mark.asyncio
async def test_page_size_and_sort_still_apply_with_a_smart_list():
    with _Search(_smart_list([_RADIUS_RULE])) as s:
        await search_entity_logic("lead", [], page=2, size=500, sort="updatedAt,desc", smart_list=146421)
    assert s.posted.kwargs["params"] == {"page": 2, "size": 100, "sort": "updatedAt,desc"}
    assert s.resolver.await_args.args == (146421, "LEAD")


@pytest.mark.asyncio
async def test_several_matching_smart_lists_ask_first_and_search_nothing():
    choice = {"matches": [{"id": 1, "name": "West A"}, {"id": 2, "name": "West B"}],
              "note": "STOP: more than one lead smart list matched 'West'."}
    with _Search(AsyncMock(return_value=(None, choice))) as s:
        result = await search_entity_logic("lead", [], smart_list="West")
    assert result.startswith("STOP") and "West A (id 1)" in result and "West B (id 2)" in result
    s.client.post.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_smart_list_is_an_error_and_searches_nothing():
    missing = KylasAPIError("No lead smart list name contains 'Nope'.", status_code=404)
    with _Search(AsyncMock(side_effect=missing)) as s:
        result = await search_entity_logic("lead", [], smart_list="Nope")
    assert result.startswith("Error searching lead:") and "No lead smart list name contains 'Nope'" in result
    s.client.post.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("entity", ["deal", "company", "task", "meeting"])
async def test_smart_list_is_refused_for_other_entities(entity):
    with _Search(_smart_list([_RADIUS_RULE])) as s:
        result = await search_entity_logic(entity, [], smart_list="Holy West Team")
    assert "lead and contact searches only" in result
    s.resolver.assert_not_awaited()
    s.client.post.assert_not_called()


@pytest.mark.asyncio
async def test_lead_search_still_needs_filters_or_a_smart_list():
    with _Search(_smart_list([])) as s:
        result = await search_entity_logic("lead", [])
    assert "or a smart_list" in result
    s.client.post.assert_not_called()


@pytest.mark.asyncio
async def test_search_without_a_smart_list_is_unchanged():
    with _Search(_smart_list([_RADIUS_RULE])) as s:
        result = await search_entity_logic("lead", [dict(_FILTER)])
    assert [r["field"] for r in s.posted.kwargs["json"]["jsonRule"]["rules"]] == ["firstName"]
    s.resolver.assert_not_awaited()
    assert "Smart list" not in result


# --- wiring: build_payload / execute_request -----------------------------------------


@pytest.mark.asyncio
async def test_build_payload_shows_smart_list_for_lead_and_contact_search_only():
    for entity_id, expected in (("lead.search", True), ("contact.search", True),
                                ("deal.search", False), ("company.search", False), ("task.search", False)):
        props = json.loads(await build_payload(entity_id))["schema"]["properties"]
        assert ("smart_list" in props) is expected, entity_id


@pytest.mark.asyncio
async def test_build_payload_for_the_smart_list_lookup_needs_an_entity():
    result = json.loads(await build_payload("smart_list.lookup"))
    assert result["schema"]["required"] == ["entity"]
    assert set(result["schema"]["properties"]) == {"entity", "query"}


@pytest.mark.asyncio
async def test_execute_request_lead_search_with_a_smart_list():
    with _Search(_smart_list([_RADIUS_RULE])) as s:
        result = json.loads(await execute_request("lead.search", {"smart_list": "Holy West Team"}))
    assert result["ok"] and "Smart list \"Holy West Team\"" in result["data"]
    assert s.posted.kwargs["json"]["jsonRule"]["rules"] == [_RADIUS_RULE]


@pytest.mark.asyncio
async def test_smart_list_lookup_routes_through_execute_request():
    rows = {"content": [{"id": 146421, "name": "Holy West Team"}, {"id": 98341, "name": "All leads"}]}
    with patch("app.campaigns._kylas_get", AsyncMock(return_value=rows)) as get:
        result = json.loads(await execute_request("smart_list.lookup", {"entity": "lead", "query": "Holy"}))
    assert result["ok"]
    assert result["data"] == {"entity": "LEAD", "smart_lists": [{"id": 146421, "name": "Holy West Team"},
                                                                {"id": 98341, "name": "All leads"}]}
    assert get.await_args.args[0] == "/v1/search-lists/lead/lookup"
