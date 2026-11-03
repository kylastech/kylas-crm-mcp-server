"""What a caller writes -> what Kylas accepts, per entity (app/references.py).

Field metadata here is shaped like QA's (GET /v1/entities/lead/fields etc.):
lead/contact companyIndustry, timezone, country… carry `internal: true` and
take the option's NAME; other lead/contact picklists take the plain id;
company/deal/meeting/call-log picklists take {"id": n}.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import references
from app.helpers import _normalize_field_values, resolve_picklist_values
from app.references import resolve_references


def _picklist(name, options, internal=False, multi=False):
    return {"name": name, "type": "MULTI_PICKLIST" if multi else "PICK_LIST", "internal": internal,
            "picklist": {"values": [{"id": i, "name": n, "displayName": d} for i, n, d in options]}}


SOURCE = _picklist("source", [(11, "GOOGLE", "Google"), (12, "WEBSITE", "Website")])
TIMEZONE = _picklist("timezone", [(501, "Asia/Calcutta", "(GMT+05:30) Kolkata")], internal=True)
MULTI = _picklist("cfMultiPick", [(71, "PICK2", "Pick two"), (72, "PICK3", "Pick three")], multi=True)
UNITS = {"name": "units", "type": "PICK_LIST", "picklist": {"values": []}}
FIELDS = [SOURCE, TIMEZONE, MULTI, UNITS]


async def _fields():
    return FIELDS


# --- picklists ------------------------------------------------------------------

@pytest.mark.fields
@pytest.mark.asyncio
async def test_a_lead_picklist_label_becomes_the_plain_option_id():
    out = await resolve_references("lead", {"source": "website"}, _fields)
    assert out["source"] == 12


@pytest.mark.fields
@pytest.mark.asyncio
async def test_a_lead_internal_picklist_takes_the_option_name():
    """companyIndustry, timezone, country… are sent as the option's name."""
    out = await resolve_references("lead", {"timezone": "(GMT+05:30) Kolkata"}, _fields)
    assert out["timezone"] == "Asia/Calcutta"


@pytest.mark.fields
@pytest.mark.asyncio
async def test_a_lead_custom_multi_picklist_takes_a_list_of_ids():
    out = await resolve_references("lead", {"customFieldValues": {"cfMultiPick": ["pick two", "PICK3"]}}, _fields)
    assert out["customFieldValues"]["cfMultiPick"] == [71, 72]


@pytest.mark.fields
@pytest.mark.asyncio
@pytest.mark.parametrize("entity", ["deal", "company", "meeting", "call_log"])
async def test_deal_company_meeting_and_call_log_picklists_take_an_object(entity):
    out = await resolve_references(entity, {"source": "Google", "cfMultiPick": ["Pick two"]}, _fields)
    assert out["source"] == {"id": 11}
    assert out["cfMultiPick"] == [{"id": 71}]


@pytest.mark.fields
@pytest.mark.asyncio
async def test_an_id_given_in_either_shape_is_reshaped_not_looked_up():
    assert (await resolve_references("lead", {"source": {"id": 11}}, _fields))["source"] == 11
    assert (await resolve_references("deal", {"source": 11}, _fields))["source"] == {"id": 11}


def test_an_unknown_label_lists_the_options():
    with pytest.raises(ValueError, match=r"'Fax' is not an option of picklist field 'source'.*Google=11.*Website=12"):
        resolve_picklist_values({"source": "Fax"}, FIELDS)


def test_an_unlisted_numeric_id_is_left_for_kylas_to_check():
    assert resolve_picklist_values({"source": 999}, FIELDS)["source"] == 999


def test_a_picklist_with_no_options_listed_is_left_as_sent():
    assert resolve_picklist_values({"units": "kg"}, FIELDS, wrap=True)["units"] == "kg"


@pytest.mark.fields
@pytest.mark.asyncio
async def test_field_metadata_is_fetched_once_per_entity_and_caller():
    fetch = AsyncMock(return_value=FIELDS)
    await resolve_references("lead", {"source": "Google"}, fetch)
    await resolve_references("lead", {"source": "Website"}, fetch)
    assert fetch.await_count == 1


# --- owners -------------------------------------------------------------------------

USERS = [{"id": 4010, "name": "Shubham Katte"}, {"id": 8238, "name": "Ajit Dada"}]


def _lookup_by_name(rows):
    async def lookup(path, wanted, extra=None):
        return [r for r in rows if wanted.lower() in r["name"].lower()]
    return lookup


@pytest.mark.asyncio
@pytest.mark.parametrize("entity, field, expected", [
    ("lead", "ownerId", 8238), ("contact", "ownerId", 8238), ("call_log", "ownerId", 8238),
    ("deal", "ownedBy", {"id": 8238}), ("company", "ownedBy", {"id": 8238}),
    ("meeting", "owner", {"id": 8238}),
])
async def test_an_owner_named_is_resolved_to_its_id_in_the_entitys_shape(entity, field, expected):
    with patch("app.references._lookup", new=_lookup_by_name(USERS)):
        out = await resolve_references(entity, {field: "ajit dada"})
    assert out[field] == expected


@pytest.mark.asyncio
async def test_an_owner_given_by_id_is_not_looked_up():
    lookup = AsyncMock()
    with patch("app.references._lookup", new=lookup):
        assert (await resolve_references("lead", {"ownerId": "4010"}))["ownerId"] == 4010
        assert (await resolve_references("deal", {"ownedBy": {"id": 4010, "name": "x"}}))["ownedBy"] == {"id": 4010}
    lookup.assert_not_called()


@pytest.mark.asyncio
async def test_an_ambiguous_owner_lists_the_candidates():
    rows = [{"id": 1, "name": "Ajit Dada"}, {"id": 2, "name": "Ajit Kumar"}]
    with patch("app.references._lookup", new=_lookup_by_name(rows)), \
            pytest.raises(ValueError, match=r"'Ajit' matches several users for ownerId: Ajit Dada=1, Ajit Kumar=2"):
        await resolve_references("lead", {"ownerId": "Ajit"})


@pytest.mark.asyncio
async def test_an_exact_name_wins_over_partial_matches():
    rows = [{"id": 1, "name": "Ajit"}, {"id": 2, "name": "Ajit Kumar"}]
    with patch("app.references._lookup", new=_lookup_by_name(rows)):
        assert (await resolve_references("lead", {"ownerId": "Ajit"}))["ownerId"] == 1


@pytest.mark.asyncio
async def test_an_unknown_owner_says_so():
    with patch("app.references._lookup", new=_lookup_by_name(USERS)), \
            pytest.raises(ValueError, match="No user named 'Nobody' for ownerId"):
        await resolve_references("lead", {"ownerId": "Nobody"})


# --- pipeline / stage -----------------------------------------------------------------

PIPELINES = [{"id": 3610, "name": "Default Deal Pipeline"}, {"id": 6372, "name": "new pipleine"}]


@pytest.mark.asyncio
async def test_a_pipeline_and_stage_named_are_resolved_to_ids():
    with patch("app.references._lookup", new=_lookup_by_name(PIPELINES)), \
            patch("app.references._stage_id", new=AsyncMock(return_value=22948)) as stage:
        out = await resolve_references("deal", {"pipeline": {"name": "Default Deal Pipeline",
                                                             "stage": {"name": "Won"}}})
    assert out["pipeline"] == {"id": 3610, "stage": {"id": 22948}}
    stage.assert_awaited_once_with(3610, "Won")


@pytest.mark.asyncio
async def test_a_stage_named_on_a_pipeline_given_by_id():
    with patch("app.references._stage_id", new=AsyncMock(return_value=22945)):
        out = await resolve_references("lead", {"pipeline": {"id": 3610, "stage": "Open"}})
    assert out["pipeline"] == {"id": 3610, "stage": {"id": 22945}}


@pytest.mark.asyncio
async def test_a_stage_is_found_on_its_pipeline_and_a_wrong_one_lists_the_stages():
    response = MagicMock()
    client = AsyncMock()
    client.get.return_value = response
    client.__aenter__.return_value = client
    stages = {"stages": [{"id": 22945, "name": "Open"}, {"id": 22948, "name": "Won"}]}
    with patch("app.references.get_client", return_value=client), \
            patch("app.references.handle_api_response", new=AsyncMock(return_value=stages)):
        assert await references._stage_id(3610, "won") == 22948
        with pytest.raises(ValueError, match="no stage named 'Lost'. Its stages: Open=22945, Won=22948"):
            await references._stage_id(3610, "Lost")


# --- products ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_products_named_are_resolved_keeping_their_other_fields():
    rows = [{"id": 55, "name": "Visa"}]
    with patch("app.references._lookup", new=_lookup_by_name(rows)):
        out = await resolve_references("deal", {"products": [
            {"name": "Visa", "quantity": 2, "price": {"currencyId": 431, "value": 100}}, 56]})
    assert out["products"] == [{"name": "Visa", "quantity": 2, "price": {"currencyId": 431, "value": 100}, "id": 55},
                               {"id": 56}]


# --- phones: an international number brings its own country ------------------------------

@pytest.mark.parametrize("value", ["+919823289810", "+91 98232 89810", "0091 98232 89810"])
def test_an_international_number_needs_no_separate_country(value):
    phones = _normalize_field_values({"phoneNumbers": [{"value": value}]})["phoneNumbers"]
    assert phones[0]["code"] == "IN"


def test_the_shorthand_phone_takes_its_country_from_the_number_too():
    phones = _normalize_field_values({"phone": "+91 98232 89810", "phone_type": "MOBILE"})["phoneNumbers"]
    assert phones[0]["code"] == "IN"


def test_a_national_number_still_needs_its_country():
    with pytest.raises(ValueError, match="country/dial code was not"):
        _normalize_field_values({"phoneNumbers": [{"value": "9823289810"}]})


# --- wired into create/update ---------------------------------------------------------------

@pytest.mark.fields
@pytest.mark.asyncio
async def test_create_deal_sends_resolved_references():
    from app.entities import create_deal_logic

    client = AsyncMock()
    created = MagicMock()
    created.json.return_value = {"id": 1}
    client.post.return_value = created
    client.__aenter__.return_value = client
    with patch("app.entities.get_client", return_value=client), \
            patch("app.entities.handle_api_response", new=AsyncMock(return_value={"id": 1})), \
            patch("app.references._fields", new=AsyncMock(return_value=FIELDS)), \
            patch("app.references._lookup", new=_lookup_by_name(USERS)):
        await create_deal_logic({"name": "Acme renewal", "source": "Google", "ownedBy": "Ajit Dada"})

    sent = client.post.call_args.kwargs["json"]
    assert sent["source"] == {"id": 11} and sent["ownedBy"] == {"id": 8238}


TZ = _picklist("timezone", [(317, "Etc/GMT+12", "(GMT-12:00) International Date Line West"),
                            (380, "Asia/Calcutta", "(GMT+05:30) Chennai, Kolkata, Mumbai, New Delhi"),
                            (381, "Asia/Colombo", "(GMT+05:30) Sri Jayawardenepura")], internal=True)


def test_a_unique_part_of_an_option_label_is_enough():
    assert resolve_picklist_values({"timezone": "kolkata"}, [TZ])["timezone"] == "Asia/Calcutta"


def test_a_part_shared_by_several_options_is_refused_with_those_first():
    with pytest.raises(ValueError, match=r"'GMT\+05:30' is not an option.*: \(GMT\+05:30\) Chennai.*=380, \(GMT\+05:30\) Sri"):
        resolve_picklist_values({"timezone": "GMT+05:30"}, [TZ])


@pytest.mark.asyncio
async def test_a_full_name_is_searched_by_its_first_word_and_picked_by_all():
    """Kylas's user lookup matches one word: "name:Shubham Katte" finds nobody."""
    rows = [{"id": 4010, "name": "Shubham Katte"}, {"id": 10586, "name": "Shubham Akshayaaa"}]
    lookup = AsyncMock(return_value=rows)
    with patch("app.references._lookup", new=lookup):
        out = await resolve_references("lead", {"ownerId": "shubham  katte"})
    assert out["ownerId"] == 4010
    assert lookup.await_args.args[1] == "shubham"


@pytest.mark.asyncio
@pytest.mark.parametrize("related", [{"id": 7, "entity": "lead"}, [{"id": 7, "entity": "lead"}]])
async def test_create_call_log_puts_the_number_in_related_to(related):
    """sd-call reads the number from relatedTo only; a top-level one is dropped."""
    from app.entities import create_call_log_logic

    client = AsyncMock()
    client.__aenter__.return_value = client
    with patch("app.entities.get_client", return_value=client), \
            patch("app.entities.handle_api_response", new=AsyncMock(return_value={"id": 1})):
        await create_call_log_logic({"outcome": "connected", "startTime": "2026-10-30T10:00:00Z",
                                     "callType": "outgoing", "duration": 30,
                                     "phoneNumber": "+919823289813", "relatedTo": related})

    sent = client.post.call_args.kwargs["json"]["relatedTo"]
    assert (sent[0] if isinstance(sent, list) else sent)["phoneNumber"] == "+919823289813"
