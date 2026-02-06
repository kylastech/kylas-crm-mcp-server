"""
Test suite for Kylas CRM MCP Server (Lead only).

Run: python test_tools.py
Or: pytest test_tools.py -v
"""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

try:
    import pytest
except ImportError:
    class pytest:
        class mark:
            asyncio = lambda f: f

from main import (
    get_lead_field_instructions_logic,
    create_lead_logic,
    search_leads_logic,
    lookup_users_logic,
    lookup_products_logic,
    _format_field,
    _normalize_field_values,
    _get_filterable_fields_map,
    _build_search_json_rule,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_FIELDS_RESPONSE = [
    {
        "id": 57256,
        "displayName": "First Name",
        "name": "firstName",
        "type": "TEXT_FIELD",
        "standard": True,
        "active": True,
        "required": False,
    },
    {
        "id": 57257,
        "displayName": "Last Name",
        "name": "lastName",
        "type": "TEXT_FIELD",
        "standard": True,
        "active": True,
        "required": True,
    },
    {
        "id": 100,
        "displayName": "Lead Source",
        "name": "leadSource",
        "type": "PICK_LIST",
        "standard": True,
        "active": True,
        "picklist": {
            "values": [
                {"id": 1001, "displayName": "Website"},
                {"id": 1002, "displayName": "Referral"},
            ]
        },
    },
    {
        "id": 57300,
        "displayName": "Company Size",
        "name": "companySize",
        "type": "PICK_LIST",
        "standard": False,
        "active": True,
        "picklist": {
            "values": [
                {"id": 12345, "displayName": "Small"},
                {"id": 67890, "displayName": "Large"},
            ]
        },
    },
]

MOCK_CREATE_LEAD_RESPONSE = {
    "id": 42619370,
    "firstName": "John",
    "lastName": "Doe",
    "emails": [{"type": "OFFICE", "value": "john@example.com", "primary": True}],
}


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------

def test_format_field_standard():
    field = {
        "id": 57256,
        "displayName": "First Name",
        "name": "firstName",
        "type": "TEXT_FIELD",
        "standard": True,
        "required": True,
    }
    lines = _format_field(field)
    assert len(lines) == 1
    assert "[STANDARD]" in lines[0]
    assert "'First Name'" in lines[0]
    assert "API Name: 'firstName'" in lines[0]
    assert "*REQUIRED*" in lines[0]


def test_format_field_custom_with_picklist():
    field = {
        "id": 57300,
        "displayName": "Company Size",
        "name": "companySize",
        "type": "PICK_LIST",
        "standard": False,
        "picklist": {
            "values": [
                {"id": 12345, "displayName": "Small"},
                {"id": 67890, "displayName": "Large"},
            ]
        },
    }
    lines = _format_field(field)
    assert "[CUSTOM]" in lines[0]
    assert "Field ID: '57300'" in lines[0]
    assert "Options:" in lines[1]
    assert "Small (ID: 12345)" in lines[2]
    assert "Large (ID: 67890)" in lines[3]


def test_normalize_field_values_standard_only():
    fv = {"firstName": "John", "lastName": "Doe"}
    payload = _normalize_field_values(fv)
    assert payload["firstName"] == "John"
    assert payload["lastName"] == "Doe"
    assert "customFieldValues" not in payload


def test_normalize_field_values_email_string():
    fv = {"firstName": "John", "lastName": "Doe", "email": "john@example.com"}
    payload = _normalize_field_values(fv)
    assert payload["firstName"] == "John"
    assert payload["lastName"] == "Doe"
    assert payload["emails"] == [{"type": "OFFICE", "value": "john@example.com", "primary": True}]


def test_normalize_field_values_phone_string():
    fv = {"firstName": "Jane", "phone": "5551234567", "phone_country_code": "+1"}
    payload = _normalize_field_values(fv)
    assert payload["firstName"] == "Jane"
    # +1 normalized to 2-letter code US
    assert payload["phoneNumbers"] == [{"type": "MOBILE", "code": "US", "value": "5551234567", "primary": True}]


def test_normalize_field_values_phone_string_requires_country_code():
    """Phone without country/dial code must not default to India; should raise so caller asks user."""
    fv = {"firstName": "Jane", "phone": "8830311640"}
    with pytest.raises(ValueError, match="country.*dial code"):
        _normalize_field_values(fv)


def test_normalize_field_values_phone_string_with_country_code():
    """Phone with phone_country_code IN or +91 normalizes to code IN."""
    fv = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "IN"}
    payload = _normalize_field_values(fv)
    assert payload["phoneNumbers"] == [{"type": "MOBILE", "code": "IN", "value": "8830311640", "primary": True}]
    fv2 = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "+91"}
    payload2 = _normalize_field_values(fv2)
    assert payload2["phoneNumbers"] == [{"type": "MOBILE", "code": "IN", "value": "8830311640", "primary": True}]


def test_normalize_field_values_emails_single_primary():
    fv = {"emails": [{"type": "OFFICE", "value": "a@k.io", "primary": True}, {"type": "PERSONAL", "value": "b@gmail.com", "primary": False}]}
    payload = _normalize_field_values(fv)
    assert payload["emails"][0]["primary"] is True
    assert payload["emails"][1]["primary"] is False


def test_normalize_field_values_phone_numbers_single_primary():
    # Top-level phone_country_code required whenever any phone data is present (even if entries have "code").
    fv = {"phone_country_code": "IN", "phoneNumbers": [{"type": "MOBILE", "code": "IN", "value": "9090909090", "primary": True}, {"type": "WORK", "code": "US", "value": "4155550132", "primary": False}]}
    payload = _normalize_field_values(fv)
    assert len(payload["phoneNumbers"]) == 2
    assert payload["phoneNumbers"][0]["primary"] is True
    assert payload["phoneNumbers"][1]["primary"] is False


def test_normalize_field_values_phone_numbers_array_requires_country_code():
    """phoneNumbers array without top-level phone_country_code must raise (do not assume India)."""
    fv = {"phoneNumbers": [{"type": "MOBILE", "code": "IN", "value": "7447631718", "primary": True}]}
    with pytest.raises(ValueError, match="country.*dial code"):
        _normalize_field_values(fv)


def test_normalize_field_values_custom_fields():
    fv = {"firstName": "John", "lastName": "Doe", "57256": 12345, "57300": "Enterprise"}
    payload = _normalize_field_values(fv)
    assert payload["firstName"] == "John"
    assert payload["lastName"] == "Doe"
    assert payload["customFieldValues"]["57256"] == 12345
    assert payload["customFieldValues"]["57300"] == "Enterprise"


def test_normalize_field_values_explicit_custom_field_values():
    """Custom fields with string keys (e.g. cfLeadCheck) via explicit customFieldValues."""
    fv = {
        "firstName": "Abhinav",
        "lastName": "Kale",
        "customFieldValues": {"cfLeadCheck": "Checked"},
    }
    payload = _normalize_field_values(fv)
    assert payload["firstName"] == "Abhinav"
    assert payload["lastName"] == "Kale"
    assert payload["customFieldValues"]["cfLeadCheck"] == "Checked"


def test_normalize_field_values_custom_field_id_resolved_to_name():
    """Custom field sent by ID (1210985) is stored under internal name (cfLeadCheck) in customFieldValues."""
    fv = {
        "firstName": "shubham",
        "lastName": "dadas",
        "companyName": "kylas",
        "1210985": "Checked",
    }
    id_to_name = {"1210985": "cfLeadCheck"}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    assert payload["firstName"] == "shubham"
    assert payload["lastName"] == "dadas"
    assert payload["companyName"] == "kylas"
    assert "1210985" not in payload.get("customFieldValues", {})
    assert payload["customFieldValues"]["cfLeadCheck"] == "Checked"


def test_normalize_field_values_picklist_at_top_level():
    fv = {"firstName": "John", "lastName": "Doe", "leadSource": 1001}
    payload = _normalize_field_values(fv)
    assert payload["leadSource"] == 1001


# ---------------------------------------------------------------------------
# Tool logic tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_lead_field_instructions_success():
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_FIELDS_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await get_lead_field_instructions_logic()

        assert "KYLAS CRM - LEAD FIELDS CHEAT SHEET" in result
        assert "[STANDARD] 'First Name' (API Name: 'firstName')" in result
        assert "[CUSTOM] 'Company Size' (Field ID: '57300')" in result
        assert "Website (ID: 1001)" in result
        assert "Small (ID: 12345)" in result


@pytest.mark.asyncio
async def test_create_lead_dynamic_field_values():
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_CREATE_LEAD_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        field_values = {
            "firstName": "John",
            "lastName": "Doe",
            "email": "john@example.com",
            "leadSource": 1001,
            "57256": 12345,
        }

        result = await create_lead_logic(field_values)

        assert result["id"] == 42619370
        call_args = mock_client.post.call_args
        payload = call_args.kwargs["json"]
        assert payload["firstName"] == "John"
        assert payload["lastName"] == "Doe"
        assert payload["emails"][0]["value"] == "john@example.com"
        assert payload["leadSource"] == 1001
        assert payload["customFieldValues"]["57256"] == 12345


@pytest.mark.asyncio
async def test_create_lead_minimal_fields():
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 1, "firstName": "A", "lastName": "B"}
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await create_lead_logic({"firstName": "A", "lastName": "B"})
        assert result["id"] == 1


def test_build_search_json_rule():
    """Search jsonRule uses filterable fields and correct rule type (string vs long)."""
    filterable_map = {
        "firstName": {"type": "TEXT_FIELD", "standard": True},
        "country": {"type": "PICK_LIST", "standard": True},
        "source": {"type": "PICK_LIST", "standard": True},
    }
    rules, err = _build_search_json_rule(
        [
            {"field": "firstName", "operator": "equal", "value": "Akshay"},
            {"field": "country", "operator": "equal", "value": "AF"},
            {"field": "source", "operator": "equal", "value": 16136},
        ],
        filterable_map,
    )
    assert err is None
    assert rules["condition"] == "AND"
    assert rules["valid"] is True
    assert len(rules["rules"]) == 3
    assert rules["rules"][0]["field"] == "firstName" and rules["rules"][0]["type"] == "string" and rules["rules"][0]["value"] == "Akshay"
    assert rules["rules"][1]["field"] == "country" and rules["rules"][1]["type"] == "string" and rules["rules"][1]["value"] == "AF"
    assert rules["rules"][2]["field"] == "source" and rules["rules"][2]["type"] == "long" and rules["rules"][2]["value"] == 16136


def test_build_search_json_rule_date_datetime():
    """Date/datetime filters use type 'date' and include timeZone."""
    filterable_map = {
        "createdAt": {"type": "DATETIME_PICKER", "standard": True},
        "convertedAt": {"type": "DATETIME_PICKER", "standard": True},
    }
    # today operator: value null, timeZone from default (patch so tests are deterministic)
    with patch("main.DEFAULT_TIMEZONE", "Asia/Calcutta"):
        rules, err = _build_search_json_rule(
            [{"field": "createdAt", "operator": "today", "value": None}],
            filterable_map,
        )
    assert err is None
    assert rules["rules"][0]["type"] == "date"
    assert rules["rules"][0]["value"] is None
    assert rules["rules"][0]["timeZone"] == "Asia/Calcutta"

    # between: value array of two ISO strings; optional timeZone in filter
    rules2, err2 = _build_search_json_rule(
        [
            {
                "field": "createdAt",
                "operator": "between",
                "value": ["2026-02-01T10:00:00.000Z", "2026-02-06T10:00:00.000Z"],
                "timeZone": "Asia/Calcutta",
            }
        ],
        filterable_map,
    )
    assert err2 is None
    assert rules2["rules"][0]["type"] == "date"
    assert rules2["rules"][0]["value"] == ["2026-02-01T10:00:00.000Z", "2026-02-06T10:00:00.000Z"]
    assert rules2["rules"][0]["timeZone"] == "Asia/Calcutta"

    # is_not_null: value null (uses default timeZone)
    with patch("main.DEFAULT_TIMEZONE", "Asia/Calcutta"):
        rules3, err3 = _build_search_json_rule(
            [{"field": "convertedAt", "operator": "is_not_null", "value": None}],
            filterable_map,
        )
    assert err3 is None
    assert rules3["rules"][0]["type"] == "date"
    assert rules3["rules"][0]["value"] is None
    assert rules3["rules"][0]["timeZone"] == "Asia/Calcutta"


def test_build_search_json_rule_custom_date_field():
    """Custom date field: field path customFieldValues.cfDateField, type date, single ISO value for greater_or_equal."""
    filterable_map = {"cfDateField": {"type": "DATETIME_PICKER", "standard": False}}
    rules, err = _build_search_json_rule(
        [
            {
                "field": "cfDateField",
                "operator": "greater_or_equal",
                "value": "2026-02-02T18:30:00.000Z",
                "timeZone": "Asia/Calcutta",
            }
        ],
        filterable_map,
    )
    assert err is None
    assert rules["rules"][0]["id"] == "cfDateField"
    assert rules["rules"][0]["field"] == "customFieldValues.cfDateField"
    assert rules["rules"][0]["type"] == "date"
    assert rules["rules"][0]["value"] == "2026-02-02T18:30:00.000Z"
    assert rules["rules"][0]["timeZone"] == "Asia/Calcutta"


def test_build_search_json_rule_custom_picklist_field_path():
    """Custom picklist/multipicklist filters must use field path customFieldValues.<name>."""
    filterable_map = {
        "cfFruits": {"type": "MULTI_PICKLIST", "standard": False},
        "cfFlower": {"type": "PICK_LIST", "standard": False},
    }
    rules, err = _build_search_json_rule(
        [
            {"field": "cfFruits", "operator": "equal", "value": 2797122},
            {"field": "cfFlower", "operator": "equal", "value": 2797126},
        ],
        filterable_map,
    )
    assert err is None
    assert rules["rules"][0]["id"] == "cfFruits"
    assert rules["rules"][0]["field"] == "customFieldValues.cfFruits"
    assert rules["rules"][0]["type"] == "long"
    assert rules["rules"][0]["value"] == 2797122
    assert rules["rules"][1]["id"] == "cfFlower"
    assert rules["rules"][1]["field"] == "customFieldValues.cfFlower"
    assert rules["rules"][1]["type"] == "long"
    assert rules["rules"][1]["value"] == 2797126


def test_build_search_json_rule_look_up_field_type_long():
    """LOOK_UP / ENTITY_FIELDS (createdBy, ownerId, etc.) use type long (user ID)."""
    filterable_map = {"createdBy": {"type": "LOOK_UP", "standard": True}}
    rules, err = _build_search_json_rule(
        [{"field": "createdBy", "operator": "equal", "value": 59867}],
        filterable_map,
    )
    assert err is None
    assert rules["rules"][0]["type"] == "long"
    assert rules["rules"][0]["value"] == 59867
    assert rules["rules"][0]["field"] == "createdBy"


def test_build_search_json_rule_rejects_non_filterable():
    filterable_map = {"firstName": {"type": "TEXT_FIELD", "standard": True}}
    _, err = _build_search_json_rule([{"field": "unknownField", "operator": "equal", "value": "x"}], filterable_map)
    assert err is not None
    assert "not filterable" in err.lower() or "not found" in err.lower()


@pytest.mark.asyncio
async def test_lookup_users_logic():
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [{"id": 59867, "name": "First Last"}, {"id": 59878, "name": "First1 Last1"}],
            "totalElements": 2,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await lookup_users_logic("firstName:last")
        assert "Found 2 user(s)" in result
        assert "59867" in result
        assert "First Last" in result
        assert "More than one user matched" in result

    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"content": [{"id": 594, "name": "Akshay"}], "totalElements": 1, "totalPages": 1}
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result_single = await lookup_users_logic("name:Akshay")
        assert "Use user ID 594" in result_single


@pytest.mark.asyncio
async def test_lookup_products_logic():
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [{"id": 245208, "name": "Widget A"}, {"id": 245209, "name": "Widget B"}],
            "totalElements": 2,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await lookup_products_logic("name:Widget")
        assert "Found 2 product(s)" in result
        assert "245208" in result
        assert "Widget A" in result
        assert "More than one product matched" in result

    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"content": [{"id": 245208, "name": "Widget Pro"}], "totalElements": 1, "totalPages": 1}
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result_single = await lookup_products_logic("Widget Pro")
        assert "Use product ID 245208" in result_single


@pytest.mark.asyncio
async def test_search_leads_logic():
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [
                {"id": 1, "firstName": "A", "lastName": "B", "emails": [{"value": "a@b.com", "primary": True}], "phoneNumbers": []},
            ],
            "totalElements": 1,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        with patch("main._fetch_lead_fields") as mock_fetch:
            mock_fetch.return_value = [
                {"id": 1, "name": "firstName", "type": "TEXT_FIELD", "active": True, "filterable": True, "standard": True},
            ]
            result = await search_leads_logic([{"field": "firstName", "operator": "equal", "value": "A"}])
            assert "Found 1 lead" in result
            assert "a@b.com" in result
            call_args = mock_client.post.call_args
            assert call_args.kwargs["json"]["jsonRule"]["condition"] == "AND"
            assert call_args.kwargs["json"]["jsonRule"]["rules"][0]["field"] == "firstName"


# ---------------------------------------------------------------------------
# Manual run
# ---------------------------------------------------------------------------

async def run_manual_tests():
    print("=" * 60)
    print("KYLAS CRM MCP (LEAD ONLY) - TEST SUITE")
    print("=" * 60)

    # Test 1
    print("\n[TEST 1] get_lead_field_instructions")
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_FIELDS_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client
        result = await get_lead_field_instructions_logic()
        assert "KYLAS CRM - LEAD FIELDS CHEAT SHEET" in result
    print("✓ PASSED")

    # Test 2
    print("\n[TEST 2] create_lead with dynamic field_values (custom field ID resolved to internal name)")
    with patch("main.get_client") as mock_get_client, patch("main._get_custom_field_id_to_name") as mock_id_to_name:
        mock_id_to_name.return_value = {"57256": "companySize"}
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_CREATE_LEAD_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client
        result = await create_lead_logic({
            "firstName": "John",
            "lastName": "Doe",
            "email": "john@example.com",
            "leadSource": 1001,
            "57256": 12345,
        })
        assert result["id"] == 42619370
        # Custom field must be sent with internal name, not ID
        payload = mock_client.post.call_args.kwargs["json"]
        assert "customFieldValues" in payload
        assert "companySize" in payload["customFieldValues"]
        assert payload["customFieldValues"]["companySize"] == 12345
        assert "57256" not in payload["customFieldValues"]
    print("✓ PASSED")

    # Test 3
    print("\n[TEST 3] _normalize_field_values (ID resolved to internal name)")
    payload = _normalize_field_values({"firstName": "J", "lastName": "D", "email": "j@d.com"})
    assert payload["emails"][0]["value"] == "j@d.com"
    # Without id_to_name map, numeric key stays as-is; with map we use internal name
    payload2 = _normalize_field_values({"firstName": "X", "57300": "Val"})
    assert payload2["customFieldValues"]["57300"] == "Val"
    payload3 = _normalize_field_values({"firstName": "shubham", "lastName": "dadas", "1210985": "Checked"}, custom_field_id_to_name={"1210985": "cfLeadCheck"})
    assert payload3["customFieldValues"]["cfLeadCheck"] == "Checked"
    assert "1210985" not in payload3["customFieldValues"]
    print("✓ PASSED")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_manual_tests())
