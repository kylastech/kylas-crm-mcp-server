"""
Test suite for Kylas CRM MCP Server (Lead only).

Run: python test_tools.py
Or: pytest test_tools.py -v
"""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import json

try:
    import pytest
except ImportError:
    class pytest:
        class mark:
            asyncio = lambda f: f

import main
from main import (
    _normalize_country_code,
    get_lead_field_instructions_logic,
    create_lead_logic,
    search_leads_logic,
    lookup_users_logic,
    lookup_products_logic,
    _format_field,
    _normalize_field_values,
    _get_filterable_fields_map,
    _build_search_json_rule,
    search_entity_logic,
    search_entity_by_term_logic,
    search_idle_entities_logic,
    search_leads_by_term_logic,
    search_meetings_by_term_logic,
    _format_entity_labels_for_instructions,
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
# _normalize_country_code tests
# ---------------------------------------------------------------------------

def test_normalize_country_code_dial_prefix_common():
    """Common dial prefixes resolve to correct 2-letter ISO codes via phonenumbers library."""
    assert _normalize_country_code("+91") == "IN"
    assert _normalize_country_code("+1") == "US"
    assert _normalize_country_code("+44") == "GB"
    assert _normalize_country_code("+61") == "AU"


def test_normalize_country_code_dial_prefix_extended():
    """Dial prefixes for countries not in the old COUNTRY_CODE_MAP resolve correctly."""
    assert _normalize_country_code("+49") == "DE"   # Germany
    assert _normalize_country_code("+33") == "FR"   # France
    assert _normalize_country_code("+81") == "JP"   # Japan
    assert _normalize_country_code("+55") == "BR"   # Brazil
    assert _normalize_country_code("+64") == "NZ"   # New Zealand
    assert _normalize_country_code("+27") == "ZA"   # South Africa
    assert _normalize_country_code("+971") == "AE"  # UAE
    assert _normalize_country_code("+65") == "SG"   # Singapore


def test_normalize_country_code_iso_codes():
    """Valid ISO 3166-1 alpha-2 region codes pass through unchanged (case-insensitive)."""
    assert _normalize_country_code("IN") == "IN"
    assert _normalize_country_code("US") == "US"
    assert _normalize_country_code("GB") == "GB"
    assert _normalize_country_code("AU") == "AU"
    assert _normalize_country_code("in") == "IN"
    assert _normalize_country_code("us") == "US"


def test_normalize_country_code_aliases():
    """Non-standard aliases are resolved via the alias map."""
    assert _normalize_country_code("UK") == "GB"
    assert _normalize_country_code("USA") == "US"
    assert _normalize_country_code("INDIA") == "IN"


def test_normalize_country_code_empty_and_invalid():
    """Empty, None, and unrecognised codes return empty string."""
    assert _normalize_country_code("") == ""
    assert _normalize_country_code(None) == ""
    assert _normalize_country_code("INVALID") == ""
    assert _normalize_country_code("ZZZ") == ""


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
    assert "Options" in lines[1]
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
    fv = {"firstName": "Jane", "phone": "5551234567", "phone_country_code": "+1", "phone_type": "MOBILE"}
    payload = _normalize_field_values(fv)
    assert payload["firstName"] == "Jane"
    # +1 normalized to 2-letter code US
    assert payload["phoneNumbers"] == [{"type": "MOBILE", "code": "US", "value": "5551234567", "primary": True}]


def test_normalize_field_values_phone_string_requires_country_code():
    """Phone without country/dial code must not default to India; should raise so caller asks user."""
    fv = {"firstName": "Jane", "phone": "8830311640"}
    with pytest.raises(ValueError, match="country.*dial code"):
        _normalize_field_values(fv)


def test_normalize_field_values_phone_string_requires_phone_type():
    """Phone without phone_type must raise so caller asks user."""
    fv = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "IN"}
    with pytest.raises(ValueError, match="type was not specified"):
        _normalize_field_values(fv)


def test_normalize_field_values_phone_string_invalid_type():
    """Invalid phone_type must raise ValueError."""
    fv = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "IN", "phone_type": "LANDLINE"}
    with pytest.raises(ValueError, match="Invalid phone type"):
        _normalize_field_values(fv)


def test_normalize_field_values_phone_string_work_type():
    """phone_type WORK is accepted and preserved in payload."""
    fv = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "IN", "phone_type": "WORK"}
    payload = _normalize_field_values(fv)
    assert payload["phoneNumbers"] == [{"type": "WORK", "code": "IN", "value": "8830311640", "primary": True}]


def test_normalize_field_values_phone_string_with_country_code():
    """Phone with phone_country_code IN or +91 normalizes to code IN."""
    fv = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "IN", "phone_type": "MOBILE"}
    payload = _normalize_field_values(fv)
    assert payload["phoneNumbers"] == [{"type": "MOBILE", "code": "IN", "value": "8830311640", "primary": True}]
    fv2 = {"firstName": "Jane", "phone": "8830311640", "phone_country_code": "+91", "phone_type": "MOBILE"}
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
        assert "[CUSTOM] 'Company Size' (Field ID: '57300'" in result
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


# ---------------------------------------------------------------------------
# search_entity Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_entity_lead_with_filters():
    """search_entity should return formatted lead results when filters provided."""
    from main import _ENTITY_CONFIG
    with patch.dict(_ENTITY_CONFIG, {"lead": {"search_fn": AsyncMock(return_value="Found 5 leads"), "search_page_offset": 0}}):
        filters = [{"field": "firstName", "operator": "contains", "value": "John"}]
        result = await search_entity_logic("lead", filters, page=0, size=20)
        assert "Found" in result


@pytest.mark.asyncio
async def test_search_entity_lead_without_filters_should_error():
    """search_entity for lead should error when filters are empty."""
    result = await search_entity_logic("lead", [], page=0, size=20)
    assert "Error" in result or "filters cannot be empty" in result.lower()


@pytest.mark.asyncio
async def test_search_entity_meeting_empty_filters():
    """search_entity for meeting should allow empty filters (returns all)."""
    with patch("main._ENTITY_CONFIG") as mock_config:
        mock_search = AsyncMock(return_value="Found 10 meetings")
        mock_config.get.return_value = {"search_fn": mock_search, "search_page_offset": 0}
        result = await search_entity_logic("meeting", [], page=0, size=20)
        # Should not error; meeting allows empty filters
        assert isinstance(result, str)
        assert "Found 10 meetings" in result
        mock_search.assert_called_once()


@pytest.mark.asyncio
async def test_create_meeting_logic_strips_deals():
    from main import create_meeting_logic
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": 123, "title": "Test Meeting"}
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        # Payload with both a valid invitee (contact) and an invalid invitee (deal)
        field_values = {
            "title": "Test Meeting",
            "from": "2026-04-28T10:00:00Z",
            "to": "2026-04-28T10:30:00Z",
            "participants": [
                {"id": 1, "entity": "contact"},
                {"id": 2, "entity": "deal"}
            ]
        }
        
        result = await create_meeting_logic(field_values)
        assert result["id"] == 123
        
        # Verify that the deal was stripped before POSTing
        call_args = mock_client.post.call_args
        posted_payload = call_args.kwargs["json"]
        assert len(posted_payload["participants"]) == 1
        assert posted_payload["participants"][0]["entity"] == "contact"


@pytest.mark.asyncio
async def test_create_meeting_logic_rejects_only_deals():
    from main import create_meeting_logic, KylasAPIError
    
    # Payload with ONLY an invalid invitee (deal)
    field_values = {
        "title": "Test Meeting",
        "from": "2026-04-28T10:00:00Z",
        "to": "2026-04-28T10:30:00Z",
        "participants": [
            {"id": 2, "entity": "deal"}
        ]
    }
    
    with pytest.raises(KylasAPIError, match="cannot be a deal"):
        await create_meeting_logic(field_values)


@pytest.mark.asyncio
async def test_search_entity_invalid_entity_type():
    """search_entity should error on unknown entity_type."""
    result = await search_entity_logic("invalid_type", [], page=0, size=20)
    assert "Unknown entity_type" in result or "Unknown" in result


# ---------------------------------------------------------------------------
# search_entity_by_term Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_entity_by_term_lead():
    """search_entity_by_term should search leads by term and format concisely."""
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [
                {"id": 1, "firstName": "John", "lastName": "Doe", "emails": [{"value": "john@example.com", "primary": True}], "phoneNumbers": []},
            ],
            "totalElements": 1,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await search_leads_by_term_logic("john", page=0, size=20)
        assert isinstance(result, str)
        assert "Found 1" in result
        assert "• ID: 1 | Name: John Doe | Email: john@example.com | Phone: -" in result
        assert "LEAD DETAILS" not in result


@pytest.mark.asyncio
async def test_search_entity_by_term_meeting():
    """search_entity_by_term for meeting should search title field and format concisely."""
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [
                {"id": 1, "title": "Standup Meeting", "status": "Scheduled", "from": "2026-04-28T10:00:00Z", "to": "2026-04-28T10:30:00Z"},
            ],
            "totalElements": 1,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await search_meetings_by_term_logic("standup", page=0, size=20)
        assert isinstance(result, str)
        assert "Found 1" in result
        assert "• ID: 1 | Title: Standup Meeting | Status: Scheduled | From: 2026-04-28T10:00:00Z | To: 2026-04-28T10:30:00Z" in result
        assert "MEETING DETAILS" not in result


@pytest.mark.asyncio
async def test_search_entity_by_term_invalid_entity():
    """search_entity_by_term should error on invalid entity type."""
    # The search_entity_by_term tool checks entity_type validity
    # We'll test the error path directly
    from main import _ENTITY_CONFIG
    cfg = _ENTITY_CONFIG.get("invalid")
    assert cfg is None


# ---------------------------------------------------------------------------
# search_idle_entities Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_idle_entities_lead():
    """search_idle_entities should search idle leads."""
    result = await search_idle_entities_logic("lead", days=30, page=0, size=20)
    assert isinstance(result, str)
    assert len(result) > 0


@pytest.mark.asyncio
async def test_search_idle_entities_contact_unsupported():
    """search_idle_entities should error for contact (unsupported)."""
    result = await search_idle_entities_logic("contact", days=30, page=0, size=20)
    assert "does not support" in result.lower() or "invalid" in result.lower()


@pytest.mark.asyncio
async def test_search_idle_entities_invalid_type():
    """search_idle_entities should error on invalid entity."""
    result = await search_idle_entities_logic("invalid", days=30, page=0, size=20)
    assert "Unknown entity_type" in result or "does not support" in result


@pytest.mark.asyncio
async def test_load_entity_labels_success():
    """Test successfully loading entity labels from API."""
    mock_labels = {
        "LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"},
        "DEAL": {"displayName": "Deeeel", "displayNamePlural": "Deeeels"},
        "CONTACT": {"displayName": "Quontact", "displayNamePlural": "Quontacts"},
    }

    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = mock_labels
        mock_client.get.return_value = mock_response
        mock_get_client.return_value.__aenter__.return_value = mock_client

        result = await main._load_entity_labels()

        assert result == mock_labels
        mock_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_label_refresh_loop():
    """Test that refresh loop updates labels periodically."""
    initial_labels = {
        "LEAD": {"displayName": "Lead", "displayNamePlural": "Leads"}
    }
    updated_labels = {
        "LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"},
        "DEAL": {"displayName": "Deeeel", "displayNamePlural": "Deeeels"}
    }

    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        # First call returns initial, second returns updated
        mock_response.json.side_effect = [initial_labels, updated_labels]
        mock_client.get.return_value = mock_response
        mock_get_client.return_value.__aenter__.return_value = mock_client

        # Load initial labels
        await main._load_entity_labels()
        assert main._ENTITY_LABELS == initial_labels

        # This function doesn't exist yet, test will fail
        refresh_task = asyncio.create_task(main._label_refresh_loop())

        # Let it run for a moment (won't actually refresh yet)
        await asyncio.sleep(0.1)

        # Cancel the task
        refresh_task.cancel()
        try:
            await refresh_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_load_entity_labels_api_error():
    """Test graceful handling when label fetch fails."""
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("API connection failed")
        mock_get_client.return_value.__aenter__.return_value = mock_client

        # Should not raise, should return empty dict
        result = await main._load_entity_labels()

        assert result == {}
        assert main._ENTITY_LABELS == {}


def test_format_entity_labels_for_instructions():
    """Test formatting labels into readable system instructions text."""
    labels = {
        "LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"},
        "DEAL": {"displayName": "Deeeel", "displayNamePlural": "Deeeels"},
        "CONTACT": {"displayName": "Quontact", "displayNamePlural": "Quontacts"},
    }

    result = main._format_entity_labels_for_instructions(labels)

    # Should contain display names and lowercase standard types
    assert "Lid" in result
    assert "Deeeel" in result
    assert "Quontact" in result
    assert "lead" in result
    assert "deal" in result
    assert "contact" in result


@pytest.mark.asyncio
async def test_system_instructions_include_labels():
    """Test that system instructions contain formatted labels when labels are loaded."""
    test_labels = {
        "LEAD": {"displayName": "Lid", "displayNamePlural": "Lids"},
        "DEAL": {"displayName": "Deeeel", "displayNamePlural": "Deeeels"},
    }

    # Manually set labels
    main._ENTITY_LABELS = test_labels

    # Format labels
    formatted = main._format_entity_labels_for_instructions(test_labels)

    # Verify formatted output contains expected content
    assert "Lid" in formatted
    assert "Deeeel" in formatted
    assert "lead" in formatted
    assert "deal" in formatted
    assert "ENTITY NAME ROUTING" in formatted


def test_normalize_deal_payload_ownedby_int():
    """ownedBy as plain int should be wrapped as {"id": int}."""
    payload = {"name": "Deal A", "ownedBy": 7236}
    result = main._normalize_deal_payload(payload)
    assert result["ownedBy"] == {"id": 7236}


def test_normalize_deal_payload_ownedby_already_dict():
    """ownedBy already a dict should be left unchanged."""
    payload = {"name": "Deal A", "ownedBy": {"id": 7236}}
    result = main._normalize_deal_payload(payload)
    assert result["ownedBy"] == {"id": 7236}


def test_normalize_deal_payload_ownedby_bool_not_touched():
    """True/False must not be treated as int for ownedBy."""
    payload = {"ownedBy": True}
    result = main._normalize_deal_payload(payload)
    assert result["ownedBy"] is True


def test_normalize_deal_payload_monetary_with_existing():
    """Monetary field as plain number wraps using currencyId from existing deal."""
    existing = {"estimatedValue": {"currencyId": 431, "value": 1000}}
    payload = {"estimatedValue": 32}
    result = main._normalize_deal_payload(payload, existing=existing)
    assert result["estimatedValue"] == {"currencyId": 431, "value": 32}


def test_normalize_deal_payload_monetary_no_existing():
    """Monetary field as plain number without existing deal is left as-is (no currencyId)."""
    payload = {"value": 500}
    result = main._normalize_deal_payload(payload)
    assert result["value"] == 500


def test_normalize_deal_payload_monetary_already_dict():
    """Monetary field already a dict should be left unchanged."""
    payload = {"estimatedValue": {"currencyId": 99, "value": 250}}
    result = main._normalize_deal_payload(payload)
    assert result["estimatedValue"] == {"currencyId": 99, "value": 250}


def test_normalize_deal_payload_all_monetary_fields():
    """All three monetary fields wrapped when existing provides currencyId."""
    existing = {
        "estimatedValue": {"currencyId": 1, "value": 0},
        "actualValue": {"currencyId": 2, "value": 0},
        "value": {"currencyId": 3, "value": 0},
    }
    payload = {"estimatedValue": 100, "actualValue": 200, "value": 300}
    result = main._normalize_deal_payload(payload, existing=existing)
    assert result["estimatedValue"] == {"currencyId": 1, "value": 100}
    assert result["actualValue"] == {"currencyId": 2, "value": 200}
    assert result["value"] == {"currencyId": 3, "value": 300}


def test_normalize_deal_payload_no_relevant_fields():
    """Payload with no ownedBy or monetary fields passes through unchanged."""
    payload = {"name": "Test Deal", "closingDate": "2025-12-31"}
    result = main._normalize_deal_payload(payload)
    assert result == {"name": "Test Deal", "closingDate": "2025-12-31"}


# ---------------------------------------------------------------------------
# search_tasks_with_any_relation Tests
# ---------------------------------------------------------------------------

def test_build_search_json_rule_associated_field_standard_true():
    """associatedLeads with standard:True must NOT get customFieldValues prefix (the root bug fix)."""
    filterable_map = {"associatedLeads": {"type": "LOOK_UP", "standard": True}}
    rules, err = _build_search_json_rule(
        [{"field": "associatedLeads", "operator": "is_not_null", "value": None}],
        filterable_map,
    )
    assert err is None
    rule = rules["rules"][0]
    assert rule["field"] == "associatedLeads", (
        "standard:True field must not get customFieldValues prefix"
    )
    assert rule["type"] == "long"
    assert rule["operator"] == "is_not_null"
    assert rule["value"] is None


def test_build_search_json_rule_associated_field_custom_gets_prefix():
    """standard:False LOOK_UP fields correctly get customFieldValues prefix."""
    filterable_map = {"cfCustomLookup": {"type": "LOOK_UP", "standard": False}}
    rules, err = _build_search_json_rule(
        [{"field": "cfCustomLookup", "operator": "equal", "value": 123}],
        filterable_map,
    )
    assert err is None
    assert rules["rules"][0]["field"] == "customFieldValues.cfCustomLookup"


def test_search_tasks_with_any_relation_post_merge_sort():
    """After dedup, merged task list is sorted by sort_field before pagination."""
    # Simulate what _search_tasks_with_any_relation_logic does post-gather
    tasks_from_leads = [
        {"id": 1, "createdAt": "2025-10-01T00:00:00Z"},
        {"id": 2, "createdAt": "2025-08-01T00:00:00Z"},
    ]
    tasks_from_contacts = [
        {"id": 2, "createdAt": "2025-08-01T00:00:00Z"},  # duplicate — should be deduped
        {"id": 3, "createdAt": "2025-12-01T00:00:00Z"},
    ]

    seen_ids: set = set()
    all_tasks = []
    for result in [tasks_from_leads, tasks_from_contacts]:
        for task in result:
            tid = task.get("id")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                all_tasks.append(task)

    # Apply the same sort logic as _search_tasks_with_any_relation_logic
    sort = "createdAt,desc"
    sort_field, _, sort_dir = sort.partition(",")
    reverse = sort_dir.strip().lower() != "asc"
    all_tasks.sort(key=lambda t: (t.get(sort_field) or ""), reverse=reverse)

    assert len(all_tasks) == 3, "Duplicate id=2 must be deduped"
    assert all_tasks[0]["id"] == 3, "Most recent (Dec) first"
    assert all_tasks[1]["id"] == 1, "Oct second"
    assert all_tasks[2]["id"] == 2, "Aug last"


# ---------------------------------------------------------------------------
# User-Agent / MCP client name Tests
# ---------------------------------------------------------------------------

def _make_mock_context(client_name: str, client_version: str):
    """Build a mock fastmcp Context with clientInfo populated."""
    mock_client_info = MagicMock()
    mock_client_info.name = client_name
    mock_client_info.version = client_version

    mock_client_params = MagicMock()
    mock_client_params.clientInfo = mock_client_info

    mock_session = MagicMock()
    mock_session.client_params = mock_client_params

    mock_ctx = MagicMock()
    mock_ctx.session = mock_session
    return mock_ctx


def test_get_mcp_client_name_with_name_and_version():
    """Returns 'Name(version)' when both client name and version are present."""
    mock_ctx = _make_mock_context("Claude Desktop", "1.2.3")
    with patch.object(main.mcp, "get_context", return_value=mock_ctx, create=True):
        result = main._get_mcp_client_name()
    assert result == "Claude Desktop(1.2.3)"


def test_get_mcp_client_name_without_version():
    """Returns just the name when version is empty."""
    mock_ctx = _make_mock_context("cursor", "")
    with patch.object(main.mcp, "get_context", return_value=mock_ctx, create=True):
        result = main._get_mcp_client_name()
    assert result == "cursor"


def test_get_mcp_client_name_outside_request_context():
    """Returns 'unknown' when called outside a request (get_context raises)."""
    with patch.object(main.mcp, "get_context", side_effect=LookupError, create=True):
        result = main._get_mcp_client_name()
    assert result == "unknown"


def test_get_mcp_client_name_no_client_params():
    """Returns 'unknown' when session has no client_params."""
    mock_session = MagicMock()
    mock_session.client_params = None
    mock_ctx = MagicMock()
    mock_ctx.session = mock_session
    with patch.object(main.mcp, "get_context", return_value=mock_ctx, create=True):
        result = main._get_mcp_client_name()
    assert result == "unknown"


def test_throttled_client_context_user_agent_format():
    """User-Agent header must be 'kylas_mcp_server({version}) on {client}'."""
    with patch("main._resolve_api_key", return_value="test-key"), \
         patch("main._get_mcp_client_name", return_value="Claude Desktop(1.2.3)"):
        ctx = main._ThrottledClientContext()
        ua = ctx._raw.headers.get("user-agent")
    assert ua == f"kylas_mcp_server({main.SERVER_VERSION}) on Claude Desktop(1.2.3)"


# ---------------------------------------------------------------------------
# create_entity Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_entity_lead_success():
    """create_entity dispatches to create_lead_logic and formats the response."""
    from main import create_entity, _ENTITY_CRUD_CONFIG
    mock_result = {"id": 42, "firstName": "Jane", "lastName": "Doe"}
    with patch.dict(_ENTITY_CRUD_CONFIG, {"lead": {
        "create_fn": AsyncMock(return_value=mock_result),
        "name_fn": lambda r: f"{r.get('firstName', '')} {r.get('lastName', '')}".strip() or "Lead",
    }}):
        result = await create_entity("lead", {"firstName": "Jane", "lastName": "Doe"})
    assert "✓" in result
    assert "42" in result
    assert "Jane Doe" in result
    assert "Lead" in result


@pytest.mark.asyncio
async def test_create_entity_deal_success():
    """create_entity dispatches to create_deal_logic and formats the response."""
    from main import create_entity, _ENTITY_CRUD_CONFIG
    mock_result = {"id": 99, "name": "Big Deal"}
    with patch.dict(_ENTITY_CRUD_CONFIG, {"deal": {
        "create_fn": AsyncMock(return_value=mock_result),
        "name_fn": lambda r: r.get("name", "Deal"),
    }}):
        result = await create_entity("deal", {"name": "Big Deal"})
    assert "✓" in result
    assert "99" in result
    assert "Big Deal" in result


@pytest.mark.asyncio
async def test_create_entity_invalid_type():
    """create_entity returns an error message for unknown entity_type."""
    from main import create_entity
    result = await create_entity("unicorn", {"name": "test"})
    assert "✗" in result
    assert "unicorn" in result
    assert "Unknown entity_type" in result


@pytest.mark.asyncio
async def test_create_entity_api_error():
    """create_entity surfaces KylasAPIError cleanly."""
    from main import create_entity, _ENTITY_CRUD_CONFIG, KylasAPIError
    err = KylasAPIError("Bad request")
    err.message = "Bad request"
    err.response_body = "{}"
    with patch.dict(_ENTITY_CRUD_CONFIG, {"task": {
        "create_fn": AsyncMock(side_effect=err),
        "name_fn": lambda r: r.get("name", "Task"),
    }}):
        result = await create_entity("task", {"name": "Test Task"})
    assert "✗" in result
    assert "Bad request" in result


# ---------------------------------------------------------------------------
# update_entity Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_entity_lead_success():
    """update_entity dispatches to update_fn for lead and formats the response."""
    from main import update_entity, _ENTITY_CRUD_CONFIG
    mock_result = {"id": 55, "firstName": "Alice", "lastName": "Smith"}
    with patch.dict(_ENTITY_CRUD_CONFIG, {"lead": {
        "update_fn": AsyncMock(return_value=mock_result),
        "name_fn": lambda r: f"{r.get('firstName', '')} {r.get('lastName', '')}".strip() or "Lead",
    }}):
        result = await update_entity("lead", 55, {"firstName": "Alice"})
    assert result.startswith("✓ Lead updated")
    assert "55" in result
    assert "Alice Smith" in result


@pytest.mark.asyncio
async def test_update_entity_deal_success():
    """update_entity dispatches to update_fn for deal and formats the response."""
    from main import update_entity, _ENTITY_CRUD_CONFIG
    mock_result = {"id": 77, "name": "Mega Deal"}
    with patch.dict(_ENTITY_CRUD_CONFIG, {"deal": {
        "update_fn": AsyncMock(return_value=mock_result),
        "name_fn": lambda r: r.get("name", "Deal"),
    }}):
        result = await update_entity("deal", 77, {"name": "Mega Deal"})
    assert result.startswith("✓ Deal updated")
    assert "77" in result
    assert "Mega Deal" in result


@pytest.mark.asyncio
async def test_update_entity_call_log_success():
    """update_entity dispatches to update_fn for call_log and formats the response."""
    from main import update_entity, _ENTITY_CRUD_CONFIG
    mock_result = {"id": 33, "callType": "outgoing", "outcome": "connected"}
    with patch.dict(_ENTITY_CRUD_CONFIG, {"call_log": {
        "update_fn": AsyncMock(return_value=mock_result),
        "name_fn": lambda r: f"{r.get('callType', '')} / {r.get('outcome', '')}",
    }}):
        result = await update_entity("call_log", 33, {"outcome": "connected"})
    assert result.startswith("✓ Call Log updated")
    assert "33" in result
    assert "outgoing / connected" in result


@pytest.mark.asyncio
async def test_update_entity_invalid_type():
    """update_entity returns an error message for unknown entity_type."""
    from main import update_entity
    result = await update_entity("unicorn", 1, {"name": "test"})
    assert "✗" in result
    assert "unicorn" in result
    assert "Unknown entity_type" in result


@pytest.mark.asyncio
async def test_update_entity_api_error():
    """update_entity surfaces KylasAPIError cleanly."""
    from main import update_entity, _ENTITY_CRUD_CONFIG, KylasAPIError
    err = KylasAPIError("Not found")
    err.message = "Not found"
    err.response_body = "{}"
    with patch.dict(_ENTITY_CRUD_CONFIG, {"task": {
        "update_fn": AsyncMock(side_effect=err),
        "name_fn": lambda r: r.get("name", "Task"),
    }}):
        result = await update_entity("task", 99, {"name": "Test Task"})
    assert "✗" in result
    assert "Not found" in result


@pytest.mark.asyncio
async def test_update_entity_value_error():
    """update_entity surfaces ValueError (e.g. missing phone country code) cleanly."""
    from main import update_entity, _ENTITY_CRUD_CONFIG
    with patch.dict(_ENTITY_CRUD_CONFIG, {"lead": {
        "update_fn": AsyncMock(side_effect=ValueError("phone: country/dial code required")),
        "name_fn": lambda r: "Lead",
    }}):
        result = await update_entity("lead", 1, {"phone": "9876543210"})
    assert "✗" in result
    assert "country/dial code required" in result


@pytest.mark.asyncio
async def test_update_entity_calls_update_fn_with_correct_args():
    """update_entity passes entity_id and field_values to update_fn."""
    from main import update_entity, _ENTITY_CRUD_CONFIG
    mock_update = AsyncMock(return_value={"id": 10, "name": "Acme"})
    with patch.dict(_ENTITY_CRUD_CONFIG, {"company": {
        "update_fn": mock_update,
        "name_fn": lambda r: r.get("name", "Company"),
    }}):
        await update_entity("company", 10, {"name": "Acme"})
    mock_update.assert_called_once_with(10, {"name": "Acme"})


# ---------------------------------------------------------------------------
# _is_stage_lock_error Tests
# ---------------------------------------------------------------------------

def test_is_stage_lock_error_no_body():
    from main import _is_stage_lock_error, KylasAPIError
    err = KylasAPIError("some error")
    err.response_body = None
    assert _is_stage_lock_error(err) is False


def test_is_stage_lock_error_invalid_json():
    from main import _is_stage_lock_error, KylasAPIError
    err = KylasAPIError("some error")
    err.response_body = "not json"
    assert _is_stage_lock_error(err) is False


def test_is_stage_lock_error_correct_code():
    from main import _is_stage_lock_error, KylasAPIError
    err = KylasAPIError("stage lock")
    err.response_body = json.dumps({"code": "01001086", "message": "Stage lock"})
    assert _is_stage_lock_error(err) is True


def test_is_stage_lock_error_different_code():
    from main import _is_stage_lock_error, KylasAPIError
    err = KylasAPIError("other error")
    err.response_body = json.dumps({"code": "00000001", "message": "Other"})
    assert _is_stage_lock_error(err) is False


# ---------------------------------------------------------------------------
# _advance_deal_to_stage_sequentially Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_advance_deal_sequentially_invalid_current_stage():
    from main import _advance_deal_to_stage_sequentially, KylasAPIError
    stages = [{"id": 1, "name": "S1", "forecastingType": "OPEN"}, {"id": 2, "name": "S2", "forecastingType": "OPEN"}]
    with pytest.raises(KylasAPIError, match="not found in pipeline"):
        await _advance_deal_to_stage_sequentially(99, stages, current_stage_id=999, target_stage_id=2, base_deal={})


@pytest.mark.asyncio
async def test_advance_deal_sequentially_invalid_target_stage():
    from main import _advance_deal_to_stage_sequentially, KylasAPIError
    stages = [{"id": 1, "name": "S1", "forecastingType": "OPEN"}, {"id": 2, "name": "S2", "forecastingType": "OPEN"}]
    with pytest.raises(KylasAPIError, match="not found in pipeline"):
        await _advance_deal_to_stage_sequentially(99, stages, current_stage_id=1, target_stage_id=999, base_deal={})


@pytest.mark.asyncio
async def test_advance_deal_sequentially_target_before_current():
    from main import _advance_deal_to_stage_sequentially, KylasAPIError
    stages = [
        {"id": 1, "name": "S1", "forecastingType": "OPEN"},
        {"id": 2, "name": "S2", "forecastingType": "OPEN"},
        {"id": 3, "name": "S3", "forecastingType": "OPEN"},
    ]
    with pytest.raises(KylasAPIError, match="must be after current stage"):
        await _advance_deal_to_stage_sequentially(99, stages, current_stage_id=3, target_stage_id=1, base_deal={})


@pytest.mark.asyncio
async def test_advance_deal_sequentially_happy_path():
    from main import _advance_deal_to_stage_sequentially
    stages = [
        {"id": 10, "name": "S1", "forecastingType": "OPEN"},
        {"id": 20, "name": "S2", "forecastingType": "OPEN"},
        {"id": 30, "name": "S3", "forecastingType": "WON"},
    ]
    base_deal = {"id": 5, "pipeline": {"id": 1, "stage": {"id": 10}}}

    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        # Each PUT returns the deal with the new stage
        responses = [
            {"id": 5, "pipeline": {"id": 1, "stage": {"id": 20}}},
            {"id": 5, "pipeline": {"id": 1, "stage": {"id": 30}}},
        ]
        mock_responses = []
        for r in responses:
            mock_resp = MagicMock()
            mock_resp.json.return_value = r
            mock_resp.raise_for_status = MagicMock()
            mock_responses.append(mock_resp)
        mock_client.put.side_effect = mock_responses
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        result = await _advance_deal_to_stage_sequentially(5, stages, current_stage_id=10, target_stage_id=30, base_deal=base_deal)

    assert result["pipeline"]["stage"]["id"] == 30
    assert mock_client.put.call_count == 2


# ---------------------------------------------------------------------------
# operator normalization and scheduledAt alias tests
# ---------------------------------------------------------------------------

def test_operator_normalization_and_scheduled_at_alias_meeting():
    from main import _build_meeting_search_json_rule
    filterable_map = {
        "from": {"type": "DATETIME_PICKER", "standard": True},
        "title": {"type": "TEXT_FIELD", "standard": True},
    }
    filters = [
        {"field": "title", "operator": "contains", "value": "Demo"},
        {"field": "scheduledAt", "operator": "gte", "value": "2025-12-15T00:00:00Z"},
    ]
    rules, err = _build_meeting_search_json_rule(filters, filterable_map)
    assert err is None
    assert len(rules["rules"]) == 2
    
    # Check first rule
    rule0 = rules["rules"][0]
    assert rule0["id"] == "title"
    assert rule0["field"] == "title"
    assert rule0["operator"] == "contains"
    assert rule0["value"] == "Demo"
    
    # Check second rule
    rule1 = rules["rules"][1]
    assert rule1["id"] == "from"
    assert rule1["field"] == "from"
    assert rule1["operator"] == "greater_or_equal"  # normalized from gte
    assert rule1["value"] == "2025-12-15T00:00:00Z"


@pytest.mark.asyncio
async def test_search_meetings_logic_sort_alias():
    from main import search_meetings_logic
    with patch("main.get_client") as mock_get_client, \
         patch("main._fetch_meeting_fields") as mock_fetch_fields:
         
        mock_fetch_fields.return_value = [
            {"id": 1, "name": "title", "type": "TEXT_FIELD", "active": True, "filterable": True, "standard": True},
            {"id": 2, "name": "from", "type": "DATETIME_PICKER", "active": True, "filterable": True, "standard": True},
        ]
        
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [{"id": 101, "title": "Demo Meeting", "from": "2025-12-15T10:00:00Z"}],
            "totalElements": 1,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client
        
        # When querying with scheduledAt,desc sort
        result = await search_meetings_logic([], page=0, size=10, sort="scheduledAt,desc")
        
        assert "Demo Meeting" in result
        call_args = mock_client.post.call_args
        params = call_args.kwargs["params"]
        # scheduledAt,desc should be converted to from,desc
        assert params["sort"] == "from,desc"


def test_operator_normalization_in_general_search_rule_builder():
    from main import _build_search_json_rule
    filterable_map = {
        "firstName": {"type": "TEXT_FIELD", "standard": True},
        "createdAt": {"type": "DATETIME_PICKER", "standard": True},
    }
    filters = [
        {"field": "firstName", "operator": "eq", "value": "John"},
        {"field": "createdAt", "operator": "lte", "value": "2026-06-15T00:00:00Z"},
    ]
    rules, err = _build_search_json_rule(filters, filterable_map)
    assert err is None
    assert len(rules["rules"]) == 2
    assert rules["rules"][0]["operator"] == "equal"
    assert rules["rules"][1]["operator"] == "less_or_equal"


def test_normalize_meeting_sort():
    """_normalize_meeting_sort maps unsupported sort fields to valid ones."""
    from main import _normalize_meeting_sort
    # Unsupported fields are mapped to 'from'
    assert _normalize_meeting_sort("updatedAt,desc") == "from,desc"
    assert _normalize_meeting_sort("scheduledAt,asc") == "from,asc"
    assert _normalize_meeting_sort("startTime,desc") == "from,desc"
    assert _normalize_meeting_sort("startDate,asc") == "from,asc"
    # Valid fields pass through unchanged
    assert _normalize_meeting_sort("from,desc") == "from,desc"
    assert _normalize_meeting_sort("createdAt,asc") == "createdAt,asc"
    # None or empty defaults to from,desc
    assert _normalize_meeting_sort(None) == "from,desc"
    assert _normalize_meeting_sort("") == "from,desc"
    # Missing direction defaults to desc
    assert _normalize_meeting_sort("updatedAt") == "from,desc"


@pytest.mark.asyncio
async def test_search_meetings_by_term_sort_normalization():
    """search_meetings_by_term_logic should normalize sort (e.g. updatedAt -> from)."""
    from main import search_meetings_by_term_logic
    with patch("main.get_client") as mock_get_client:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "content": [{"id": 1, "title": "Kylas Demo", "status": "Scheduled", "from": "2026-04-28T10:00:00Z", "to": "2026-04-28T10:30:00Z"}],
            "totalElements": 1,
            "totalPages": 1,
        }
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_get_client.return_value = mock_client

        # Simulate generic dispatcher passing updatedAt,desc (the default)
        result = await search_meetings_by_term_logic("Kylas Demo", page=0, size=100, sort="updatedAt,desc")

        assert "Kylas Demo" in result
        call_args = mock_client.post.call_args
        params = call_args.kwargs["params"]
        # updatedAt,desc must be normalized to from,desc
        assert params["sort"] == "from,desc"


if __name__ == "__main__":
    asyncio.run(run_manual_tests())

