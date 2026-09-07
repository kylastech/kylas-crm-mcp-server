"""
entities/deal.py — everything specific to the "deal" bucket: the
sequential-stage-lock handling (Kylas rejects skipping a stage on a
pipeline with sequentialStageFlow=true; this retries by advancing one
stage at a time), field instructions, deal-specific search rule builder,
deal-specific payload normalization (ownedBy/monetary fields), create/
update/get, display formatting, search (+ by-term, + idle).

DEAL_LARGE_PICKLIST_FIELDS mirrors entities/lead.py's fix for the same
_BUCKET_PICKLIST_RULES circular-import issue — see that file's docstring.
Deal already had its own DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME before this
refactor (both here and in _build_deal_search_json_rule, which already used
it directly, no fix needed there).

update_deal_logic imports get_contact_logic from entities.contact (to
resolve contact IDs to display names for associatedContacts) — an entity
importing another entity is fine, no cycle, since contact doesn't need deal.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
    _threshold_iso_days_ago,
    _convert_date_value_to_utc,
    _epoch_to_iso_utc,
    _fetch_current_user,
    OPERATOR_MAPPING,
    OPERATOR_SYMBOL_MAP,
    _format_field,
    _get_filterable_fields_map,
    _rule_type_for_value,
    _normalize_field_values,
    _multi_field_json_rule,
    _get_pipeline_details_raw,
)
from entities.contact import get_contact_logic

# Kylas error code returned when trying to skip a stage on a pipeline with sequentialStageFlow=true
STAGE_LOCK_ERROR_CODE = "01001086"


def _is_stage_lock_error(error: KylasAPIError) -> bool:
    """Return True if this error is the Kylas sequential-stage-flow lock (code 01001086)."""
    if not error.response_body:
        return False
    try:
        body = json.loads(error.response_body)
        return body.get("code") == STAGE_LOCK_ERROR_CODE
    except (json.JSONDecodeError, AttributeError, TypeError):
        return False


async def _advance_deal_to_stage_sequentially(
    deal_id: int,
    stages: list,
    current_stage_id: int,
    target_stage_id: int,
    base_deal: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Move deal through pipeline stages one step at a time (required when sequentialStageFlow=true).
    stages: list of stage dicts ordered by position (each has id, forecastingType).
    Advances from current_stage_id → target_stage_id inclusive, returning the final deal state.
    """
    stage_ids = [s["id"] for s in stages]
    if current_stage_id not in stage_ids:
        raise KylasAPIError(f"Current stage {current_stage_id} not found in pipeline stages.")
    if target_stage_id not in stage_ids:
        raise KylasAPIError(f"Target stage {target_stage_id} not found in pipeline stages.")

    current_idx = stage_ids.index(current_stage_id)
    target_idx = stage_ids.index(target_stage_id)

    if target_idx <= current_idx:
        raise KylasAPIError(
            f"Target stage (position {target_idx + 1}) must be after current stage (position {current_idx + 1}) "
            "for sequential advancement."
        )

    result = base_deal
    async with get_client() as client:
        for idx in range(current_idx + 1, target_idx + 1):
            stage = stages[idx]
            stage_id = stage["id"]
            merged = dict(result)
            if isinstance(merged.get("pipeline"), dict):
                merged["pipeline"] = dict(merged["pipeline"])
                merged["pipeline"]["stage"] = dict(merged["pipeline"].get("stage") or {})
                merged["pipeline"]["stage"]["id"] = stage_id
                forecast_type = stage.get("forecastingType")
                if forecast_type:
                    merged["forecastingType"] = forecast_type
            response = await client.put(f"/deals/{deal_id}", json=merged)
            result = await handle_api_response(response, f"Advance deal {deal_id} to stage {stage_id}")
            logger.info("Deal %s advanced to stage %s (%s)", deal_id, stage_id, stage.get("name", ""))

    return result


# ===========================================================================
# DEAL TOOLS
# ===========================================================================

# Picklist fields that use internal name (string) in search; all others use Option ID (long)
DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME = {"currency", "country", "dealSource"}

# This bucket has no oversized picklists (matches main.py's _BUCKET_PICKLIST_RULES["deal"]).
DEAL_LARGE_PICKLIST_FIELDS: set = set()

# ---------------------------------------------------------------------------
# Deal field metadata helpers
# ---------------------------------------------------------------------------

async def _fetch_deal_fields() -> List[Dict[str, Any]]:
    """Fetch deal field metadata from Kylas API. Returns list of field dicts."""
    async with get_client() as client:
        response = await client.get(
            "/deals/fields",
            params={"page": 0, "size": 100}
        )
        data = await handle_api_response(response, "Fetch deal fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def _get_deal_custom_field_id_to_name() -> Dict[str, str]:
    """Return mapping of custom field ID (string) -> internal name (e.g. cfDealStatus)."""
    fields = await _fetch_deal_fields()
    custom = [f for f in fields if not f.get("standard", False)]
    return {str(f["id"]): (f.get("name") or str(f["id"])) for f in custom if f.get("id") is not None}


async def get_deal_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_deal_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    large_fields = {n.lower() for n in DEAL_LARGE_PICKLIST_FIELDS}
    internal_name_fields = DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME
    lines = [
        "=" * 60,
        "KYLAS CRM - DEAL FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists, internal_name_fields=internal_name_fields))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists, internal_name_fields=internal_name_fields))
    lines.extend(["", "=" * 60, "END OF CHEAT SHEET", "=" * 60])
    return "\n".join(lines)


@mcp.tool()
async def get_deal_field_instructions() -> str:
    """
    Get all deal fields for the current tenant. CALL THIS FIRST before creating or updating a deal.
    Returns a cheat sheet with API names (standard fields), Field IDs (custom fields), and Picklist Option IDs.
    Use this to build field_values for create_deal based on what the user wants—do not use static fields.
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching deal field instructions")
        result = await get_deal_field_instructions_logic()
        return result
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_deal_field_instructions")
        return f"Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Deal search rule builder (with deal-specific picklist fields)
# ---------------------------------------------------------------------------

def _build_deal_search_json_rule(
    filters: List[Dict[str, Any]],
    filterable_map: Dict[str, Dict[str, Any]],
    default_timezone: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Build jsonRule for POST /search/deal. Returns (jsonRule, error_message).
    Same as lead version but uses DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME.
    """
    tz_for_date = default_timezone or DEFAULT_TIMEZONE
    rules = []
    for i, f in enumerate(filters):
        field_name = f.get("field")
        operator = (f.get("operator") or "equal").strip().lower().replace(" ", "_")
        operator = OPERATOR_SYMBOL_MAP.get(operator, operator)
        value = f.get("value")
        field_type_key = (f.get("type") or "TEXT_FIELD").strip().upper().replace(" ", "_")

        if not field_name:
            return {}, f"Filter #{i + 1}: missing 'field'."
        if field_name not in filterable_map:
            return {}, f"Filter #{i + 1}: field '{field_name}' is not filterable or not found. Use only a field listed as [FILTERABLE] in build_payload(\"deal.search\")'s tenant_filterable_fields."
        meta = filterable_map[field_name]
        api_type = meta.get("type", "TEXT_FIELD")
        allowed = OPERATOR_MAPPING.get(api_type) or OPERATOR_MAPPING.get("TEXT_FIELD", [])
        if operator not in allowed:
            return {}, f"Filter #{i + 1}: operator '{operator}' not allowed for field '{field_name}' (type {api_type}). Allowed: {', '.join(allowed)}."

        # Deal-specific picklist handling: use DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME instead of PICKLIST_FIELDS_USE_INTERNAL_NAME
        if api_type in ("PICK_LIST", "MULTI_PICKLIST"):
            rule_type = "string" if field_name in DEAL_PICKLIST_FIELDS_USE_INTERNAL_NAME else "long"
        else:
            rule_type = _rule_type_for_value(api_type, field_name, value)
        if rule_type in ("long", "double") and value is not None and not isinstance(value, (int, float)):
            try:
                value = float(value) if rule_type == "double" else int(value)
            except (TypeError, ValueError):
                value = value
        # Date/datetime fields: convert date values from user's timezone to UTC
        # e.g. "11th Aug 00:00" in Asia/Calcutta → "10th Aug 18:30" UTC
        if rule_type == "date" and value is not None:
            filter_tz = f.get("timeZone") or tz_for_date
            value = _convert_date_value_to_utc(value, filter_tz)

        is_custom = not meta.get("standard", True)
        rule_field = f"customFieldValues.{field_name}" if is_custom else field_name

        rule = {
            "operator": operator,
            "id": field_name,
            "field": rule_field,
            "type": rule_type,
            "value": value,
            "relatedFieldIds": None,
        }
        if field_name == "pipeline":
            rule["dependentFieldIds"] = ["pipelineStage", "pipelineStageReason"]
        elif field_name == "pipelineStage":
            rule["relatedFieldIds"] = ["pipeline"]
        if rule_type == "date":
            rule["timeZone"] = f.get("timeZone") or tz_for_date
        rules.append(rule)

    return {"rules": rules, "condition": "AND", "valid": True}, None


# ---------------------------------------------------------------------------
# Deal create/update/get logic
# ---------------------------------------------------------------------------

def _normalize_deal_payload(payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Apply deal-specific field normalization on top of generic _normalize_field_values output.
    - ownedBy: int → {"id": int}
    - estimatedValue / actualValue / value: number → {"currencyId": <id>, "value": number}
      (currencyId taken from existing deal if available)
    """
    _MONETARY_FIELDS = ("estimatedValue", "actualValue", "value")

    if "ownedBy" in payload and isinstance(payload["ownedBy"], (int, float)) and not isinstance(payload["ownedBy"], bool):
        payload["ownedBy"] = {"id": int(payload["ownedBy"])}

    for field in _MONETARY_FIELDS:
        if field not in payload:
            continue
        v = payload[field]
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            # Try to get currencyId from existing deal object for this field
            currency_id = None
            if existing and isinstance(existing.get(field), dict):
                currency_id = existing[field].get("currencyId")
            if currency_id:
                payload[field] = {"currencyId": currency_id, "value": v}
            else:
                # Can't wrap without a currencyId — leave as-is; system instructions tell Claude to pass full object
                logger.warning("Deal field '%s' is a plain number but no currencyId available; sending as-is.", field)
    return payload


async def create_deal_logic(field_values: Dict[str, Any]) -> Dict[str, Any]:
    """Create a deal with the given dynamic field_values (Kylas API payload shape).
    If the target pipeline has sequentialStageFlow=true and the target stage is not the first stage,
    the deal is automatically created in the first stage then advanced sequentially to the target stage.
    """
    fv = dict(field_values)
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_deal_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    payload = _normalize_deal_payload(payload)
    if not payload:
        raise KylasAPIError("field_values cannot be empty")

    # Extract pipeline/stage info for potential stage-lock fallback
    target_stage_id: Optional[int] = None
    pipeline_id_for_lock: Optional[int] = None
    if isinstance(payload.get("pipeline"), dict):
        pipeline_id_for_lock = payload["pipeline"].get("id")
        stage = payload["pipeline"].get("stage")
        if isinstance(stage, dict):
            target_stage_id = stage.get("id")

    logger.info("Creating deal with fields: %s", list(payload.keys()))
    async with get_client() as client:
        try:
            response = await client.post("/deals", json=payload)
            result = await handle_api_response(response, "Create deal")
            logger.info("Deal created with ID: %s", result.get("id"))
            return result
        except KylasAPIError as e:
            if not _is_stage_lock_error(e) or target_stage_id is None or pipeline_id_for_lock is None:
                raise

    # Stage lock hit — create in first stage then advance sequentially
    logger.info(
        "Stage lock on pipeline %s; creating deal in first stage then advancing to stage %s",
        pipeline_id_for_lock, target_stage_id,
    )
    pipeline_data = await _get_pipeline_details_raw(pipeline_id_for_lock)
    stages = sorted(pipeline_data.get("stages", []), key=lambda s: s.get("position", 0))
    if not stages:
        raise KylasAPIError("Pipeline has no stages; cannot create deal.")

    first_stage = stages[0]
    first_stage_id = first_stage["id"]

    if first_stage_id == target_stage_id:
        # Already targeting stage 1 but got lock error — unexpected, re-raise
        raise KylasAPIError(
            "Sequential stage lock error even when targeting the first stage. "
            "Check pipeline configuration."
        )

    # Build payload with first stage
    first_payload = dict(payload)
    first_payload["pipeline"] = dict(first_payload["pipeline"])
    first_payload["pipeline"]["stage"] = {"id": first_stage_id}
    first_payload["forecastingType"] = first_stage.get("forecastingType", first_payload.get("forecastingType"))

    async with get_client() as client:
        response = await client.post("/deals", json=first_payload)
        deal = await handle_api_response(response, "Create deal (first stage)")

    deal_id = deal["id"]
    logger.info("Deal %s created in first stage %s; advancing to target stage %s", deal_id, first_stage_id, target_stage_id)

    return await _advance_deal_to_stage_sequentially(deal_id, stages, first_stage_id, target_stage_id, deal)


async def update_deal_logic(deal_id: int, field_values: Dict[str, Any]) -> Dict[str, Any]:
    """GET the deal first, merge field_values into it, then PUT the full body. No partial update."""
    deal_id = int(deal_id)
    fv = dict(field_values)
    if not fv:
        raise KylasAPIError("field_values cannot be empty for update.")
    has_custom_by_id = any(str(k).isdigit() for k in fv if k != "customFieldValues")
    id_to_name = await _get_deal_custom_field_id_to_name() if has_custom_by_id else {}
    payload = _normalize_field_values(fv, custom_field_id_to_name=id_to_name)
    if not payload:
        raise KylasAPIError("field_values produced an empty payload.")
    logger.info("Updating deal %s with fields: %s", deal_id, list(payload.keys()))
    async with get_client() as client:
        get_response = await client.get(f"/deals/{deal_id}")
        existing = await handle_api_response(get_response, "Get deal")
        payload = _normalize_deal_payload(payload, existing=existing)
        merged = dict(existing)
        for key, value in payload.items():
            if key == "customFieldValues" and isinstance(value, dict):
                merged["customFieldValues"] = {**(merged.get("customFieldValues") or {}), **value}
            elif key in ["contacts", "associatedContacts"] and isinstance(value, list):
                # Handle contact associations: merge with existing associatedContacts
                new_contacts = []
                for contact in value:
                    if isinstance(contact, dict):
                        # Contact object with id and possibly name
                        contact_id = contact.get("id")
                        contact_name = contact.get("name")
                        if contact_id:
                            # If no name provided, fetch it from the API
                            if not contact_name:
                                try:
                                    contact_details = await get_contact_logic(contact_id)
                                    contact_name = f"{contact_details.get('firstName', '')} {contact_details.get('lastName', '')}".strip()
                                except Exception as e:
                                    logger.warning(f"Could not fetch contact {contact_id} details: {e}")
                                    contact_name = f"Contact {contact_id}"
                            new_contacts.append({"id": contact_id, "name": contact_name})
                    elif isinstance(contact, (int, str)):
                        # Just an ID, fetch the contact
                        try:
                            contact_id = int(contact)
                            contact_details = await get_contact_logic(contact_id)
                            contact_name = f"{contact_details.get('firstName', '')} {contact_details.get('lastName', '')}".strip()
                            new_contacts.append({"id": contact_id, "name": contact_name})
                        except Exception as e:
                            logger.warning(f"Could not fetch contact {contact}: {e}")

                # Merge with existing associatedContacts (avoid duplicates by ID)
                existing_contacts = merged.get("associatedContacts", []) or []
                existing_ids = {c.get("id") for c in existing_contacts if isinstance(c, dict)}
                for contact in new_contacts:
                    if contact.get("id") not in existing_ids:
                        existing_contacts.append(contact)
                merged["associatedContacts"] = existing_contacts
            elif key == "products" and isinstance(value, list):
                # Handle products: merge with existing products on the deal
                new_products = []
                for product in value:
                    if isinstance(product, dict) and product.get("id"):
                        # Build product object with defaults for missing fields
                        prod = {
                            "id": product["id"],
                            "name": product.get("name", ""),
                            "quantity": product.get("quantity", 1),
                            "discount": product.get("discount", {"value": 0, "type": "PERCENTAGE"}),
                            "price": product.get("price", {}),
                            "units": product.get("units"),
                            "category": product.get("category"),
                            "hsnSacCode": product.get("hsnSacCode"),
                            "countryOfOrigin": product.get("countryOfOrigin"),
                            "customFieldValues": product.get("customFieldValues", {}),
                        }
                        new_products.append(prod)
                    elif isinstance(product, (int, str)):
                        # Just a product ID — add with quantity 1
                        try:
                            prod_id = int(product)
                            new_products.append({
                                "id": prod_id,
                                "name": "",
                                "quantity": 1,
                                "discount": {"value": 0, "type": "PERCENTAGE"},
                                "price": {},
                                "units": None,
                                "category": None,
                                "hsnSacCode": None,
                                "countryOfOrigin": None,
                                "customFieldValues": {},
                            })
                        except (TypeError, ValueError):
                            logger.warning(f"Invalid product ID: {product}")

                # Merge with existing products (avoid duplicates by ID)
                existing_products = merged.get("products", []) or []
                existing_product_ids = {p.get("id") for p in existing_products if isinstance(p, dict)}
                for prod in new_products:
                    if prod.get("id") not in existing_product_ids:
                        existing_products.append(prod)
                merged["products"] = existing_products
            elif key == "pipelineStage" and isinstance(value, (int, str)):
                # Special handling: pipelineStage should update the existing pipeline's stage ID
                # Also update forecastingType to match the stage's forecastingType
                stage_id = int(value)
                if isinstance(merged.get("pipeline"), dict):
                    pipeline_id = merged["pipeline"].get("id")
                    if not isinstance(merged["pipeline"].get("stage"), dict):
                        merged["pipeline"]["stage"] = {}
                    merged["pipeline"]["stage"]["id"] = stage_id

                    # Fetch pipeline details to get the correct forecastingType for this stage
                    try:
                        if pipeline_id:
                            pipeline_data = await _get_pipeline_details_raw(pipeline_id)
                            for stage in pipeline_data.get("stages", []):
                                if stage.get("id") == stage_id:
                                    forecast_type = stage.get("forecastingType")
                                    if forecast_type:
                                        merged["forecastingType"] = forecast_type
                                    break
                    except Exception as e:
                        logger.warning("Could not fetch forecastingType for stage %s: %s", stage_id, e)
                else:
                    logger.warning("Trying to set pipelineStage but deal has no pipeline object")
                    raise KylasAPIError("Deal has no pipeline; cannot set stage.")
            else:
                merged[key] = value

        # Capture stage-move info before PUT for sequential fallback
        update_target_stage_id: Optional[int] = None
        current_stage_id_for_lock: Optional[int] = None
        pipeline_id_for_lock: Optional[int] = None
        if "pipelineStage" in payload:
            update_target_stage_id = int(payload["pipelineStage"])
            if isinstance(existing.get("pipeline"), dict):
                pipeline_id_for_lock = existing["pipeline"].get("id")
                existing_stage = existing["pipeline"].get("stage")
                if isinstance(existing_stage, dict):
                    current_stage_id_for_lock = existing_stage.get("id")

        try:
            response = await client.put(f"/deals/{deal_id}", json=merged)
            result = await handle_api_response(response, "Update deal")
            logger.info("Deal %s updated", deal_id)
            return result
        except KylasAPIError as e:
            if (
                not _is_stage_lock_error(e)
                or update_target_stage_id is None
                or current_stage_id_for_lock is None
                or pipeline_id_for_lock is None
            ):
                raise

        # Stage lock hit during update — advance sequentially
        logger.info(
            "Stage lock on pipeline %s; advancing deal %s from stage %s to stage %s sequentially",
            pipeline_id_for_lock, deal_id, current_stage_id_for_lock, update_target_stage_id,
        )
        pipeline_data = await _get_pipeline_details_raw(pipeline_id_for_lock)
        stages = sorted(pipeline_data.get("stages", []), key=lambda s: s.get("position", 0))
        return await _advance_deal_to_stage_sequentially(
            deal_id, stages, current_stage_id_for_lock, update_target_stage_id, existing
        )


async def get_deal_logic(deal_id: int) -> Dict[str, Any]:
    """Fetch a single deal by ID (GET /deals/{id}). Returns full deal object."""
    deal_id = int(deal_id)
    async with get_client() as client:
        response = await client.get(f"/deals/{deal_id}")
        return await handle_api_response(response, "Get deal")


def _format_deal_for_display(deal: Dict[str, Any]) -> str:
    """Format a deal object into a readable multi-line string."""
    lines = ["=" * 60, "DEAL DETAILS", "=" * 60]
    lines.append(f"ID: {deal.get('id', '—')}")
    lines.append(f"Name: {deal.get('name', '—')}")
    lines.append(f"Value: {deal.get('value', '—')}")
    lines.append(f"Currency: {deal.get('currency', '—')}")
    lines.append(f"Closing Date: {_epoch_to_iso_utc(deal.get('closingDate', '—'))}")
    # Emails
    emails = deal.get("emails") or []
    if emails:
        for e in emails:
            val = e.get("value", "")
            typ = e.get("type", "")
            prim = " (primary)" if e.get("primary") else ""
            lines.append(f"Email ({typ}): {val}{prim}")
    else:
        lines.append("Email: —")
    # Phones
    phones = deal.get("phoneNumbers") or []
    if phones:
        for p in phones:
            code = p.get("code", "")
            val = p.get("value", "")
            typ = p.get("type", "")
            prim = " (primary)" if p.get("primary") else ""
            lines.append(f"Phone ({typ}): +{code} {val}{prim}")
    else:
        lines.append("Phone: —")
    # Pipeline / Stage
    pipeline = deal.get("pipeline") or {}
    if isinstance(pipeline, dict):
        pl_name = pipeline.get("name", "—")
        stage = pipeline.get("stage") or {}
        stage_name = stage.get("name", "—") if isinstance(stage, dict) else "—"
        lines.append(f"Pipeline: {pl_name}")
        lines.append(f"Stage: {stage_name}")
    else:
        lines.append(f"Pipeline: {pipeline}")
    lines.append(f"Owner ID: {deal.get('ownerId', '—')}")
    lines.append(f"Created At: {_epoch_to_iso_utc(deal.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(deal.get('updatedAt', '—'))}")
    # Products
    products = deal.get("products") or []
    lines.append("")
    if products:
        lines.append(f"Products ({len(products)}):")
        products_total = 0.0
        has_all_totals = True
        for prod in products:
            prod_name = prod.get("name") or prod.get("displayName") or f"ID:{prod.get('id', '?')}"
            qty = prod.get("quantity")
            price_obj = prod.get("price") or {}
            unit_price = price_obj.get("value")
            disc_obj = prod.get("discount") or {}
            disc_val = disc_obj.get("value")
            disc_type = disc_obj.get("type", "")
            # Build display parts
            qty_str = str(qty) if qty is not None else "?"
            price_str = str(unit_price) if unit_price is not None else "?"
            disc_str = f"{disc_val}{'%' if disc_type == 'PERCENTAGE' else ''}" if disc_val is not None else "—"
            # Compute line total
            line_total = None
            if qty is not None and unit_price is not None:
                try:
                    q, p = float(qty), float(unit_price)
                    if disc_val is not None:
                        d = float(disc_val)
                        if disc_type == "PERCENTAGE":
                            line_total = q * p * (1 - d / 100)
                        else:
                            line_total = q * p - d
                    else:
                        line_total = q * p
                    products_total += line_total
                except (TypeError, ValueError):
                    has_all_totals = False
            else:
                has_all_totals = False
            line_total_str = f"{line_total:,.2f}" if line_total is not None else "?"
            lines.append(f"  • {prod_name:<20} qty: {qty_str:<5} unit: {price_str:<10} discount: {disc_str:<10} line total: {line_total_str}")
        if has_all_totals and products:
            lines.append(f"  {'─' * 55}")
            lines.append(f"  Products Total: {products_total:,.2f}")
    else:
        lines.append("Products: —")
    # Custom fields
    custom = deal.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


@mcp.tool()
async def get_deal(deal_id: int) -> str:
    """
    Get full details of a deal by ID (GET /deals/{id}). Use when the user asks for complete deal info.
    deal_id: The deal ID (e.g. from search_deals or search_deals_by_term results).
    """
    try:
        _reset_api_call_count()
        deal = await get_deal_logic(deal_id)
        return _format_deal_for_display(deal)
    except KylasAPIError as e:
        return f"✗ Failed to get deal: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_deal")
        return f"✗ Unexpected error: {str(e)}"


# ---------------------------------------------------------------------------
# Deal search logic
# ---------------------------------------------------------------------------

def _extract_primary_deal_value(value: Any) -> str:
    """Extract deal value for display."""
    if value is None:
        return "-"
    return str(value)


async def search_deals_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """Search deals with jsonRule; only filterable fields allowed. Uses current user timezone for date/datetime filters."""
    fields_list = await _fetch_deal_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable deal fields found for this tenant."
    default_tz = None
    date_field_types = {"DATETIME_PICKER", "DATE", "DATE_PICKER"}
    for f in filters:
        fn = f.get("field")
        if fn and fn in filterable_map and filterable_map[fn].get("type") in date_field_types and not f.get("timeZone"):
            try:
                user = await _fetch_current_user()
                default_tz = user.get("timezone") or DEFAULT_TIMEZONE
            except Exception:
                default_tz = DEFAULT_TIMEZONE
            break
    json_rule, err = _build_deal_search_json_rule(filters, filterable_map, default_timezone=default_tz)
    if err:
        return f"Invalid filters: {err}"
    payload = {
        "fields": ["id", "name", "value", "currency", "closingDate", "ownedBy", "createdAt", "actualValue", "estimatedValue", "associatedContacts", "associatedLeads", "associatedCompanies", "products"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching deals with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/search/deal", params=params, json=payload)
        data = await handle_api_response(response, "Search deals")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No deals found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} deal(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for deal in results:
        did = deal.get("id", "?")
        name = deal.get("name", "—")
        value = _extract_primary_deal_value(deal.get("value"))
        actual_val = _extract_primary_deal_value(deal.get("actualValue"))
        estimated_val = _extract_primary_deal_value(deal.get("estimatedValue"))
        owner_obj = deal.get("ownedBy", {})
        owner_name = owner_obj.get("name", "—") if isinstance(owner_obj, dict) else "—"
        associated_contacts = deal.get("associatedContacts") or []
        associated_contacts_str = ", ".join(str(cid) for cid in associated_contacts) if associated_contacts else "—"
        products = deal.get("products") or []
        if products:
            prod_names = [p.get("name") or p.get("displayName") or f"ID:{p.get('id','?')}" for p in products]
            products_str = ", ".join(prod_names[:3]) + (" ..." if len(prod_names) > 3 else "")
        else:
            products_str = "—"
        lines.append(f"• ID: {did} | Name: {name} | Owner: {owner_name} | Value: {value} | Products: {products_str} | Contacts: {associated_contacts_str}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_deals(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Search/filter deals by specific field criteria. Use this to:
    - Get ALL deals: filters=[{"field":"id","operator":"is_not_null"}]
    - Get deals by field criteria (e.g., value > 50000, status = "Won")
    - Filter by any [FILTERABLE] field from get_deal_field_instructions

    DO NOT use this for keyword/text searches - use search_deals_by_term instead.
    Call get_deal_field_instructions first to get filterable fields and their types.

    **Returned fields include associated entities:**
    - associatedContacts (array of contact IDs)
    - associatedLeads (array of lead IDs)
    - associatedCompanies (array of company IDs)
    These are real IDs that can be used directly in subsequent searches without needing to look them up separately.

    filters: List of filter objects. Each must have:
      - field (str): Field internal/API name (e.g. name, value, dealSource, createdAt).
      - operator (str): One of the allowed operators for that field type (e.g. equal, contains, greater).
      - value: Value to compare. For PICK_LIST/MULTI_PICKLIST use Option ID (number), except
        currency, country, dealSource — use internal name (string).
      - timeZone (str, optional): For date/datetime filters only.
      - type (str, optional): Field type from cheat sheet.
    page: 0-based page (default 0).
    size: Page size, max 100 (default 20).
    sort: Sort e.g. "createdAt,desc" (default).
    """
    try:
        _reset_api_call_count()
        if not filters:
            return "Error: filters list cannot be empty. Provide at least one filter with field, operator, and value."
        return await search_deals_logic(filters, page, size, sort)
    except KylasAPIError as e:
        return f"✗ Search failed: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("search_deals")
        return f"✗ Unexpected error: {str(e)}"


async def search_deals_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search deals by a single term across multiple fields via POST /search/deal with multi_field jsonRule."""
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    json_rule = _multi_field_json_rule(term)
    payload = {
        "fields": ["id", "name", "value", "currency", "closingDate", "ownedBy", "createdAt", "actualValue", "estimatedValue"],
        "jsonRule": json_rule,
    }
    params = {"page": page, "size": min(size, 100)}
    if sort:
        params["sort"] = sort
    logger.info("Searching deals by term: %r", term)
    async with get_client() as client:
        response = await client.post("/search/deal", params=params, json=payload)
        data = await handle_api_response(response, "Search deals by term")
    results = data.get("content", data.get("data", []))
    total = data.get("totalElements", data.get("total", len(results)))
    total_pages = data.get("totalPages", 1)
    if not results:
        return f"No deals found matching '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} deal(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for deal in results:
        did = deal.get("id", "?")
        name = deal.get("name", "—")
        value = _extract_primary_deal_value(deal.get("value"))
        actual_val = _extract_primary_deal_value(deal.get("actualValue"))
        estimated_val = _extract_primary_deal_value(deal.get("estimatedValue"))
        owner_obj = deal.get("ownedBy", {})
        owner_name = owner_obj.get("name", "—") if isinstance(owner_obj, dict) else "—"
        lines.append(f"• ID: {did} | Name: {name} | Owner: {owner_name} | Value: {value} | Actual: {actual_val} | Estimated: {estimated_val}")
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_idle_deals_logic(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "createdAt,desc",
) -> str:
    """
    Find deals with no activity for at least `days` days.
    Uses last-activity = max(updatedAt, latestActivityCreatedAt).
    If time_zone is not provided, uses current user's timezone.
    """
    if time_zone:
        tz = time_zone
    else:
        try:
            user = await _fetch_current_user()
            tz = user.get("timezone") or DEFAULT_TIMEZONE
        except Exception:
            tz = DEFAULT_TIMEZONE
    threshold_iso = _threshold_iso_days_ago(days, tz)
    base = {"operator": "less_or_equal", "value": threshold_iso, "timeZone": tz}
    fields_list = await _fetch_deal_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    filters = []
    for name in ("updatedAt", "latestActivityCreatedAt"):
        if name in filterable_map:
            filters.append({"field": name, **base})
    if not filters:
        return "Error: Neither 'updatedAt' nor 'latestActivityCreatedAt' is filterable for this tenant. Check build_payload(\"deal.search\")'s tenant_filterable_fields."
    return await search_deals_logic(filters, page=page, size=size, sort=sort)
