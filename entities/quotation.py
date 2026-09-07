"""
entities/quotation.py — everything specific to the "quotation" bucket:
field instructions, display formatting, get, search (+ by-term, + idle).
Read-only (no create/update — matches the registry's documented shape).
Fully contiguous in main.py (no far-away by-term section, unlike lead/
contact/task/company/meeting).

QUOTATION_LARGE_PICKLIST_FIELDS / QUOTATION_PICKLIST_FIELDS_USE_INTERNAL_NAME
mirror entities/lead.py's fix for the same _BUCKET_PICKLIST_RULES circular-
import issue — see that file's docstring. Both are empty sets here (matches
main.py's _BUCKET_PICKLIST_RULES["quotation"] entry) — quotation has no
oversized picklists and no picklist fields that need the internal name.
"""

from typing import Any, Dict, List, Optional

from shared.app import mcp, logger
from shared.http_client import KylasAPIError, get_client, handle_api_response, _reset_api_call_count
from shared.meta import (
    DEFAULT_TIMEZONE,
    _threshold_iso_days_ago,
    _epoch_to_iso_utc,
    _fetch_current_user,
    _format_field,
    _get_filterable_fields_map,
    _build_search_json_rule,
    _multi_field_json_rule,
)

# This bucket has no oversized picklists and no picklist fields needing the
# internal name (matches main.py's _BUCKET_PICKLIST_RULES["quotation"]).
QUOTATION_LARGE_PICKLIST_FIELDS: set = set()
QUOTATION_PICKLIST_FIELDS_USE_INTERNAL_NAME: set = set()


# ---------------------------------------------------------------------------
# Quotation field instructions / get / search logic
# ---------------------------------------------------------------------------

async def _fetch_quotation_fields() -> List[Dict[str, Any]]:
    """Fetch quotation field metadata (GET /quotations/fields). Returns list of active field dicts."""
    async with get_client() as client:
        response = await client.get("/quotations/fields", params={"page": 0, "size": 100})
        data = await handle_api_response(response, "Fetch quotation fields")
        if isinstance(data, list):
            fields = data
        elif isinstance(data, dict):
            fields = data.get("data", data.get("content", []))
        else:
            fields = []
        return [f for f in fields if f.get("active", True)]


async def get_quotation_field_instructions_logic(
    fields_meta: Optional[List[Dict[str, Any]]] = None,
    requested_picklists: Optional[set] = None,
) -> str:
    fields = fields_meta if fields_meta is not None else await _fetch_quotation_fields()
    standard = [f for f in fields if f.get("standard", False)]
    custom = [f for f in fields if not f.get("standard", False)]
    large_fields = {n.lower() for n in QUOTATION_LARGE_PICKLIST_FIELDS}
    lines = [
        "=" * 60,
        "KYLAS CRM - QUOTATION FIELDS CHEAT SHEET",
        "=" * 60,
        "",
        "## STANDARD FIELDS",
        "-" * 40,
    ]
    for f in standard:
        lines.extend(_format_field(f, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists))
    if custom:
        lines.extend(["", "## CUSTOM FIELDS", "-" * 40])
        for f in custom:
            lines.extend(_format_field(f, include_filterable=True, large_fields=large_fields, requested_picklists=requested_picklists))
    lines.extend(["", "=" * 60, "END OF CHEAT SHEET", "=" * 60])
    return "\n".join(lines)


@mcp.tool()
async def get_quotation_field_instructions() -> str:
    """
    Get all quotation fields for the current tenant. CALL THIS FIRST before searching or filtering quotations.
    Returns a cheat sheet with API names (standard fields), Field IDs (custom fields), Picklist Option IDs,
    and which fields are [FILTERABLE] for search_entity("quotation", ...).
    """
    try:
        _reset_api_call_count()
        logger.info("Fetching quotation field instructions")
        return await get_quotation_field_instructions_logic()
    except KylasAPIError as e:
        return f"Error: {e.message}"
    except Exception as e:
        logger.exception("get_quotation_field_instructions")
        return f"Unexpected error: {str(e)}"


def _format_quotation_for_display(q: Dict[str, Any]) -> str:
    """Format a quotation object into a readable multi-line string."""
    def _name_of(obj: Any) -> str:
        if isinstance(obj, dict):
            return obj.get("name") or (str(obj.get("id")) if obj.get("id") is not None else "—")
        return str(obj) if obj is not None else "—"

    lines = ["=" * 60, "QUOTATION DETAILS", "=" * 60]
    lines.append(f"ID: {q.get('id', '—')}")
    lines.append(f"Quotation Number: {q.get('quotationNumber', '—')}")
    lines.append(f"Summary: {q.get('summary', '—')}")
    status = q.get("status")
    lines.append(f"Status: {_name_of(status)}")

    def _money(v: Any) -> Any:
        return v.get("value") if isinstance(v, dict) else (v if v is not None else "—")

    lines.append(f"Sub Total: {_money(q.get('subTotal'))}")
    lines.append(f"Grand Total: {_money(q.get('grandTotal'))}")
    lines.append(f"Valid Till: {_epoch_to_iso_utc(q.get('validTill', '—'))}")
    lines.append(f"Owner: {_name_of(q.get('owner'))}")
    lines.append(f"Associated Deal: {_name_of(q.get('associatedDeal'))}")
    lines.append(f"Associated Company: {_name_of(q.get('associatedCompany'))}")
    contacts = q.get("associatedContacts") or []
    if contacts:
        lines.append("Associated Contacts: " + ", ".join(_name_of(c) for c in contacts))
    lines.append(f"Created At: {_epoch_to_iso_utc(q.get('createdAt', '—'))}")
    lines.append(f"Updated At: {_epoch_to_iso_utc(q.get('updatedAt', '—'))}")
    # Products
    products = q.get("products") or []
    lines.append("")
    if products:
        lines.append(f"Products ({len(products)}):")
        for prod in products:
            prod_name = prod.get("name") or f"ID:{prod.get('id', '?')}"
            qty = prod.get("quantity", "?")
            price_obj = prod.get("price") or {}
            unit_price = price_obj.get("value") if isinstance(price_obj, dict) else price_obj
            total = prod.get("totalAmount")
            total_val = total.get("value") if isinstance(total, dict) else total
            lines.append(f"  • {prod_name:<24} qty: {qty}  unit: {unit_price}  total: {total_val}")
    else:
        lines.append("Products: —")
    # Custom fields
    custom = q.get("customFieldValues") or {}
    if custom:
        lines.append("")
        lines.append("Custom fields:")
        for k, v in custom.items():
            lines.append(f"  {k}: {v}")
    lines.append("=" * 60)
    return "\n".join(lines)


async def get_quotation_logic(quotation_id: int) -> Dict[str, Any]:
    """Fetch a single quotation by ID (GET /quotations/{id})."""
    quotation_id = int(quotation_id)
    async with get_client() as client:
        response = await client.get(f"/quotations/{quotation_id}")
        return await handle_api_response(response, "Get quotation")


@mcp.tool()
async def get_quotation(quotation_id: int) -> str:
    """
    Get full details of a quotation by ID (GET /quotations/{id}). Use when the user asks for complete
    quotation info (products, totals, associated deal/company/contacts).
    quotation_id: The quotation ID (e.g. from search_entity("quotation", ...) results).
    """
    try:
        _reset_api_call_count()
        q = await get_quotation_logic(quotation_id)
        return _format_quotation_for_display(q)
    except KylasAPIError as e:
        return f"✗ Failed to get quotation: {e.message}\n  Details: {e.response_body}"
    except Exception as e:
        logger.exception("get_quotation")
        return f"✗ Unexpected error: {str(e)}"


async def search_quotations_logic(
    filters: List[Dict[str, Any]],
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """Search quotations with jsonRule (POST /quotations/search); only filterable fields allowed."""
    fields_list = await _fetch_quotation_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    if not filterable_map:
        return "No filterable quotation fields found for this tenant."
    sortable = {f.get("name") for f in fields_list if f.get("sortable") and f.get("name")}

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

    json_rule, err = _build_search_json_rule(
        filters, filterable_map, default_timezone=default_tz,
        internal_name_fields=QUOTATION_PICKLIST_FIELDS_USE_INTERNAL_NAME,
    )
    if err:
        return f"Invalid filters: {err}"
    payload = {"jsonRule": json_rule}
    params: Dict[str, Any] = {"page": page, "size": min(size, 100)}
    # Only pass sort if the field is sortable; otherwise let the server apply its default.
    if sort:
        sort_by = sort.split(",")[0].strip()
        if sort_by in sortable:
            params["sort"] = sort
    logger.info("Searching quotations with %d filter(s)", len(filters))
    async with get_client() as client:
        response = await client.post("/quotations/search", params=params, json=payload)
        data = await handle_api_response(response, "Search quotations")
    results = data.get("content", data.get("data", [])) if isinstance(data, dict) else []
    total = data.get("totalElements", data.get("total", len(results))) if isinstance(data, dict) else len(results)
    total_pages = data.get("totalPages", 1) if isinstance(data, dict) else 1
    if not results:
        return f"No quotations found matching the filters. (Total in DB: {total})"
    lines = [f"Found {len(results)} quotation(s) (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for q in results:
        qid = q.get("id", "?")
        number = q.get("quotationNumber", "—")
        summary = q.get("summary", "—")
        status = q.get("status")
        status_str = status.get("name", "—") if isinstance(status, dict) else (status or "—")
        gt = q.get("grandTotal")
        grand_total = gt.get("value") if isinstance(gt, dict) else (gt if gt is not None else "—")
        deal = q.get("associatedDeal")
        deal_str = deal.get("name", deal.get("id", "—")) if isinstance(deal, dict) else "—"
        lines.append(
            f"• ID: {qid} | No: {number} | Summary: {summary} | Status: {status_str} | Grand Total: {grand_total} | Deal: {deal_str}"
        )
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_quotations_by_term_logic(
    search_term: str,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Search quotations by a free-text term across summary, quotation number, and the associated
    deal / company / contact / product names (POST /quotations/search with a multi_field jsonRule).
    """
    term = (search_term or "").strip()
    if not term:
        return "Error: search_term cannot be empty."
    # Determine sortable fields so we only pass a valid sort (server rejects non-sortable sorts).
    fields_list = await _fetch_quotation_fields()
    sortable = {f.get("name") for f in fields_list if f.get("sortable") and f.get("name")}
    payload = {"jsonRule": _multi_field_json_rule(term)}
    params: Dict[str, Any] = {"page": page, "size": min(size, 100)}
    if sort and sort.split(",")[0].strip() in sortable:
        params["sort"] = sort
    logger.info("Searching quotations by term: %r", term)
    async with get_client() as client:
        response = await client.post("/quotations/search", params=params, json=payload)
        data = await handle_api_response(response, "Search quotations by term")
    results = data.get("content", data.get("data", [])) if isinstance(data, dict) else []
    total = data.get("totalElements", data.get("total", len(results))) if isinstance(data, dict) else len(results)
    total_pages = data.get("totalPages", 1) if isinstance(data, dict) else 1
    if not results:
        return f"No quotations found for '{term}'. (Total in DB: {total})"
    lines = [f"Found {len(results)} quotation(s) for '{term}' (page {page + 1} of {total_pages}, total {total})", "-" * 60]
    for q in results:
        qid = q.get("id", "?")
        number = q.get("quotationNumber", "—")
        summary = q.get("summary", "—")
        status = q.get("status")
        status_str = status.get("name", "—") if isinstance(status, dict) else (status or "—")
        gt = q.get("grandTotal")
        grand_total = gt.get("value") if isinstance(gt, dict) else (gt if gt is not None else "—")
        deal = q.get("associatedDeal")
        deal_str = deal.get("name", deal.get("id", "—")) if isinstance(deal, dict) else "—"
        lines.append(
            f"• ID: {qid} | No: {number} | Summary: {summary} | Status: {status_str} | Grand Total: {grand_total} | Deal: {deal_str}"
        )
    lines.append("-" * 60)
    return "\n".join(lines)


async def search_idle_quotations_logic(
    days: int,
    time_zone: Optional[str] = None,
    page: int = 0,
    size: int = 20,
    sort: Optional[str] = "updatedAt,desc",
) -> str:
    """
    Find quotations with no update for at least `days` days (updatedAt on or before now - days).
    Quotations have no activity feed, so idleness is based on updatedAt only.
    If time_zone is not provided, uses the current user's timezone.
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
    fields_list = await _fetch_quotation_fields()
    filterable_map = _get_filterable_fields_map(fields_list)
    base = {"operator": "less_or_equal", "value": threshold_iso, "timeZone": tz}
    filters = []
    for name in ("updatedAt", "latestActivityCreatedAt"):
        if name in filterable_map:
            filters.append({"field": name, **base})
    if not filters:
        return "Error: 'updatedAt' is not filterable for quotations in this tenant. Check build_payload(\"quotation.search\")'s tenant_filterable_fields."
    return await search_quotations_logic(filters, page=page, size=size, sort=sort)
