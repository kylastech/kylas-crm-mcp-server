"""Turn what a caller writes into what Kylas accepts, before a create/update.

A model (or a person) writes "source: Website", "owner: Ajit Dada" or
"pipeline: Sales, stage: Negotiation"; Kylas wants option ids, user ids and
pipeline/stage ids, each in the shape its own service expects. This module
resolves those references with what the server can look up, so the caller does
not need a lookup round trip per field.

Shapes, per the core services (sd-sales, sd-company, sd-deal, sd-meetings,
sd-call):

    entity     picklist value   owner field          pipeline / products
    lead       plain id         ownerId: n           {id, stage: {id}} / [{id}]
    contact    plain id         ownerId: n           -
    company    {id}             ownedBy: {id}        -
    deal       {id}             ownedBy: {id}        {id, stage: {id}} / [{id, ...}]
    meeting    {id}             owner: {id}          -
    call_log   {id}             ownerId: n           -
    task       plain id         (unchanged)          -

Lead/contact companyIndustry, companyBusinessType, country, timezone,
companyCountry and requirementCurrency take the option's internal NAME, as
does a custom picklist flagged `internal`.

A name that matches nothing, or several, is a ValueError listing what it could
be — the caller fixes it in one retry rather than guessing.
"""

import hashlib
import json
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from app.client import _resolve_auth_headers, get_client, handle_api_response
from app.helpers import PICKLIST_FIELDS_USE_INTERNAL_NAME, resolve_picklist_values

FieldsFetcher = Callable[[], Awaitable[List[Dict[str, Any]]]]

_RULES: Dict[str, Dict[str, Any]] = {
    "lead":     {"wrap": False, "name_fields": PICKLIST_FIELDS_USE_INTERNAL_NAME,
                 "owner": ("ownerId", False), "pipeline": "LEAD", "products": True},
    "contact":  {"wrap": False, "name_fields": PICKLIST_FIELDS_USE_INTERNAL_NAME,
                 "owner": ("ownerId", False)},
    "company":  {"wrap": True, "owner": ("ownedBy", True)},
    "deal":     {"wrap": True, "owner": ("ownedBy", True), "pipeline": "DEAL", "products": True},
    "meeting":  {"wrap": True, "owner": ("owner", True)},
    "call_log": {"wrap": True, "owner": ("ownerId", False)},
    "task":     {"wrap": False},
}

# An entity's field metadata changes on an admin's edit, not per call: held
# briefly, per caller (their key decides what they may see).
FIELDS_TTL_SECONDS = 300
_FIELDS: Dict[Tuple[str, str], Tuple[float, List[Dict[str, Any]]]] = {}


def _caller() -> str:
    """A digest of this request's credential — never the credential."""
    try:
        headers = _resolve_auth_headers()
    except Exception:  # noqa: BLE001 - the API call itself reports missing auth
        return ""
    return hashlib.sha256(json.dumps(sorted(headers.items())).encode()).hexdigest()[:16]


async def _fields(entity: str, fetch: FieldsFetcher) -> List[Dict[str, Any]]:
    key = (entity, _caller())
    cached = _FIELDS.get(key)
    if cached and time.time() - cached[0] < FIELDS_TTL_SECONDS:
        return cached[1]
    fields = await fetch()
    if len(_FIELDS) >= 256 and key not in _FIELDS:
        _FIELDS.pop(min(_FIELDS, key=lambda k: _FIELDS[k][0]), None)
    _FIELDS[key] = (time.time(), fields)
    return fields


def forget_fields() -> None:
    """Drop cached field metadata (tests; or after a field is added)."""
    _FIELDS.clear()


# --- looking things up by name -------------------------------------------------

def _norm(text: Any) -> str:
    return " ".join(str(text or "").split()).lower()


def _is_id(value: Any) -> bool:
    return (isinstance(value, int) and not isinstance(value, bool)) or (
        isinstance(value, str) and value.strip().isdigit())


def _pick(kind: str, wanted: str, rows: List[Dict[str, Any]], field: str) -> int:
    """The one row named `wanted`: an exact (case-insensitive) match wins,
    else the only row the search returned. Otherwise a ValueError naming
    the candidates."""
    def name(row: Dict[str, Any]) -> str:
        return _norm(row.get("name") or row.get("displayName"))

    exact = [r for r in rows if name(r) == _norm(wanted)]
    words = _norm(wanted).split()
    # "Shubham Katte" against a one-word search: the rows having every word.
    every = [r for r in rows if all(w in name(r).split() for w in words)] if len(words) > 1 else []
    chosen = exact or every or rows
    if len(chosen) == 1 and chosen[0].get("id") is not None:
        return int(chosen[0]["id"])
    if not chosen:
        raise ValueError(f"No {kind} named '{wanted}' for {field}. Check the name, or send its id.")
    names = ", ".join(f"{r.get('name') or r.get('displayName')}={r.get('id')}" for r in chosen[:15])
    raise ValueError(f"'{wanted}' matches several {kind}s for {field}: {names}. "
                     f"Ask which one is meant and send its id.")


async def _lookup(path: str, wanted: str, extra: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    async with get_client() as client:
        response = await client.get(path, params={"q": f"name:{wanted}", "page": 0, "size": 20,
                                                   **(extra or {})})
        data = await handle_api_response(response, f"Lookup {path}")
    return data.get("content", data.get("data", [])) if isinstance(data, dict) else (data or [])


async def _user_id(wanted: str, field: str) -> int:
    # Kylas's user lookup matches one word ("name:Shubham Katte" finds nobody),
    # so search by the first and pick by the whole name.
    first = wanted.split()[0]
    return _pick("user", wanted, await _lookup("/v1/users/lookup", first), field)


async def _product_id(wanted: str) -> int:
    return _pick("product", wanted, await _lookup("/v1/products/lookup", wanted), "products")


async def _pipeline_id(wanted: str, entity_type: str) -> int:
    rows = await _lookup("/v1/pipelines/lookup", wanted, {"entityType": entity_type})
    return _pick("pipeline", wanted, rows, "pipeline")


async def _stage_id(pipeline_id: int, wanted: str) -> int:
    async with get_client() as client:
        response = await client.get(f"/v1/pipelines/{int(pipeline_id)}")
        pipeline = await handle_api_response(response, "Get pipeline details")
    stages = pipeline.get("stages") or []
    exact = [s for s in stages if _norm(s.get("name")) == _norm(wanted)]
    if len(exact) == 1:
        return int(exact[0]["id"])
    names = ", ".join(f"{s.get('name')}={s.get('id')}" for s in stages)
    raise ValueError(f"Pipeline {pipeline_id} has no stage named '{wanted}'. Its stages: {names}.")


def _name_of(value: Any) -> Optional[str]:
    """The name a reference was given by, when it has no id."""
    if isinstance(value, str) and value.strip() and not _is_id(value):
        return value.strip()
    if isinstance(value, dict) and value.get("id") in (None, ""):
        name = value.get("name") or value.get("displayName")
        return str(name).strip() if name else None
    return None


def _id_of(value: Any) -> Optional[int]:
    if _is_id(value):
        return int(value)
    if isinstance(value, dict) and _is_id(value.get("id")):
        return int(value["id"])
    return None


# --- each reference -------------------------------------------------------------

async def _owner(payload: Dict[str, Any], field: str, wrap: bool) -> None:
    if field not in payload or payload[field] in (None, ""):
        return
    value = payload[field]
    owner_id = _id_of(value)
    if owner_id is None:
        name = _name_of(value)
        if not name:
            return
        owner_id = await _user_id(name, field)
    payload[field] = {"id": owner_id} if wrap else owner_id


async def _pipeline(payload: Dict[str, Any], entity_type: str) -> None:
    value = payload.get("pipeline")
    if value in (None, "", {}):
        return
    if not isinstance(value, dict):
        value = {"id": value} if _is_id(value) else {"name": value}
    pipeline_id = _id_of(value)
    if pipeline_id is None:
        name = _name_of(value)
        if not name:
            return
        pipeline_id = await _pipeline_id(name, entity_type)
    resolved: Dict[str, Any] = {"id": pipeline_id}
    stage = value.get("stage")
    if stage not in (None, "", {}):
        stage_id = _id_of(stage)
        if stage_id is None:
            name = _name_of(stage) or (str(stage).strip() if isinstance(stage, str) else None)
            if name:
                stage_id = await _stage_id(pipeline_id, name)
        if stage_id is not None:
            resolved["stage"] = {"id": stage_id}
    payload["pipeline"] = resolved


async def _products(payload: Dict[str, Any]) -> None:
    products = payload.get("products")
    if not isinstance(products, list):
        return
    resolved = []
    for item in products:
        entry = dict(item) if isinstance(item, dict) else {"id": item} if _is_id(item) else {"name": item}
        if _id_of(entry) is None:
            name = _name_of(entry)
            if name:
                entry["id"] = await _product_id(name)
        elif _is_id(entry.get("id")):
            entry["id"] = int(entry["id"])
        resolved.append(entry)
    payload["products"] = resolved


async def resolve_references(entity: str, payload: Dict[str, Any],
                             fetch_fields: Optional[FieldsFetcher] = None) -> Dict[str, Any]:
    """`payload` with picklists, owner, pipeline/stage and products in the ids
    and shapes Kylas accepts for `entity`. Ids already given are kept."""
    rules = _RULES.get(entity, {})
    resolved = dict(payload)
    if fetch_fields is not None:
        resolved = resolve_picklist_values(
            resolved, await _fields(entity, fetch_fields),
            wrap=rules.get("wrap", False), name_fields=set(rules.get("name_fields") or ()))
    if rules.get("owner"):
        await _owner(resolved, *rules["owner"])
    if rules.get("pipeline"):
        await _pipeline(resolved, rules["pipeline"])
    if rules.get("products"):
        await _products(resolved)
    return resolved
