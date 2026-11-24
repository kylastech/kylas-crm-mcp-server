"""Resource links: MCP `resource_link` blocks saying what an execute_request call did.

Added after the call's text block, one per record created, updated, cancelled
or deleted, and one per search. Example:

    {"type": "resource_link", "uri": "kylas://lead/9", "name": "Asha Rao",
     "_meta": {"com.kylas/action": "CreateAction", "com.kylas/entity": "lead",
               "com.kylas/id": 9}}

A search link names the entity (`kylas://lead`) and adds `com.kylas/count`
and `com.kylas/query` (the rule Kylas ran). Same contract as sd-ai-agent's
app/core/resource_links.py.
"""

import json
import os
import re
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple

from mcp.types import ResourceLink

from app.client import _SEARCHES_SENT

SCHEME = "kylas"

CREATE = "CreateAction"
UPDATE = "UpdateAction"
DELETE = "DeleteAction"
SEARCH = "SearchAction"

ACTION_KEY = "com.kylas/action"
ENTITY_KEY = "com.kylas/entity"
ID_KEY = "com.kylas/id"
QUERY_KEY = "com.kylas/query"
COUNT_KEY = "com.kylas/count"
ABOUT_KEY = "com.kylas/about"

MAX_NAME = 200
# A query larger than this (a rule over a thousand ids) is left out of the
# link — its action and count stay; a client then shows a count, not a list.
MAX_QUERY_BYTES = 4096

# The one endpoint each entity's search posts its jsonRule to. The rule a
# search link carries is the last one sent THERE — never a lookup's
# (/v1/search/lead/lookup) or another entity's.
SEARCH_ENDPOINTS: Dict[str, str] = {
    "lead": "/v1/search/lead",
    "contact": "/v1/search/contact",
    "deal": "/v1/search/deal",
    "company": "/v1/search/company",
    "task": "/v1/tasks/search",
    "meeting": "/v1/meetings/search",
    "call_log": "/v1/call-logs/search",
    "quotation": "/v1/quotations/search",
}

_SEARCH_INTENTS = ("search", "search_by_term", "search_idle")


def uri_for(entity: str, record_id: Any = None) -> str:
    """Return `kylas://<entity>/<id>`, or `kylas://<entity>` when there is no id."""
    entity = str(entity).strip().lower()
    return f"{SCHEME}://{entity}/{record_id}" if record_id is not None else f"{SCHEME}://{entity}"


def _as_id(value: Any) -> Optional[int]:
    """Return a positive record id as an int (digit strings included), else None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip()) or None
    return None


def link(action: str, entity: str, record_id: Any = None, name: Optional[str] = None, *,
         query: Any = None, count: Optional[int] = None, about: Optional[str] = None) -> ResourceLink:
    """Build one ResourceLink for an action on a record or, for a search, on an entity.

    `query`, `count` and `about` are added to `_meta` only when given; `name`
    defaults to "lead 9" or "12 lead result(s)".
    """
    entity = str(entity).strip().lower()
    record_id = _as_id(record_id)
    meta: Dict[str, Any] = {ACTION_KEY: action, ENTITY_KEY: entity}
    if record_id is not None:
        meta[ID_KEY] = record_id
    if query is not None:
        meta[QUERY_KEY] = query
    if count is not None:
        meta[COUNT_KEY] = count
    if about:
        meta[ABOUT_KEY] = about
    if not name:
        noun = entity.replace("_", " ")
        name = (f"{count} {noun} result(s)" if count is not None else f"{noun} search") \
            if action == SEARCH else (f"{noun} {record_id}" if record_id is not None else noun)
    return ResourceLink(type="resource_link", uri=uri_for(entity, record_id),
                        name=str(name)[:MAX_NAME], _meta=meta)


def record_name(record: Any) -> Optional[str]:
    """Return a record's name, title or "first last" name, else None."""
    if not isinstance(record, dict):
        return None
    for key in ("name", "title"):
        if isinstance(record.get(key), str) and record[key].strip():
            return record[key].strip()
    full = " ".join(p for p in (record.get("firstName"), record.get("lastName"))
                    if isinstance(p, str) and p.strip())
    return full or None


# --- reading a finished call ------------------------------------------------------

# "Found 12 lead(s) (page 1 of 1, total 12)" / "Found 3 lead(s) matching 'x':" /
# "No leads found matching the filters. (Total in DB: 900)". The search
# routers answer in text (see execute_request), so the count is read from it.
_TOTAL = re.compile(r"\btotal (\d+)\)")
_FOUND = re.compile(r"^Found (\d+)\b")


def search_count(text: Any) -> Optional[int]:
    """Return the match count from a search's answer text ("... total 12)"), else None."""
    if not isinstance(text, str):
        return None
    head = text.strip().splitlines()[0] if text.strip() else ""
    if head.startswith("No "):
        return 0
    total = _TOTAL.search(head)
    if total:
        return int(total.group(1))
    found = _FOUND.search(head)
    return int(found.group(1)) if found else None


def links_enabled() -> bool:
    """Return False when MCP_RESOURCE_LINKS_ENABLED is set to false/0/no/off."""
    return os.getenv("MCP_RESOURCE_LINKS_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")


def client_accepts_links() -> bool:
    """Return whether this call's answer should carry resource links."""
    return links_enabled()


@contextmanager
def records_searches() -> Iterator[List[Tuple[str, Dict[str, Any]]]]:
    """Collect the (path, jsonRule) of every search sent to Kylas inside the block.

    Scoped per call with a context variable, so concurrent calls never mix.
    """
    sent: List[Tuple[str, Dict[str, Any]]] = []
    token = _SEARCHES_SENT.set(sent)
    try:
        yield sent
    finally:
        _SEARCHES_SENT.reset(token)


def executed_query(rule: Any) -> Optional[Dict[str, Any]]:
    """Turn the jsonRule Kylas ran into a link query; None for nested rules."""
    if not isinstance(rule, dict) or not isinstance(rule.get("rules"), list):
        return None
    rules = []
    for r in rule["rules"]:
        if not isinstance(r, dict) or not (r.get("id") or r.get("field")) or "rules" in r:
            return None
        item = {"field": r.get("id") or r.get("field"), "operator": r.get("operator")}
        for key in ("type", "value"):
            if key in r:
                item[key] = r[key]
        rules.append(item)
    return {"condition": str(rule.get("condition") or "AND").upper(), "rules": rules}


def executed_rule_for(bucket: str, searches: Optional[List[Tuple[str, Dict[str, Any]]]]) -> Optional[Dict[str, Any]]:
    """Return the last jsonRule sent to this entity's own search endpoint, else None."""
    endpoint = SEARCH_ENDPOINTS.get(bucket)
    if not endpoint:
        return None
    for path, rule in reversed(searches or []):
        if isinstance(path, str) and path.rstrip("/").endswith(endpoint):
            return rule
    return None


def _bounded(query: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the query, or None when it is larger than MAX_QUERY_BYTES."""
    try:
        return query if query is None or len(json.dumps(query, default=str)) <= MAX_QUERY_BYTES else None
    except (TypeError, ValueError):
        return None


def search_query(intent: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a term or idle-days query for a search with no recorded rule.

    Never the caller's filters: only the rule Kylas ran may open a list.
    """
    if intent == "search_by_term" and payload.get("search_term"):
        return {"term": payload["search_term"]}
    if intent == "search_idle" and payload.get("days") is not None:
        return {"idleDays": payload["days"]}
    return None


def links_for(endpoint_id: str, entry: Optional[Dict[str, Any]], payload: Dict[str, Any],
              response_text: str,
              searches: Optional[List[Tuple[str, Dict[str, Any]]]] = None) -> List[ResourceLink]:
    """Return the resource links one successful execute_request call earned.

    create/update/note/cancel/delete link to the record; a search links to
    the entity with its count and executed rule. Reads and failures get none.
    """
    if not entry:
        return []
    try:
        envelope = json.loads(response_text)
    except (TypeError, ValueError):
        return []
    if not isinstance(envelope, dict) or envelope.get("ok") is not True:
        return []
    bucket, intent = entry.get("bucket"), entry.get("intent")
    data = envelope.get("data")
    payload = payload or {}

    if intent in _SEARCH_INTENTS:
        # The rule Kylas ran (the last one sent to this entity's search),
        # else only what names no list (a term, idle days) — never the
        # filters as asked. Too large a rule is left out; the count stays.
        query = executed_query(executed_rule_for(bucket, searches)) or search_query(intent, payload)
        return [link(SEARCH, bucket, query=_bounded(query), count=search_count(data))]

    if intent == "create" and isinstance(data, dict):
        record_id = _as_id(data.get("id"))
        if record_id is None:
            return []
        if bucket == "note":
            about_type, about_id = payload.get("entity_type"), _as_id(payload.get("entity_id"))
            about = uri_for(about_type, about_id) if about_type and about_id else None
            return [link(CREATE, "note", record_id,
                         f"Note on {about_type} {about_id}" if about else None, about=about)]
        return [link(CREATE, bucket, record_id, record_name(data))]

    if intent == "update" and isinstance(data, dict):
        record_id = _as_id(data.get("id")) or _as_id(payload.get("entity_id")) \
            or _as_id(payload.get(f"{bucket}_id"))
        return [link(UPDATE, bucket, record_id, record_name(data))] if record_id else []

    if intent in ("cancel", "delete"):
        # These answer "✓ …" or "✗ …" inside an ok envelope.
        record_id = _as_id(payload.get(f"{bucket}_id")) or _as_id(payload.get("entity_id"))
        if not record_id or not (isinstance(data, str) and data.lstrip().startswith("✓")):
            return []
        return [link(DELETE if intent == "delete" else UPDATE, bucket, record_id)]

    return []
