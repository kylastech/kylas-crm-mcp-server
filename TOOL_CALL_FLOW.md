# How a Tool Call Flows Through This Server

This documents the exact call chain that runs, function by function, when an MCP client
invokes a tool on this server (`main.py`). Every function name and line number below was
read directly from the current source — use it as a map while reading the code.

---

## 1. The full stack, top to bottom

```
MCP client (Claude, etc.)
   │  JSON-RPC "tools/call" { name: "get_lead", arguments: {lead_id: 123} }
   ▼
FastMCP dispatcher (mcp = FastMCP(...), main.py:1198)
   │  looks up the tool by name in its registry, builds a MiddlewareContext
   ▼
Middleware chain, IN REGISTRATION ORDER (main.py:1199-1200)
   1. _ToolCallLoggingMiddleware.on_call_tool   (main.py:1113)
   2. _EnsureEntityLabelsMiddleware.on_call_tool (main.py:1140)
   ▼
The tool function itself — a thin @mcp.tool() wrapper (e.g. get_lead, main.py:2311)
   │  validates/reshapes arguments, calls the matching "_logic" function,
   │  catches exceptions and turns them into a human-readable "✓ .../✗ ..." string
   ▼
The "_logic" function — the actual implementation (e.g. get_lead_logic, main.py:2244)
   │  opens an HTTP client via get_client(), makes the real Kylas API call(s)
   ▼
get_client() → _ThrottledClientContext (main.py:686-691)
   │  resolves auth headers, builds User-Agent, opens httpx.AsyncClient
   ▼
_ThrottledClient.get/post/put (main.py:556-584)
   │  sleeps 100-500ms if this is the 2nd+ Kylas call in this tool run
   ▼
httpx.AsyncClient  → event_hooks: _log_request / _log_response (main.py:114, 148)
   │  the actual HTTPS request to Kylas's REST API
   ▼
handle_api_response(response, operation)  (main.py:696)
   │  raises KylasAPIError on non-2xx, otherwise returns response.json()
   ▼
… result bubbles back up through the _logic function → the tool wrapper →
  the middleware chain (which logs "TOOL CALL x — N Kylas API call(s) — Ys — OK/FAILED") →
  FastMCP → the MCP client, as a text/structured tool result.
```

Middleware wraps every tool call like nested layers of an onion: `_ToolCallLoggingMiddleware`
registers first, so it is the *outermost* layer (its `call_next` calls
`_EnsureEntityLabelsMiddleware`, whose `call_next` calls the actual tool). This ordering
matters because `_ToolCallLoggingMiddleware` calls `_reset_api_call_count()` before
`call_next`, and every tool's own body *also* calls `_reset_api_call_count()` again at
the top of its try-block — belt-and-suspenders so the throttle counter and the per-call
Kylas-API-call tally always start at 0 for a fresh tool invocation.

---

## 2. The two-layer pattern used by almost every tool

Nearly every capability in this file is split into two functions:

| Layer | Naming | Responsibility |
|---|---|---|
| **Tool wrapper** | `@mcp.tool()` + plain name, e.g. `get_lead`, `create_entity` | Owns the MCP-facing docstring (this is what the LLM reads to decide when/how to call it), does light validation, calls the `_logic` function, and converts exceptions (`KylasAPIError`, `ValueError`, generic `Exception`) into a formatted `✓ .../✗ ...` string. **Never talks to httpx directly except in a few older tools that inline the client call (e.g. `get_task`, `cancel_meeting`).** |
| **Logic function** | same name + `_logic` suffix, e.g. `get_lead_logic`, `create_lead_logic` | Does the real work: builds the payload, opens `get_client()`, calls Kylas's REST API, calls `handle_api_response`, returns a raw dict (not a string). Reusable — other logic functions call each other directly (see §5). |

Why split them: the `_logic` functions return raw `dict`s and can be composed/called from
*other* logic functions (e.g. `update_lead_logic` calls `get_pipeline_details_logic`).
The tool wrapper is the only layer that ever returns a formatted string to the LLM, and is
the only layer FastMCP actually registers as a callable tool.

---

## 3. Generic dispatch tools — one tool, many entity types

Five tools don't hardcode one entity — they look up the right `_logic` function in a
registry keyed by `entity_type` ("lead", "contact", "deal", "task", "company", "meeting",
"call_log", "quotation"):

- `search_entity(entity_type, filters, ...)` → `main.py:6241` → `search_entity_logic` (`6205`) → `_ENTITY_CONFIG[entity_type]["search_fn"]`
- `search_entity_by_term(entity_type, term, ...)` → `6317` → `search_entity_by_term_logic` (`6282`) → `_ENTITY_CONFIG[entity_type]["by_term_fn"]`
- `search_idle_entities(entity_type, days, ...)` → `6383` → `search_idle_entities_logic` (`6347`) → `_ENTITY_CONFIG[entity_type]["idle_fn"]`
- `create_entity(entity_type, field_values)` → `6412` → directly looks up `_ENTITY_CRUD_CONFIG[entity_type]["create_fn"]`
- `update_entity(entity_type, entity_id, field_values)` → `6501` → directly looks up `_ENTITY_CRUD_CONFIG[entity_type]["update_fn"]`

The two registries (`main.py:6066` and `main.py:6162`):

```python
_ENTITY_CONFIG = {
  "lead":   {"search_fn": search_leads_logic, "by_term_fn": search_leads_by_term_logic,
             "idle_fn": search_idle_leads_logic, "search_endpoint": "/search/lead", ...},
  "deal":   {"search_fn": search_deals_logic, ..., "idle_fn": search_idle_deals_logic, ...},
  "meeting":{"search_fn": search_meetings_logic, "idle_fn": None, ...},   # no idle support → tool returns an error string
  ...
}

_ENTITY_CRUD_CONFIG = {
  "lead": {"create_fn": create_lead_logic, "update_fn": update_lead_logic,
           "name_fn": lambda r: f"{r.get('firstName','')} {r.get('lastName','')}".strip() or "Lead"},
  "deal": {"create_fn": create_deal_logic, "update_fn": update_deal_logic, "name_fn": lambda r: r.get("name", "Deal")},
  ...
}
```

`name_fn` is used purely for the human-readable confirmation message
(`"✓ Lead created successfully.\n  ID: 123\n  Name: Jane Doe"`).

**Important non-obvious fact:** `search_leads`, `search_contacts`, `search_deals`, and
`search_meetings` still exist as plain `async def` functions in this file (e.g.
`main.py:2405`, `2723`, `3847`, and one after `5022`) but are **no longer decorated with
`@mcp.tool()`** — they are not registered as callable MCP tools. They were superseded by
the generic `search_entity` tool, which calls their underlying `_logic` functions
(`search_leads_logic`, etc.) through `_ENTITY_CONFIG` instead. The wrapper functions are
dead code left in place; don't be misled into thinking they're reachable from an MCP
client.

---

## 4. Walkthrough #1 — a simple read: `get_lead(lead_id=123)`

```
get_lead(123)                                   main.py:2311
  └─ get_lead_logic(123)                        main.py:2244
       └─ get_client()                          main.py:686  → _ThrottledClientContext
            └─ _resolve_auth_headers()          main.py:618  (Bearer / x-api-key / env var)
       └─ client.get("/leads/123")              main.py:562  (_ThrottledClient.get)
            └─ _before_api_call()                main.py:538  (no sleep — 1st call)
            └─ httpx request  →  _log_request/_log_response hooks
            └─ _after_api_call()                 main.py:546  (counter → 1)
       └─ handle_api_response(resp, "Get lead")  main.py:696  → raises KylasAPIError or returns dict
  └─ _format_lead_for_display(lead)              main.py:2252  (dict → readable text block)
  ← "==== LEAD DETAILS ==== \nID: 123\n..."
```

## 5. Walkthrough #2 — generic create: `create_entity("lead", {...})`

```
create_entity("lead", {"firstName": "Jane", ...})            main.py:6412
  └─ cfg = _ENTITY_CRUD_CONFIG["lead"]                        main.py:6162
  └─ cfg["create_fn"](field_values)   == create_lead_logic     main.py:2162
       └─ _get_custom_field_id_to_name()  (only if custom fields were passed by numeric ID)
       └─ _normalize_field_values(...)                        main.py:2050
            (splits "phone"/"email" shorthand into phoneNumbers/emails arrays,
             normalizes country codes, resolves custom field IDs → internal names)
       └─ get_client() → client.post("/leads", json=payload)
       └─ handle_api_response(resp, "Create lead")
  └─ name = cfg["name_fn"](result)                             # "Jane Doe"
  ← "✓ Lead created successfully.\n  ID: 456\n  Name: Jane Doe"
```

If `_ENTITY_CRUD_CONFIG` has no entry for the given `entity_type`, `create_entity` never
calls any logic function — it returns `"✗ Unknown entity_type '...'. Valid: lead, contact, ..."`
immediately (main.py:6482-6484).

## 6. Walkthrough #3 — cross-function chain: updating a deal into a locked pipeline stage

This is the deepest call chain in the file — moving a deal to a stage several steps ahead
in a pipeline that has `sequentialStageFlow=true` (stages can't be skipped):

```
update_entity("deal", 789, {"pipelineStage": 55})              main.py:6501
  └─ cfg["update_fn"](789, {...})  == update_deal_logic         main.py:3485
       └─ get_client() → client.put("/deals/789", json=merged)  # first attempt: direct jump
       └─ handle_api_response(...) raises KylasAPIError
       └─ _is_stage_lock_error(error)                           main.py:1985
            → checks error.response_body for Kylas's code "01001086"
       └─ (lock detected) _get_pipeline_details_raw(pipeline_id) main.py:1996
            → GET /pipelines/{id}  (a fresh, separate Kylas API call)
       └─ _advance_deal_to_stage_sequentially(                  main.py:2003
              deal_id=789, stages=[...], current_stage_id=20,
              target_stage_id=55, base_deal=existing_deal)
            └─ for each stage between current and target (inclusive):
                 client.put(f"/deals/789", json=merged)          # one PUT per intermediate stage
                 handle_api_response(...)
            └─ returns the deal's final state after the last PUT
  └─ result bubbles back up to update_entity's tool wrapper
  ← "✓ Deal updated successfully.\n  ID: 789\n  Name: Big Deal"
```

Every one of those PUTs and the GET goes through the *same* `_ThrottledClient`, so the
100–500 ms throttle delay applies between them too — a multi-stage advance across, say,
4 stages makes 5 Kylas API calls in one tool invocation, each after the first sleeping a
random 100–500 ms. The `_ToolCallLoggingMiddleware` line at the end reports the total:
`TOOL CALL update_entity — 5 Kylas API call(s) — 1.62s — OK`.

`update_lead_logic` has a similar (simpler) internal chain: when a caller passes
`pipelineStage`, it fetches `get_pipeline_details_logic(pipeline_id)` (main.py:2215) just
to look up the `forecastingType` that corresponds to the target stage, before doing its
own single PUT.

---

## 7. The shared HTTP layer (every `_logic` function goes through this)

```python
get_client()                              # main.py:691 — returns a context manager
  → _ThrottledClientContext.__init__       # main.py:660
       auth_headers = _resolve_auth_headers()   # main.py:618
         1. FastMCP OAuth access token (get_access_token()) → "Authorization: Bearer <token>"
         2. per-request "x-api-key" HTTP header (get_http_request())
         3. env var KYLAS_API_KEY
         (raises KylasAPIError if none of the three are available)
       user_agent = f"kylas_mcp_server({SERVER_VERSION}) on {_get_mcp_client_name()}"
         _get_mcp_client_name() reads ctx.session.client_params.clientInfo — the
         name/version the MCP client sent during the "initialize" handshake.
       httpx.AsyncClient(base_url=BASE_URL, headers={...}, event_hooks={
           "request": [_log_request], "response": [_log_response]})
  → __aenter__ wraps the raw client in _ThrottledClient
       .get/.post/.put → _before_api_call() [sleep if not first call] → real request →
                          _after_api_call() [increment contextvars counter]
  → __aexit__ closes the httpx client
```

`handle_api_response(response, operation)` (main.py:696) is the single place that turns
an HTTP failure into a `KylasAPIError`:
- non-2xx with status 429 → a specific "rate limit hit, wait N seconds" message
- any other non-2xx → `"{operation} failed: {status_code}"` with the raw body attached as `response_body`
- anything else that throws (bad JSON, etc.) → wrapped generically

Every tool wrapper's `except KylasAPIError as e:` branch is what turns that back into
the `"✗ ... failed: ...\n  Details: ..."` string the LLM sees.

---

## 8. Throttle / API-call counting mechanism

A `contextvars.ContextVar` (`_api_call_count`, main.py:536) tracks how many Kylas API
calls have happened *within the current tool invocation* (contextvars keep this isolated
per concurrent request):

- `_reset_api_call_count()` — set to 0. Called both by `_ToolCallLoggingMiddleware` before
  `call_next`, and again at the top of most tool/logic functions' try-blocks.
- `_before_api_call()` — if the counter is `> 0` (i.e. this is not the first Kylas call
  in this tool run), sleep a random 100–500 ms before making the request. This spaces out
  bursts of calls a single tool makes (e.g. GET-then-PUT in an update, or N PUTs in the
  sequential stage-advance above) so the server doesn't hammer Kylas's API.
- `_after_api_call()` — increments the counter after each call.

`_ToolCallLoggingMiddleware` reads the final counter value after `call_next` returns (or
raises) to print exactly how many Kylas API calls that one tool invocation made.

---

## 9. Startup sequence (before any tool call is even possible)

```
run()                                      main.py:6564  (console-script entry point)
  reads MCP_TRANSPORT env var → mcp.run(transport="stdio" | "streamable-http" | "sse")
  → FastMCP starts the transport, then enters the lifespan context:

_app_lifespan(app)                         main.py:1157
  await _load_entity_labels()              main.py:274  → GET the tenant's custom entity
                                              display-name mapping from Kylas (works only
                                              if KYLAS_API_KEY env var is set — OAuth-only
                                              deployments have no credential at startup)
  if labels loaded:
      app._mcp_server.instructions = _build_instructions()   main.py:1031
      _update_tool_description(app)         main.py:1051   (rewrites get_entity_labels' description)
      _patch_entity_tool_descriptions(app)   main.py:1083   (prefixes every entity tool's
                                                description with a "resolve via kylas://entity-labels" hint)
  else:
      logs "will load lazily on first request"
  asyncio.create_task(_label_refresh_loop(interval_seconds=1800))   # background refresh every 30 min
  yield {}   # server is now live, tools can be called
  # --- on shutdown ---
  cancel the refresh task
  await _kylas_token_verifier.aclose()      # closes the pooled httpx client used to verify OAuth tokens
```

If OAuth-only (no env-var API key), `_EnsureEntityLabelsMiddleware` (main.py:1140) does
the same label-loading work **lazily**, on the first tool call that arrives with valid
credentials — inside an `asyncio.Lock` so concurrent first-requests don't double-load.

**This is not a per-request call.** `_EnsureEntityLabelsMiddleware` runs on every tool
call (it's in the chain), but its body starts with `if not _ENTITY_LABELS:` — once the
cache has been populated (by the lifespan, by this middleware's first successful fetch,
or by the refresh loop below), every later call just falls through to `call_next(context)`
with no extra dict lookup cost and no Kylas API call. `_load_entity_labels()` itself only
actually executes: once at startup, once more lazily if that startup fetch came back
empty (first tool call after credentials become available), and then only again every
30 minutes via the background `_label_refresh_loop` (main.py:298) — never per-request.

---

## 10. Complete tool inventory (all 36 registered `@mcp.tool()` functions)

| Tool (MCP-visible name) | Line | Calls |
|---|---|---|
| `get_entity_labels` | 1384 | reads module-level `_ENTITY_LABELS` cache |
| `get_lead_field_instructions` | 1440 | `get_lead_field_instructions_logic` |
| `get_current_user` | 1470 | `_fetch_current_user` → `get_client()` → `GET /users/me` |
| `lookup_users` | 1568 | `lookup_users_logic` |
| `lookup_products` | 1635 | `lookup_products_logic` |
| `lookup_pipelines` | 1700 | `lookup_pipelines_logic` |
| `get_pipeline_stages` | 1772 | `get_pipeline_stages_logic` |
| `get_pipeline_details` | 1848 | `get_pipeline_details_logic` |
| `parse_datetime_to_utc_iso_tool` | 1892 | `parse_datetime_to_utc_iso` (pure function, no API call) |
| `get_lead` | 2310 | `get_lead_logic` → `_format_lead_for_display` |
| `get_contact_field_instructions` | 2621 | inline `get_client()` call |
| `get_contact` | 2651 | `get_contact_logic` |
| `get_task_field_instructions` | 2896 | inline `get_client()` call |
| `get_task` | 2917 | inline `get_client()` call (no separate `_logic`) |
| `search_tasks` | 2991 | `search_tasks_logic` |
| `search_tasks_with_any_relation` | 3207 | `_search_tasks_with_any_relation_logic` → `_fetch_raw_tasks_for_relation` |
| `get_deal_field_instructions` | 3299 | `get_deal_field_instructions_logic` |
| `get_deal` | 3756 | `get_deal_logic` |
| `get_company_field_instructions` | 4060 | `get_company_field_instructions_logic` |
| `get_company` | 4236 | `get_company_logic` |
| `get_meeting_field_instructions` | 4425 | `get_meeting_field_instructions_logic` |
| `get_meeting` | 4806 | `get_meeting_logic` |
| `cancel_meeting` | 4827 | inline `get_client()` call (PUT status=cancelled) |
| `delete_meeting` | 4851 | inline `get_client()` call (DELETE) |
| `lookup_meeting_related_entity` | 4986 | dispatches by `entity_type` to `lookup_{leads,contacts,deals,companies}_for_meeting_logic` or `lookup_meeting_invitees_logic` |
| `get_call_log_field_instructions` | 5132 | `get_call_log_field_instructions_logic` |
| `get_call_logs` | 5243 | `get_lead_logic` / `get_contact_logic` / `get_deal_logic` (to display the related entity's name) + inline call-log fetch |
| `add_note` | 5521 | inline `get_client()` call, entity-type-agnostic endpoint |
| `get_quotation_field_instructions` | 5822 | `get_quotation_field_instructions_logic` |
| `get_quotation` | 5902 | `get_quotation_logic` |
| `search_entity` | 6241 | `search_entity_logic` → `_ENTITY_CONFIG[entity_type]["search_fn"]` |
| `search_entity_by_term` | 6317 | `search_entity_by_term_logic` → `_ENTITY_CONFIG[entity_type]["by_term_fn"]` |
| `search_idle_entities` | 6383 | `search_idle_entities_logic` → `_ENTITY_CONFIG[entity_type]["idle_fn"]` |
| `create_entity` | 6412 | `_ENTITY_CRUD_CONFIG[entity_type]["create_fn"]` |
| `update_entity` | 6501 | `_ENTITY_CRUD_CONFIG[entity_type]["update_fn"]` |
| `lookup_entity_for_task` | 3022 | `lookup_{leads,contacts,deals,companies}_for_task` (looked up via a small dict keyed by `entity_type`) |

There is also one `@mcp.resource(...)` (not a tool): `entity_labels_resource`
(`kylas://entity-labels`, registered near main.py:1407) — served when a client reads the
resource URI that the patched tool descriptions point to (see §9).

---

## 11. Error propagation, summarized

```
Kylas API returns 4xx/5xx
  → handle_api_response() raises KylasAPIError(message, status_code, response_body)
  → propagates up through any intermediate _logic functions (nothing catches it there)
  → the @mcp.tool() wrapper's `except KylasAPIError as e:` catches it
  → returns "✗ <action> failed: {e.message}\n  Details: {e.response_body}"
  → _ToolCallLoggingMiddleware still logs "... FAILED (KylasAPIError: ...)" because the
    exception was already caught and turned into a normal string return — so from the
    middleware's point of view the call actually *succeeded* (no exception reached it).
    Only truly unhandled exceptions (bugs, not API errors) show up as MCP-level tool errors.
```

This is deliberate: the LLM client always gets a plain-text tool result (never a raw MCP
protocol error) for expected failure modes like "lead not found" or "rate limited", so it
can read the message and decide what to do next (retry, ask the user, etc.).
