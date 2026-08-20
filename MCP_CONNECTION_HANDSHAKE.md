# The MCP Connection Handshake — Everything Sent & Received at Startup

This documents, field by field, everything exchanged between an MCP client (Claude
Desktop, Claude Code, etc.) and this server (`main.py`) from the moment a connection
opens to the moment the client has a usable tool list. Every JSON snippet below except
where marked "illustrative" was captured by actually introspecting the live server
objects in this repo (`FastMCP("Kylas CRM", ...)` at `main.py:1198`), not copied from
documentation — so the numbers and shapes are this server's real output, not generic
MCP examples. See [`TOOL_CALL_FLOW.md`](./TOOL_CALL_FLOW.md) for what happens *after*
this handshake, once tools start getting called.

---

## 1. Where this sits in the connection lifecycle

```
1. Transport connects           (stdio spawn, or first HTTP request)
2. OAuth discovery + token       (HTTP transport only, skipped for stdio)
   exchange
3. "initialize" request/response  ◄── this document, §4-6
4. "initialized" notification     ◄── this document, §7
5. "tools/list" request/response  ◄── this document, §8
6. "resources/list" / "resources/read" (optional, on demand) ◄── §9
7. normal operation: tools/call, ... (see TOOL_CALL_FLOW.md)
```

---

## 2. Transport connects

Picked by the `MCP_TRANSPORT` env var (`main.py:6564-6581`):
- **stdio** — the client spawns `python main.py` as a subprocess; JSON-RPC travels over
  stdin/stdout. No network, so §3 (OAuth) never happens.
- **streamable-http** / **sse** — the client POSTs JSON-RPC to `http://host:port/mcp`
  (or `/sse`). Network-reachable, so OAuth discovery applies if `KYLAS_CLIENT_ID`/
  `KYLAS_CLIENT_SECRET` are set (`create_kylas_auth`, `kylas_oauth.py:222`).

---

## 3. OAuth discovery + token exchange (HTTP transport, only when OAuth is configured)

This server is currently configured with OAuth in this environment (`KYLAS_CLIENT_ID`/
`KYLAS_CLIENT_SECRET` are set) — the `KylasOAuthProxy` (`kylas_oauth.py:190`) registers
these HTTP routes *before* any MCP JSON-RPC message is exchanged:

| Route | Purpose |
|---|---|
| `/.well-known/oauth-authorization-server` | RFC 8414 metadata: where `/authorize` and `/token` live, supported grant types, etc. |
| `/.well-known/openid-configuration` | Same metadata, OpenID-Connect-shaped alias some clients probe instead |
| `/.well-known/oauth-protected-resource` | Tells the client which authorization server protects this MCP endpoint |
| `/oauth/authorize` (upstream: Kylas's own `/oauth/authorize`) | Where the client redirects the user to log in |
| `/oauth/token` (upstream: Kylas's own `/oauth/token`) | Where the client exchanges an authorization code for an access token |

Sequence: client discovers the metadata → redirects the user to authorize → gets a code
→ exchanges it at `/oauth/token` → gets back an opaque Kylas access token → attaches it
as `Authorization: Bearer <token>` on **every** subsequent request, including the
`initialize` call in §4. `KylasTokenVerifier.verify_token()` (`kylas_oauth.py:80`)
validates that token against Kylas's `GET /users/me` on every request (with a 30s cache).
Full detail on this flow is in the earlier discussion of `kylas_oauth.py` — this section
only covers what's relevant to the handshake: **by the time `initialize` arrives, the
client already holds a validated Bearer token (or, legacy path, an `x-api-key` header)**.

For **stdio**, none of this exists — trust is implicit in having spawned the process.

---

## 4. The `initialize` request (client → server)

The client sends this once, as the very first JSON-RPC message on the connection:

```json
{
  "jsonrpc": "2.0",
  "id": 0,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": { "roots": {}, "sampling": {} },
    "clientInfo": { "name": "Claude Desktop", "version": "1.2.3" }
  }
}
```

This server doesn't validate or negotiate `protocolVersion` itself — that's handled by
the underlying `mcp` SDK. What this server *does* read back out of this message later:
`ctx.session.client_params.clientInfo` — see `_get_mcp_client_name()` (`main.py:594-615`),
used to build the `User-Agent` string this server sends to Kylas's own API
(`kylas_mcp_server(3.4.7) on Claude Desktop(1.2.3)`).

---

## 5. The `initialize` response (server → client) — real captured output

This is `main.mcp._mcp_server.create_initialization_options()` actually evaluated
against this repo's current `main.py`, right after import (before the lifespan has run —
see the caveat in §5d):

```json
{
  "server_name": "Kylas CRM",
  "server_version": "3.4.7",
  "capabilities": {
    "experimental": {},
    "logging": {},
    "prompts": { "listChanged": true },
    "resources": { "subscribe": false, "listChanged": true },
    "tools": { "listChanged": true },
    "extensions": { "io.modelcontextprotocol/ui": {} }
  },
  "instructions": "\n# Kylas CRM MCP Server - Lead, Contact & Task Support\n\n## ⚠️ MANDATORY SESSION RULES — READ FIRST\n\n### Rule 0 — Call first, every session\n... <22,345 characters total>"
}
```

### 5a. `server_name` / `server_version`
- `"Kylas CRM"` — the first positional arg to `FastMCP(...)` (`main.py:1198`).
- `"3.4.7"` — read from installed package metadata: `SERVER_VERSION = _pkg_version("kylas-crm-mcp-server")` (`main.py:95-98`), falling back to `"unknown"` if the package isn't pip-installed (e.g. running straight from a checkout without `pip install -e .`).
- No `title`, `websiteUrl`, or `icons` are set anywhere in this codebase, so those `Implementation` fields are simply absent.

### 5b. `capabilities`
These come from the underlying `mcp` SDK's `LowLevelServer.get_capabilities()`, derived
from **which JSON-RPC handlers are registered** — not from anything this app explicitly
configures:
- `tools: {listChanged: true}` — because this server has tools (36 of them, §8). FastMCP always registers `notification_options.tools_changed = True`, meaning the server is *capable* of sending a `notifications/tools/list_changed` push — but as covered earlier, nothing in `main.py` ever calls the method that would actually emit that notification when `_patch_entity_tool_descriptions` mutates descriptions post-startup. The capability is declared; it's just never exercised after the initial handshake.
- `resources: {subscribe: false, listChanged: true}` — present because one resource is registered (`entity_labels_resource`, §9). `subscribe: false` because this server never implements per-resource subscription.
- `prompts: {listChanged: true}` — present even though **this server defines zero `@mcp.prompt` functions**. FastMCP always wires up the `list_prompts` handler regardless of whether any prompts exist, so the capability always appears (advertising "I can list prompts," which here means "I can tell you the list is empty").
- `logging: {}` — the server can receive `logging/setLevel` requests from the client (a completely separate channel from this server's own Python `logging` module / `kylas-mcp.log` file).
- `extensions: {"io.modelcontextprotocol/ui": {}}` — a FastMCP-framework-level capability flag, unrelated to any Kylas-specific code in this repo.
- `experimental: {}` — empty; this app registers no experimental capabilities.

### 5c. `instructions`
Built by concatenating seven blocks defined across `main.py:333-1017`, joined with `"\n\n"` (`main.py:1023-1027`):

```
_base_instructions =
    SYSTEM_INSTRUCTIONS               (main.py:333)  — session rules, entity label
                                                         routing placeholder, pagination
                                                         rules, default date range rule
  + DEAL_SYSTEM_INSTRUCTIONS          (main.py:725)  — deal-specific field formats,
                                                         pipeline/stage rules, products
  + COMPANY_SYSTEM_INSTRUCTIONS       (main.py:768)
  + MEETING_SYSTEM_INSTRUCTIONS       (main.py:787)
  + CALL_LOG_SYSTEM_INSTRUCTIONS      (main.py:842)
  + QUOTATION_SYSTEM_INSTRUCTIONS     (main.py:875)  — marks quotations read-only
  + DIAGNOSIS_AND_REPORTING_INSTRUCTIONS (main.py:913) — the 🔴/🟡/🟢 diagnosis format
                                                         and report-formatting rules
```

Measured length: **22,345 characters** (≈ 5,500-6,000 tokens by a rough 4-chars-per-token
estimate) as delivered in `initialize`, before any entity-label enrichment. This is sent
to the client **once**, on the first `initialize` of a session — it is the closest thing
this server has to a system prompt, and every rule an LLM client follows about pagination,
date defaults, diagnosis formatting, etc. lives entirely in this one string.

### 5d. The `{ENTITY_LABEL_MAPPING}` placeholder — a startup race worth knowing about
`SYSTEM_INSTRUCTIONS` contains a literal `{ENTITY_LABEL_MAPPING}` token (`main.py:368`).
`_build_instructions()` (`main.py:1031-1034`) replaces it with the tenant's actual custom
entity names — but only **after** `_load_entity_labels()` has successfully run
(`_app_lifespan`, `main.py:1160`). If a client's `initialize` request happens to race
ahead of that startup fetch (or the fetch fails because no `KYLAS_API_KEY` env var is set
and OAuth hasn't authenticated yet), the client receives the **literal placeholder text**
`{ENTITY_LABEL_MAPPING}` sitting in the instructions, and the entity-tool descriptions in
§8 won't have their tenant-name prefix yet either. This self-heals the moment the first
tool call succeeds (`_EnsureEntityLabelsMiddleware`, `main.py:1140`) or the 30-minute
refresh loop runs — but because there's no `list_changed` notification (§5b), an
already-connected client's `instructions` string stays stale as the placeholder for the
rest of that session. Only a fresh `initialize` (i.e., a new connection) picks up the
filled-in mapping.

---

## 6. The `initialized` notification (client → server)

Once the client has processed the `initialize` response, it sends:

```json
{ "jsonrpc": "2.0", "method": "notifications/initialized" }
```

A one-way notification — no response. This is the client's signal that the session is
live; well-behaved clients don't call `tools/list` or `tools/call` before sending it.
Nothing in `main.py` hooks this notification specifically; FastMCP/the `mcp` SDK handles
it internally to flip the session into a "ready" state.

---

## 7. `tools/list` (client → server → client) — real captured output

The client typically calls this right after `initialized`. This server currently
registers **36 tools** (all via bare `@mcp.tool()` — no explicit `name=`, `description=`,
or `annotations=` overrides anywhere in this codebase, confirmed by grep). That means for
every tool:
- **`name`** = the Python function's name, verbatim (e.g. `get_lead`, `create_entity`).
- **`description`** = the function's **entire docstring**, not just the first line — FastMCP's docstring parser splits the summary/long-description from an `Args:`-style parameter section, and injects per-parameter text into `inputSchema` properties (see `create_entity` below), but the tool-level `description` field is the full prose block above the parameter list.
- **`inputSchema`** = a JSON Schema generated by Pydantic from the function's Python type hints and default values — no schema is hand-written anywhere in this file.
- **`outputSchema`** = auto-generated too, with a FastMCP-specific twist (see below).

### 7a. Simple example — `get_lead(lead_id: int) -> str` (`main.py:2311`)

```json
{
  "name": "get_lead",
  "description": "Get full details of a lead by ID (GET /leads/{id}). Use when the user asks for complete lead info, lead details, or to view a specific lead.\nlead_id: The lead ID (e.g. from search_leads or search_leads_by_term results).",
  "inputSchema": {
    "type": "object",
    "properties": { "lead_id": { "type": "integer" } },
    "required": ["lead_id"],
    "additionalProperties": false
  },
  "outputSchema": {
    "type": "object",
    "properties": { "result": { "type": "string" } },
    "required": ["result"],
    "x-fastmcp-wrap-result": true
  }
}
```

Note the `outputSchema`: even though `get_lead` returns a plain Python `str`, FastMCP
still emits a structured schema wrapping it as `{"result": "<the string>"}`, flagged with
its own `x-fastmcp-wrap-result: true` extension key. This lets clients that support
structured tool results (`structuredContent`) get a typed wrapper, while the actual
content block returned in a `tools/call` response is still plain text (see
`TOOL_CALL_FLOW.md` for what a call response looks like).

### 7b. Object-parameter example — `create_entity(entity_type: str, field_values: Dict[str, Any]) -> str` (`main.py:6412`)

```json
{
  "name": "create_entity",
  "inputSchema": {
    "type": "object",
    "properties": {
      "entity_type": { "type": "string" },
      "field_values": { "type": "object", "additionalProperties": true }
    },
    "required": ["entity_type", "field_values"],
    "additionalProperties": false
  }
}
```
`Dict[str, Any]` becomes a schema-less open object (`additionalProperties: true`) — the
real shape of `field_values` isn't enforced by the schema at all; it's documented entirely
in prose inside the 3,410-character docstring (the longest of any tool in this server —
it spells out phone/email shorthand rules and a full payload example per entity type).
This is a deliberate pattern in this codebase: generic dispatch tools (`create_entity`,
`update_entity`, `search_entity`) take loosely-typed dicts/lists, so **all** of the actual
input contract is carried in the docstring text the LLM reads, not in machine-checkable
JSON Schema.

### 7c. Aggregate payload size (measured, this server, right now)

| | chars | rough tokens (÷4) |
|---|---|---|
| `instructions` (§5c) | 22,345 | ~5,600 |
| all 36 tools' `description` fields, summed | 19,819 | ~4,950 |
| all 36 tools' full JSON (name+description+input/outputSchema), summed | 35,440 | ~8,860 |
| the one resource's `description` (§9) | 250 | ~65 |

So a client connecting to this server pays roughly **14,500 tokens** total
(`instructions` + full `tools/list` payload) just to establish a session — before a
single tool has been called. This is the number from the earlier "does this hamper my
token usage" discussion, made concrete: it's a **fixed, one-time cost per session**
(re-paid only if the client reconnects), it does not grow per tool call, and — per §5d —
it grows by only a small, bounded amount once entity labels are loaded (a routing line
per custom entity type in `instructions`, plus a one-line prefix on ~7 tool descriptions
in `_patch_entity_tool_descriptions`, `main.py:1083`).

---

## 8. `resources/list` / `resources/read` — the one resource this server exposes

```json
// resources/list response
[{
  "uri": "kylas://entity-labels",
  "name": "Entity Label Mapping",
  "description": "Tenant-specific entity name mapping. Read this to resolve custom entity names to standard CRM types. Example: 'animals' may map to 'contact', 'cars' may map to 'deal'. Always read this resource when the user refers to an entity by an unfamiliar name.",
  "mimeType": "text/plain"
}]
```

Registered via `@mcp.resource("kylas://entity-labels", name=..., description=..., mime_type="text/plain")` (`main.py:1408-1417`), unlike tools this resource's `name`/`description`/`mimeType` are **explicitly authored**, not derived from the function signature — only the *body* (`entity_labels_resource`, `main.py:1418`) is a plain function that gets called on `resources/read`. Its content is the same entity-label mapping used to enrich `instructions`, formatted as plain text; `_patch_entity_tool_descriptions` (§5d) is what nudges the LLM to actually go read it when it sees a tenant-custom name it doesn't recognize.

---

## 9. What is deliberately never sent during this handshake

No message in `initialize`, `tools/list`, or `resources/list` ever contains:
- The Kylas Bearer token or `x-api-key` header value (those live only in server-side request headers to Kylas — `_resolve_auth_headers`, `main.py:618`, is called per-outbound-API-call, never surfaced to the MCP client).
- `KYLAS_CLIENT_SECRET`, `KYLAS_API_KEY`, or any other env var.
- Per-user Kylas data — `instructions` and tool descriptions only ever contain the tenant's **entity-label mapping** (display names like "animals"→"contact"), never actual CRM records. Records only appear in `tools/call` results (see `TOOL_CALL_FLOW.md`), long after this handshake is done.

---

## 10. Full sequence, one picture

```
Client                          This Server                      Kylas
  │                                  │                              │
  │ (HTTP only) OAuth discovery ────►│                              │
  │◄── well-known metadata ──────────┤                              │
  │──── authorize / token exchange ─►├───── validate/forward ──────►│
  │◄──── Bearer access token ────────┤◄──────────────────────────────┤
  │                                  │                              │
  ├── initialize {clientInfo, ...} ─►│                              │
  │                                  │ _get_mcp_client_name() will   │
  │                                  │ later read clientInfo back    │
  │◄── InitializeResult ─────────────┤ {serverInfo, capabilities,    │
  │    (§5: name/version/            │  instructions (22.3K chars,   │
  │     capabilities/instructions)   │  possibly still has the raw   │
  │                                  │  {ENTITY_LABEL_MAPPING} token │
  │                                  │  if labels haven't loaded)}   │
  │                                  │                              │
  ├── notifications/initialized ────►│  (one-way, no reply)          │
  │                                  │                              │
  ├── tools/list ────────────────────►│                              │
  │◄── 36 Tool objects (§7-§7c) ─────┤                              │
  │                                  │                              │
  ├── resources/list ────────────────►│  (optional)                  │
  │◄── 1 Resource (§8) ──────────────┤                              │
  │                                  │                              │
  │  ... session now live; tools/call proceeds as in                │
  │      TOOL_CALL_FLOW.md ...                                      │
```
