# Design: Collapsing 36 Tools to 3 (Discover → Build → Execute)

Status: **proposal / discussion draft** — no implementation yet.
Scope: this document only. No code changes in this pass.

---

## 0. Method note

Everything in §1 below was re-verified directly against the current `main.py` on this
branch (grep, AST-walk, manual read) rather than taken from this repo's existing
`TOOL_CALL_FLOW.md` / `MCP_CONNECTION_HANDSHAKE.md`. Those two docs describe the right
*shape* of the system but contain fabricated specifics (a middleware class that has
never existed in git history, systematically wrong line numbers, a server version that
matches none of the four real version fields in this repo) — see
`tool-call-flow-docs-stale` in project memory. Where I reuse a number from them (the
character counts in §5), I re-measured it myself first and note that it checked out.

---

## 1. What's actually in the repo today

### 1.1 How tools are registered and described

Every tool is a bare `@mcp.tool()`-decorated function in `main.py`. FastMCP derives,
with no manual overrides anywhere in this file:

- **name** — the Python function name verbatim.
- **description** — the *entire* docstring (not just the summary line).
- **inputSchema** — JSON Schema generated from the function's type hints / defaults.
- **outputSchema** — auto-generated, wrapping the (almost always plain-`str`) return
  value as `{"result": "<string>"}`.

There are exactly **36** such tools right now (verified by AST walk, not grep-on-text,
so decorator false-positives are ruled out). They fall into six groups:

| Group | Count | Examples |
|---|---|---|
| Per-entity field/schema instructions | 8 | `get_lead_field_instructions`, `get_deal_field_instructions`, … one per entity |
| Per-entity single-record `get` | 8 | `get_lead`, `get_contact`, `get_deal`, `get_call_logs`, `get_quotation`, … |
| **Generic CRUD/search dispatch** | 5 | `search_entity`, `search_entity_by_term`, `search_idle_entities`, `create_entity`, `update_entity` |
| Shared utilities | 8 | `lookup_users`, `lookup_products`, `lookup_pipelines`, `get_pipeline_stages`, `get_pipeline_details`, `get_current_user`, `parse_datetime_to_utc_iso_tool`, `get_entity_labels` |
| Task-relation tools | 3 | `search_tasks` (legacy, redundant with `search_entity("task",…)` but still registered — an existing inconsistency, not something I introduced), `lookup_entity_for_task`, `search_tasks_with_any_relation` |
| Meeting actions + cross-entity note | 4 | `cancel_meeting`, `delete_meeting`, `lookup_meeting_related_entity`, `add_note` |

**Important, and it changes the framing of the whole exercise**: this server has
*already* done half of the consolidation you're proposing. `search_entity`,
`search_entity_by_term`, `search_idle_entities`, `create_entity`, `update_entity` are
one tool each covering 7–8 entity "buckets" via two internal registries,
`_ENTITY_CONFIG` and `_ENTITY_CRUD_CONFIG`, keyed by `entity_type` string. Their
docstrings are already the longest in the file (up to ~3,400 chars for `create_entity`)
because all the per-entity payload-shape knowledge that would otherwise live in 8
separate schemas is written as prose in one docstring instead. That is the same
progressive-disclosure problem you're describing, just at 1/8th the current scale —
worth knowing going in, because it's a preview of a real failure mode: prose-only
"schema" in a single doctring gets long and the model still has to hold it all in
context to use *any* of the 8 entities it covers, since there's no way to ask for
"just the deal-specific part."

### 1.2 Auth, base URL, headers, error handling

All already centralized, and this part needs little rework:

- `BASE_URL` — one module-level constant from `KYLAS_BASE_URL` env var.
- `_resolve_auth_headers()` (`main.py:601`) — tries, in order: FastMCP OAuth Bearer
  token → per-request `x-api-key` HTTP header → `KYLAS_API_KEY` env var. Raises
  `KylasAPIError` if none apply.
- `get_client()` → `_ThrottledClientContext` → `_ThrottledClient` — one `httpx.AsyncClient`
  per tool call, with a `contextvars`-based throttle that sleeps 100–500ms before the
  2nd+ Kylas call within a single tool invocation (to avoid 429s on multi-call tools).
- `handle_api_response(response, operation)` — the one place that turns a non-2xx
  response into a `KylasAPIError(message, status_code, response_body)`, with a special
  message for 429.
- Every tool wrapper's `except KylasAPIError` turns that into a `"✗ ... failed: ...\n
  Details: ..."` string — never a raw exception reaches the MCP client.

This is genuinely good news for the migration: `execute_request`'s "centralized
auth/base URL/headers, normalized envelope" requirement (AC5) is close to already
built. The work there is mostly renaming/wrapping, not designing from scratch.

### 1.3 How much of each tool is boilerplate vs. unique

Two different things are true at once, and the distinction matters for what a YAML
registry can and can't absorb:

- **The control-flow shell is boilerplate** (~10 lines, near-identical across all 36):
  `_reset_api_call_count()` → call the `_logic` function → `except KylasAPIError` →
  `except Exception`. This collapses cleanly into `execute_request`.
- **The docstrings are not boilerplate — they're the actual product.** I measured this
  directly (AST-walked every `@mcp.tool()` function, summed docstring lengths):
  **19,819 characters** across 36 tools, average **550 chars/tool**, and it's almost
  entirely domain knowledge with no generic substitute: phone/email shorthand rules,
  which picklist fields use option-ID vs. internal-name, pagination base (0- vs
  1-based), which fields need a prior `lookup_*` call, pipeline-stage semantics, etc.
  None of that is repeated boilerplate you can delete by consolidating tools — it has
  to go *somewhere* in the new design (registry entries, most likely — see §5).
- **The `_logic` functions vary enormously in size and uniqueness.** A simple GET
  (`get_lead_logic`) is ~6 lines. `update_lead_logic` / `update_deal_logic` are
  GET-merge-PUT (not a partial PATCH), handle nested `pipeline.stage` mutation, and
  fetch pipeline details mid-flight to backfill `forecastingType`. `create_lead_logic`
  routes through `_normalize_field_values` (~110 lines) which does phone/email
  shorthand expansion, country-code normalization via the `phonenumbers` library,
  custom-field ID→internal-name resolution, and primary-flag enforcement. None of that
  is "boilerplate repeated 36 times" — it's real, non-generic business logic that a
  method+path+schema YAML row cannot express; it has to keep living in code that a
  registry entry *points at*.

### 1.4 What will resist a purely generic pattern

Concretely, from reading the code (not hypothetically):

1. **Dynamic, tenant-specific schema.** The 8 `get_*_field_instructions` tools exist
   because Kylas exposes *live, per-tenant* field metadata — custom fields and
   picklist option IDs are not fixed and differ per Kylas account. A YAML registry can
   describe the *shape* of the standard payload, but it cannot bake in a tenant's
   custom field list or picklist IDs; that has to be fetched at request time
   (`_fetch_lead_fields()` et al., cacheable but not static). This directly collides
   with **AC4** ("build_payload validates … and returns an actionable error before any
   network call") — see §6.
2. **A genuinely complex, shared filter DSL.** `_build_search_json_rule` +
   `OPERATOR_MAPPING` (13 field types × their allowed operators) +
   `PICKLIST_FIELDS_USE_INTERNAL_NAME` (a *different* exception set per entity — leads
   use one set, deals use another) is the single most intricate piece of logic in the
   file. It already depends on live per-tenant `filterable_map` data, not just static
   schema. This is the hardest 20% of the whole migration, and it's mostly invisible if
   you only look at the "36 tools → 3 tools" framing.
3. **Multi-request composite operations that are not "one method + one path":**
   - `update_deal_logic`'s sequential-stage-advance: on a `01001086` (stage-lock)
     error it fetches pipeline details and issues **one PUT per intermediate stage** in
     a loop. One logical "move deal to stage X" call can become 1 GET + N PUTs.
   - `search_tasks_with_any_relation`: fires 4 parallel `is_not_null` searches and
     de-dupes client-side.
   - `get_call_logs`: fetches the call log **and** the related lead/contact/deal (via
     `get_lead_logic`/`get_contact_logic`/`get_deal_logic`) to resolve a display name —
     a join, not a single GET.
   None of these are "method + path + schema" rows. See §7.
4. **Pagination is not uniform.** Lead/contact/task/deal/company are 0-based, max page
   size 100. Meeting/call_log are **1-based**, max page size **500**. This has to be a
   first-class field per registry entry, not something re-derived per call.
5. **Side-effecting, low-frequency, hard-to-undo actions**: `delete_meeting` (DELETE,
   permanent), `cancel_meeting` (soft, reversible). Mechanically these fit the generic
   pattern fine — but see §9 on why they deserve extra guardrails a generic tool
   structurally weakens.
6. **A read-only bucket enforced only by omission.** Quotations have `get`/`search`
   but deliberately no `create_fn`/`update_fn` entry in `_ENTITY_CRUD_CONFIG` — the
   restriction is enforced by *absence*, not by an explicit flag. Fine today at 36
   endpoints; fragile at 150 if someone adds a row without noticing the intent (see
   open question in §12).

---

## 2. Reaction to the hypothesis

The Discover → Build → Execute shape is directionally right, and it matches something
this codebase has already half-done on its own (`search_entity`/`create_entity`/etc.).
I'd keep the 3-tool shape. Three places I want to push back or qualify before you
treat this as settled:

- **AC4 ("no network call before validation") is not fully achievable as literally
  stated**, because validating a create/update payload for `lead`/`deal`/etc. requires
  live tenant metadata (custom fields, picklist IDs) that isn't static YAML. I'll
  propose a version of this that's honest about that (§6).
- **AC7 ("adding an endpoint = adding a YAML entry, no code change") is true for
  the large majority of rows but not all of them.** The composite operations in §1.4.3
  need a named handler, not a bare method+path. I'd rather say that explicitly than
  force a fake genericization that quietly breaks the one deal-stage-advance case
  everyone actually depends on.
- **"3 exposed tools" and "no loss of safety signal for destructive ops" are in
  tension.** Collapsing `delete_meeting` into the same `execute_request` as
  `get_lead` removes a structural signal (a distinct tool name, and MCP's
  `destructiveHint` annotation) that today lets a client's UI or the model itself treat
  deletion differently from a read. I don't think this kills the design, but it needs
  a deliberate answer, not a silent regression (§9, §12).

Everything else — bucket/intent tagging, YAML-driven registry, progressive
disclosure via a discovery tool — I think is sound and is what the rest of this
document designs.

---

## 3. Discovery: how `list_tool` and `build_payload` divide the work

**Corrected from an earlier draft**: `list_tool` used to have a "detail mode"
(pass an exact `id`, get the full schema back) alongside its coarse listing mode. That
was redundant the moment `build_payload` also returns the full schema for an `id` —
two tools answering the same question. Fixed below: `list_tool` now does exactly
one thing, and `build_payload` owns "give me everything about this one endpoint"
entirely, which also changes what `build_payload` *doesn't* do anymore (§3.2, §6).

### 3.1 `list_tool` — one mode, coarse listing only

Given `bucket`/`intent`/a free-text keyword, returns short rows only — `id`, `bucket`,
`intent`, `description`. Never a schema, method, path, or example, no matter how
specific the query gets. Its only job is narrowing the registry down to 1-5 candidate
ids:

```
list_tool(bucket="lead", intent="search")
  → [{id: "lead.search", bucket: "lead", intent: "search", description: "Filter/search leads by field criteria."},
     {id: "lead.search_by_term", bucket: "lead", intent: "search", description: "Free-text search across lead fields."}]
```

### 3.2 `build_payload` now owns the "detail" job too — and only the detail job

Once the model has picked an `id`, it calls `build_payload({"id": "lead.create"})` —
**note: just the id, no args yet** — and gets back everything: method, path, usage
notes, and a schema built from a live fetch of this tenant's real field metadata
(custom fields, picklist option IDs included, not just the static YAML shape):

```
build_payload({"id": "lead.create"})
  → { id: "lead.create", method: "POST", path: "/leads",
      usage_notes: "phone requires phone_country_code + phone_type; customFieldValues keyed by internal name, not ID.",
      schema: { required: [...], properties: { ownerId: {type: "long", resolve_via: "user.lookup"}, ... } },
      example: {firstName: "Jane", lastName: "Doe", email: "jane@x.com"} }
```

The model then builds the actual payload **itself**, from this schema, and sends it
straight to `execute_request` — `build_payload` never sees or validates the model's
specific filled-in values in this design. That's a real behavior change from earlier
drafts (where `build_payload` took `(id, args)` and validated them) — see §6 for what
that means for AC4, and the new open question in §12 about who (if anyone) validates
the model's actual attempt before it reaches Kylas.

If a field carries `resolve_via` (e.g. `ownerId` → `user.lookup`), the model runs a
second, full three-call cycle against the shared `_meta` bucket (renamed from `util`,
see §13) — `list_tool` → `build_payload` → `execute_request` — to resolve the
human name to a real ID, then comes back and fills that field in itself. Nothing here
is automatic; every resolution the model needs, it goes and gets, one full cycle at a
time (§8.2 already established this — unchanged by this correction).

### 3.3 Smallest thing that still works

`list_tool` rows are **id + bucket + intent + description only** — at every
specificity, forever. That job belongs entirely to `build_payload` now. Bucket/intent
stay closed enums (8 buckets + `_meta`, ~7 intents), so list-mode filtering stays cheap
regardless of registry size.

---

## 4. What permanently lives in the 3 tools' own descriptions/schemas

This is the one cost that recurs regardless of registry size, so it needs to be small
and *not* proportional to endpoint count:

- **`list_tool`**: explain the bucket/intent enums, the list-vs-detail dual mode,
  and how to read a detail response. Bounded by enum cardinality, not endpoint count.
- **`build_payload`**: explain the generic validation contract and error shape
  (`{ok: false, error, missing_fields, hint}`), and that `endpoint_id` + open `args`
  dict is the calling convention (same "prose-documented open object" pattern the
  existing `create_entity`/`update_entity` already use — nothing new here).
- **`execute_request`**: explain the success/error envelope shape and that it expects
  the payload `build_payload` produced.

Estimate: each of these three descriptions is comparable in shape to today's
`create_entity` docstring (~3.4K chars, the longest in the file) or smaller, since two
of the three (list/build) are mostly protocol explanation, not per-entity domain
rules. Call it **~1,000–1,500 chars each, ~3.5–4.5K chars / ~900–1,100 tokens total**
for the fixed cost of the 3 tool descriptions + their (small, mostly-enum)
`inputSchema`s — against **19,819 chars / ~4,950 tokens** today for 36 tool
descriptions alone, before their JSON schemas. That's the real, durable win, and it's
flat as the registry grows to 150 rows — which is the entire point.

---

## 5. Where the per-endpoint domain rules go (the part your framing doesn't fully cover)

Collapsing the *tool list* is only half the token story. The other half is the
**22,333-character `instructions` block** (measured directly, matches the earlier
docs' figure) that FastMCP sends as the MCP server's `instructions` string. This is
not "sent once and forgotten" the way `MCP_CONNECTION_HANDSHAKE.md` implies — MCP's
`initialize` handshake happens once per *connection*, but the underlying model API
calls underneath (Claude/OpenAI-style Messages APIs) are stateless: the system prompt
*and* the full tool-schema array have to be resent on every single inference call for
the life of the conversation. So both numbers I measured — 19,819 chars of tool
descriptions and 22,333 chars of instructions — are effectively paid **every turn**,
not once per session. Your framing in the prompt ("re-sent on every single turn") is
the accurate one; the repo's own doc undersells this.

That means: if you shrink the 36-tool schema tax but leave `SYSTEM_INSTRUCTIONS` +
its 6 sibling blocks untouched, you keep paying ~5.6K tokens/turn for meeting-invitee
rules, quotation read-only warnings, and deal-money-field shape — regardless of
whether the session ever touches meetings, quotations, or deals. Recommend treating
this as part of the same migration, in two tiers:

- **Universal core** (stays in the global `instructions` string, small): call
  `list_tool` before anything else, envelope shape, pagination discipline
  ("don't auto-fetch all pages"), the entity-label routing rule. Maybe 1–1.5K chars.
- **Per-endpoint notes** (move into each registry entry's `usage_notes`/`example`,
  surfaced only in detail-mode `list_tool` responses): phone/email shorthand,
  picklist option-ID-vs-name exceptions, pipeline-stage rules, quotation-is-read-only,
  meeting-invitee restrictions. Paid only in the turn where that endpoint is actually
  looked up — marginal cost, not fixed cost.

This is a bigger lever than the tool-schema shrink alone, but it's also the riskiest
part to get right (see §14, Stage 4) — rules that are always-visible today might stop
being reliably obeyed once they're conditionally surfaced.

**One more source of savings this framing hides, and it's a big one:** today, per
`SYSTEM_INSTRUCTIONS` Rule 1, the model is told to call each entity's
`get_*_field_instructions` tool once per session **before the first create/update of
that entity** — and that tool dumps the *entire* field list: every standard field,
every custom field, every picklist's full option list, regardless of which 3–4 fields
the user actually wants to set. That dump is unconditionally paid once per entity per
session today, and it can be large (proportional to how many custom fields/picklists a
tenant has configured — not something I can size precisely without a live tenant, but
it's routinely the single largest tool-result in a create/update-heavy session). Under
the new design, `build_payload` only needs to surface the parts of that metadata that
are actually relevant to the fields the model is trying to set (§11.3, Scenario C) —
this turns an unconditional full-schema dump into a targeted, as-needed one. This is
probably worth more, in real sessions that create or update records, than the 36→3
tool-list shrink itself.

---

## 6. `build_payload`'s network call, and where validation of the model's attempt now lives

AC4 originally said build_payload validates and returns an actionable error "before
any network call" — worded for a design where `build_payload` took `(id, args)`. Since
§3.2's correction, `build_payload` only ever takes an `id`; it never receives the
model's filled-in values at all. That splits what used to be one question into two,
and only the first is settled:

**1. Does `build_payload` still make a network call?** Yes — building an accurate
schema for `create`/`update`/`search` intents on entities with tenant-custom fields
requires a live fetch of this tenant's real field metadata
(`_fetch_lead_fields()`-equivalent, §1.4.1) — the static YAML alone only has the
*standard* shape, never a tenant's custom fields or picklist option IDs. This is a
metadata read, never the target endpoint's own method+path (`POST /leads`, etc.) —
that stays `execute_request`'s job alone, and the distinction should be explicit in
`build_payload`'s own description. Cache this per tenant, short TTL (today's
field-fetch has no cache at all — worth fixing regardless of anything else here).

**2. Who validates the model's actual filled-in payload before it reaches Kylas? —
genuinely open, see §12.** Since `build_payload` no longer receives the model's args,
it structurally cannot be that validation step anymore — a real gap the earlier design
didn't have. Two live options for where validation (if any) now happens:
- **`execute_request` re-checks** the incoming payload against the same schema
  `build_payload` just returned, before making the real Kylas call — catches bad input
  locally with a clear, actionable error, same spirit AC4 originally wanted, just
  moved one tool over. Reuses logic that already has to exist to build the schema.
- **`execute_request` just forwards** whatever it's given straight to Kylas, and lets
  Kylas's own validation error be the model's feedback loop — simpler to build, no
  duplicated validation logic, but means every malformed payload costs a real network
  round-trip to discover instead of being caught locally, and Kylas's error text is
  written for a human developer, not phrased as the actionable hint this design's
  error envelope aims for elsewhere.

I lean toward the first (re-check in `execute_request`) — it's a small amount of reused
validation logic and preserves the "never a raw exception, always an actionable
envelope" property the rest of this design relies on — but this is exactly the kind of
decision that changes what `execute_request`'s own description has to promise, so it's
flagged as open (§12) rather than decided silently here.

`build_payload`'s output otherwise stays a plain dict (schema + example), not an opaque
handle/token — passing a live dict rather than a session-cached reference avoids
needing server-side session state and avoids a confused-deputy risk (a stale cached
reference being replayed against a different endpoint or after the underlying record
changed).

---

## 7. Composite/orchestrated operations — the part that resists "YAML entry only"

Three operations in §1.4.3 are not expressible as method+path+schema:

| Operation | Why it's not a plain row |
|---|---|
| Deal → target pipeline stage (sequential flow) | 1 logical call fans out to 1 GET + N PUTs on a `01001086` lock |
| `search_tasks_with_any_relation` | 4 parallel searches + client-side de-dupe |
| `get_call_logs` | joins a call-log fetch with a related-entity name lookup |

Recommendation: don't force these into the same registry shape as the other ~33
rows. Give the registry a second row `type`:

```yaml
- id: deal.advance_to_stage
  bucket: deal
  intent: update
  type: composite            # ← not "http"
  handler: advance_deal_to_stage   # looked up in a small internal Python dict,
                                    # same pattern as today's _ENTITY_CONFIG
  description: "Move a deal to a target pipeline stage, advancing through
                intermediate stages automatically if the pipeline requires it."
```

This means AC7 ("adding an endpoint = YAML only, no code") is true for the ~33 plain
CRUD/search/get rows, and explicitly **not** true for this small, named, stable set —
say so up front rather than discover it mid-implementation when someone tries to
represent a "loop until stage reached" as a static schema and can't.

Importantly (and this matters for §11 below): **this doesn't cost anything extra in
model-facing round trips.** Whether the server does 1 HTTP call or 1 GET + 5 PUTs
under the hood, the model still only sees one `build_payload` + one `execute_request`
call — the orchestration is entirely server-side and invisible to the model's context,
in both the old design and the new one. The "resists genericization" problem here is
about *where the code for the loop lives* (§7 above), not about how many tool calls
the model has to make (§11 below).

---

## 8. Handling prerequisite / stacked tool calls

This answers a real gap in the design so far: **some tool calls only make sense, or
are only safe, after another call has already happened.** Today's server has this
problem too, in two genuinely different shapes, and they need different treatment —
worth separating them before deciding how the new 3-tool shape should handle either.

### 8.1 The chain *inside* one logical operation: discover → build → execute

This is a stack by construction — `execute_request` needs a payload the model built
from `build_payload`'s schema, which needs a known `id` (usually from
`list_tool`). §11 covers the token/round-trip cost of this chain in detail.

**Revised by §3.2/§6's correction**: this section previously discussed "should
`execute_request` refuse to run unless it can prove `build_payload` was called first"
(a signed-token idea). That question mostly dissolves now — since `build_payload`
never sees the model's args, skipping it isn't a *validation* bypass, only an
*information* one (the model just didn't fetch the schema/example first, and is
building the payload from memory or a guess instead). There's nothing sensitive to
prove was checked, because `build_payload` never checked anything about the specific
attempt to begin with.

**What actually still matters** is squarely §6's open question: does *anything* verify
the model's real, filled-in payload before it reaches Kylas? That's no longer a
"how strictly do we enforce the discover→build→execute order" question — it's "does
`execute_request` re-validate, or not, period" — see §12 for the open decision. This
section's earlier signed-token proposal doesn't apply to that question (a token only
proves a schema was *fetched*, not that the final payload matches it), so it's dropped
here rather than carried forward as a red herring.

### 8.2 Cross-endpoint prerequisites: "resolve an ID before you can act"

This is the more interesting case, and likely the one behind the question. Today, a
real chunk of `SYSTEM_INSTRUCTIONS` (§1.3) exists purely to tell the model, in prose,
things like: *"Never guess IDs — always resolve first. Users: call
`lookup_users(query)`. Products: call `lookup_products(query)`. Entity IDs for
association filters: search for the entity first."* Concretely, today's dependency
chains look like:

| To do this... | ...you must first call | Why |
|---|---|---|
| Filter/create/update anything by owner, createdBy, assignedTo, etc. | `lookup_users` | These are Kylas user IDs, not guessable |
| Filter/create by product | `lookup_products` | Same — a real product ID |
| Filter/update by pipeline or pipelineStage | `lookup_pipelines` → (user confirms) → `get_pipeline_stages` | Two calls *and* a mandatory human-confirmation step in between |
| Create/update a task's `relation`, or filter tasks by an associated entity | `lookup_entity_for_task` | Resolve the lead/contact/deal/company ID by name first |
| Add a meeting participant or `relatedTo` entry | `lookup_meeting_related_entity` | Same idea, meeting-specific endpoint |

None of this is enforced by the server today — it's all convention, sitting in a
global instructions blob the model has to remember and apply correctly on its own. If
the model skips a step and just invents a plausible-looking ID, nothing here stops it;
Kylas either 404s (loud, recoverable) or — worse — the ID happens to belong to a real,
different record, and the operation silently succeeds against the wrong entity. That's
a real gap **today**, not something this migration would introduce, and it's worth
fixing regardless of what happens to the tool count.

Three concrete mechanisms for the new design, additive not mutually exclusive:

1. **Field-level "resolve via" hints in the registry**, surfaced exactly where
   they're relevant instead of buried in a global rule. E.g. `lead.search`'s detail
   response includes, for the `ownerId` filter field specifically:
   ```yaml
   fields:
     ownerId: {type: long, resolve_via: "user.lookup", note: "Do not guess — resolve via user.lookup first."}
   ```
   The model sees this only when it actually looks up `lead.search`'s detail schema —
   progressive disclosure applied to prerequisites, not just to the endpoints
   themselves.
2. **`build_payload` sanity-checks lookup-style fields against live data**, not just
   their type. If a field is tagged as a user/product/pipeline reference,
   `build_payload` can verify the given ID actually exists (a cheap `GET /users/{id}`-
   style check) before accepting it — turning "the model should have called
   `lookup_users` first" from an unenforced convention into an actual validated fact,
   and catching exactly the silent-wrong-ID failure mode above. This is a genuine
   improvement over today's behavior, not just a port of it, and it fits naturally
   alongside the tenant-metadata fetch `build_payload` already has to do (§6).
3. **A `see_also` list on each registry entry** for the cases that can't be reduced to
   "verify an ID" — most notably `lookup_pipelines → get_pipeline_stages`, where the
   real requirement is a *conversation-level* fact ("did the user actually confirm
   which pipeline"), not a data fact any server-side check can verify:
   ```yaml
   - id: lead.set_pipeline_stage
     see_also: ["pipeline.lookup", "pipeline.get_stages"]
     usage_notes: >
       Call pipeline.lookup, present the pipeline(s) to the user, wait for
       confirmation, THEN pipeline.get_stages for that pipeline only. Do not
       guess a stage ID even if only one pipeline exists.
   ```
   This category genuinely stays soft, prose-enforced convention, same as today — no
   registry mechanism can substitute for "did the human actually confirm this." The
   best the new design can do is make the rule appear exactly when it's relevant
   (attached to `lead.set_pipeline_stage`'s own detail response) instead of once,
   globally, at the start of a session where it's easy to have scrolled past.

**Net effect on the round-trip math in §11**: none of this is new cost the migration
introduces — these are already multi-call chains today (`lookup_users` then
`search_entity`/`create_entity` with the resolved ID is already two tool calls in the
current 36-tool server). The new design needs to preserve, not regress, this pattern;
mechanism 2 above (live sanity-checking) is a place it can genuinely get *safer* than
today, not just equivalent — worth calling out as a concrete improvement to point to
when pitching this to your team, alongside the token savings.

---

## 9. What gets worse, concretely

- **Type safety at the tool boundary weakens for the ~31 endpoints that currently get
  real Pydantic-derived schemas** (e.g. `get_lead(lead_id: int)` rejects a non-numeric
  ID at the MCP protocol layer, for free). Under the generic shape, *every* endpoint's
  args become an open dict validated by your own code, which means you're now on the
  hook for writing (and testing) a validator with the same rigor FastMCP gave you for
  free — not fatal, but real added surface, and a new class of bug (validator bugs)
  you now own. Note this is already true today for the 5 generic dispatch tools; the
  migration makes it true for all 36 instead of 5.
- **Weaker tool-name affordance for destructive/irreversible actions.**
  `delete_meeting` as a distinct, prominently-named tool is a real (if soft) signal
  that slows a model down. Folded into `execute_request`, it's just an id string. MCP's
  `destructiveHint`/`readOnlyHint`/`idempotentHint` tool annotations — which some
  clients use to gate confirmation UI — become close to useless too: `execute_request`
  has to declare itself as capable of destructive actions in general (since it
  sometimes deletes), which makes *every* call through it look equally dangerous to a
  client that reads those hints, which is strictly worse than today's per-tool
  granularity. Mitigation options, not mutually exclusive: (a) tag `intent: delete` /
  `destructive: true` in the registry and have `build_payload`/`execute_request` require
  an explicit `confirm: true` arg for those rows; (b) keep genuinely hard/irreversible
  deletes (today: just `delete_meeting`) as their own explicit tool, outside the
  generic 3, and accept "3 generic + a small named allowlist of dangerous exceptions"
  rather than literally 3. I lean toward (a) for now since the list of hard deletes is
  currently tiny, but flag this as a decision for you, not something I should assume
  (see §12).
- **Harder debugging / observability.** Logs and any tool-call analytics will show
  `execute_request` N times instead of a readable `get_lead`/`create_deal`/... trace,
  unless `endpoint_id` is threaded prominently into every log line, trace span, and any
  future logging middleware. Cheap to fix, easy to forget.
- **Loss of per-entity `outputSchema`.** Today every tool at least nominally has a
  typed `outputSchema` (even though in practice almost everything returns
  `{"result": "<string>"}`). A generic `execute_request` envelope is one shape for
  everything; any client that wanted to render, say, a Deal object in a specialized
  widget via `structuredContent` loses that. Probably a minor, acceptable loss given
  today's tools already mostly return formatted strings, but worth naming.
- **Added round-trip latency per logical operation.** Every user-visible "do a CRM
  thing" now costs more model round trips than a single tool call did (quantified
  precisely in §11, not just "3x") — even where token cost nets out ahead, *latency*
  for a single operation goes up, and that's a real UX cost independent of tokens.

---

## 10. Token cost — the fixed-tax baseline (measured, not estimated)

| | chars | ~tokens |
|---|---|---|
| 36 tool descriptions (docstrings only) | 19,819 | ~4,950 |
| `instructions` (7 concatenated blocks) | 22,333 | ~5,580 |
| **Total fixed tax, before JSON schemas** | 42,152 | **~10,530** |

This is paid on **every single underlying model API call for the life of the
conversation** (see §5's correction to the repo's own doc), not once per MCP session.
It doesn't grow as the conversation gets longer, but it also never shrinks, and it
grows linearly as you add tools — this is the real, uncherry-picked baseline problem,
and it's large enough on its own to justify collapsing the tool list regardless of
what happens to round-trip count.

New fixed tax, estimated: 3 tool descriptions + small enum-heavy schemas
(~3.5–4.5K chars / ~900–1,100 tokens, §4) plus a pruned universal `instructions` core
(~1–1.5K chars, §5) — call it **~1,300–1,700 tokens total**, an **~85% reduction**.
That's the headline number. §11 below works through what happens to it once you
account for the extra round trips honestly, with actual tool-call sequences, rather
than asserting a multiplier.

---

## 11. Concrete walkthroughs: how tool calls change, one by one

This section answers directly: *what does the user-visible (and model-visible) flow
look like, call by call, and where do the tokens actually go.* Numbers below are
illustrative — built from the measured fixed-tax figures above plus reasonable
estimates for tool-result sizes — not an empirical benchmark. Treat them as "which
direction and roughly what order of magnitude," and confirm with the real measurement
plan in §11.6 before quoting them externally.

### 11.0 The round-trip unit, precisely

Every tool call costs **one full extra model API request/response pair**, because the
Messages-API-style loop is: model emits a `tool_use` block → the server executes it →
the result is appended to the conversation → the model is re-invoked (paying the full
fixed tax again) to decide what happens next. For **N sequential tool calls**, that's
**N+1 total model requests** (the extra "+1" is the final request where the model
composes its natural-language reply once all tool results are in). This is the
correct way to count what changes — not "3 tools vs. 1 tool," but "how many times does
the fixed tax get paid, and how much history accumulates in between."

### 11.1 Scenario A — "Get lead 123" (first CRM operation this session)

**Today (1 tool call → 2 model requests):**

```
Request 1:  [user: "get lead 123"] + 36 tool schemas + instructions   (~10,530 tok tax)
Response 1: tool_use → get_lead(lead_id=123)
  [server executes get_lead_logic → formats lead block, ~350 tok]
Request 2:  [..., tool_result: "LEAD DETAILS ... ID: 123 ..."] + 36 schemas + instructions
            (~10,530 tok tax again, + ~350 tok history)
Response 2: final natural-language reply
```
Total fixed tax paid: **2 × 10,530 ≈ 21,060 tokens**, plus one ~350-token result
carried into the second request.

**New design (2 tool calls → 3 model requests):**

```
Request 1:  [user: "get lead 123"] + 3 tool schemas + slim instructions (~1,500 tok tax)
Response 1: tool_use → list_tool(bucket="lead", intent="get")
  [server returns list-mode row(s), ~40-60 tok: {id:"lead.get", intent:"get", ...}]
Request 2:  [..., tool_result: row] + 3 schemas + instructions          (~1,500 tok tax)
Response 2: tool_use → build_payload("lead.get", {"lead_id": 123})
  [server validates, no network call needed for a bare ID — ~30 tok result]
Request 3:  [..., tool_result: {ok:true, payload:{"lead_id":123}}] + schemas (~1,500 tok)
Response 3: tool_use → execute_request("lead.get", {"lead_id": 123})
  [server calls Kylas, returns normalized envelope, ~350-450 tok — same size as today's result]
Request 4:  [..., tool_result: envelope] + schemas                     (~1,500 tok tax)
Response 4: final natural-language reply
```
Total fixed tax paid: **4 × 1,500 ≈ 6,000 tokens**, plus ~40+30+400 ≈ 470 tokens of
small results accumulating across requests.

**Net for this scenario: ~6,470 tokens (new) vs. ~21,410 tokens (old) — roughly a 3.3×
reduction, even though the new flow takes 2 extra model round trips.** The fixed-tax
cut (85% per request) dominates the extra request count (2x) for a trivial read.

### 11.2 Scenario B — steady state: "Now get lead 456" (later, same session)

This is the case that matters most for real usage, and it's not covered by a naive
"3x tool calls" framing: **once the model has already discovered `lead.get` earlier in
the same conversation, that discovery result is still sitting in the conversation
history** — it doesn't need to call `list_tool` again for the same endpoint.

```
Request N:   [..., user: "now get lead 456"] + 3 schemas + instructions (~1,500 tok)
Response N:  tool_use → build_payload("lead.get", {"lead_id": 456})   [skips discovery]
Request N+1: [..., tool_result] + schemas                              (~1,500 tok)
Response N+1: tool_use → execute_request("lead.get", {"lead_id": 456})
Request N+2: [..., tool_result: envelope] + schemas                    (~1,500 tok)
Response N+2: final reply
```
3 requests (not 4) — **only the first touch of a given endpoint per session pays the
full discover→build→execute cost; every later call to that same endpoint in the same
session is build→execute (2 tool calls, 3 requests)**, vs. the old design's flat 2
requests per operation, forever, with no way to ever get cheaper. In a session that
touches the same handful of endpoints repeatedly (very common — "get lead 123", then
"now get lead 456", then "and 789"), the new design's *relative* advantage grows over
the session, not shrinks — this revises the more cautious "it could go either way"
framing in an earlier draft of this section: precise round-trip accounting says the
new design comes out ahead in both the cold-start and steady-state case, and the gap
doesn't close as operation count grows. What *does* stay real is the latency point
from §9 — 3 model round trips still take longer wall-clock than 1, even when the token
total is lower.

### 11.3 Scenario C — create with dynamic fields: "Create a lead for Jane Doe,
email jane@acme.com, US mobile 555-1234, source Website"

**Today (2 tool calls → 3 requests):**
```
Response 1: tool_use → get_lead_field_instructions()
  [mandatory per SYSTEM_INSTRUCTIONS Rule 1 — dumps the ENTIRE field cheat sheet:
   every standard field, every custom field, every picklist's full option list.
   Size scales with how much this tenant has customized leads — commonly the
   single largest tool-result in a create-heavy session, easily 1,000-3,000+ tok.]
Response 2: tool_use → create_lead(field_values={...})
  [confirmation string, ~80-120 tok]
Response 3: final reply
```
Fixed tax: 3 × 10,530 ≈ 31,590, **plus** the large, unconditional field-cheat-sheet
dump (§5's last point) landing in history for both of the remaining requests.

**New design (3 tool calls → 4 requests):**
```
Response 1: tool_use → list_tool(bucket="lead", intent="create")
  [~40-60 tok: {id: "lead.create", ...}]
Response 2: tool_use → build_payload({"id": "lead.create"})
  [server fetches/caches tenant field metadata internally (§6) to build the schema —
   this is where the full custom-field/picklist detail lives, but only the RELEVANT
   shape/rules are returned (~150-250 tok), not a full cheat-sheet dump of every field.
   The model builds the actual payload itself from this schema — build_payload never
   sees or checks the model's real values (§3.2).]
Response 3: tool_use → execute_request({"id": "lead.create", "payload": {firstName: "Jane", ...}})
  [~80-120 tok result if valid, same order of magnitude as today's confirmation string
   — or a structured error if invalid, see below]
Response 4: final reply
```
Fixed tax: 4 × 1,500 ≈ 6,000, plus small accumulating results (~330-470 tok total) —
**no unconditional full-schema dump**, because the field metadata now backs a
targeted schema response instead of being pushed into the model's context wholesale.
This scenario is where the new design wins by the widest margin, precisely because
today's "call field instructions once per entity per session" rule is an
all-or-nothing, unconditionally-large cost that the new design turns into a
conditional, targeted one.

If the model's actual attempt *is* invalid (e.g. it guesses a picklist value by name
instead of ID) — **assuming `execute_request` re-validates before sending, per the
still-open question in §12** — it returns a structured, actionable error
(`{ok: false, missing_fields: [...], hint: "..."}`) that teaches just the relevant
slice of the schema, the same self-correcting role the current mandatory
field-instructions call plays today, but paid only when actually needed. If that
open question resolves the other way (forward-and-let-Kylas-reject), this same catch
happens one network hop later, with Kylas's own error text instead.

### 11.4 Scenario D — the composite case: "Move deal 789 to stage 55" (locked pipeline)

Both designs hide the orchestration server-side, so the model-facing shape is
identical in spirit to Scenario A/B — only the tool names differ:

```
Today:  update_deal(deal_id=789, field_values={"pipelineStage": 55})
        → server does: PUT → 01001086 lock error → GET pipeline → N sequential PUTs
        → tool wrapper returns one confirmation string regardless of N

New:    build_payload({"id": "deal.update"})   → schema back, model builds the payload itself
        → execute_request({"id": "deal.update", "payload": {"deal_id": 789, "pipelineStage": 55}})
        → same server-side N-PUT loop (type: composite handler, §7), one envelope back
```
No extra model-facing round trips versus a plain update in either design — confirms
the §7 point that composite operations don't change the round-trip math, only where
the orchestration code lives.

### 11.5 Summary table

| Scenario | Today: tool calls → requests | New: tool calls → requests | Today fixed tax | New fixed tax | Net direction |
|---|---|---|---|---|---|
| Get by ID, first touch this session | 1 → 2 | 2 → 3 | ~21,060 | ~6,000 | New wins (~3.3×) |
| Get by ID, endpoint already discovered | 1 → 2 | 2 → 3 | ~21,060 | ~4,500 | New wins (~4.7×) |
| Create with fields, first touch | 2 → 3 | 3 → 4 | ~31,590 + large uncond. dump | ~6,000 + small cond. result | New wins by the widest margin |
| Composite (sequential stage advance) | 1 → 2 | 2 → 3 | ~21,060 | ~4,500–6,000 | New wins; no extra round trips from the orchestration itself |

The pattern across all four: the fixed-tax cut (~85% per request) is large enough
that it outweighs the extra 1–2 round trips per operation in every scenario examined
here, and the advantage *improves* as a session goes on and endpoints get reused
(11.2), and improves further for anything that today pays the unconditional
field-cheat-sheet tax (11.3). Two things this table does **not** capture, both still
real: (a) wall-clock latency per operation goes up (more sequential round trips,
§9), and (b) if the client uses prompt caching on tool schemas/system prompt, the
"fixed tax per request" numbers above shrink a lot on *both* sides after the first
request, which narrows the gap — see §12's caching question, still unresolved.

### 11.6 How to actually verify this, not just estimate it

Same recommendation as before, tightened now that we have concrete sequences to
script: build the two servers (or a prototype 3-tool one), replay the exact tool-call
sequences in §11.1–11.4 against a real or recorded Kylas tenant, and log actual token
usage per request from the model API's own usage fields (not char/4 estimates) for a
handful of full sessions that mix cold-start and steady-state operations. That
replaces every estimate in this section with a measured number before you commit to
citing it externally.

---

## 12. Open questions — for team discussion before finalizing the design

Grouped by theme so a team review can walk through them section by section. Each
question names the decision it unlocks, and where I already have a lean, I've said so
— but these are genuinely open, not rhetorical.

### A. How the client and users actually behave (changes the whole ROI case in §11)

1. **Prompt caching**: does the MCP client(s) you actually run this behind (Claude
   Desktop, Claude Code, Cursor, a custom agent — possibly more than one) cache the
   system prompt/tool definitions across turns? If so, the "fixed tax per request"
   numbers in §11 shrink a lot after the first request, on *both* designs, which
   narrows the gap between them. This single answer changes how much of the pitch to
   your team should be "token savings" vs. "tool-selection accuracy" (36 similar tools
   confusing the model), since the second argument holds regardless of caching.
2. **Which MCP client(s) actually connect to this server in production, and do they
   all behave the same way?** The §11 walkthroughs assume a standard stateless
   Messages-API-style tool-use loop. If different clients batch, summarize, or manage
   history differently, the round-trip accounting in §11 may not hold uniformly across
   your user base.
3. **Are there non-LLM, programmatic callers of these 36 tools today** — scripts,
   internal services, automated test harnesses, or partner integrations that call
   specific tool names directly over the MCP protocol, not through an LLM's own
   reasoning? This matters a lot: an LLM naturally adapts to a new discover→build→
   execute pattern turn by turn, but a hardcoded script does not — it has to be
   rewritten to do the 3-call dance itself. If such callers exist, they're a strong
   argument for keeping a stable subset of tool names available (see C.9–10 below),
   not just a migration-order detail.
4. **Do you have real session telemetry today** (tool-call sequences, session
   lengths, which of the 36 tools actually get used vs. sit idle)? Without it, every
   number in §11 is a plausible estimate, not a measured one, and prioritizing which
   endpoints get parity-tested first (§14, Stage 2) is a guess rather than data-driven.

### B. Safety / product decisions (things a generic tool structurally weakens — §9)

5. **Destructive-action guardrails**: require an explicit `confirm: true` argument on
   `intent: delete` rows (or any row tagged `destructive: true`), keep genuinely
   irreversible actions (today: just `delete_meeting`) as their own named tool outside
   the generic 3, do both, or neither? This is a direct tradeoff against "exactly 3
   tools" (AC1) — I lean toward the `confirm: true` flag since the list of hard
   deletes is small today, but it's your risk call, not mine to assume.
6. **Composite operations (§7)**: is the team OK with an explicit, small "not
   literally YAML-only" exception for the ~3 orchestrated operations (deal
   sequential-stage-advance, task-any-relation search, call-log join), or does AC7
   ("adding an endpoint = YAML only, no code") need to hold with zero exceptions? If
   the latter, that likely means building a generic orchestration mini-engine inside
   `execute_request` (e.g. "retry until condition X"), which is real added complexity
   for a very small, stable set of cases.
7. **Read-only bucket enforcement** (quotations today, maybe others later): explicit
   registry data (`readonly: true` at bucket level, checked in code) or enforcement by
   omission (no `create`/`update` rows exist for that bucket, same as today)? I lean
   explicit — omission-based enforcement is exactly the kind of thing that silently
   breaks when someone adds a row later without registering the original intent.
8. **Are any of today's "always-visible" instruction rules non-negotiable policy**
   (e.g. "never assume a phone's country code," "always confirm the pipeline before
   moving stages"), such that they must stay in the global `instructions` string
   permanently regardless of token cost — vs. rules that are safe to make conditional/
   progressive (surfaced only when that endpoint is looked up, §5, §14 Stage 4)? This
   is a product/data-quality call, not just an engineering one, and probably needs
   input from whoever owns customer-facing data quality, not just engineering.
9. **Prerequisite enforcement strength (§8)**: for cross-endpoint prerequisites like
   "resolve via `lookup_users` before filtering by owner," is it enough to add
   `build_payload`-side sanity-checking (verify the ID actually exists, §8.2 mechanism
   2), or does the team want something stronger — e.g. refusing to execute a
   write/filter on a raw numeric ID that was never confirmed via a lookup call this
   session? The latter needs some form of session-scoped tracking that this stateless
   design otherwise avoids (§8.1), so it's a real tradeoff to weigh, not a default.
10. **NEW, and now the most consequential open question in this doc (§3.2, §6)**:
    since `build_payload` no longer sees the model's filled-in values (only an `id`),
    should `execute_request` re-validate the payload against the schema before sending
    to Kylas, or just forward it and let Kylas's own error be the feedback loop? This
    used to be implicitly covered by "build_payload validates" — that's no longer
    true, so this needs an explicit answer, not an inherited assumption. I lean toward
    re-validating in `execute_request` (§6), but it's a real design commitment (extra
    logic in the one tool that also does the real Kylas call) versus a real simplicity
    win (no duplicated validation logic at all, at the cost of every bad payload
    costing a live network round-trip and getting Kylas's raw error text instead of
    this design's actionable-hint envelope).

### C. Compatibility, ownership, and rollout

11. **Backward compatibility for external consumers**: does anyone outside this team
    depend on the current 36 tool names/schemas directly — customer-written prompts or
    scripts, published docs, partner integrations? If tool names are effectively a
    public interface for some consumers, that's a real compatibility constraint, not
    just an internal refactor.
12. **If such consumers exist, should a version-pinned/stable subset of the 36 stay
    available indefinitely** alongside the new 3 generic tools, rather than a full
    cutover? This is a direct follow-on from Q11 and changes Stage 3/4 of the migration
    plan (§14) from "flip a flag" to "maintain two surfaces long-term."
13. **Rollback criteria**: once the 36-tool path is flagged off in Stage 3 (§14), what
    specific, measurable signal (error-rate threshold, latency SLA breach, a support-
    ticket spike, a specific customer complaint) triggers flipping back, and who has
    the authority to make that call in production? Deciding this before cutover, not
    during an incident, is the point.
14. **Registry ownership**: who writes and maintains the YAML registry going forward
    — the same engineers who write the `_logic` functions, a separate platform/tooling
    owner, or (for `usage_notes` prose specifically) someone non-engineering like
    support or product? This affects whether the review process for a registry change
    should look like a normal code PR or something lighter-weight for prose-only edits.

### D. Security / correctness at the new execution boundary

15. **Multi-tenant metadata caching**: this server already has a latent bug unrelated
    to this migration — `_ENTITY_LABELS` (and the field-metadata fetches) are a single
    module-level global with no per-tenant keying, which is wrong for a multi-tenant
    OAuth deployment (confirmed: `create_kylas_auth`/`KylasOAuthProxy` exist precisely
    because multiple users/tenants connect). Any new metadata cache `build_payload`
    introduces (§6) must be keyed per-tenant from the start, or it inherits the same
    bug at larger scale. Do you want this existing bug fixed as part of this effort,
    or filed and tracked separately?
16. **Does Kylas's own API already reject actions outside a user's granted
    permissions** (the `recordActions` map returned by `GET /users/me`, already
    surfaced in `get_current_user`'s output), or does any current tool wrapper do its
    *own* pre-check that a single generic `execute_request` would need to replicate to
    avoid silently dropping a safety check that today lived inside one specific
    function? Worth an explicit audit before consolidating, not an assumption that
    "Kylas enforces it downstream anyway" is airtight.
17. **Does the build→execute split increase real Kylas API call volume per logical
    operation** — e.g. a metadata-fetch call inside `build_payload` on top of the
    actual call inside `execute_request`, where today's equivalent might have made
    only one Kylas call? If so, does that add meaningful rate-limit/429 pressure,
    given Kylas already throttles aggressively enough that this server has its own
    100–500ms inter-call delay (§1.2)? This may argue for longer-lived metadata
    caching than "per build_payload call."

### E. Success criteria, scope, and timeline

18. **What does "success" look like, concretely?** A target token-reduction
    percentage, a target improvement in tool-selection accuracy, a maximum acceptable
    latency increase per operation, an error-rate-parity bar versus today. Defining
    this now gives the team something to check the migration against later, instead of
    arguing about it retroactively.
19. **How much engineering time is the team willing to commit**, over what timeframe,
    to the 5-stage migration in §14 — and is there a specific deadline or complaint
    (a cost threshold, a customer escalation, a target model-accuracy metric) driving
    this that should compress or relax how cautious the staged rollout can afford to
    be?
20. **Growth timeline**: "we expect to keep adding them" — roughly how many endpoints,
    over what horizon? Affects how aggressively to design the file-splitting strategy
    in §13 now vs. defer.
21. **Scope boundary**: should this migration also touch the existing tenant
    entity-label system (the "animals"→contact-style custom-name mapping, its own
    routing instructions and `kylas://entity-labels` resource) since it's a related,
    pre-existing form of progressive disclosure — or is that explicitly out of scope
    and left untouched for this pass? Worth stating explicitly so it isn't assumed
    either way.

---

## 13. YAML registry organization

### 13.1 Shape of one entry

```yaml
- id: lead.search                 # bucket.action, globally unique, enforced at startup
  bucket: lead
  intent: search                  # get | list | search | create | update | delete | meta
                                   # — separate from HTTP method, since filtering is
                                   # POST .../search, not a GET
  method: POST
  path: /search/lead
  type: http                      # http | composite (see §7)
  pagination: {style: "0-based", max_size: 100}
  dynamic_fields: true            # schema depends on live tenant field metadata (§6)
  description: "Filter/search leads by field criteria."   # ← list_tool text (§3.1)
  usage_notes: >                  # ← build_payload response only (§3.2), was DEAL_SYSTEM_INSTRUCTIONS-style prose
    Only [FILTERABLE] fields apply. Picklist fields use Option ID except
    requirementCurrency, companyBusinessType, country, timezone, companyIndustry
    (use internal name). Date/datetime filters keep the user's timezone; pass
    timeZone or it defaults to the current user's.
  example:
    filters: [{field: "updatedAt", operator: "greater_or_equal", value: "<ISO>"}]
```

### 13.2 File layout

One file per bucket at 36 entries (`registry/lead.yaml`, `registry/deal.yaml`,
`registry/meeting.yaml`, …, plus `registry/_meta.yaml` for the 8 shared-utility rows
like `lookup_users`/`get_current_user`), plus a thin `registry/index.yaml` declaring
the closed bucket/intent enums the startup validator checks against.

At 150 entries, don't widen the bucket files — split further along the same axis you'd
already split code: `registry/lead/search.yaml`, `registry/lead/create.yaml`, etc.,
once a single bucket file passes roughly 15–20 entries or a few hundred lines — the
same "keep a PR diff reviewable" instinct that presumably already governs how you'd
split a growing Python module. Keep `id` uniqueness and required-field checks
(AC9 — duplicate ids, missing fields, invalid schema shape) enforced by one startup
validator that walks every file in `registry/` and fails loudly, naming the offending
file and entry, before the server is allowed to advertise any of the 3 tools — never
partially load the registry.

---

## 14. Migration path (staged, not big-bang)

1. **Stage 0 — registry + validator only, zero behavior change.** Build the YAML
   registry and startup validator as a new module. Transcribe today's 36 tools into
   registry entries 1:1 (mechanical, not creative — the goal is a faithful transcription,
   not a redesign yet). The 36 tools stay registered and live exactly as today. No
   user-facing change, no risk.
2. **Stage 1 — add the 3 new tools alongside the 36, don't remove anything yet.**
   `build_payload`/`execute_request` call the *existing* `_logic` functions already in
   `main.py` (`search_leads_logic`, `create_lead_logic`, …) as their handlers for the
   ~33 plain rows — not reimplementations. This lets you dogfood the new flow in
   production, side by side, with the 36 originals as the safety net and the source of
   truth for parity.
3. **Stage 2 — automated parity suite.** For each of the 36 original tools, script the
   equivalent tool-call sequence from §11 and diff the final normalized result against
   the original tool's output for the same inputs, against a test/staging tenant or
   recorded fixtures. This extends the existing `test_tools.py` pattern (it already
   mocks Kylas responses and asserts on formatted output) rather than inventing a new
   harness. Wire this into CI so a registry entry that stops matching breaks the build
   — this is how you *prove* parity (AC6), not just assert it.
4. **Stage 3 — cut the client-facing surface, behind a flag.** Once parity is green
   and you've watched real dogfood traffic through the new 3-tool path for a period
   with logging on both, un-register the 36 `@mcp.tool()` decorators (they become
   internal-only functions, the same pattern this codebase already uses for the dead
   `search_leads`/`search_contacts`/`search_deals`/`search_meetings` wrappers) behind
   an env var, so you can flip back to "expose all 36" instantly if the new path
   regresses in production. Remove the flag and the old surface in a later cleanup
   release once you're confident.
5. **Stage 4 — prune `instructions` last, separately.** Move domain rules from the
   global `SYSTEM_INSTRUCTIONS` blob into per-registry-entry `usage_notes` (§5) *after*
   the 3-tool cutover has been stable — this is the change most likely to cause subtle
   behavior regressions (a rule that was always-visible becomes conditional, and might
   stop being reliably obeyed), and isolating it from the cutover keeps it bisectable
   if something breaks.

---

## 15. Summary of where I'd push back if this became a spec today

| Your framing | My read |
|---|---|
| Exactly 3 tools, no exceptions | Keep 3 as the rule; carve out a small, named exception list for composite operations (§7) and consider one for hard-delete confirmation (§9/§12-B.5) — say so explicitly rather than force-fit |
| `build_payload` validates before any network call | Revised (§3.2, §6): `build_payload` now takes only an `id` (no args) and returns a schema — it never sees or validates the model's actual filled-in values at all. That job moved entirely to `execute_request`, or nowhere, depending on the still-open §12-B.10 decision. A metadata-fetch call is still unavoidable either way, for tenant-custom-field schema-building. |
| `list_tool` should return full detail for a specific id | Corrected (§3.1): that was a redundant second copy of what `build_payload` already returns for an `id`. `list_tool` now does exactly one thing — coarse `id`/`bucket`/`intent`/`description` rows — at every specificity level, forever. |
| Adding an endpoint = YAML only, no code | True for ~33/36 today's-equivalent rows; not true for the ~3 composite ones, which need a named handler (§7) |
| Some tools must be called before others | Real today too (§8) — split into the internal discover→build→execute chain (mostly dissolved as an enforcement question now that `build_payload` doesn't validate anything, §8.1) and cross-endpoint ID-resolution prerequisites (currently unenforced convention; §8.2 proposes making it *safer* than today, not just equivalent) |
| Token savings are the main argument | Real and large across every scenario I walked through (§11), and it *improves* as a session reuses endpoints or does field-heavy creates — but wall-clock latency per operation goes up, and prompt caching (§12-A.1) could narrow the token gap; verify before treating the numbers as settled |
