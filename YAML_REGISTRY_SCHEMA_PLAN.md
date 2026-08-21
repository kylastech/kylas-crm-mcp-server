# Plan: Define the YAML Registry Schema (bucket config + endpoint entries, split across files)

Status: **plan / design draft** — no implementation yet. Companion to `TOOL_CONSOLIDATION_DESIGN.md`
(this document is a deeper, more concrete drill-down into that design's §13 registry sketch).

> **Amendment**: since this plan was written, the *calling convention* of the 3 tools was
> corrected in `TOOL_CONSOLIDATION_DESIGN.md` §3/§6/§12-B.10 — `list_tool` now has
> exactly one response mode (no more "detail mode"); `build_payload` takes only an `id`
> (no args) and returns the full schema, never validating the model's actual filled-in
> values; and whether `execute_request` re-validates before sending to Kylas is now an
> open decision (§12-B.10), not settled. **None of that changes the YAML schema itself**
> — the bucket/endpoint shapes below are still accurate — it only changes what the 3
> tools' request/response payloads look like when they read this data. The bucket-less
> shared-lookup file is also renamed `util` → `_meta` throughout, to match.

## Context

This is one step of a larger, already-agreed migration (see `TOOL_CONSOLIDATION_DESIGN.md` on
this branch): collapsing the Kylas CRM MCP server's 36 separate `@mcp.tool()` functions into
exactly 3 generic tools — `list_tool`, `build_payload`, `execute_request` — driven entirely
by a YAML registry instead of hardcoded per-tool functions. This plan covers **only** designing
that registry's schema and file layout — not writing the 3 tools' own code, not writing a
validator, not populating all ~55 real entries. That's deliberately out of scope here (per your
answer) so this stays a template/pattern other files can follow, not a giant one-shot artifact.

Three rounds of direct code exploration (grep + read, exact line numbers, no paraphrasing) plus
two independent schema-design passes went into this. The single most important finding driving
the shape below: **today's 8 entity types are NOT structurally uniform** — each has its own
picklist "use internal name vs Option ID" exception set, its own search-payload shape, its own
pagination base, and meeting/call_log diverge further still (different field-metadata shape, no
`"fields"` projection key, synthetic filterable fields, a filter-field alias). A flat one-schema-
fits-all row per tool would either silently break these differences or duplicate them per
endpoint (which is exactly how today's real bug — the deal cheat-sheet showing wrong picklist
guidance — happened: two different code paths reading two different constants for the same fact).

**Constraint carried through from earlier in this conversation: zero change to the actual
`_logic` functions or their real behavior.** Every decision below was checked against that —
see "Bug-preservation stance" section.

---

## 1. Two-level model, one file per bucket

Two distinct schema objects, but living **together in one file per bucket** (not split into
separate `buckets/` and `endpoints/` directories) — a single bucket file should answer "what
does this entity do" in one top-to-bottom read:

```
registry/
  index.yaml        # closed enums: bucket names, intent values, type values, formatter keys —
                     # the startup validator (future work) checks every entry against these
  _shape_defs.yaml   # the "standard" vs "meeting" field-dict-shape table (§4), defined once,
                     # referenced by name from each bucket file — never copy-pasted
  lead.yaml
  contact.yaml
  task.yaml
  deal.yaml
  company.yaml
  meeting.yaml
  call_log.yaml
  quotation.yaml     # search/get only — no create/update block, and no create/update endpoint
                     # entries anywhere in the file. Read-only enforced by ABSENCE, exactly
                     # matching how `_ENTITY_CRUD_CONFIG` already omits "quotation" today.
  _meta.yaml         # bucket-less shared reference lookups: lookup_users, lookup_products,
                     # lookup_pipelines, get_pipeline_stages, get_pipeline_details,
                     # get_current_user, get_entity_labels — plus add_note and
                     # parse_datetime_to_utc_iso_tool, which don't fit the "reference
                     # lookup" theme exactly but have no other natural home either;
                     # flagging this as a scope call rather than assuming it's obvious
```

**Renamed from `util` → `_meta`** to match the naming settled on in review. Note the
narrower framing: `_meta` is really "shared reference lookups" (resolve a name to an
ID) — `add_note` (an action, not a lookup) and `parse_datetime_to_utc_iso_tool` (a pure
computation, not a lookup at all) are grouped in here for lack of a better home, not
because they're the same kind of thing. If this bucket-less group grows, revisit
whether pure/utility entries deserve their own separate file instead of sharing one
with the lookup entries.

Each `{bucket}.yaml` has exactly two top-level keys: `bucket:` (the shared config, §2) and
`endpoints:` (the list of operations against it, §3) — no `kind:` discriminator needed, the
section name IS the discriminator.

Growth rule (for going from 36 tools / 8 buckets to ~150 / more buckets later): keep one file
per bucket; only split a single bucket file further once it exceeds ~150-200 lines or ~15
endpoints — a size-triggered split, not a scheme designed for a hypothetical future now.

---

## 2. Bucket-config schema

```yaml
# lead.yaml
bucket:
  label: "Lead"
  search:
    endpoint: /search/lead
    method: POST
    fields_key: present            # present | omitted — controls whether the search POST body
                                    # includes a top-level "fields": [...] projection array at all
    fields: [id, firstName, lastName, emails, phoneNumbers, ownerId, companyName, createdAt]
    rule_builder: generic           # generic | deal | company | meeting — symbolic tag naming
                                    # which of the 4 distinct real search-rule-building code
                                    # paths this bucket uses (call_log is tagged "meeting" since
                                    # it shares that formatter/shape, not its own bucket name)
    client_side_utc_convert: true   # true only for generic-rule-builder buckets (lead/contact/
                                    # task/quotation); deal/company/meeting/call_log attach
                                    # timeZone and let Kylas convert server-side instead
    pagination:
      base: 0                       # 0 | 1 — the dispatcher's actual source of truth for
                                    # zero- vs one-based page numbers (see §5, this REPLACES
                                    # today's dead `search_page_offset` field)
      max_size: 100
      default_sort: "createdAt,desc"
      sort_aliases: {}              # {caller_facing_sort_field: real_field} — empty for most
                                    # buckets, populated for meeting (see §2 meeting example)
    quirks:                         # escape hatch — see §2a. Empty for lead.
      operator_fallback: {}
      field_remap: {}
      synthetic_filterable_fields: {}
  picklist_internal_name_fields:    # THE one list for this bucket. Read by BOTH the endpoint's
    - requirementCurrency          # display/cheat-sheet-equivalent text (usage_notes) AND the
    - companyBusinessType          # filter-building logic. One list per bucket makes the
    - country                      # deal-cheat-sheet-drift bug class structurally impossible —
    - timezone                     # there is no second copy anywhere to disagree with this one.
    - companyIndustry
  field_metadata:
    endpoint: /entities/lead/fields
    method: GET
    params: {entityType: lead, custom-only: false, page: 0, size: 100}
    formatter: standard             # standard | meeting — key into _shape_defs.yaml (§4)
    cache_ttl_seconds: 300           # no caching exists today at all (every fetch hits Kylas
                                    # fresh, even multiple times per tool call) — this is a
                                    # deliberate improvement; MUST be keyed per-tenant when
                                    # implemented, not a single global dict (see the existing
                                    # `_ENTITY_LABELS` multi-tenant caching bug already flagged
                                    # in TOOL_CONSOLIDATION_DESIGN.md §12-D.14)
  normalize: true                   # true = create/update route field_values through the
                                    # shared normalization helpers (phone/email shorthand,
                                    # country-code normalization, custom-field-ID resolution);
                                    # false = passed straight through raw (meeting, call_log)
  supports_custom_fields: true      # false ONLY for call_log — no customFieldValues concept
                                    # at all for that bucket
```

### 2a. The `quirks` escape hatch — deliberately small, named, per-bucket

Only populated for buckets that actually need it (today: meeting, call_log). Nothing generic
in `build_payload`/`execute_request` branches on "does quirks exist" — each key is looked up by
name only when relevant, so 7 of 8 buckets carry `quirks: {}` at zero cost.

```yaml
# meeting.yaml (excerpt — the one bucket exercising every quirk)
bucket:
  search:
    fields_key: omitted            # meeting's search body is {"jsonRule": ...} only — no
    fields: null                   # field projection, always full objects back
    rule_builder: meeting
    client_side_utc_convert: false
    pagination:
      base: 1                      # meeting's Kylas API is 1-based
      max_size: 500                # notably higher cap than every other bucket
      default_sort: "from,desc"
      sort_aliases: {scheduledAt: from, updatedAt: from, startTime: from, startDate: from}
    quirks:
      operator_fallback: {ENTITY_PICKLIST: TEXT_FIELD}   # explicit declaration of today's
                                    # real behavior (OPERATOR_MAPPING has no ENTITY_PICKLIST
                                    # entry; meeting/call_log fields of that type fall back to
                                    # TEXT_FIELD's operator list). PRESERVED AS-IS PER YOUR
                                    # ANSWER — no behavior change, just made legible instead of
                                    # an implicit code fallthrough.
      field_remap: {scheduledAt: from}   # a caller-supplied FILTER on "scheduledAt" is
                                    # silently rewritten to "from" before validation — distinct
                                    # from sort_aliases above (same source field, different
                                    # target: a jsonRule field name vs. a sort-string token)
      synthetic_filterable_fields:  # fields the search-rule builder allows even though they
                                    # don't exist in the tenant's real field metadata at all
        associatedLeads:     {type: LOOK_UP, standard: true}
        associatedContacts:  {type: LOOK_UP, standard: true}
        associatedDeals:     {type: LOOK_UP, standard: true}
        associatedCompanies: {type: LOOK_UP, standard: true}
        participants:        {type: PARTICIPANTS_LOOKUP, standard: true}
  picklist_internal_name_fields: [status, medium]
  field_metadata:
    endpoint: /meetings/fields
    method: GET
    params: {custom-only: false, page: 0, size: 100}
    formatter: meeting
    cache_ttl_seconds: 300
  normalize: false
  supports_custom_fields: true
```

---

## 3. Endpoint-entry schema

```yaml
# lead.yaml (continued)
endpoints:
  - id: lead.search                 # <bucket>.<action>, globally unique across ALL files
    intent: search                  # closed enum: get | list | search | create | update |
                                     # delete | cancel | meta | pure
    type: http                      # http | composite | pure — see §3a
    method: POST                    # required for type: http
    path: /search/lead               # required for type: http; {param} templating for path params
    dynamic_fields: true             # true = this endpoint's accepted shape depends on live
                                     # tenant field metadata fetched at build_payload time (all
                                     # create/update/search rows); false = fixed shape
                                     # (_meta.parse_datetime, meeting.cancel's own body)
    destructive: false               # true only for meeting.delete-equivalent rows
    requires_confirmation: false     # kept distinct from destructive on purpose — destructive
                                     # is a property of the operation (irreversible);
                                     # requires_confirmation is a UX policy toggle. Today they're
                                     # 1:1, but don't conflate "dangerous" with "ask first" in
                                     # case a future non-destructive-but-expensive op needs the
                                     # latter without the former.
    resolve_via:                    # cross-entity ID fields + which OTHER endpoint resolves a
      - field: ownerId               # human name to that ID — a discovery HINT surfaced to the
        via: _meta.lookup_users       # model, not a runtime-enforced dependency (see design doc
                                     # §8 for the "light vs strict" enforcement discussion —
                                     # this plan only defines the data shape, not enforcement)
    description: "Filter/search leads by field criteria."   # ONE line — list-mode text, shown
                                     # for every entry regardless of detail level
    usage_notes: >                  # detail-mode-ONLY text — shown only when list_tool is
      Only [FILTERABLE] fields apply. Picklist fields use Option ID except
      requirementCurrency, companyBusinessType, country, timezone, companyIndustry
      (use internal name — see this bucket's picklist_internal_name_fields).
    see_also: [lead.search_by_term, lead.search_idle, lead.get, _meta.lookup_users]
    examples:
      - args: {filters: [{field: "companyName", operator: "contains", value: "Acme"}]}
        note: "Leads at companies matching 'Acme'"
```

### 3a. `type` enum — what each requires/forbids

| `type` | Requires | Forbids | Example |
|---|---|---|---|
| `http` | `method`, `path` | `handler` | `lead.search`, `meeting.delete` |
| `composite` | `handler` (a symbolic name resolved against a small internal Python dict at implementation time — same pattern as today's `_ENTITY_CONFIG` lookup) | `method`, `path` | `meeting.cancel`, `deal.update`, `task.search_any_relation`, `call_log.get_joined` |
| `pure` | nothing network-related | `method`, `path`, `resolve_via` | `_meta.parse_datetime` |

**Composite operations get `type: composite` + `handler: <name>` — nothing else.** No steps, no
conditionals, no DAG in YAML. The stage-lock fallback, the GET-then-POST cancel, the 4-way
parallel dedupe, and the two-call client join are control flow that already exists correctly in
Python; re-describing "if error code X, fetch Y, then loop N times" as YAML would mean building
a small workflow-expression-language purely to describe 4 known instances twice. `deal.create`/
`deal.update` are tagged `composite` too (not a separate `deal.advance_to_stage` endpoint) —
there is no distinct model-facing "advance stage" operation today, just ordinary deal
create/update that may internally fan out to N PUTs on a specific Kylas error code.

---

## 4. `_shape_defs.yaml` — the "standard" vs "meeting" field-dict-shape table (defined once)

```yaml
# _shape_defs.yaml
standard:
  picklist_options_path: [picklist, values]
  picklist_options_fallback_path: [picklist, picklistValues]
  standard_default: false
  show_options_for_types: [PICK_LIST, MULTI_PICKLIST, ENTITY_PICKLIST]
meeting:
  picklist_options_path: [picklist, picklistValues]      # reversed priority vs "standard"
  picklist_options_fallback_path: [picklist, values]
  standard_default: true                                  # opposite default vs "standard"
  show_options_for_types: [MULTI_PICKLIST, ENTITY_PICKLIST]   # plain PICK_LIST options are
                                                           # silently NOT shown for this shape —
                                                           # a real, preserved quirk
```
Each bucket's `field_metadata.formatter` is just a key into this file — declared once, not
copy-pasted into all 8 bucket files. If a 3rd shape is ever needed, it's a 3rd entry here.

---

## 5. Bug-preservation stance (checked against "zero change in current tools logic")

| Known issue found during exploration | Decision | Why it doesn't violate "zero change in logic" |
|---|---|---|
| `_ENTITY_CONFIG["search_page_offset"]` is hardcoded `0` for every entity — dead code; the real 1-based conversion for meeting/call_log lives in separate, unreferenced helper functions | **Fix**: `pagination.base` becomes the dispatcher's real source of truth | Purely internal plumbing — external HTTP requests to Kylas end up with the exact same page numbers either way, since the existing helper functions already compute the correct value today. No user-visible or API-visible behavior changes. |
| Deal's cheat-sheet display reads the *global* picklist-internal-name set, but deal's real search-rule builder reads its *own* separate set (`currency`/`dealSource` are affected) — confirmed drift, the cheat sheet currently shows wrong guidance | **Fix, structurally**: one `picklist_internal_name_fields` list per bucket, read by both the display text and the filter logic | The real filter-building `_logic` behavior is 100% untouched — only the previously-*wrong* display text becomes accurate. Flag this as an **expected, deliberate diff** in later parity testing (deal field-instructions text for `currency`/`dealSource` will legitimately change), not a regression. |
| `OPERATOR_MAPPING` has no `ENTITY_PICKLIST` entry — meeting/call_log picklist-type fields silently accept `TEXT_FIELD`'s operator list (`contains`, `begins_with`, etc.) instead of a proper picklist-shaped list | **Preserve exactly as-is**, per your answer — declare it explicitly via `quirks.operator_fallback: {ENTITY_PICKLIST: TEXT_FIELD}` rather than fixing it | This one DOES change real filtering behavior if "fixed" (which operators succeed/fail for a live tenant field). Declaring it as data makes the current choice legible without changing what any actual request does. |

---

## 6. What's explicitly OUT of scope for this task (per your scope answer)

- No Python code — not the 3 tools, not a startup validator, not the "small internal Python
  dict" the `handler:` field resolves against.
- No full population of all ~55 real registry entries across all 8 buckets — only the worked
  examples below, as a template. Populating the rest is separate follow-on work.
- No decision yet on runtime enforcement strength for `resolve_via` (light vs. strict, per the
  design doc's §8 discussion) — this plan only defines the data shape that hint lives in.

## Deliverable for this task

Create `registry/index.yaml`, `registry/_shape_defs.yaml`, and 5 fully worked example files
demonstrating every schema shape above:
1. `registry/lead.yaml` — plain CRUD bucket, no quirks (baseline pattern most future buckets follow)
2. `registry/meeting.yaml` — full quirks showcase (field_remap, synthetic_filterable_fields,
   operator_fallback, 1-based pagination, `meeting.cancel` as composite, `meeting.delete` as destructive)
3. `registry/deal.yaml` — `deal.update`/`deal.create` tagged `composite` (pipeline-stage-lock
   fallback), own picklist set demonstrating the drift-fix
4. `registry/call_log.yaml` — `supports_custom_fields: false`, reuses `meeting`'s formatter/shape tag
5. `registry/_meta.yaml` — bucket-less entries: `_meta.parse_datetime` (`type: pure`), `_meta.add_note`
   (entity-agnostic via body, not URL — carries a `target_entity_types` list inline since it has
   no bucket to hang that on)

## Verification

Since this is a schema-design-only deliverable (no code), verification is a manual/read-based
check, not a test run:
1. Read each of the 5 example files back and confirm every field traces to a specific, cited
   line/behavior in `main.py` from the exploration notes above (no invented fields).
2. Cross-check the 5 files against `TOOL_CONSOLIDATION_DESIGN.md` §7/§8/§9/§13 for consistency
   (this plan should be a strict refinement of that doc's §13 sketch, not a contradiction).
3. Confirm every one of the 36 current tool names can be traced to exactly one entry across
   these bucket files or an obvious follow-on entry in a not-yet-written bucket file (i.e. the
   schema shape has a home for all 36, even though only 5 buckets are fully written out).
4. When implementation actually begins (separate task), the real test is: write the startup
   validator, point it at these 5 files plus stub files for the other 3 buckets, and confirm it
   passes cleanly and fails loudly (naming file+entry) when a duplicate `id` or a `type: http`
   entry missing `path` is deliberately introduced.
